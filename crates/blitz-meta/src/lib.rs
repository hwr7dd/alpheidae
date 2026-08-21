//! blitz-meta — replicated Iceberg catalog with Raft election + membership.
//!
//!   * Leader election via RequestVote / AppendEntries heartbeats
//!   * Writes: leader appends to the Raft log, replicates, commits on majority
//!   * Dynamic membership: AddPeer / RemovePeer as Raft config log entries
//!     (single-server change). New peers get a snapshot so they catch up.
//!   * Epoch fencing on the catalog path; stale leaders cannot commit

pub mod persist;
pub mod raft;

use raft::{AppendEntries, LogEntry as RaftLogEntry, RaftNode, RaftState, RequestVote};
use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::Duration;

const OP_GET: u8 = 1;
const OP_PUT: u8 = 2;
const OP_REPL: u8 = 3;
const OP_PROMOTE: u8 = 4;
const OP_STATUS: u8 = 5;
const OP_ADD_PEER: u8 = 6;
const OP_REMOVE_PEER: u8 = 7;
const OP_SNAPSHOT: u8 = 8;

/// In-process node registry so AddPeer snapshot catch-up works without relying
/// on Docker loopback TCP (which can surface EAGAIN under load).
fn node_registry() -> &'static Mutex<HashMap<String, Weak<MetaNode>>> {
    static REG: OnceLock<Mutex<HashMap<String, Weak<MetaNode>>>> = OnceLock::new();
    REG.get_or_init(|| Mutex::new(HashMap::new()))
}

fn register_node(node: &Arc<MetaNode>) {
    node_registry()
        .lock()
        .unwrap()
        .insert(node.addr.clone(), Arc::downgrade(node));
}

fn lookup_node(addr: &str) -> Option<Arc<MetaNode>> {
    node_registry()
        .lock()
        .unwrap()
        .get(addr)
        .and_then(|w| w.upgrade())
}
fn w_str(s: &mut impl Write, v: &str) -> std::io::Result<()> {
    s.write_all(&(v.len() as u32).to_le_bytes())?;
    s.write_all(v.as_bytes())
}
fn r_str(s: &mut impl Read) -> std::io::Result<String> {
    let mut b = [0u8; 4];
    s.read_exact(&mut b)?;
    let n = u32::from_le_bytes(b) as usize;
    let mut v = vec![0u8; n];
    s.read_exact(&mut v)?;
    String::from_utf8(v).map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "utf8"))
}
fn w_u64(s: &mut impl Write, v: u64) -> std::io::Result<()> {
    s.write_all(&v.to_le_bytes())
}
fn r_u64(s: &mut impl Read) -> std::io::Result<u64> {
    let mut b = [0u8; 8];
    s.read_exact(&mut b)?;
    Ok(u64::from_le_bytes(b))
}

#[derive(Clone, Debug)]
pub struct LogEntry {
    pub epoch: u64,
    pub index: u64,
    pub key: String,
    pub val: String,
}

pub(crate) struct NodeState {
    pub(crate) epoch: u64,
    pub(crate) is_leader: bool,
    pub(crate) map: HashMap<String, String>,
    pub(crate) log: Vec<LogEntry>,
    pub(crate) alive: bool,
    /// New node waiting for AddPeer snapshot — must not self-elect as a 1-node cluster.
    pub(crate) awaiting_snapshot: bool,
}

pub struct MetaNode {
    pub addr: String,
    state: Arc<Mutex<NodeState>>,
    raft: Arc<Mutex<RaftNode>>,
    data_dir: Option<PathBuf>,
}

impl MetaNode {
    /// Start a node. `peers` is the *initial* membership (excluding self).
    /// Additional peers can be added later via `MetaClient::add_peer`.
    pub fn start(addr: &str, peers: Vec<String>, _is_leader: bool) -> Arc<MetaNode> {
        Self::start_inner(addr, peers, false, None)
    }

    /// Start with durable Raft/catalog storage under `data_dir`.
    pub fn start_persistent(
        addr: &str,
        peers: Vec<String>,
        data_dir: impl Into<PathBuf>,
    ) -> Arc<MetaNode> {
        Self::start_inner(addr, peers, false, Some(data_dir.into()))
    }

    /// Start a node that is not yet in any cluster. It will not elect itself;
    /// call `MetaClient::add_peer` on the leader, which installs a snapshot here.
    pub fn start_joiner(addr: &str) -> Arc<MetaNode> {
        Self::start_inner(addr, vec![], true, None)
    }

    pub fn start_joiner_persistent(addr: &str, data_dir: impl Into<PathBuf>) -> Arc<MetaNode> {
        Self::start_inner(addr, vec![], true, Some(data_dir.into()))
    }

    fn start_inner(
        addr: &str,
        peers: Vec<String>,
        awaiting_snapshot: bool,
        data_dir: Option<PathBuf>,
    ) -> Arc<MetaNode> {
        if let Some(dir) = &data_dir {
            let _ = std::fs::create_dir_all(dir);
        }

        let mut raft = if let Some(dir) = &data_dir {
            persist::load_raft(dir, addr)
                .ok()
                .flatten()
                .unwrap_or_else(|| RaftNode::new(addr.to_string(), peers.clone()))
        } else {
            RaftNode::new(addr.to_string(), peers.clone())
        };
        // Fresh peers from env override empty persisted peer list on first boot only.
        if raft.peers.is_empty() && !peers.is_empty() && !awaiting_snapshot {
            raft.peers = peers;
        }

        let (epoch, map, catalog_log) = if let Some(dir) = &data_dir {
            persist::load_catalog(dir)
                .ok()
                .flatten()
                .unwrap_or((1, HashMap::new(), vec![]))
        } else {
            (1, HashMap::new(), vec![])
        };

        let node = Arc::new(MetaNode {
            addr: addr.to_string(),
            state: Arc::new(Mutex::new(NodeState {
                epoch,
                is_leader: false,
                map,
                log: catalog_log,
                alive: true,
                awaiting_snapshot,
            })),
            raft: Arc::new(Mutex::new(raft)),
            data_dir,
        });

        let n2 = node.clone();
        let listener = TcpListener::bind(addr).expect("meta bind");
        let _ = listener.set_nonblocking(false);
        std::thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                let _ = stream.set_nodelay(true);
                let _ = stream.set_nonblocking(false);
                let n3 = n2.clone();
                std::thread::spawn(move || {
                    let _ = n3.handle(stream);
                });
            }
        });

        let n_ticker = node.clone();
        std::thread::spawn(move || {
            loop {
                std::thread::sleep(Duration::from_millis(100));
                n_ticker.tick();
            }
        });

        register_node(&node);
        node
    }

    fn persist_now(&self) {
        let Some(dir) = &self.data_dir else {
            return;
        };
        let raft = self.raft.lock().unwrap();
        let st = self.state.lock().unwrap();
        if let Err(e) = persist::save_raft(dir, &raft) {
            eprintln!("[{}] persist raft: {e}", self.addr);
        }
        if let Err(e) = persist::save_catalog(dir, &st) {
            eprintln!("[{}] persist catalog: {e}", self.addr);
        }
    }

    fn tick(&self) {
        if self.state.lock().unwrap().awaiting_snapshot {
            return; // joiner: wait for leader snapshot, do not self-elect
        }

        let raft_state = self.raft.lock().unwrap().state;

        if raft_state == RaftState::Follower {
            let mut raft = self.raft.lock().unwrap();
            if raft.election_timeout_expired() {
                raft.start_election();
                eprintln!(
                    "[{}] Starting election for term {}",
                    raft.id, raft.current_term
                );
            }
        }

        let raft_state = self.raft.lock().unwrap().state;

        if raft_state == RaftState::Candidate {
            let (id, term, peers) = {
                let raft = self.raft.lock().unwrap();
                (raft.id.clone(), raft.current_term, raft.peers.clone())
            };
            let mut votes = 1usize;
            for peer in &peers {
                // Do not hold the Raft mutex across the network call.
                let resp = {
                    let raft = self.raft.lock().unwrap();
                    let msg = (
                        raft.id.clone(),
                        raft.current_term,
                        raft.last_log_index(),
                        raft.last_log_term(),
                    );
                    drop(raft);
                    RaftNode::send_request_vote(msg.0.as_str(), msg.1, msg.2, msg.3, peer)
                };
                if let Some(resp) = resp {
                    if resp.vote_granted && resp.term == term {
                        votes += 1;
                    }
                }
            }
            let majority = {
                let raft = self.raft.lock().unwrap();
                raft.majority()
            };
            if votes >= majority {
                let mut raft = self.raft.lock().unwrap();
                if raft.state == RaftState::Candidate {
                    raft.become_leader();
                    let term = raft.current_term;
                    drop(raft);
                    let mut st = self.state.lock().unwrap();
                    st.is_leader = true;
                    st.epoch = term;
                    eprintln!("[{id}] WON election (term {term}, {votes} votes)");
                }
            }
        }

        let raft_state = self.raft.lock().unwrap().state;
        if raft_state == RaftState::Leader {
            let should_hb = {
                let raft = self.raft.lock().unwrap();
                raft.heartbeat_timeout_expired()
            };
            if should_hb {
                let (id, term, commit, peers) = {
                    let raft = self.raft.lock().unwrap();
                    (
                        raft.id.clone(),
                        raft.current_term,
                        raft.commit_index,
                        raft.peers.clone(),
                    )
                };
                for peer in &peers {
                    let _ = RaftNode::send_heartbeat(&id, term, commit, peer);
                }
                self.raft.lock().unwrap().reset_heartbeat_timeout();
            }
        }
    }

    pub fn kill(&self) {
        self.state.lock().unwrap().alive = false;
    }

    pub fn log_len(&self) -> usize {
        self.state.lock().unwrap().log.len()
    }

    pub fn peers(&self) -> Vec<String> {
        self.raft.lock().unwrap().peers.clone()
    }

    /// Apply committed Raft entries into the catalog map + membership.
    fn apply_committed(&self) -> Vec<String> {
        let applicable = self.raft.lock().unwrap().take_applicable();
        let mut newly_added = vec![];
        for e in applicable {
            if e.is_add_peer().is_some() || e.is_remove_peer().is_some() {
                if let Some(addr) = self.raft.lock().unwrap().apply_config_entry(&e) {
                    newly_added.push(addr);
                }
            } else if !e.key.starts_with("__raft/") {
                let mut st = self.state.lock().unwrap();
                st.map.insert(e.key.clone(), e.val.clone());
                st.log.push(LogEntry {
                    epoch: e.term,
                    index: e.index,
                    key: e.key,
                    val: e.val,
                });
            }
        }
        newly_added
    }

    /// Leader: append entry, replicate, commit, apply. Returns (index, newly_added_peers).
    fn leader_propose(&self, key: String, val: String) -> Result<(u64, Vec<String>), u8> {
        let entry = {
            let mut raft = self.raft.lock().unwrap();
            if raft.state != RaftState::Leader {
                return Err(2); // NOT_LEADER
            }
            raft.append_entry(key, val)
        };

        // Clone everything needed for network I/O — never hold the Raft lock
        // across TCP (that starves heartbeats and triggers election storms).
        let (peers, majority, term, epoch) = {
            let raft = self.raft.lock().unwrap();
            let st = self.state.lock().unwrap();
            (
                raft.peers.clone(),
                raft.majority(),
                raft.current_term,
                st.epoch,
            )
        };

        let mut acks = 1usize;
        for p in &peers {
            let (id, term_now, commit, log) = {
                let raft = self.raft.lock().unwrap();
                (
                    raft.id.clone(),
                    raft.current_term,
                    raft.commit_index,
                    raft.log.clone(),
                )
            };
            let ae_ok = RaftNode::send_append_entries(
                &id,
                term_now,
                commit,
                &log,
                p,
                std::slice::from_ref(&entry),
            )
            .map(|r| r.success && r.term == term)
            .unwrap_or(false);
            let repl_ok = Self::replicate_to(p, epoch, entry.index, &entry.key, &entry.val);
            if ae_ok || repl_ok {
                acks += 1;
            }
        }

        if acks < majority {
            return Err(3); // NO_QUORUM
        }

        {
            let mut raft = self.raft.lock().unwrap();
            if raft.state != RaftState::Leader {
                return Err(2);
            }
            raft.commit_index = entry.index;
        }
        let added = self.apply_committed();
        self.persist_now();
        Ok((entry.index, added))
    }

    fn activate_new_peers(&self, added: &[String]) {
        for addr in added {
            // Activate first so concurrent client writes can target the peer,
            // then install snapshot (map/log catch-up).
            self.raft.lock().unwrap().activate_peer(addr);
            if self.install_snapshot_on(addr) {
                eprintln!("[{}] peer {addr} activated + snapshotted", self.addr);
            } else {
                // Roll back activation if catch-up failed.
                let mut raft = self.raft.lock().unwrap();
                if let Some(i) = raft.peers.iter().position(|p| p == addr) {
                    raft.peers.remove(i);
                    if i < raft.next_index.len() {
                        raft.next_index.remove(i);
                    }
                    if i < raft.match_index.len() {
                        raft.match_index.remove(i);
                    }
                }
                eprintln!(
                    "[{}] snapshot to {addr} failed — peer deactivated",
                    self.addr
                );
            }
        }
    }

    /// Send full map + log to a newly added peer so it catches up.
    fn install_snapshot_on(&self, peer: &str) -> bool {
        // Lock order: raft then state (same as RPC handlers) to avoid deadlock.
        let (epoch, entries, map) = {
            let raft = self.raft.lock().unwrap();
            let st = self.state.lock().unwrap();
            (st.epoch, raft.log.clone(), st.map.clone())
        };
        let mut all = self.raft.lock().unwrap().peers.clone();
        if !all.iter().any(|p| p == &self.addr) {
            all.push(self.addr.clone());
        }

        if let Some(target) = lookup_node(peer) {
            target.apply_snapshot(epoch, all, entries, map);
            eprintln!("[{}] snapshot installed on {peer} (in-process)", self.addr);
            return true;
        }

        let entries_t: Vec<_> = entries
            .iter()
            .map(|e| (e.term, e.index, e.key.clone(), e.val.clone()))
            .collect();
        let map_t: Vec<_> = map.into_iter().collect();
        for attempt in 1..=5 {
            match Self::try_send_snapshot(peer, epoch, &all, &entries_t, &map_t) {
                Ok(true) => {
                    eprintln!("[{}] snapshot installed on {peer} (tcp)", self.addr);
                    return true;
                }
                Ok(false) => return false,
                Err(e) => {
                    eprintln!("[{}] snapshot tcp attempt {attempt} to {peer}: {e}", self.addr);
                    std::thread::sleep(Duration::from_millis(40 * attempt as u64));
                }
            }
        }
        false
    }

    fn apply_snapshot(
        &self,
        epoch: u64,
        snap_peers: Vec<String>,
        entries: Vec<RaftLogEntry>,
        map: HashMap<String, String>,
    ) {
        // Lock order: raft then state.
        {
            let mut raft = self.raft.lock().unwrap();
            raft.peers = snap_peers.into_iter().filter(|p| p != &self.addr).collect();
            raft.log = entries.clone();
            raft.commit_index = raft.last_log_index();
            raft.last_applied = raft.commit_index;
            raft.current_term = raft.current_term.max(epoch);
            raft.state = RaftState::Follower;
            raft.reset_election_timeout();
        }
        {
            let mut st = self.state.lock().unwrap();
            st.epoch = st.epoch.max(epoch);
            st.is_leader = false;
            st.awaiting_snapshot = false;
            st.map = map;
            st.log = entries
                .iter()
                .filter(|e| !e.key.starts_with("__raft/"))
                .map(|e| LogEntry {
                    epoch: e.term,
                    index: e.index,
                    key: e.key.clone(),
                    val: e.val.clone(),
                })
                .collect();
        }
        for e in &self.raft.lock().unwrap().log.clone() {
            if e.is_add_peer().is_some() || e.is_remove_peer().is_some() {
                self.raft.lock().unwrap().apply_config_entry(e);
            }
        }
        self.persist_now();
    }

    fn try_send_snapshot(
        peer: &str,
        epoch: u64,
        all: &[String],
        entries: &[(u64, u64, String, String)],
        map: &[(String, String)],
    ) -> std::io::Result<bool> {
        let mut s = TcpStream::connect(peer)?;
        s.set_nodelay(true)?;
        s.set_read_timeout(Some(Duration::from_secs(3)))?;
        s.set_write_timeout(Some(Duration::from_secs(3)))?;
        s.write_all(&[OP_SNAPSHOT])?;
        w_u64(&mut s, epoch)?;
        w_u64(&mut s, all.len() as u64)?;
        for p in all {
            w_str(&mut s, p)?;
        }
        w_u64(&mut s, entries.len() as u64)?;
        for (term, index, key, val) in entries {
            w_u64(&mut s, *term)?;
            w_u64(&mut s, *index)?;
            w_str(&mut s, key)?;
            w_str(&mut s, val)?;
        }
        w_u64(&mut s, map.len() as u64)?;
        for (k, v) in map {
            w_str(&mut s, k)?;
            w_str(&mut s, v)?;
        }
        let mut ack = [0u8; 1];
        s.read_exact(&mut ack)?;
        Ok(ack[0] == 1)
    }

    fn handle(&self, mut s: TcpStream) -> std::io::Result<()> {
        let mut op = [0u8; 1];
        s.read_exact(&mut op)?;

        if op[0] == 0xA1 {
            // Joiners must not participate in elections until snapshotted in.
            if self.state.lock().unwrap().awaiting_snapshot {
                s.write_all(&0u64.to_le_bytes())?;
                s.write_all(&[0u8])?; // vote denied
                return Ok(());
            }
            let mut term_buf = [0u8; 8];
            s.read_exact(&mut term_buf)?;
            let term = u64::from_le_bytes(term_buf);
            let mut cid_len_buf = [0u8; 4];
            s.read_exact(&mut cid_len_buf)?;
            let cid_len = u32::from_le_bytes(cid_len_buf) as usize;
            let mut cid_buf = vec![0u8; cid_len];
            s.read_exact(&mut cid_buf)?;
            let candidate_id = String::from_utf8(cid_buf)
                .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "utf8"))?;
            let last_log_index = r_u64(&mut s)?;
            let last_log_term = r_u64(&mut s)?;
            let req = RequestVote {
                term,
                candidate_id,
                last_log_index,
                last_log_term,
            };
            let mut raft = self.raft.lock().unwrap();
            let resp = raft.handle_request_vote(&req);
            if raft.state == RaftState::Follower {
                self.state.lock().unwrap().is_leader = false;
            }
            s.write_all(&resp.term.to_le_bytes())?;
            s.write_all(&[resp.vote_granted as u8])?;
            return Ok(());
        }

        if op[0] == 0xA2 {
            let mut term_buf = [0u8; 8];
            s.read_exact(&mut term_buf)?;
            let term = u64::from_le_bytes(term_buf);
            let mut lid_len_buf = [0u8; 4];
            s.read_exact(&mut lid_len_buf)?;
            let lid_len = u32::from_le_bytes(lid_len_buf) as usize;
            let mut lid_buf = vec![0u8; lid_len];
            s.read_exact(&mut lid_buf)?;
            let leader_id = String::from_utf8(lid_buf)
                .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "utf8"))?;
            let prev_log_index = r_u64(&mut s)?;
            let prev_log_term = r_u64(&mut s)?;
            let leader_commit = r_u64(&mut s)?;
            let mut count_buf = [0u8; 4];
            s.read_exact(&mut count_buf)?;
            let entry_count = u32::from_le_bytes(count_buf) as usize;
            let mut entries = Vec::new();
            for _ in 0..entry_count {
                let eterm = r_u64(&mut s)?;
                let eidx = r_u64(&mut s)?;
                let key = r_str(&mut s)?;
                let val = r_str(&mut s)?;
                entries.push(RaftLogEntry {
                    term: eterm,
                    index: eidx,
                    key,
                    val,
                });
            }
            let req = AppendEntries {
                term,
                leader_id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            };
            let mut raft = self.raft.lock().unwrap();
            let resp = raft.handle_append_entries(&req);
            if raft.state == RaftState::Follower {
                self.state.lock().unwrap().is_leader = false;
            }
            drop(raft);
            self.apply_committed();
            s.write_all(&resp.term.to_le_bytes())?;
            s.write_all(&[resp.success as u8])?;
            return Ok(());
        }

        match op[0] {
            OP_GET => {
                let key = r_str(&mut s)?;
                let st = self.state.lock().unwrap();
                if !st.alive {
                    return Ok(());
                }
                match st.map.get(&key) {
                    Some(v) => {
                        s.write_all(&[1])?;
                        w_str(&mut s, v)?;
                    }
                    None => s.write_all(&[0])?,
                }
            }
            OP_PUT => {
                let key = r_str(&mut s)?;
                let val = r_str(&mut s)?;
                {
                    let st = self.state.lock().unwrap();
                    if !st.alive {
                        return Ok(());
                    }
                    if !st.is_leader {
                        s.write_all(&[2])?;
                        return Ok(());
                    }
                }
                match self.leader_propose(key, val) {
                    Ok((index, added)) => {
                        s.write_all(&[1])?;
                        w_u64(&mut s, index)?;
                        self.activate_new_peers(&added);
                    }
                    Err(code) => s.write_all(&[code])?,
                }
            }
            OP_ADD_PEER => {
                let addr = r_str(&mut s)?;
                {
                    let st = self.state.lock().unwrap();
                    if !st.alive || !st.is_leader {
                        s.write_all(&[2])?;
                        return Ok(());
                    }
                }
                if self.raft.lock().unwrap().peers.iter().any(|p| p == &addr) || addr == self.addr
                {
                    s.write_all(&[1])?;
                    w_u64(&mut s, 0)?;
                    return Ok(());
                }
                let key = format!("__raft/add:{addr}");
                match self.leader_propose(key, String::new()) {
                    Ok((index, added)) => {
                        s.write_all(&[1])?;
                        w_u64(&mut s, index)?;
                        let addr = self.addr.clone();
                        let state = self.state.clone();
                        let raft = self.raft.clone();
                        std::thread::spawn(move || {
                            let node = MetaNode {
                                addr,
                                state,
                                raft,
                                data_dir: None,
                            };
                            node.activate_new_peers(&added);
                        });
                        return Ok(());
                    }
                    Err(code) => s.write_all(&[code])?,
                }
            }
            OP_REMOVE_PEER => {
                let addr = r_str(&mut s)?;
                {
                    let st = self.state.lock().unwrap();
                    if !st.alive || !st.is_leader {
                        s.write_all(&[2])?;
                        return Ok(());
                    }
                }
                let key = format!("__raft/remove:{addr}");
                match self.leader_propose(key, String::new()) {
                    Ok((index, added)) => {
                        s.write_all(&[1])?;
                        w_u64(&mut s, index)?;
                        self.activate_new_peers(&added);
                    }
                    Err(code) => s.write_all(&[code])?,
                }
            }
            OP_SNAPSHOT => {
                let epoch = r_u64(&mut s)?;
                let n_peers = r_u64(&mut s)? as usize;
                let mut snap_peers = Vec::with_capacity(n_peers);
                for _ in 0..n_peers {
                    snap_peers.push(r_str(&mut s)?);
                }
                let n_entries = r_u64(&mut s)? as usize;
                let mut entries = Vec::with_capacity(n_entries);
                for _ in 0..n_entries {
                    let term = r_u64(&mut s)?;
                    let index = r_u64(&mut s)?;
                    let key = r_str(&mut s)?;
                    let val = r_str(&mut s)?;
                    entries.push(RaftLogEntry {
                        term,
                        index,
                        key,
                        val,
                    });
                }
                let n_map = r_u64(&mut s)? as usize;
                let mut map = HashMap::new();
                for _ in 0..n_map {
                    let k = r_str(&mut s)?;
                    let v = r_str(&mut s)?;
                    map.insert(k, v);
                }
                self.apply_snapshot(epoch, snap_peers, entries, map);
                s.write_all(&[1])?;
            }
            OP_REPL => {
                let epoch = r_u64(&mut s)?;
                let index = r_u64(&mut s)?;
                let key = r_str(&mut s)?;
                let val = r_str(&mut s)?;
                let mut st = self.state.lock().unwrap();
                if !st.alive {
                    return Ok(());
                }
                if epoch < st.epoch {
                    s.write_all(&[0])?;
                } else {
                    st.epoch = epoch;
                    st.is_leader = false;
                    if key.starts_with("__raft/add:") || key.starts_with("__raft/remove:") {
                        drop(st);
                        let e = RaftLogEntry {
                            term: epoch,
                            index,
                            key,
                            val,
                        };
                        self.raft.lock().unwrap().apply_config_entry(&e);
                    } else {
                        st.log.push(LogEntry {
                            epoch,
                            index,
                            key: key.clone(),
                            val: val.clone(),
                        });
                        st.map.insert(key, val);
                    }
                    s.write_all(&[1])?;
                }
            }
            OP_PROMOTE => {
                let epoch = r_u64(&mut s)?;
                let mut st = self.state.lock().unwrap();
                if !st.alive {
                    return Ok(());
                }
                if epoch > st.epoch {
                    st.epoch = epoch;
                    st.is_leader = true;
                    s.write_all(&[1])?;
                } else {
                    s.write_all(&[0])?;
                }
            }
            OP_STATUS => {
                let (is_leader, epoch, log_len) = {
                    let st = self.state.lock().unwrap();
                    (st.is_leader, st.epoch, st.log.len() as u64)
                };
                let peers = self.raft.lock().unwrap().peers.len() as u64;
                s.write_all(&[is_leader as u8])?;
                w_u64(&mut s, epoch)?;
                w_u64(&mut s, log_len)?;
                w_u64(&mut s, peers)?;
            }
            _ => {}
        }
        Ok(())
    }

    fn replicate_to(peer: &str, epoch: u64, index: u64, key: &str, val: &str) -> bool {
        let Ok(mut s) = TcpStream::connect(peer) else {
            return false;
        };
        s.set_read_timeout(Some(Duration::from_millis(200))).ok();
        (|| -> std::io::Result<bool> {
            s.write_all(&[OP_REPL])?;
            w_u64(&mut s, epoch)?;
            w_u64(&mut s, index)?;
            w_str(&mut s, key)?;
            w_str(&mut s, val)?;
            let mut r = [0u8; 1];
            s.read_exact(&mut r)?;
            Ok(r[0] == 1)
        })()
        .unwrap_or(false)
    }
}

// ---------------------------------------------------------------------------
// client
// ---------------------------------------------------------------------------

pub struct MetaClient {
    pub leader: String,
}

#[derive(Debug, PartialEq)]
pub enum PutResult {
    Committed(u64),
    NotLeader,
    Fenced,
    Unreachable,
}

impl MetaClient {
    pub fn new(leader: &str) -> Self {
        MetaClient {
            leader: leader.to_string(),
        }
    }

    pub fn put(&self, key: &str, val: &str) -> PutResult {
        self.rpc_kv(OP_PUT, key, Some(val))
    }

    /// Grow membership: leader commits `__raft/add:<addr>` then snapshots the peer.
    pub fn add_peer(&self, addr: &str) -> PutResult {
        self.rpc_kv(OP_ADD_PEER, addr, None)
    }

    /// Shrink membership: leader commits `__raft/remove:<addr>`.
    pub fn remove_peer(&self, addr: &str) -> PutResult {
        self.rpc_kv(OP_REMOVE_PEER, addr, None)
    }

    fn rpc_kv(&self, op: u8, a: &str, b: Option<&str>) -> PutResult {
        let Ok(mut s) = TcpStream::connect(&self.leader) else {
            return PutResult::Unreachable;
        };
        s.set_read_timeout(Some(Duration::from_millis(2000))).ok();
        (|| -> std::io::Result<PutResult> {
            s.write_all(&[op])?;
            w_str(&mut s, a)?;
            if let Some(v) = b {
                w_str(&mut s, v)?;
            }
            let mut code = [0u8; 1];
            s.read_exact(&mut code)?;
            Ok(match code[0] {
                1 => PutResult::Committed(r_u64(&mut s).unwrap_or(0)),
                2 => PutResult::NotLeader,
                _ => PutResult::Fenced,
            })
        })()
        .unwrap_or(PutResult::Unreachable)
    }

    pub fn get(&self, key: &str) -> Option<String> {
        let mut s = TcpStream::connect_timeout(
            &self.leader.parse().ok()?,
            Duration::from_millis(500),
        )
        .ok()?;
        s.set_read_timeout(Some(Duration::from_millis(500))).ok()?;
        s.write_all(&[OP_GET]).ok()?;
        w_str(&mut s, key).ok()?;
        let mut b = [0u8; 1];
        s.read_exact(&mut b).ok()?;
        if b[0] == 1 {
            r_str(&mut s).ok()
        } else {
            None
        }
    }

    pub fn promote(addr: &str, epoch: u64) -> bool {
        let Ok(mut s) = TcpStream::connect(addr) else {
            return false;
        };
        let _ = s.write_all(&[OP_PROMOTE]);
        let _ = w_u64(&mut s, epoch);
        let mut b = [0u8; 1];
        s.read_exact(&mut b).map(|_| b[0] == 1).unwrap_or(false)
    }

    /// (is_leader, epoch, log_len, peer_count)
    pub fn status(addr: &str) -> Option<(bool, u64, u64, u64)> {
        let mut s = TcpStream::connect_timeout(
            &addr.parse().ok()?,
            Duration::from_millis(500),
        )
        .ok()?;
        s.set_read_timeout(Some(Duration::from_millis(500))).ok()?;
        s.write_all(&[OP_STATUS]).ok()?;
        let mut leader = [0u8; 1];
        s.read_exact(&mut leader).ok()?;
        let epoch = r_u64(&mut s).ok()?;
        let log_len = r_u64(&mut s).ok()?;
        let peers = r_u64(&mut s).unwrap_or(0);
        Some((leader[0] != 0, epoch, log_len, peers))
    }

    pub fn wait_for_leader(addrs: &[&str], timeout_ms: u64) -> Option<String> {
        let deadline = std::time::Instant::now() + Duration::from_millis(timeout_ms);
        while std::time::Instant::now() < deadline {
            for a in addrs {
                if let Some((true, _, _, _)) = Self::status(a) {
                    return Some((*a).to_string());
                }
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        None
    }

    pub fn commit_table(&self, name: &str, metadata_path: &str) -> PutResult {
        self.put(&format!("iceberg.table.{name}"), metadata_path)
    }
    pub fn load_table(&self, name: &str) -> Option<String> {
        self.get(&format!("iceberg.table.{name}"))
    }
}
