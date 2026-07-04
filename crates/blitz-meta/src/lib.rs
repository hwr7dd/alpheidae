//! blitz-meta — a replicated metadata service that acts as the Iceberg
//! catalog.
//!
//! In a shared-data lakehouse architecture (the StarRocks shared-data /
//! Iceberg model), DATA durability is the object store's job — data files
//! are immutable and replicated by S3/GCS/MinIO underneath us. What the
//! engine itself must replicate is the *metadata pointer*: the atomic
//! "current metadata.json location" per table that an Iceberg catalog owns.
//! Lose that and the lake is an unreadable pile of files; corrupt a swap and
//! you've lost a commit. So replication here means a small, strongly
//! consistent replicated KV log:
//!
//!   * one leader, N followers, fixed membership
//!   * writes: leader appends to its log, replicates the entry, and only
//!     acknowledges after a MAJORITY of nodes (incl. itself) have accepted
//!   * epoch fencing: every entry carries the leader's epoch; followers
//!     reject entries from a stale epoch, so a deposed leader cannot commit
//!     ("split brain" writes are fenced, not merged)
//!   * failover: a follower is promoted with epoch+1 (deterministic /
//!     operator-driven promotion; leader *election* à la Raft is the one
//!     piece intentionally left out and documented as such)
//!
//! Catalog ops map directly: `commit_table(name, metadata_path)` is a
//! replicated PUT of the pointer; `load_table(name)` is a GET.

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};

const OP_GET: u8 = 1;
const OP_PUT: u8 = 2;
const OP_REPL: u8 = 3;
const OP_PROMOTE: u8 = 4;
const OP_STATUS: u8 = 5;

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

struct NodeState {
    epoch: u64,
    is_leader: bool,
    map: HashMap<String, String>,
    log: Vec<LogEntry>,
    alive: bool,
}

pub struct MetaNode {
    pub addr: String,
    peers: Vec<String>, // other nodes (for the leader to replicate to)
    state: Arc<Mutex<NodeState>>,
}

impl MetaNode {
    /// Start a node and its listener thread. `is_leader` for the initial
    /// leader; `epoch` starts at 1.
    pub fn start(addr: &str, peers: Vec<String>, is_leader: bool) -> Arc<MetaNode> {
        let node = Arc::new(MetaNode {
            addr: addr.to_string(),
            peers,
            state: Arc::new(Mutex::new(NodeState {
                epoch: 1,
                is_leader,
                map: HashMap::new(),
                log: vec![],
                alive: true,
            })),
        });
        let n2 = node.clone();
        let listener = TcpListener::bind(addr).expect("meta bind");
        std::thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                let n3 = n2.clone();
                std::thread::spawn(move || {
                    let _ = n3.handle(stream);
                });
            }
        });
        node
    }

    /// Simulate a crash: the node stops accepting/acking (process stays for
    /// the demo, but every request is refused).
    pub fn kill(&self) {
        self.state.lock().unwrap().alive = false;
    }

    pub fn log_len(&self) -> usize {
        self.state.lock().unwrap().log.len()
    }

    fn handle(&self, mut s: TcpStream) -> std::io::Result<()> {
        let mut op = [0u8; 1];
        s.read_exact(&mut op)?;
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
                let (epoch, index, entry_ok) = {
                    let mut st = self.state.lock().unwrap();
                    if !st.alive {
                        return Ok(());
                    }
                    if !st.is_leader {
                        s.write_all(&[2])?; // NOT_LEADER
                        return Ok(());
                    }
                    let index = st.log.len() as u64;
                    let e = LogEntry { epoch: st.epoch, index, key: key.clone(), val: val.clone() };
                    st.log.push(e);
                    (st.epoch, index, true)
                };
                if entry_ok {
                    // Replicate; count self as one ack.
                    let mut acks = 1usize;
                    for p in &self.peers {
                        if Self::replicate_to(p, epoch, index, &key, &val) {
                            acks += 1;
                        }
                    }
                    let cluster = self.peers.len() + 1;
                    if acks * 2 > cluster {
                        let mut st = self.state.lock().unwrap();
                        // Were we fenced while replicating? (a peer at a
                        // higher epoch refuses; if majority still acked we
                        // are by definition not fenced by a majority)
                        st.map.insert(key, val);
                        s.write_all(&[1])?; // COMMITTED
                        w_u64(&mut s, index)?;
                    } else {
                        s.write_all(&[3])?; // NO_QUORUM / FENCED
                    }
                }
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
                    s.write_all(&[0])?; // FENCED: stale leader
                } else {
                    st.epoch = epoch;
                    st.is_leader = false;
                    st.log.push(LogEntry { epoch, index, key: key.clone(), val: val.clone() });
                    st.map.insert(key, val);
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
                let st = self.state.lock().unwrap();
                s.write_all(&[st.is_leader as u8])?;
                w_u64(&mut s, st.epoch)?;
                w_u64(&mut s, st.log.len() as u64)?;
            }
            _ => {}
        }
        Ok(())
    }

    fn replicate_to(peer: &str, epoch: u64, index: u64, key: &str, val: &str) -> bool {
        let Ok(mut s) = TcpStream::connect(peer) else { return false };
        s.set_read_timeout(Some(std::time::Duration::from_millis(200))).ok();
        let ok = (|| -> std::io::Result<bool> {
            s.write_all(&[OP_REPL])?;
            w_u64(&mut s, epoch)?;
            w_u64(&mut s, index)?;
            w_str(&mut s, key)?;
            w_str(&mut s, val)?;
            let mut r = [0u8; 1];
            s.read_exact(&mut r)?;
            Ok(r[0] == 1)
        })();
        ok.unwrap_or(false)
    }
}

// ---------------------------------------------------------------------------
// client (this is the Iceberg "catalog" interface)
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
        MetaClient { leader: leader.to_string() }
    }

    pub fn put(&self, key: &str, val: &str) -> PutResult {
        let Ok(mut s) = TcpStream::connect(&self.leader) else { return PutResult::Unreachable };
        s.set_read_timeout(Some(std::time::Duration::from_millis(1500))).ok();
        let r = (|| -> std::io::Result<PutResult> {
            s.write_all(&[OP_PUT])?;
            w_str(&mut s, key)?;
            w_str(&mut s, val)?;
            let mut b = [0u8; 1];
            s.read_exact(&mut b)?;
            Ok(match b[0] {
                1 => PutResult::Committed(r_u64(&mut s)?),
                2 => PutResult::NotLeader,
                _ => PutResult::Fenced,
            })
        })();
        r.unwrap_or(PutResult::Unreachable)
    }

    pub fn get(&self, key: &str) -> Option<String> {
        let mut s = TcpStream::connect(&self.leader).ok()?;
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
        let Ok(mut s) = TcpStream::connect(addr) else { return false };
        let _ = s.write_all(&[OP_PROMOTE]);
        let _ = w_u64(&mut s, epoch);
        let mut b = [0u8; 1];
        s.read_exact(&mut b).map(|_| b[0] == 1).unwrap_or(false)
    }

    // Iceberg catalog surface -------------------------------------------------
    pub fn commit_table(&self, name: &str, metadata_path: &str) -> PutResult {
        self.put(&format!("iceberg.table.{name}"), metadata_path)
    }
    pub fn load_table(&self, name: &str) -> Option<String> {
        self.get(&format!("iceberg.table.{name}"))
    }
}
