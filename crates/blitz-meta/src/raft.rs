//! Raft consensus: leader election, log replication, and dynamic membership.
//!
//! Membership changes follow the single-server change approach from the Raft
//! paper (§6): add/remove one peer at a time. A config change is a normal log
//! entry (`__raft/add:<addr>` / `__raft/remove:<addr>`). After it commits under
//! the *old* majority, the new configuration takes effect and the leader
//! installs a snapshot on a newly added peer so it catches up on history.

use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RaftState {
    Follower,
    Candidate,
    Leader,
}

/// Raft log entry. Config changes use reserved keys:
///   `__raft/add:<addr>` / `__raft/remove:<addr>`
#[derive(Clone, Debug)]
pub struct LogEntry {
    pub term: u64,
    pub index: u64,
    pub key: String,
    pub val: String,
}

impl LogEntry {
    pub fn is_add_peer(&self) -> Option<&str> {
        self.key.strip_prefix("__raft/add:")
    }
    pub fn is_remove_peer(&self) -> Option<&str> {
        self.key.strip_prefix("__raft/remove:")
    }
    pub fn add_peer(term: u64, index: u64, addr: &str) -> Self {
        LogEntry {
            term,
            index,
            key: format!("__raft/add:{addr}"),
            val: String::new(),
        }
    }
    pub fn remove_peer(term: u64, index: u64, addr: &str) -> Self {
        LogEntry {
            term,
            index,
            key: format!("__raft/remove:{addr}"),
            val: String::new(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct RequestVote {
    pub term: u64,
    pub candidate_id: String,
    pub last_log_index: u64,
    pub last_log_term: u64,
}

#[derive(Clone, Debug)]
pub struct RequestVoteResponse {
    pub term: u64,
    pub vote_granted: bool,
}

#[derive(Clone, Debug)]
pub struct AppendEntries {
    pub term: u64,
    pub leader_id: String,
    pub prev_log_index: u64,
    pub prev_log_term: u64,
    pub entries: Vec<LogEntry>,
    pub leader_commit: u64,
}

#[derive(Clone, Debug)]
pub struct AppendEntriesResponse {
    pub term: u64,
    pub success: bool,
}

pub struct RaftNode {
    pub id: String,
    /// Current cluster peers (excluding self). Updated when config changes commit.
    pub peers: Vec<String>,

    pub current_term: u64,
    pub voted_for: Option<String>,
    pub log: Vec<LogEntry>,

    pub state: RaftState,
    pub commit_index: u64,
    pub last_applied: u64,

    pub next_index: Vec<u64>,
    pub match_index: Vec<u64>,

    pub election_timeout: Instant,
    pub heartbeat_timeout: Instant,
}

impl RaftNode {
    pub fn new(id: String, peers: Vec<String>) -> Self {
        let election_timeout_ms = 250 + Self::jitter_ms(&id);
        RaftNode {
            id,
            peers: peers.clone(),
            current_term: 0,
            voted_for: None,
            log: vec![],
            state: RaftState::Follower,
            commit_index: 0,
            last_applied: 0,
            next_index: vec![0; peers.len()],
            match_index: vec![0; peers.len()],
            election_timeout: Instant::now() + Duration::from_millis(election_timeout_ms),
            heartbeat_timeout: Instant::now() + Duration::from_millis(50),
        }
    }

    fn jitter_ms(id: &str) -> u64 {
        let mut h: u64 = 0xcbf29ce484222325;
        for b in id.bytes() {
            h ^= b as u64;
            h = h.wrapping_mul(0x100000001b3);
        }
        // Spread 0..400 so co-started nodes rarely share the same deadline.
        (h % 400) + (h.wrapping_shr(8) % 50)
    }

    pub fn cluster_size(&self) -> usize {
        self.peers.len() + 1
    }

    pub fn majority(&self) -> usize {
        self.cluster_size() / 2 + 1
    }

    pub fn election_timeout_expired(&self) -> bool {
        Instant::now() > self.election_timeout
    }

    pub fn heartbeat_timeout_expired(&self) -> bool {
        Instant::now() > self.heartbeat_timeout
    }

    pub fn reset_election_timeout(&mut self) {
        // 250–700ms — above heartbeat tick, with wide jitter to avoid split votes.
        let election_timeout_ms = 250 + Self::jitter_ms(&self.id);
        self.election_timeout = Instant::now() + Duration::from_millis(election_timeout_ms);
    }

    pub fn reset_heartbeat_timeout(&mut self) {
        self.heartbeat_timeout = Instant::now() + Duration::from_millis(50);
    }

    pub fn start_election(&mut self) {
        self.state = RaftState::Candidate;
        self.current_term += 1;
        self.voted_for = Some(self.id.clone());
        self.reset_election_timeout();
    }

    pub fn become_leader(&mut self) {
        self.state = RaftState::Leader;
        let n = self.peers.len();
        let next = self.log.len() as u64 + 1;
        self.next_index = vec![next; n];
        self.match_index = vec![0; n];
        self.reset_heartbeat_timeout();
    }

    pub fn last_log_index(&self) -> u64 {
        self.log.last().map(|e| e.index).unwrap_or(0)
    }

    pub fn last_log_term(&self) -> u64 {
        self.log.last().map(|e| e.term).unwrap_or(0)
    }

    pub fn append_entry(&mut self, key: String, val: String) -> LogEntry {
        let index = self.last_log_index() + 1;
        let e = LogEntry {
            term: self.current_term,
            index,
            key,
            val,
        };
        self.log.push(e.clone());
        e
    }

    /// Apply membership side-effects for a committed config entry.
    /// On the leader, AddPeer is deferred until `activate_peer` after snapshot.
    /// Followers activate immediately (they don't heartbeat to the new peer).
    pub fn apply_config_entry(&mut self, entry: &LogEntry) -> Option<String> {
        if let Some(addr) = entry.is_add_peer() {
            if addr != self.id && !self.peers.iter().any(|p| p == addr) {
                if self.state == RaftState::Leader {
                    return Some(addr.to_string());
                }
                self.peers.push(addr.to_string());
            }
            return None;
        } else if let Some(addr) = entry.is_remove_peer() {
            if let Some(i) = self.peers.iter().position(|p| p == addr) {
                self.peers.remove(i);
                if self.state == RaftState::Leader
                    && i < self.next_index.len()
                    && i < self.match_index.len()
                {
                    self.next_index.remove(i);
                    self.match_index.remove(i);
                }
            }
            if addr == self.id {
                self.state = RaftState::Follower;
            }
        }
        None
    }

    /// Activate a peer for voting/replication after snapshot install succeeds.
    pub fn activate_peer(&mut self, addr: &str) {
        if addr != self.id && !self.peers.iter().any(|p| p == addr) {
            self.peers.push(addr.to_string());
            if self.state == RaftState::Leader {
                self.next_index.push(self.log.len() as u64 + 1);
                self.match_index.push(0);
            }
        }
    }

    /// Heartbeats: prev_log_index=0 so lagging followers still reset timers.
    pub fn send_heartbeat(
        id: &str,
        term: u64,
        commit_index: u64,
        peer: &str,
    ) -> Option<AppendEntriesResponse> {
        Self::send_append_entries(id, term, commit_index, &[], peer, &[])
    }

    pub fn handle_request_vote(&mut self, req: &RequestVote) -> RequestVoteResponse {
        if req.term > self.current_term {
            self.current_term = req.term;
            self.state = RaftState::Follower;
            self.voted_for = None;
        }

        if req.term < self.current_term {
            return RequestVoteResponse {
                term: self.current_term,
                vote_granted: false,
            };
        }

        if let Some(voted) = &self.voted_for {
            if voted != &req.candidate_id {
                return RequestVoteResponse {
                    term: self.current_term,
                    vote_granted: false,
                };
            }
        }

        let last_log_index = self.last_log_index();
        let last_log_term = self.last_log_term();
        if req.last_log_term < last_log_term
            || (req.last_log_term == last_log_term && req.last_log_index < last_log_index)
        {
            return RequestVoteResponse {
                term: self.current_term,
                vote_granted: false,
            };
        }

        self.voted_for = Some(req.candidate_id.clone());
        self.reset_election_timeout();
        RequestVoteResponse {
            term: self.current_term,
            vote_granted: true,
        }
    }

    pub fn handle_append_entries(&mut self, req: &AppendEntries) -> AppendEntriesResponse {
        if req.term > self.current_term {
            self.current_term = req.term;
            self.state = RaftState::Follower;
            self.voted_for = None;
        }

        if req.term < self.current_term {
            return AppendEntriesResponse {
                term: self.current_term,
                success: false,
            };
        }

        self.reset_election_timeout();
        self.state = RaftState::Follower;

        let prev_log_ok = if req.prev_log_index == 0 {
            true
        } else if req.prev_log_index as usize > self.log.len() {
            false
        } else {
            self.log[req.prev_log_index as usize - 1].term == req.prev_log_term
        };

        if !prev_log_ok {
            return AppendEntriesResponse {
                term: self.current_term,
                success: false,
            };
        }

        let mut idx = req.prev_log_index as usize;
        for entry in &req.entries {
            idx += 1;
            if idx <= self.log.len() && self.log[idx - 1].term != entry.term {
                self.log.truncate(idx - 1);
            }
            if idx > self.log.len() {
                self.log.push(entry.clone());
            } else if idx <= self.log.len() {
                // Same index already present with matching term — keep.
            }
        }

        if req.leader_commit > self.commit_index {
            self.commit_index = req.leader_commit.min(self.last_log_index());
        }

        AppendEntriesResponse {
            term: self.current_term,
            success: true,
        }
    }

    /// Entries with index in (last_applied, commit_index] ready to apply.
    pub fn take_applicable(&mut self) -> Vec<LogEntry> {
        let mut out = vec![];
        while self.last_applied < self.commit_index {
            self.last_applied += 1;
            if let Some(e) = self.log.iter().find(|e| e.index == self.last_applied) {
                out.push(e.clone());
            }
        }
        out
    }

    pub fn send_request_vote_to_peer(&self, peer: &str) -> Option<RequestVoteResponse> {
        Self::send_request_vote(
            &self.id,
            self.current_term,
            self.last_log_index(),
            self.last_log_term(),
            peer,
        )
    }

    pub fn send_request_vote(
        id: &str,
        term: u64,
        last_log_index: u64,
        last_log_term: u64,
        peer: &str,
    ) -> Option<RequestVoteResponse> {
        use std::io::{Read, Write};
        use std::net::TcpStream;

        let mut stream = TcpStream::connect_timeout(
            &peer.parse().ok()?,
            Duration::from_millis(100),
        )
        .ok()?;
        stream
            .set_read_timeout(Some(Duration::from_millis(100)))
            .ok()?;

        let mut m = vec![0xA1u8];
        m.extend(term.to_le_bytes());
        let cid = id.as_bytes();
        m.extend((cid.len() as u32).to_le_bytes());
        m.extend(cid);
        m.extend(last_log_index.to_le_bytes());
        m.extend(last_log_term.to_le_bytes());
        stream.write_all(&m).ok()?;

        let mut buf = [0u8; 9];
        stream.read_exact(&mut buf).ok()?;
        let resp_term = u64::from_le_bytes(buf[0..8].try_into().unwrap());
        Some(RequestVoteResponse {
            term: resp_term,
            vote_granted: buf[8] != 0,
        })
    }

    pub fn send_append_entries_to_peer(
        &self,
        peer: &str,
        entries: &[LogEntry],
    ) -> Option<AppendEntriesResponse> {
        Self::send_append_entries(
            &self.id,
            self.current_term,
            self.commit_index,
            &self.log,
            peer,
            entries,
        )
    }

    /// Network send that does not need a Raft mutex held by the caller.
    pub fn send_append_entries(
        id: &str,
        term: u64,
        commit_index: u64,
        log: &[LogEntry],
        peer: &str,
        entries: &[LogEntry],
    ) -> Option<AppendEntriesResponse> {
        use std::io::{Read, Write};
        use std::net::TcpStream;

        let mut stream = TcpStream::connect_timeout(
            &peer.parse().ok()?,
            Duration::from_millis(100),
        )
        .ok()?;
        stream
            .set_read_timeout(Some(Duration::from_millis(100)))
            .ok()?;
        stream
            .set_write_timeout(Some(Duration::from_millis(100)))
            .ok()?;

        let last_idx = log.last().map(|e| e.index).unwrap_or(0);
        let prev_log_index = if entries.is_empty() {
            last_idx
        } else {
            entries[0].index.saturating_sub(1)
        };

        let prev_log_term = if prev_log_index == 0 {
            0u64
        } else if (prev_log_index as usize) <= log.len() {
            log[prev_log_index as usize - 1].term
        } else {
            0
        };

        let mut m = vec![0xA2u8];
        m.extend(term.to_le_bytes());
        let lid = id.as_bytes();
        m.extend((lid.len() as u32).to_le_bytes());
        m.extend(lid);
        m.extend(prev_log_index.to_le_bytes());
        m.extend(prev_log_term.to_le_bytes());
        m.extend(commit_index.to_le_bytes());
        m.extend((entries.len() as u32).to_le_bytes());
        for entry in entries {
            m.extend(entry.term.to_le_bytes());
            m.extend(entry.index.to_le_bytes());
            let kb = entry.key.as_bytes();
            m.extend((kb.len() as u32).to_le_bytes());
            m.extend(kb);
            let vb = entry.val.as_bytes();
            m.extend((vb.len() as u32).to_le_bytes());
            m.extend(vb);
        }
        stream.write_all(&m).ok()?;

        let mut buf = [0u8; 9];
        stream.read_exact(&mut buf).ok()?;
        let resp_term = u64::from_le_bytes(buf[0..8].try_into().unwrap());
        Some(AppendEntriesResponse {
            term: resp_term,
            success: buf[8] != 0,
        })
    }

    /// Replicate `entries` to all peers; return how many nodes (incl. self) stored them.
    pub fn replicate_to_majority(&self, entries: &[LogEntry]) -> usize {
        let mut acks = 1usize;
        for peer in &self.peers {
            if let Some(resp) = self.send_append_entries_to_peer(peer, entries) {
                if resp.success && resp.term == self.current_term {
                    acks += 1;
                }
            }
        }
        acks
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_peer_grows_cluster() {
        let mut n = RaftNode::new("a".into(), vec!["b".into()]);
        assert_eq!(n.cluster_size(), 2);
        let e = LogEntry::add_peer(1, 1, "c");
        // Follower applies immediately.
        assert!(n.apply_config_entry(&e).is_none());
        assert_eq!(n.cluster_size(), 3);
        assert_eq!(n.majority(), 2);
    }

    #[test]
    fn leader_defers_add_until_activate() {
        let mut n = RaftNode::new("a".into(), vec!["b".into()]);
        n.become_leader();
        let e = LogEntry::add_peer(1, 1, "c");
        assert_eq!(n.apply_config_entry(&e).as_deref(), Some("c"));
        assert_eq!(n.cluster_size(), 2); // not yet active
        n.activate_peer("c");
        assert_eq!(n.cluster_size(), 3);
    }

    #[test]
    fn remove_peer_shrinks() {
        let mut n = RaftNode::new("a".into(), vec!["b".into(), "c".into()]);
        let e = LogEntry::remove_peer(1, 1, "c");
        assert!(n.apply_config_entry(&e).is_none());
        assert_eq!(n.peers, vec!["b".to_string()]);
    }
}
