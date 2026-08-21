//! On-disk persistence for Raft + catalog state.
//!
//! Layout under `data_dir/`:
//!   raft.bin   — term, voted_for, peers, log, commit/applied indices
//!   catalog.bin — epoch + KV map
//!
//! Writes are atomic (temp file + rename). Loaded on node start so a restart
//! retains the Iceberg catalog pointer and Raft identity.

use crate::raft::{LogEntry as RaftLogEntry, RaftNode};
use crate::{LogEntry, NodeState};
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

const MAGIC: &[u8; 4] = b"BLZM";
const VERSION: u32 = 1;

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

fn atomic_write(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let tmp = path.with_extension("tmp");
    {
        let mut f = File::create(&tmp)?;
        f.write_all(bytes)?;
        f.sync_all()?;
    }
    fs::rename(&tmp, path)?;
    Ok(())
}

pub fn raft_path(dir: &Path) -> PathBuf {
    dir.join("raft.bin")
}
pub fn catalog_path(dir: &Path) -> PathBuf {
    dir.join("catalog.bin")
}

pub(crate) fn save_raft(dir: &Path, raft: &RaftNode) -> std::io::Result<()> {
    let mut buf = Vec::new();
    buf.extend_from_slice(MAGIC);
    buf.extend_from_slice(&VERSION.to_le_bytes());
    w_u64(&mut buf, raft.current_term)?;
    match &raft.voted_for {
        Some(v) => {
            buf.push(1);
            w_str(&mut buf, v)?;
        }
        None => buf.push(0),
    }
    w_u64(&mut buf, raft.commit_index)?;
    w_u64(&mut buf, raft.last_applied)?;
    w_u64(&mut buf, raft.peers.len() as u64)?;
    for p in &raft.peers {
        w_str(&mut buf, p)?;
    }
    w_u64(&mut buf, raft.log.len() as u64)?;
    for e in &raft.log {
        w_u64(&mut buf, e.term)?;
        w_u64(&mut buf, e.index)?;
        w_str(&mut buf, &e.key)?;
        w_str(&mut buf, &e.val)?;
    }
    atomic_write(&raft_path(dir), &buf)
}

pub(crate) fn load_raft(dir: &Path, id: &str) -> std::io::Result<Option<RaftNode>> {
    let path = raft_path(dir);
    if !path.exists() {
        return Ok(None);
    }
    let mut f = File::open(&path)?;
    let mut magic = [0u8; 4];
    f.read_exact(&mut magic)?;
    if &magic != MAGIC {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "bad raft magic",
        ));
    }
    let mut ver = [0u8; 4];
    f.read_exact(&mut ver)?;
    let current_term = r_u64(&mut f)?;
    let mut has = [0u8; 1];
    f.read_exact(&mut has)?;
    let voted_for = if has[0] == 1 {
        Some(r_str(&mut f)?)
    } else {
        None
    };
    let commit_index = r_u64(&mut f)?;
    let last_applied = r_u64(&mut f)?;
    let n_peers = r_u64(&mut f)? as usize;
    let mut peers = Vec::with_capacity(n_peers);
    for _ in 0..n_peers {
        peers.push(r_str(&mut f)?);
    }
    let n_log = r_u64(&mut f)? as usize;
    let mut log = Vec::with_capacity(n_log);
    for _ in 0..n_log {
        let term = r_u64(&mut f)?;
        let index = r_u64(&mut f)?;
        let key = r_str(&mut f)?;
        let val = r_str(&mut f)?;
        log.push(RaftLogEntry {
            term,
            index,
            key,
            val,
        });
    }
    let mut node = RaftNode::new(id.to_string(), peers);
    node.current_term = current_term;
    node.voted_for = voted_for;
    node.commit_index = commit_index;
    node.last_applied = last_applied;
    node.log = log;
    Ok(Some(node))
}

pub(crate) fn save_catalog(dir: &Path, st: &NodeState) -> std::io::Result<()> {
    let mut buf = Vec::new();
    buf.extend_from_slice(MAGIC);
    buf.extend_from_slice(&VERSION.to_le_bytes());
    w_u64(&mut buf, st.epoch)?;
    w_u64(&mut buf, st.map.len() as u64)?;
    for (k, v) in &st.map {
        w_str(&mut buf, k)?;
        w_str(&mut buf, v)?;
    }
    w_u64(&mut buf, st.log.len() as u64)?;
    for e in &st.log {
        w_u64(&mut buf, e.epoch)?;
        w_u64(&mut buf, e.index)?;
        w_str(&mut buf, &e.key)?;
        w_str(&mut buf, &e.val)?;
    }
    atomic_write(&catalog_path(dir), &buf)
}

pub(crate) fn load_catalog(dir: &Path) -> std::io::Result<Option<(u64, HashMap<String, String>, Vec<LogEntry>)>> {
    let path = catalog_path(dir);
    if !path.exists() {
        return Ok(None);
    }
    let mut f = File::open(&path)?;
    let mut magic = [0u8; 4];
    f.read_exact(&mut magic)?;
    if &magic != MAGIC {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "bad catalog magic",
        ));
    }
    let mut ver = [0u8; 4];
    f.read_exact(&mut ver)?;
    let epoch = r_u64(&mut f)?;
    let n_map = r_u64(&mut f)? as usize;
    let mut map = HashMap::with_capacity(n_map);
    for _ in 0..n_map {
        let k = r_str(&mut f)?;
        let v = r_str(&mut f)?;
        map.insert(k, v);
    }
    let n_log = r_u64(&mut f)? as usize;
    let mut log = Vec::with_capacity(n_log);
    for _ in 0..n_log {
        let epoch = r_u64(&mut f)?;
        let index = r_u64(&mut f)?;
        let key = r_str(&mut f)?;
        let val = r_str(&mut f)?;
        log.push(LogEntry {
            epoch,
            index,
            key,
            val,
        });
    }
    Ok(Some((epoch, map, log)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::RaftNode;
    use std::env;

    #[test]
    fn roundtrip_raft_and_catalog() {
        let dir = env::temp_dir().join(format!("blitz-meta-persist-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        let mut raft = RaftNode::new("a".into(), vec!["b".into()]);
        raft.current_term = 7;
        raft.voted_for = Some("a".into());
        raft.append_entry("k".into(), "v".into());
        raft.commit_index = 1;
        raft.last_applied = 1;
        save_raft(&dir, &raft).unwrap();

        let loaded = load_raft(&dir, "a").unwrap().unwrap();
        assert_eq!(loaded.current_term, 7);
        assert_eq!(loaded.log.len(), 1);
        assert_eq!(loaded.peers, vec!["b".to_string()]);

        let st = NodeState {
            epoch: 7,
            is_leader: false,
            map: HashMap::from([("iceberg.table.t".into(), "s3://w/t".into())]),
            log: vec![],
            alive: true,
            awaiting_snapshot: false,
        };
        save_catalog(&dir, &st).unwrap();
        let (epoch, map, _) = load_catalog(&dir).unwrap().unwrap();
        assert_eq!(epoch, 7);
        assert_eq!(map.get("iceberg.table.t").map(String::as_str), Some("s3://w/t"));
        let _ = fs::remove_dir_all(&dir);
    }
}
