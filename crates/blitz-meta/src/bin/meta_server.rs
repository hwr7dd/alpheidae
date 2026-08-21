//! Standalone replicated Iceberg catalog node for Kubernetes / EC2 deployment.
//!
//! Environment:
//!   BLITZ_META_ADDR     — bind address (default 0.0.0.0:7401)
//!   BLITZ_META_PEERS    — comma-separated peer addresses (excluding self)
//!   BLITZ_META_DATA_DIR — durable Raft/catalog directory (required in prod)
//!   POD_NAME            — optional; used to filter self from peer list in K8s

use blitz_meta::MetaNode;
use std::path::PathBuf;
use std::thread;
use std::time::Duration;

fn main() {
    let addr = std::env::var("BLITZ_META_ADDR").unwrap_or_else(|_| "0.0.0.0:7401".into());
    let mut peers: Vec<String> = std::env::var("BLITZ_META_PEERS")
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(String::from)
        .collect();

    if let Ok(pod) = std::env::var("POD_NAME") {
        let self_dns = format!("{pod}.blitz-meta-headless.blitz.svc.cluster.local:7401");
        peers.retain(|p| p != &self_dns);
    }

    let data_dir = std::env::var("BLITZ_META_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("/var/lib/blitz/meta"));

    eprintln!("[blitz-meta] starting on {addr} peers={peers:?} data_dir={data_dir:?}");
    let _node = MetaNode::start_persistent(&addr, peers, data_dir);

    loop {
        thread::sleep(Duration::from_secs(3600));
    }
}
