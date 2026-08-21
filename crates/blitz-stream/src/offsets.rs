//! Consumer offset store — external stream state (not on workers).

use blitz_meta::{MetaClient, PutResult};
use std::collections::HashMap;
use std::sync::Mutex;

pub trait OffsetStore: Send + Sync {
    fn get(&self, stream: &str, partition: u32) -> Option<u64>;
    fn put(&self, stream: &str, partition: u32, offset: u64) -> Result<(), String>;
}

fn key(stream: &str, partition: u32) -> String {
    format!("stream.{stream}.partition.{partition}.offset")
}

/// In-process offsets (unit tests / single-node demos).
pub struct MemoryOffsetStore {
    inner: Mutex<HashMap<String, u64>>,
}

impl MemoryOffsetStore {
    pub fn new() -> Self {
        MemoryOffsetStore {
            inner: Mutex::new(HashMap::new()),
        }
    }
}

impl Default for MemoryOffsetStore {
    fn default() -> Self {
        Self::new()
    }
}

impl OffsetStore for MemoryOffsetStore {
    fn get(&self, stream: &str, partition: u32) -> Option<u64> {
        self.inner.lock().unwrap().get(&key(stream, partition)).copied()
    }

    fn put(&self, stream: &str, partition: u32, offset: u64) -> Result<(), String> {
        self.inner
            .lock()
            .unwrap()
            .insert(key(stream, partition), offset);
        Ok(())
    }
}

/// Offsets durable in the Iceberg catalog / Raft KV (`blitz-meta`).
pub struct MetaOffsetStore {
    client: MetaClient,
}

impl MetaOffsetStore {
    pub fn new(leader: &str) -> Self {
        MetaOffsetStore {
            client: MetaClient::new(leader),
        }
    }
}

impl OffsetStore for MetaOffsetStore {
    fn get(&self, stream: &str, partition: u32) -> Option<u64> {
        self.client
            .get(&key(stream, partition))
            .and_then(|v| v.parse().ok())
    }

    fn put(&self, stream: &str, partition: u32, offset: u64) -> Result<(), String> {
        match self.client.put(&key(stream, partition), &offset.to_string()) {
            PutResult::Committed(_) => Ok(()),
            other => Err(format!("offset commit failed: {other:?}")),
        }
    }
}
