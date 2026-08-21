//! Stream sources and stealable work units.

use crate::offsets::OffsetStore;

#[derive(Clone, Debug)]
pub struct Record {
    pub offset: u64,
    pub key: u64,
    pub value: u64,
}

/// One stealable unit: a contiguous offset range on a single partition.
#[derive(Clone, Debug)]
pub struct WorkUnit {
    pub partition: u32,
    pub start_offset: u64,
    pub end_offset: u64, // exclusive
    pub records: Vec<Record>,
}

pub trait StreamSource: Send + Sync {
    fn partition_count(&self) -> u32;
    fn high_watermark(&self, partition: u32) -> u64;
    /// Build stealable units for uncommitted records `[committed, high)`.
    fn plan_work(
        &self,
        stream: &str,
        offsets: &dyn OffsetStore,
        max_per_unit: u64,
    ) -> Vec<WorkUnit>;
}

/// Kafka-like in-memory partitioned log for demos and tests.
pub struct InMemorySource {
    partitions: Vec<MutexLog>,
}

struct MutexLog {
    records: std::sync::Mutex<Vec<Record>>,
}

impl InMemorySource {
    pub fn new(partitions: u32) -> Self {
        InMemorySource {
            partitions: (0..partitions)
                .map(|_| MutexLog {
                    records: std::sync::Mutex::new(Vec::new()),
                })
                .collect(),
        }
    }

    pub fn push(&self, partition: u32, mut rec: Record) {
        let log = &self.partitions[partition as usize];
        let mut g = log.records.lock().unwrap();
        rec.offset = g.len() as u64;
        g.push(rec);
    }

    pub fn push_batch(&self, partition: u32, values: impl IntoIterator<Item = u64>) {
        for (i, v) in values.into_iter().enumerate() {
            self.push(
                partition,
                Record {
                    offset: 0,
                    key: v,
                    value: v.wrapping_mul(3) + i as u64,
                },
            );
        }
    }
}

impl StreamSource for InMemorySource {
    fn partition_count(&self) -> u32 {
        self.partitions.len() as u32
    }

    fn high_watermark(&self, partition: u32) -> u64 {
        self.partitions[partition as usize]
            .records
            .lock()
            .unwrap()
            .len() as u64
    }

    fn plan_work(
        &self,
        stream: &str,
        offsets: &dyn OffsetStore,
        max_per_unit: u64,
    ) -> Vec<WorkUnit> {
        let max_per_unit = max_per_unit.max(1);
        let mut units = Vec::new();
        for p in 0..self.partition_count() {
            let committed = offsets.get(stream, p).unwrap_or(0);
            let log = self.partitions[p as usize].records.lock().unwrap();
            let high = log.len() as u64;
            let mut start = committed;
            while start < high {
                let end = (start + max_per_unit).min(high);
                let records = log[start as usize..end as usize].to_vec();
                units.push(WorkUnit {
                    partition: p,
                    start_offset: start,
                    end_offset: end,
                    records,
                });
                start = end;
            }
        }
        // LIFO steal: reverse so earliest units tend to pop last — either order is fine.
        units
    }
}
