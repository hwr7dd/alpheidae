//! Epoch-based streaming for Alpheidae.
//!
//! Model (deliberately not Flink-style continuous operators):
//!   1. Measure backlog (lag) across partitions
//!   2. Size worker count N from that backlog for *this* epoch only
//!   3. Workers steal work units (partition slices) from a live queue
//!   4. Commit results + consumer offsets to external state (blitz-meta)
//!   5. Workers go idle / scale to zero until the next epoch
//!
//! Resizing does not stop/restart a long-lived job or migrate keyed operator
//! state — the next epoch simply resumes a different N.

mod offsets;
mod sizer;
mod source;

pub use offsets::{MemoryOffsetStore, MetaOffsetStore, OffsetStore};
pub use sizer::{size_workers, SizerConfig};
pub use source::{InMemorySource, Record, StreamSource, WorkUnit};

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::thread;
use std::time::{Duration, Instant};

/// Outcome of one streaming epoch.
#[derive(Debug, Clone)]
pub struct EpochReport {
    pub epoch_id: u64,
    pub lag_before: u64,
    pub workers: usize,
    pub units_total: usize,
    pub records_processed: u64,
    pub wall_ms: f64,
    pub offsets_committed: Vec<(u32, u64)>,
}

/// Process one epoch: size from lag, steal work units, commit offsets.
pub fn run_epoch<S, F>(
    epoch_id: u64,
    source: &S,
    offsets: &dyn OffsetStore,
    stream_name: &str,
    sizer: &SizerConfig,
    n_local: usize,
    process: F,
) -> Result<EpochReport, String>
where
    S: StreamSource + ?Sized,
    F: Fn(&WorkUnit) -> u64 + Sync,
{
    let t0 = Instant::now();
    let partitions = source.partition_count();
    let mut committed = Vec::with_capacity(partitions as usize);
    let mut lag_before = 0u64;

    for p in 0..partitions {
        let committed_off = offsets.get(stream_name, p).unwrap_or(0);
        let high = source.high_watermark(p);
        lag_before += high.saturating_sub(committed_off);
        committed.push((p, committed_off));
    }

    let workers = size_workers(lag_before, sizer).max(n_local);
    let units = source.plan_work(stream_name, offsets, sizer.max_records_per_unit);
    let units_total = units.len();

    if units_total == 0 {
        return Ok(EpochReport {
            epoch_id,
            lag_before,
            workers,
            units_total: 0,
            records_processed: 0,
            wall_ms: t0.elapsed().as_secs_f64() * 1e3,
            offsets_committed: committed,
        });
    }

    let queue = Mutex::new(units);
    let processed = AtomicUsize::new(0);
    let records = AtomicUsize::new(0);
    // High-water progress per partition within this epoch (exclusive end offset).
    let progress: Mutex<Vec<u64>> = Mutex::new(
        (0..partitions)
            .map(|p| offsets.get(stream_name, p).unwrap_or(0))
            .collect(),
    );

    thread::scope(|scope| {
        let process = &process;
        let queue = &queue;
        let records = &records;
        let processed = &processed;
        let progress = &progress;
        for w in 0..workers {
            let delay_ms = if w < n_local {
                0
            } else {
                2 + (w - n_local) as u64 * 3
            };
            scope.spawn(move || {
                if delay_ms > 0 {
                    thread::sleep(Duration::from_millis(delay_ms));
                }
                loop {
                    let unit = {
                        let mut q = queue.lock().unwrap();
                        q.pop()
                    };
                    let Some(unit) = unit else { break };
                    let n = process(&unit);
                    records.fetch_add(n as usize, Ordering::SeqCst);
                    processed.fetch_add(1, Ordering::SeqCst);
                    let mut prog = progress.lock().unwrap();
                    let p = unit.partition as usize;
                    if unit.end_offset > prog[p] {
                        prog[p] = unit.end_offset;
                    }
                }
            });
        }
    });

    // Commit per-partition offsets after the epoch succeeds.
    let prog = progress.lock().unwrap().clone();
    let mut offsets_committed = Vec::new();
    for (p, end) in prog.iter().enumerate() {
        let p = p as u32;
        let prev = offsets.get(stream_name, p).unwrap_or(0);
        if *end > prev {
            offsets
                .put(stream_name, p, *end)
                .map_err(|e| e.to_string())?;
        }
        offsets_committed.push((p, offsets.get(stream_name, p).unwrap_or(0)));
    }

    Ok(EpochReport {
        epoch_id,
        lag_before,
        workers,
        units_total,
        records_processed: records.load(Ordering::SeqCst) as u64,
        wall_ms: t0.elapsed().as_secs_f64() * 1e3,
        offsets_committed,
    })
}

/// Drive multiple epochs until lag is drained or `max_epochs` is hit.
pub fn run_until_caught_up<S, F>(
    source: &S,
    offsets: &dyn OffsetStore,
    stream_name: &str,
    sizer: &SizerConfig,
    n_local: usize,
    max_epochs: u64,
    process: F,
) -> Result<Vec<EpochReport>, String>
where
    S: StreamSource + ?Sized,
    F: Fn(&WorkUnit) -> u64 + Sync,
{
    let mut reports = Vec::new();
    for epoch_id in 1..=max_epochs {
        let rep = run_epoch(
            epoch_id,
            source,
            offsets,
            stream_name,
            sizer,
            n_local,
            &process,
        )?;
        let empty = rep.units_total == 0;
        reports.push(rep);
        if empty {
            break;
        }
    }
    Ok(reports)
}

#[cfg(test)]
mod tests {
    use super::*;
    use offsets::MemoryOffsetStore;

    #[test]
    fn epoch_drains_and_commits_offsets() {
        let src = InMemorySource::new(4);
        for i in 0..1000u64 {
            src.push(
                (i % 4) as u32,
                Record {
                    offset: 0, // assigned by push
                    key: i,
                    value: i * 10,
                },
            );
        }
        let offsets = MemoryOffsetStore::new();
        let sizer = SizerConfig {
            records_per_worker: 100,
            max_workers: 8,
            min_workers: 1,
            max_records_per_unit: 50,
        };
        let reports = run_until_caught_up(
            &src,
            &offsets,
            "demo",
            &sizer,
            1,
            20,
            |u| u.records.len() as u64,
        )
        .unwrap();
        assert!(reports.len() >= 1);
        let total: u64 = reports.iter().map(|r| r.records_processed).sum();
        assert_eq!(total, 1000);
        for p in 0..4u32 {
            assert_eq!(offsets.get("demo", p).unwrap(), src.high_watermark(p));
        }
    }
}
