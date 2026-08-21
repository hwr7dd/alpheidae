//! Epoch streaming demo.
//!
//! Shows the Alpheidae stream model (not a Flink-style continuous operator):
//!   1. Inject partitioned backlog into an in-memory log
//!   2. Each epoch: measure lag → size N → workers steal units → commit offsets
//!   3. Persist offsets in blitz-meta (Raft KV) so a "restart" resumes correctly
//!   4. Late workers join mid-epoch (same ramp idea as batch queries)

use blitz_format::{ColumnData, DataType, Writer};
use blitz_iceberg::{
    commit_append, ser_long, write_manifest, write_manifest_list, write_metadata, DataFile, Field,
    TableMeta,
};
use blitz_meta::{MetaClient, MetaNode};
use blitz_stream::{
    run_epoch, run_until_caught_up, InMemorySource, MemoryOffsetStore, MetaOffsetStore, Record,
    SizerConfig, WorkUnit,
};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

fn banner(s: &str) {
    println!("\n=== {s} {}", "=".repeat(74usize.saturating_sub(s.len())));
}

fn process_unit(u: &WorkUnit) -> u64 {
    // Tiny CPU stand-in for a real transform / sink write.
    let mut acc = 0u64;
    for r in &u.records {
        acc = acc.wrapping_add(r.value);
    }
    u.records.len() as u64 + (acc & 0)
}

fn print_report(tag: &str, r: &blitz_stream::EpochReport) {
    let offs: Vec<String> = r
        .offsets_committed
        .iter()
        .map(|(p, o)| format!("p{p}={o}"))
        .collect();
    println!(
        "  [{tag}] epoch={} lag={} N={} units={} records={} {:.1}ms offsets=[{}]",
        r.epoch_id,
        r.lag_before,
        r.workers,
        r.units_total,
        r.records_processed,
        r.wall_ms,
        offs.join(" ")
    );
}

fn i64_bounds(vals: &[i64]) -> (Vec<u8>, Vec<u8>) {
    let lo = vals.iter().min().copied().unwrap_or(0);
    let hi = vals.iter().max().copied().unwrap_or(0);
    (ser_long(lo), ser_long(hi))
}

fn append_epoch_sink(
    warehouse: &Path,
    meta: &mut TableMeta,
    version: &mut u32,
    epoch_id: u64,
    batches: &[(u32, Vec<i64>, Vec<i64>)],
) -> std::io::Result<()> {
    if batches.is_empty() {
        return Ok(());
    }
    let data_dir = warehouse.join("data");
    std::fs::create_dir_all(&data_dir)?;
    let mut files = Vec::new();
    let mut total_rows = 0i64;
    for (partition, keys, values) in batches {
        if keys.is_empty() {
            continue;
        }
        let path = data_dir.join(format!("epoch{epoch_id}_p{partition}.blitz"));
        let mut w = Writer::create(
            &path,
            vec![
                ("key".into(), DataType::Int64),
                ("value".into(), DataType::Int64),
            ],
        )?;
        w.write_rowgroup(&[
            ColumnData::Int64(keys.clone()),
            ColumnData::Int64(values.clone()),
        ])?;
        w.finish()?;
        let (k_lo, k_hi) = i64_bounds(keys);
        let (v_lo, v_hi) = i64_bounds(values);
        total_rows += keys.len() as i64;
        files.push(DataFile {
            path: path.clone(),
            file_size: std::fs::metadata(&path)?.len() as i64,
            record_count: keys.len() as i64,
            bounds: vec![(1, k_lo, k_hi), (2, v_lo, v_hi)],
        });
    }
    if files.is_empty() {
        return Ok(());
    }
    let snap = 1000 + epoch_id as i64;
    let mpath = warehouse
        .join("metadata")
        .join(format!("snap-{snap}-m0.avro"));
    let mlen = write_manifest(&mpath, snap, &files)?;
    let ml = warehouse
        .join("metadata")
        .join(format!("snap-{snap}-ml.avro"));
    write_manifest_list(&ml, snap, *version as i64 + 1, &[(mpath, mlen, total_rows)])?;
    *version += 1;
    commit_append(meta, snap, &ml, *version)?;
    Ok(())
}

fn main() {
    println!("Blitz epoch streaming — lag-sized workers, stealable units, external offsets\n");

    let sizer = SizerConfig {
        records_per_worker: 200,
        max_workers: 8,
        min_workers: 1,
        max_records_per_unit: 50,
    };

    // ------------------------------------------------------------------
    banner("A. MEMORY OFFSETS — backlog burst, N scales with lag");
    // ------------------------------------------------------------------
    let src = InMemorySource::new(4);
    for i in 0..800u64 {
        src.push(
            (i % 4) as u32,
            Record {
                offset: 0,
                key: i,
                value: i * 7,
            },
        );
    }
    let mem = MemoryOffsetStore::new();
    let reports = run_until_caught_up(&src, &mem, "orders", &sizer, 1, 20, process_unit)
        .expect("drain");
    for r in &reports {
        print_report("mem", r);
    }
    let drained: u64 = reports.iter().map(|r| r.records_processed).sum();
    println!("  drained {drained} records across {} epoch(s)", reports.len());

    // ------------------------------------------------------------------
    banner("B. CONTINUOUS INGEST — more records between epochs");
    // ------------------------------------------------------------------
    for i in 800..1200u64 {
        src.push(
            (i % 4) as u32,
            Record {
                offset: 0,
                key: i,
                value: i * 7,
            },
        );
    }
    let more = run_until_caught_up(&src, &mem, "orders", &sizer, 1, 10, process_unit)
        .expect("catch-up");
    for r in &more {
        print_report("ingest", r);
    }

    // ------------------------------------------------------------------
    banner("C. META OFFSETS — Raft-durable consumer position + restart");
    // ------------------------------------------------------------------
    let _m0 = MetaNode::start(
        "127.0.0.1:7701",
        vec!["127.0.0.1:7702".into()],
        false,
    );
    let _m1 = MetaNode::start(
        "127.0.0.1:7702",
        vec!["127.0.0.1:7701".into()],
        false,
    );
    thread::sleep(Duration::from_millis(200));
    let leader =
        MetaClient::wait_for_leader(&["127.0.0.1:7701", "127.0.0.1:7702"], 10000)
            .expect("meta leader");
    println!("  meta leader = {leader}");

    let src2 = InMemorySource::new(4);
    for i in 0..600u64 {
        src2.push(
            (i % 4) as u32,
            Record {
                offset: 0,
                key: i,
                value: i,
            },
        );
    }
    let durable = MetaOffsetStore::new(&leader);

    // First "session": drain current backlog, then "crash".
    let first = run_epoch(1, &src2, &durable, "payments", &sizer, 1, process_unit)
        .expect("epoch1");
    print_report("session-1", &first);
    println!("  simulated crash; offsets remain in Raft");

    // Ingest while down — new client must resume from committed offsets only.
    for i in 600..900u64 {
        src2.push(
            (i % 4) as u32,
            Record {
                offset: 0,
                key: i,
                value: i,
            },
        );
    }
    let durable2 = MetaOffsetStore::new(&leader);
    let rest = run_until_caught_up(&src2, &durable2, "payments", &sizer, 1, 20, process_unit)
        .expect("resume");
    for r in &rest {
        print_report("session-2", r);
    }
    let total_meta: u64 =
        first.records_processed + rest.iter().map(|r| r.records_processed).sum::<u64>();
    println!("  total across sessions = {total_meta} (expect 900)");
    assert_eq!(total_meta, 900);

    // ------------------------------------------------------------------
    banner("D. ICEBERG SINK — epoch appends + offset commit");
    // ------------------------------------------------------------------
    let warehouse = PathBuf::from(std::env::temp_dir()).join("blitz-stream-demo");
    let _ = std::fs::remove_dir_all(&warehouse);
    std::fs::create_dir_all(warehouse.join("metadata")).unwrap();
    let mut table = TableMeta {
        uuid: "stream-demo-uuid".into(),
        location: warehouse.clone(),
        fields: vec![
            Field {
                id: 1,
                name: "key".into(),
                dtype: DataType::Int64,
            },
            Field {
                id: 2,
                name: "value".into(),
                dtype: DataType::Int64,
            },
        ],
        current_snapshot: None,
        snapshots: vec![],
        last_sequence_number: 0,
    };
    write_metadata(&table, 0).unwrap();
    let mut version = 0u32;

    let src3 = InMemorySource::new(2);
    for i in 0..200u64 {
        src3.push(
            (i % 2) as u32,
            Record {
                offset: 0,
                key: i,
                value: i * 3,
            },
        );
    }
    let sink_offsets = MemoryOffsetStore::new();
    let written = Arc::new(AtomicU64::new(0));
    let warehouse_c = warehouse.clone();

    // Collect per-unit rows into Iceberg files inside the process callback,
    // then commit offsets only after the epoch returns (run_epoch already does that).
    let sizer_sink = SizerConfig {
        records_per_worker: 80,
        max_workers: 4,
        min_workers: 1,
        max_records_per_unit: 40,
    };
    let mut epoch = 0u64;
    loop {
        epoch += 1;
        let batch_keys: Arc<std::sync::Mutex<Vec<(u32, Vec<i64>, Vec<i64>)>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));
        let batch_c = batch_keys.clone();
        let written_epoch = written.clone();
        let rep = run_epoch(
            epoch,
            &src3,
            &sink_offsets,
            "sink",
            &sizer_sink,
            1,
            move |u| {
                let keys: Vec<i64> = u.records.iter().map(|r| r.key as i64).collect();
                let vals: Vec<i64> = u.records.iter().map(|r| r.value as i64).collect();
                batch_c
                    .lock()
                    .unwrap()
                    .push((u.partition, keys, vals));
                written_epoch.fetch_add(u.records.len() as u64, Ordering::SeqCst);
                u.records.len() as u64
            },
        )
        .expect("sink epoch");
        print_report("iceberg", &rep);
        if rep.units_total == 0 {
            break;
        }
        let batches = batch_keys.lock().unwrap().clone();
        append_epoch_sink(&warehouse_c, &mut table, &mut version, epoch, &batches)
            .expect("iceberg append");
        if epoch > 20 {
            break;
        }
    }
    println!(
        "  Iceberg snapshots={} records_written={} warehouse={}",
        table.snapshots.len(),
        written.load(Ordering::SeqCst),
        warehouse.display()
    );
    assert_eq!(written.load(Ordering::SeqCst), 200);

    println!("\nAll streaming checks passed.");
}
