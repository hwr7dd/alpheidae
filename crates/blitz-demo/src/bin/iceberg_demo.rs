//! End-to-end Iceberg demo:
//!   1. Replicated metadata service (the Iceberg catalog) with leader
//!      failover + epoch fencing.
//!   2. Generate an Iceberg v2 warehouse: BlitzCol data files, Avro
//!      manifests + manifest lists, JSON table metadata — committed through
//!      the replicated catalog.
//!   3. Q1: broadcast hash join + two-phase agg, with manifest-bounds file
//!      pruning and page-level late materialization metrics.
//!   4. Q2: shuffle hash join (build side too big to broadcast).

use blitz_engine::{execute, render, EngineOpts};
use blitz_format::{ColumnData, DataType, Writer};
use blitz_iceberg::{
    commit_append, plan_files, read_metadata, ser_long, write_manifest, write_manifest_list,
    write_metadata, DataFile, Field, TableMeta,
};
use blitz_meta::{MetaClient, MetaNode, PutResult};
use blitz_plan::{explain, parse, plan, Catalog};
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;

fn banner(s: &str) {
    println!("\n=== {s} {}", "=".repeat(74usize.saturating_sub(s.len())));
}

// ---------------------------------------------------------------------------
// Warehouse generation
// ---------------------------------------------------------------------------

struct Gen {
    state: u64,
}
impl Gen {
    fn new(seed: u64) -> Self {
        Gen { state: seed }
    }
    fn next(&mut self) -> u64 {
        // xorshift64*
        let mut x = self.state;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.state = x;
        x.wrapping_mul(0x2545F4914F6CDD1D)
    }
    fn range(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

fn i64_bounds(vals: &[i64]) -> (Vec<u8>, Vec<u8>) {
    let lo = vals.iter().min().copied().unwrap_or(0);
    let hi = vals.iter().max().copied().unwrap_or(0);
    (ser_long(lo), ser_long(hi))
}

/// Write one .blitz data file and return its Iceberg DataFile entry
/// (with per-field lower/upper bounds for manifest pruning).
fn write_data_file(
    path: &Path,
    schema: Vec<(String, DataType)>,
    field_ids: &[i32],
    cols: Vec<ColumnData>,
    rowgroup_rows: usize,
) -> DataFile {
    let nrows = cols[0].len();
    let mut w = Writer::create(path, schema).unwrap();
    let mut start = 0;
    while start < nrows {
        let end = (start + rowgroup_rows).min(nrows);
        let rg: Vec<ColumnData> = cols
            .iter()
            .map(|c| match c {
                ColumnData::Int64(v) => ColumnData::Int64(v[start..end].to_vec()),
                ColumnData::Utf8(v) => ColumnData::Utf8(v[start..end].to_vec()),
            })
            .collect();
        w.write_rowgroup(&rg).unwrap();
        start = end;
    }
    let meta = w.finish().unwrap();
    let mut bounds = vec![];
    for (fi, c) in cols.iter().enumerate() {
        if let ColumnData::Int64(v) = c {
            let (lo, hi) = i64_bounds(v);
            bounds.push((field_ids[fi], lo, hi));
        }
    }
    DataFile {
        path: path.to_path_buf(),
        file_size: meta.file_size as i64,
        record_count: nrows as i64,
        bounds,
    }
}

/// Create the table directory layout, write data files + manifests +
/// metadata.json, and return (metadata path, raw uncompressed bytes).
fn create_table(
    wh: &Path,
    name: &str,
    fields: Vec<Field>,
    files: Vec<DataFile>,
) -> (PathBuf, TableMeta) {
    let loc = wh.join(name);
    std::fs::create_dir_all(loc.join("metadata")).unwrap();
    let mut meta = TableMeta {
        uuid: format!("blitz-{name}"),
        location: loc.clone(),
        fields,
        current_snapshot: None,
        snapshots: vec![],
        last_sequence_number: 0,
    };
    write_metadata(&meta, 1).unwrap(); // v1: empty table
    let snap_id = 1001;
    let mpath = loc.join("metadata").join("manifest-1.avro");
    let mlen = write_manifest(&mpath, snap_id, &files).unwrap();
    let nrows: i64 = files.iter().map(|f| f.record_count).sum();
    let mlpath = loc.join("metadata").join("snap-1001-manifest-list.avro");
    write_manifest_list(&mlpath, snap_id, 1, &[(mpath, mlen, nrows)]).unwrap();
    let mpath = commit_append(&mut meta, snap_id, &mlpath, 2).unwrap(); // v2: data appended
    (mpath, meta)
}

fn main() {
    let wh = PathBuf::from("/tmp/blitz_warehouse");
    let _ = std::fs::remove_dir_all(&wh);
    std::fs::create_dir_all(&wh).unwrap();

    // =======================================================================
    // 1. Replicated catalog: 3-node metadata service
    // =======================================================================
    banner("1. REPLICATED METADATA (Iceberg catalog, majority commit + fencing)");
    let n1 = MetaNode::start("127.0.0.1:7401", vec!["127.0.0.1:7402".into(), "127.0.0.1:7403".into()], true);
    let _n2 = MetaNode::start("127.0.0.1:7402", vec!["127.0.0.1:7401".into(), "127.0.0.1:7403".into()], false);
    let _n3 = MetaNode::start("127.0.0.1:7403", vec!["127.0.0.1:7401".into(), "127.0.0.1:7402".into()], false);
    std::thread::sleep(std::time::Duration::from_millis(50));
    let client = MetaClient::new("127.0.0.1:7401");
    println!("3 meta nodes up; leader = 127.0.0.1:7401 (epoch 1)");
    match client.put("cluster.name", "blitz-prod") {
        PutResult::Committed(idx) => println!("PUT cluster.name -> committed at log[{idx}] (replicated to majority)"),
        other => println!("PUT failed: {other:?}"),
    }

    // =======================================================================
    // 2. Build the Iceberg warehouse
    // =======================================================================
    banner("2. ICEBERG WAREHOUSE (BlitzCol data files + Avro manifests)");
    let t = std::time::Instant::now();
    let mut g = Gen::new(0x5EED);

    // ---- sales fact: 4M rows, 8 files clustered by ts ----
    const SALES_ROWS: usize = 4_000_000;
    const SALES_FILES: usize = 8;
    let per = SALES_ROWS / SALES_FILES;
    let sales_schema = vec![
        ("id".to_string(), DataType::Int64),
        ("gid".to_string(), DataType::Int64),
        ("amount".to_string(), DataType::Int64),
        ("ts".to_string(), DataType::Int64),
    ];
    let mut sales_files = vec![];
    let mut sales_bytes = 0u64;
    for f in 0..SALES_FILES {
        let base = f * per;
        let mut id = Vec::with_capacity(per);
        let mut gid = Vec::with_capacity(per);
        let mut amount = Vec::with_capacity(per);
        let mut ts = Vec::with_capacity(per);
        for i in 0..per {
            id.push((base + i) as i64);
            gid.push(g.range(200) as i64);
            amount.push((g.range(1000) + 1) as i64);
            // clustered: each file owns a disjoint time slice
            ts.push((base + i) as i64 * 10 + g.range(10) as i64);
        }
        let p = wh.join(format!("sales-{f:02}.blitz"));
        let df = write_data_file(
            &p,
            sales_schema.clone(),
            &[1, 2, 3, 4],
            vec![
                ColumnData::Int64(id),
                ColumnData::Int64(gid),
                ColumnData::Int64(amount),
                ColumnData::Int64(ts),
            ],
            65536,
        );
        sales_bytes += df.file_size as u64;
        sales_files.push(df);
    }
    let sales_fields = vec![
        Field { id: 1, name: "id".into(), dtype: DataType::Int64 },
        Field { id: 2, name: "gid".into(), dtype: DataType::Int64 },
        Field { id: 3, name: "amount".into(), dtype: DataType::Int64 },
        Field { id: 4, name: "ts".into(), dtype: DataType::Int64 },
    ];
    let (sales_meta_path, _) = create_table(&wh, "sales", sales_fields, sales_files);
    let sales_raw = (SALES_ROWS * 4 * 8) as u64;
    println!(
        "sales:   {SALES_ROWS} rows x 4 cols in {SALES_FILES} files (clustered by ts) | raw {:.1} MB -> {:.1} MB on disk ({:.1}x compression)",
        sales_raw as f64 / 1e6, sales_bytes as f64 / 1e6, sales_raw as f64 / sales_bytes as f64
    );

    // ---- regions dim: 200 rows with a Utf8 column ----
    let region_names = ["north", "south", "east", "west", "central", "apac", "emea", "latam"];
    let rid: Vec<i64> = (0..200).collect();
    let rname: Vec<String> = (0..200)
        .map(|i| format!("{}-{:02}", region_names[i % region_names.len()], i / region_names.len()))
        .collect();
    let p = wh.join("regions-00.blitz");
    let df = write_data_file(
        &p,
        vec![("id".into(), DataType::Int64), ("region".into(), DataType::Utf8)],
        &[1, 2],
        vec![ColumnData::Int64(rid), ColumnData::Utf8(rname)],
        65536,
    );
    let regions_fields = vec![
        Field { id: 1, name: "id".into(), dtype: DataType::Int64 },
        Field { id: 2, name: "region".into(), dtype: DataType::Utf8 },
    ];
    let (regions_meta_path, _) = create_table(&wh, "regions", regions_fields, vec![df]);
    println!("regions: 200 rows x 2 cols (id, region utf8-dict) in 1 file");

    // ---- returns: 1M rows, big enough that broadcast is rejected ----
    const RET_ROWS: usize = 1_000_000;
    let mut sale_id = Vec::with_capacity(RET_ROWS);
    let mut qty = Vec::with_capacity(RET_ROWS);
    for _ in 0..RET_ROWS {
        sale_id.push(g.range(SALES_ROWS as u64) as i64);
        qty.push((g.range(5) + 1) as i64);
    }
    let mut ret_files = vec![];
    for f in 0..2 {
        let lo = f * RET_ROWS / 2;
        let hi = (f + 1) * RET_ROWS / 2;
        let p = wh.join(format!("returns-{f:02}.blitz"));
        ret_files.push(write_data_file(
            &p,
            vec![("sale_id".into(), DataType::Int64), ("qty".into(), DataType::Int64)],
            &[1, 2],
            vec![
                ColumnData::Int64(sale_id[lo..hi].to_vec()),
                ColumnData::Int64(qty[lo..hi].to_vec()),
            ],
            65536,
        ));
    }
    let ret_fields = vec![
        Field { id: 1, name: "sale_id".into(), dtype: DataType::Int64 },
        Field { id: 2, name: "qty".into(), dtype: DataType::Int64 },
    ];
    let (returns_meta_path, _) = create_table(&wh, "returns", ret_fields, ret_files);
    println!("returns: {RET_ROWS} rows x 2 cols in 2 files");
    println!("warehouse built in {:.2}s (Avro manifests + manifest lists + metadata.json per table)", t.elapsed().as_secs_f64());

    // Commit table pointers through the replicated catalog
    for (name, p) in [
        ("sales", &sales_meta_path),
        ("regions", &regions_meta_path),
        ("returns", &returns_meta_path),
    ] {
        match client.commit_table(name, &p.to_string_lossy()) {
            PutResult::Committed(idx) => println!("catalog commit: iceberg.table.{name} -> {} (log[{idx}])", p.display()),
            other => println!("catalog commit FAILED for {name}: {other:?}"),
        }
    }

    // =======================================================================
    // 3. Leader failover + fencing
    // =======================================================================
    banner("3. CATALOG FAILOVER (leader dies mid-flight, epoch fencing)");
    println!("killing leader 127.0.0.1:7401 ...");
    n1.kill();
    println!("promoting 127.0.0.1:7402 to leader with epoch 2 (deterministic promotion; \n  a Raft election would pick the same node — see ARCHITECTURE.md)");
    let ok = MetaClient::promote("127.0.0.1:7402", 2);
    println!("promotion {}", if ok { "accepted" } else { "rejected" });
    let client = MetaClient::new("127.0.0.1:7402");
    match client.put("cluster.note", "served-by-epoch-2") {
        PutResult::Committed(idx) => println!("PUT through new leader -> committed at log[{idx}]"),
        other => println!("PUT through new leader failed: {other:?}"),
    }
    let stale = MetaClient::new("127.0.0.1:7401");
    println!("write through dead old leader -> {:?}", stale.put("x", "y"));
    match client.load_table("sales") {
        Some(p) => println!("catalog read after failover: iceberg.table.sales -> {p}"),
        None => println!("catalog read after failover FAILED"),
    }

    // Catalog used by the planner: replicated KV -> metadata.json -> manifests
    let load = |name: &str| -> Option<(TableMeta, Vec<DataFile>)> {
        let p = client.load_table(name)?;
        let meta = read_metadata(Path::new(&p)).ok()?;
        let files = plan_files(&meta).ok()?;
        Some((meta, files))
    };
    let catalog = Catalog { load: &load };

    // =======================================================================
    // 4. Q1 — broadcast join, file pruning, late materialization
    // =======================================================================
    banner("4. Q1: BROADCAST JOIN + TWO-PHASE AGG");
    let sql1 = "SELECT r.region, SUM(s.amount), COUNT(*) FROM sales s \
                JOIN regions r ON s.gid = r.id \
                WHERE s.ts > 33000000 \
                GROUP BY r.region ORDER BY 2 DESC LIMIT 5";
    println!("{sql1}\n");
    let ast = parse(sql1).unwrap();
    let p1 = plan(&ast, &catalog).unwrap();
    println!("{}", explain(&p1));

    blitz_format::reset_counters();
    let opts = EngineOpts { local_threads: 1, worker_join_ms: vec![5, 6, 8] };
    let r1 = execute(&p1, &opts);
    println!("ramp timeline (1 local thread + 3 microVM workers resuming at 5/6/8 ms):");
    for l in &r1.timeline {
        println!("{l}");
    }
    let dec = blitz_format::BYTES_DECODED.load(Ordering::Relaxed);
    let skp = blitz_format::BYTES_SKIPPED.load(Ordering::Relaxed);
    let pd = blitz_format::PAGES_DECODED.load(Ordering::Relaxed);
    let ps = blitz_format::PAGES_SKIPPED.load(Ordering::Relaxed);
    println!("\nlate materialization:");
    println!("  pages decoded {pd}, pages skipped {ps} ({:.1}% skipped)", 100.0 * ps as f64 / (pd + ps).max(1) as f64);
    println!("  bytes decoded {:.2} MB, bytes skipped {:.2} MB (of {:.2} MB sales on disk)",
        dec as f64 / 1e6, skp as f64 / 1e6, sales_bytes as f64 / 1e6);
    println!("  rows scanned {} | rows joined {} | wall {:.1} ms", r1.scanned_rows, r1.joined_rows, r1.elapsed_ms);
    println!("\n{}", render(&r1));

    // =======================================================================
    // 5. Q2 — shuffle join (build side too big to broadcast)
    // =======================================================================
    banner("5. Q2: SHUFFLE JOIN (build side over 4 MB broadcast threshold)");
    let sql2 = "SELECT s.gid, SUM(r.qty), COUNT(*) FROM sales s \
                JOIN returns r ON s.id = r.sale_id \
                WHERE s.amount > 900 \
                GROUP BY s.gid ORDER BY 2 DESC LIMIT 5";
    println!("{sql2}\n");
    let ast = parse(sql2).unwrap();
    let p2 = plan(&ast, &catalog).unwrap();
    println!("{}", explain(&p2));

    blitz_format::reset_counters();
    let r2 = execute(&p2, &opts);
    println!("ramp timeline (shuffle map phase ramps; join phase runs on full cluster):");
    for l in &r2.timeline {
        println!("{l}");
    }
    let pd = blitz_format::PAGES_DECODED.load(Ordering::Relaxed);
    let ps = blitz_format::PAGES_SKIPPED.load(Ordering::Relaxed);
    println!("\n  pages decoded {pd}, pages skipped {ps} | rows scanned {} | rows joined {} | wall {:.1} ms",
        r2.scanned_rows, r2.joined_rows, r2.elapsed_ms);
    println!("\n{}", render(&r2));

    println!("note: this host has 1 CPU core, so extra workers add scheduling overhead\nrather than speedup — the timelines demonstrate mid-query ramp mechanics,\nnot multi-core scaling.");
}
