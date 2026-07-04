//! blitz-demo — end-to-end benchmark of the ramped cold-start architecture.
//!
//! What it shows, on real data, with real TCP workers:
//!   1. Liquid clustering turning zone maps from useless → surgical.
//!   2. A query whose first morsel executes ~0 ms after arrival on a single
//!      node, while 6 "microVM" workers (staggered by a realistic 5 ms
//!      Firecracker snapshot-resume latency) join mid-query and absorb the
//!      remaining morsels.
//!   3. The same query forced to (a) stay single-node and (b) wait for the
//!      full cluster before starting — to quantify what ramping buys.

use blitz_cluster::{ClusteredTable, LiquidStats};
use blitz_core::{Block, Column, BLOCK_ROWS};
use blitz_exec::{run_ramped, worker_main, Timeline};
use blitz_sql::parse;
use std::sync::Arc;
use std::time::{Duration, Instant};

// xorshift64* — no external RNG dependency.
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545F4914F6CDD1D)
    }
    fn range(&mut self, n: u64) -> i64 {
        (self.next() % n) as i64
    }
}

fn gen_table(rows: usize) -> Vec<Block> {
    let mut rng = Rng(0x9E3779B97F4A7C15);
    let mut blocks = vec![];
    let mut done = 0;
    while done < rows {
        let n = BLOCK_ROWS.min(rows - done);
        let mut c0 = Vec::with_capacity(n); // event_ts-ish key, random layout
        let mut c1 = Vec::with_capacity(n); // measure
        let mut c2 = Vec::with_capacity(n); // group key (16 groups)
        let mut c3 = Vec::with_capacity(n); // secondary key, correlated w/ c0
        for _ in 0..n {
            let k = rng.range(100_000_000);
            c0.push(k);
            c1.push(rng.range(10_000));
            c2.push(rng.range(16));
            c3.push(k / 1000 + rng.range(500));
        }
        blocks.push(Block {
            rows: n,
            columns: vec![
                Column::I64(c0),
                Column::I64(c1),
                Column::I64(c2),
                Column::I64(c3),
            ],
        });
        done += n;
    }
    blocks
}

const ROWS: usize = 8_000_000;
const ADDR_RAMP: &str = "127.0.0.1:7311";
const ADDR_STATIC: &str = "127.0.0.1:7312";
const RESUME_MS: u64 = 5; // measured Firecracker snapshot-resume latency class
const N_WORKERS: usize = 6;

fn spawn_simulated_microvm_workers(
    addr: &'static str,
    n: usize,
    resume_ms: u64,
    storage: Arc<ClusteredTable>, // shared storage (S3/local-cache stand-in)
) {
    for i in 0..n {
        let st = storage.clone();
        std::thread::spawn(move || {
            // Simulate the snapshot resume latency of worker i's microVM.
            // All resumes are fired in parallel at t=0 (see blitz-igniter),
            // with slight jitter between hosts.
            std::thread::sleep(Duration::from_millis(resume_ms + (i as u64 % 3)));
            loop {
                match worker_main(addr, Some(st.clone())) {
                    Ok(_) => break,
                    Err(_) => std::thread::sleep(Duration::from_micros(300)),
                }
            }
        });
    }
}

fn main() {
    println!("⚡ BlitzDB — ramped cold-start distributed SQL demo");
    println!("   {} rows × 4 cols, {}-row blocks\n", ROWS, BLOCK_ROWS);

    let t = Instant::now();
    let mut table = ClusteredTable::from_blocks(gen_table(ROWS));
    println!("data generated in {:.0} ms, {} blocks", t.elapsed().as_secs_f64() * 1e3, table.blocks.len());

    let sql = "SELECT SUM(c1) FROM t WHERE c0 > 95000000 GROUP BY c2";
    let q = parse(sql).expect("parse");
    println!("\nquery: {sql}");

    // ---------------- Liquid clustering ----------------
    println!("\n── liquid clustering ──────────────────────────────────────");
    let q_before = table.clustering_quality(0);
    let pruned_before = table.blocks.len() - table.pruned_morsels(q.filter).len();
    println!("before: zone overlap {:.3} (1.0 = useless), {} / {} blocks pruned",
        q_before, pruned_before, table.blocks.len());

    // Workload observation: queries keep filtering on c0 (and sometimes c3).
    let mut stats = LiquidStats::default();
    for _ in 0..40 { stats.record_predicate(0); }
    for _ in 0..12 { stats.record_predicate(3); }
    let t = Instant::now();
    table.recluster(&stats);
    let q_after = table.clustering_quality(0);
    let pruned_after = table.blocks.len() - table.pruned_morsels(q.filter).len();
    println!("recluster (z-order on hot keys {:?}) took {:.0} ms", table.cluster_keys, t.elapsed().as_secs_f64() * 1e3);
    println!("after:  zone overlap {:.3}, {} / {} blocks pruned",
        q_after, pruned_after, table.blocks.len());

    let table = Arc::new(table);

    // ---------------- Baseline 1: single node, no ramp ----------------
    println!("\n── baseline: single node only ─────────────────────────────");
    let tl = Arc::new(Timeline::new());
    let t = Instant::now();
    let rep = run_ramped(table.clone(), q, 2, "127.0.0.1:7313", 0, false, tl);
    let single_ms = t.elapsed().as_secs_f64() * 1e3;
    println!("single-node total: {:.2} ms ({} morsels)", single_ms, rep.morsels_executed);

    // ---------------- Baseline 2: wait-for-cluster (classic MPP) -------
    println!("\n── baseline: wait for full cluster, then start ────────────");
    let t = Instant::now();
    spawn_simulated_microvm_workers(ADDR_STATIC, N_WORKERS, RESUME_MS, table.clone());
    // Classic engines block until every worker registers:
    std::thread::sleep(Duration::from_millis(RESUME_MS + 3)); // resume + handshake
    let tl = Arc::new(Timeline::new());
    let rep = run_ramped(table.clone(), q, 2, ADDR_STATIC, N_WORKERS, false, tl);
    let staticc_ms = t.elapsed().as_secs_f64() * 1e3;
    println!("wait-then-run total: {:.2} ms (first work only after cluster up)", staticc_ms);
    drop(rep);

    // ---------------- Ramped execution ----------------
    println!("\n── BlitzDB ramped execution ───────────────────────────────");
    let tl = Arc::new(Timeline::new());
    tl.mark("query arrived (coordinator just resumed from snapshot)");
    spawn_simulated_microvm_workers(ADDR_RAMP, N_WORKERS, RESUME_MS, table.clone()); // resumes fired in parallel
    let t = Instant::now();
    let rep = run_ramped(table.clone(), q, 2, ADDR_RAMP, N_WORKERS, false, tl);
    let ramp_ms = t.elapsed().as_secs_f64() * 1e3;

    for (ms, ev) in &rep.timeline {
        println!("  [{:>8.3} ms] {ev}", ms);
    }
    println!("\nresult (SUM(c1) GROUP BY c2):");
    let mut groups: Vec<_> = rep.result.groups.iter().collect();
    groups.sort_by_key(|(k, _)| **k);
    for (k, a) in groups.iter().take(4) {
        println!("  c2={k}: sum={} count={}", a.sum, a.count);
    }
    println!("  … ({} groups total)", groups.len());

    // ---------------- Heavy query: ramp visibly absorbs work -----------
    println!("\n── heavy query (full scan — watch workers join mid-query) ─");
    let heavy_sql = "SELECT SUM(c1) FROM t GROUP BY c2";
    let hq = parse(heavy_sql).expect("parse");
    println!("query: {heavy_sql}");
    let tl = Arc::new(Timeline::new());
    let th = Instant::now();
    let rep1 = run_ramped(table.clone(), hq, 1, "127.0.0.1:7320", 0, false, tl);
    let heavy_single_ms = th.elapsed().as_secs_f64() * 1e3;

    let tl = Arc::new(Timeline::new());
    tl.mark("heavy query arrived");
    spawn_simulated_microvm_workers("127.0.0.1:7321", N_WORKERS, RESUME_MS, table.clone());
    let th = Instant::now();
    let rep2 = run_ramped(table.clone(), hq, 1, "127.0.0.1:7321", N_WORKERS, false, tl);
    let heavy_ramp_ms = th.elapsed().as_secs_f64() * 1e3;
    for (ms, ev) in &rep2.timeline {
        println!("  [{:>8.3} ms] {ev}", ms);
    }
    println!("  heavy single-node: {:.2} ms | heavy ramped: {:.2} ms ({} morsels)",
        heavy_single_ms, heavy_ramp_ms, rep1.morsels_executed);
    let cores = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1);
    if cores <= 2 {
        println!("  NOTE: this host has {cores} core(s) — simulated workers share the");
        println!("  coordinator's CPU, so the ramp shows join-timeline mechanics here,");
        println!("  not throughput. On real hardware each worker is its own microVM");
        println!("  with its own cores; ramped time ≈ single-node start latency +");
        println!("  work/(growing cluster width).");
    }

    println!("\n── summary ────────────────────────────────────────────────");
    println!("  zone-map pruning:        {} → {} blocks pruned after liquid recluster", pruned_before, pruned_after);
    println!("  single node:             {:>8.2} ms", single_ms);
    println!("  wait-for-cluster MPP:    {:>8.2} ms (idle until workers up)", staticc_ms);
    println!("  ramped (BlitzDB):        {:>8.2} ms — first morsel at ~0 ms, cluster forms mid-query", ramp_ms);
}
