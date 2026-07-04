//! blitz-engine: executes a `blitz_plan::PhysicalPlan` against Iceberg tables
//! stored as BlitzCol (.blitz) files.
//!
//! Pipeline per query:
//!   IcebergScan (late materialization: zone-map page pruning -> selection
//!   vectors -> gather only needed pages of only needed columns)
//!   -> HashJoin (BROADCAST: build table replicated to every task;
//!                SHUFFLE: both sides hash-partitioned on the join key into
//!                exchange buffers, then one build+probe task per partition)
//!   -> PartialAggregate (per task)  -> FinalAggregate (merge)
//!   -> TopN (ORDER BY + LIMIT)
//!
//! Tasks are morsels: one (data file, rowgroup) pair. The ramped scheduler
//! starts `local_threads` immediately and lets extra "workers" join mid-query
//! after a simulated microVM-resume delay, stealing tasks from the same
//! atomic queue. Over the wire this is the same protocol blitz-exec speaks
//! (task IDs only — shared storage means data never moves to move work);
//! in this demo the workers are in-process threads so it runs on one host.

use blitz_format::{ColumnData, Reader};
use blitz_plan::{AggFn, JoinNode, JoinStrategy, OutExpr, PhysicalPlan, ScanNode};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::Instant;

// ---------------------------------------------------------------------------
// Values
// ---------------------------------------------------------------------------

/// Hashable cell used for join keys and group-by keys.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Cell {
    I(i64),
    S(String),
}

/// Final output value (AVG produces a float).
#[derive(Clone, Debug)]
pub enum Val {
    I(i64),
    S(String),
    F(f64),
}

impl std::fmt::Display for Val {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Val::I(v) => write!(f, "{v}"),
            Val::S(v) => write!(f, "{v}"),
            Val::F(v) => write!(f, "{v:.2}"),
        }
    }
}

fn val_cmp(a: &Val, b: &Val) -> std::cmp::Ordering {
    use std::cmp::Ordering::*;
    match (a, b) {
        (Val::I(x), Val::I(y)) => x.cmp(y),
        (Val::S(x), Val::S(y)) => x.cmp(y),
        (Val::F(x), Val::F(y)) => x.partial_cmp(y).unwrap_or(Equal),
        (Val::I(x), Val::F(y)) => (*x as f64).partial_cmp(y).unwrap_or(Equal),
        (Val::F(x), Val::I(y)) => x.partial_cmp(&(*y as f64)).unwrap_or(Equal),
        _ => Equal,
    }
}

// ---------------------------------------------------------------------------
// Aggregation state
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct Acc {
    pub sum: i64,
    pub count: i64,
    pub min: i64,
    pub max: i64,
}

impl Acc {
    fn new() -> Self {
        Acc { sum: 0, count: 0, min: i64::MAX, max: i64::MIN }
    }
    #[inline]
    fn add(&mut self, v: i64) {
        self.sum += v;
        self.count += 1;
        if v < self.min { self.min = v; }
        if v > self.max { self.max = v; }
    }
    #[inline]
    fn add_count_only(&mut self) {
        self.count += 1;
    }
    fn merge(&mut self, o: &Acc) {
        self.sum += o.sum;
        self.count += o.count;
        if o.min < self.min { self.min = o.min; }
        if o.max > self.max { self.max = o.max; }
    }
    fn result(&self, f: AggFn) -> Val {
        match f {
            AggFn::Sum => Val::I(self.sum),
            AggFn::Count => Val::I(self.count),
            AggFn::Min => Val::I(self.min),
            AggFn::Max => Val::I(self.max),
            AggFn::Avg => Val::F(if self.count == 0 { 0.0 } else { self.sum as f64 / self.count as f64 }),
        }
    }
}

/// Per-task partial state: either grouped accumulators or raw rows.
pub struct Partial {
    pub groups: HashMap<Vec<Cell>, Vec<Acc>>,
    pub rows: Vec<Vec<Val>>,
    pub joined_rows: u64,
    pub scanned_rows: u64,
}

impl Partial {
    fn new() -> Self {
        Partial { groups: HashMap::new(), rows: vec![], joined_rows: 0, scanned_rows: 0 }
    }
}

// ---------------------------------------------------------------------------
// Scanning with late materialization
// ---------------------------------------------------------------------------

/// Columns of one rowgroup after predicate evaluation + gather:
/// only the rows that passed, only the columns the plan needs.
struct ScanBatch {
    /// Parallel to `scan.cols`.
    cols: Vec<ColumnData>,
    rows: usize,
}

fn intersect_sorted(a: &[u32], b: &[u32]) -> Vec<u32> {
    let mut out = Vec::with_capacity(a.len().min(b.len()));
    let (mut i, mut j) = (0, 0);
    while i < a.len() && j < b.len() {
        match a[i].cmp(&b[j]) {
            std::cmp::Ordering::Less => i += 1,
            std::cmp::Ordering::Greater => j += 1,
            std::cmp::Ordering::Equal => {
                out.push(a[i]);
                i += 1;
                j += 1;
            }
        }
    }
    out
}

/// Evaluate all predicates on one rowgroup (zone-map pruning happens inside
/// `eval_pred`), then gather only the projected columns at surviving rows.
fn scan_rowgroup(reader: &mut Reader, scan: &ScanNode, rg: usize) -> Option<ScanBatch> {
    let total_rows = reader.meta.rowgroups[rg].rows;
    // 1. predicate phase — touches only predicate columns, page-pruned
    let mut sel: Option<Vec<u32>> = None;
    for p in &scan.preds {
        let s = reader.eval_pred(rg, p.col_idx, p.op, &p.lit);
        sel = Some(match sel {
            None => s,
            Some(prev) => intersect_sorted(&prev, &s),
        });
        if sel.as_ref().unwrap().is_empty() {
            return None;
        }
    }
    let sel = sel.unwrap_or_else(|| (0..total_rows as u32).collect());
    if sel.is_empty() {
        return None;
    }
    // 2. materialization phase — decode only pages containing selected rows,
    //    only for columns the plan actually outputs/joins/groups on
    let cols = scan
        .cols
        .iter()
        .map(|(schema_idx, _, _)| reader.gather(rg, *schema_idx, &sel))
        .collect();
    Some(ScanBatch { cols, rows: sel.len() })
}

fn cell_at(col: &ColumnData, i: usize) -> Cell {
    match col {
        ColumnData::Int64(v) => Cell::I(v[i]),
        ColumnData::Utf8(v) => Cell::S(v[i].clone()),
    }
}

fn i64_at(col: &ColumnData, i: usize) -> i64 {
    match col {
        ColumnData::Int64(v) => v[i],
        _ => panic!("join/agg key must be i64"),
    }
}

// ---------------------------------------------------------------------------
// Output expression machinery
// ---------------------------------------------------------------------------

/// A joined (or single-table) row is `cells[scan_idx][col_pos]`.
type Row = Vec<Vec<Cell>>;

struct OutputCtx {
    /// (scan, pos) -> index in group key, for OutExpr::Col under GROUP BY
    group_pos: HashMap<(usize, usize), usize>,
    aggs: Vec<(AggFn, usize, usize)>, // in output order
    has_aggs: bool,
}

fn output_ctx(plan: &PhysicalPlan) -> OutputCtx {
    let mut group_pos = HashMap::new();
    for (i, gk) in plan.group_by.iter().enumerate() {
        group_pos.insert(*gk, i);
    }
    let mut aggs = vec![];
    let mut has_aggs = false;
    for o in &plan.output {
        if let OutExpr::Agg(f, s, p) = o {
            aggs.push((*f, *s, *p));
            has_aggs = true;
        }
    }
    OutputCtx { group_pos, aggs, has_aggs }
}

/// Feed one joined row into the partial aggregation state.
#[inline]
fn consume_row(plan: &PhysicalPlan, ctx: &OutputCtx, part: &mut Partial, row: &Row) {
    if ctx.has_aggs {
        let key: Vec<Cell> = plan
            .group_by
            .iter()
            .map(|(s, p)| row[*s][*p].clone())
            .collect();
        let accs = part
            .groups
            .entry(key)
            .or_insert_with(|| vec![Acc::new(); ctx.aggs.len()]);
        for (ai, (_, s, p)) in ctx.aggs.iter().enumerate() {
            if *p == usize::MAX {
                accs[ai].add_count_only(); // COUNT(*)
            } else {
                match &row[*s][*p] {
                    Cell::I(v) => accs[ai].add(*v),
                    Cell::S(_) => accs[ai].add_count_only(),
                }
            }
        }
    } else {
        let vals: Vec<Val> = plan
            .output
            .iter()
            .map(|o| match o {
                OutExpr::Col(s, p) => match &row[*s][*p] {
                    Cell::I(v) => Val::I(*v),
                    Cell::S(v) => Val::S(v.clone()),
                },
                OutExpr::Agg(..) => unreachable!(),
            })
            .collect();
        part.rows.push(vals);
    }
}

// ---------------------------------------------------------------------------
// Ramped task scheduler
// ---------------------------------------------------------------------------

pub struct EngineOpts {
    pub local_threads: usize,
    /// Simulated microVM resume delays (ms) for workers that join mid-query.
    pub worker_join_ms: Vec<u64>,
}

impl Default for EngineOpts {
    fn default() -> Self {
        EngineOpts { local_threads: 1, worker_join_ms: vec![] }
    }
}

struct RampOutcome {
    partials: Vec<Partial>,
    timeline: Vec<String>,
}

/// Work-stealing over an atomic task counter. Local threads start at t=0;
/// each ramp worker sleeps its resume delay, then joins the same queue.
fn run_ramped<F>(ntasks: usize, opts: &EngineOpts, phase: &str, task_fn: F, t0: Instant) -> RampOutcome
where
    F: Fn(usize, &mut Partial) + Sync,
{
    let next = AtomicUsize::new(0);
    let results: Mutex<Vec<(String, Partial, usize, f64, f64)>> = Mutex::new(vec![]);
    std::thread::scope(|s| {
        for t in 0..opts.local_threads {
            let (next, results, task_fn) = (&next, &results, &task_fn);
            s.spawn(move || {
                let joined = t0.elapsed().as_secs_f64() * 1e3;
                let mut part = Partial::new();
                let mut done = 0;
                loop {
                    let i = next.fetch_add(1, Ordering::Relaxed);
                    if i >= ntasks { break; }
                    task_fn(i, &mut part);
                    done += 1;
                }
                let fin = t0.elapsed().as_secs_f64() * 1e3;
                results.lock().unwrap().push((format!("local-{t}"), part, done, joined, fin));
            });
        }
        for (w, delay) in opts.worker_join_ms.iter().enumerate() {
            let (next, results, task_fn, delay) = (&next, &results, &task_fn, *delay);
            s.spawn(move || {
                // simulated Firecracker snapshot-resume latency
                std::thread::sleep(std::time::Duration::from_millis(delay));
                let joined = t0.elapsed().as_secs_f64() * 1e3;
                let mut part = Partial::new();
                let mut done = 0;
                loop {
                    let i = next.fetch_add(1, Ordering::Relaxed);
                    if i >= ntasks { break; }
                    task_fn(i, &mut part);
                    done += 1;
                }
                let fin = t0.elapsed().as_secs_f64() * 1e3;
                results.lock().unwrap().push((format!("microvm-{w}"), part, done, joined, fin));
            });
        }
    });
    let mut rows = results.into_inner().unwrap();
    rows.sort_by(|a, b| a.3.partial_cmp(&b.3).unwrap());
    let mut timeline = vec![];
    let mut partials = vec![];
    for (name, part, done, joined, fin) in rows {
        timeline.push(format!(
            "  [{phase}] {name:<10} joined {joined:7.2} ms   tasks={done:<4} finished {fin:7.2} ms"
        ));
        partials.push(part);
    }
    RampOutcome { partials, timeline }
}

// ---------------------------------------------------------------------------
// Query execution
// ---------------------------------------------------------------------------

pub struct QueryResult {
    pub header: Vec<String>,
    pub rows: Vec<Vec<Val>>,
    pub timeline: Vec<String>,
    pub scanned_rows: u64,
    pub joined_rows: u64,
    pub elapsed_ms: f64,
}

fn header(plan: &PhysicalPlan) -> Vec<String> {
    plan.output
        .iter()
        .map(|o| match o {
            OutExpr::Col(s, p) => plan.scans[*s].cols[*p].1.clone(),
            OutExpr::Agg(f, s, p) => {
                let f = match f {
                    AggFn::Sum => "SUM",
                    AggFn::Count => "COUNT",
                    AggFn::Min => "MIN",
                    AggFn::Max => "MAX",
                    AggFn::Avg => "AVG",
                };
                if *p == usize::MAX {
                    format!("{f}(*)")
                } else {
                    format!("{f}({})", plan.scans[*s].cols[*p].1)
                }
            }
        })
        .collect()
}

/// Enumerate morsels: one task per (file, rowgroup) of a scan.
fn tasks_for(scan: &ScanNode) -> Vec<(usize, usize)> {
    let mut tasks = vec![];
    for (fi, f) in scan.files.iter().enumerate() {
        if let Ok(r) = Reader::open(&f.path) {
            for rg in 0..r.meta.rowgroups.len() {
                tasks.push((fi, rg));
            }
        }
    }
    tasks
}

/// Scan an entire (small) side into rows of its projected columns.
fn scan_all(scan: &ScanNode) -> Vec<Vec<Cell>> {
    let mut out = vec![];
    for f in &scan.files {
        let mut r = match Reader::open(&f.path) { Ok(r) => r, Err(_) => continue };
        for rg in 0..r.meta.rowgroups.len() {
            if let Some(b) = scan_rowgroup(&mut r, scan, rg) {
                for i in 0..b.rows {
                    out.push(b.cols.iter().map(|c| cell_at(c, i)).collect());
                }
            }
        }
    }
    out
}

pub fn execute(plan: &PhysicalPlan, opts: &EngineOpts) -> QueryResult {
    let t0 = Instant::now();
    let ctx = output_ctx(plan);
    let mut timeline = vec![];
    let mut partials: Vec<Partial> = vec![];

    match &plan.join {
        None => {
            let scan = &plan.scans[0];
            let tasks = tasks_for(scan);
            let out = run_ramped(tasks.len(), opts, "scan", |i, part| {
                let (fi, rg) = tasks[i];
                let mut r = Reader::open(&scan.files[fi].path).unwrap();
                if let Some(b) = scan_rowgroup(&mut r, scan, rg) {
                    part.scanned_rows += b.rows as u64;
                    let mut row: Row = vec![vec![]];
                    for i in 0..b.rows {
                        row[0] = b.cols.iter().map(|c| cell_at(c, i)).collect();
                        consume_row(plan, &ctx, part, &row);
                    }
                }
            }, t0);
            timeline.extend(out.timeline);
            partials.extend(out.partials);
        }
        Some(j @ JoinNode { strategy: JoinStrategy::Broadcast, .. }) => {
            // Build side: scanned once, hash table broadcast (shared) to all tasks.
            let build_scan = &plan.scans[j.build];
            let build_rows = scan_all(build_scan);
            let mut ht: HashMap<i64, Vec<usize>> = HashMap::new();
            for (i, r) in build_rows.iter().enumerate() {
                if let Cell::I(k) = r[j.build_key] {
                    ht.entry(k).or_default().push(i);
                }
            }
            timeline.push(format!(
                "  [build] hash table: {} rows, {} keys ({:.2} ms)",
                build_rows.len(), ht.len(), t0.elapsed().as_secs_f64() * 1e3
            ));
            let probe_scan = &plan.scans[j.probe];
            let tasks = tasks_for(probe_scan);
            let (ht, build_rows) = (&ht, &build_rows);
            let out = run_ramped(tasks.len(), opts, "probe", |i, part| {
                let (fi, rg) = tasks[i];
                let mut r = Reader::open(&probe_scan.files[fi].path).unwrap();
                if let Some(b) = scan_rowgroup(&mut r, probe_scan, rg) {
                    part.scanned_rows += b.rows as u64;
                    let nscans = plan.scans.len();
                    let mut row: Row = vec![vec![]; nscans];
                    for i in 0..b.rows {
                        let k = i64_at(&b.cols[j.probe_key], i);
                        if let Some(matches) = ht.get(&k) {
                            row[j.probe] = b.cols.iter().map(|c| cell_at(c, i)).collect();
                            for &bi in matches {
                                row[j.build] = build_rows[bi].clone();
                                part.joined_rows += 1;
                                consume_row(plan, &ctx, part, &row);
                            }
                        }
                    }
                }
            }, t0);
            timeline.extend(out.timeline);
            partials.extend(out.partials);
        }
        Some(j @ JoinNode { strategy: JoinStrategy::Shuffle { partitions }, .. }) => {
            let nparts = *partitions;
            // Exchange buffers: hash(key) % partitions, one bucket per partition
            // per side. Over a network these buffers are the shuffle streams
            // between nodes; same hash, same protocol.
            let exchange: Vec<Mutex<(Vec<Vec<Cell>>, Vec<Vec<Cell>>)>> =
                (0..nparts).map(|_| Mutex::new((vec![], vec![]))).collect();

            // Phase 1 (map): scan both sides, partition rows by join key.
            let build_scan = &plan.scans[j.build];
            let probe_scan = &plan.scans[j.probe];
            let btasks = tasks_for(build_scan);
            let ptasks = tasks_for(probe_scan);
            let total = btasks.len() + ptasks.len();
            let exch = &exchange;
            let out = run_ramped(total, opts, "shuffle-map", |i, part| {
                let (scan, key_pos, side, fi, rg) = if i < btasks.len() {
                    let (fi, rg) = btasks[i];
                    (build_scan, j.build_key, 0usize, fi, rg)
                } else {
                    let (fi, rg) = ptasks[i - btasks.len()];
                    (probe_scan, j.probe_key, 1usize, fi, rg)
                };
                let mut r = Reader::open(&scan.files[fi].path).unwrap();
                if let Some(b) = scan_rowgroup(&mut r, scan, rg) {
                    part.scanned_rows += b.rows as u64;
                    // bucket locally first, lock each partition once
                    let mut local: Vec<Vec<Vec<Cell>>> = vec![vec![]; nparts];
                    for i in 0..b.rows {
                        let k = i64_at(&b.cols[key_pos], i);
                        let p = (k.rem_euclid(nparts as i64)) as usize;
                        local[p].push(b.cols.iter().map(|c| cell_at(c, i)).collect());
                    }
                    for (p, rows) in local.into_iter().enumerate() {
                        if rows.is_empty() { continue; }
                        let mut g = exch[p].lock().unwrap();
                        if side == 0 { g.0.extend(rows); } else { g.1.extend(rows); }
                    }
                }
            }, t0);
            timeline.extend(out.timeline);
            partials.extend(out.partials);
            let exchanged: usize = exchange
                .iter()
                .map(|m| { let g = m.lock().unwrap(); g.0.len() + g.1.len() })
                .sum();
            timeline.push(format!(
                "  [exchange] {exchanged} rows hash-partitioned into {nparts} partitions ({:.2} ms)",
                t0.elapsed().as_secs_f64() * 1e3
            ));

            // Phase 2 (reduce): per-partition build + probe. All workers have
            // resumed by now, so they all start immediately.
            let phase2 = EngineOpts {
                local_threads: opts.local_threads + opts.worker_join_ms.len(),
                worker_join_ms: vec![],
            };
            let out = run_ramped(nparts, &phase2, "join", |p, part| {
                let g = exch[p].lock().unwrap();
                let (build, probe) = (&g.0, &g.1);
                let mut ht: HashMap<i64, Vec<usize>> = HashMap::new();
                for (i, r) in build.iter().enumerate() {
                    if let Cell::I(k) = r[j.build_key] {
                        ht.entry(k).or_default().push(i);
                    }
                }
                let nscans = plan.scans.len();
                let mut row: Row = vec![vec![]; nscans];
                for pr in probe {
                    if let Cell::I(k) = pr[j.probe_key] {
                        if let Some(matches) = ht.get(&k) {
                            row[j.probe] = pr.clone();
                            for &bi in matches {
                                row[j.build] = build[bi].clone();
                                part.joined_rows += 1;
                                consume_row(plan, &ctx, part, &row);
                            }
                        }
                    }
                }
            }, t0);
            timeline.extend(out.timeline);
            partials.extend(out.partials);
        }
    }

    // FinalAggregate: merge per-task partial states.
    let mut groups: HashMap<Vec<Cell>, Vec<Acc>> = HashMap::new();
    let mut rows: Vec<Vec<Val>> = vec![];
    let mut scanned_rows = 0;
    let mut joined_rows = 0;
    for p in partials {
        scanned_rows += p.scanned_rows;
        joined_rows += p.joined_rows;
        rows.extend(p.rows);
        for (k, accs) in p.groups {
            match groups.entry(k) {
                std::collections::hash_map::Entry::Occupied(mut e) => {
                    for (a, b) in e.get_mut().iter_mut().zip(accs.iter()) {
                        a.merge(b);
                    }
                }
                std::collections::hash_map::Entry::Vacant(e) => {
                    e.insert(accs);
                }
            }
        }
    }
    if ctx.has_aggs {
        for (key, accs) in groups {
            let mut ai = 0;
            let out_row: Vec<Val> = plan
                .output
                .iter()
                .map(|o| match o {
                    OutExpr::Col(s, p) => match &key[ctx.group_pos[&(*s, *p)]] {
                        Cell::I(v) => Val::I(*v),
                        Cell::S(v) => Val::S(v.clone()),
                    },
                    OutExpr::Agg(f, _, _) => {
                        let v = accs[ai].result(*f);
                        ai += 1;
                        v
                    }
                })
                .collect();
            rows.push(out_row);
        }
    }

    // TopN
    if let Some((pos, desc)) = plan.order_by {
        rows.sort_by(|a, b| {
            let c = val_cmp(&a[pos], &b[pos]);
            if desc { c.reverse() } else { c }
        });
    }
    if let Some(n) = plan.limit {
        rows.truncate(n);
    }

    QueryResult {
        header: header(plan),
        rows,
        timeline,
        scanned_rows,
        joined_rows,
        elapsed_ms: t0.elapsed().as_secs_f64() * 1e3,
    }
}

/// Pretty-print a result table.
pub fn render(r: &QueryResult) -> String {
    let mut widths: Vec<usize> = r.header.iter().map(|h| h.len()).collect();
    let cells: Vec<Vec<String>> = r
        .rows
        .iter()
        .map(|row| row.iter().map(|v| v.to_string()).collect())
        .collect();
    for row in &cells {
        for (i, c) in row.iter().enumerate() {
            widths[i] = widths[i].max(c.len());
        }
    }
    let mut s = String::new();
    let line = |s: &mut String| {
        s.push('+');
        for w in &widths {
            s.push_str(&"-".repeat(w + 2));
            s.push('+');
        }
        s.push('\n');
    };
    line(&mut s);
    s.push('|');
    for (h, w) in r.header.iter().zip(&widths) {
        s.push_str(&format!(" {h:<w$} |"));
    }
    s.push('\n');
    line(&mut s);
    for row in &cells {
        s.push('|');
        for (c, w) in row.iter().zip(&widths) {
            s.push_str(&format!(" {c:>w$} |"));
        }
        s.push('\n');
    }
    line(&mut s);
    s
}
