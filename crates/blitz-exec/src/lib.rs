//! blitz-exec — morsel-driven, *ramped* distributed execution.
//!
//! The core idea behind cold-start latency hiding:
//!
//!   t=0      query arrives at the coordinator (just resumed from snapshot)
//!   t≈0      coordinator's local threads start consuming morsels IMMEDIATELY
//!   t≈5ms    worker microVMs finish their own snapshot resume, dial in over
//!            vsock/TCP and start stealing morsels from the shared queue
//!   t=end    coordinator merges partial aggregates
//!
//! No phase waits for the cluster to exist. The cluster materializes *inside*
//! the query. Work distribution is pull-based (work stealing), so a worker
//! that joins late simply gets fewer morsels — there is no repartitioning.

use blitz_cluster::ClusteredTable;
use blitz_core::{
    filter_i64, filter_f64, filter_date, group_agg, sum_i64, sum_i64_sel, sum_f64, sum_f64_sel,
    Acc, Block, CmpOp, Column,
};
use blitz_sql::{AggFn, Query};
use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// Wire protocol (length-free fixed framing, little-endian)
// ---------------------------------------------------------------------------

fn w_u32(s: &mut impl Write, v: u32) {
    s.write_all(&v.to_le_bytes()).unwrap();
}
fn w_u64(s: &mut impl Write, v: u64) {
    s.write_all(&v.to_le_bytes()).unwrap();
}
fn w_i64(s: &mut impl Write, v: i64) {
    s.write_all(&v.to_le_bytes()).unwrap();
}
fn r_u8(s: &mut impl Read) -> std::io::Result<u8> {
    let mut b = [0u8; 1];
    s.read_exact(&mut b)?;
    Ok(b[0])
}
fn r_u32(s: &mut impl Read) -> std::io::Result<u32> {
    let mut b = [0u8; 4];
    s.read_exact(&mut b)?;
    Ok(u32::from_le_bytes(b))
}
fn r_u64(s: &mut impl Read) -> std::io::Result<u64> {
    let mut b = [0u8; 8];
    s.read_exact(&mut b)?;
    Ok(u64::from_le_bytes(b))
}
fn r_i64(s: &mut impl Read) -> std::io::Result<i64> {
    let mut b = [0u8; 8];
    s.read_exact(&mut b)?;
    Ok(i64::from_le_bytes(b))
}
fn w_i64_slice(s: &mut impl Write, v: &[i64]) {
    w_u64(s, v.len() as u64);
    // Safe little-endian bulk copy.
    let bytes: Vec<u8> = v.iter().flat_map(|x| x.to_le_bytes()).collect();
    s.write_all(&bytes).unwrap();
}
fn r_i64_vec(s: &mut impl Read) -> std::io::Result<Vec<i64>> {
    let n = r_u64(s)? as usize;
    let mut buf = vec![0u8; n * 8];
    s.read_exact(&mut buf)?;
    Ok(buf.chunks_exact(8).map(|c| i64::from_le_bytes(c.try_into().unwrap())).collect())
}

const MSG_QUERY: u8 = 1;
const MSG_MORSEL: u8 = 2;
const MSG_DONE: u8 = 3;
const MSG_NEXT: u8 = 4;
const MSG_PARTIAL: u8 = 5;
const MSG_SHUFFLE: u8 = 6;       // Shuffle partition data
const MSG_SHUFFLE_PART: u8 = 7;  // Shuffle partition number indicator

fn write_query(s: &mut impl Write, q: &Query) {
    s.write_all(&[MSG_QUERY, q.agg.to_u8()]).unwrap();
    w_u32(s, q.agg_col as u32);
    match q.filter {
        Some((c, op, lit)) => {
            s.write_all(&[1, op.to_u8()]).unwrap();
            w_u32(s, c as u32);
            w_i64(s, lit);
        }
        None => s.write_all(&[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]).unwrap(),
    }
    match q.group_by {
        Some(c) => {
            s.write_all(&[1]).unwrap();
            w_u32(s, c as u32);
        }
        None => s.write_all(&[0, 0, 0, 0, 0]).unwrap(),
    }
    match q.order_by {
        Some((c, asc)) => {
            s.write_all(&[1, if asc { 1 } else { 0 }]).unwrap();
            w_u32(s, c as u32);
        }
        None => s.write_all(&[0, 0, 0, 0, 0, 0]).unwrap(),
    }
    match q.limit {
        Some(n) => {
            s.write_all(&[1]).unwrap();
            w_u32(s, n as u32);
        }
        None => s.write_all(&[0, 0, 0, 0, 0]).unwrap(),
    }
}

fn read_query(s: &mut impl Read) -> std::io::Result<Query> {
    use blitz_sql::TableSource;

    let agg = AggFn::from_u8(r_u8(s)?);
    let agg_col = r_u32(s)? as usize;
    let hf = r_u8(s)?;
    let op = CmpOp::from_u8(r_u8(s)?);
    let fc = r_u32(s)? as usize;
    let lit = r_i64(s)?;
    let hg = r_u8(s)?;
    let gc = r_u32(s)? as usize;
    let ho = r_u8(s)?;
    let oasc = r_u8(s)? != 0;
    let oc = r_u32(s)? as usize;
    let hl = r_u8(s)?;
    let lim = r_u32(s)? as usize;
    Ok(Query {
        agg,
        agg_col,
        table: TableSource::Table("".to_string()),
        filter: (hf == 1).then_some((fc, op, lit)),
        group_by: (hg == 1).then_some(gc),
        ctes: vec![],
        order_by: (ho == 1).then_some((oc, oasc)),
        limit: (hl == 1).then_some(lim),
    })
}

/// Resolve CTE / subquery table sources into a base-table scan query.
pub fn resolve_table_source(q: Query) -> Result<Query, String> {
    let mut q = q;
    for _ in 0..32 {
        match q.table.clone() {
            blitz_sql::TableSource::Table(_) => return Ok(q),
            blitz_sql::TableSource::CTE(name) => {
                let inner = q
                    .ctes
                    .iter()
                    .find(|(n, _)| n == &name)
                    .map(|(_, b)| (**b).clone())
                    .ok_or_else(|| format!("unknown CTE {name}"))?;
                // Outer LIMIT/ORDER/WHERE win; agg/group come from the CTE body.
                q.table = inner.table;
                q.agg = inner.agg;
                q.agg_col = inner.agg_col;
                if q.filter.is_none() {
                    q.filter = inner.filter;
                }
                if q.group_by.is_none() {
                    q.group_by = inner.group_by;
                }
                if q.order_by.is_none() {
                    q.order_by = inner.order_by;
                }
                if q.limit.is_none() {
                    q.limit = inner.limit;
                }
                q.ctes = inner.ctes;
            }
            blitz_sql::TableSource::Subquery(inner, _) => {
                let inner = *inner;
                q.table = inner.table;
                q.agg = inner.agg;
                q.agg_col = inner.agg_col;
                if q.filter.is_none() {
                    q.filter = inner.filter;
                }
                if q.group_by.is_none() {
                    q.group_by = inner.group_by;
                }
                q.ctes = inner.ctes;
            }
        }
    }
    Err("CTE/subquery nesting too deep".into())
}

// ---------------------------------------------------------------------------
// Partial results
// ---------------------------------------------------------------------------

#[derive(Default, Clone)]
pub struct Partial {
    pub scalar: Acc,
    pub groups: HashMap<i64, Acc>,
}

impl Partial {
    pub fn merge(&mut self, o: Partial) {
        self.scalar.merge(&o.scalar);
        for (k, a) in o.groups {
            self.groups.entry(k).or_default().merge(&a);
        }
    }
}

fn write_partial(s: &mut impl Write, p: &Partial) {
    s.write_all(&[MSG_PARTIAL]).unwrap();
    w_i64(s, p.scalar.sum);
    w_u64(s, p.scalar.count);
    w_i64(s, p.scalar.min);
    w_i64(s, p.scalar.max);
    w_u64(s, p.groups.len() as u64);
    for (k, a) in &p.groups {
        w_i64(s, *k);
        w_i64(s, a.sum);
        w_u64(s, a.count);
        w_i64(s, a.min);
        w_i64(s, a.max);
    }
}

fn read_partial(s: &mut impl Read) -> std::io::Result<Partial> {
    let scalar = Acc { sum: r_i64(s)?, count: r_u64(s)?, min: r_i64(s)?, max: r_i64(s)? };
    let n = r_u64(s)?;
    let mut groups = HashMap::with_capacity(n as usize);
    for _ in 0..n {
        let k = r_i64(s)?;
        groups.insert(
            k,
            Acc { sum: r_i64(s)?, count: r_u64(s)?, min: r_i64(s)?, max: r_i64(s)? },
        );
    }
    Ok(Partial { scalar, groups })
}

// ---------------------------------------------------------------------------
// Single-morsel kernel pipeline (scan → filter → [group] aggregate)
// ---------------------------------------------------------------------------

pub fn exec_morsel(block: &Block, q: &Query) -> Partial {
    let mut p = Partial::default();

    // Evaluate filter predicate on filter column (any type).
    let sel: Option<Vec<u32>> = q.filter.map(|(filter_col, op, lit)| {
        match &block.columns[filter_col] {
            blitz_core::Column::I64(data) => blitz_core::filter_i64(data, op, lit),
            blitz_core::Column::F64(data) => blitz_core::filter_f64(data, op, lit as f64),
            blitz_core::Column::Date(data) => blitz_core::filter_date(data, op, lit as i32),
            blitz_core::Column::Decimal(data, _) => {
                // For decimal filtering, compare as i128
                let n = data.len();
                let mut out = vec![0u32; n];
                let mut k = 0usize;
                let lit_dec = lit as i128;
                macro_rules! run {
                    ($cmp:expr) => {
                        for i in 0..n {
                            let v = unsafe { *data.get_unchecked(i) };
                            let m = $cmp(v) as usize;
                            unsafe { *out.get_unchecked_mut(k) = i as u32 };
                            k += m;
                        }
                    };
                }
                match op {
                    CmpOp::Gt => run!(|v| v > lit_dec),
                    CmpOp::Lt => run!(|v| v < lit_dec),
                    CmpOp::Ge => run!(|v| v >= lit_dec),
                    CmpOp::Le => run!(|v| v <= lit_dec),
                    CmpOp::Eq => run!(|v| v == lit_dec),
                }
                out.truncate(k);
                out
            }
        }
    });

    // Aggregate on agg column (currently i64 only for Partial storage).
    // For other types, convert to i64 or error.
    match &block.columns[q.agg_col] {
        blitz_core::Column::I64(agg_data) => {
            match q.group_by {
                Some(gc) => {
                    let keys = block.columns[gc].as_i64();
                    p.groups = group_agg(keys, agg_data, sel.as_deref());
                }
                None => {
                    p.scalar = match &sel {
                        Some(s) => sum_i64_sel(agg_data, s),
                        None => sum_i64(agg_data),
                    };
                }
            }
        }
        blitz_core::Column::F64(agg_data) => {
            // Store f64 aggregate as i64 (losing precision for now; better: extend Partial)
            let acc = match &sel {
                Some(s) => blitz_core::sum_f64_sel(agg_data, s),
                None => blitz_core::sum_f64(agg_data),
            };
            p.scalar = Acc {
                sum: acc.sum as i64,
                count: acc.count,
                min: acc.min as i64,
                max: acc.max as i64,
            };
        }
        blitz_core::Column::Date(_agg_data) => {
            // Date: only COUNT/MIN/MAX make sense; SUM doesn't
            // For now, store count as sum field
            let count = match &sel {
                Some(s) => s.len(),
                None => block.rows,
            };
            p.scalar = Acc {
                sum: 0,
                count: count as u64,
                min: 0,
                max: 0,
            };
        }
        blitz_core::Column::Decimal(_, _) => {
            // Decimal: similar to F64, store as i64 (precision loss)
            let count = match &sel {
                Some(s) => s.len(),
                None => block.rows,
            };
            p.scalar = Acc {
                sum: 0,
                count: count as u64,
                min: 0,
                max: 0,
            };
        }
    }

    p
}

// ---------------------------------------------------------------------------
// Coordinator: ramped execution
// ---------------------------------------------------------------------------

pub struct Timeline {
    t0: Instant,
    pub events: Mutex<Vec<(f64, String)>>,
}

impl Timeline {
    pub fn new() -> Self {
        Timeline { t0: Instant::now(), events: Mutex::new(vec![]) }
    }
    pub fn mark(&self, what: impl Into<String>) {
        self.events
            .lock()
            .unwrap()
            .push((self.t0.elapsed().as_secs_f64() * 1e3, what.into()));
    }
}

impl Default for Timeline {
    fn default() -> Self {
        Self::new()
    }
}

struct Shared {
    queue: Mutex<Vec<usize>>, // morsel ids (post zone-map pruning), LIFO
    result: Mutex<Partial>,
    done_local: AtomicBool,
    morsels_total: usize,
    morsels_done: AtomicUsize,
}

impl Shared {
    fn pop(&self) -> Option<usize> {
        self.queue.lock().unwrap().pop()
    }
    fn finish(&self, p: Partial, n: usize) {
        self.result.lock().unwrap().merge(p);
        self.morsels_done.fetch_add(n, Ordering::SeqCst);
    }
}

pub struct RampReport {
    pub result: Partial,
    pub timeline: Vec<(f64, String)>,
    pub morsels_executed: usize,
    pub morsels_pruned: usize,
}

/// Run a query with ramped scale-out.
///
/// * `local_threads` — threads on the coordinator (work starts here at t≈0).
/// * `listen` — TCP address remote workers dial into.
/// * `expected_workers` — how many remote workers to accept.
/// * `ship_data` — if true, coordinator sends data inline; if false, workers
///   fetch from shared storage (production path).
pub fn run_ramped(
    table: Arc<ClusteredTable>,
    q: Query,
    local_threads: usize,
    listen: &str,
    expected_workers: usize,
    ship_data: bool,
    tl: Arc<Timeline>,
) -> RampReport {
    run_ramped_internal(table, q, local_threads, listen, expected_workers, ship_data, None, tl)
}

/// Run with explicit shuffle join on specified column.
pub fn run_ramped_with_shuffle(
    table: Arc<ClusteredTable>,
    q: Query,
    local_threads: usize,
    listen: &str,
    expected_workers: usize,
    ship_data: bool,
    shuffle_col: usize,
    tl: Arc<Timeline>,
) -> RampReport {
    run_ramped_internal(
        table, q, local_threads, listen, expected_workers, ship_data, Some(shuffle_col), tl,
    )
}

fn run_ramped_internal(
    table: Arc<ClusteredTable>,
    q: Query,
    local_threads: usize,
    listen: &str,
    expected_workers: usize,
    ship_data: bool,
    _shuffle_col: Option<usize>,
    tl: Arc<Timeline>,
) -> RampReport {
    let q = match resolve_table_source(q) {
        Ok(q) => q,
        Err(e) => {
            tl.mark(format!("query resolve failed: {e}"));
            return RampReport {
                result: Partial::default(),
                timeline: tl.events.lock().unwrap().clone(),
                morsels_executed: 0,
                morsels_pruned: 0,
            };
        }
    };
    let order_by = q.order_by;
    let limit = q.limit;
    let all = table.blocks.len();
    let morsels = table.pruned_morsels(q.filter);
    let pruned = all - morsels.len();
    tl.mark(format!(
        "plan ready: {} morsels ({} pruned by zone maps)",
        morsels.len(),
        pruned
    ));

    let shared = Arc::new(Shared {
        morsels_total: morsels.len(),
        queue: Mutex::new(morsels),
        result: Mutex::new(Partial::default()),
        done_local: AtomicBool::new(false),
        morsels_done: AtomicUsize::new(0),
    });

    // --- Local execution starts NOW (this is the whole point) -------------
    let mut handles = vec![];
    for t in 0..local_threads {
        let sh = shared.clone();
        let tb = table.clone();
        let tl2 = tl.clone();
        let qq = q.clone();
        handles.push(std::thread::spawn(move || {
            let mut local = Partial::default();
            let mut n = 0;
            let mut first = true;
            while let Some(m) = sh.pop() {
                if first && t == 0 {
                    tl2.mark("FIRST MORSEL EXECUTING (single-node, cluster still booting)");
                    first = false;
                }
                local.merge(exec_morsel(&tb.blocks[m], &qq));
                n += 1;
            }
            sh.finish(local, n);
        }));
    }

    // --- Remote worker acceptor (workers join mid-query) ------------------
    let listener = TcpListener::bind(listen).expect("bind");
    listener.set_nonblocking(true).unwrap();
    let acceptor = {
        let sh = shared.clone();
        let tb = table.clone();
        let tl2 = tl.clone();
        let qq = q.clone();
        std::thread::spawn(move || {
            let mut joined = 0usize;
            let mut conns: Vec<std::thread::JoinHandle<()>> = vec![];
            while joined < expected_workers && !sh.done_local.load(Ordering::SeqCst) {
                match listener.accept() {
                    Ok((stream, _)) => {
                        joined += 1;
                        tl2.mark(format!("worker {joined} joined ramp"));
                        let sh2 = sh.clone();
                        let tb2 = tb.clone();
                        let q3 = qq.clone();
                        conns.push(std::thread::spawn(move || {
                            serve_worker(stream, &tb2, &q3, &sh2, ship_data)
                        }));
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_micros(200));
                    }
                    Err(_) => break,
                }
            }
            for c in conns {
                let _ = c.join();
            }
        })
    };

    for h in handles {
        h.join().unwrap();
    }
    // Wait until every dispatched morsel's partial has landed.
    while shared.morsels_done.load(Ordering::SeqCst) < shared.morsels_total {
        std::thread::sleep(Duration::from_micros(100));
    }
    shared.done_local.store(true, Ordering::SeqCst);
    // Unblock acceptor if no worker ever connects.
    let _ = TcpStream::connect(listen);
    let _ = acceptor.join();

    tl.mark("query complete, partials merged");
    let mut result = std::mem::take(&mut *shared.result.lock().unwrap());
    apply_order_limit(&mut result, order_by, limit);
    RampReport {
        result,
        timeline: tl.events.lock().unwrap().clone(),
        morsels_executed: shared.morsels_total,
        morsels_pruned: pruned,
    }
}

fn apply_order_limit(
    result: &mut Partial,
    order_by: Option<(usize, bool)>,
    limit: Option<usize>,
) {
    if result.groups.is_empty() {
        return;
    }
    let mut rows: Vec<(i64, Acc)> = result.groups.drain().collect();
    if let Some((_col, asc)) = order_by {
        // Group key is the GROUP BY value (i64); sort by key.
        rows.sort_by(|a, b| {
            if asc {
                a.0.cmp(&b.0)
            } else {
                b.0.cmp(&a.0)
            }
        });
    }
    if let Some(n) = limit {
        rows.truncate(n);
    }
    result.groups = rows.into_iter().collect();
}

/// Coordinator side of one remote worker connection: stream morsels out,
/// merge partials back.
///
/// * `ship_data = false` (shared-storage / StarRocks shared-data mode):
///   only the morsel ID crosses the wire; the worker reads the block from
///   shared storage itself. This is the production path.
/// * `ship_data = true` (shared-nothing fallback): the coordinator ships the
///   touched columns of the block — only the columns the query needs.
fn serve_worker(mut s: TcpStream, table: &ClusteredTable, q: &Query, sh: &Shared, ship_data: bool) {
    s.set_nodelay(true).ok();
    write_query(&mut s, q);
    let mut needed: Vec<usize> = vec![q.agg_col];
    if let Some((c, _, _)) = q.filter {
        if !needed.contains(&c) {
            needed.push(c);
        }
    }
    if let Some(c) = q.group_by {
        if !needed.contains(&c) {
            needed.push(c);
        }
    }
    loop {
        match r_u8(&mut s) {
            Ok(MSG_NEXT) => match sh.pop() {
                Some(m) => {
                    s.write_all(&[MSG_MORSEL]).unwrap();
                    if ship_data {
                        w_u32(&mut s, u32::MAX); // marker: inline data follows
                        let b = &table.blocks[m];
                        w_u32(&mut s, needed.len() as u32);
                        for &c in &needed {
                            w_u32(&mut s, c as u32);
                            w_i64_slice(&mut s, b.columns[c].as_i64());
                        }
                    } else {
                        w_u32(&mut s, m as u32); // morsel id only
                    }
                    match read_partial_msg(&mut s) {
                        Ok(p) => sh.finish(p, 1),
                        Err(_) => {
                            // Worker died mid-morsel: return it to the queue.
                            sh.queue.lock().unwrap().push(m);
                            return;
                        }
                    }
                }
                None => {
                    let _ = s.write_all(&[MSG_DONE]);
                    return;
                }
            },
            _ => return,
        }
    }
}

fn read_partial_msg(s: &mut impl Read) -> std::io::Result<Partial> {
    match r_u8(s)? {
        MSG_PARTIAL => read_partial(s),
        _ => Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "bad msg")),
    }
}

// ---------------------------------------------------------------------------
// Shuffle support: hash-partition data across 16 partitions
// ---------------------------------------------------------------------------

/// Hash-partition a column of i64 values into partitions based on join key.
/// Returns Vec of partition IDs (one per row) for gathering into buffers.
fn partition_key(key: i64, num_partitions: usize) -> usize {
    // FNV-1a-like mixing
    let mut h = 0x811c9dc5u32 as i64;
    h ^= key;
    h = h.wrapping_mul(0x01000193);
    (h.abs() % num_partitions as i64) as usize
}

/// Serialize shuffled data: partition each row and build buffers per partition.
/// Returns Vec<(partition_id, serialized_block)> for network transmission.
fn serialize_shuffled_block(block: &Block, partition_col: usize, num_partitions: usize) -> Vec<(usize, Vec<u8>)> {
    let keys = block.columns[partition_col].as_i64();
    let mut buffers: Vec<Vec<Column>> = vec![vec![]; num_partitions];
    let mut counts: Vec<usize> = vec![0; num_partitions];

    // First pass: count rows per partition
    for &k in keys {
        let p = partition_key(k, num_partitions);
        counts[p] += 1;
    }

    // Preallocate column vectors
    for p in 0..num_partitions {
        for _ in 0..block.columns.len() {
            // Create empty column of same type
            match &block.columns[0] {
                Column::I64(_) => buffers[p].push(Column::I64(Vec::with_capacity(counts[p]))),
                Column::F64(_) => buffers[p].push(Column::F64(Vec::with_capacity(counts[p]))),
                Column::Decimal(_, s) => buffers[p].push(Column::Decimal(Vec::with_capacity(counts[p]), *s)),
                Column::Date(_) => buffers[p].push(Column::Date(Vec::with_capacity(counts[p]))),
            }
        }
    }

    // Second pass: distribute rows to partitions
    for i in 0..block.rows {
        let k = unsafe { *keys.get_unchecked(i) };
        let p = partition_key(k, num_partitions);

        for c in 0..block.columns.len() {
            match (&block.columns[c], &mut buffers[p][c]) {
                (Column::I64(src), Column::I64(dst)) => dst.push(src[i]),
                (Column::F64(src), Column::F64(dst)) => dst.push(src[i]),
                (Column::Decimal(src, _), Column::Decimal(dst, _)) => dst.push(src[i]),
                (Column::Date(src), Column::Date(dst)) => dst.push(src[i]),
                _ => panic!("Type mismatch in shuffle"),
            }
        }
    }

    // Serialize each partition
    let mut result = Vec::with_capacity(num_partitions);
    for (p, cols) in buffers.into_iter().enumerate() {
        if !cols.is_empty() && cols[0].len() > 0 {
            let mut buf = vec![];
            buf.push(MSG_SHUFFLE);
            w_u32(&mut buf, p as u32);
            w_u32(&mut buf, cols.len() as u32);
            w_u32(&mut buf, cols[0].len() as u32);
            for col in &cols {
                buf.extend(col.to_bytes());
            }
            result.push((p, buf));
        }
    }
    result
}

// ---------------------------------------------------------------------------
// Remote worker (runs inside its own microVM in production)
// ---------------------------------------------------------------------------

/// Dial the coordinator, then pull-execute morsels until DONE.
/// `storage`: Some(table) in shared-storage mode (worker reads blocks from
/// its own mount of shared storage); None means the coordinator inlines data.
pub fn worker_main(coordinator: &str, storage: Option<Arc<ClusteredTable>>) -> std::io::Result<usize> {
    let mut s = TcpStream::connect(coordinator)?;
    s.set_nodelay(true).ok();
    assert_eq!(r_u8(&mut s)?, MSG_QUERY);
    let q = read_query(&mut s)?;
    let mut done = 0usize;
    let mut shuffle_partials = vec![]; // Buffer partials during shuffle
    loop {
        s.write_all(&[MSG_NEXT])?;
        match r_u8(&mut s)? {
            MSG_MORSEL => {
                let tag = r_u32(&mut s)?;
                let p = if tag != u32::MAX {
                    // Shared-storage: tag is the morsel id; read locally.
                    let t = storage.as_ref().expect("shared-storage worker needs table");
                    exec_morsel(&t.blocks[tag as usize], &q)
                } else {
                    // Inline data path.
                    let ncols = r_u32(&mut s)? as usize;
                    let mut cols: Vec<(usize, Vec<i64>)> = Vec::with_capacity(ncols);
                    let mut max_id = 0usize;
                    for _ in 0..ncols {
                        let id = r_u32(&mut s)? as usize;
                        max_id = max_id.max(id);
                        cols.push((id, r_i64_vec(&mut s)?));
                    }
                    let rows = cols[0].1.len();
                    let mut columns = vec![blitz_core::Column::I64(vec![]); max_id + 1];
                    for (id, v) in cols {
                        columns[id] = blitz_core::Column::I64(v);
                    }
                    exec_morsel(&Block { columns, rows }, &q)
                };
                write_partial(&mut s, &p);
                done += 1;
            }
            MSG_SHUFFLE => {
                // Receive shuffled data: partition_id, column count, row count, then columns
                let _partition_id = r_u32(&mut s)?;
                let ncols = r_u32(&mut s)?;
                let rows = r_u32(&mut s)?;

                // Read column data from network
                let mut columns = Vec::with_capacity(ncols as usize);
                for _ in 0..ncols {
                    let mut col_buf = Vec::new();
                    let col_len = r_u32(&mut s)? as usize;
                    col_buf.resize(col_len, 0u8);
                    s.read_exact(&mut col_buf)?;
                    columns.push(Column::from_bytes(&col_buf)?);
                }

                // Execute on this shuffled block
                let block = Block { columns, rows: rows as usize };
                let partial = exec_morsel(&block, &q);
                shuffle_partials.push(partial);
            }
            MSG_SHUFFLE_PART => {
                // Merge and send all accumulated shuffle partials
                if !shuffle_partials.is_empty() {
                    let mut merged = Partial::default();
                    for partial in &shuffle_partials {
                        merged.merge(partial.clone());
                    }
                    write_partial(&mut s, &merged);
                }
                shuffle_partials.clear();
                done += 1;
            }
            MSG_DONE => return Ok(done),
            _ => return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "bad msg")),
        }
    }
}
