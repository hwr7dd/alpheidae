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
use blitz_core::{filter_i64, group_agg, sum_i64, sum_i64_sel, Acc, Block, CmpOp};
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
}

fn read_query(s: &mut impl Read) -> std::io::Result<Query> {
    let agg = AggFn::from_u8(r_u8(s)?);
    let agg_col = r_u32(s)? as usize;
    let hf = r_u8(s)?;
    let op = CmpOp::from_u8(r_u8(s)?);
    let fc = r_u32(s)? as usize;
    let lit = r_i64(s)?;
    let hg = r_u8(s)?;
    let gc = r_u32(s)? as usize;
    Ok(Query {
        agg,
        agg_col,
        filter: (hf == 1).then_some((fc, op, lit)),
        group_by: (hg == 1).then_some(gc),
    })
}

// ---------------------------------------------------------------------------
// Partial results
// ---------------------------------------------------------------------------

#[derive(Default)]
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
    let agg_data = block.columns[q.agg_col].as_i64();
    let sel: Option<Vec<u32>> = q
        .filter
        .map(|(c, op, lit)| filter_i64(block.columns[c].as_i64(), op, lit));
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
/// * `listen` — TCP address remote workers dial into (vsock in the microVM
///   build; TCP here so it runs anywhere).
/// * `expected_workers` — how many remote workers the ramp controller will
///   admit before it stops accepting (the resume requests themselves are
///   issued by blitz-boot in production; the demo simulates resume latency).
pub fn run_ramped(
    table: Arc<ClusteredTable>,
    q: Query,
    local_threads: usize,
    listen: &str,
    expected_workers: usize,
    ship_data: bool,
    tl: Arc<Timeline>,
) -> RampReport {
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
        handles.push(std::thread::spawn(move || {
            let mut local = Partial::default();
            let mut n = 0;
            let mut first = true;
            while let Some(m) = sh.pop() {
                if first && t == 0 {
                    tl2.mark("FIRST MORSEL EXECUTING (single-node, cluster still booting)");
                    first = false;
                }
                local.merge(exec_morsel(&tb.blocks[m], &q));
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
                        let qq = q;
                        conns.push(std::thread::spawn(move || {
                            serve_worker(stream, &tb2, &qq, &sh2, ship_data)
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
    let result = std::mem::take(&mut *shared.result.lock().unwrap());
    RampReport {
        result,
        timeline: tl.events.lock().unwrap().clone(),
        morsels_executed: shared.morsels_total,
        morsels_pruned: pruned,
    }
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
            MSG_DONE => return Ok(done),
            _ => return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "bad msg")),
        }
    }
}
