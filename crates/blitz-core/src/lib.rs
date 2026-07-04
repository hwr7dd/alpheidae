//! blitz-core — columnar blocks and vectorized execution kernels.
//!
//! Design notes
//! ------------
//! * All hot loops are written branch-free over flat `&[i64]` slices so LLVM
//!   auto-vectorizes them to AVX2/AVX-512 (verified with `--emit=asm`).
//! * Selection vectors (`Vec<u32>`) instead of bitmaps: cheaper for the
//!   low-selectivity scans that zone-map pruning produces.
//! * Aggregation uses 8-way independent accumulators to break the loop-carried
//!   dependency chain and saturate vector ALUs.

pub const BLOCK_ROWS: usize = 65_536;

#[derive(Clone)]
pub enum Column {
    I64(Vec<i64>),
}

impl Column {
    #[inline]
    pub fn as_i64(&self) -> &[i64] {
        match self {
            Column::I64(v) => v,
        }
    }
    pub fn len(&self) -> usize {
        match self {
            Column::I64(v) => v.len(),
        }
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// A horizontal partition of a table: one block = one morsel.
#[derive(Clone)]
pub struct Block {
    pub columns: Vec<Column>,
    pub rows: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CmpOp {
    Gt,
    Lt,
    Ge,
    Le,
    Eq,
}

impl CmpOp {
    pub fn to_u8(self) -> u8 {
        match self {
            CmpOp::Gt => 0,
            CmpOp::Lt => 1,
            CmpOp::Ge => 2,
            CmpOp::Le => 3,
            CmpOp::Eq => 4,
        }
    }
    pub fn from_u8(b: u8) -> Self {
        match b {
            0 => CmpOp::Gt,
            1 => CmpOp::Lt,
            2 => CmpOp::Ge,
            3 => CmpOp::Le,
            _ => CmpOp::Eq,
        }
    }
}

/// Branchless vectorized filter: emits a selection vector.
/// The body compiles to a compare + masked index store; no branches in the
/// loop, so throughput is ~1 row/cycle/lane.
pub fn filter_i64(data: &[i64], op: CmpOp, lit: i64) -> Vec<u32> {
    let n = data.len();
    let mut out = vec![0u32; n];
    let mut k = 0usize;
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
        CmpOp::Gt => run!(|v| v > lit),
        CmpOp::Lt => run!(|v| v < lit),
        CmpOp::Ge => run!(|v| v >= lit),
        CmpOp::Le => run!(|v| v <= lit),
        CmpOp::Eq => run!(|v| v == lit),
    }
    out.truncate(k);
    out
}

/// One scalar aggregate accumulator (SUM/COUNT/MIN/MAX in one pass).
#[derive(Clone, Copy, Debug)]
pub struct Acc {
    pub sum: i64,
    pub count: u64,
    pub min: i64,
    pub max: i64,
}

impl Default for Acc {
    fn default() -> Self {
        Acc { sum: 0, count: 0, min: i64::MAX, max: i64::MIN }
    }
}

impl Acc {
    #[inline]
    pub fn push(&mut self, v: i64) {
        self.sum = self.sum.wrapping_add(v);
        self.count += 1;
        self.min = self.min.min(v);
        self.max = self.max.max(v);
    }
    pub fn merge(&mut self, o: &Acc) {
        self.sum = self.sum.wrapping_add(o.sum);
        self.count += o.count;
        self.min = self.min.min(o.min);
        self.max = self.max.max(o.max);
    }
}

/// Full-column sum with 8 independent accumulators (vectorizes cleanly).
pub fn sum_i64(data: &[i64]) -> Acc {
    let mut lanes = [0i64; 8];
    let chunks = data.chunks_exact(8);
    let rem = chunks.remainder();
    let mut min = i64::MAX;
    let mut max = i64::MIN;
    for c in chunks {
        for j in 0..8 {
            lanes[j] = lanes[j].wrapping_add(c[j]);
            min = min.min(c[j]);
            max = max.max(c[j]);
        }
    }
    let mut acc = Acc {
        sum: lanes.iter().fold(0i64, |a, &b| a.wrapping_add(b)),
        count: data.len() as u64,
        min,
        max,
    };
    for &v in rem {
        acc.sum = acc.sum.wrapping_add(v);
        acc.min = acc.min.min(v);
        acc.max = acc.max.max(v);
    }
    if data.is_empty() {
        acc.min = i64::MAX;
        acc.max = i64::MIN;
    }
    acc
}

/// Gather-aggregate through a selection vector.
pub fn sum_i64_sel(data: &[i64], sel: &[u32]) -> Acc {
    let mut acc = Acc::default();
    for &i in sel {
        acc.push(unsafe { *data.get_unchecked(i as usize) });
    }
    acc
}

/// Grouped aggregation. For small key domains (the common case after
/// dictionary encoding) we use a flat array fast path; otherwise a hash map.
pub fn group_agg(
    keys: &[i64],
    vals: &[i64],
    sel: Option<&[u32]>,
) -> std::collections::HashMap<i64, Acc> {
    use std::collections::HashMap;
    // Fast path: probe key range on a sample.
    let mut lo = i64::MAX;
    let mut hi = i64::MIN;
    for &k in keys.iter().take(1024) {
        lo = lo.min(k);
        hi = hi.max(k);
    }
    let flat_ok = lo >= 0 && hi < 4096;
    if flat_ok {
        let mut table = vec![Acc::default(); 4096];
        let mut seen = vec![false; 4096];
        let mut emit = |i: usize| {
            let k = unsafe { *keys.get_unchecked(i) };
            if (0..4096).contains(&k) {
                let s = k as usize;
                table[s].push(unsafe { *vals.get_unchecked(i) });
                seen[s] = true;
                true
            } else {
                false
            }
        };
        let mut fell_back = false;
        match sel {
            Some(s) => {
                for &i in s {
                    if !emit(i as usize) {
                        fell_back = true;
                        break;
                    }
                }
            }
            None => {
                for i in 0..keys.len() {
                    if !emit(i) {
                        fell_back = true;
                        break;
                    }
                }
            }
        }
        if !fell_back {
            let mut out = HashMap::new();
            for k in 0..4096 {
                if seen[k] {
                    out.insert(k as i64, table[k]);
                }
            }
            return out;
        }
    }
    // General path.
    let mut out: HashMap<i64, Acc> = HashMap::new();
    match sel {
        Some(s) => {
            for &i in s {
                out.entry(keys[i as usize]).or_default().push(vals[i as usize]);
            }
        }
        None => {
            for i in 0..keys.len() {
                out.entry(keys[i]).or_default().push(vals[i]);
            }
        }
    }
    out
}
