//! blitz-format — the BlitzCol (`.blitz`) columnar file format.
//!
//! Layout (single file = one Iceberg data file):
//!
//!   "BLZC" u32 version
//!   [page payloads, written row-group by row-group, column by column]
//!   footer (binary):
//!     schema: ncols, per col (name, dtype)
//!     rowgroups: per rg rows, per col chunk:
//!        dtype-specific meta (i64 min/max | utf8 dict offset/len + min/max)
//!        page directory: per page (file_off, comp_len, rows, i64 min/max)
//!   footer_len u32, "BLZC"
//!
//! Encodings: Int64 pages are delta + zigzag-varint, then LZ4. Utf8 columns
//! are dictionary-encoded per chunk (dict block LZ4'd, codes u32-LE LZ4'd).
//!
//! Late materialization: predicates are evaluated against per-page zone maps
//! first; only pages that can contain qualifying rows are decompressed, and
//! non-predicate columns are decoded only for pages that contain selected
//! rows. Global counters expose decoded-vs-on-disk bytes so the win is
//! measurable.

mod lz4;
pub mod parquet;

use std::collections::HashMap;
use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::atomic::{AtomicU64, Ordering};

pub const PAGE_ROWS: usize = 8192;
const MAGIC: &[u8; 4] = b"BLZC";

pub static BYTES_DECODED: AtomicU64 = AtomicU64::new(0);
pub static BYTES_SKIPPED: AtomicU64 = AtomicU64::new(0);
pub static PAGES_DECODED: AtomicU64 = AtomicU64::new(0);
pub static PAGES_SKIPPED: AtomicU64 = AtomicU64::new(0);

pub fn reset_counters() {
    BYTES_DECODED.store(0, Ordering::Relaxed);
    BYTES_SKIPPED.store(0, Ordering::Relaxed);
    PAGES_DECODED.store(0, Ordering::Relaxed);
    PAGES_SKIPPED.store(0, Ordering::Relaxed);
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DataType {
    Int64,
    Utf8,
}

#[derive(Clone, Debug)]
pub enum ColumnData {
    Int64(Vec<i64>),
    Utf8(Vec<String>),
}

impl ColumnData {
    pub fn len(&self) -> usize {
        match self {
            ColumnData::Int64(v) => v.len(),
            ColumnData::Utf8(v) => v.len(),
        }
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub fn as_i64(&self) -> &[i64] {
        match self {
            ColumnData::Int64(v) => v,
            _ => panic!("not i64"),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CmpOp {
    Gt,
    Lt,
    Ge,
    Le,
    Eq,
}

#[derive(Clone, Debug)]
pub enum Literal {
    Int(i64),
    Str(String),
}

// ---------------------------------------------------------------------------
// varint / zigzag / delta
// ---------------------------------------------------------------------------

fn zigzag(n: i64) -> u64 {
    ((n << 1) ^ (n >> 63)) as u64
}
fn unzigzag(n: u64) -> i64 {
    ((n >> 1) as i64) ^ -((n & 1) as i64)
}
fn put_varint(out: &mut Vec<u8>, mut v: u64) {
    loop {
        let b = (v & 0x7f) as u8;
        v >>= 7;
        if v == 0 {
            out.push(b);
            break;
        }
        out.push(b | 0x80);
    }
}
fn get_varint(buf: &[u8], pos: &mut usize) -> u64 {
    let mut v = 0u64;
    let mut shift = 0;
    loop {
        let b = buf[*pos];
        *pos += 1;
        v |= ((b & 0x7f) as u64) << shift;
        if b & 0x80 == 0 {
            return v;
        }
        shift += 7;
    }
}

fn encode_i64_page(vals: &[i64]) -> Vec<u8> {
    let mut raw = Vec::with_capacity(vals.len() * 2);
    let mut prev = 0i64;
    for &v in vals {
        put_varint(&mut raw, zigzag(v.wrapping_sub(prev)));
        prev = v;
    }
    lz4::compress_prepend_size(&raw)
}

fn decode_i64_page(comp: &[u8], rows: usize) -> Vec<i64> {
    let raw = lz4::decompress_size_prepended(comp).expect("lz4");
    let mut out = Vec::with_capacity(rows);
    let mut pos = 0usize;
    let mut prev = 0i64;
    for _ in 0..rows {
        prev = prev.wrapping_add(unzigzag(get_varint(&raw, &mut pos)));
        out.push(prev);
    }
    out
}

// ---------------------------------------------------------------------------
// footer plumbing
// ---------------------------------------------------------------------------

fn ws(out: &mut Vec<u8>, s: &str) {
    out.extend((s.len() as u32).to_le_bytes());
    out.extend(s.as_bytes());
}
fn rs(buf: &[u8], pos: &mut usize) -> String {
    let n = u32::from_le_bytes(buf[*pos..*pos + 4].try_into().unwrap()) as usize;
    *pos += 4;
    let s = String::from_utf8(buf[*pos..*pos + n].to_vec()).unwrap();
    *pos += n;
    s
}
fn w64(out: &mut Vec<u8>, v: u64) {
    out.extend(v.to_le_bytes());
}
fn wi64(out: &mut Vec<u8>, v: i64) {
    out.extend(v.to_le_bytes());
}
fn w32(out: &mut Vec<u8>, v: u32) {
    out.extend(v.to_le_bytes());
}
fn r64(buf: &[u8], pos: &mut usize) -> u64 {
    let v = u64::from_le_bytes(buf[*pos..*pos + 8].try_into().unwrap());
    *pos += 8;
    v
}
fn ri64(buf: &[u8], pos: &mut usize) -> i64 {
    let v = i64::from_le_bytes(buf[*pos..*pos + 8].try_into().unwrap());
    *pos += 8;
    v
}
fn r32(buf: &[u8], pos: &mut usize) -> u32 {
    let v = u32::from_le_bytes(buf[*pos..*pos + 4].try_into().unwrap());
    *pos += 4;
    v
}

#[derive(Clone, Debug)]
pub struct PageMeta {
    pub offset: u64,
    pub comp_len: u32,
    pub rows: u32,
    pub min: i64, // i64 pages only; 0 for utf8 code pages
    pub max: i64,
}

#[derive(Clone, Debug)]
pub struct ChunkMeta {
    pub dtype: DataType,
    pub min_i: i64,
    pub max_i: i64,
    pub min_s: String,
    pub max_s: String,
    pub dict_off: u64,
    pub dict_len: u32,
    pub dict_count: u32,
    pub pages: Vec<PageMeta>,
}

#[derive(Clone, Debug)]
pub struct RowGroupMeta {
    pub rows: usize,
    pub chunks: Vec<ChunkMeta>,
}

#[derive(Clone, Debug)]
pub struct FileMeta {
    pub schema: Vec<(String, DataType)>,
    pub rowgroups: Vec<RowGroupMeta>,
    pub file_size: u64,
}

// ---------------------------------------------------------------------------
// Writer
// ---------------------------------------------------------------------------

pub struct Writer {
    file: std::fs::File,
    pos: u64,
    schema: Vec<(String, DataType)>,
    rowgroups: Vec<RowGroupMeta>,
}

impl Writer {
    pub fn create(path: &std::path::Path, schema: Vec<(String, DataType)>) -> std::io::Result<Self> {
        if let Some(p) = path.parent() {
            std::fs::create_dir_all(p)?;
        }
        let mut file = std::fs::File::create(path)?;
        file.write_all(MAGIC)?;
        file.write_all(&1u32.to_le_bytes())?;
        Ok(Writer { file, pos: 8, schema, rowgroups: vec![] })
    }

    pub fn write_rowgroup(&mut self, cols: &[ColumnData]) -> std::io::Result<()> {
        assert_eq!(cols.len(), self.schema.len());
        let rows = cols[0].len();
        let mut chunks = vec![];
        for (ci, col) in cols.iter().enumerate() {
            let chunk = match col {
                ColumnData::Int64(v) => {
                    assert_eq!(self.schema[ci].1, DataType::Int64);
                    let mut pages = vec![];
                    let (mut cmin, mut cmax) = (i64::MAX, i64::MIN);
                    for p in v.chunks(PAGE_ROWS) {
                        let (mut mn, mut mx) = (i64::MAX, i64::MIN);
                        for &x in p {
                            mn = mn.min(x);
                            mx = mx.max(x);
                        }
                        cmin = cmin.min(mn);
                        cmax = cmax.max(mx);
                        let enc = encode_i64_page(p);
                        pages.push(PageMeta {
                            offset: self.pos,
                            comp_len: enc.len() as u32,
                            rows: p.len() as u32,
                            min: mn,
                            max: mx,
                        });
                        self.file.write_all(&enc)?;
                        self.pos += enc.len() as u64;
                    }
                    ChunkMeta {
                        dtype: DataType::Int64,
                        min_i: cmin,
                        max_i: cmax,
                        min_s: String::new(),
                        max_s: String::new(),
                        dict_off: 0,
                        dict_len: 0,
                        dict_count: 0,
                        pages,
                    }
                }
                ColumnData::Utf8(v) => {
                    assert_eq!(self.schema[ci].1, DataType::Utf8);
                    // chunk-local dictionary
                    let mut dict: Vec<&str> = vec![];
                    let mut ids: HashMap<&str, u32> = HashMap::new();
                    let mut codes: Vec<u32> = Vec::with_capacity(v.len());
                    for s in v {
                        let id = *ids.entry(s.as_str()).or_insert_with(|| {
                            dict.push(s.as_str());
                            (dict.len() - 1) as u32
                        });
                        codes.push(id);
                    }
                    let (min_s, max_s) = {
                        let mut sorted: Vec<&&str> = dict.iter().collect();
                        sorted.sort();
                        (
                            sorted.first().map(|s| s.to_string()).unwrap_or_default(),
                            sorted.last().map(|s| s.to_string()).unwrap_or_default(),
                        )
                    };
                    let mut draw = Vec::new();
                    w32(&mut draw, dict.len() as u32);
                    for s in &dict {
                        ws(&mut draw, s);
                    }
                    let denc = lz4::compress_prepend_size(&draw);
                    let dict_off = self.pos;
                    self.file.write_all(&denc)?;
                    self.pos += denc.len() as u64;
                    let mut pages = vec![];
                    for p in codes.chunks(PAGE_ROWS) {
                        let mut raw = Vec::with_capacity(p.len() * 4);
                        for &c in p {
                            raw.extend(c.to_le_bytes());
                        }
                        let enc = lz4::compress_prepend_size(&raw);
                        pages.push(PageMeta {
                            offset: self.pos,
                            comp_len: enc.len() as u32,
                            rows: p.len() as u32,
                            min: 0,
                            max: 0,
                        });
                        self.file.write_all(&enc)?;
                        self.pos += enc.len() as u64;
                    }
                    ChunkMeta {
                        dtype: DataType::Utf8,
                        min_i: 0,
                        max_i: 0,
                        min_s,
                        max_s,
                        dict_off,
                        dict_len: denc.len() as u32,
                        dict_count: dict.len() as u32,
                        pages,
                    }
                }
            };
            chunks.push(chunk);
        }
        self.rowgroups.push(RowGroupMeta { rows, chunks });
        Ok(())
    }

    pub fn finish(mut self) -> std::io::Result<FileMeta> {
        let mut f = Vec::new();
        w32(&mut f, self.schema.len() as u32);
        for (n, t) in &self.schema {
            ws(&mut f, n);
            f.push(match t {
                DataType::Int64 => 0,
                DataType::Utf8 => 1,
            });
        }
        w32(&mut f, self.rowgroups.len() as u32);
        for rg in &self.rowgroups {
            w64(&mut f, rg.rows as u64);
            for c in &rg.chunks {
                f.push(match c.dtype {
                    DataType::Int64 => 0,
                    DataType::Utf8 => 1,
                });
                wi64(&mut f, c.min_i);
                wi64(&mut f, c.max_i);
                ws(&mut f, &c.min_s);
                ws(&mut f, &c.max_s);
                w64(&mut f, c.dict_off);
                w32(&mut f, c.dict_len);
                w32(&mut f, c.dict_count);
                w32(&mut f, c.pages.len() as u32);
                for p in &c.pages {
                    w64(&mut f, p.offset);
                    w32(&mut f, p.comp_len);
                    w32(&mut f, p.rows);
                    wi64(&mut f, p.min);
                    wi64(&mut f, p.max);
                }
            }
        }
        self.file.write_all(&f)?;
        self.file.write_all(&(f.len() as u32).to_le_bytes())?;
        self.file.write_all(MAGIC)?;
        let file_size = self.pos + f.len() as u64 + 8;
        self.file.flush()?;
        Ok(FileMeta { schema: self.schema, rowgroups: self.rowgroups, file_size })
    }
}

// ---------------------------------------------------------------------------
// Reader (late-materializing)
// ---------------------------------------------------------------------------

pub struct Reader {
    file: std::fs::File,
    pub meta: FileMeta,
}

impl Reader {
    pub fn open(path: &std::path::Path) -> std::io::Result<Self> {
        let mut file = std::fs::File::open(path)?;
        let size = file.metadata()?.len();
        file.seek(SeekFrom::Start(size - 8))?;
        let mut tail = [0u8; 8];
        file.read_exact(&mut tail)?;
        assert_eq!(&tail[4..], MAGIC, "bad magic");
        let flen = u32::from_le_bytes(tail[..4].try_into().unwrap()) as u64;
        file.seek(SeekFrom::Start(size - 8 - flen))?;
        let mut f = vec![0u8; flen as usize];
        file.read_exact(&mut f)?;
        let mut pos = 0usize;
        let ncols = r32(&f, &mut pos) as usize;
        let mut schema = vec![];
        for _ in 0..ncols {
            let n = rs(&f, &mut pos);
            let t = if f[pos] == 0 { DataType::Int64 } else { DataType::Utf8 };
            pos += 1;
            schema.push((n, t));
        }
        let nrg = r32(&f, &mut pos) as usize;
        let mut rowgroups = vec![];
        for _ in 0..nrg {
            let rows = r64(&f, &mut pos) as usize;
            let mut chunks = vec![];
            for _ in 0..ncols {
                let dtype = if f[pos] == 0 { DataType::Int64 } else { DataType::Utf8 };
                pos += 1;
                let min_i = ri64(&f, &mut pos);
                let max_i = ri64(&f, &mut pos);
                let min_s = rs(&f, &mut pos);
                let max_s = rs(&f, &mut pos);
                let dict_off = r64(&f, &mut pos);
                let dict_len = r32(&f, &mut pos);
                let dict_count = r32(&f, &mut pos);
                let np = r32(&f, &mut pos) as usize;
                let mut pages = vec![];
                for _ in 0..np {
                    pages.push(PageMeta {
                        offset: r64(&f, &mut pos),
                        comp_len: r32(&f, &mut pos),
                        rows: r32(&f, &mut pos),
                        min: ri64(&f, &mut pos),
                        max: ri64(&f, &mut pos),
                    });
                }
                chunks.push(ChunkMeta {
                    dtype, min_i, max_i, min_s, max_s, dict_off, dict_len, dict_count, pages,
                });
            }
            rowgroups.push(RowGroupMeta { rows, chunks });
        }
        Ok(Reader { file, meta: FileMeta { schema, rowgroups, file_size: size } })
    }

    fn read_at(&mut self, off: u64, len: usize) -> Vec<u8> {
        let mut buf = vec![0u8; len];
        self.file.seek(SeekFrom::Start(off)).unwrap();
        self.file.read_exact(&mut buf).unwrap();
        buf
    }

    fn read_dict(&mut self, c: &ChunkMeta) -> Vec<String> {
        let comp = self.read_at(c.dict_off, c.dict_len as usize);
        let raw = lz4::decompress_size_prepended(&comp).unwrap();
        let mut pos = 0usize;
        let n = r32(&raw, &mut pos) as usize;
        (0..n).map(|_| rs(&raw, &mut pos)).collect()
    }

    fn pred_page_prunes(p: &PageMeta, op: CmpOp, lit: i64) -> bool {
        match op {
            CmpOp::Gt => p.max <= lit,
            CmpOp::Ge => p.max < lit,
            CmpOp::Lt => p.min >= lit,
            CmpOp::Le => p.min > lit,
            CmpOp::Eq => lit < p.min || lit > p.max,
        }
    }

    /// Evaluate one predicate over one rowgroup; returns selected row indices
    /// (relative to the rowgroup). Pages whose zone maps prove no match are
    /// never decompressed.
    pub fn eval_pred(&mut self, rg: usize, col: usize, op: CmpOp, lit: &Literal) -> Vec<u32> {
        let chunk = self.meta.rowgroups[rg].chunks[col].clone();
        let mut sel = vec![];
        match (&chunk.dtype, lit) {
            (DataType::Int64, Literal::Int(l)) => {
                let mut base = 0u32;
                for p in &chunk.pages {
                    if Self::pred_page_prunes(p, op, *l) {
                        PAGES_SKIPPED.fetch_add(1, Ordering::Relaxed);
                        BYTES_SKIPPED.fetch_add(p.comp_len as u64, Ordering::Relaxed);
                    } else {
                        let comp = self.read_at(p.offset, p.comp_len as usize);
                        PAGES_DECODED.fetch_add(1, Ordering::Relaxed);
                        BYTES_DECODED.fetch_add(p.comp_len as u64, Ordering::Relaxed);
                        let vals = decode_i64_page(&comp, p.rows as usize);
                        for (i, &v) in vals.iter().enumerate() {
                            let hit = match op {
                                CmpOp::Gt => v > *l,
                                CmpOp::Lt => v < *l,
                                CmpOp::Ge => v >= *l,
                                CmpOp::Le => v <= *l,
                                CmpOp::Eq => v == *l,
                            };
                            if hit {
                                sel.push(base + i as u32);
                            }
                        }
                    }
                    base += p.rows;
                }
            }
            (DataType::Utf8, Literal::Str(s)) => {
                assert_eq!(op, CmpOp::Eq, "utf8 predicates: equality only");
                // Decode dict once; if the value isn't in the dict, the whole
                // chunk prunes without touching code pages.
                let dict = self.read_dict(&chunk);
                let target = dict.iter().position(|d| d == s).map(|i| i as u32);
                let Some(code) = target else {
                    for p in &chunk.pages {
                        PAGES_SKIPPED.fetch_add(1, Ordering::Relaxed);
                        BYTES_SKIPPED.fetch_add(p.comp_len as u64, Ordering::Relaxed);
                    }
                    return sel;
                };
                let mut base = 0u32;
                for p in &chunk.pages {
                    let comp = self.read_at(p.offset, p.comp_len as usize);
                    PAGES_DECODED.fetch_add(1, Ordering::Relaxed);
                    BYTES_DECODED.fetch_add(p.comp_len as u64, Ordering::Relaxed);
                    let raw = lz4::decompress_size_prepended(&comp).unwrap();
                    for (i, c) in raw.chunks_exact(4).enumerate() {
                        if u32::from_le_bytes(c.try_into().unwrap()) == code {
                            sel.push(base + i as u32);
                        }
                    }
                    base += p.rows;
                }
            }
            _ => panic!("type mismatch in predicate"),
        }
        sel
    }

    /// Late-materialized gather: decode only the pages that contain selected
    /// rows, returning values aligned with `sel` order.
    pub fn gather(&mut self, rg: usize, col: usize, sel: &[u32]) -> ColumnData {
        let chunk = self.meta.rowgroups[rg].chunks[col].clone();
        match chunk.dtype {
            DataType::Int64 => {
                let mut out = Vec::with_capacity(sel.len());
                let mut si = 0usize;
                let mut base = 0u32;
                for p in &chunk.pages {
                    let end = base + p.rows;
                    // rows of this page that are selected
                    let start_si = si;
                    while si < sel.len() && sel[si] < end {
                        si += 1;
                    }
                    if si > start_si {
                        let comp = self.read_at(p.offset, p.comp_len as usize);
                        PAGES_DECODED.fetch_add(1, Ordering::Relaxed);
                        BYTES_DECODED.fetch_add(p.comp_len as u64, Ordering::Relaxed);
                        let vals = decode_i64_page(&comp, p.rows as usize);
                        for &s in &sel[start_si..si] {
                            out.push(vals[(s - base) as usize]);
                        }
                    } else {
                        PAGES_SKIPPED.fetch_add(1, Ordering::Relaxed);
                        BYTES_SKIPPED.fetch_add(p.comp_len as u64, Ordering::Relaxed);
                    }
                    base = end;
                }
                ColumnData::Int64(out)
            }
            DataType::Utf8 => {
                let dict = self.read_dict(&chunk);
                let mut out = Vec::with_capacity(sel.len());
                let mut si = 0usize;
                let mut base = 0u32;
                for p in &chunk.pages {
                    let end = base + p.rows;
                    let start_si = si;
                    while si < sel.len() && sel[si] < end {
                        si += 1;
                    }
                    if si > start_si {
                        let comp = self.read_at(p.offset, p.comp_len as usize);
                        PAGES_DECODED.fetch_add(1, Ordering::Relaxed);
                        BYTES_DECODED.fetch_add(p.comp_len as u64, Ordering::Relaxed);
                        let raw = lz4::decompress_size_prepended(&comp).unwrap();
                        for &s in &sel[start_si..si] {
                            let i = (s - base) as usize;
                            let code =
                                u32::from_le_bytes(raw[i * 4..i * 4 + 4].try_into().unwrap());
                            out.push(dict[code as usize].clone());
                        }
                    } else {
                        PAGES_SKIPPED.fetch_add(1, Ordering::Relaxed);
                        BYTES_SKIPPED.fetch_add(p.comp_len as u64, Ordering::Relaxed);
                    }
                    base = end;
                }
                ColumnData::Utf8(out)
            }
        }
    }

    /// Full-chunk read (no selection): scan path for unfiltered columns.
    pub fn read_chunk(&mut self, rg: usize, col: usize) -> ColumnData {
        let rows = self.meta.rowgroups[rg].rows;
        let sel: Vec<u32> = (0..rows as u32).collect();
        self.gather(rg, col, &sel)
    }

    /// Rowgroup-level zone check.
    pub fn rg_prunes(&self, rg: usize, col: usize, op: CmpOp, lit: &Literal) -> bool {
        let c = &self.meta.rowgroups[rg].chunks[col];
        match (c.dtype, lit) {
            (DataType::Int64, Literal::Int(l)) => match op {
                CmpOp::Gt => c.max_i <= *l,
                CmpOp::Ge => c.max_i < *l,
                CmpOp::Lt => c.min_i >= *l,
                CmpOp::Le => c.min_i > *l,
                CmpOp::Eq => *l < c.min_i || *l > c.max_i,
            },
            (DataType::Utf8, Literal::Str(s)) => {
                op == CmpOp::Eq && (s.as_str() < c.min_s.as_str() || s.as_str() > c.max_s.as_str())
            }
            _ => false,
        }
    }
}
