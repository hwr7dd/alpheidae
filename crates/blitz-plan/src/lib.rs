//! blitz-plan — SQL front end and cost-based optimizer.
//!
//! Passes, in order:
//!   1. parse        → AST
//!   2. resolve      → bind names against Iceberg schemas (table, field id)
//!   3. pushdown     → single-table predicates pushed into their scans
//!   4. prune        → projection pruning: scans read only referenced columns
//!                     (this is what makes late materialization bite)
//!   5. estimate     → per-scan cardinality from Iceberg manifest stats
//!                     (record counts + bounds → range-predicate selectivity)
//!   6. join strategy→ smaller estimated side becomes the hash build side;
//!                     BROADCAST if est build bytes < threshold, else SHUFFLE
//!                     (partitioned hash join through shared-storage scratch)
//!   7. agg split    → partial aggregation below the exchange, final above
//!
//! The optimizer's decisions are visible via `explain()`.

use blitz_format::{CmpOp, DataType, Literal};
use blitz_iceberg::{de_long, DataFile, TableMeta};

// ---------------------------------------------------------------------------
// AST
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AggFn {
    Sum,
    Count,
    Min,
    Max,
    Avg,
}

#[derive(Clone, Debug)]
pub enum SelectItem {
    Col(String, String),          // (qualifier-or-empty, name)
    Agg(AggFn, String, String),   // (fn, qualifier, name) — COUNT(*) = ("", "*")
}

#[derive(Clone, Debug)]
pub struct AstPred {
    pub qual: String,
    pub col: String,
    pub op: CmpOp,
    pub lit: Literal,
}

#[derive(Clone, Debug)]
pub struct AstJoin {
    pub table: String,
    pub alias: String,
    pub lqual: String,
    pub lcol: String,
    pub rqual: String,
    pub rcol: String,
}

#[derive(Clone, Debug)]
pub struct Ast {
    pub select: Vec<SelectItem>,
    pub table: String,
    pub alias: String,
    pub joins: Vec<AstJoin>,
    pub preds: Vec<AstPred>,
    pub group_by: Vec<(String, String)>,
    pub order_by: Option<(usize, bool)>, // (select position, desc)
    pub limit: Option<usize>,
}

// ---------------------------------------------------------------------------
// Tokenizer + parser
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq)]
enum Tok {
    Ident(String),
    Num(i64),
    Str(String),
    Sym(char),
    Op(String),
}

fn lex(sql: &str) -> Vec<Tok> {
    let mut out = vec![];
    let b: Vec<char> = sql.chars().collect();
    let mut i = 0;
    while i < b.len() {
        let c = b[i];
        if c.is_whitespace() {
            i += 1;
        } else if c == '\'' {
            let mut s = String::new();
            i += 1;
            while i < b.len() && b[i] != '\'' {
                s.push(b[i]);
                i += 1;
            }
            i += 1;
            out.push(Tok::Str(s));
        } else if c.is_ascii_digit() || (c == '-' && i + 1 < b.len() && b[i + 1].is_ascii_digit()) {
            let mut s = String::new();
            s.push(c);
            i += 1;
            while i < b.len() && b[i].is_ascii_digit() {
                s.push(b[i]);
                i += 1;
            }
            out.push(Tok::Num(s.parse().unwrap()));
        } else if c.is_alphabetic() || c == '_' || c == '*' {
            let mut s = String::new();
            while i < b.len() && (b[i].is_alphanumeric() || b[i] == '_' || b[i] == '*') {
                s.push(b[i]);
                i += 1;
            }
            out.push(Tok::Ident(s));
        } else if c == '>' || c == '<' || c == '=' || c == '!' {
            let mut s = String::new();
            s.push(c);
            i += 1;
            if i < b.len() && b[i] == '=' {
                s.push('=');
                i += 1;
            }
            out.push(Tok::Op(s));
        } else {
            out.push(Tok::Sym(c));
            i += 1;
        }
    }
    out
}

struct P {
    t: Vec<Tok>,
    i: usize,
}

impl P {
    fn peek(&self) -> Option<&Tok> {
        self.t.get(self.i)
    }
    fn next(&mut self) -> Option<Tok> {
        let t = self.t.get(self.i).cloned();
        self.i += 1;
        t
    }
    fn kw(&mut self, w: &str) -> bool {
        if let Some(Tok::Ident(s)) = self.peek() {
            if s.eq_ignore_ascii_case(w) {
                self.i += 1;
                return true;
            }
        }
        false
    }
    fn ident(&mut self) -> Result<String, String> {
        match self.next() {
            Some(Tok::Ident(s)) => Ok(s),
            t => Err(format!("expected identifier, got {t:?}")),
        }
    }
    fn sym(&mut self, c: char) -> Result<(), String> {
        match self.next() {
            Some(Tok::Sym(s)) if s == c => Ok(()),
            t => Err(format!("expected '{c}', got {t:?}")),
        }
    }
    /// qualified name: a.b or b
    fn qual(&mut self) -> Result<(String, String), String> {
        let a = self.ident()?;
        if let Some(Tok::Sym('.')) = self.peek() {
            self.i += 1;
            let b = self.ident()?;
            Ok((a, b))
        } else {
            Ok((String::new(), a))
        }
    }
}

const KEYWORDS: &[&str] = &["FROM", "JOIN", "ON", "WHERE", "GROUP", "ORDER", "LIMIT", "AND", "BY", "DESC", "ASC", "AS", "SELECT", "INNER"];

fn is_kw(s: &str) -> bool {
    KEYWORDS.iter().any(|k| s.eq_ignore_ascii_case(k))
}

pub fn parse(sql: &str) -> Result<Ast, String> {
    let mut p = P { t: lex(sql.trim().trim_end_matches(';')), i: 0 };
    if !p.kw("SELECT") {
        return Err("expected SELECT".into());
    }
    let mut select = vec![];
    loop {
        let agg = match p.peek() {
            Some(Tok::Ident(s)) => match s.to_uppercase().as_str() {
                "SUM" => Some(AggFn::Sum),
                "COUNT" => Some(AggFn::Count),
                "MIN" => Some(AggFn::Min),
                "MAX" => Some(AggFn::Max),
                "AVG" => Some(AggFn::Avg),
                _ => None,
            },
            _ => None,
        };
        if let Some(f) = agg {
            p.i += 1;
            p.sym('(')?;
            let (q, c) = p.qual()?;
            p.sym(')')?;
            select.push(SelectItem::Agg(f, q, c));
        } else {
            let (q, c) = p.qual()?;
            select.push(SelectItem::Col(q, c));
        }
        if let Some(Tok::Sym(',')) = p.peek() {
            p.i += 1;
        } else {
            break;
        }
    }
    if !p.kw("FROM") {
        return Err("expected FROM".into());
    }
    let table = p.ident()?;
    let alias = match p.peek() {
        Some(Tok::Ident(s)) if !is_kw(s) => p.ident()?,
        _ => table.clone(),
    };
    let mut joins = vec![];
    loop {
        let _ = p.kw("INNER");
        if !p.kw("JOIN") {
            break;
        }
        let jt = p.ident()?;
        let ja = match p.peek() {
            Some(Tok::Ident(s)) if !is_kw(s) => p.ident()?,
            _ => jt.clone(),
        };
        if !p.kw("ON") {
            return Err("expected ON".into());
        }
        let (lq, lc) = p.qual()?;
        match p.next() {
            Some(Tok::Op(o)) if o == "=" => {}
            t => return Err(format!("expected '=' in join, got {t:?}")),
        }
        let (rq, rc) = p.qual()?;
        joins.push(AstJoin { table: jt, alias: ja, lqual: lq, lcol: lc, rqual: rq, rcol: rc });
    }
    let mut preds = vec![];
    if p.kw("WHERE") {
        loop {
            let (q, c) = p.qual()?;
            let op = match p.next() {
                Some(Tok::Op(o)) => match o.as_str() {
                    ">" => CmpOp::Gt,
                    "<" => CmpOp::Lt,
                    ">=" => CmpOp::Ge,
                    "<=" => CmpOp::Le,
                    "=" | "==" => CmpOp::Eq,
                    o => return Err(format!("bad op {o}")),
                },
                t => return Err(format!("expected op, got {t:?}")),
            };
            let lit = match p.next() {
                Some(Tok::Num(n)) => Literal::Int(n),
                Some(Tok::Str(s)) => Literal::Str(s),
                t => return Err(format!("expected literal, got {t:?}")),
            };
            preds.push(AstPred { qual: q, col: c, op, lit });
            if !p.kw("AND") {
                break;
            }
        }
    }
    let mut group_by = vec![];
    if p.kw("GROUP") {
        if !p.kw("BY") {
            return Err("expected BY".into());
        }
        loop {
            group_by.push(p.qual()?);
            if let Some(Tok::Sym(',')) = p.peek() {
                p.i += 1;
            } else {
                break;
            }
        }
    }
    let mut order_by = None;
    if p.kw("ORDER") {
        if !p.kw("BY") {
            return Err("expected BY".into());
        }
        let pos = match p.next() {
            Some(Tok::Num(n)) => (n as usize).saturating_sub(1),
            Some(Tok::Ident(name)) => select
                .iter()
                .position(|it| match it {
                    SelectItem::Col(_, c) => *c == name,
                    SelectItem::Agg(_, _, c) => *c == name,
                })
                .ok_or(format!("ORDER BY column {name} not in select list"))?,
            t => return Err(format!("bad ORDER BY {t:?}")),
        };
        let desc = if p.kw("DESC") { true } else { !p.kw("ASC") && false };
        order_by = Some((pos, desc));
    }
    let mut limit = None;
    if p.kw("LIMIT") {
        match p.next() {
            Some(Tok::Num(n)) => limit = Some(n as usize),
            t => return Err(format!("bad LIMIT {t:?}")),
        }
    }
    Ok(Ast { select, table, alias, joins, preds, group_by, order_by, limit })
}

// ---------------------------------------------------------------------------
// Resolved / optimized physical plan
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JoinStrategy {
    Broadcast,
    Shuffle { partitions: usize },
}

#[derive(Clone, Debug)]
pub struct BoundPred {
    pub col_idx: usize, // index in the table's schema
    pub field_id: i32,
    pub op: CmpOp,
    pub lit: Literal,
}

#[derive(Clone, Debug)]
pub struct ScanNode {
    pub table: String,
    pub alias: String,
    pub meta: TableMeta,
    /// columns this scan must produce, as (schema index, name, type)
    pub cols: Vec<(usize, String, DataType)>,
    pub preds: Vec<BoundPred>,
    pub est_rows: f64,
    pub total_rows: f64,
    pub files: Vec<DataFile>,
    pub files_pruned: usize,
}

#[derive(Clone, Debug)]
pub struct JoinNode {
    /// index into `scans` for build / probe sides
    pub build: usize,
    pub probe: usize,
    pub build_key: usize, // position within that scan's `cols`
    pub probe_key: usize,
    pub strategy: JoinStrategy,
}

#[derive(Clone, Debug)]
pub enum OutExpr {
    /// (scan idx, col position within scan.cols)
    Col(usize, usize),
    Agg(AggFn, usize, usize), // fn, scan idx, col position (usize::MAX = COUNT(*))
}

#[derive(Clone, Debug)]
pub struct PhysicalPlan {
    pub scans: Vec<ScanNode>,
    pub join: Option<JoinNode>,
    pub output: Vec<OutExpr>,
    pub group_by: Vec<(usize, usize)>, // (scan idx, col position)
    pub order_by: Option<(usize, bool)>,
    pub limit: Option<usize>,
    pub two_phase_agg: bool,
}

pub const BROADCAST_THRESHOLD_BYTES: f64 = 4.0 * 1024.0 * 1024.0;
pub const EST_ROW_WIDTH: f64 = 24.0;

/// Selectivity of a range predicate from manifest bounds (uniform assumption).
fn selectivity(files: &[DataFile], p: &BoundPred) -> f64 {
    match (&p.lit, p.op) {
        (Literal::Int(l), op) => {
            let (mut lo, mut hi) = (i64::MAX, i64::MIN);
            for f in files {
                if let Some((_, blo, bhi)) = f.bounds.iter().find(|(k, _, _)| *k == p.field_id) {
                    lo = lo.min(de_long(blo));
                    hi = hi.max(de_long(bhi));
                }
            }
            if lo > hi {
                return 1.0;
            }
            let domain = (hi - lo).max(1) as f64;
            match op {
                CmpOp::Gt | CmpOp::Ge => ((hi - l).max(0) as f64 / domain).min(1.0),
                CmpOp::Lt | CmpOp::Le => ((l - lo).max(0) as f64 / domain).min(1.0),
                CmpOp::Eq => 1.0 / domain.min(1000.0),
            }
        }
        (Literal::Str(_), _) => 0.1, // equality on a dict column: heuristic
    }
}

pub struct Catalog<'a> {
    pub load: &'a dyn Fn(&str) -> Option<(TableMeta, Vec<DataFile>)>,
}

pub fn plan(ast: &Ast, catalog: &Catalog) -> Result<PhysicalPlan, String> {
    // ---- bind tables ----
    let mut scans: Vec<ScanNode> = vec![];
    let mut bind_table = |name: &str, alias: &str| -> Result<usize, String> {
        let (meta, files) =
            (catalog.load)(name).ok_or(format!("table {name} not found in catalog"))?;
        let total: i64 = files.iter().map(|f| f.record_count).sum();
        scans.push(ScanNode {
            table: name.into(),
            alias: alias.into(),
            meta,
            cols: vec![],
            preds: vec![],
            est_rows: total as f64,
            total_rows: total as f64,
            files,
            files_pruned: 0,
        });
        Ok(scans.len() - 1)
    };
    bind_table(&ast.table, &ast.alias)?;
    for j in &ast.joins {
        bind_table(&j.table, &j.alias)?;
    }

    // ---- name resolution ----
    let find_scan = |scans: &Vec<ScanNode>, qual: &str, col: &str| -> Result<usize, String> {
        if !qual.is_empty() {
            scans
                .iter()
                .position(|s| s.alias == qual || s.table == qual)
                .ok_or(format!("unknown table alias {qual}"))
        } else {
            let hits: Vec<usize> = scans
                .iter()
                .enumerate()
                .filter(|(_, s)| s.meta.fields.iter().any(|f| f.name == col))
                .map(|(i, _)| i)
                .collect();
            match hits.len() {
                1 => Ok(hits[0]),
                0 => Err(format!("column {col} not found")),
                _ => Err(format!("column {col} is ambiguous; qualify it")),
            }
        }
    };
    let col_meta = |scans: &Vec<ScanNode>, si: usize, col: &str| -> Result<(usize, i32, DataType), String> {
        scans[si]
            .meta
            .fields
            .iter()
            .enumerate()
            .find(|(_, f)| f.name == col)
            .map(|(i, f)| (i, f.id, f.dtype))
            .ok_or(format!("column {col} not in {}", scans[si].table))
    };

    // projection pruning: register needed columns, return position in cols
    fn need_col(scan: &mut ScanNode, idx: usize, name: &str, dt: DataType) -> usize {
        if let Some(p) = scan.cols.iter().position(|(i, _, _)| *i == idx) {
            p
        } else {
            scan.cols.push((idx, name.to_string(), dt));
            scan.cols.len() - 1
        }
    }

    // ---- predicate pushdown (all preds are single-table → push to scans) ----
    for p in &ast.preds {
        let si = find_scan(&scans, &p.qual, &p.col)?;
        let (ci, fid, dt) = col_meta(&scans, si, &p.col)?;
        need_col(&mut scans[si], ci, &p.col, dt);
        scans[si].preds.push(BoundPred { col_idx: ci, field_id: fid, op: p.op, lit: p.lit.clone() });
    }

    // ---- bind join keys / output / group by ----
    let mut join = None;
    if let Some(j) = ast.joins.first() {
        if ast.joins.len() > 1 {
            return Err("one join per query in this version".into());
        }
        let lsi = find_scan(&scans, &j.lqual, &j.lcol)?;
        let rsi = find_scan(&scans, &j.rqual, &j.rcol)?;
        let (lci, _, ldt) = col_meta(&scans, lsi, &j.lcol)?;
        let (rci, _, rdt) = col_meta(&scans, rsi, &j.rcol)?;
        if ldt != DataType::Int64 || rdt != DataType::Int64 {
            return Err("join keys must be Int64".into());
        }
        let lpos = need_col(&mut scans[lsi], lci, &j.lcol, ldt);
        let rpos = need_col(&mut scans[rsi], rci, &j.rcol, rdt);
        join = Some((lsi, lpos, rsi, rpos));
    }

    let mut output = vec![];
    let mut has_agg = false;
    for it in &ast.select {
        match it {
            SelectItem::Col(q, c) => {
                let si = find_scan(&scans, q, c)?;
                let (ci, _, dt) = col_meta(&scans, si, c)?;
                let pos = need_col(&mut scans[si], ci, c, dt);
                output.push(OutExpr::Col(si, pos));
            }
            SelectItem::Agg(f, q, c) => {
                has_agg = true;
                if c == "*" {
                    output.push(OutExpr::Agg(*f, 0, usize::MAX));
                } else {
                    let si = find_scan(&scans, q, c)?;
                    let (ci, _, dt) = col_meta(&scans, si, c)?;
                    if dt != DataType::Int64 {
                        return Err("aggregates require Int64 columns".into());
                    }
                    let pos = need_col(&mut scans[si], ci, c, dt);
                    output.push(OutExpr::Agg(*f, si, pos));
                }
            }
        }
    }
    let mut group_by = vec![];
    for (q, c) in &ast.group_by {
        let si = find_scan(&scans, q, c)?;
        let (ci, _, dt) = col_meta(&scans, si, c)?;
        let pos = need_col(&mut scans[si], ci, c, dt);
        group_by.push((si, pos));
    }

    // ---- file pruning + cardinality estimation per scan ----
    for s in scans.iter_mut() {
        let before = s.files.len();
        s.files.retain(|f| {
            !s.preds.iter().any(|p| {
                let dt = s.meta.fields.iter().find(|fl| fl.id == p.field_id).unwrap().dtype;
                blitz_iceberg::file_prunes(f, p.field_id, dt, p.op, &p.lit)
            })
        });
        s.files_pruned = before - s.files.len();
        let surviving: i64 = s.files.iter().map(|f| f.record_count).sum();
        let mut est = surviving as f64;
        for p in &s.preds {
            est *= selectivity(&s.files, p);
        }
        s.est_rows = est.max(1.0);
    }

    // ---- join strategy: pick build side by estimated size ----
    let join = join.map(|(lsi, lpos, rsi, rpos)| {
        let (build, probe, bk, pk) = if scans[lsi].est_rows <= scans[rsi].est_rows {
            (lsi, rsi, lpos, rpos)
        } else {
            (rsi, lsi, rpos, lpos)
        };
        let build_bytes = scans[build].est_rows * EST_ROW_WIDTH;
        let strategy = if build_bytes < BROADCAST_THRESHOLD_BYTES {
            JoinStrategy::Broadcast
        } else {
            JoinStrategy::Shuffle { partitions: 16 }
        };
        JoinNode { build, probe, build_key: bk, probe_key: pk, strategy }
    });

    Ok(PhysicalPlan {
        scans,
        join,
        output,
        group_by,
        order_by: ast.order_by,
        limit: ast.limit,
        two_phase_agg: has_agg,
    })
}

pub fn explain(p: &PhysicalPlan) -> String {
    let mut s = String::new();
    let mut pad = 0usize;
    let mut line = |s: &mut String, pad: usize, t: String| {
        s.push_str(&" ".repeat(pad));
        s.push_str(&t);
        s.push('\n');
    };
    if let Some(n) = p.limit {
        let ord = p
            .order_by
            .map(|(i, d)| format!(" by output#{}{}", i + 1, if d { " DESC" } else { "" }))
            .unwrap_or_default();
        line(&mut s, pad, format!("TopN(limit={n}{ord})"));
        pad += 2;
    } else if let Some((i, d)) = p.order_by {
        line(&mut s, pad, format!("Sort(output#{}{})", i + 1, if d { " DESC" } else { "" }));
        pad += 2;
    }
    if p.two_phase_agg {
        line(&mut s, pad, "FinalAggregate(merge partials)".into());
        pad += 2;
        line(&mut s, pad, "Exchange(partials -> coordinator)".into());
        pad += 2;
        line(&mut s, pad, "PartialAggregate(per node, per morsel)".into());
        pad += 2;
    }
    if let Some(j) = &p.join {
        let strat = match j.strategy {
            JoinStrategy::Broadcast => format!(
                "BROADCAST (build={} est {:.0} rows < {:.0} KB threshold)",
                p.scans[j.build].table,
                p.scans[j.build].est_rows,
                BROADCAST_THRESHOLD_BYTES / 1024.0
            ),
            JoinStrategy::Shuffle { partitions } => format!(
                "SHUFFLE x{partitions} via shared-storage scratch (build={} est {:.0} rows > threshold)",
                p.scans[j.build].table, p.scans[j.build].est_rows
            ),
        };
        line(&mut s, pad, format!("HashJoin[{strat}]"));
        pad += 2;
    }
    for sc in &p.scans {
        let preds: Vec<String> = sc
            .preds
            .iter()
            .map(|pr| {
                let opn = match pr.op {
                    CmpOp::Gt => ">",
                    CmpOp::Lt => "<",
                    CmpOp::Ge => ">=",
                    CmpOp::Le => "<=",
                    CmpOp::Eq => "=",
                };
                let lit = match &pr.lit {
                    Literal::Int(i) => i.to_string(),
                    Literal::Str(st) => format!("'{st}'"),
                };
                format!("{}{}{}", sc.meta.fields[pr.col_idx].name, opn, lit)
            })
            .collect();
        let cols: Vec<&str> = sc.cols.iter().map(|(_, n, _)| n.as_str()).collect();
        line(
            &mut s,
            pad,
            format!(
                "IcebergScan {} cols[{}]{} | {} files ({} pruned by manifest bounds), est {:.0}/{:.0} rows ({:.1}% sel), late-materialized",
                sc.table,
                cols.join(","),
                if preds.is_empty() { String::new() } else { format!(" pred[{}]", preds.join(" AND ")) },
                sc.files.len(),
                sc.files_pruned,
                sc.est_rows,
                sc.total_rows,
                100.0 * sc.est_rows / sc.total_rows.max(1.0)
            ),
        );
    }
    s
}
