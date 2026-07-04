//! blitz-sql — a deliberately small SQL front end.
//!
//! Grammar:
//!   SELECT <AGG>(cN) FROM t [WHERE cM <op> <int>] [GROUP BY cK]
//!   AGG ∈ { SUM, COUNT, MIN, MAX, AVG }

use blitz_core::CmpOp;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AggFn {
    Sum,
    Count,
    Min,
    Max,
    Avg,
}

impl AggFn {
    pub fn to_u8(self) -> u8 {
        match self {
            AggFn::Sum => 0,
            AggFn::Count => 1,
            AggFn::Min => 2,
            AggFn::Max => 3,
            AggFn::Avg => 4,
        }
    }
    pub fn from_u8(b: u8) -> Self {
        match b {
            0 => AggFn::Sum,
            1 => AggFn::Count,
            2 => AggFn::Min,
            3 => AggFn::Max,
            _ => AggFn::Avg,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Query {
    pub agg: AggFn,
    pub agg_col: usize,
    pub filter: Option<(usize, CmpOp, i64)>,
    pub group_by: Option<usize>,
}

pub fn parse(sql: &str) -> Result<Query, String> {
    let up = sql.trim().trim_end_matches(';').to_uppercase();
    let toks: Vec<&str> = up
        .split(|c: char| c.is_whitespace() || c == '(' || c == ')' || c == ',')
        .filter(|t| !t.is_empty())
        .collect();
    let mut i = 0;
    let expect = |toks: &[&str], i: &mut usize, w: &str| -> Result<(), String> {
        if toks.get(*i).copied() == Some(w) {
            *i += 1;
            Ok(())
        } else {
            Err(format!("expected {w} at token {}", *i))
        }
    };
    let col = |t: Option<&&str>| -> Result<usize, String> {
        let t = t.ok_or("missing column")?;
        t.strip_prefix('C')
            .and_then(|n| n.parse().ok())
            .ok_or_else(|| format!("bad column {t} (use c0..cN)"))
    };

    expect(&toks, &mut i, "SELECT")?;
    let agg = match toks.get(i).copied() {
        Some("SUM") => AggFn::Sum,
        Some("COUNT") => AggFn::Count,
        Some("MIN") => AggFn::Min,
        Some("MAX") => AggFn::Max,
        Some("AVG") => AggFn::Avg,
        t => return Err(format!("unknown aggregate {t:?}")),
    };
    i += 1;
    let agg_col = col(toks.get(i))?;
    i += 1;
    expect(&toks, &mut i, "FROM")?;
    i += 1; // table name

    let mut filter = None;
    let mut group_by = None;
    while i < toks.len() {
        match toks[i] {
            "WHERE" => {
                let c = col(toks.get(i + 1))?;
                let op = match toks.get(i + 2).copied() {
                    Some(">") => CmpOp::Gt,
                    Some("<") => CmpOp::Lt,
                    Some(">=") => CmpOp::Ge,
                    Some("<=") => CmpOp::Le,
                    Some("=") | Some("==") => CmpOp::Eq,
                    t => return Err(format!("bad op {t:?}")),
                };
                let lit: i64 = toks
                    .get(i + 3)
                    .and_then(|t| t.parse().ok())
                    .ok_or("bad literal")?;
                filter = Some((c, op, lit));
                i += 4;
            }
            "GROUP" => {
                expect(&toks, &mut i, "GROUP")?;
                expect(&toks, &mut i, "BY")?;
                group_by = Some(col(toks.get(i))?);
                i += 1;
            }
            t => return Err(format!("unexpected token {t}")),
        }
    }
    Ok(Query { agg, agg_col, filter, group_by })
}
