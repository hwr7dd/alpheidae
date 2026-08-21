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

/// Table source: can be a named table, CTE reference, or subquery.
#[derive(Clone, Debug)]
pub enum TableSource {
    Table(String),
    Subquery(Box<Query>, String), // (subquery, alias)
    CTE(String),                   // CTE reference by name
}

#[derive(Clone, Debug)]
pub struct Query {
    pub agg: AggFn,
    pub agg_col: usize,
    pub table: TableSource,
    pub filter: Option<(usize, CmpOp, i64)>,
    pub group_by: Option<usize>,
    pub ctes: Vec<(String, Box<Query>)>,
    /// ORDER BY column index (cN), true = ASC.
    pub order_by: Option<(usize, bool)>,
    pub limit: Option<usize>,
}

/// Parse WITH clause (if present) with recursive subquery support.
fn parse_with_clause(
    toks: &[&str],
    i: &mut usize,
) -> Result<Vec<(String, Box<Query>)>, String> {
    let mut ctes = Vec::new();

    if toks.get(*i).copied() != Some("WITH") {
        return Ok(ctes);
    }

    *i += 1; // skip WITH

    loop {
        // CTE name
        let cte_name = toks
            .get(*i)
            .copied()
            .ok_or("missing CTE name")?
            .to_string();
        *i += 1;

        if toks.get(*i).copied() != Some("AS") {
            return Err("expected AS in CTE".to_string());
        }
        *i += 1;

        // Parse subquery: collect tokens until matching close paren
        if toks.get(*i).copied() != Some("(") {
            return Err("expected ( in CTE".to_string());
        }
        *i += 1;

        let mut depth = 1;
        let start = *i;
        while *i < toks.len() && depth > 0 {
            if toks[*i] == "(" {
                depth += 1;
            } else if toks[*i] == ")" {
                depth -= 1;
            }
            if depth > 0 {
                *i += 1;
            }
        }

        if depth != 0 {
            return Err("mismatched parens in CTE".to_string());
        }

        // Recursively parse the subquery
        let subquery_toks = &toks[start..*i];
        let subquery = parse_subquery_tokens(subquery_toks)?;

        ctes.push((cte_name, Box::new(subquery)));

        *i += 1; // skip closing paren

        if toks.get(*i).copied() == Some(",") {
            *i += 1;
        } else {
            break;
        }
    }

    Ok(ctes)
}

/// Parse a subquery (SELECT ...) from its token slice
fn parse_subquery_tokens(toks: &[&str]) -> Result<Query, String> {
    let mut i = 0;
    parse_inner_query(toks, &mut i)
}

pub fn parse(sql: &str) -> Result<Query, String> {
    let up = sql.trim().trim_end_matches(';').to_uppercase();
    let toks: Vec<&str> = up
        .split(|c: char| c.is_whitespace() || c == '(' || c == ')' || c == ',')
        .filter(|t| !t.is_empty())
        .collect();
    let mut i = 0;
    parse_inner_query(&toks, &mut i)
}

fn parse_inner_query(toks: &[&str], i: &mut usize) -> Result<Query, String> {
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
            .ok_or_else(|| format!("bad column {t}"))
    };

    // Parse WITH clause
    let ctes = parse_with_clause(toks, i)?;

    expect(toks, i, "SELECT")?;
    let agg = match toks.get(*i).copied() {
        Some("SUM") => AggFn::Sum,
        Some("COUNT") => AggFn::Count,
        Some("MIN") => AggFn::Min,
        Some("MAX") => AggFn::Max,
        Some("AVG") => AggFn::Avg,
        t => return Err(format!("unknown agg {t:?}")),
    };
    *i += 1;
    let agg_col = col(toks.get(*i))?;
    *i += 1;
    expect(toks, i, "FROM")?;

    // Parse table source
    let table = match toks.get(*i).copied() {
        Some("(") => {
            // Subquery
            *i += 1;
            let mut depth = 1;
            let start = *i;
            while *i < toks.len() && depth > 0 {
                if toks[*i] == "(" {
                    depth += 1;
                } else if toks[*i] == ")" {
                    depth -= 1;
                }
                if depth > 0 {
                    *i += 1;
                }
            }
            let subq_toks = &toks[start..*i];
            let subq = parse_subquery_tokens(subq_toks)?;
            *i += 1; // skip )

            // Parse alias
            let alias = if toks.get(*i).copied() == Some("AS") {
                *i += 1;
                toks.get(*i).copied().ok_or("missing alias")?.to_string()
            } else if *i < toks.len() {
                let name = toks[*i].to_string();
                *i += 1;
                name
            } else {
                "subq".to_string()
            };

            TableSource::Subquery(Box::new(subq), alias)
        }
        Some(name) => {
            *i += 1;
            if ctes.iter().any(|(n, _)| n == name) {
                TableSource::CTE(name.to_string())
            } else {
                TableSource::Table(name.to_string())
            }
        }
        None => return Err("missing table".to_string()),
    };

    let mut filter = None;
    let mut group_by = None;
    let mut order_by = None;
    let mut limit = None;
    while *i < toks.len() {
        match toks[*i] {
            "WHERE" => {
                let c = col(toks.get(*i + 1))?;
                let op = match toks.get(*i + 2).copied() {
                    Some(">") => CmpOp::Gt,
                    Some("<") => CmpOp::Lt,
                    Some(">=") => CmpOp::Ge,
                    Some("<=") => CmpOp::Le,
                    Some("=") | Some("==") => CmpOp::Eq,
                    t => return Err(format!("bad op {t:?}")),
                };
                let lit: i64 = toks
                    .get(*i + 3)
                    .and_then(|t| t.parse().ok())
                    .ok_or("bad lit")?;
                filter = Some((c, op, lit));
                *i += 4;
            }
            "GROUP" => {
                expect(toks, i, "GROUP")?;
                expect(toks, i, "BY")?;
                group_by = Some(col(toks.get(*i))?);
                *i += 1;
            }
            "ORDER" => {
                expect(toks, i, "ORDER")?;
                expect(toks, i, "BY")?;
                let c = col(toks.get(*i))?;
                *i += 1;
                let asc = match toks.get(*i).copied() {
                    Some("DESC") => {
                        *i += 1;
                        false
                    }
                    Some("ASC") => {
                        *i += 1;
                        true
                    }
                    _ => true,
                };
                order_by = Some((c, asc));
            }
            "LIMIT" => {
                *i += 1;
                let n: usize = toks
                    .get(*i)
                    .and_then(|t| t.parse().ok())
                    .ok_or("bad LIMIT")?;
                *i += 1;
                limit = Some(n);
            }
            _ => break,
        }
    }
    Ok(Query {
        agg,
        agg_col,
        table,
        filter,
        group_by,
        ctes,
        order_by,
        limit,
    })
}
