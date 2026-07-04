//! Minimal JSON — enough for Iceberg `metadata.json` and Avro schema JSON.

use std::fmt::Write as _;

#[derive(Clone, Debug, PartialEq)]
pub enum Json {
    Null,
    Bool(bool),
    Num(f64),
    Str(String),
    Arr(Vec<Json>),
    Obj(Vec<(String, Json)>),
}

impl Json {
    pub fn get(&self, k: &str) -> Option<&Json> {
        match self {
            Json::Obj(o) => o.iter().find(|(n, _)| n == k).map(|(_, v)| v),
            _ => None,
        }
    }
    pub fn s(&self) -> Option<&str> {
        match self {
            Json::Str(s) => Some(s),
            _ => None,
        }
    }
    pub fn i(&self) -> Option<i64> {
        match self {
            Json::Num(n) => Some(*n as i64),
            _ => None,
        }
    }
    pub fn b(&self) -> Option<bool> {
        match self {
            Json::Bool(b) => Some(*b),
            _ => None,
        }
    }
    pub fn arr(&self) -> Option<&[Json]> {
        match self {
            Json::Arr(a) => Some(a),
            _ => None,
        }
    }
    pub fn render(&self) -> String {
        let mut s = String::new();
        self.write(&mut s);
        s
    }
    fn write(&self, out: &mut String) {
        match self {
            Json::Null => out.push_str("null"),
            Json::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
            Json::Num(n) => {
                if n.fract() == 0.0 && n.abs() < 9e15 {
                    let _ = write!(out, "{}", *n as i64);
                } else {
                    let _ = write!(out, "{n}");
                }
            }
            Json::Str(s) => {
                out.push('"');
                for c in s.chars() {
                    match c {
                        '"' => out.push_str("\\\""),
                        '\\' => out.push_str("\\\\"),
                        '\n' => out.push_str("\\n"),
                        '\t' => out.push_str("\\t"),
                        '\r' => out.push_str("\\r"),
                        c if (c as u32) < 0x20 => {
                            let _ = write!(out, "\\u{:04x}", c as u32);
                        }
                        c => out.push(c),
                    }
                }
                out.push('"');
            }
            Json::Arr(a) => {
                out.push('[');
                for (i, v) in a.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    v.write(out);
                }
                out.push(']');
            }
            Json::Obj(o) => {
                out.push('{');
                for (i, (k, v)) in o.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    Json::Str(k.clone()).write(out);
                    out.push(':');
                    v.write(out);
                }
                out.push('}');
            }
        }
    }
}

pub fn obj(fields: Vec<(&str, Json)>) -> Json {
    Json::Obj(fields.into_iter().map(|(k, v)| (k.to_string(), v)).collect())
}

pub fn parse(input: &str) -> Result<Json, String> {
    let b = input.as_bytes();
    let mut p = 0usize;
    let v = parse_value(b, &mut p)?;
    skip_ws(b, &mut p);
    if p != b.len() {
        return Err(format!("trailing data at byte {p}"));
    }
    Ok(v)
}

fn skip_ws(b: &[u8], p: &mut usize) {
    while *p < b.len() && matches!(b[*p], b' ' | b'\t' | b'\n' | b'\r') {
        *p += 1;
    }
}

fn parse_value(b: &[u8], p: &mut usize) -> Result<Json, String> {
    skip_ws(b, p);
    match b.get(*p) {
        Some(b'{') => {
            *p += 1;
            let mut o = vec![];
            skip_ws(b, p);
            if b.get(*p) == Some(&b'}') {
                *p += 1;
                return Ok(Json::Obj(o));
            }
            loop {
                skip_ws(b, p);
                let k = match parse_value(b, p)? {
                    Json::Str(s) => s,
                    _ => return Err("object key must be string".into()),
                };
                skip_ws(b, p);
                if b.get(*p) != Some(&b':') {
                    return Err(format!("expected ':' at {p:?}"));
                }
                *p += 1;
                let v = parse_value(b, p)?;
                o.push((k, v));
                skip_ws(b, p);
                match b.get(*p) {
                    Some(b',') => *p += 1,
                    Some(b'}') => {
                        *p += 1;
                        return Ok(Json::Obj(o));
                    }
                    _ => return Err(format!("expected ',' or '}}' at {p:?}")),
                }
            }
        }
        Some(b'[') => {
            *p += 1;
            let mut a = vec![];
            skip_ws(b, p);
            if b.get(*p) == Some(&b']') {
                *p += 1;
                return Ok(Json::Arr(a));
            }
            loop {
                a.push(parse_value(b, p)?);
                skip_ws(b, p);
                match b.get(*p) {
                    Some(b',') => *p += 1,
                    Some(b']') => {
                        *p += 1;
                        return Ok(Json::Arr(a));
                    }
                    _ => return Err(format!("expected ',' or ']' at {p:?}")),
                }
            }
        }
        Some(b'"') => {
            *p += 1;
            let mut s = String::new();
            loop {
                match b.get(*p) {
                    None => return Err("unterminated string".into()),
                    Some(b'"') => {
                        *p += 1;
                        return Ok(Json::Str(s));
                    }
                    Some(b'\\') => {
                        *p += 1;
                        match b.get(*p) {
                            Some(b'"') => s.push('"'),
                            Some(b'\\') => s.push('\\'),
                            Some(b'/') => s.push('/'),
                            Some(b'n') => s.push('\n'),
                            Some(b't') => s.push('\t'),
                            Some(b'r') => s.push('\r'),
                            Some(b'b') => s.push('\u{8}'),
                            Some(b'f') => s.push('\u{c}'),
                            Some(b'u') => {
                                let hex = std::str::from_utf8(&b[*p + 1..*p + 5])
                                    .map_err(|_| "bad \\u")?;
                                let cp = u32::from_str_radix(hex, 16).map_err(|_| "bad \\u")?;
                                s.push(char::from_u32(cp).unwrap_or('\u{fffd}'));
                                *p += 4;
                            }
                            _ => return Err("bad escape".into()),
                        }
                        *p += 1;
                    }
                    Some(&c) => {
                        // UTF-8 passthrough
                        let start = *p;
                        let len = match c {
                            0x00..=0x7f => 1,
                            0xc0..=0xdf => 2,
                            0xe0..=0xef => 3,
                            _ => 4,
                        };
                        s.push_str(
                            std::str::from_utf8(&b[start..start + len])
                                .map_err(|_| "bad utf8")?,
                        );
                        *p += len;
                    }
                }
            }
        }
        Some(b't') => {
            if b[*p..].starts_with(b"true") {
                *p += 4;
                Ok(Json::Bool(true))
            } else {
                Err("bad literal".into())
            }
        }
        Some(b'f') => {
            if b[*p..].starts_with(b"false") {
                *p += 5;
                Ok(Json::Bool(false))
            } else {
                Err("bad literal".into())
            }
        }
        Some(b'n') => {
            if b[*p..].starts_with(b"null") {
                *p += 4;
                Ok(Json::Null)
            } else {
                Err("bad literal".into())
            }
        }
        Some(_) => {
            let start = *p;
            while *p < b.len()
                && matches!(b[*p], b'0'..=b'9' | b'-' | b'+' | b'.' | b'e' | b'E')
            {
                *p += 1;
            }
            std::str::from_utf8(&b[start..*p])
                .ok()
                .and_then(|s| s.parse::<f64>().ok())
                .map(Json::Num)
                .ok_or_else(|| format!("bad number at {start}"))
        }
        None => Err("unexpected eof".into()),
    }
}
