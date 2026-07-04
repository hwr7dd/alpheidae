//! Minimal Avro — schema-driven binary codec + object container files.
//! Iceberg manifest lists and manifests are Avro container files; this module
//! reads and writes them (null codec) using the writer schema embedded in the
//! file header, exactly as the Avro spec requires.

use crate::json::{self, Json};
use std::collections::HashMap;

#[derive(Clone, Debug)]
pub enum Schema {
    Null,
    Boolean,
    Int,
    Long,
    Float,
    Double,
    Bytes,
    String,
    Fixed(usize),
    Array(Box<Schema>),
    Map(Box<Schema>),
    Union(Vec<Schema>),
    Record { name: String, fields: Vec<(String, Schema)> },
}

#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    Long(i64), // int + long unified
    Double(f64),
    Bytes(Vec<u8>),
    Str(String),
    Arr(Vec<Value>),
    Map(Vec<(String, Value)>),
    Record(Vec<(String, Value)>),
}

impl Value {
    pub fn field(&self, name: &str) -> Option<&Value> {
        match self {
            Value::Record(f) => f.iter().find(|(n, _)| n == name).map(|(_, v)| v),
            _ => None,
        }
    }
    pub fn long(&self) -> Option<i64> {
        match self {
            Value::Long(v) => Some(*v),
            _ => None,
        }
    }
    pub fn str(&self) -> Option<&str> {
        match self {
            Value::Str(s) => Some(s),
            _ => None,
        }
    }
}

pub fn schema_from_json(j: &Json, reg: &mut HashMap<String, Schema>) -> Result<Schema, String> {
    match j {
        Json::Str(s) => match s.as_str() {
            "null" => Ok(Schema::Null),
            "boolean" => Ok(Schema::Boolean),
            "int" => Ok(Schema::Int),
            "long" => Ok(Schema::Long),
            "float" => Ok(Schema::Float),
            "double" => Ok(Schema::Double),
            "bytes" => Ok(Schema::Bytes),
            "string" => Ok(Schema::String),
            name => reg
                .get(name)
                .cloned()
                .ok_or_else(|| format!("unknown named type {name}")),
        },
        Json::Arr(branches) => Ok(Schema::Union(
            branches
                .iter()
                .map(|b| schema_from_json(b, reg))
                .collect::<Result<_, _>>()?,
        )),
        Json::Obj(_) => {
            let ty = j.get("type").and_then(|t| t.s()).ok_or("missing type")?;
            match ty {
                "record" => {
                    let name = j.get("name").and_then(|n| n.s()).unwrap_or("anon").to_string();
                    let mut fields = vec![];
                    for f in j.get("fields").and_then(|f| f.arr()).ok_or("record w/o fields")? {
                        let fname = f.get("name").and_then(|n| n.s()).ok_or("field w/o name")?;
                        let fty = schema_from_json(f.get("type").ok_or("field w/o type")?, reg)?;
                        fields.push((fname.to_string(), fty));
                    }
                    let rec = Schema::Record { name: name.clone(), fields };
                    reg.insert(name, rec.clone());
                    Ok(rec)
                }
                "array" => Ok(Schema::Array(Box::new(schema_from_json(
                    j.get("items").ok_or_else(|| "array w/o items".to_string())?,
                    reg,
                )?))),
                "map" => Ok(Schema::Map(Box::new(schema_from_json(
                    j.get("values").ok_or_else(|| "map w/o values".to_string())?,
                    reg,
                )?))),
                "fixed" => Ok(Schema::Fixed(
                    j.get("size").and_then(|s| s.i()).ok_or("fixed w/o size")? as usize,
                )),
                // {"type":"long"} style wrapping (Iceberg uses this with logical types)
                _ => schema_from_json(&Json::Str(ty.to_string()), reg),
            }
        }
        _ => Err("bad schema json".into()),
    }
}

// ---- varint / zigzag ------------------------------------------------------

pub fn zz_enc(v: i64) -> u64 {
    ((v << 1) ^ (v >> 63)) as u64
}
pub fn zz_dec(u: u64) -> i64 {
    ((u >> 1) as i64) ^ -((u & 1) as i64)
}
pub fn vw(out: &mut Vec<u8>, mut u: u64) {
    loop {
        let b = (u & 0x7f) as u8;
        u >>= 7;
        if u == 0 {
            out.push(b);
            return;
        }
        out.push(b | 0x80);
    }
}
pub fn vr(b: &[u8], p: &mut usize) -> Result<u64, String> {
    let mut u = 0u64;
    let mut shift = 0;
    loop {
        let byte = *b.get(*p).ok_or("varint eof")?;
        *p += 1;
        u |= ((byte & 0x7f) as u64) << shift;
        if byte & 0x80 == 0 {
            return Ok(u);
        }
        shift += 7;
        if shift > 63 {
            return Err("varint too long".into());
        }
    }
}
fn wlong(out: &mut Vec<u8>, v: i64) {
    vw(out, zz_enc(v));
}
fn rlong(b: &[u8], p: &mut usize) -> Result<i64, String> {
    Ok(zz_dec(vr(b, p)?))
}

// ---- binary encode/decode (schema-driven) ---------------------------------

pub fn encode(s: &Schema, v: &Value, out: &mut Vec<u8>) -> Result<(), String> {
    match (s, v) {
        (Schema::Null, _) => Ok(()),
        (Schema::Boolean, Value::Bool(b)) => {
            out.push(*b as u8);
            Ok(())
        }
        (Schema::Int | Schema::Long, Value::Long(n)) => {
            wlong(out, *n);
            Ok(())
        }
        (Schema::Double, Value::Double(f)) => {
            out.extend_from_slice(&f.to_le_bytes());
            Ok(())
        }
        (Schema::Float, Value::Double(f)) => {
            out.extend_from_slice(&(*f as f32).to_le_bytes());
            Ok(())
        }
        (Schema::Bytes, Value::Bytes(b)) => {
            wlong(out, b.len() as i64);
            out.extend_from_slice(b);
            Ok(())
        }
        (Schema::String, Value::Str(t)) => {
            wlong(out, t.len() as i64);
            out.extend_from_slice(t.as_bytes());
            Ok(())
        }
        (Schema::Fixed(n), Value::Bytes(b)) if b.len() == *n => {
            out.extend_from_slice(b);
            Ok(())
        }
        (Schema::Array(item), Value::Arr(a)) => {
            if !a.is_empty() {
                wlong(out, a.len() as i64);
                for it in a {
                    encode(item, it, out)?;
                }
            }
            wlong(out, 0);
            Ok(())
        }
        (Schema::Map(val), Value::Map(m)) => {
            if !m.is_empty() {
                wlong(out, m.len() as i64);
                for (k, vv) in m {
                    wlong(out, k.len() as i64);
                    out.extend_from_slice(k.as_bytes());
                    encode(val, vv, out)?;
                }
            }
            wlong(out, 0);
            Ok(())
        }
        (Schema::Union(branches), v) => {
            // Pick the first branch that matches the value's shape.
            for (i, br) in branches.iter().enumerate() {
                let ok = matches!(
                    (br, v),
                    (Schema::Null, Value::Null)
                        | (Schema::Boolean, Value::Bool(_))
                        | (Schema::Int | Schema::Long, Value::Long(_))
                        | (Schema::Float | Schema::Double, Value::Double(_))
                        | (Schema::Bytes | Schema::Fixed(_), Value::Bytes(_))
                        | (Schema::String, Value::Str(_))
                        | (Schema::Array(_), Value::Arr(_))
                        | (Schema::Map(_), Value::Map(_))
                        | (Schema::Record { .. }, Value::Record(_))
                );
                if ok {
                    wlong(out, i as i64);
                    return encode(br, v, out);
                }
            }
            Err("no union branch matches".into())
        }
        (Schema::Record { fields, .. }, Value::Record(fv)) => {
            for (fname, fsch) in fields {
                let val = fv
                    .iter()
                    .find(|(n, _)| n == fname)
                    .map(|(_, v)| v)
                    .unwrap_or(&Value::Null);
                encode(fsch, val, out)?;
            }
            Ok(())
        }
        _ => Err(format!("encode mismatch {s:?}")),
    }
}

pub fn decode(s: &Schema, b: &[u8], p: &mut usize) -> Result<Value, String> {
    match s {
        Schema::Null => Ok(Value::Null),
        Schema::Boolean => {
            let v = *b.get(*p).ok_or("eof")? != 0;
            *p += 1;
            Ok(Value::Bool(v))
        }
        Schema::Int | Schema::Long => Ok(Value::Long(rlong(b, p)?)),
        Schema::Float => {
            let v = f32::from_le_bytes(b[*p..*p + 4].try_into().map_err(|_| "eof")?);
            *p += 4;
            Ok(Value::Double(v as f64))
        }
        Schema::Double => {
            let v = f64::from_le_bytes(b[*p..*p + 8].try_into().map_err(|_| "eof")?);
            *p += 8;
            Ok(Value::Double(v))
        }
        Schema::Bytes => {
            let n = rlong(b, p)? as usize;
            let v = b.get(*p..*p + n).ok_or("eof")?.to_vec();
            *p += n;
            Ok(Value::Bytes(v))
        }
        Schema::String => {
            let n = rlong(b, p)? as usize;
            let v = String::from_utf8(b.get(*p..*p + n).ok_or("eof")?.to_vec())
                .map_err(|_| "bad utf8")?;
            *p += n;
            Ok(Value::Str(v))
        }
        Schema::Fixed(n) => {
            let v = b.get(*p..*p + n).ok_or("eof")?.to_vec();
            *p += n;
            Ok(Value::Bytes(v))
        }
        Schema::Array(item) => {
            let mut out = vec![];
            loop {
                let mut count = rlong(b, p)?;
                if count == 0 {
                    return Ok(Value::Arr(out));
                }
                if count < 0 {
                    count = -count;
                    let _block_size = rlong(b, p)?;
                }
                for _ in 0..count {
                    out.push(decode(item, b, p)?);
                }
            }
        }
        Schema::Map(val) => {
            let mut out = vec![];
            loop {
                let mut count = rlong(b, p)?;
                if count == 0 {
                    return Ok(Value::Map(out));
                }
                if count < 0 {
                    count = -count;
                    let _block_size = rlong(b, p)?;
                }
                for _ in 0..count {
                    let n = rlong(b, p)? as usize;
                    let k = String::from_utf8(b[*p..*p + n].to_vec()).map_err(|_| "k")?;
                    *p += n;
                    out.push((k, decode(val, b, p)?));
                }
            }
        }
        Schema::Union(branches) => {
            let idx = rlong(b, p)? as usize;
            decode(branches.get(idx).ok_or("bad union index")?, b, p)
        }
        Schema::Record { fields, .. } => {
            let mut out = Vec::with_capacity(fields.len());
            for (fname, fsch) in fields {
                out.push((fname.clone(), decode(fsch, b, p)?));
            }
            Ok(Value::Record(out))
        }
    }
}

// ---- object container files ------------------------------------------------

const MAGIC: &[u8; 4] = b"Obj\x01";

pub fn write_container(schema_json: &str, values: &[Value]) -> Result<Vec<u8>, String> {
    let sj = json::parse(schema_json)?;
    let schema = schema_from_json(&sj, &mut HashMap::new())?;
    let mut out = Vec::new();
    out.extend_from_slice(MAGIC);
    // header metadata map
    let meta: Vec<(&str, &[u8])> = vec![
        ("avro.schema", schema_json.as_bytes()),
        ("avro.codec", b"null"),
    ];
    wlong(&mut out, meta.len() as i64);
    for (k, v) in meta {
        wlong(&mut out, k.len() as i64);
        out.extend_from_slice(k.as_bytes());
        wlong(&mut out, v.len() as i64);
        out.extend_from_slice(v);
    }
    wlong(&mut out, 0);
    let sync: [u8; 16] = *b"BLITZSYNC0123456";
    out.extend_from_slice(&sync);
    // one block
    let mut payload = Vec::new();
    for v in values {
        encode(&schema, v, &mut payload)?;
    }
    wlong(&mut out, values.len() as i64);
    wlong(&mut out, payload.len() as i64);
    out.extend_from_slice(&payload);
    out.extend_from_slice(&sync);
    Ok(out)
}

pub fn read_container(b: &[u8]) -> Result<(Schema, Vec<Value>), String> {
    if b.get(..4) != Some(MAGIC.as_slice()) {
        return Err("not an avro container".into());
    }
    let mut p = 4usize;
    let mut schema_json = None;
    let mut codec = "null".to_string();
    loop {
        let mut count = rlong(b, &mut p)?;
        if count == 0 {
            break;
        }
        if count < 0 {
            count = -count;
            let _sz = rlong(b, &mut p)?;
        }
        for _ in 0..count {
            let n = rlong(b, &mut p)? as usize;
            let k = String::from_utf8(b[p..p + n].to_vec()).map_err(|_| "k")?;
            p += n;
            let n = rlong(b, &mut p)? as usize;
            let v = b[p..p + n].to_vec();
            p += n;
            match k.as_str() {
                "avro.schema" => schema_json = Some(String::from_utf8(v).map_err(|_| "s")?),
                "avro.codec" => codec = String::from_utf8(v).map_err(|_| "c")?,
                _ => {}
            }
        }
    }
    if codec != "null" {
        return Err(format!("codec {codec} not supported (use null)"));
    }
    let sj = json::parse(&schema_json.ok_or("no schema in header")?)?;
    let schema = schema_from_json(&sj, &mut HashMap::new())?;
    let sync = &b[p..p + 16];
    p += 16;
    let mut values = vec![];
    while p < b.len() {
        let count = rlong(b, &mut p)?;
        let size = rlong(b, &mut p)? as usize;
        let end = p + size;
        for _ in 0..count.unsigned_abs() {
            values.push(decode(&schema, b, &mut p)?);
        }
        p = end;
        if &b[p..p + 16] != sync {
            return Err("sync marker mismatch".into());
        }
        p += 16;
    }
    Ok((schema, values))
}
