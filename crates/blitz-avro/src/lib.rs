//! blitz-avro — a schema-driven Apache Avro object-container reader/writer.
//!
//! Iceberg's manifest lists and manifest files are Avro container files, so a
//! native Iceberg integration requires native Avro. This implements the
//! subset Iceberg uses: records, arrays, maps, unions (for optional fields),
//! the primitive types, and the `null` / `deflate` block codecs. The decoder
//! is driven by the writer schema embedded in the file header, so it reads
//! manifests produced by other engines as long as they stick to these types.

use serde_json::Value as J;

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
    Record { name: String, fields: Vec<(String, Schema)> },
    Array(Box<Schema>),
    Map(Box<Schema>),
    Union(Vec<Schema>),
}

#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Null,
    Boolean(bool),
    Int(i32),
    Long(i64),
    Float(f32),
    Double(f64),
    Bytes(Vec<u8>),
    String(String),
    Record(Vec<(String, Value)>),
    Array(Vec<Value>),
    Map(Vec<(String, Value)>),
    Union(usize, Box<Value>),
}

impl Value {
    pub fn field(&self, name: &str) -> Option<&Value> {
        match self {
            Value::Record(fs) => fs.iter().find(|(n, _)| n == name).map(|(_, v)| v),
            _ => None,
        }
    }
    /// Unwrap unions transparently.
    pub fn flat(&self) -> &Value {
        match self {
            Value::Union(_, v) => v.flat(),
            v => v,
        }
    }
    pub fn as_long(&self) -> Option<i64> {
        match self.flat() {
            Value::Long(v) => Some(*v),
            Value::Int(v) => Some(*v as i64),
            _ => None,
        }
    }
    pub fn as_str(&self) -> Option<&str> {
        match self.flat() {
            Value::String(s) => Some(s),
            _ => None,
        }
    }
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self.flat() {
            Value::Bytes(b) => Some(b),
            _ => None,
        }
    }
}

pub fn parse_schema(j: &J) -> Schema {
    match j {
        J::String(s) => match s.as_str() {
            "null" => Schema::Null,
            "boolean" => Schema::Boolean,
            "int" => Schema::Int,
            "long" => Schema::Long,
            "float" => Schema::Float,
            "double" => Schema::Double,
            "bytes" => Schema::Bytes,
            "string" => Schema::String,
            other => panic!("unsupported avro primitive {other}"),
        },
        J::Array(opts) => Schema::Union(opts.iter().map(parse_schema).collect()),
        J::Object(o) => match o.get("type").and_then(|t| t.as_str()) {
            Some("record") => Schema::Record {
                name: o.get("name").and_then(|n| n.as_str()).unwrap_or("r").to_string(),
                fields: o["fields"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|f| {
                        (
                            f["name"].as_str().unwrap().to_string(),
                            parse_schema(&f["type"]),
                        )
                    })
                    .collect(),
            },
            Some("array") => Schema::Array(Box::new(parse_schema(&o["items"]))),
            Some("map") => Schema::Map(Box::new(parse_schema(&o["values"]))),
            Some(prim) => parse_schema(&J::String(prim.to_string())),
            None => panic!("bad avro schema object"),
        },
        _ => panic!("bad avro schema"),
    }
}

pub fn schema_to_json(s: &Schema) -> J {
    match s {
        Schema::Null => J::String("null".into()),
        Schema::Boolean => J::String("boolean".into()),
        Schema::Int => J::String("int".into()),
        Schema::Long => J::String("long".into()),
        Schema::Float => J::String("float".into()),
        Schema::Double => J::String("double".into()),
        Schema::Bytes => J::String("bytes".into()),
        Schema::String => J::String("string".into()),
        Schema::Record { name, fields } => serde_json::json!({
            "type": "record", "name": name,
            "fields": fields.iter().map(|(n, t)| serde_json::json!({"name": n, "type": schema_to_json(t)})).collect::<Vec<_>>()
        }),
        Schema::Array(i) => serde_json::json!({"type": "array", "items": schema_to_json(i)}),
        Schema::Map(v) => serde_json::json!({"type": "map", "values": schema_to_json(v)}),
        Schema::Union(opts) => J::Array(opts.iter().map(schema_to_json).collect()),
    }
}

// --- zigzag varint ---------------------------------------------------------

fn put_long(out: &mut Vec<u8>, n: i64) {
    let mut v = ((n << 1) ^ (n >> 63)) as u64;
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
fn get_long(buf: &[u8], pos: &mut usize) -> i64 {
    let mut v = 0u64;
    let mut shift = 0;
    loop {
        let b = buf[*pos];
        *pos += 1;
        v |= ((b & 0x7f) as u64) << shift;
        if b & 0x80 == 0 {
            break;
        }
        shift += 7;
    }
    ((v >> 1) as i64) ^ -((v & 1) as i64)
}

// --- value encode/decode -----------------------------------------------------

pub fn encode(v: &Value, s: &Schema, out: &mut Vec<u8>) {
    match (v, s) {
        (Value::Null, Schema::Null) => {}
        (Value::Boolean(b), Schema::Boolean) => out.push(*b as u8),
        (Value::Int(i), Schema::Int) => put_long(out, *i as i64),
        (Value::Long(l), Schema::Long) => put_long(out, *l),
        (Value::Float(f), Schema::Float) => out.extend(f.to_le_bytes()),
        (Value::Double(d), Schema::Double) => out.extend(d.to_le_bytes()),
        (Value::Bytes(b), Schema::Bytes) => {
            put_long(out, b.len() as i64);
            out.extend(b);
        }
        (Value::String(st), Schema::String) => {
            put_long(out, st.len() as i64);
            out.extend(st.as_bytes());
        }
        (Value::Record(fs), Schema::Record { fields, .. }) => {
            for ((_, fv), (_, fs2)) in fs.iter().zip(fields) {
                encode(fv, fs2, out);
            }
        }
        (Value::Array(items), Schema::Array(it)) => {
            if !items.is_empty() {
                put_long(out, items.len() as i64);
                for i in items {
                    encode(i, it, out);
                }
            }
            put_long(out, 0);
        }
        (Value::Map(kv), Schema::Map(vt)) => {
            if !kv.is_empty() {
                put_long(out, kv.len() as i64);
                for (k, val) in kv {
                    put_long(out, k.len() as i64);
                    out.extend(k.as_bytes());
                    encode(val, vt, out);
                }
            }
            put_long(out, 0);
        }
        (Value::Union(idx, inner), Schema::Union(opts)) => {
            put_long(out, *idx as i64);
            encode(inner, &opts[*idx], out);
        }
        (v, s) => panic!("avro encode mismatch: {v:?} vs {s:?}"),
    }
}

pub fn decode(s: &Schema, buf: &[u8], pos: &mut usize) -> Value {
    match s {
        Schema::Null => Value::Null,
        Schema::Boolean => {
            let b = buf[*pos] != 0;
            *pos += 1;
            Value::Boolean(b)
        }
        Schema::Int => Value::Int(get_long(buf, pos) as i32),
        Schema::Long => Value::Long(get_long(buf, pos)),
        Schema::Float => {
            let f = f32::from_le_bytes(buf[*pos..*pos + 4].try_into().unwrap());
            *pos += 4;
            Value::Float(f)
        }
        Schema::Double => {
            let d = f64::from_le_bytes(buf[*pos..*pos + 8].try_into().unwrap());
            *pos += 8;
            Value::Double(d)
        }
        Schema::Bytes => {
            let n = get_long(buf, pos) as usize;
            let b = buf[*pos..*pos + n].to_vec();
            *pos += n;
            Value::Bytes(b)
        }
        Schema::String => {
            let n = get_long(buf, pos) as usize;
            let st = String::from_utf8(buf[*pos..*pos + n].to_vec()).unwrap();
            *pos += n;
            Value::String(st)
        }
        Schema::Record { fields, .. } => Value::Record(
            fields
                .iter()
                .map(|(n, fs)| (n.clone(), decode(fs, buf, pos)))
                .collect(),
        ),
        Schema::Array(it) => {
            let mut out = vec![];
            loop {
                let mut n = get_long(buf, pos);
                if n == 0 {
                    break;
                }
                if n < 0 {
                    // negative count: followed by byte size (skip it)
                    n = -n;
                    let _bytes = get_long(buf, pos);
                }
                for _ in 0..n {
                    out.push(decode(it, buf, pos));
                }
            }
            Value::Array(out)
        }
        Schema::Map(vt) => {
            let mut out = vec![];
            loop {
                let mut n = get_long(buf, pos);
                if n == 0 {
                    break;
                }
                if n < 0 {
                    n = -n;
                    let _bytes = get_long(buf, pos);
                }
                for _ in 0..n {
                    let kl = get_long(buf, pos) as usize;
                    let k = String::from_utf8(buf[*pos..*pos + kl].to_vec()).unwrap();
                    *pos += kl;
                    out.push((k, decode(vt, buf, pos)));
                }
            }
            Value::Map(out)
        }
        Schema::Union(opts) => {
            let idx = get_long(buf, pos) as usize;
            Value::Union(idx, Box::new(decode(&opts[idx], buf, pos)))
        }
    }
}

// --- object container file ---------------------------------------------------

const SYNC: [u8; 16] = *b"BLITZAVRO_SYNC__";

/// Write an Avro object container file (codec: deflate).
pub fn write_container(path: &std::path::Path, schema: &Schema, rows: &[Value]) -> std::io::Result<()> {
    use std::io::Write;
    if let Some(p) = path.parent() {
        std::fs::create_dir_all(p)?;
    }
    let mut out = Vec::new();
    out.extend(b"Obj\x01");
    // header metadata map<string, bytes>
    let schema_json = serde_json::to_string(&schema_to_json(schema)).unwrap();
    let meta: Vec<(&str, &[u8])> =
        vec![("avro.schema", schema_json.as_bytes()), ("avro.codec", b"deflate")];
    put_long(&mut out, meta.len() as i64);
    for (k, v) in &meta {
        put_long(&mut out, k.len() as i64);
        out.extend(k.as_bytes());
        put_long(&mut out, v.len() as i64);
        out.extend(*v);
    }
    put_long(&mut out, 0);
    out.extend(SYNC);
    // one block
    let mut payload = Vec::new();
    for r in rows {
        encode(r, schema, &mut payload);
    }
    let comp = miniz_oxide::deflate::compress_to_vec(&payload, 6);
    put_long(&mut out, rows.len() as i64);
    put_long(&mut out, comp.len() as i64);
    out.extend(&comp);
    out.extend(SYNC);
    std::fs::File::create(path)?.write_all(&out)
}

/// Read an Avro object container file, returning (writer schema, rows).
pub fn read_container(path: &std::path::Path) -> std::io::Result<(Schema, Vec<Value>)> {
    let buf = std::fs::read(path)?;
    assert_eq!(&buf[..4], b"Obj\x01", "not an avro container");
    let mut pos = 4usize;
    let mut schema_json = String::new();
    let mut codec = "null".to_string();
    loop {
        let mut n = get_long(&buf, &mut pos);
        if n == 0 {
            break;
        }
        if n < 0 {
            n = -n;
            let _ = get_long(&buf, &mut pos);
        }
        for _ in 0..n {
            let kl = get_long(&buf, &mut pos) as usize;
            let k = String::from_utf8(buf[pos..pos + kl].to_vec()).unwrap();
            pos += kl;
            let vl = get_long(&buf, &mut pos) as usize;
            let v = buf[pos..pos + vl].to_vec();
            pos += vl;
            match k.as_str() {
                "avro.schema" => schema_json = String::from_utf8(v).unwrap(),
                "avro.codec" => codec = String::from_utf8(v).unwrap(),
                _ => {}
            }
        }
    }
    let schema = parse_schema(&serde_json::from_str(&schema_json).unwrap());
    pos += 16; // sync
    let mut rows = vec![];
    while pos < buf.len() {
        let count = get_long(&buf, &mut pos);
        let size = get_long(&buf, &mut pos) as usize;
        let block = &buf[pos..pos + size];
        pos += size + 16; // payload + sync
        let raw: Vec<u8> = match codec.as_str() {
            "null" => block.to_vec(),
            "deflate" => miniz_oxide::inflate::decompress_to_vec(block)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, format!("{e:?}")))?,
            c => panic!("unsupported avro codec {c}"),
        };
        let mut bp = 0usize;
        for _ in 0..count {
            rows.push(decode(&schema, &raw, &mut bp));
        }
    }
    Ok((schema, rows))
}
