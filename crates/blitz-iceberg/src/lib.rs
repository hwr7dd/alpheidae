//! blitz-iceberg — native Apache Iceberg (format v2) table support.
//!
//! Implements the metadata layer end to end with no Iceberg SDK:
//!   * `metadata.json` read/write (schemas, snapshots, specs)
//!   * manifest lists and manifest files as real Avro containers
//!     (deflate codec) with spec-named fields, via blitz-avro
//!   * per-column lower/upper bounds in manifests, serialized as Iceberg
//!     single-value binary (little-endian for longs, UTF-8 for strings)
//!   * snapshot commits (append) and scan planning with file-level pruning
//!     from manifest bounds — files are eliminated before any data I/O.
//!
//! Data files are BlitzCol (`file_format` recorded accordingly). Reading
//! Parquet data files written by other engines would slot in behind the same
//! `DataFile` struct; the metadata layer above is format-agnostic.

use blitz_avro as avro;
use blitz_format::{CmpOp, DataType, Literal};
use serde_json::{json, Value as J};
use std::path::{Path, PathBuf};

#[derive(Clone, Debug)]
pub struct Field {
    pub id: i32,
    pub name: String,
    pub dtype: DataType,
}

#[derive(Clone, Debug)]
pub struct TableMeta {
    pub uuid: String,
    pub location: PathBuf,
    pub fields: Vec<Field>,
    pub current_snapshot: Option<i64>,
    pub snapshots: Vec<(i64, String)>, // (snapshot-id, manifest-list path)
    pub last_sequence_number: i64,
}

#[derive(Clone, Debug)]
pub struct DataFile {
    pub path: PathBuf,
    pub file_size: i64,
    pub record_count: i64,
    /// field-id -> (lower, upper) in Iceberg single-value binary
    pub bounds: Vec<(i32, Vec<u8>, Vec<u8>)>,
}

pub fn ser_long(v: i64) -> Vec<u8> {
    v.to_le_bytes().to_vec()
}
pub fn de_long(b: &[u8]) -> i64 {
    i64::from_le_bytes(b[..8].try_into().unwrap())
}

// ---------------------------------------------------------------------------
// metadata.json
// ---------------------------------------------------------------------------

fn type_str(d: DataType) -> &'static str {
    match d {
        DataType::Int64 => "long",
        DataType::Utf8 => "string",
    }
}
fn type_of(s: &str) -> DataType {
    match s {
        "long" => DataType::Int64,
        "string" => DataType::Utf8,
        o => panic!("unsupported iceberg type {o}"),
    }
}

pub fn write_metadata(meta: &TableMeta, version: u32) -> std::io::Result<PathBuf> {
    let snaps: Vec<J> = meta
        .snapshots
        .iter()
        .enumerate()
        .map(|(i, (id, ml))| {
            json!({
                "snapshot-id": id,
                "sequence-number": (i + 1) as i64,
                "timestamp-ms": 1750000000000i64 + i as i64,
                "manifest-list": ml,
                "summary": {"operation": "append"},
                "schema-id": 0
            })
        })
        .collect();
    let doc = json!({
        "format-version": 2,
        "table-uuid": meta.uuid,
        "location": meta.location.to_string_lossy(),
        "last-sequence-number": meta.last_sequence_number,
        "last-updated-ms": 1750000000000i64,
        "last-column-id": meta.fields.iter().map(|f| f.id).max().unwrap_or(0),
        "current-schema-id": 0,
        "schemas": [{
            "schema-id": 0, "type": "struct",
            "fields": meta.fields.iter().map(|f| json!({
                "id": f.id, "name": f.name, "required": true, "type": type_str(f.dtype)
            })).collect::<Vec<_>>()
        }],
        "default-spec-id": 0,
        "partition-specs": [{"spec-id": 0, "fields": []}],
        "last-partition-id": 999,
        "default-sort-order-id": 0,
        "sort-orders": [{"order-id": 0, "fields": []}],
        "current-snapshot-id": meta.current_snapshot,
        "snapshots": snaps,
        "refs": {}
    });
    let dir = meta.location.join("metadata");
    std::fs::create_dir_all(&dir)?;
    let path = dir.join(format!("v{version}.metadata.json"));
    std::fs::write(&path, serde_json::to_string_pretty(&doc)?)?;
    Ok(path)
}

pub fn read_metadata(path: &Path) -> std::io::Result<TableMeta> {
    let doc: J = serde_json::from_str(&std::fs::read_to_string(path)?)?;
    assert_eq!(doc["format-version"], 2, "only iceberg v2 supported");
    let schema = doc["schemas"]
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s["schema-id"] == doc["current-schema-id"])
        .expect("current schema");
    let fields = schema["fields"]
        .as_array()
        .unwrap()
        .iter()
        .map(|f| Field {
            id: f["id"].as_i64().unwrap() as i32,
            name: f["name"].as_str().unwrap().to_string(),
            dtype: type_of(f["type"].as_str().unwrap()),
        })
        .collect();
    let snapshots = doc["snapshots"]
        .as_array()
        .map(|a| {
            a.iter()
                .map(|s| {
                    (
                        s["snapshot-id"].as_i64().unwrap(),
                        s["manifest-list"].as_str().unwrap().to_string(),
                    )
                })
                .collect()
        })
        .unwrap_or_default();
    Ok(TableMeta {
        uuid: doc["table-uuid"].as_str().unwrap_or("").to_string(),
        location: PathBuf::from(doc["location"].as_str().unwrap()),
        fields,
        current_snapshot: doc["current-snapshot-id"].as_i64(),
        snapshots,
        last_sequence_number: doc["last-sequence-number"].as_i64().unwrap_or(0),
    })
}

// ---------------------------------------------------------------------------
// manifests (Avro)
// ---------------------------------------------------------------------------

fn bounds_schema() -> avro::Schema {
    // Iceberg serializes bounds as array<record{key:int, value:bytes}> (k145/k146 style)
    avro::Schema::Array(Box::new(avro::Schema::Record {
        name: "k_v".into(),
        fields: vec![("key".into(), avro::Schema::Int), ("value".into(), avro::Schema::Bytes)],
    }))
}

fn manifest_entry_schema() -> avro::Schema {
    avro::Schema::Record {
        name: "manifest_entry".into(),
        fields: vec![
            ("status".into(), avro::Schema::Int),
            (
                "snapshot_id".into(),
                avro::Schema::Union(vec![avro::Schema::Null, avro::Schema::Long]),
            ),
            (
                "data_file".into(),
                avro::Schema::Record {
                    name: "r2".into(),
                    fields: vec![
                        ("content".into(), avro::Schema::Int),
                        ("file_path".into(), avro::Schema::String),
                        ("file_format".into(), avro::Schema::String),
                        (
                            "partition".into(),
                            avro::Schema::Record { name: "r102".into(), fields: vec![] },
                        ),
                        ("record_count".into(), avro::Schema::Long),
                        ("file_size_in_bytes".into(), avro::Schema::Long),
                        (
                            "lower_bounds".into(),
                            avro::Schema::Union(vec![avro::Schema::Null, bounds_schema()]),
                        ),
                        (
                            "upper_bounds".into(),
                            avro::Schema::Union(vec![avro::Schema::Null, bounds_schema()]),
                        ),
                    ],
                },
            ),
        ],
    }
}

fn manifest_file_schema() -> avro::Schema {
    avro::Schema::Record {
        name: "manifest_file".into(),
        fields: vec![
            ("manifest_path".into(), avro::Schema::String),
            ("manifest_length".into(), avro::Schema::Long),
            ("partition_spec_id".into(), avro::Schema::Int),
            ("content".into(), avro::Schema::Int),
            ("sequence_number".into(), avro::Schema::Long),
            ("min_sequence_number".into(), avro::Schema::Long),
            ("added_snapshot_id".into(), avro::Schema::Long),
            ("added_files_count".into(), avro::Schema::Int),
            ("existing_files_count".into(), avro::Schema::Int),
            ("deleted_files_count".into(), avro::Schema::Int),
            ("added_rows_count".into(), avro::Schema::Long),
            ("existing_rows_count".into(), avro::Schema::Long),
            ("deleted_rows_count".into(), avro::Schema::Long),
        ],
    }
}

fn bounds_value(b: &[(i32, Vec<u8>)]) -> avro::Value {
    avro::Value::Union(
        1,
        Box::new(avro::Value::Array(
            b.iter()
                .map(|(k, v)| {
                    avro::Value::Record(vec![
                        ("key".into(), avro::Value::Int(*k)),
                        ("value".into(), avro::Value::Bytes(v.clone())),
                    ])
                })
                .collect(),
        )),
    )
}

pub fn write_manifest(
    path: &Path,
    snapshot_id: i64,
    files: &[DataFile],
) -> std::io::Result<i64> {
    let schema = manifest_entry_schema();
    let rows: Vec<avro::Value> = files
        .iter()
        .map(|f| {
            let lower: Vec<(i32, Vec<u8>)> =
                f.bounds.iter().map(|(k, lo, _)| (*k, lo.clone())).collect();
            let upper: Vec<(i32, Vec<u8>)> =
                f.bounds.iter().map(|(k, _, hi)| (*k, hi.clone())).collect();
            avro::Value::Record(vec![
                ("status".into(), avro::Value::Int(1)), // ADDED
                (
                    "snapshot_id".into(),
                    avro::Value::Union(1, Box::new(avro::Value::Long(snapshot_id))),
                ),
                (
                    "data_file".into(),
                    avro::Value::Record(vec![
                        ("content".into(), avro::Value::Int(0)),
                        (
                            "file_path".into(),
                            avro::Value::String(f.path.to_string_lossy().into()),
                        ),
                        ("file_format".into(), avro::Value::String("BLITZCOL".into())),
                        ("partition".into(), avro::Value::Record(vec![])),
                        ("record_count".into(), avro::Value::Long(f.record_count)),
                        ("file_size_in_bytes".into(), avro::Value::Long(f.file_size)),
                        ("lower_bounds".into(), bounds_value(&lower)),
                        ("upper_bounds".into(), bounds_value(&upper)),
                    ]),
                ),
            ])
        })
        .collect();
    avro::write_container(path, &schema, &rows)?;
    Ok(std::fs::metadata(path)?.len() as i64)
}

pub fn write_manifest_list(
    path: &Path,
    snapshot_id: i64,
    seq: i64,
    manifests: &[(PathBuf, i64, i64)], // (path, length, rows)
) -> std::io::Result<()> {
    let schema = manifest_file_schema();
    let rows: Vec<avro::Value> = manifests
        .iter()
        .map(|(p, len, nrows)| {
            avro::Value::Record(vec![
                ("manifest_path".into(), avro::Value::String(p.to_string_lossy().into())),
                ("manifest_length".into(), avro::Value::Long(*len)),
                ("partition_spec_id".into(), avro::Value::Int(0)),
                ("content".into(), avro::Value::Int(0)),
                ("sequence_number".into(), avro::Value::Long(seq)),
                ("min_sequence_number".into(), avro::Value::Long(seq)),
                ("added_snapshot_id".into(), avro::Value::Long(snapshot_id)),
                ("added_files_count".into(), avro::Value::Int(1)),
                ("existing_files_count".into(), avro::Value::Int(0)),
                ("deleted_files_count".into(), avro::Value::Int(0)),
                ("added_rows_count".into(), avro::Value::Long(*nrows)),
                ("existing_rows_count".into(), avro::Value::Long(0)),
                ("deleted_rows_count".into(), avro::Value::Long(0)),
            ])
        })
        .collect();
    avro::write_container(path, &schema, &rows)
}

/// Walk snapshot → manifest list → manifests → data files (all Avro reads).
pub fn plan_files(meta: &TableMeta) -> std::io::Result<Vec<DataFile>> {
    let Some(cur) = meta.current_snapshot else { return Ok(vec![]) };
    let ml = meta
        .snapshots
        .iter()
        .find(|(id, _)| *id == cur)
        .map(|(_, p)| p.clone())
        .expect("current snapshot in snapshot list");
    let (_, manifests) = avro::read_container(Path::new(&ml))?;
    let mut out = vec![];
    for m in manifests {
        let mpath = m.field("manifest_path").and_then(|v| v.as_str()).unwrap().to_string();
        let (_, entries) = avro::read_container(Path::new(&mpath))?;
        for e in entries {
            let status = match e.field("status").unwrap().flat() {
                avro::Value::Int(i) => *i,
                _ => 1,
            };
            if status == 2 {
                continue; // DELETED
            }
            let df = e.field("data_file").unwrap();
            let mut bounds = vec![];
            let parse_b = |v: &avro::Value| -> Vec<(i32, Vec<u8>)> {
                match v.flat() {
                    avro::Value::Array(items) => items
                        .iter()
                        .map(|r| {
                            (
                                match r.field("key").unwrap().flat() {
                                    avro::Value::Int(k) => *k,
                                    _ => 0,
                                },
                                r.field("value").unwrap().as_bytes().unwrap().to_vec(),
                            )
                        })
                        .collect(),
                    _ => vec![],
                }
            };
            let lows = df.field("lower_bounds").map(|v| parse_b(v)).unwrap_or_default();
            let highs = df.field("upper_bounds").map(|v| parse_b(v)).unwrap_or_default();
            for (k, lo) in lows {
                if let Some((_, hi)) = highs.iter().find(|(hk, _)| *hk == k) {
                    bounds.push((k, lo, hi.clone()));
                }
            }
            out.push(DataFile {
                path: PathBuf::from(df.field("file_path").unwrap().as_str().unwrap()),
                file_size: df.field("file_size_in_bytes").unwrap().as_long().unwrap_or(0),
                record_count: df.field("record_count").unwrap().as_long().unwrap_or(0),
                bounds,
            });
        }
    }
    Ok(out)
}

/// File-level pruning from manifest bounds — happens before any data I/O.
pub fn file_prunes(f: &DataFile, field_id: i32, dtype: DataType, op: CmpOp, lit: &Literal) -> bool {
    let Some((_, lo, hi)) = f.bounds.iter().find(|(k, _, _)| *k == field_id) else {
        return false;
    };
    match (dtype, lit) {
        (DataType::Int64, Literal::Int(l)) => {
            let (mn, mx) = (de_long(lo), de_long(hi));
            match op {
                CmpOp::Gt => mx <= *l,
                CmpOp::Ge => mx < *l,
                CmpOp::Lt => mn >= *l,
                CmpOp::Le => mn > *l,
                CmpOp::Eq => *l < mn || *l > mx,
            }
        }
        (DataType::Utf8, Literal::Str(s)) => {
            let (mn, mx) = (
                String::from_utf8_lossy(lo).to_string(),
                String::from_utf8_lossy(hi).to_string(),
            );
            op == CmpOp::Eq && (*s < mn || *s > mx)
        }
        _ => false,
    }
}

/// Append a new snapshot (manifest list already written) and emit the next
/// metadata.json. Returns the new metadata path — the pointer that the
/// catalog (blitz-meta) swaps atomically.
pub fn commit_append(
    meta: &mut TableMeta,
    snapshot_id: i64,
    manifest_list: &Path,
    version: u32,
) -> std::io::Result<PathBuf> {
    meta.last_sequence_number += 1;
    meta.snapshots
        .push((snapshot_id, manifest_list.to_string_lossy().into()));
    meta.current_snapshot = Some(snapshot_id);
    write_metadata(meta, version)
}

// ---------------------------------------------------------------------------
// Object-store backed metadata (file:// or s3:// warehouses)
// ---------------------------------------------------------------------------

/// Read `metadata.json` bytes from an object store key.
pub fn read_metadata_from_store(
    store: &dyn blitz_store::ObjectStore,
    key: &str,
) -> std::io::Result<TableMeta> {
    let bytes = store.get(key)?;
    let text = String::from_utf8(bytes)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    // Reuse path-based parser via temp — or parse JSON directly.
    let tmp = std::env::temp_dir().join(format!("blitz-meta-{}.json", std::process::id()));
    std::fs::write(&tmp, &text)?;
    let meta = read_metadata(&tmp);
    let _ = std::fs::remove_file(&tmp);
    meta
}

/// Write metadata.json to the object store; returns the object key.
pub fn write_metadata_to_store(
    store: &dyn blitz_store::ObjectStore,
    meta: &TableMeta,
    version: u32,
) -> std::io::Result<String> {
    let path = write_metadata(meta, version)?;
    let bytes = std::fs::read(&path)?;
    let key = format!("metadata/v{version}.metadata.json");
    store.put(&key, &bytes)?;
    Ok(key)
}

/// Open a warehouse URI (`file:///...` or `s3://bucket/prefix`).
pub fn open_warehouse(uri: &str) -> std::io::Result<Box<dyn blitz_store::ObjectStore>> {
    blitz_store::open(uri)
}

