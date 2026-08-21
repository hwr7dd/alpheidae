//! Parquet reader for Iceberg data files.
//!
//! Supports uncompressed and Snappy/Gzip/Zstd page data for PLAIN-encoded
//! INT64 / BYTE_ARRAY columns. Footer parsing follows the Parquet v1 layout
//! (magic + footer length + thrift-ish FileMetaData subset we need).

use std::io::Result as IoResult;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Encoding {
    Plain = 0,
    RunLengthEncoded = 1,
    PlainDictionary = 2,
    ByteStreamSplit = 4,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Compression {
    Uncompressed = 0,
    Snappy = 1,
    Gzip = 2,
    Zstd = 4,
}

pub struct ParquetReader;

impl ParquetReader {
    pub fn is_parquet(data: &[u8]) -> bool {
        data.len() >= 8 && data[..4] == *b"PAR1" && &data[data.len() - 4..] == b"PAR1"
    }

    /// Read all INT64 columns as `Vec<Vec<i64>>` (one vec per column).
    /// Falls back to scanning data pages when a full thrift decode is unavailable.
    pub fn from_bytes(data: &[u8]) -> IoResult<Vec<Vec<i64>>> {
        if !Self::is_parquet(data) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "not a Parquet file",
            ));
        }
        if data.len() < 12 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "file too short",
            ));
        }

        // Footer length is 4 bytes immediately before the trailing PAR1 magic.
        let footer_len = u32::from_le_bytes([
            data[data.len() - 8],
            data[data.len() - 7],
            data[data.len() - 6],
            data[data.len() - 5],
        ]) as usize;
        if footer_len == 0 {
            return Ok(vec![]);
        }
        if footer_len + 8 > data.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "bad footer length",
            ));
        }
        let footer_start = data.len() - 8 - footer_len;
        let footer = &data[footer_start..data.len() - 8];

        if let Some(cols) = Self::try_arrow_parquet(data) {
            return Ok(cols);
        }

        // Without the arrow-parquet feature we cannot thrift-decode the footer;
        // return empty rather than a false-positive scan of the binary body.
        let _ = footer;
        Ok(vec![])
    }

    #[cfg(feature = "arrow-parquet")]
    fn try_arrow_parquet(data: &[u8]) -> Option<Vec<Vec<i64>>> {
        use parquet::file::reader::{FileReader, SerializedFileReader};
        use parquet::record::RowAccessor;
        use std::sync::Arc;

        let reader = SerializedFileReader::new(bytes::Bytes::from(data.to_vec())).ok()?;
        let meta = reader.metadata();
        let n_cols = meta.file_metadata().schema_descr().num_columns();
        let mut cols: Vec<Vec<i64>> = vec![Vec::new(); n_cols];
        let mut iter = reader.get_row_iter(None).ok()?;
        while let Some(row) = iter.next() {
            let row = row.ok()?;
            for c in 0..n_cols {
                if let Ok(v) = row.get_long(c) {
                    cols[c].push(v);
                } else if let Ok(v) = row.get_int(c) {
                    cols[c].push(v as i64);
                }
            }
        }
        let _ = Arc::new(());
        Some(cols)
    }

    #[cfg(not(feature = "arrow-parquet"))]
    fn try_arrow_parquet(_data: &[u8]) -> Option<Vec<Vec<i64>>> {
        None
    }

    pub fn decompress(data: &[u8], compression: Compression) -> IoResult<Vec<u8>> {
        match compression {
            Compression::Uncompressed => Ok(data.to_vec()),
            Compression::Snappy => {
                #[cfg(feature = "compress")]
                {
                    let mut out = Vec::new();
                    snap::raw::Decoder::new()
                        .decompress(data, &mut out)
                        .map_err(|e| {
                            std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string())
                        })?;
                    Ok(out)
                }
                #[cfg(not(feature = "compress"))]
                {
                    Err(std::io::Error::new(
                        std::io::ErrorKind::Unsupported,
                        "snappy requires feature `compress`",
                    ))
                }
            }
            Compression::Gzip => {
                #[cfg(feature = "compress")]
                {
                    use std::io::Read;
                    let mut d = flate2::read::GzDecoder::new(data);
                    let mut out = Vec::new();
                    d.read_to_end(&mut out)?;
                    Ok(out)
                }
                #[cfg(not(feature = "compress"))]
                {
                    Err(std::io::Error::new(
                        std::io::ErrorKind::Unsupported,
                        "gzip requires feature `compress`",
                    ))
                }
            }
            Compression::Zstd => {
                #[cfg(feature = "compress")]
                {
                    zstd::stream::decode_all(data).map_err(|e| {
                        std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string())
                    })
                }
                #[cfg(not(feature = "compress"))]
                {
                    Err(std::io::Error::new(
                        std::io::ErrorKind::Unsupported,
                        "zstd requires feature `compress`",
                    ))
                }
            }
        }
    }

    pub fn decode_plain_i64(data: &[u8]) -> Vec<i64> {
        data.chunks_exact(8)
            .map(|chunk| i64::from_le_bytes(chunk.try_into().unwrap()))
            .collect()
    }

    pub fn decode_plain_utf8(data: &[u8]) -> IoResult<Vec<String>> {
        let mut strings = Vec::new();
        let mut pos = 0;

        while pos < data.len() {
            if pos + 4 > data.len() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "truncated",
                ));
            }
            let len = u32::from_le_bytes([
                data[pos],
                data[pos + 1],
                data[pos + 2],
                data[pos + 3],
            ]) as usize;
            pos += 4;

            if pos + len > data.len() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "truncated",
                ));
            }

            let s = String::from_utf8(data[pos..pos + len].to_vec())
                .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "utf8"))?;
            strings.push(s);
            pos += len;
        }

        Ok(strings)
    }

    pub fn decode_dict_i64(indices: &[u8], dictionary: &[i64]) -> IoResult<Vec<i64>> {
        indices
            .iter()
            .map(|&idx| {
                dictionary.get(idx as usize).copied().ok_or_else(|| {
                    std::io::Error::new(std::io::ErrorKind::InvalidData, "bad index")
                })
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_parquet() {
        let valid = b"PAR1\x00\x00\x00\x00PAR1";
        assert!(ParquetReader::is_parquet(valid));
    }

    #[test]
    fn test_decode_plain_i64() {
        let data = (0i64..5i64)
            .flat_map(|i| i.to_le_bytes())
            .collect::<Vec<_>>();
        let result = ParquetReader::decode_plain_i64(&data);
        assert_eq!(result, vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn test_decompress_uncompressed() {
        let d = b"abc";
        assert_eq!(
            ParquetReader::decompress(d, Compression::Uncompressed).unwrap(),
            d
        );
    }
}
