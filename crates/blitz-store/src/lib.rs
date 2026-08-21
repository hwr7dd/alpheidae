//! Object store for Iceberg warehouses: local filesystem and optional S3.

use std::io;
use std::path::{Path, PathBuf};

/// Read/write/list blob storage under a warehouse root.
pub trait ObjectStore: Send + Sync {
    fn get(&self, key: &str) -> io::Result<Vec<u8>>;
    fn put(&self, key: &str, bytes: &[u8]) -> io::Result<()>;
    fn delete(&self, key: &str) -> io::Result<()>;
    fn list(&self, prefix: &str) -> io::Result<Vec<String>>;
    fn exists(&self, key: &str) -> io::Result<bool> {
        match self.get(key) {
            Ok(_) => Ok(true),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(e),
        }
    }
}

/// Local directory warehouse (`file://` / bare path).
pub struct LocalStore {
    root: PathBuf,
}

impl LocalStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        LocalStore { root: root.into() }
    }

    fn resolve(&self, key: &str) -> PathBuf {
        let key = key.trim_start_matches('/');
        self.root.join(key)
    }
}

impl ObjectStore for LocalStore {
    fn get(&self, key: &str) -> io::Result<Vec<u8>> {
        std::fs::read(self.resolve(key))
    }

    fn put(&self, key: &str, bytes: &[u8]) -> io::Result<()> {
        let path = self.resolve(key);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let tmp = path.with_extension("tmp");
        std::fs::write(&tmp, bytes)?;
        std::fs::rename(&tmp, &path)?;
        Ok(())
    }

    fn delete(&self, key: &str) -> io::Result<()> {
        let path = self.resolve(key);
        match std::fs::remove_file(path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e),
        }
    }

    fn list(&self, prefix: &str) -> io::Result<Vec<String>> {
        let base = self.resolve(prefix);
        let mut out = Vec::new();
        if !base.exists() {
            return Ok(out);
        }
        fn walk(dir: &Path, root: &Path, out: &mut Vec<String>) -> io::Result<()> {
            for ent in std::fs::read_dir(dir)? {
                let ent = ent?;
                let p = ent.path();
                if p.is_dir() {
                    walk(&p, root, out)?;
                } else if let Ok(rel) = p.strip_prefix(root) {
                    out.push(rel.to_string_lossy().replace('\\', "/"));
                }
            }
            Ok(())
        }
        walk(&base, &self.root, &mut out)?;
        out.sort();
        Ok(out)
    }
}

/// Parse `s3://bucket/prefix` or a local path into a store.
pub fn open(uri: &str) -> io::Result<Box<dyn ObjectStore>> {
    if let Some(_rest) = uri.strip_prefix("s3://") {
        #[cfg(feature = "s3")]
        {
            let rest = _rest;
            let (bucket, prefix) = match rest.split_once('/') {
                Some((b, p)) => (b.to_string(), p.to_string()),
                None => (rest.to_string(), String::new()),
            };
            return Ok(Box::new(S3Store::new(bucket, prefix)?));
        }
        #[cfg(not(feature = "s3"))]
        {
            let _ = _rest;
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "S3 support requires blitz-store with feature `s3`",
            ));
        }
    }
    let path = uri.strip_prefix("file://").unwrap_or(uri);
    Ok(Box::new(LocalStore::new(path)))
}

#[cfg(feature = "s3")]
mod s3_impl {
    use super::*;
    use aws_sdk_s3::primitives::ByteStream;
    use aws_sdk_s3::Client;
    use std::sync::Arc;

    pub struct S3Store {
        client: Client,
        bucket: String,
        prefix: String,
        rt: Arc<tokio::runtime::Runtime>,
    }

    impl S3Store {
        pub fn new(bucket: String, prefix: String) -> io::Result<Self> {
            let rt = tokio::runtime::Runtime::new().map_err(|e| {
                io::Error::new(io::ErrorKind::Other, format!("tokio runtime: {e}"))
            })?;
            let client = rt.block_on(async {
                let conf = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
                Client::new(&conf)
            });
            let prefix = prefix.trim_matches('/').to_string();
            Ok(S3Store {
                client,
                bucket,
                prefix,
                rt: Arc::new(rt),
            })
        }

        fn full_key(&self, key: &str) -> String {
            let key = key.trim_start_matches('/');
            if self.prefix.is_empty() {
                key.to_string()
            } else {
                format!("{}/{}", self.prefix, key)
            }
        }
    }

    impl ObjectStore for S3Store {
        fn get(&self, key: &str) -> io::Result<Vec<u8>> {
            let key = self.full_key(key);
            let client = self.client.clone();
            let bucket = self.bucket.clone();
            self.rt.block_on(async move {
                let resp = client
                    .get_object()
                    .bucket(&bucket)
                    .key(&key)
                    .send()
                    .await
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
                let bytes = resp
                    .body
                    .collect()
                    .await
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?
                    .into_bytes();
                Ok(bytes.to_vec())
            })
        }

        fn put(&self, key: &str, bytes: &[u8]) -> io::Result<()> {
            let key = self.full_key(key);
            let client = self.client.clone();
            let bucket = self.bucket.clone();
            let body = ByteStream::from(bytes.to_vec());
            self.rt.block_on(async move {
                client
                    .put_object()
                    .bucket(&bucket)
                    .key(&key)
                    .body(body)
                    .send()
                    .await
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
                Ok(())
            })
        }

        fn delete(&self, key: &str) -> io::Result<()> {
            let key = self.full_key(key);
            let client = self.client.clone();
            let bucket = self.bucket.clone();
            self.rt.block_on(async move {
                client
                    .delete_object()
                    .bucket(&bucket)
                    .key(&key)
                    .send()
                    .await
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
                Ok(())
            })
        }

        fn list(&self, prefix: &str) -> io::Result<Vec<String>> {
            let full = self.full_key(prefix);
            let client = self.client.clone();
            let bucket = self.bucket.clone();
            let strip = if self.prefix.is_empty() {
                String::new()
            } else {
                format!("{}/", self.prefix)
            };
            self.rt.block_on(async move {
                let mut out = Vec::new();
                let mut token: Option<String> = None;
                loop {
                    let mut req = client.list_objects_v2().bucket(&bucket).prefix(&full);
                    if let Some(t) = &token {
                        req = req.continuation_token(t);
                    }
                    let resp = req
                        .send()
                        .await
                        .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
                    for obj in resp.contents() {
                        if let Some(k) = obj.key() {
                            let rel = k.strip_prefix(&strip).unwrap_or(k);
                            out.push(rel.to_string());
                        }
                    }
                    if resp.is_truncated() == Some(true) {
                        token = resp.next_continuation_token().map(|s| s.to_string());
                    } else {
                        break;
                    }
                }
                Ok(out)
            })
        }
    }
}

#[cfg(feature = "s3")]
pub use s3_impl::S3Store;

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    #[test]
    fn local_roundtrip() {
        let dir = env::temp_dir().join(format!("blitz-store-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let store = LocalStore::new(&dir);
        store.put("a/b.txt", b"hello").unwrap();
        assert_eq!(store.get("a/b.txt").unwrap(), b"hello");
        assert!(store.list("a/").unwrap().iter().any(|k| k.ends_with("b.txt")));
        store.delete("a/b.txt").unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }
}
