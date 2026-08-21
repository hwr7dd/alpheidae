//! HTTP query daemon for the Firecracker host tier.
//!
//! Accepts POST /v1/query with JSON {"sql":"...", "workers":6}.
//!
//! Environment:
//!   BLITZ_SNAPDIR     — snapshot directory (default /var/lib/blitz/snapshots)
//!   BLITZ_RUNDIR      — firecracker API socket dir (default /run/blitz)
//!   BLITZ_WORKERS     — default worker count (default 6)
//!   BLITZ_LISTEN      — listen address (default 0.0.0.0:8080)
//!   BLITZ_API_TOKEN   — if set, require `Authorization: Bearer <token>`
//!   BLITZ_WAREHOUSE   — object store URI (file:// or s3://) for metrics/info

use blitz_boot::{ignite_ramp, Firecracker, RampPlan, WorkerSlot};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

struct Metrics {
    queries_total: AtomicU64,
    queries_ok: AtomicU64,
    queries_err: AtomicU64,
    queries_unauthorized: AtomicU64,
    bytes_out: AtomicU64,
}

impl Metrics {
    fn new() -> Self {
        Metrics {
            queries_total: AtomicU64::new(0),
            queries_ok: AtomicU64::new(0),
            queries_err: AtomicU64::new(0),
            queries_unauthorized: AtomicU64::new(0),
            bytes_out: AtomicU64::new(0),
        }
    }

    fn prometheus(&self) -> String {
        format!(
            "# HELP blitz_queries_total Total query attempts\n\
             # TYPE blitz_queries_total counter\n\
             blitz_queries_total {}\n\
             # HELP blitz_queries_ok Successful queries\n\
             # TYPE blitz_queries_ok counter\n\
             blitz_queries_ok {}\n\
             # HELP blitz_queries_err Failed queries\n\
             # TYPE blitz_queries_err counter\n\
             blitz_queries_err {}\n\
             # HELP blitz_queries_unauthorized Auth failures\n\
             # TYPE blitz_queries_unauthorized counter\n\
             blitz_queries_unauthorized {}\n\
             # HELP blitz_bytes_out Response body bytes\n\
             # TYPE blitz_bytes_out counter\n\
             blitz_bytes_out {}\n",
            self.queries_total.load(Ordering::Relaxed),
            self.queries_ok.load(Ordering::Relaxed),
            self.queries_err.load(Ordering::Relaxed),
            self.queries_unauthorized.load(Ordering::Relaxed),
            self.bytes_out.load(Ordering::Relaxed),
        )
    }
}

fn env(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.into())
}

fn parse_json_sql(body: &str) -> Option<(String, Option<usize>)> {
    let sql_key = "\"sql\"";
    let k = body.find(sql_key)? + sql_key.len();
    let rest = body[k..].trim_start();
    let rest = rest.strip_prefix(':')?.trim_start();
    let q = rest.chars().next()?;
    if q != '"' {
        return None;
    }
    let mut sql = String::new();
    let mut esc = false;
    for c in rest.chars().skip(1) {
        if esc {
            sql.push(c);
            esc = false;
        } else if c == '\\' {
            esc = true;
        } else if c == '"' {
            break;
        } else {
            sql.push(c);
        }
    }
    let workers = body.find("\"workers\"").and_then(|i| {
        let tail = &body[i..];
        tail.find(':')
            .and_then(|j| tail[j + 1..].split(|c: char| !c.is_ascii_digit()).next())
            .and_then(|n| n.parse().ok())
    });
    Some((sql, workers))
}

fn authorized(req: &str, token: &Option<String>) -> bool {
    let Some(expected) = token else {
        return true; // auth disabled
    };
    for line in req.lines() {
        let lower = line.to_ascii_lowercase();
        if let Some(rest) = lower.strip_prefix("authorization:") {
            let rest = rest.trim();
            if let Some(got) = rest.strip_prefix("bearer ") {
                return got.trim() == expected;
            }
        }
    }
    false
}

fn spawn_firecracker(rundir: &PathBuf, snapdir: &PathBuf, n_workers: usize) -> std::io::Result<()> {
    use std::process::Command;
    std::fs::create_dir_all(rundir)?;
    let slots: Vec<String> = std::iter::once("coord".into())
        .chain((0..n_workers).map(|i| format!("worker{i}")))
        .collect();
    for slot in &slots {
        let sock = rundir.join(format!("{slot}.sock"));
        let _ = std::fs::remove_file(&sock);
        Command::new("firecracker")
            .arg("--api-sock")
            .arg(&sock)
            .spawn()?;
    }
    for slot in &slots {
        let sock = rundir.join(format!("{slot}.sock"));
        for _ in 0..5000 {
            if sock.exists() {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        let vmstate = snapdir.join(format!("{slot}.vmstate"));
        let mem = snapdir.join(format!("{slot}.mem"));
        let run_vmstate = rundir.join(format!("{slot}.vmstate"));
        let run_mem = rundir.join(format!("{slot}.mem"));
        let _ = std::fs::remove_file(&run_vmstate);
        let _ = std::fs::remove_file(&run_mem);
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(&vmstate, &run_vmstate)?;
            std::os::unix::fs::symlink(&mem, &run_mem)?;
        }
        #[cfg(not(unix))]
        {
            std::fs::copy(&vmstate, &run_vmstate)?;
            std::fs::copy(&mem, &run_mem)?;
        }
    }
    Ok(())
}

fn run_query(rundir: &PathBuf, n_workers: usize, sql: &str) -> std::io::Result<String> {
    let t0 = Instant::now();
    let mut log = String::new();

    let coord = Firecracker::connect(rundir.join("coord.sock").to_string_lossy().to_string());
    coord.snapshot_resume(
        &rundir.join("coord.vmstate"),
        &rundir.join("coord.mem"),
        None,
    )?;
    log.push_str(&format!(
        "[{:>8.3} ms] coordinator resumed\n",
        t0.elapsed().as_secs_f64() * 1e3
    ));

    let plan = RampPlan {
        workers: (0..n_workers)
            .map(|i| WorkerSlot {
                api_sock: rundir
                    .join(format!("worker{i}.sock"))
                    .to_string_lossy()
                    .to_string(),
                vmstate: rundir.join(format!("worker{i}.vmstate")),
                mem: rundir.join(format!("worker{i}.mem")),
            })
            .collect(),
    };
    let handles = ignite_ramp(&plan);
    log.push_str(&format!(
        "[{:>8.3} ms] {n_workers} worker resumes in flight\n",
        t0.elapsed().as_secs_f64() * 1e3
    ));

    #[cfg(unix)]
    {
        use std::os::unix::net::UnixStream;
        if let Ok(mut s) = UnixStream::connect("/tmp/blitz-vsock.sock_7311") {
            s.write_all(sql.as_bytes())?;
            log.push_str(&format!(
                "[{:>8.3} ms] query delivered\n",
                t0.elapsed().as_secs_f64() * 1e3
            ));
        } else {
            log.push_str("warning: coordinator vsock unreachable\n");
        }
    }
    #[cfg(not(unix))]
    {
        let _ = sql;
        log.push_str("warning: vsock unavailable on this platform\n");
    }

    for h in handles {
        let _ = h.join();
    }
    log.push_str(&format!(
        "[{:>8.3} ms] ramp complete\n",
        t0.elapsed().as_secs_f64() * 1e3
    ));
    Ok(log)
}

fn http_json(code: &str, body: &str) -> String {
    format!(
        "HTTP/1.1 {code}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
        body.len(),
        body
    )
}

fn handle_client(
    mut stream: TcpStream,
    snapdir: PathBuf,
    rundir: PathBuf,
    default_workers: usize,
    api_token: Option<String>,
    metrics: Arc<Metrics>,
    warehouse: String,
) -> std::io::Result<()> {
    stream.set_read_timeout(Some(std::time::Duration::from_secs(30)))?;
    let mut buf = vec![0u8; 65536];
    let n = stream.read(&mut buf)?;
    let req = String::from_utf8_lossy(&buf[..n]);

    if req.starts_with("GET /health") {
        let body = format!(
            "{{\"status\":\"ok\",\"warehouse\":{}}}",
            json_escape(&warehouse)
        );
        stream.write_all(http_json("200 OK", &body).as_bytes())?;
        return Ok(());
    }

    if req.starts_with("GET /metrics") {
        let body = metrics.prometheus();
        let resp = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4\r\nContent-Length: {}\r\n\r\n{}",
            body.len(),
            body
        );
        stream.write_all(resp.as_bytes())?;
        return Ok(());
    }

    if !req.starts_with("POST /v1/query") {
        stream.write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n")?;
        return Ok(());
    }

    metrics.queries_total.fetch_add(1, Ordering::Relaxed);
    if !authorized(&req, &api_token) {
        metrics
            .queries_unauthorized
            .fetch_add(1, Ordering::Relaxed);
        let body = "{\"status\":\"error\",\"message\":\"unauthorized\"}";
        stream.write_all(http_json("401 Unauthorized", body).as_bytes())?;
        return Ok(());
    }

    let body = req.split("\r\n\r\n").nth(1).unwrap_or("").to_string();
    let (sql, workers) = parse_json_sql(&body).ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid JSON body")
    })?;
    let n_workers = workers.unwrap_or(default_workers);

    spawn_firecracker(&rundir, &snapdir, n_workers)?;
    match run_query(&rundir, n_workers, &sql) {
        Ok(log) => {
            metrics.queries_ok.fetch_add(1, Ordering::Relaxed);
            let body = format!("{{\"status\":\"ok\",\"log\":{}}}", json_escape(&log));
            metrics
                .bytes_out
                .fetch_add(body.len() as u64, Ordering::Relaxed);
            stream.write_all(http_json("200 OK", &body).as_bytes())?;
        }
        Err(e) => {
            metrics.queries_err.fetch_add(1, Ordering::Relaxed);
            let body = format!(
                "{{\"status\":\"error\",\"message\":{}}}",
                json_escape(&e.to_string())
            );
            stream.write_all(http_json("500 Internal Server Error", &body).as_bytes())?;
        }
    }
    Ok(())
}

fn json_escape(s: &str) -> String {
    format!(
        "\"{}\"",
        s.replace('\\', "\\\\")
            .replace('"', "\\\"")
            .replace('\n', "\\n")
    )
}

fn main() {
    let snapdir = PathBuf::from(env("BLITZ_SNAPDIR", "/var/lib/blitz/snapshots"));
    let rundir = PathBuf::from(env("BLITZ_RUNDIR", "/run/blitz"));
    let default_workers: usize = env("BLITZ_WORKERS", "6").parse().unwrap_or(6);
    let listen = env("BLITZ_LISTEN", "0.0.0.0:8080");
    let api_token = std::env::var("BLITZ_API_TOKEN").ok().filter(|s| !s.is_empty());
    let warehouse = env("BLITZ_WAREHOUSE", "file:///var/lib/blitz/warehouse");
    let metrics = Arc::new(Metrics::new());

    if api_token.is_some() {
        eprintln!("[blitz-queryd] auth ENABLED (Bearer token)");
    } else {
        eprintln!("[blitz-queryd] WARNING: BLITZ_API_TOKEN unset — query API is open");
    }
    eprintln!("[blitz-queryd] snapdir={snapdir:?} listen={listen} warehouse={warehouse}");

    let listener = TcpListener::bind(&listen).expect("bind");
    for stream in listener.incoming().flatten() {
        let snapdir = snapdir.clone();
        let rundir = rundir.clone();
        let api_token = api_token.clone();
        let metrics = metrics.clone();
        let warehouse = warehouse.clone();
        std::thread::spawn(move || {
            let _ = handle_client(
                stream,
                snapdir,
                rundir,
                default_workers,
                api_token,
                metrics,
                warehouse,
            );
        });
    }
}
