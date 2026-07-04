//! blitz-igniter — the only thing that runs on the host when a query arrives.
//!
//! Query path:
//!   1. resume coordinator snapshot           (~3–8 ms, one HTTP PUT)
//!   2. forward the SQL over vsock            (engine starts executing)
//!   3. in parallel, resume N worker snapshots (they join the ramp themselves)
//!
//! Usage:
//!   blitz-igniter <snapshot_dir> <workers> "<SQL>"
//!
//! snapshot_dir layout:
//!   coord.sock coord.vmstate coord.mem
//!   worker{i}.sock worker{i}.vmstate worker{i}.mem

use blitz_boot::{ignite_ramp, Firecracker, RampPlan, WorkerSlot};
use std::path::PathBuf;
use std::time::Instant;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 4 {
        eprintln!("usage: blitz-igniter <snapshot_dir> <n_workers> <sql>");
        std::process::exit(2);
    }
    let dir = PathBuf::from(&args[1]);
    let n: usize = args[2].parse().expect("n_workers");
    let sql = &args[3];

    let t0 = Instant::now();

    // 1. Coordinator first — it must exist before anything else.
    let coord = Firecracker::connect(dir.join("coord.sock").to_string_lossy().to_string());
    coord
        .snapshot_resume(&dir.join("coord.vmstate"), &dir.join("coord.mem"), None)
        .expect("coordinator resume failed");
    println!("[{:>8.3} ms] coordinator resumed", t0.elapsed().as_secs_f64() * 1e3);

    // 2. Fire the worker ramp WITHOUT waiting for it.
    let plan = RampPlan {
        workers: (0..n)
            .map(|i| WorkerSlot {
                api_sock: dir.join(format!("worker{i}.sock")).to_string_lossy().to_string(),
                vmstate: dir.join(format!("worker{i}.vmstate")),
                mem: dir.join(format!("worker{i}.mem")),
            })
            .collect(),
    };
    let handles = ignite_ramp(&plan);
    println!(
        "[{:>8.3} ms] {n} worker resumes in flight (not waiting)",
        t0.elapsed().as_secs_f64() * 1e3
    );

    // 3. Hand the query to the coordinator over its vsock UDS.
    //    (Firecracker exposes guest vsock port P at <uds>_<P>.)
    use std::io::Write;
    use std::os::unix::net::UnixStream;
    if let Ok(mut s) = UnixStream::connect("/tmp/blitz-vsock.sock_7311") {
        s.write_all(sql.as_bytes()).ok();
        println!(
            "[{:>8.3} ms] query delivered — engine is executing",
            t0.elapsed().as_secs_f64() * 1e3
        );
    } else {
        eprintln!("could not reach coordinator vsock (is the snapshot warm?)");
    }

    for h in handles {
        match h.join() {
            Ok(Ok(())) => {}
            Ok(Err(e)) => eprintln!("worker resume error: {e}"),
            Err(_) => eprintln!("worker resume thread panicked"),
        }
    }
    println!("[{:>8.3} ms] full ramp resumed", t0.elapsed().as_secs_f64() * 1e3);
}
