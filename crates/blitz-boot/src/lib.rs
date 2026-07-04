//! blitz-boot — Firecracker lifecycle control for millisecond cold starts.
//!
//! The power model
//! ---------------
//! "Engine fully off" means: zero guest vCPUs scheduled, zero engine
//! processes running. What persists is a *snapshot pair* per VM:
//!
//!   vmstate file  (~10 KB)   — device + vCPU register state
//!   memory file   (RAM size) — guest memory image
//!
//! Resume path (measured by AWS/Firecracker at low single-digit ms):
//!
//!   PUT /snapshot/load { mem_backend, snapshot_path, resume_vm: true }
//!
//! Two latency tiers for the memory file:
//!   * Tier A (warm host): memory file sits in the host page cache /
//!     hugetlbfs → resume ≈ 3–8 ms, first instruction immediately.
//!   * Tier B (cold host, host was physically powered off): memory file on
//!     NVMe, mapped with a userfaultfd backend so pages stream in lazily —
//!     the guest runs before its memory has finished loading. Host power-on
//!     itself is the floor here (LinuxBoot/coreboot + kexec gets a server
//!     from power to kernel in ~1–2 s; commodity BIOS POST is 30 s+ and is
//!     the thing to engineer away, not the hypervisor).
//!
//! This module is intentionally dependency-free: raw HTTP/1.1 over the
//! Firecracker API unix socket.

use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::path::Path;

pub struct Firecracker {
    api_sock: String,
}

impl Firecracker {
    pub fn connect(api_sock: impl Into<String>) -> Self {
        Firecracker { api_sock: api_sock.into() }
    }

    fn put(&self, path: &str, body: &str) -> std::io::Result<u16> {
        let mut s = UnixStream::connect(&self.api_sock)?;
        let req = format!(
            "PUT {path} HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        s.write_all(req.as_bytes())?;
        let mut resp = String::new();
        s.read_to_string(&mut resp)?;
        let code = resp
            .split_whitespace()
            .nth(1)
            .and_then(|c| c.parse().ok())
            .unwrap_or(0);
        if code >= 300 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("firecracker {path} -> {code}: {resp}"),
            ));
        }
        Ok(code)
    }

    /// Pause the running VM (vCPUs stop executing — guest is "off").
    pub fn pause(&self) -> std::io::Result<()> {
        self.put("/vm", r#"{"state":"Paused"}"#).map(|_| ())
    }

    pub fn resume(&self) -> std::io::Result<()> {
        self.put("/vm", r#"{"state":"Resumed"}"#).map(|_| ())
    }

    /// Take a full snapshot of a paused VM.
    pub fn snapshot_create(&self, vmstate: &Path, mem: &Path) -> std::io::Result<()> {
        let body = format!(
            r#"{{"snapshot_type":"Full","snapshot_path":"{}","mem_file_path":"{}"}}"#,
            vmstate.display(),
            mem.display()
        );
        self.put("/snapshot/create", &body).map(|_| ())
    }

    /// THE hot path: load a snapshot and resume in one call.
    /// `uffd_sock`: pass Some(path) to use a userfaultfd page server
    /// (lazy memory loading — guest executes before RAM finishes loading).
    pub fn snapshot_resume(
        &self,
        vmstate: &Path,
        mem: &Path,
        uffd_sock: Option<&Path>,
    ) -> std::io::Result<()> {
        let backend = match uffd_sock {
            Some(u) => format!(
                r#"{{"backend_type":"Uffd","backend_path":"{}"}}"#,
                u.display()
            ),
            None => format!(
                r#"{{"backend_type":"File","backend_path":"{}"}}"#,
                mem.display()
            ),
        };
        let body = format!(
            r#"{{"snapshot_path":"{}","mem_backend":{backend},"resume_vm":true}}"#,
            vmstate.display()
        );
        self.put("/snapshot/load", &body).map(|_| ())
    }

    /// First-boot configuration (only used once, when *creating* the golden
    /// snapshot — never on the query path).
    pub fn configure_and_boot(
        &self,
        kernel: &Path,
        rootfs: &Path,
        vcpus: u32,
        mem_mib: u32,
        boot_args: &str,
    ) -> std::io::Result<()> {
        self.put(
            "/boot-source",
            &format!(
                r#"{{"kernel_image_path":"{}","boot_args":"{boot_args}"}}"#,
                kernel.display()
            ),
        )?;
        self.put(
            "/drives/rootfs",
            &format!(
                r#"{{"drive_id":"rootfs","path_on_host":"{}","is_root_device":true,"is_read_only":true}}"#,
                rootfs.display()
            ),
        )?;
        self.put(
            "/machine-config",
            &format!(r#"{{"vcpu_count":{vcpus},"mem_size_mib":{mem_mib},"smt":false}}"#),
        )?;
        // vsock for the coordinator<->host control channel
        self.put(
            "/vsock",
            r#"{"guest_cid":3,"uds_path":"/tmp/blitz-vsock.sock"}"#,
        )?;
        self.put("/actions", r#"{"action_type":"InstanceStart"}"#).map(|_| ())
    }
}

/// Ramp plan: which worker snapshots to resume, staggered or all at once.
pub struct RampPlan {
    pub workers: Vec<WorkerSlot>,
}

pub struct WorkerSlot {
    pub api_sock: String,
    pub vmstate: std::path::PathBuf,
    pub mem: std::path::PathBuf,
}

/// Fire all worker resumes in parallel. Each is one HTTP PUT; the guests are
/// executing within single-digit milliseconds and will dial the coordinator
/// themselves (the coordinator address is baked into the snapshot via MMDS).
pub fn ignite_ramp(plan: &RampPlan) -> Vec<std::thread::JoinHandle<std::io::Result<()>>> {
    plan.workers
        .iter()
        .map(|w| {
            let fc = Firecracker::connect(w.api_sock.clone());
            let vmstate = w.vmstate.clone();
            let mem = w.mem.clone();
            std::thread::spawn(move || fc.snapshot_resume(&vmstate, &mem, None))
        })
        .collect()
}
