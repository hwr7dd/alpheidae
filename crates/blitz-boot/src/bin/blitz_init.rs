//! blitz-init — PID 1 of the BlitzOS guest image.
//!
//! This *is* the "custom quick start OS": the rootfs contains exactly two
//! files (this binary and the engine binary). No systemd, no shell, no
//! udev, no getty. Kernel → this → engine listening, in well under 10 ms of
//! guest time on first boot — and first boot only ever happens once, when
//! the golden snapshot is created. Every query-path start is a snapshot
//! resume that skips boot entirely.
//!
//! Boot args we rely on (see microvm/kernel.config):
//!   console=ttyS0 reboot=k panic=1 pci=off nomodules quiet
//!   init=/blitz-init blitz.role=coordinator blitz.coord=10.0.0.1:7311

use std::ffi::CString;

fn mount(src: &str, dst: &str, fstype: &str) {
    let s = CString::new(src).unwrap();
    let d = CString::new(dst).unwrap();
    let t = CString::new(fstype).unwrap();
    unsafe {
        libc::mkdir(d.as_ptr(), 0o755);
        libc::mount(s.as_ptr(), d.as_ptr(), t.as_ptr(), 0, std::ptr::null());
    }
}

fn cmdline_val(key: &str) -> Option<String> {
    let cmdline = std::fs::read_to_string("/proc/cmdline").ok()?;
    cmdline
        .split_whitespace()
        .find_map(|kv| kv.strip_prefix(&format!("{key}=")).map(|v| v.to_string()))
}

fn main() {
    // Minimal pseudo-filesystems; nothing else.
    mount("proc", "/proc", "proc");
    mount("sysfs", "/sys", "sysfs");
    mount("devtmpfs", "/dev", "devtmpfs");
    // RAM-backed scratch for spill files / shuffle buffers.
    mount("tmpfs", "/scratch", "tmpfs");

    let role = cmdline_val("blitz.role").unwrap_or_else(|| "worker".into());
    let coord = cmdline_val("blitz.coord").unwrap_or_else(|| "10.0.0.1:7311".into());

    // After a snapshot RESUME (the real query path) this process is already
    // alive and the engine is already warm — none of the above re-runs.
    // The engine just receives the query over vsock and goes.
    let engine = CString::new("/blitz-engine").unwrap();
    let role_arg = CString::new(format!("--role={role}")).unwrap();
    let coord_arg = CString::new(format!("--coordinator={coord}")).unwrap();
    let argv = [engine.as_ptr(), role_arg.as_ptr(), coord_arg.as_ptr(), std::ptr::null()];
    unsafe {
        libc::execv(engine.as_ptr(), argv.as_ptr());
    }
    // execv only returns on failure.
    eprintln!("blitz-init: failed to exec /blitz-engine");
    unsafe { libc::reboot(libc::LINUX_REBOOT_CMD_POWER_OFF) };
}
