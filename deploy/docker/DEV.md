# Local Docker Dev (Windows)

Run Alpheidae on **Docker Desktop for Windows** without installing Rust or Linux/KVM.
This uses **simulated** multi-node workers (threads + TCP), not Firecracker microVMs.

## Prerequisites

1. [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed and running
2. WSL2 backend enabled (Docker Desktop default on Windows)
3. At least **4 GB RAM** allocated to Docker (Settings → Resources)

First build takes ~5–10 minutes (compiles Rust from source). Later runs use cache.

## Quick start

From PowerShell in the repo root:

```powershell
# Build images (once, or after code changes)
docker compose build

# Cold-start / liquid clustering / ramp benchmark (~30 s)
docker compose run --rm blitz-demo

# Full lakehouse demo: meta + Iceberg + joins (~1–2 min)
docker compose run --rm iceberg-demo
```

Or use the helper script:

```powershell
.\scripts\dev.ps1 build
.\scripts\dev.ps1 demo
.\scripts\dev.ps1 iceberg
```

## What runs inside Docker

| Command | What it does |
|---------|--------------|
| `blitz-demo` | 8M-row benchmark; simulates 6 microVM workers with 5 ms resume delay |
| `iceberg-demo` | 3-node in-process meta, Iceberg warehouse, broadcast + shuffle joins |
| `--profile meta up` | 3 **separate containers** for distributed meta testing |

**Not included:** Firecracker, KVM, real microVM snapshot resume. Those require bare-metal Linux (see `deploy/README.md` for AWS).

## Interactive development

Mount your source and run `cargo` inside a Linux container (avoids Windows Rust toolchain issues):

```powershell
docker compose --profile dev run --rm dev
# inside container:
cargo build --release -p blitz-demo
cargo test
./target/release/blitz-demo
```

Named volumes cache `target/` and the Cargo registry between sessions so rebuilds are faster.

## Distributed meta cluster (optional)

Test the catalog across separate containers on the Docker network:

```powershell
docker compose --profile meta up
# meta-0 → localhost:7401, meta-1 → :7402, meta-2 → :7403
```

Stop with `Ctrl+C` or `docker compose --profile meta down`.

## Troubleshooting (Windows)

| Problem | Fix |
|---------|-----|
| `docker: command not found` | Install/start Docker Desktop |
| Build OOM / killed | Increase Docker RAM to 6–8 GB |
| Slow first build | Normal — Rust LTO release build is heavy |
| `volume mount permission denied` | Enable file sharing for the drive in Docker Desktop settings |
| Line ending issues in scripts | Use `dev.ps1`; shell scripts run inside Linux containers |

## Files

```
docker-compose.yml          Compose services + profiles
deploy/docker/Dockerfile.dev  Multi-stage build (runtime + dev)
scripts/dev.ps1             Windows helper commands
.dockerignore                 Keeps build context small
```
