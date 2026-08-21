# Alpheidae local dev helper (Windows PowerShell)
param(
    [Parameter(Position = 0)]
    [ValidateSet("build", "demo", "iceberg", "meta", "shell", "help")]
    [string]$Command = "help"
)

$ErrorActionPreference = "Stop"

function Require-Docker {
    if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
        Write-Error "Docker not found. Install Docker Desktop: https://www.docker.com/products/docker-desktop/"
    }
    docker info 2>&1 | Out-Null
    if ($LASTEXITCODE -ne 0) {
        Write-Error "Docker is not running. Start Docker Desktop and retry."
    }
}

switch ($Command) {
    "build" {
        Require-Docker
        docker compose build
    }
    "demo" {
        Require-Docker
        docker compose run --rm blitz-demo
    }
    "iceberg" {
        Require-Docker
        docker compose run --rm iceberg-demo
    }
    "meta" {
        Require-Docker
        Write-Host "Starting 3-node meta cluster (Ctrl+C to stop)..."
        docker compose --profile meta up
    }
    "shell" {
        Require-Docker
        docker compose --profile dev run --rm dev
    }
    default {
        Write-Host @"
Alpheidae Docker dev (Windows)

  .\scripts\dev.ps1 build     Build images
  .\scripts\dev.ps1 demo      Run blitz-demo benchmark
  .\scripts\dev.ps1 iceberg   Run iceberg-demo lakehouse demo
  .\scripts\dev.ps1 meta      Start 3-container meta cluster
  .\scripts\dev.ps1 shell     Interactive Rust dev container

See deploy/docker/DEV.md for details.
"@
    }
}
