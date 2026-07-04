#!/usr/bin/env bash
# make_rootfs.sh — build the BlitzOS root filesystem.
# The entire userspace is TWO static binaries. Nothing else.
set -euo pipefail

OUT=${1:-blitzos.ext4}
SIZE_MB=${2:-64}

echo "==> building static musl binaries"
rustup target add x86_64-unknown-linux-musl 2>/dev/null || true
cargo build --release --target x86_64-unknown-linux-musl \
    -p blitz-boot --bin blitz-init
# blitz-engine = the coordinator/worker binary. In this repo the demo binary
# stands in for it; swap for your production entrypoint.
cargo build --release --target x86_64-unknown-linux-musl \
    -p blitz-demo --bin blitz-demo

echo "==> assembling $OUT (${SIZE_MB} MB ext4)"
dd if=/dev/zero of="$OUT" bs=1M count="$SIZE_MB" status=none
mkfs.ext4 -q -F "$OUT"
MNT=$(mktemp -d)
sudo mount -o loop "$OUT" "$MNT"

T=target/x86_64-unknown-linux-musl/release
sudo cp "$T/blitz-init"  "$MNT/blitz-init"
sudo cp "$T/blitz-demo"  "$MNT/blitz-engine"
sudo mkdir -p "$MNT"/{proc,sys,dev,scratch}
sudo chmod +x "$MNT/blitz-init" "$MNT/blitz-engine"

sudo umount "$MNT"; rmdir "$MNT"
echo "==> done: $OUT"
ls -lh "$OUT"
