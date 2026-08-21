#!/usr/bin/env bash
# Bootstrap a Firecracker host: verify KVM, fetch snapshots from S3, create
# tap devices, start blitz-queryd.
set -euo pipefail

SNAPDIR="${BLITZ_SNAPDIR:-/var/lib/blitz/snapshots}"
ARTIFACTS_BUCKET="${BLITZ_ARTIFACTS_BUCKET:-}"
WORKERS="${BLITZ_WORKERS:-6}"

if [[ ! -e /dev/kvm ]]; then
  echo "FATAL: /dev/kvm not found — Firecracker requires bare-metal EC2 (e.g. m5.metal)"
  exit 1
fi

mkdir -p "$SNAPDIR" /run/blitz /var/lib/blitz/artifacts

# Download golden snapshots + kernel + rootfs from S3 (built by build-artifacts.sh)
if [[ -n "$ARTIFACTS_BUCKET" ]]; then
  echo "==> syncing artifacts from s3://${ARTIFACTS_BUCKET}/"
  aws s3 sync "s3://${ARTIFACTS_BUCKET}/snapshots/" "$SNAPDIR/" --no-progress
  aws s3 sync "s3://${ARTIFACTS_BUCKET}/images/" /var/lib/blitz/artifacts/ --no-progress
fi

# Create tap devices for coordinator + workers (idempotent)
setup_tap() {
  local name=$1
  ip link show "$name" &>/dev/null && return 0
  ip tuntap add dev "$name" mode tap
  ip link set "$name" up
  ip addr add "10.0.0.$2/24" dev "$name" 2>/dev/null || true
}

setup_tap tap-coord 1
for i in $(seq 0 "$((WORKERS - 1))"); do
  setup_tap "tap-worker${i}" $((i + 2))
done

# Enable IP forwarding for guest virtio-net
sysctl -w net.ipv4.ip_forward=1 >/dev/null 2>&1 || true

echo "==> starting blitz-queryd on ${BLITZ_LISTEN:-0.0.0.0:8080}"
exec blitz-queryd
