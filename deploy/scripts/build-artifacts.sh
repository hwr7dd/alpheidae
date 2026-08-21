#!/usr/bin/env bash
# Build microVM artifacts on a KVM-capable Linux host and upload to S3.
# Run once per release (CI or bare-metal builder instance).
#
# usage: ./build-artifacts.sh <s3-bucket> [workers]
set -euo pipefail

BUCKET=${1:?need s3 bucket name}
WORKERS=${2:-6}
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"

if [[ ! -e /dev/kvm ]]; then
  echo "FATAL: need /dev/kvm — run on bare-metal EC2 or a KVM host"
  exit 1
fi

cd "$ROOT/microvm"
export SNAPDIR=/tmp/blitz-snap-build
rm -rf "$SNAPDIR"
mkdir -p "$SNAPDIR"

echo "==> building rootfs + static binaries"
./make_rootfs.sh

echo "==> building kernel (requires linux source + blitzos.config)"
if [[ ! -f vmlinux-blitzos ]]; then
  echo "Build vmlinux-blitzos from microvm/blitzos.config first — see README"
  exit 1
fi

echo "==> creating golden snapshots"
./make_snapshot.sh coord coordinator 4 2048
for i in $(seq 0 "$((WORKERS - 1))"); do
  ./make_snapshot.sh "worker$i" worker 4 2048
done

echo "==> uploading to s3://${BUCKET}/"
aws s3 sync "$SNAPDIR/" "s3://${BUCKET}/snapshots/"
aws s3 cp vmlinux-blitzos "s3://${BUCKET}/images/vmlinux-blitzos"
aws s3 cp blitzos.ext4 "s3://${BUCKET}/images/blitzos.ext4"
echo "==> done"
