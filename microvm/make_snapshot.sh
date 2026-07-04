#!/usr/bin/env bash
# make_snapshot.sh — create the golden snapshots (run ONCE per image version).
#
# After this script, the fleet is "off": no guest vCPUs exist. What remains
# on disk (or pinned in host page cache / hugetlbfs for Tier-A latency) is
# one (vmstate, mem) pair per VM slot.
#
# usage: ./make_snapshot.sh <slot-name> <role> <vcpus> <mem_mib>
set -euo pipefail

SLOT=${1:-coord}; ROLE=${2:-coordinator}; VCPUS=${3:-4}; MEM=${4:-2048}
KERNEL=vmlinux-blitzos
ROOTFS=blitzos.ext4
SNAPDIR=${SNAPDIR:-/var/lib/blitz/snapshots}
API=/tmp/fc-$SLOT.sock

mkdir -p "$SNAPDIR"
rm -f "$API"

echo "==> launching firecracker for one-time golden boot ($SLOT)"
firecracker --api-sock "$API" &
FCPID=$!
sleep 0.2

put() { curl -sS --unix-socket "$API" -X PUT "http://localhost$1" \
        -H 'Content-Type: application/json' -d "$2" >/dev/null; }

put /boot-source "{
  \"kernel_image_path\": \"$PWD/$KERNEL\",
  \"boot_args\": \"console=ttyS0 reboot=k panic=1 pci=off quiet init=/blitz-init blitz.role=$ROLE blitz.coord=10.0.0.1:7311\"
}"
put /drives/rootfs "{
  \"drive_id\": \"rootfs\", \"path_on_host\": \"$PWD/$ROOTFS\",
  \"is_root_device\": true, \"is_read_only\": true
}"
put /machine-config "{ \"vcpu_count\": $VCPUS, \"mem_size_mib\": $MEM, \"smt\": false }"
put /network-interfaces/eth0 "{
  \"iface_id\": \"eth0\", \"host_dev_name\": \"tap-$SLOT\"
}"
put /vsock "{ \"guest_cid\": 3, \"uds_path\": \"/tmp/blitz-vsock-$SLOT.sock\" }"
put /actions '{ "action_type": "InstanceStart" }'

echo "==> warming up guest (engine loads code, JITs nothing — it's Rust —"
echo "    touches its pages, opens its listening socket, preloads metadata)"
sleep 2   # in production: wait for the engine's READY byte on vsock

echo "==> pausing vCPUs (guest is now 'off') and snapshotting"
put /vm '{ "state": "Paused" }'
put /snapshot/create "{
  \"snapshot_type\": \"Full\",
  \"snapshot_path\": \"$SNAPDIR/$SLOT.vmstate\",
  \"mem_file_path\": \"$SNAPDIR/$SLOT.mem\"
}"

kill $FCPID
echo "==> golden snapshot ready:"
ls -lh "$SNAPDIR/$SLOT".{vmstate,mem}

# Tier-A latency: pin the memory file in host page cache so resume never
# touches disk. (vmtouch, or copy to hugetlbfs/tmpfs.)
command -v vmtouch >/dev/null && vmtouch -t "$SNAPDIR/$SLOT.mem" || true
