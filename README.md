# ⚡ Alpheidae

A distributed, vectorized SQL engine in Rust built around one constraint: the
engine is **completely off when idle**, and the time from query arrival to the
first morsel executing is **single-digit milliseconds**.

How: every node is a Firecracker microVM that booted once (ever), warmed up,
and was snapshotted. A query resumes the coordinator snapshot (~3–8 ms) and
starts executing on it immediately, single-node, while worker snapshots resume
in parallel and join the morsel queue mid-query. The cluster forms *inside*
the query. See `ARCHITECTURE.md` for the full design, including the honest
physics of "physically off" (Tier A: guest off / host on → milliseconds;
Tier B: host at zero watts → seconds, bounded by firmware, mitigated with
LinuxBoot + userfaultfd lazy snapshot loading).

## Run the demos (no virtualization needed)

```
cargo build --release
./target/release/alpheidae-demo      # cold-start / liquid-clustering / ramp benchmark
./target/release/iceberg-demo    # full lakehouse path (below)
```

`alpheidae-demo` generates 8M rows, demonstrates liquid clustering (zone-map
pruning going from 0/123 to 115/123 blocks skipped after a workload-driven
Z-order recluster), then runs the same query three ways: single node, classic
wait-for-cluster MPP, and ramped — printing the millisecond timeline of the
first morsel and of each simulated 5 ms-resume worker joining.

`iceberg-demo` runs the full lakehouse stack end to end:

1. starts a **3-node replicated metadata service** (the Iceberg catalog),
   commits with majority ack, then kills the leader, promotes a follower with
   epoch fencing, and shows the catalog surviving;
2. writes a real **Iceberg v2 warehouse** — BlitzCol data files (delta +
   varint + LZ4, 7.4x compression), Avro manifests + manifest lists, versioned
   `metadata.json` — and registers the tables through the replicated catalog;
3. runs a **broadcast hash join** with two-phase aggregation: the optimizer's
   `EXPLAIN` shows predicate pushdown, projection pruning, manifest-bounds
   file pruning (6 of 8 files eliminated before any data I/O) and page-level
   late materialization (79% of on-disk bytes never decoded);
4. runs a **shuffle hash join** where the cost model rejects broadcast
   (estimated build side over the 4 MB threshold) and hash-partitions both
   sides across 16 partitions instead.

## Run it on real microVMs (KVM host)

```
cd microvm
./make_rootfs.sh              # 2-file BlitzOS image (static musl binaries)
# build the kernel from blitzos.config (tinyconfig + fragment)
./make_snapshot.sh coord coordinator 4 2048    # one-time golden boot
for i in 0 1 2 3 4 5; do ./make_snapshot.sh worker$i worker 4 2048; done
# ... everything is now OFF ...
./resume_and_query.sh 6 "SELECT SUM(c1) FROM t WHERE c0 > 95000000 GROUP BY c2"
```

## Layout

```
crates/alpheidae-core      vectorized kernels (branch-free, auto-SIMD)
crates/alpheidae-cluster   liquid clustering + zone maps + Z-order
crates/alpheidae-sql       SQL front end (session-1 aggregate-scan subset)
crates/alpheidae-exec      ramped morsel-stealing distributed executor (TCP)
crates/alpheidae-boot      firecracker control plane, PID-1 init, igniter
crates/alpheidae-format    BlitzCol columnar format: compression, page zone maps,
                       late-materialized reader
crates/alpheidae-avro      Avro object-container codec (Iceberg manifests)
crates/alpheidae-iceberg   Iceberg v2 metadata, manifests, commits, file pruning
crates/alpheidae-meta      replicated metadata service / Iceberg catalog
crates/alpheidae-plan      SQL parser + cost-based optimizer (joins, pushdown)
crates/alpheidae-engine    executor: late-mat scans, broadcast/shuffle hash joins,
                       two-phase agg, ramped scheduler
crates/alpheidae-demo      alpheidae-demo + iceberg-demo binaries
microvm/               kernel config + rootfs/snapshot/query scripts
```
