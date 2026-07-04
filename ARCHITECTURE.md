# Alpheidae Architecture

A distributed, vectorized SQL engine in Rust whose defining property is the time from "everything is off" to "the query is executing": single-digit milliseconds. The engine consumes zero compute when idle; the cluster does not exist between queries and materializes *inside* each query.

## The cold-start problem, stated precisely

A classical MPP engine (StarRocks, Trino, ClickHouse cluster) assumes a standing cluster. Powering one off and back on costs seconds to minutes: firmware POST, kernel boot, service init, catalog warm-up, cluster membership convergence. The naive serverless fix — boot a VM per query — still pays kernel plus userspace boot on the critical path.

Alpheidae removes boot from the query path entirely. The unit of compute is a Firecracker microVM that booted exactly once in its life, at image-build time. After that one boot, the engine inside it warms itself (loads catalog metadata, opens its sockets, touches its working pages), the VM is paused, and a snapshot is taken: a tiny vmstate file holding vCPU and device state, plus a memory file holding guest RAM. From that moment the VM is off in every sense that matters — no vCPU is scheduled, no process of the engine exists. The query path is `PUT /snapshot/load {resume_vm: true}`, which Firecracker completes in roughly 3–8 ms, after which the guest continues from the exact instruction it was paused at, with its caches of metadata still hot because they were frozen hot.

### What "physically off" can mean, honestly

There are two power tiers, and it is important to be precise about which one you are buying.

**Tier A — guest off, host on.** The host runs nothing but idle Firecracker processes (or not even those; a bare `firecracker` process spawns in ~8 ms). Snapshot memory files are pinned in the host page cache or hugetlbfs. Resume-to-first-instruction is 3–8 ms; query-to-first-morsel is under 10 ms end to end. This is the configuration this repo implements, and it is what every production "scale to zero in milliseconds" system (e.g. Lambda SnapStart-class designs) actually does. The host's idle power draw is the cost of the millisecond class.

**Tier B — host physically off.** If the machine must be drawn at zero watts, something external (Wake-on-LAN packet from a gateway, BMC/IPMI, a smart PDU) has to apply power, and physics sets the floor: DRAM is volatile, so "saved state in RAM" cannot survive true power-off — the snapshot must live on NVMe (or persistent memory such as CXL/NVDIMM, which genuinely can hold state across power loss). The dominant cost is then firmware: commodity BIOS/UEFI POST is 10–60 s and must be engineered away with coreboot/LinuxBoot, which gets power-button-to-Linux into the 1–2 s range on server hardware. From there, kexec into a pre-staged kernel plus userfaultfd lazy-loading of the snapshot memory file means the guest executes before its RAM has finished streaming from NVMe. A realistic Tier-B floor is therefore single-digit *seconds*, not milliseconds, and the bottleneck is firmware, not this engine. The recommended design is a tiered fleet: a Tier-A "pilot light" host (one small machine, snapshots in RAM) answers instantly and begins executing, while Tier-B hosts power up behind it and join the ramp if the query is large enough to want them.

## Ramped execution: the cluster forms inside the query

The second pillar is that nothing ever waits for the cluster. The timeline of a query is:

```
t = 0        SQL arrives at the igniter (host-side, ~200 LOC, no deps)
t ≈ 0        igniter PUTs /snapshot/load on the coordinator slot
t ≈ 3–8 ms   coordinator guest is executing; SQL delivered over vsock
t ≈ +0.1 ms  plan ready: zone maps prune the morsel list
t ≈ +0.2 ms  FIRST MORSEL EXECUTING on the coordinator's own cores
   (in parallel since t≈0: igniter fired N worker /snapshot/load PUTs)
t ≈ 6–15 ms  workers resume, dial the coordinator, begin stealing morsels
t = end      coordinator merges partial aggregates, returns rows
```

This works because work distribution is pull-based morsel stealing rather than push-based partitioning. The table is pre-split into fixed 64K-row blocks; the post-pruning block list is a shared LIFO queue. The coordinator's local threads start draining it immediately. A worker that joins 8 ms late simply takes fewer morsels; a worker that never joins costs nothing; a worker that dies mid-morsel has its morsel returned to the queue (the coordinator detects the broken connection). There is no repartitioning step, no membership barrier, no epoch. Elasticity is a property of the scheduler, not an event.

In shared-storage mode (the StarRocks shared-data analogue, and the production default) only morsel IDs cross the wire: each worker reads blocks from shared storage (object store with local NVMe cache, or in this repo's demo, shared memory) so the coordinator is never a data bottleneck. A shared-nothing fallback exists in which the coordinator inlines the touched columns of each block.

The aggregation model is the standard two-phase one: each node computes thread-local partial aggregates (SUM/COUNT/MIN/MAX, grouped or scalar) and the coordinator merges partials, so the merge cost is proportional to group cardinality, not row count.

## Speed inside a morsel

Execution is vectorized in the DuckDB/StarRocks sense rather than tuple-at-a-time. Columns are flat `i64` vectors processed by branch-free kernels that LLVM auto-vectorizes to AVX2/AVX-512: the filter kernel emits a selection vector via a compare-and-masked-index-store loop with no branches, and aggregation runs eight independent accumulators to break the loop-carried dependency chain and saturate the vector ALUs. Grouped aggregation takes a flat-array fast path when the key domain is small (the common case after dictionary encoding) and falls back to a hash table otherwise. With `lto = "fat"`, `codegen-units = 1` and `panic = "abort"`, the measured demo numbers in this container are a 2 ms end-to-end grouped, filtered aggregate over 8M rows on a single core — and most of that win comes from the layer below.

## Liquid clustering

Zone maps (per-block min/max per column) are the cheapest pruning structure that exists, but they are worthless if the physical layout is random: every block's range covers the whole domain and nothing prunes. Liquid clustering makes the layout chase the workload. The engine records which columns queries actually filter on; a reclusterer picks the top predicate columns as the current clustering keys, normalizes them, and re-sorts rows along a Z-order space-filling curve over those keys, rebuilding fixed-size blocks and their zone maps. Because the keys are chosen from observed predicates rather than declared at table creation, the clustering migrates when the workload does — the production version reclusters incrementally, targeting only the block runs with the worst zone overlap, exactly as Delta's liquid clustering does.

The demo quantifies the effect: on a random layout the mean zone-overlap metric is 1.0 (useless) and zero of 123 blocks prune for a selective predicate; after one recluster on the two hottest keys the overlap drops to 0.011 and 115 of 123 blocks are skipped before a single byte is scanned. Pruning compounds with everything above it: fewer morsels means less to ramp.

## BlitzOS: the custom quick-start OS

The guest image is deliberately almost nothing. The kernel is a `tinyconfig` build with only virtio-mmio block/net/vsock, ext4, proc/sys/tmpfs/devtmpfs and hugepage support (see `microvm/blitzos.config`); PCI, ACPI, modules, USB and everything else are off, yielding a <5 MB vmlinux that boots in tens of milliseconds. Userspace is two static musl binaries: `alpheidae-init`, a Rust PID 1 that mounts the pseudo-filesystems, reads its role and the coordinator address from the kernel command line, and `execv`s the engine; and the engine itself. There is no shell, no systemd, no libc.so, no init scripts. But the deeper point is that even this fast boot is *off* the query path — it is paid once at snapshot-creation time. Snapshot resume skips the kernel entirely; the OS's job is only to be small enough that the memory file stays small (smaller file, faster Tier-B streaming, cheaper page-cache pinning in Tier A).

## Crate map

```
alpheidae-core      columnar blocks, branch-free SIMD-friendly kernels
alpheidae-cluster   zone maps, Z-order curve, workload stats, liquid recluster
alpheidae-sql       parser for the session-1 aggregate-scan subset
alpheidae-exec      morsel queue, ramped TCP scheduler, wire protocol, worker loop
alpheidae-boot      firecracker API client (snapshot create/resume), alpheidae-init
                PID 1, alpheidae-igniter query-path CLI
alpheidae-format    BlitzCol (.blitz) columnar file format: delta+varint+LZ4,
                dictionary Utf8, page zone maps, late-materialized reader
alpheidae-avro      Avro object-container reader/writer (Iceberg's manifest format)
alpheidae-iceberg   Iceberg v2: metadata.json, Avro manifests + manifest lists,
                snapshot commits, manifest-bounds file pruning
alpheidae-meta      replicated metadata service = the Iceberg catalog
                (majority-commit log, epoch fencing, failover)
alpheidae-plan      SQL front end + cost-based optimizer (joins, pushdown,
                projection pruning, broadcast-vs-shuffle decision)
alpheidae-engine    executor: late-materialized Iceberg scans, broadcast and
                shuffle hash joins, two-phase agg, ramped morsel scheduler
alpheidae-demo      two binaries: alpheidae-demo (cold-start/ramp benchmark) and
                iceberg-demo (end-to-end lakehouse query path)
microvm/        kernel config, rootfs builder, snapshot + query-path scripts
```

## The lakehouse stack (session 2)

The engine is Iceberg-primary: a table is its `metadata.json`, its Avro
manifest lists and manifests, and its data files. Alpheidae owns no proprietary
table state — only the catalog pointer, which lives in the replicated
metadata service. This is the StarRocks shared-data posture taken to its
conclusion: the object store is the database; the engine is a stateless,
snapshot-resumable view over it. That is also what makes millisecond cold
start coherent — there is nothing on the engine to warm up.

**BlitzCol file format (`alpheidae-format`).** A `.blitz` file is rowgroups of
8192-row pages. Int64 chunks are delta-encoded, zigzag-varint-packed, then
LZ4-block-compressed (the LZ4 codec is hand-written because crates.io's
lz4_flex requires rustc 1.81 and this toolchain is 1.75 — it emits
spec-compatible LZ4 block format and round-trips under test). Utf8 chunks are
chunk-local dictionaries with u32 codes. Every i64 page carries a min/max
zone map in the footer. The demo's 4M-row sales fact compresses 7.4x.

**Late materialization (`Reader`).** Predicates are evaluated first, touching
only predicate columns; a page whose zone map cannot satisfy the predicate is
never decompressed (a Utf8 equality probe that misses the dictionary prunes
the whole chunk without decoding a single page). The surviving selection
vector then drives `gather`, which decodes only the pages of only the
projected columns that contain selected rows. Global counters expose
decoded-vs-skipped bytes; the demo shows three pruning tiers stacking:
manifest bounds drop 6 of 8 files, page zone maps drop 48 more pages inside
the survivors, and 79% of the table's on-disk bytes are never decoded.

**Iceberg v2 (`alpheidae-iceberg` + `alpheidae-avro`).** Real spec shapes, not
look-alikes: versioned `v{N}.metadata.json`, manifest lists and manifest
entries as Avro object-container files with deflate codec and spec field
names, lower/upper bounds serialized per the Iceberg single-value binary
spec, snapshot commits that write a new metadata version. `plan_files` walks
snapshot → manifest list → manifests → data files, and `file_prunes` does
min/max file elimination from manifest bounds before any data file is opened.
The one honest divergence: `file_format` is `"BLITZCOL"` — reading foreign
Parquet data files would need a Parquet decoder this codebase doesn't have.

**Replicated metadata (`alpheidae-meta`).** In a shared-data lakehouse the data
is replicated by the object store; what the engine must replicate is the
*pointer* — which `metadata.json` is current. The service is a 3-node
replicated log: the leader appends, replicates to peers, and commits on
majority ack; followers reject any leader with a stale epoch (fencing), so a
deposed leader cannot commit a split-brain write. The demo kills the leader
mid-session, promotes a follower to epoch 2, and shows reads and commits
continuing. Leader *election* is deliberately a deterministic promotion call
rather than Raft's randomized timeouts — the fencing and quorum logic, where
correctness lives, is real; the election heuristic is not the interesting
part and is documented as such.

**Optimizer (`alpheidae-plan`).** Parsing through physical planning: name
resolution across aliases, predicate pushdown into scans, projection pruning
(a scan decodes only columns the plan consumes — predicate-only columns are
touched solely during predicate evaluation), manifest-bounds file pruning at
plan time, selectivity estimation from file bounds (uniform-domain ranges,
0.1 for string equality), and a costed join decision: the smaller estimated
side builds, and it broadcasts if its estimated bytes (est rows × 24B) fall
under 4 MB, otherwise the join shuffles across 16 hash partitions. `EXPLAIN`
output shows the full tree with the rationale inline.

**Executor (`alpheidae-engine`).** Morsels are (file, rowgroup) pairs claimed
from an atomic queue — the same ramp as session 1: local threads start at
t=0, microVM workers join mid-query after their resume latency and steal from
the same queue. Broadcast joins build the small side once and share the hash
table; shuffle joins hash-partition both sides into exchange buffers in a map
phase (over a network these buffers are the shuffle streams; in this
single-host demo they are shared memory with identical semantics), then run
one build+probe task per partition on the by-then-fully-ramped cluster.
Aggregation is two-phase: per-task partials merged at the coordinator, then
TopN.

## What is real here and what is scaffolding

The kernels, the liquid reclusterer, the zone-map pruner, the morsel scheduler, the TCP ramp protocol with worker fault handling, the Firecracker API client, the PID-1 init, the igniter, the BlitzCol format with its LZ4 codec, the late-materialized reader, the Avro container codec, the Iceberg metadata/manifest read-write path, the replicated commit log with epoch fencing, the cost-based planner, and the broadcast/shuffle join executor are all real, compiling, running code, exercised end to end by the two demo binaries.

The honest gaps that remain between this and a StarRocks competitor:

- **Parquet.** Iceberg tables in the wild hold Parquet; this engine writes and reads only BlitzCol data files. The manifest/metadata layer is format-agnostic, so a Parquet decoder slots in at the `Reader` seam.
- **Leader election.** Fencing and majority commit are implemented; election is a deterministic promotion call, not Raft.
- **SQL breadth.** One inner join per query, conjunctive predicates, no subqueries, no nulls, i64 + dictionary-Utf8 types only.
- **Single-host shuffle.** The exchange buffers are in-process; the hash-partition protocol is what would cross the wire.
- **One core.** This container has a single CPU, so the ramp timelines demonstrate scheduling mechanics, not multi-core speedups — distributed gains require distributed hardware.

None of these change the cold-start architecture, which is the thesis: an engine whose entire state is an Iceberg pointer in a replicated log can be a paused snapshot at zero marginal cost, resume in milliseconds, and grow its cluster inside the query it is already running.
