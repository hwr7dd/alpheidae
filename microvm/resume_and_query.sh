#!/usr/bin/env bash
# resume_and_query.sh — THE QUERY PATH.
#
# Precondition: nothing is running. The "cluster" is a directory of
# snapshot files. This script:
#   1. spawns bare firecracker processes (microseconds each, no VM yet)
#   2. runs blitz-igniter, which:
#        a. PUT /snapshot/load on the coordinator  → executing in ~3–8 ms
#        b. forwards the SQL over vsock            → first morsel runs
#        c. fires all worker snapshot resumes in parallel (not awaited)
#   3. workers join the morsel queue mid-query
#
# usage: ./resume_and_query.sh 6 "SELECT SUM(c1) FROM t WHERE c0 > 95000000 GROUP BY c2"
set -euo pipefail

N=${1:-6}; SQL=${2:?need sql}
SNAPDIR=${SNAPDIR:-/var/lib/blitz/snapshots}
RUN=/tmp/blitz-run; mkdir -p "$RUN"

# 1. Bare hypervisor processes. A firecracker process with no VM configured
#    starts in ~8 ms and uses ~3 MB RSS; you can also keep these pre-spawned
#    at zero CPU cost if you want to shave that too.
for s in coord $(seq -f 'worker%g' 0 $((N-1))); do
  rm -f "$RUN/$s.sock"
  firecracker --api-sock "$RUN/$s.sock" &
done
# Wait for API sockets to appear (~ms).
for s in coord $(seq -f 'worker%g' 0 $((N-1))); do
  while [ ! -S "$RUN/$s.sock" ]; do sleep 0.001; done
  ln -sf "$SNAPDIR/$s.vmstate" "$RUN/$s.vmstate"
  ln -sf "$SNAPDIR/$s.mem"     "$RUN/$s.mem"
done

# 2+3. Ignite.
exec blitz-igniter "$RUN" "$N" "$SQL"
