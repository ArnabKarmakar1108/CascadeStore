#!/usr/bin/env bash
# Workload A × 3 strategies with settings chosen to exercise compaction.
#
# Goals:
#   - Single thread (no lock contention on embedded CascadeStore)
#   - Smaller MemTable → more L0 flushes during 100k load/run
#   - compaction.threshold=2 → LTCS/STCS L0 trigger + THRESHOLD file trigger
#   - Periodic compaction attempts via sub-minute interval
#
# Usage:
#   ./scripts/run-ycsb-compaction-matrix.sh
#   RECORDCOUNT=1000000 ./scripts/run-ycsb-compaction-matrix.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUN_MATRIX="${SCRIPT_DIR}/run-ycsb-matrix.sh"

export THREADS="${THREADS:-1}"
export TARGET="${TARGET:-0}"
export RECORDCOUNT="${RECORDCOUNT:-100000}"
export OPERATIONCOUNT="${OPERATIONCOUNT:-100000}"
export MEMTABLE_MB="${MEMTABLE_MB:-64}"
export COMPACTION_THRESHOLD="${COMPACTION_THRESHOLD:-2}"
export COMPACTION_INTERVAL_MINUTES="${COMPACTION_INTERVAL_MINUTES:-0.17}"
export FLUSH_INTERVAL_SECONDS="${FLUSH_INTERVAL_SECONDS:-5}"

echo "Compaction matrix profile:"
echo "  THREADS=${THREADS} RECORDCOUNT=${RECORDCOUNT} MEMTABLE_MB=${MEMTABLE_MB}"
echo "  COMPACTION_THRESHOLD=${COMPACTION_THRESHOLD} COMPACTION_INTERVAL_MINUTES=${COMPACTION_INTERVAL_MINUTES}"
echo "  FLUSH_INTERVAL_SECONDS=${FLUSH_INTERVAL_SECONDS}"
echo ""

exec "${RUN_MATRIX}" workloada "${RECORDCOUNT}" "${OPERATIONCOUNT}"
