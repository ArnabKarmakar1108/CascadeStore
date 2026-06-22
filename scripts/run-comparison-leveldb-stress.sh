#!/usr/bin/env bash
# Run LevelDB alone on the comparison profile (compaction stress @ 250k).
#
# Workloads: A, B, C, F
#
# Usage:
#   ./scripts/run-comparison-leveldb-stress.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
export PROJECT_ROOT
# shellcheck source=comparison-env.sh
source "${SCRIPT_DIR}/comparison-env.sh"

RUN_LEVELDB="${SCRIPT_DIR}/run-leveldb-ycsb.sh"
WORKLOADS=(workloada workloadb workloadc workloadf)

echo "LevelDB comparison profile (compaction stress):"
echo "  scale=${RECORDCOUNT} threads=${THREADS} shards=${SHARDS}"
echo "  memtable=${MEMTABLE_MB}MB block_cache=${BLOCK_CACHE_MB}MB"
echo "  results=${RESULTS_DIR}"
echo ""

mkdir -p "${RESULTS_DIR}"

for workload in "${WORKLOADS[@]}"; do
  echo ""
  echo "========== LevelDB ${workload} @ ${RECORDCOUNT} =========="
  DATADIR="/tmp/ycsb-comparison-leveldb-${workload}" \
    "${RUN_LEVELDB}" all "${workload}"
done

echo ""
echo "LevelDB comparison run complete."
echo "Summarize: ./scripts/collect-ycsb-metrics.sh ${RESULTS_DIR}/leveldb-*.txt"
