#!/usr/bin/env bash
# Run CascadeStore alone on the comparison profile (compaction stress @ 250k).
#
# Workloads: A, B, C, F — single strategy (LEVEL_TIERED) by default.
#
# Usage:
#   ./scripts/run-comparison-cascade-stress.sh
#   STRATEGY=SIZE_TIERED ./scripts/run-comparison-cascade-stress.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
export PROJECT_ROOT
# shellcheck source=comparison-env.sh
source "${SCRIPT_DIR}/comparison-env.sh"

RUN_YCSB="${SCRIPT_DIR}/run-ycsb.sh"
STRATEGY="${STRATEGY:-LEVEL_TIERED}"
WORKLOADS=(workloada workloadb workloadc workloadf)

echo "CascadeStore comparison profile (compaction stress):"
echo "  scale=${RECORDCOUNT} threads=${THREADS} shards=${SHARDS}"
echo "  memtable=${MEMTABLE_MB}MB threshold=${COMPACTION_THRESHOLD}"
echo "  flush=${FLUSH_INTERVAL_SECONDS}s compaction_interval=${COMPACTION_INTERVAL_MINUTES}min"
echo "  block_cache=${BLOCK_CACHE_MB}MB strategy=${STRATEGY}"
echo "  results=${RESULTS_DIR}"
echo ""

mkdir -p "${RESULTS_DIR}"

for workload in "${WORKLOADS[@]}"; do
  echo ""
  echo "========== CascadeStore ${workload} @ ${RECORDCOUNT} =========="
  DATADIR="/tmp/ycsb-comparison-cascade-${workload}-${STRATEGY}" \
    "${RUN_YCSB}" all "${workload}" "${STRATEGY}"
done

echo ""
echo "CascadeStore comparison run complete."
echo "Summarize: ./scripts/collect-ycsb-metrics.sh ${RESULTS_DIR}/workload*.txt"
