#!/usr/bin/env bash
# Run YCSB workloads B, C, and F at small scale across compaction strategies.
#
# Usage:
#   ./scripts/run-ycsb-workloads-bcf.sh              # 10k, 1 trial, THRESHOLD baseline first
#   ./scripts/run-ycsb-workloads-bcf.sh 100000       # 100k via upstream workload files
#   RECORDCOUNT=10000 TRIALS=3 ./scripts/run-ycsb-workloads-bcf.sh
#
# Optional env: THREADS MEMTABLE_MB COMPACTION_THRESHOLD TRIALS WARMUP_SECONDS BLOCK_CACHE_MB

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUN_MATRIX="${SCRIPT_DIR}/run-ycsb-matrix.sh"

export THREADS="${THREADS:-1}"
export TARGET="${TARGET:-0}"
export MEMTABLE_MB="${MEMTABLE_MB:-256}"
export COMPACTION_THRESHOLD="${COMPACTION_THRESHOLD:-4}"
export TRIALS="${TRIALS:-1}"
export WARMUP_SECONDS="${WARMUP_SECONDS:-10}"
export JAVA_TOOL_OPTIONS="${JAVA_TOOL_OPTIONS:--Xms2G -Xmx2G}"

SCALE="${1:-10000}"
export RECORDCOUNT="${RECORDCOUNT:-${SCALE}}"
export OPERATIONCOUNT="${OPERATIONCOUNT:-${SCALE}}"

if [[ "${SCALE}" -le 10000 ]]; then
  WORKLOADS=(workloadb-10k workloadc-10k workloadf-10k)
  TAG=10k
else
  WORKLOADS=(workloadb workloadc workloadf)
  TAG="${SCALE}"
fi

echo "YCSB B/C/F matrix: scale=${TAG} records=${RECORDCOUNT} trials=${TRIALS}"
echo "  threads=${THREADS} memtable=${MEMTABLE_MB}MB threshold=${COMPACTION_THRESHOLD}"

for workload in "${WORKLOADS[@]}"; do
  echo ""
  echo "========== ${workload} @ ${TAG} =========="
  "${RUN_MATRIX}" "${workload}" "${RECORDCOUNT}" "${OPERATIONCOUNT}"
done

echo ""
echo "B/C/F workloads complete. Results in ${RESULTS_DIR:-benchmark/results}"
echo "Summarize: ./scripts/collect-ycsb-metrics.sh benchmark/results/workload{b,c,f}*.txt"
