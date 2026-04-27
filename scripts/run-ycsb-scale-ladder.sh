#!/usr/bin/env bash
# Run Workload A at multiple scales × 3 compaction strategies (single-thread baseline).
#
# Usage:
#   ./scripts/run-ycsb-scale-ladder.sh           # 10k + 100k
#   ./scripts/run-ycsb-scale-ladder.sh 1000000   # include 1M scale
#
# Scales: 10k (dry), 100k (standard), optional 1M+

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUN="${SCRIPT_DIR}/run-ycsb.sh"

export THREADS="${THREADS:-1}"
export TARGET="${TARGET:-0}"
export MEMTABLE_MB="${MEMTABLE_MB:-256}"
export COMPACTION_THRESHOLD="${COMPACTION_THRESHOLD:-4}"
export JAVA_TOOL_OPTIONS="${JAVA_TOOL_OPTIONS:--Xms2G -Xmx2G}"

MAX_SCALE="${1:-100000}"
SCALES=(10000 100000)
if [[ "${MAX_SCALE}" -ge 1000000 ]]; then
  SCALES+=(1000000)
fi

for RC in "${SCALES[@]}"; do
  if [[ "${RC}" -eq 10000 ]]; then
    WORKLOAD=workloada-10k
    TAG=10k
  else
    WORKLOAD=workloada
    TAG="${RC}"
  fi
  echo ""
  echo "========== Workload A scale=${TAG} records=${RC} =========="
  for STRATEGY in THRESHOLD SIZE_TIERED LEVEL_TIERED; do
    DATADIR="/tmp/ycsb-workloada-${TAG}-${STRATEGY}" \
    RECORDCOUNT="${RC}" OPERATIONCOUNT="${RC}" \
    "${RUN}" all "${WORKLOAD}" "${STRATEGY}"
  done
done

echo ""
echo "Scale ladder complete. Summarize with:"
echo "  ./scripts/collect-ycsb-metrics.sh benchmark/results/workloada*.txt"
