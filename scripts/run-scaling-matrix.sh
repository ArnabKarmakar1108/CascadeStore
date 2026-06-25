#!/usr/bin/env bash
# Scaling matrix — throughput × scale × shards × threads.
#
# Grid:
#   scales: 100k, 1M (override with SCALING_MATRIX_SCALES)
#   (shards, threads): (1,1), (4,4), (8,8)
#   workloads: A, B, C, F
#   strategy: LEVEL_TIERED, block cache off
#
# Usage:
#   ./scripts/run-scaling-matrix.sh
#   SCALING_MATRIX_SCALES=100000 ./scripts/run-scaling-matrix.sh
#   SCALING_MATRIX_COMBOS=1x1 ./scripts/run-scaling-matrix.sh
#
# Results: benchmark/results/plan/scaling-matrix/

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
TAG="scaling-matrix"
# shellcheck source=plan-env.sh
source "${SCRIPT_DIR}/plan-env.sh"

RUN="${SCRIPT_DIR}/run-ycsb.sh"
export RESULTS_DIR="${PROJECT_ROOT}/benchmark/results/plan/${TAG}"
export STRATEGY="${STRATEGY:-LEVEL_TIERED}"
export BLOCK_CACHE_MB=0
export MEMTABLE_MB=256
export COMPACTION_THRESHOLD=4
export COMPACTION_INTERVAL_MINUTES=30
export METRICS_ENABLED=false
export SSTABLE_LZ4_ENABLED=false

SCALES=(${SCALING_MATRIX_SCALES:-100000 1000000})
WORKLOADS=(workloada workloadb workloadc workloadf)
COMBOS=("1x1" "4x4" "8x8")

if [[ -n "${SCALING_MATRIX_COMBOS:-}" ]]; then
  IFS=' ' read -r -a COMBOS <<< "${SCALING_MATRIX_COMBOS}"
fi

mkdir -p "${RESULTS_DIR}"

echo "Scaling matrix benchmark"
echo "  strategy=${STRATEGY} cache=${BLOCK_CACHE_MB}MB memtable=${MEMTABLE_MB}MB"
echo "  scales=${SCALES[*]} combos=${COMBOS[*]}"
echo "  results=${RESULTS_DIR}"
echo ""

for scale in "${SCALES[@]}"; do
  export RECORDCOUNT="${scale}"
  export OPERATIONCOUNT="${scale}"

  for combo in "${COMBOS[@]}"; do
    shards="${combo%%x*}"
    threads="${combo##*x}"
    export SHARDS="${shards}"
    export THREADS="${threads}"

    if [[ "${shards}" -ge 8 || "${scale}" -ge 1000000 ]]; then
      export JAVA_TOOL_OPTIONS="-Xms8G -Xmx16G"
    else
      export JAVA_TOOL_OPTIONS="-Xms2G -Xmx4G"
    fi

    for workload in "${WORKLOADS[@]}"; do
      cell_tag="${TAG}-${workload}-${scale}-${combo}"
      echo ""
      echo "========== scaling-matrix ${workload} scale=${scale} ${combo} =========="
      DATADIR="/tmp/ycsb-plan-${cell_tag}-${STRATEGY}" \
        "${RUN}" all "${workload}" "${STRATEGY}"
    done
  done
done

echo ""
echo "Scaling matrix complete."
echo "Collect CSV:"
echo "  ./scripts/collect-plan-csv.sh track-c ${RESULTS_DIR}/*.txt > ${RESULTS_DIR}/track_c_matrix.csv"
