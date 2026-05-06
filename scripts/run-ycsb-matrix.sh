#!/usr/bin/env bash
# Run the strategy × workload YCSB matrix from YCSB_BENCHMARK_PLAN.md.
#
# Usage:
#   ./scripts/run-ycsb-matrix.sh workloada-dryrun          # dry-run matrix (small)
#   ./scripts/run-ycsb-matrix.sh workloada                 # full Workload A
#   ./scripts/run-ycsb-matrix.sh workloada 1000000 5000000 # custom record/operation counts
#
# Optional env: THREADS TARGET TRIALS WARMUP_SECONDS
# THREADS defaults to 1 (embedded store; avoids synchronization overhead).
# TRIALS defaults to 3 (F7a median reporting).
# WARMUP_SECONDS defaults to 30 pause between matrix cells (F7b).

set -euo pipefail

export THREADS="${THREADS:-1}"
export TARGET="${TARGET:-0}"
export TRIALS="${TRIALS:-3}"
export WARMUP_SECONDS="${WARMUP_SECONDS:-30}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUN_YCSB="${SCRIPT_DIR}/run-ycsb.sh"

WORKLOAD="${1:-workloada-dryrun}"
RECORDCOUNT="${2:-}"
OPERATIONCOUNT="${3:-}"
STRATEGIES=(THRESHOLD SIZE_TIERED LEVEL_TIERED)
WORKLOADS=(workloada workloadb workloadc workloadf)

if [[ -n "${RECORDCOUNT}" ]]; then
  export RECORDCOUNT
fi
if [[ -n "${OPERATIONCOUNT}" ]]; then
  export OPERATIONCOUNT
fi

run_cell() {
  local workload="$1"
  local strategy="$2"
  local trial="$3"
  echo ""
  echo "############################################"
  echo "# trial=${trial} workload=${workload} strategy=${strategy}"
  echo "############################################"
  DATADIR="/tmp/ycsb-matrix-${workload}-${strategy}-trial${trial}" \
    "${RUN_YCSB}" all "${workload}" "${strategy}"
  if [[ "${WARMUP_SECONDS}" -gt 0 ]]; then
    echo "Warmup pause ${WARMUP_SECONDS}s before next cell..."
    sleep "${WARMUP_SECONDS}"
  fi
}

if [[ "${WORKLOAD}" == "matrix" ]]; then
  for trial in $(seq 1 "${TRIALS}"); do
    for workload in "${WORKLOADS[@]}"; do
      for strategy in "${STRATEGIES[@]}"; do
        run_cell "${workload}" "${strategy}" "${trial}"
      done
    done
  done
else
  for trial in $(seq 1 "${TRIALS}"); do
    for strategy in "${STRATEGIES[@]}"; do
      run_cell "${WORKLOAD}" "${strategy}" "${trial}"
    done
  done
fi

echo ""
echo "Matrix complete. Results in ${RESULTS_DIR:-benchmark/results}"
