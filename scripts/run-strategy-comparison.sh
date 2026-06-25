#!/usr/bin/env bash
# Strategy comparison — one workload per amplification track.
#
# Usage:
#   ./scripts/run-strategy-comparison.sh read    # Workload C — read amplification
#   ./scripts/run-strategy-comparison.sh write   # Workload A load — write amplification
#   ./scripts/run-strategy-comparison.sh space   # Workload F — space amplification
#   ./scripts/run-strategy-comparison.sh all
#
# Optional env:
#   PLAN_SCALE=500000
#   RESULTS_DIR=benchmark/results/plan/strategy-comparison-read-500k

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

FOCUS="${1:-}"
if [[ -z "${FOCUS}" ]]; then
  cat <<EOF
Usage: $(basename "$0") <read|write|space|all>

Focused strategy-comparison runs at PLAN_SCALE (default 500000):

  read   Load + run Workload C (100% read, zipfian) — isolates read amplification
  write  Load-only (inserts) — isolates write amplification from compaction rewrites
  space  Load + run Workload F (50% RMW) — overlapping versions, SIZE_TIERED space bloat
  all    Run read, write, and space tracks back-to-back
EOF
  exit 1
fi

export PLAN_SCALE="${PLAN_SCALE:-500000}"
unset RESULTS_DIR RECORDCOUNT OPERATIONCOUNT

# shellcheck source=plan-env.sh
source "${SCRIPT_DIR}/plan-env.sh"

export PLAN_SCALE="${PLAN_SCALE:-500000}"
export RECORDCOUNT="${PLAN_SCALE}"
export OPERATIONCOUNT="${PLAN_SCALE}"

RUN="${SCRIPT_DIR}/run-ycsb.sh"

run_track() {
  local focus="$1"
  local workload phase
  local tag="strategy-comparison-${focus}-${PLAN_SCALE}"

  case "${focus}" in
    read)
      workload="workloadc"
      # Two-phase read track: shape the LSM during load, freeze compaction during run.
      export MEMTABLE_MB="${READ_TRACK_MEMTABLE_MB:-16}"
      export COMPACTION_THRESHOLD="${READ_TRACK_COMPACTION_THRESHOLD:-4}"
      export COMPACTION_INTERVAL_MINUTES="${READ_TRACK_COMPACTION_INTERVAL_MINUTES:-0.17}"
      export FLUSH_INTERVAL_SECONDS="${READ_TRACK_FLUSH_INTERVAL_SECONDS:-5}"
      export METRICS_ENABLED="${READ_TRACK_METRICS_ENABLED:-true}"
      ;;
    write)
      workload="workloada"
      phase="load"
      ;;
    space)
      workload="workloadf"
      phase="all"
      ;;
    *)
      echo "unknown focus: ${focus}" >&2
      exit 1
      ;;
  esac

  export RESULTS_DIR="${PROJECT_ROOT}/benchmark/results/plan/${tag}"
  mkdir -p "${RESULTS_DIR}"

  echo "Strategy comparison — ${focus} track"
  echo "  workload=${workload} scale=${RECORDCOUNT}"
  echo "  threads=${THREADS} shards=${SHARDS} memtable=${MEMTABLE_MB}MB"
  echo "  threshold=${COMPACTION_THRESHOLD} interval=${COMPACTION_INTERVAL_MINUTES}min cache=${BLOCK_CACHE_MB}MB"
  echo "  results=${RESULTS_DIR}"
  echo ""

  if [[ "${focus}" == "read" ]]; then
    for STRATEGY in THRESHOLD SIZE_TIERED LEVEL_TIERED; do
      local datadir="/tmp/ycsb-plan-${tag}-${workload}-${STRATEGY}"
      echo "--- ${STRATEGY}: load (compaction active) ---"
      DATADIR="${datadir}" "${RUN}" load "${workload}" "${STRATEGY}"

      echo "--- ${STRATEGY}: run (compaction frozen) ---"
      COMPACTION_THRESHOLD="${READ_TRACK_RUN_COMPACTION_THRESHOLD:-999}" \
      COMPACTION_INTERVAL_MINUTES="${READ_TRACK_RUN_COMPACTION_INTERVAL_MINUTES:-10000}" \
        DATADIR="${datadir}" "${RUN}" run "${workload}" "${STRATEGY}"
    done
  else
    for STRATEGY in THRESHOLD SIZE_TIERED LEVEL_TIERED; do
      DATADIR="/tmp/ycsb-plan-${tag}-${workload}-${STRATEGY}" \
        "${RUN}" "${phase}" "${workload}" "${STRATEGY}"
    done
  fi

  echo ""
  echo "${focus} track complete. Summarize:"
  echo "  ./scripts/collect-plan-csv.sh track-b ${RESULTS_DIR}/*.txt > ${RESULTS_DIR}/track_b_amplification.csv"
}

case "${FOCUS}" in
  all)
    run_track read
    unset RESULTS_DIR
    run_track write
    unset RESULTS_DIR
    run_track space
    ;;
  read|write|space)
    run_track "${FOCUS}"
    ;;
  *)
    echo "unknown focus: ${FOCUS}" >&2
    exit 1
    ;;
esac
