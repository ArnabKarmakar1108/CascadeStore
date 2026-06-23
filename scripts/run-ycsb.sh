#!/usr/bin/env bash
# Run a single YCSB load/run (or both) against CascadeStore.
#
# Usage:
#   ./scripts/run-ycsb.sh load  workloada-dryrun LEVEL_TIERED
#   ./scripts/run-ycsb.sh run   workloada-dryrun LEVEL_TIERED
#   ./scripts/run-ycsb.sh all   workloada-dryrun LEVEL_TIERED
#
# Optional env:
#   THREADS=1 (default; set THREADS=cascadestore.shards for multi-core runs)
#   SHARDS=1 (independent CascadeStore instances under <datadir>/shard-N)
#   TARGET=0 RECORDCOUNT=1000 OPERATIONCOUNT=1000
#   MEMTABLE_MB=16
#   COMPACTION_INTERVAL_MINUTES=30  (>= 1: minutes; < 1: seconds)
#   COMPACTION_THRESHOLD=4          (L0 trigger for LTCS/STCS; file count for THRESHOLD)
#   FLUSH_INTERVAL_SECONDS=10
#   BLOCK_CACHE_MB=0  (0 disables per-shard block cache)
#   JAVA_TOOL_OPTIONS=-Xms4G -Xmx8G  (recommended for 1M in ~32GB containers)
#   YCSB_JVM_OPTS=-XX:+UseG1GC -XX:MaxGCPauseMillis=200  (appended to java invocation)
#   DATADIR=/tmp/ycsb-cascade-data RESULTS_DIR=benchmark/results SHARDS=4

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=ycsb-env.sh
source "${SCRIPT_DIR}/ycsb-env.sh"

usage() {
  cat <<EOF
Usage: $(basename "$0") <load|run|all> <workload> [compaction_strategy]

  workload           workload name (e.g. workloada, workloada-dryrun, workloadb)
  compaction_strategy THRESHOLD | SIZE_TIERED | LEVEL_TIERED (default: LEVEL_TIERED)

Environment overrides:
  THREADS RECORDCOUNT OPERATIONCOUNT TARGET MEMTABLE_MB SHARDS
  COMPACTION_INTERVAL_MINUTES COMPACTION_THRESHOLD FLUSH_INTERVAL_SECONDS DATADIR
EOF
}

if [[ $# -lt 2 ]]; then
  usage
  exit 1
fi

PHASE="$1"
WORKLOAD="$2"
STRATEGY="${3:-LEVEL_TIERED}"
THREADS="${THREADS:-1}"
TARGET="${TARGET:-0}"
RECORDCOUNT="${RECORDCOUNT:-}"
OPERATIONCOUNT="${OPERATIONCOUNT:-}"
MEMTABLE_MB="${MEMTABLE_MB:-}"
COMPACTION_INTERVAL_MINUTES="${COMPACTION_INTERVAL_MINUTES:-}"
COMPACTION_THRESHOLD="${COMPACTION_THRESHOLD:-}"
FLUSH_INTERVAL_SECONDS="${FLUSH_INTERVAL_SECONDS:-}"
BLOCK_CACHE_MB="${BLOCK_CACHE_MB:-}"
SHARDS="${SHARDS:-1}"
DATADIR="${DATADIR:-/tmp/ycsb-cascade-${WORKLOAD}-${STRATEGY}}"
YCSB_JVM_OPTS="${YCSB_JVM_OPTS:--XX:+UseG1GC -XX:MaxGCPauseMillis=200}"

ycsb_setup
WORKLOAD_FILE="$(resolve_workload_file "${WORKLOAD}")"

run_phase() {
  local phase="$1"
  local ycsb_flag="$2"
  local reset_datadir="$3"
  local timestamp
  timestamp="$(date +%Y%m%d-%H%M%S)"
  local outfile="${RESULTS_DIR}/${WORKLOAD}-${STRATEGY}-${phase}-${timestamp}.txt"

  echo "==> YCSB ${phase} workload=${WORKLOAD} strategy=${STRATEGY}"
  echo "    workload file: ${WORKLOAD_FILE}"
  echo "    data dir:      ${DATADIR}"
  echo "    results:       ${outfile}"

  local -a extra_props=(
    "-p" "cascadestore.datadir=${DATADIR}"
    "-p" "cascadestore.compaction.strategy=${STRATEGY}"
    "-p" "cascadestore.reset.datadir=${reset_datadir}"
    "-p" "threadcount=${THREADS}"
    "-p" "target=${TARGET}"
  )

  if [[ -n "${RECORDCOUNT}" ]]; then
    extra_props+=("-p" "recordcount=${RECORDCOUNT}")
  fi
  if [[ -n "${OPERATIONCOUNT}" ]]; then
    extra_props+=("-p" "operationcount=${OPERATIONCOUNT}")
  fi
  if [[ -n "${MEMTABLE_MB}" ]]; then
    extra_props+=("-p" "cascadestore.memtable.mb=${MEMTABLE_MB}")
  fi
  if [[ -n "${COMPACTION_INTERVAL_MINUTES}" ]]; then
    extra_props+=("-p" "cascadestore.compaction.interval.minutes=${COMPACTION_INTERVAL_MINUTES}")
  fi
  if [[ -n "${COMPACTION_THRESHOLD}" ]]; then
    extra_props+=("-p" "cascadestore.compaction.threshold=${COMPACTION_THRESHOLD}")
  fi
  if [[ -n "${FLUSH_INTERVAL_SECONDS}" ]]; then
    extra_props+=("-p" "cascadestore.flush.interval.seconds=${FLUSH_INTERVAL_SECONDS}")
  fi
  if [[ -n "${SHARDS}" ]]; then
    extra_props+=("-p" "cascadestore.shards=${SHARDS}")
  fi
  if [[ -n "${BLOCK_CACHE_MB}" ]]; then
    extra_props+=("-p" "cascadestore.block.cache.mb=${BLOCK_CACHE_MB}")
  fi
  if [[ -n "${METRICS_ENABLED:-}" ]]; then
    extra_props+=("-p" "cascadestore.metrics.enabled=${METRICS_ENABLED}")
  fi
  if [[ -n "${SSTABLE_LZ4_ENABLED:-}" ]]; then
    extra_props+=("-p" "cascadestore.sstable.lz4.enabled=${SSTABLE_LZ4_ENABLED}")
  fi

  java ${YCSB_JVM_OPTS} -cp "${YCSB_CP}" site.ycsb.Client "${ycsb_flag}" \
    -db "${DB_CLASS}" \
    -P "${CASCADE_PROPS}" \
    -P "${WORKLOAD_FILE}" \
    "${extra_props[@]}" \
    -s 2>&1 | tee "${outfile}"
}

case "${PHASE}" in
  load)
    run_phase "load" "-load" "true"
    ;;
  run)
    run_phase "run" "-t" "false"
    ;;
  all)
    run_phase "load" "-load" "true"
    run_phase "run" "-t" "false"
    ;;
  *)
    usage
    exit 1
    ;;
esac
