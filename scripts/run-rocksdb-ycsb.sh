#!/usr/bin/env bash
# Run a single YCSB load/run (or both) against embedded RocksDB.
#
# Usage:
#   ./scripts/run-rocksdb-ycsb.sh all workloada
#   ./scripts/run-rocksdb-ycsb.sh all workloadc
#
# Optional env: THREADS SHARDS RECORDCOUNT OPERATIONCOUNT MEMTABLE_MB
#   COMPACTION_THRESHOLD BLOCK_CACHE_MB DATADIR RESULTS_DIR

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=rocksdb-env.sh
source "${SCRIPT_DIR}/rocksdb-env.sh"

usage() {
  cat <<EOF
Usage: $(basename "$0") <load|run|all> <workload>

Environment overrides:
  THREADS SHARDS RECORDCOUNT OPERATIONCOUNT TARGET MEMTABLE_MB
  COMPACTION_THRESHOLD BLOCK_CACHE_MB DATADIR RESULTS_DIR
EOF
}

if [[ $# -lt 2 ]]; then
  usage
  exit 1
fi

PHASE="$1"
WORKLOAD="$2"
THREADS="${THREADS:-1}"
TARGET="${TARGET:-0}"
RECORDCOUNT="${RECORDCOUNT:-}"
OPERATIONCOUNT="${OPERATIONCOUNT:-}"
MEMTABLE_MB="${MEMTABLE_MB:-}"
COMPACTION_THRESHOLD="${COMPACTION_THRESHOLD:-}"
BLOCK_CACHE_MB="${BLOCK_CACHE_MB:-}"
SHARDS="${SHARDS:-1}"
DATADIR="${DATADIR:-/tmp/ycsb-rocksdb-${WORKLOAD}}"
YCSB_JVM_OPTS="${YCSB_JVM_OPTS:--XX:+UseG1GC -XX:MaxGCPauseMillis=200}"

rocksdb_ycsb_setup
WORKLOAD_FILE="$(resolve_workload_file "${WORKLOAD}")"

run_phase() {
  local phase="$1"
  local ycsb_flag="$2"
  local reset_datadir="$3"
  local timestamp
  timestamp="$(date +%Y%m%d-%H%M%S)"
  local outfile="${RESULTS_DIR}/rocksdb-${WORKLOAD}-${phase}-${timestamp}.txt"

  echo "==> RocksDB YCSB ${phase} workload=${WORKLOAD}"
  echo "    workload file: ${WORKLOAD_FILE}"
  echo "    data dir:      ${DATADIR}"
  echo "    results:       ${outfile}"

  local -a extra_props=(
    "-p" "rocksdb.datadir=${DATADIR}"
    "-p" "rocksdb.reset.datadir=${reset_datadir}"
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
    extra_props+=("-p" "rocksdb.memtable.mb=${MEMTABLE_MB}")
  fi
  if [[ -n "${COMPACTION_THRESHOLD}" ]]; then
    extra_props+=("-p" "rocksdb.compaction.threshold=${COMPACTION_THRESHOLD}")
  fi
  if [[ -n "${BLOCK_CACHE_MB}" ]]; then
    extra_props+=("-p" "rocksdb.block.cache.mb=${BLOCK_CACHE_MB}")
  fi
  if [[ -n "${SHARDS}" ]]; then
    extra_props+=("-p" "rocksdb.shards=${SHARDS}")
  fi

  java ${YCSB_JVM_OPTS} -cp "${YCSB_CP}" site.ycsb.Client "${ycsb_flag}" \
    -db "${DB_CLASS}" \
    -P "${ROCKSDB_PROPS}" \
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
