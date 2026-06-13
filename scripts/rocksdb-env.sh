#!/usr/bin/env bash
# Shared setup for RocksDB YCSB runs.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
YCSB_VERSION="${YCSB_VERSION:-0.17.0}"
YCSB_HOME="${YCSB_HOME:-${PROJECT_ROOT}/.ycsb/YCSB}"
RESULTS_DIR="${RESULTS_DIR:-${PROJECT_ROOT}/benchmark/results}"
ROCKSDB_PROPS="${ROCKSDB_PROPS:-${PROJECT_ROOT}/src/test/resources/ycsb/rocksdb.properties}"
DB_CLASS="io.cascadestore.lsm.benchmark.ycsb.RocksDbShardedYcsbClient"

ensure_ycsb_checkout() {
  if [[ -d "${YCSB_HOME}/workloads" ]]; then
    return 0
  fi

  echo "Cloning YCSB ${YCSB_VERSION} into ${YCSB_HOME}..."
  mkdir -p "$(dirname "${YCSB_HOME}")"
  git clone --depth 1 --branch "${YCSB_VERSION}" \
    https://github.com/brianfrankcooper/YCSB.git "${YCSB_HOME}"
}

ensure_built() {
  if [[ ! -f "${PROJECT_ROOT}/target/cascade-store-1.0-SNAPSHOT-tests.jar" ]]; then
    echo "Building CascadeStore (package + test-jar)..."
    (cd "${PROJECT_ROOT}" && mvn -q -DskipTests package)
  fi
}

build_classpath() {
  local maven_cp
  maven_cp="$(cd "${PROJECT_ROOT}" && mvn -q -DincludeScope=test dependency:build-classpath -Dmdep.outputFile=/dev/stdout)"
  echo "${PROJECT_ROOT}/target/cascade-store-1.0-SNAPSHOT.jar:${PROJECT_ROOT}/target/cascade-store-1.0-SNAPSHOT-tests.jar:${maven_cp}"
}

resolve_workload_file() {
  local workload="$1"
  local custom="${PROJECT_ROOT}/src/test/resources/ycsb/workloads/${workload}.properties"
  local upstream="${YCSB_HOME}/workloads/${workload}"

  if [[ -f "${custom}" ]]; then
    echo "${custom}"
  elif [[ -f "${upstream}" ]]; then
    echo "${upstream}"
  else
    echo "Unknown workload: ${workload}" >&2
    exit 1
  fi
}

rocksdb_ycsb_setup() {
  ensure_built
  ensure_ycsb_checkout
  mkdir -p "${RESULTS_DIR}"
  YCSB_CP="$(build_classpath)"
  export YCSB_CP RESULTS_DIR ROCKSDB_PROPS DB_CLASS YCSB_HOME PROJECT_ROOT
}
