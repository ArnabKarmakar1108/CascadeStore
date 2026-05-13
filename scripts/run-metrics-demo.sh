#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

export METRICS_PORT="${METRICS_PORT:-9090}"
export DATA_DIR="${DATA_DIR:-./data/metrics-demo}"

echo "Compiling..."
mvn -q compile -DskipTests

echo "Starting metrics demo (port ${METRICS_PORT})..."
exec mvn -q exec:java \
  -Dexec.mainClass=io.cascadestore.lsm.metrics.MetricsDemo \
  -Dexec.classpathScope=compile \
  -Dexec.cleanupDaemonThreads=false
