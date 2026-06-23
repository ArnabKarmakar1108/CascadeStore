#!/usr/bin/env bash
# Shared YCSB benchmark defaults (strategy-comparison compaction-stress profile).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

export PROJECT_ROOT
export RESULTS_DIR="${RESULTS_DIR:-${PROJECT_ROOT}/benchmark/results/plan}"
export PLAN_SCALE="${PLAN_SCALE:-250000}"

export THREADS="${THREADS:-1}"
export SHARDS="${SHARDS:-1}"
export TARGET="${TARGET:-0}"
export RECORDCOUNT="${RECORDCOUNT:-${PLAN_SCALE}}"
export OPERATIONCOUNT="${OPERATIONCOUNT:-${PLAN_SCALE}}"
export MEMTABLE_MB="${MEMTABLE_MB:-64}"
export COMPACTION_THRESHOLD="${COMPACTION_THRESHOLD:-2}"
export COMPACTION_INTERVAL_MINUTES="${COMPACTION_INTERVAL_MINUTES:-0.17}"
export FLUSH_INTERVAL_SECONDS="${FLUSH_INTERVAL_SECONDS:-5}"
export BLOCK_CACHE_MB="${BLOCK_CACHE_MB:-0}"
export METRICS_ENABLED="${METRICS_ENABLED:-false}"
export SSTABLE_LZ4_ENABLED="${SSTABLE_LZ4_ENABLED:-false}"
export JAVA_TOOL_OPTIONS="${JAVA_TOOL_OPTIONS:--Xms2G -Xmx2G}"
export YCSB_JVM_OPTS="${YCSB_JVM_OPTS:--XX:+UseG1GC -XX:MaxGCPauseMillis=200}"
