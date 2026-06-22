#!/usr/bin/env bash
# Shared comparison profile for CascadeStore vs RocksDB YCSB runs.
#
# Compaction-stress settings (see benchmark/comparison/RESULTS.md):
#   64 MB memtable, L0 threshold 2, frequent flush/compaction attempts.

set -euo pipefail

export COMPARISON_SCALE="${COMPARISON_SCALE:-250000}"
export RECORDCOUNT="${RECORDCOUNT:-${COMPARISON_SCALE}}"
export OPERATIONCOUNT="${OPERATIONCOUNT:-${COMPARISON_SCALE}}"

export THREADS="${THREADS:-1}"
export SHARDS="${SHARDS:-1}"
export TARGET="${TARGET:-0}"
export TRIALS="${TRIALS:-1}"
export WARMUP_SECONDS="${WARMUP_SECONDS:-0}"

export MEMTABLE_MB="${MEMTABLE_MB:-64}"
export COMPACTION_THRESHOLD="${COMPACTION_THRESHOLD:-2}"
export FLUSH_INTERVAL_SECONDS="${FLUSH_INTERVAL_SECONDS:-5}"
export COMPACTION_INTERVAL_MINUTES="${COMPACTION_INTERVAL_MINUTES:-0.17}"
export BLOCK_CACHE_MB="${BLOCK_CACHE_MB:-128}"

export JAVA_TOOL_OPTIONS="${JAVA_TOOL_OPTIONS:--Xms2G -Xmx4G}"
export YCSB_JVM_OPTS="${YCSB_JVM_OPTS:--XX:+UseG1GC -XX:MaxGCPauseMillis=200}"

export RESULTS_DIR="${RESULTS_DIR:-${PROJECT_ROOT:-.}/benchmark/results/comparison}"
