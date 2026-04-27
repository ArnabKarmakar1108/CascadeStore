#!/usr/bin/env bash
# Print a one-line summary from a YCSB result file.
#
# Usage: ./scripts/collect-ycsb-metrics.sh benchmark/results/workloada-THRESHOLD-run-*.txt

set -euo pipefail

file="${1:?usage: collect-ycsb-metrics.sh <result.txt>}"

threads="$(rg -o 'threadcount=[0-9]+' "${file}" | head -1 | cut -d= -f2 || true)"
recordcount="$(rg -o 'recordcount=[0-9]+' "${file}" | head -1 | cut -d= -f2 || true)"
throughput="$(rg '^\[OVERALL\], Throughput' "${file}" | tail -1 | cut -d, -f3 || true)"
runtime_ms="$(rg '^\[OVERALL\], RunTime' "${file}" | tail -1 | cut -d, -f3 || true)"
read_p99="$(rg '^\[READ\], 99thPercentileLatency' "${file}" | tail -1 | cut -d, -f3 || true)"
update_p99="$(rg '^\[UPDATE\], 99thPercentileLatency' "${file}" | tail -1 | cut -d, -f3 || true)"
insert_p99="$(rg '^\[INSERT\], 99thPercentileLatency' "${file}" | tail -1 | cut -d, -f3 || true)"
insert_ok="$(rg '^\[INSERT\], Return=OK' "${file}" | tail -1 | cut -d, -f3 || true)"
read_ok="$(rg '^\[READ\], Return=OK' "${file}" | tail -1 | cut -d, -f3 || true)"
update_ok="$(rg '^\[UPDATE\], Return=OK' "${file}" | tail -1 | cut -d, -f3 || true)"

echo "${file} threads=${threads:-?} records=${recordcount:-?} throughput=${throughput:-?} runtime_ms=${runtime_ms:-?} read_p99=${read_p99:-?} update_p99=${update_p99:-?} insert_p99=${insert_p99:-?} insert_ok=${insert_ok:-?} read_ok=${read_ok:-?} update_ok=${update_ok:-?}"
