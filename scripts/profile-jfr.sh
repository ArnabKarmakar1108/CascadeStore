#!/usr/bin/env bash
# Capture a short JFR profile during a YCSB run phase (F5a diagnostic).
#
# Usage:
#   ./scripts/profile-jfr.sh run workloada THRESHOLD
#   RECORDCOUNT=10000 ./scripts/profile-jfr.sh load workloada-dryrun THRESHOLD
#
# Writes benchmark/results/jfr-<timestamp>.jfr

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=ycsb-env.sh
source "${SCRIPT_DIR}/ycsb-env.sh"

PHASE="${1:-run}"
WORKLOAD="${2:-workloada-dryrun}"
STRATEGY="${3:-THRESHOLD}"
DURATION_SECONDS="${JFR_SECONDS:-120}"
RESULTS_DIR="${RESULTS_DIR:-benchmark/results}"
mkdir -p "${RESULTS_DIR}"

timestamp="$(date +%Y%m%d-%H%M%S)"
jfr_file="${RESULTS_DIR}/jfr-${WORKLOAD}-${STRATEGY}-${PHASE}-${timestamp}.jfr"

export JAVA_TOOL_OPTIONS="${JAVA_TOOL_OPTIONS:-} -XX:StartFlightRecording=duration=${DURATION_SECONDS}s,filename=${jfr_file},settings=profile"

echo "JFR recording -> ${jfr_file} (${DURATION_SECONDS}s)"
"${SCRIPT_DIR}/run-ycsb.sh" "${PHASE}" "${WORKLOAD}" "${STRATEGY}"
echo "JFR saved: ${jfr_file}"
