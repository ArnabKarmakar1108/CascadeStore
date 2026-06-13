#!/usr/bin/env bash
# Run the full CascadeStore vs RocksDB comparison suite (compaction stress @ 250k).
#
# Order:
#   1. CascadeStore alone (A/B/C/F)
#   2. RocksDB alone (A/B/C/F)
#
# Usage:
#   ./scripts/run-comparison-suite.sh
#   COMPARISON_SCALE=250000 ./scripts/run-comparison-suite.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

"${SCRIPT_DIR}/run-comparison-cascade-stress.sh"
echo ""
echo "############################################"
echo "# CascadeStore complete — starting RocksDB"
echo "############################################"
echo ""
"${SCRIPT_DIR}/run-comparison-rocksdb-stress.sh"

echo ""
echo "Comparison suite complete. See benchmark/COMPARISON.md for reporting guidance."
