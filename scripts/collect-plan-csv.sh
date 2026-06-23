#!/usr/bin/env bash
# Turn YCSB result files into benchmark-plan CSV rows.
#
# Usage:
#   ./scripts/collect-plan-csv.sh track-b path/to/*.txt > track_b_amplification.csv
#   ./scripts/collect-plan-csv.sh track-a path/to/*.txt > track_a_scale.csv

set -euo pipefail

MODE="${1:?usage: collect-plan-csv.sh <track-a|track-b|track-c> <result-files...>}"
shift

if [[ $# -lt 1 ]]; then
  echo "usage: collect-plan-csv.sh ${MODE} <result-files...>" >&2
  exit 1
fi

metric_value() {
  local file="$1"
  local name="$2"
  rg "^\[CASCADE_METRICS\], ${name}," "${file}" 2>/dev/null | tail -1 | awk -F, '{gsub(/^ +| +$/,"",$3); print $3}' || true
}

ycsb_value() {
  local file="$1"
  local section="$2"
  local field="$3"
  local escaped_field
  escaped_field="$(printf '%s' "${field}" | sed 's/[()]/\\&/g')"
  rg "^\[${section}\], ${escaped_field}" "${file}" 2>/dev/null | tail -1 | awk -F, '{gsub(/^ +| +$/,"",$3); print $3}' || true
}

prop_value() {
  local file="$1"
  local key="$2"
  rg -o "${key}=[0-9]+" "${file}" 2>/dev/null | head -1 | cut -d= -f2 || true
}

strategy_from_file() {
  local base
  base="$(basename "$1")"
  if [[ "${base}" =~ -(THRESHOLD|SIZE_TIERED|LEVEL_TIERED)- ]]; then
    echo "${BASH_REMATCH[1]}"
  else
    echo "UNKNOWN"
  fi
}

phase_from_file() {
  local base
  base="$(basename "$1")"
  if [[ "${base}" =~ -(load|run)- ]]; then
    echo "${BASH_REMATCH[1]}"
  else
    echo "unknown"
  fi
}

metric_sum() {
  local total=0
  local value
  for file in "$@"; do
    value="$(metric_value "${file}" "${METRIC_NAME}")"
    if [[ -n "${value}" ]]; then
      total=$((total + value))
    fi
  done
  echo "${total}"
}

ratio_or_empty() {
  local numerator="$1"
  local denominator="$2"
  if [[ -z "${numerator}" || -z "${denominator}" || "${denominator}" -le 0 ]]; then
    echo ""
    return
  fi
  awk -v n="${numerator}" -v d="${denominator}" 'BEGIN { printf "%.6f", n / d }'
}

latest_file_for_strategy_phase() {
  local strategy="$1"
  local phase="$2"
  local file
  local latest=""
  for file in "$@"; do
    if [[ "$(strategy_from_file "${file}")" == "${strategy}" && "$(phase_from_file "${file}")" == "${phase}" ]]; then
      if [[ -z "${latest}" || "${file}" > "${latest}" ]]; then
        latest="${file}"
      fi
    fi
  done
  echo "${latest}"
}

case "${MODE}" in
  track-b)
    echo "strategy,scale,lookup_read_amp,files_probed_amp,live_sstable_count,write_amp,space_amp,run_ops,compactions,live_sstable_data_bytes,user_write_bytes"
    declare -A seen_strategy=()
    for file in "$@"; do
      strategy="$(strategy_from_file "${file}")"
      seen_strategy["${strategy}"]=1
    done
    for strategy in THRESHOLD SIZE_TIERED LEVEL_TIERED UNKNOWN; do
      if [[ -z "${seen_strategy[${strategy}]+x}" ]]; then
        continue
      fi
      load_file="$(latest_file_for_strategy_phase "${strategy}" load "$@")"
      run_file="$(latest_file_for_strategy_phase "${strategy}" run "$@")"
      if [[ -z "${run_file}" && -z "${load_file}" ]]; then
        continue
      fi

      files=()
      if [[ -n "${load_file}" ]]; then
        files+=("${load_file}")
      fi
      if [[ -n "${run_file}" ]]; then
        files+=("${run_file}")
      fi

      snapshot_file="${run_file:-${load_file}}"
      scale="$(prop_value "${snapshot_file}" recordcount)"
      if [[ -z "${scale}" && -n "${load_file}" ]]; then
        scale="$(prop_value "${load_file}" recordcount)"
      fi

      METRIC_NAME="read_operations_total"
      read_ops="$(metric_sum "${files[@]}")"
      METRIC_NAME="sstable_lookups_total"
      sstable_lookups="$(metric_sum "${files[@]}")"
      METRIC_NAME="bloom_probes_total"
      bloom_probes="$(metric_sum "${files[@]}")"
      METRIC_NAME="user_write_bytes_total"
      user_bytes="$(metric_sum "${files[@]}")"
      METRIC_NAME="sstable_bytes_written_total"
      sstable_bytes="$(metric_sum "${files[@]}")"
      METRIC_NAME="compaction_total"
      compactions="$(metric_sum "${files[@]}")"

      lookup_read_amp="$(ratio_or_empty "${sstable_lookups}" "${read_ops}")"
      files_probed_amp="$(ratio_or_empty "${bloom_probes}" "${read_ops}")"
      write_amp="$(ratio_or_empty "${sstable_bytes}" "${user_bytes}")"
      live_bytes="$(metric_value "${snapshot_file}" live_sstable_data_bytes)"
      live_sstable_count="$(metric_value "${snapshot_file}" live_sstable_count)"
      space_amp="$(ratio_or_empty "${live_bytes}" "${user_bytes}")"
      run_ops=""
      if [[ -n "${run_file}" ]]; then
        run_ops="$(ycsb_value "${run_file}" OVERALL "Throughput(ops/sec)")"
      elif [[ -n "${load_file}" ]]; then
        run_ops="$(ycsb_value "${load_file}" OVERALL "Throughput(ops/sec)")"
      fi

      echo "${strategy},${scale:-?},${lookup_read_amp:-},${files_probed_amp:-},${live_sstable_count:-},${write_amp:-},${space_amp:-},${run_ops:-},${compactions:-},${live_bytes:-},${user_bytes:-}"
    done
    ;;
  track-a)
    echo "engine,scale,phase,load_ops,run_ops,read_p99_us,update_p99_us,errors"
    for file in "$@"; do
      phase="$(phase_from_file "${file}")"
      strategy="$(strategy_from_file "${file}")"
      scale="$(prop_value "${file}" recordcount)"
      throughput="$(ycsb_value "${file}" OVERALL "Throughput(ops/sec)")"
      read_p99="$(ycsb_value "${file}" READ "99thPercentileLatency(us)")"
      update_p99="$(ycsb_value "${file}" UPDATE "99thPercentileLatency(us)")"
      insert_ok="$(ycsb_value "${file}" INSERT "Return=OK")"
      read_ok="$(ycsb_value "${file}" READ "Return=OK")"
      update_ok="$(ycsb_value "${file}" UPDATE "Return=OK")"
      errors=0
      if [[ "${phase}" == "load" && -n "${insert_ok}" && "${insert_ok}" != "${scale}" ]]; then
        errors=1
      fi
      if [[ "${phase}" == "run" ]]; then
        echo "cascadestore,${scale:-?},run, ,${throughput:-},${read_p99:-},${update_p99:-},${errors}"
      else
        echo "cascadestore,${scale:-?},load,${throughput:-}, ,${read_p99:-},${update_p99:-},${errors}"
      fi
      # strategy kept in filename only for now
      _="${strategy}"
      _="${read_ok}"
      _="${update_ok}"
    done
    ;;
  track-c)
    echo "workload,scale,shards,threads,phase,run_ops,read_p99_us,update_p99_us"
    for file in "$@"; do
      phase="$(phase_from_file "${file}")"
      if [[ "${phase}" != "run" ]]; then
        continue
      fi
      workload="$(basename "${file}" | cut -d- -f1)"
      scale="$(prop_value "${file}" recordcount)"
      shards="$(prop_value "${file}" cascadestore.shards)"
      threads="$(prop_value "${file}" threadcount)"
      run_ops="$(ycsb_value "${file}" OVERALL "Throughput(ops/sec)")"
      read_p99="$(ycsb_value "${file}" READ "99thPercentileLatency(us)")"
      update_p99="$(ycsb_value "${file}" UPDATE "99thPercentileLatency(us)")"
      echo "${workload},${scale:-?},${shards:-?},${threads:-?},${phase},${run_ops:-},${read_p99:-},${update_p99:-}"
    done
    ;;
  *)
    echo "unknown mode: ${MODE} (expected track-a, track-b, or track-c)" >&2
    exit 1
    ;;
esac
