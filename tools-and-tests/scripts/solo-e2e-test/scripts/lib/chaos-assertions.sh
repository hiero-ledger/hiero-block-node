#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Phase 3: latency-aware assertion primitives. Sourced by solo-test-runner.sh
# and by scripts/test/test-chaos-assertions.sh (which provides mocks).
#
# Expects the following to be defined by the caller:
#   - kctl()                   - kubectl wrapper that honours CONTEXT
#   - get_bn_metrics_port()    - maps a BN name to its local metrics port
#   - get_all_block_nodes()    - lists BN names from the topology file
#   - NAMESPACE                - kubernetes namespace string
#
# Functions exported:
#   compare_numeric             - awk-based float-safe comparator
#   fetch_metric                - read a single Prometheus metric value
#   fetch_pod_logs              - read recent pod logs (overridable in tests)
#   assert_metric_threshold     - generic metric primitive
#   assert_block_rate_floor     - sugar built on metric scraping
#   assert_log_match            - log-substring presence (powers backfill-triggered)

# An empty Block Node reports firstAvailableBlock/lastAvailableBlock as UINT64_MAX,
# not null and not 0 — 0 means "block 0 is available". The field is present and
# populated, so a `== "null"` check does not catch it.
NO_BLOCKS_SENTINEL="18446744073709551615"

# is_valid_block_number: true only for a plain decimal that fits in a signed 64-bit
# integer, which is the range bash arithmetic can compare without wrapping.
#
# Any uint64 field out of serverStatus carries this hazard: UINT64_MAX wraps to -1
# inside `[[ ... -gt ... ]]`, so a naive `[[ "${first}" -gt "${min}" ]]` *passes* for
# a node holding no blocks at all. Rejecting the whole out-of-range class rather than
# just the current sentinel value means a future sentinel change cannot silently
# reintroduce the overflow.
function is_valid_block_number {
  local value="${1-}"

  [[ -n "${value}" && "${value}" != "null" ]] || return 1
  [[ "${value}" =~ ^[0-9]+$ ]] || return 1

  # Compare by length, then lexically. The value may exceed what bash arithmetic can
  # hold, so it cannot be range-checked numerically here without hitting the very
  # overflow this guard exists to prevent.
  local int64_max="9223372036854775807"
  (( ${#value} < ${#int64_max} )) && return 0
  (( ${#value} > ${#int64_max} )) && return 1
  [[ "${value}" > "${int64_max}" ]] && return 1
  return 0
}

# compare_numeric: arithmetic comparison via awk (handles floats).
# Echoes ok/violation line; returns 0 on pass, 1 on fail.
function compare_numeric {
    local lhs="$1" op="$2" rhs="$3" label="${4:-value}"
    local result
    result=$(awk -v l="$lhs" -v r="$rhs" -v op="$op" 'BEGIN {
        if      (op == "<")  print (l+0 <  r+0) ? "true" : "false"
        else if (op == "<=") print (l+0 <= r+0) ? "true" : "false"
        else if (op == ">")  print (l+0 >  r+0) ? "true" : "false"
        else if (op == ">=") print (l+0 >= r+0) ? "true" : "false"
        else if (op == "==") print (l+0 == r+0) ? "true" : "false"
        else                 print "bad-op"
    }')
    case "$result" in
        true)    echo "${label}=${lhs} ${op} ${rhs}: ok"; return 0 ;;
        false)   echo "${label}=${lhs} NOT ${op} ${rhs}"; return 1 ;;
        bad-op)  echo "Unknown comparator: '${op}' (expected <, <=, >, >=, ==)"; return 1 ;;
    esac
}

# fetch_metric: read one Prometheus metric value from a BN's /metrics endpoint
# via the locally-bound port. Returns empty string on miss; 5s timeout.
# Matches both a bare metric line ("metric value") and a labelled one
# ("metric{...} value"), so it keeps working if a scraped metric gains labels.
# Exposed for override in fixture tests.
function fetch_metric {
    local target="$1" metric="$2"
    local port
    port=$(get_bn_metrics_port "$target")
    curl -s --max-time 5 "http://localhost:${port}/metrics" 2>/dev/null \
        | awk -v m="$metric" '$1 == m || index($1, m "{") == 1 { print $2; exit }'
}

function assert_metric_threshold_single {
    local target="$1" metric="$2" op="$3" threshold="$4"
    local samples="${5:-1}" wait_seconds="${6:-0}"
    local i=1 value
    while [[ $i -le $samples ]]; do
        if [[ $i -gt 1 && $wait_seconds -gt 0 ]]; then sleep "$wait_seconds"; fi
        value=$(fetch_metric "$target" "$metric")
        if [[ -z "$value" ]]; then
            echo "${target}: metric '${metric}' not found on /metrics (port $(get_bn_metrics_port "$target"))"
            return 1
        fi
        if ! compare_numeric "$value" "$op" "$threshold" "${target}:${metric}"; then
            return 1
        fi
        i=$((i + 1))
    done
}

function assert_metric_threshold {
    local target="${1:-all}" metric="$2" op="$3" threshold="$4"
    local samples="${5:-1}" wait_seconds="${6:-0}"
    if [[ "$target" == "all" ]]; then
        local failed=0 results=""
        for bn in $(get_all_block_nodes); do
            local result
            result=$(assert_metric_threshold_single "$bn" "$metric" "$op" "$threshold" "$samples" "$wait_seconds") || failed=1
            results="${results}${result}\n"
        done
        echo -e "${results%\\n}"
        return $failed
    else
        assert_metric_threshold_single "$target" "$metric" "$op" "$threshold" "$samples" "$wait_seconds"
    fi
}

# block-rate-floor: derives Δblocks/Δtime from publisher_highest_block_number_inbound,
# asserts the rate (blocks/sec) is at or above min_rate. Provides the
# "blocks-per-second under chaos" signal the Phase 3 ticket called for; the BN
# does not currently expose a histogram metric for true p99.
function assert_block_rate_floor_single {
    local target="$1" min_rate="$2" window_seconds="${3:-30}"
    local baseline current rate
    baseline=$(fetch_metric "$target" "blocknode_publisher_highest_block_number_inbound")
    if [[ -z "$baseline" ]]; then
        echo "${target}: no baseline (publisher_highest_block_number_inbound unavailable)"
        return 1
    fi
    sleep "$window_seconds"
    current=$(fetch_metric "$target" "blocknode_publisher_highest_block_number_inbound")
    if [[ -z "$current" ]]; then
        echo "${target}: no current sample after ${window_seconds}s wait"
        return 1
    fi
    rate=$(awk -v b="$baseline" -v c="$current" -v w="$window_seconds" 'BEGIN { printf "%.3f", (c - b) / w }')
    if compare_numeric "$rate" ">=" "$min_rate" "${target}:rate(blk/s)" >/dev/null 2>&1; then
        echo "${target}: ${baseline} -> ${current} over ${window_seconds}s (rate=${rate}/s, floor=${min_rate}/s)"
        return 0
    else
        echo "${target}: ${baseline} -> ${current} over ${window_seconds}s (rate=${rate}/s, BELOW floor ${min_rate}/s)"
        return 1
    fi
}

function assert_block_rate_floor {
    local target="${1:-all}" min_rate="$2" window_seconds="${3:-30}"
    if [[ "$target" == "all" ]]; then
        local failed=0 results=""
        for bn in $(get_all_block_nodes); do
            local result
            result=$(assert_block_rate_floor_single "$bn" "$min_rate" "$window_seconds") || failed=1
            results="${results}${result}\n"
        done
        echo -e "${results%\\n}"
        return $failed
    else
        assert_block_rate_floor_single "$target" "$min_rate" "$window_seconds"
    fi
}

# avg-block-size-floor: derives Δbytes/Δblocks from blocknode_files_recent_total_bytes_stored
# and blocknode_files_recent_blocks_written_total, asserts the average bytes-per-block over the
# window is at or above min_bytes. Gives a real, direct block-size number instead
# of inferring size from the presence/absence of CN-side slow-request warnings.
# Same "assertions run after all events complete" constraint as block-rate-floor
# applies here: this measures the post-load-stop/recovery window, not bytes-per-
# block during active chaos. files_recent evicts on its own retention schedule,
# not expected to trigger inside a single e2e run, so the gauge behaves as an
# effectively-monotonic running total for the purposes of this Δ.
# Metric names carry the "blocknode" category prefix (MetricKey.addCategory in
# BlockNodePlugin.METRICS_CATEGORY) same as blocknode_publisher_highest_block_number_inbound
# used by block-rate-floor above — the Java-side key strings themselves
# ("files_recent_total_bytes_stored" etc.) do NOT include it.
function assert_avg_block_size_floor_single {
    local target="$1" min_bytes="$2" window_seconds="${3:-30}"
    local baseline_bytes baseline_blocks current_bytes current_blocks delta_blocks avg_size
    baseline_bytes=$(fetch_metric "$target" "blocknode_files_recent_total_bytes_stored")
    baseline_blocks=$(fetch_metric "$target" "blocknode_files_recent_blocks_written_total")
    if [[ -z "$baseline_bytes" || -z "$baseline_blocks" ]]; then
        echo "${target}: no baseline (blocknode_files_recent_total_bytes_stored/blocknode_files_recent_blocks_written_total unavailable)"
        return 1
    fi
    sleep "$window_seconds"
    current_bytes=$(fetch_metric "$target" "blocknode_files_recent_total_bytes_stored")
    current_blocks=$(fetch_metric "$target" "blocknode_files_recent_blocks_written_total")
    if [[ -z "$current_bytes" || -z "$current_blocks" ]]; then
        echo "${target}: no current sample after ${window_seconds}s wait"
        return 1
    fi
    delta_blocks=$(awk -v b="$baseline_blocks" -v c="$current_blocks" 'BEGIN { print c - b }')
    if [[ "$delta_blocks" -le 0 ]]; then
        echo "${target}: no blocks written during ${window_seconds}s window (baseline=${baseline_blocks}, current=${current_blocks}) — cannot compute average size"
        return 1
    fi
    avg_size=$(awk -v bb="$baseline_bytes" -v cb="$current_bytes" -v d="$delta_blocks" 'BEGIN { printf "%.0f", (cb - bb) / d }')
    if compare_numeric "$avg_size" ">=" "$min_bytes" "${target}:avg_block_size(bytes)" >/dev/null 2>&1; then
        echo "${target}: ${delta_blocks} blocks averaged ${avg_size} bytes/block over ${window_seconds}s (floor=${min_bytes}B)"
        return 0
    else
        echo "${target}: ${delta_blocks} blocks averaged ${avg_size} bytes/block over ${window_seconds}s (BELOW floor ${min_bytes}B)"
        return 1
    fi
}

function assert_avg_block_size_floor {
    local target="${1:-all}" min_bytes="$2" window_seconds="${3:-30}"
    if [[ "$target" == "all" ]]; then
        local failed=0 results=""
        for bn in $(get_all_block_nodes); do
            local result
            result=$(assert_avg_block_size_floor_single "$bn" "$min_bytes" "$window_seconds") || failed=1
            results="${results}${result}\n"
        done
        echo -e "${results%\\n}"
        return $failed
    else
        assert_avg_block_size_floor_single "$target" "$min_bytes" "$window_seconds"
    fi
}

# fetch_pod_logs: read recent pod logs for a target (matched by label, not
# pod-name suffix, so it doesn't depend on Deployment vs StatefulSet pod
# naming). Overridable in fixture tests.
function fetch_pod_logs {
    local target="$1" since="$2"
    kctl logs -n "${NAMESPACE}" -l "app.kubernetes.io/name=${target}" \
        --since="${since}s" --tail=10000 --prefix 2>/dev/null || true
}

function assert_log_match_single {
    local target="$1" grep_pattern="$2" since_seconds="${3:-300}"
    local logs
    logs=$(fetch_pod_logs "$target" "$since_seconds")
    if echo "$logs" | grep -F -- "$grep_pattern" >/dev/null 2>&1; then
        local count
        count=$(echo "$logs" | grep -F -c -- "$grep_pattern")
        echo "${target}: matched '${grep_pattern}' (${count} hit$([[ $count -gt 1 ]] && echo s))"
        return 0
    else
        echo "${target}: pattern '${grep_pattern}' NOT found in last ${since_seconds}s of logs"
        return 1
    fi
}

function assert_log_match {
    local target="${1:-all}" grep_pattern="$2" since_seconds="${3:-300}"
    if [[ "$target" == "all" ]]; then
        local failed=0 results=""
        for bn in $(get_all_block_nodes); do
            local result
            result=$(assert_log_match_single "$bn" "$grep_pattern" "$since_seconds") || failed=1
            results="${results}${result}\n"
        done
        echo -e "${results%\\n}"
        return $failed
    else
        assert_log_match_single "$target" "$grep_pattern" "$since_seconds"
    fi
}
