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
