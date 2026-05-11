#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Fixture-based unit tests for the Phase 3 latency-aware assertions in
# solo-test-runner.sh:
#   - compare_numeric (helper)
#   - assert_metric_threshold (primitive)
#   - assert_block_rate_floor  (sugar)
#   - assert_log_match         (used by backfill-triggered)
#
# Runs without a cluster: overrides fetch_metric / fetch_pod_logs to serve
# pre-recorded fixtures. Exit 0 on all-pass, 1 on any failure.

set -u -o pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LIB="${SCRIPT_DIR}/../lib/chaos-assertions.sh"
FIXTURES="${SCRIPT_DIR}/fixtures"

NAMESPACE="solo-network"

# Provide mocks BEFORE sourcing the lib — the lib reads from these on call.
function get_bn_metrics_port { echo 16007; }
function get_all_block_nodes  { echo "block-node-1"; }
function kctl { return 0; }

# shellcheck disable=SC1090
source "${LIB}"

passed=0
failed=0
function pass { echo "  PASS  $1"; passed=$((passed+1)); }
function fail { echo "  FAIL  $1"; failed=$((failed+1)); }

# ----------------------------------------------------------------------------
echo "[1] compare_numeric: all comparators (one method, table-driven)"
# Group simple cases — per CLAUDE.md, don't proliferate methods for trivial assertions.
for tc in "1 < 2 OK" "2 < 1 NO" "1 <= 1 OK" "5 >= 5 OK" "5 > 4 OK" \
          "1 == 1 OK" "1 == 2 NO" "1.5 < 2.1 OK" "3.14 > 3.13 OK"; do
    # shellcheck disable=SC2086
    set -- $tc
    l=$1; op=$2; r=$3; expected=$4
    if compare_numeric "$l" "$op" "$r" "test" >/dev/null 2>&1; then actual=OK; else actual=NO; fi
    if [[ "$actual" == "$expected" ]]; then
        pass "compare_numeric $l $op $r => $expected"
    else
        fail "compare_numeric $l $op $r => expected $expected, got $actual"
    fi
done

# Bad operator → fail (returns 1). Capture into a var first; pipefail would
# otherwise propagate compare_numeric's non-zero exit through the grep pipe.
bad_op_output=$(compare_numeric 1 "!=" 2 "test" 2>&1 || true)
if echo "$bad_op_output" | grep -q "Unknown comparator"; then
    pass "compare_numeric rejects unknown operator"
else
    fail "compare_numeric did not reject unknown operator (got: $bad_op_output)"
fi

# ----------------------------------------------------------------------------
echo "[2] assert_metric_threshold: green / red / metric-missing"
function fetch_metric {
    awk -v m="$2" '$1 == m { print $2; exit }' "${FIXTURES}/metrics-sample.txt"
}
# Green: highest_block_number_inbound is 1842, assert >= 100
if assert_metric_threshold "block-node-1" "blocknode_publisher_highest_block_number_inbound" ">=" 100 >/dev/null 2>&1; then
    pass "metric-threshold green (1842 >= 100)"
else
    fail "metric-threshold green"
fi
# Red: assert >= 99999
if ! assert_metric_threshold "block-node-1" "blocknode_publisher_highest_block_number_inbound" ">=" 99999 >/dev/null 2>&1; then
    pass "metric-threshold red (1842 < 99999)"
else
    fail "metric-threshold red"
fi
# Metric not present
if ! assert_metric_threshold "block-node-1" "nonexistent_metric" "==" 0 >/dev/null 2>&1; then
    pass "metric-threshold missing metric → fail"
else
    fail "metric-threshold missing metric should fail"
fi
# Comparator '==' with exact match
if assert_metric_threshold "block-node-1" "blocknode_publisher_stream_errors_total" "==" 0 >/dev/null 2>&1; then
    pass "metric-threshold == with exact match (errors == 0)"
else
    fail "metric-threshold exact match"
fi

# ----------------------------------------------------------------------------
echo "[3] assert_block_rate_floor: derives Δblocks/Δtime, compares to floor"
# fetch_metric runs in command substitution (subshell) inside the assertion,
# so we use a file-based counter to persist state across calls.
COUNTER_FILE=$(mktemp)
function fetch_metric_seq {
    local i
    i=$(cat "$COUNTER_FILE")
    i=$((i + 1))
    echo "$i" > "$COUNTER_FILE"
    case "$i" in
        1) echo "$1" ;;   # baseline
        2) echo "$2" ;;   # current
        *) echo ""   ;;
    esac
}

# Two-sample green: baseline=100, current=115, window=1s -> 15/s >= 0.5/s
echo 0 > "$COUNTER_FILE"
function fetch_metric { fetch_metric_seq 100 115; }
if assert_block_rate_floor "block-node-1" "0.5" "1" >/dev/null 2>&1; then
    pass "block-rate-floor green (15/s >= 0.5/s)"
else
    fail "block-rate-floor green"
fi

# No-progress red: baseline=100, current=100, window=1s -> 0/s < 0.5/s
echo 0 > "$COUNTER_FILE"
function fetch_metric { fetch_metric_seq 100 100; }
if ! assert_block_rate_floor "block-node-1" "0.5" "1" >/dev/null 2>&1; then
    pass "block-rate-floor red (0/s < 0.5/s)"
else
    fail "block-rate-floor red"
fi

# Baseline missing
function fetch_metric { echo ""; }
if ! assert_block_rate_floor "block-node-1" "0.5" "1" >/dev/null 2>&1; then
    pass "block-rate-floor red (no baseline available)"
else
    fail "block-rate-floor missing baseline should fail"
fi

rm -f "$COUNTER_FILE"

# ----------------------------------------------------------------------------
echo "[4] assert_log_match (powers backfill-triggered): pattern present / absent"
function fetch_pod_logs { cat "${FIXTURES}/logs-with-backfill.txt"; }
if assert_log_match "block-node-1" "Received backfill" 300 >/dev/null 2>&1; then
    pass "log-match green ('Received backfill' present, 3 hits)"
else
    fail "log-match green"
fi
function fetch_pod_logs { cat "${FIXTURES}/logs-without-backfill.txt"; }
if ! assert_log_match "block-node-1" "Received backfill" 300 >/dev/null 2>&1; then
    pass "log-match red ('Received backfill' absent)"
else
    fail "log-match red"
fi
# Multiple hits — count surfaces in the output
function fetch_pod_logs { cat "${FIXTURES}/logs-with-backfill.txt"; }
output=$(assert_log_match "block-node-1" "Received backfill" 300 2>&1)
if echo "$output" | grep -q "3 hits"; then
    pass "log-match reports hit count"
else
    fail "log-match did not report hit count (got: $output)"
fi

# ----------------------------------------------------------------------------
echo
echo "RESULT: ${passed} passed, ${failed} failed"
[[ $failed -eq 0 ]]
