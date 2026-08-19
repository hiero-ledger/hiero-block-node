#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Fixture-based unit tests for the Mirror Node assertion primitives in
# scripts/lib/mirror-assertions.sh:
#   - assert_mirror_blocks_increasing  (MN blocks still flowing)
#   - assert_mirror_lag                (MN not falling behind BN)
#
# Runs without a cluster: overrides fetch_mn_latest_block and
# fetch_bn_server_status to serve pre-recorded fixtures.
# Exit 0 on all-pass, 1 on any failure.

set -u -o pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LIB="${SCRIPT_DIR}/../lib/mirror-assertions.sh"

# Topology resolution used by assert_mirror_lag_single (reads mirror_nodes YAML).
TOPOLOGIES_DIR="${SCRIPT_DIR}/../../topologies"
TOPOLOGY="single"

# shellcheck disable=SC1090
source "${LIB}"

# Mocks override lib definitions; must come AFTER source so they are not overwritten.
function get_all_mirror_nodes  { echo "mirror-1"; }
function get_mn_rest_port      { echo 5551; }
function fetch_mn_latest_block { echo "1842"; }
function fetch_bn_server_status { echo '{"firstAvailableBlock":"0","lastAvailableBlock":"1872"}'; }

passed=0
failed=0
function pass { echo "  PASS  $1"; passed=$((passed+1)); }
function fail { echo "  FAIL  $1"; failed=$((failed+1)); }

# ----------------------------------------------------------------------------
echo "[1] assert_mirror_blocks_increasing: green / red / missing-data"

# Green: baseline < current.
# fetch_mn_latest_block is called inside subshells; use a counter file to
# return different values on successive calls (same technique as test-chaos-assertions.sh).
COUNTER_FILE=$(mktemp)

function fetch_mn_latest_block_seq {
    local i
    i=$(cat "$COUNTER_FILE")
    i=$((i + 1))
    echo "$i" > "$COUNTER_FILE"
    case "$i" in
        1) echo "$1" ;;   # baseline
        2) echo "$2" ;;   # current (attempt 1)
        *) echo ""   ;;
    esac
}

echo 0 > "$COUNTER_FILE"
function fetch_mn_latest_block { fetch_mn_latest_block_seq 1800 1842; }
if assert_mirror_blocks_increasing_single "mirror-1" "0" "3" >/dev/null 2>&1; then
    pass "mirror-blocks-increasing green (1800 -> 1842)"
else
    fail "mirror-blocks-increasing green"
fi

# Red: current == baseline → not increasing after all attempts.
echo 0 > "$COUNTER_FILE"
function fetch_mn_latest_block { fetch_mn_latest_block_seq 1842 1842; }
if ! assert_mirror_blocks_increasing_single "mirror-1" "0" "1" >/dev/null 2>&1; then
    pass "mirror-blocks-increasing red (1842 -> 1842, no increase)"
else
    fail "mirror-blocks-increasing red"
fi

# Missing data on baseline → fail immediately.
function fetch_mn_latest_block { echo ""; }
if ! assert_mirror_blocks_increasing_single "mirror-1" "0" "1" >/dev/null 2>&1; then
    pass "mirror-blocks-increasing missing-data → fail"
else
    fail "mirror-blocks-increasing missing-data should fail"
fi

# 'all' fans out over get_all_mirror_nodes (one MN, green).
echo 0 > "$COUNTER_FILE"
function fetch_mn_latest_block { fetch_mn_latest_block_seq 1800 1842; }
if assert_mirror_blocks_increasing "all" "0" "3" >/dev/null 2>&1; then
    pass "mirror-blocks-increasing all=green (fans out)"
else
    fail "mirror-blocks-increasing all=green"
fi

rm -f "$COUNTER_FILE"

# ----------------------------------------------------------------------------
echo "[2] assert_mirror_lag: green / red / missing-mn / missing-bn"

# Restore a fixed fetch_mn_latest_block for lag tests.
function fetch_mn_latest_block { echo "1842"; }

# Green: mn=1842, bn=1872, lag=30, max=30 → pass (lag not strictly > max).
function fetch_bn_server_status { echo '{"firstAvailableBlock":"0","lastAvailableBlock":"1872"}'; }
if assert_mirror_lag_single "mirror-1" "30" >/dev/null 2>&1; then
    pass "mirror-lag green (lag=30, max=30)"
else
    fail "mirror-lag green"
fi

# Red: mn=1800, bn=1842, lag=42, max=30 → fail.
function fetch_mn_latest_block { echo "1800"; }
function fetch_bn_server_status { echo '{"firstAvailableBlock":"0","lastAvailableBlock":"1842"}'; }
if ! assert_mirror_lag_single "mirror-1" "30" >/dev/null 2>&1; then
    pass "mirror-lag red (lag=42, max=30)"
else
    fail "mirror-lag red"
fi

# Missing MN data → fail.
function fetch_mn_latest_block { echo ""; }
function fetch_bn_server_status { echo '{"firstAvailableBlock":"0","lastAvailableBlock":"1842"}'; }
if ! assert_mirror_lag_single "mirror-1" "30" >/dev/null 2>&1; then
    pass "mirror-lag missing MN data → fail"
else
    fail "mirror-lag missing MN data should fail"
fi

# Missing BN data → fail.
function fetch_mn_latest_block { echo "1842"; }
function fetch_bn_server_status { echo '{}'; }
if ! assert_mirror_lag_single "mirror-1" "30" >/dev/null 2>&1; then
    pass "mirror-lag missing BN data → fail"
else
    fail "mirror-lag missing BN data should fail"
fi

# 'all' fans out over get_all_mirror_nodes (one MN, green).
function fetch_mn_latest_block { echo "1842"; }
function fetch_bn_server_status { echo '{"firstAvailableBlock":"0","lastAvailableBlock":"1872"}'; }
if assert_mirror_lag "all" "30" >/dev/null 2>&1; then
    pass "mirror-lag all=green (fans out)"
else
    fail "mirror-lag all=green"
fi

# Output includes lag details.
output=$(assert_mirror_lag_single "mirror-1" "30" 2>&1)
if echo "$output" | grep -q "lag="; then
    pass "mirror-lag output contains lag details"
else
    fail "mirror-lag output missing lag details (got: $output)"
fi

# ----------------------------------------------------------------------------
echo
echo "RESULT: ${passed} passed, ${failed} failed"
[[ $failed -eq 0 ]]
