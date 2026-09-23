#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Regression check for assert_blocks_diverged and the new
# assert_blocks_diverged_max in solo-test-runner.sh. Neither had unit
# coverage before this -- assert_blocks_diverged_max exists because a single
# fixed-delay snapshot's spread varies a lot run to run (session data: 1-6 at
# the same rate/content profile), so blocks-diverged can now sample several
# snapshots across the chaos window and pass on the PEAK spread seen, instead
# of gambling on one point in time.
#
# Runs without a cluster: extracts both functions via sed (same approach as
# test-execute-load-start.sh) and writes real fixture files to the same
# /tmp/chaos-snapshot-<id>.txt path the functions themselves hardcode, using
# test-only IDs to avoid colliding with a real run's snapshots.
#
# Exit 0 on all-pass, 1 on any failure.

set -u -o pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUNNER_SCRIPT="${SCRIPT_DIR}/../solo-test-runner.sh"

passed=0
failed=0
function pass { echo "  PASS  $1"; passed=$((passed+1)); }
function fail { echo "  FAIL  $1"; failed=$((failed+1)); }

eval "$(sed -n '/^function assert_blocks_diverged {/,/^}/p' "${RUNNER_SCRIPT}")"
eval "$(sed -n '/^function assert_blocks_diverged_max {/,/^}/p' "${RUNNER_SCRIPT}")"

if ! declare -f assert_blocks_diverged >/dev/null; then
    echo "FATAL: could not extract assert_blocks_diverged from ${RUNNER_SCRIPT}"
    exit 1
fi
if ! declare -f assert_blocks_diverged_max >/dev/null; then
    echo "FATAL: could not extract assert_blocks_diverged_max from ${RUNNER_SCRIPT}"
    exit 1
fi

written_snapshots=()
function write_snapshot {
    local id="$1"
    shift
    local file="/tmp/chaos-snapshot-${id}.txt"
    printf '%s\n' "$@" > "${file}"
    written_snapshots+=("${file}")
}
function cleanup_snapshots {
    for f in "${written_snapshots[@]}"; do
        rm -f "${f}"
    done
}
trap cleanup_snapshots EXIT

# ----------------------------------------------------------------------------
echo "[1] assert_blocks_diverged: PASS when spread >= min_spread"
write_snapshot "test-single-pass" "block-node-1=100" "block-node-2=105" "block-node-3=105"
if assert_blocks_diverged "test-single-pass" 3 >/dev/null; then
    pass "spread 5 >= min_spread 3"
else
    fail "expected PASS for spread 5 >= min_spread 3"
fi

# ----------------------------------------------------------------------------
echo "[2] assert_blocks_diverged: FAIL when spread < min_spread"
write_snapshot "test-single-fail" "block-node-1=100" "block-node-2=101" "block-node-3=101"
if assert_blocks_diverged "test-single-fail" 3 >/dev/null; then
    fail "expected FAIL for spread 1 < min_spread 3"
else
    pass "spread 1 < min_spread 3"
fi

# ----------------------------------------------------------------------------
echo "[3] assert_blocks_diverged: FAIL when snapshot file is missing"
if assert_blocks_diverged "test-does-not-exist" 3 >/dev/null 2>&1; then
    fail "expected FAIL for a missing snapshot file"
else
    pass "missing snapshot file -> FAIL, not a crash"
fi

# ----------------------------------------------------------------------------
echo "[4] assert_blocks_diverged_max: PASS when the PEAK across several snapshots meets the threshold (not the first or last)"
write_snapshot "test-max-low-1" "block-node-1=200" "block-node-2=201" "block-node-3=201"
write_snapshot "test-max-high" "block-node-1=200" "block-node-2=207" "block-node-3=207"
write_snapshot "test-max-low-2" "block-node-1=210" "block-node-2=211" "block-node-3=211"
got="$(assert_blocks_diverged_max 5 test-max-low-1 test-max-high test-max-low-2)"
if echo "${got}" | grep -q "PASS: max spread 7 (at snapshot 'test-max-high'"; then
    pass "picked the real peak (spread 7 at the middle snapshot), not the first/last"
else
    fail "expected PASS quoting max spread 7 at test-max-high, got: ${got}"
fi

# ----------------------------------------------------------------------------
echo "[5] assert_blocks_diverged_max: FAIL when every snapshot is below the threshold"
write_snapshot "test-max-allfail-1" "block-node-1=300" "block-node-2=301" "block-node-3=301"
write_snapshot "test-max-allfail-2" "block-node-1=310" "block-node-2=311" "block-node-3=312"
if assert_blocks_diverged_max 5 test-max-allfail-1 test-max-allfail-2 >/dev/null; then
    fail "expected FAIL when best spread (2) is still below min_spread 5"
else
    pass "best spread 2 < min_spread 5 -> FAIL"
fi

# ----------------------------------------------------------------------------
echo "[6] assert_blocks_diverged_max: skips a missing snapshot with a WARNING but still passes using the ones that exist"
write_snapshot "test-max-partial-ok" "block-node-1=400" "block-node-2=409" "block-node-3=409"
got="$(assert_blocks_diverged_max 5 test-max-partial-ok test-max-partial-missing)"
if echo "${got}" | grep -q "WARNING: snapshot 'test-max-partial-missing' not found" && echo "${got}" | grep -q "PASS: max spread 9"; then
    pass "missing snapshot warned and skipped, remaining snapshot still evaluated"
else
    fail "expected a WARNING for the missing snapshot plus a PASS on the real one, got: ${got}"
fi

# ----------------------------------------------------------------------------
echo "[7] assert_blocks_diverged_max: FAILs cleanly when NONE of the requested snapshots exist"
if assert_blocks_diverged_max 3 test-max-none-1 test-max-none-2 >/dev/null 2>&1; then
    fail "expected FAIL when no snapshot in the list exists"
else
    pass "no snapshots found -> FAIL, not a crash"
fi

# ----------------------------------------------------------------------------
echo "[8] assert_blocks_diverged_max: FAILs cleanly when called with zero snapshot IDs"
if assert_blocks_diverged_max 3 >/dev/null 2>&1; then
    fail "expected FAIL when called with no snapshot IDs at all"
else
    pass "zero snapshot IDs -> FAIL, not a crash"
fi

echo
echo "RESULT: ${passed} passed, ${failed} failed"
[[ $failed -eq 0 ]]
