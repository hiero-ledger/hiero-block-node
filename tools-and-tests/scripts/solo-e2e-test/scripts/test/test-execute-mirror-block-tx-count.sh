#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Regression check for execute_mirror_block_tx_count's jq parsing in
# solo-test-runner.sh -- the mirror-block-tx-count event type used to
# ground-truth NLG's self-reported TPS against Mirror Node's own recorded
# transaction count per block, instead of NLG's own log or BN's own counters.
#
# Runs without a cluster: extracts the function via sed (same approach as
# test-execute-load-start.sh) and mocks `curl` with a shell function returning
# canned Mirror Node REST responses.
#
# Exit 0 on all-pass, 1 on any failure.

set -u -o pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUNNER_SCRIPT="${SCRIPT_DIR}/../solo-test-runner.sh"

passed=0
failed=0
function pass { echo "  PASS  $1"; passed=$((passed+1)); }
function fail { echo "  FAIL  $1"; failed=$((failed+1)); }

eval "$(sed -n '/^function execute_mirror_block_tx_count/,/^}/p' "${RUNNER_SCRIPT}")"

if ! declare -f execute_mirror_block_tx_count >/dev/null; then
    echo "FATAL: could not extract execute_mirror_block_tx_count from ${RUNNER_SCRIPT}"
    exit 1
fi

# ----------------------------------------------------------------------------
echo "[1] execute_mirror_block_tx_count parses number/count/timestamp from a real-shaped response"
function curl {
    echo '{"blocks":[{"number":572,"count":12451,"timestamp":{"from":"1234567890.000000000","to":"1234567891.000000000"}}]}'
}
got="$(execute_mirror_block_tx_count 5551)"
if [[ "${got}" == "Mirror Node block (port 5551): number=572 count=12451 timestamp=1234567890.000000000" ]]; then
    pass "well-formed response -> number/count/timestamp extracted"
else
    fail "expected well-formed extraction, got '${got}'"
fi

# ----------------------------------------------------------------------------
echo "[2] execute_mirror_block_tx_count falls back to 'unavailable' on an empty/malformed response"
for resp in '{"blocks":[]}' '' '{"not_blocks":true}'; do
    # shellcheck disable=SC2317
    function curl { echo "${MOCK_RESPONSE}"; }
    MOCK_RESPONSE="${resp}"
    export MOCK_RESPONSE
    got="$(execute_mirror_block_tx_count 5551)"
    label="response='${resp:-<empty>}'"
    if [[ "${got}" == "Mirror Node block (port 5551): number=unavailable count=unavailable timestamp=unavailable" ]]; then
        pass "${label} -> falls back to unavailable"
    else
        fail "${label} -> expected unavailable fallback, got '${got}'"
    fi
done

# ----------------------------------------------------------------------------
echo "[3] execute_mirror_block_tx_count defaults to port 5551 when called with no argument"
function curl {
    echo '{"blocks":[{"number":1,"count":0,"timestamp":{"from":"0.0"}}]}'
}
got="$(execute_mirror_block_tx_count)"
if [[ "${got}" == "Mirror Node block (port 5551): number=1 count=0 timestamp=0.0" ]]; then
    pass "no-arg call -> defaults to port 5551"
else
    fail "expected default port 5551, got '${got}'"
fi

echo
echo "RESULT: ${passed} passed, ${failed} failed"
[[ $failed -eq 0 ]]