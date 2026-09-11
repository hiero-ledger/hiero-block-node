#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Regression check for execute_mirror_tx_distribution's jq/tally logic in
# solo-test-runner.sh -- the mirror-tx-distribution event type used to
# confirm which HAPI transaction types are actually landing on-chain when
# NLG runs multiple transaction types concurrently and its own job classes
# don't all self-report (LongevityLoadTest, see finding 5e in the handoff).
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

eval "$(sed -n '/^function execute_mirror_tx_distribution/,/^}/p' "${RUNNER_SCRIPT}")"

if ! declare -f execute_mirror_tx_distribution >/dev/null; then
    echo "FATAL: could not extract execute_mirror_tx_distribution from ${RUNNER_SCRIPT}"
    exit 1
fi

# ----------------------------------------------------------------------------
echo "[1] execute_mirror_tx_distribution tallies transaction types, most-frequent first"
function curl {
    echo '{"transactions":[
        {"name":"CRYPTOTRANSFER"},{"name":"CRYPTOTRANSFER"},{"name":"CRYPTOTRANSFER"},
        {"name":"CONSENSUSSUBMITMESSAGE"},{"name":"CONSENSUSSUBMITMESSAGE"},
        {"name":"CONTRACTCALL"}
    ]}'
}
got="$(execute_mirror_tx_distribution 5551 100)"
if [[ "${got}" == "Mirror Node tx distribution (port 5551, sample=100): CRYPTOTRANSFER=3 CONSENSUSSUBMITMESSAGE=2 CONTRACTCALL=1 " ]]; then
    pass "mixed response -> tallied and sorted by frequency"
else
    fail "expected a frequency-sorted tally, got '${got}'"
fi

# ----------------------------------------------------------------------------
echo "[2] execute_mirror_tx_distribution falls back to 'unavailable' on an empty/malformed response"
for resp in '{"transactions":[]}' '' '{"not_transactions":true}'; do
    # shellcheck disable=SC2317
    function curl { echo "${MOCK_RESPONSE}"; }
    MOCK_RESPONSE="${resp}"
    export MOCK_RESPONSE
    got="$(execute_mirror_tx_distribution 5551 100)"
    label="response='${resp:-<empty>}'"
    if [[ "${got}" == "Mirror Node tx distribution (port 5551, sample=100): unavailable" ]]; then
        pass "${label} -> falls back to unavailable"
    else
        fail "${label} -> expected unavailable fallback, got '${got}'"
    fi
done

# ----------------------------------------------------------------------------
echo "[3] execute_mirror_tx_distribution defaults to port 5551 and sample size 100"
function curl {
    echo '{"transactions":[{"name":"CRYPTOTRANSFER"}]}'
}
got="$(execute_mirror_tx_distribution)"
if [[ "${got}" == "Mirror Node tx distribution (port 5551, sample=100): CRYPTOTRANSFER=1 " ]]; then
    pass "no-arg call -> defaults to port 5551, sample=100"
else
    fail "expected default port/sample, got '${got}'"
fi

echo
echo "RESULT: ${passed} passed, ${failed} failed"
[[ $failed -eq 0 ]]
