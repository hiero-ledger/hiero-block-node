#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Regression check for execute_mirror_tx_presence's per-type query/parsing
# logic in solo-test-runner.sh -- the mirror-tx-presence event type used to
# confirm whether a HAPI transaction type landed at all within a time
# window, immune to the sampling-skew problem that made
# execute_mirror_tx_distribution's fixed-size "most recent N" sample
# unreliable when one type (e.g. HeliSwapJob's ETHEREUMTRANSACTION traffic)
# dominates the others.
#
# Runs without a cluster: extracts the function via sed (same approach as
# test-execute-load-start.sh) and mocks `curl` with a shell function that
# inspects its own arguments to respond differently per transactiontype.
#
# Exit 0 on all-pass, 1 on any failure.

set -u -o pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUNNER_SCRIPT="${SCRIPT_DIR}/../solo-test-runner.sh"

passed=0
failed=0
function pass { echo "  PASS  $1"; passed=$((passed+1)); }
function fail { echo "  FAIL  $1"; failed=$((failed+1)); }

eval "$(sed -n '/^function execute_mirror_tx_presence/,/^}/p' "${RUNNER_SCRIPT}")"

if ! declare -f execute_mirror_tx_presence >/dev/null; then
    echo "FATAL: could not extract execute_mirror_tx_presence from ${RUNNER_SCRIPT}"
    exit 1
fi

# ----------------------------------------------------------------------------
echo "[1] execute_mirror_tx_presence reports present/absent per type independently"
function curl {
    case "$*" in
        *transactiontype=CRYPTOTRANSFER*) echo '{"transactions":[{"name":"CRYPTOTRANSFER"}]}' ;;
        *transactiontype=CONTRACTCALL*) echo '{"transactions":[]}' ;;
        *) echo '{"transactions":[]}' ;;
    esac
}
got="$(execute_mirror_tx_presence 5551 60 "CRYPTOTRANSFER,CONTRACTCALL")"
if [[ "${got}" == "Mirror Node tx type presence (port 5551, window=60s): CRYPTOTRANSFER=present CONTRACTCALL=absent " ]]; then
    pass "mixed present/absent types -> each reported independently"
else
    fail "expected independent present/absent per type, got '${got}'"
fi

# ----------------------------------------------------------------------------
echo "[2] execute_mirror_tx_presence treats an empty/malformed response as absent, not an error"
function curl { echo ''; }
got="$(execute_mirror_tx_presence 5551 60 "CRYPTOTRANSFER")"
if [[ "${got}" == "Mirror Node tx type presence (port 5551, window=60s): CRYPTOTRANSFER=absent " ]]; then
    pass "empty response -> absent (not a crash)"
else
    fail "expected absent fallback on empty response, got '${got}'"
fi

# ----------------------------------------------------------------------------
echo "[3] execute_mirror_tx_presence defaults to port 5551, 60s window, and the 4 known NLG job types"
function curl { echo '{"transactions":[]}'; }
got="$(execute_mirror_tx_presence)"
if [[ "${got}" == "Mirror Node tx type presence (port 5551, window=60s): CRYPTOTRANSFER=absent CONSENSUSSUBMITMESSAGE=absent CONTRACTCALL=absent ETHEREUMTRANSACTION=absent " ]]; then
    pass "no-arg call -> defaults to port 5551, window=60s, all 4 known types"
else
    fail "expected default port/window/types, got '${got}'"
fi

echo
echo "RESULT: ${passed} passed, ${failed} failed"
[[ $failed -eq 0 ]]
