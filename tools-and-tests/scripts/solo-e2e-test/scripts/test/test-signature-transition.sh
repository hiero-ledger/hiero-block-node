#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Fixture-based unit test for assert_signature_transition in solo-test-runner.sh:
# a monitor-block-proofs.sh that never returns must be killed by the `timeout`
# wrapper and reported as a timeout, not as "WRAPS not detected"; one that fails
# immediately must still be reported as "WRAPS not detected", not a timeout.
#
# Runs without a cluster: SCRIPT_DIR is pointed at fixture directories whose
# monitor-block-proofs.sh either sleeps or exits 1.
# Exit 0 on all-pass, 1 on any failure.

set -u -o pipefail
TEST_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# The runner parses its options at source time, so hand it a real test file; main()
# itself stays unrun (it is guarded on BASH_SOURCE). Sourcing overwrites SCRIPT_DIR
# with the runner's own directory, so the fixture override has to come after.
# shellcheck disable=SC1091
source "${TEST_DIR}/../solo-test-runner.sh" --test "${TEST_DIR}/../../tests/tss-signature-transition.yaml"

SCRIPT_DIR="${TEST_DIR}/fixtures/signature-transition"
# Any existing directory satisfies validate_proto_path; the fake monitor ignores it.
# shellcheck disable=SC2034 # consumed by the sourced runner
PROTO_PATH="${SCRIPT_DIR}"
# max_block 1 plus 1s of grace gives a 3s ceiling.
# shellcheck disable=SC2034 # consumed by the sourced runner
SIGNATURE_TRANSITION_GRACE_SECONDS=1

passed=0
failed=0
function record {
    local name="$1" ok="$2"
    if [[ "${ok}" == "true" ]]; then
        echo "  PASS  ${name}"
        passed=$((passed + 1))
    else
        echo "  FAIL  ${name}"
        failed=$((failed + 1))
    fi
}

echo "assert_signature_transition with a hung monitor-block-proofs.sh"

started=${SECONDS}
status=0
output=$(assert_signature_transition block-node-1 1 2>/dev/null) || status=$?
elapsed=$((SECONDS - started))

[[ "${status}" -ne 0 ]] && ok=true || ok=false
record "fails instead of reporting a transition (rc=${status})" "${ok}"

# The 3s ceiling plus process teardown; anything near the fixture's 300s sleep means
# the wrapper did not fire.
[[ "${elapsed}" -lt 10 ]] && ok=true || ok=false
record "returns in ${elapsed}s rather than hanging" "${ok}"

[[ "${output}" == *"exceeded 3s and was killed"* ]] && ok=true || ok=false
record "names the timeout rather than 'WRAPS not detected'" "${ok}"

echo ""
echo "assert_signature_transition with a monitor-block-proofs.sh that fails immediately"

SCRIPT_DIR="${TEST_DIR}/fixtures/signature-transition-failure"
# shellcheck disable=SC2034 # consumed by the sourced runner
PROTO_PATH="${SCRIPT_DIR}"
status=0
output=$(assert_signature_transition block-node-1 1 2>/dev/null) || status=$?

[[ "${status}" -ne 0 ]] && ok=true || ok=false
record "fails instead of reporting a transition (rc=${status})" "${ok}"

[[ "${output}" == *"WRAPS not detected within 1 blocks (exit 1)"* ]] && ok=true || ok=false
record "reports 'WRAPS not detected' with the exit status, not a timeout" "${ok}"

echo ""
echo "Passed: ${passed}, Failed: ${failed}"
[[ ${failed} -eq 0 ]]
