#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Fixture-based unit test for the signature-transition assertion:
#
#   - assert_signature_transition in solo-test-runner.sh, against stand-in monitors that
#     hang, leak a descendant, or fail immediately. These need GNU `timeout`; they skip
#     when it is absent (stock macOS) rather than hanging for 300s.
#   - monitor-block-proofs.sh's own max_block*2 deadline, against a fake grpcurl. These
#     need no `timeout`: the monitor bounds itself.
#
# Runs without a cluster. Exit 0 on all-pass, 1 on any failure.

set -u -o pipefail
TEST_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REAL_PATH="${PATH}"

# The runner parses its options at source time, so hand it a real test file; main()
# itself stays unrun (it is guarded on BASH_SOURCE). Sourcing overwrites SCRIPT_DIR
# with the runner's own directory, so the fixture override has to come after.
# shellcheck disable=SC1091
source "${TEST_DIR}/../solo-test-runner.sh" --test "${TEST_DIR}/../../tests/tss-signature-transition.yaml"

# assert_signature_transition pre-flights its target with assert_block_available_single,
# which needs a live Block Node. These tests are about what happens once the monitor is
# running, so stand the pre-flight down rather than mocking serverStatus.
function assert_block_available_single {
    echo "$1: Blocks 0-1"
}

passed=0
failed=0
skipped=0

# Same signature as test-archive-assertions.sh's record: the real value is printed on
# failure so a reader does not have to reconstruct the output by hand.
function record {
    local name="$1" expected="$2" actual="$3"
    if [[ "${actual}" == "${expected}" ]]; then
        echo "  PASS  ${name}"
        passed=$((passed + 1))
    else
        echo "  FAIL  ${name} (expected ${expected}, got ${actual})"
        failed=$((failed + 1))
    fi
}

# record_contains <name> <substring> <output> — on mismatch reports the last output line,
# which is the summary assert_signature_transition ends with.
function record_contains {
    local name="$1" needle="$2" haystack="$3" actual
    if [[ "${haystack}" == *"${needle}"* ]]; then
        actual="${needle}"
    else
        actual="$(echo "${haystack}" | tail -1)"
    fi
    record "${name}" "${needle}" "${actual}"
}

function skip {
    echo "  SKIP  $1"
    skipped=$((skipped + 1))
}

# under <seconds> <ceiling> — an "elapsed" value record can compare for equality.
function under {
    if [[ "$1" -lt "$2" ]]; then echo "under $2s"; else echo "$1s"; fi
}

# ============================================================================
# assert_signature_transition's `timeout` wrapper
# ============================================================================

# Any existing directory satisfies validate_proto_path; the fake monitors ignore it.
# max_block 1 plus 1s of grace gives a 3s ceiling.
# shellcheck disable=SC2034 # consumed by the sourced runner
SIGNATURE_TRANSITION_GRACE_SECONDS=1

if [[ -z "${TIMEOUT_BIN}" ]]; then
    echo "assert_signature_transition's timeout wrapper"
    skip "hung monitor / leaked descendant scenarios (no timeout or gtimeout on PATH)"
else
    echo "assert_signature_transition with a hung monitor-block-proofs.sh"

    SCRIPT_DIR="${TEST_DIR}/fixtures/signature-transition"
    # shellcheck disable=SC2034 # consumed by the sourced runner
    PROTO_PATH="${SCRIPT_DIR}"
    started=${SECONDS}
    status=0
    output=$(assert_signature_transition block-node-1 1 2>/dev/null) || status=$?
    elapsed=$((SECONDS - started))

    record "fails instead of reporting a transition" "rc=1" "rc=${status}"
    # The 3s ceiling plus process teardown; anything near the fixture's 300s sleep means
    # the wrapper did not fire.
    record "returns rather than hanging" "under 10s" "$(under "${elapsed}" 10)"
    record_contains "names the timeout rather than 'WRAPS not detected'" \
        "exceeded 3s and was killed by the wrapper" "${output}"

    echo ""
    echo "assert_signature_transition with a monitor-block-proofs.sh that leaks a descendant"

    SCRIPT_DIR="${TEST_DIR}/fixtures/signature-transition-leaked-descendant"
    # shellcheck disable=SC2034 # consumed by the sourced runner
    PROTO_PATH="${SCRIPT_DIR}"
    started=${SECONDS}
    status=0
    output=$(assert_signature_transition block-node-1 1 2>/dev/null) || status=$?
    elapsed=$((SECONDS - started))

    record "fails instead of reporting a transition" "rc=1" "rc=${status}"
    # The leaked helper sleeps 300s. Anything near that means the runner waited on the
    # descendant's copy of a pipe rather than on the child `timeout` actually killed.
    record "returns despite the leaked descendant" "under 10s" "$(under "${elapsed}" 10)"
    record_contains "names the timeout rather than 'WRAPS not detected'" \
        "exceeded 3s and was killed by the wrapper" "${output}"
fi

echo ""
echo "assert_signature_transition with a monitor-block-proofs.sh that fails immediately"

SCRIPT_DIR="${TEST_DIR}/fixtures/signature-transition-failure"
# shellcheck disable=SC2034 # consumed by the sourced runner
PROTO_PATH="${SCRIPT_DIR}"
status=0
output=$(assert_signature_transition block-node-1 1 2>/dev/null) || status=$?

record "fails instead of reporting a transition" "rc=1" "rc=${status}"
record_contains "reports 'WRAPS not detected' with the exit status, not a timeout" \
    "WRAPS not detected within 1 blocks (exit 1)" "${output}"

echo ""
echo "assert_signature_transition with a monitor-block-proofs.sh that found WRAPS imprecisely"

SCRIPT_DIR="${TEST_DIR}/fixtures/signature-transition-wraps-imprecise"
# shellcheck disable=SC2034 # consumed by the sourced runner
PROTO_PATH="${SCRIPT_DIR}"
status=0
output=$(assert_signature_transition block-node-1 1 2>/dev/null) || status=$?

# Observing WRAPS satisfies the assertion; the deadline only cost the exact block number.
record "treats an unnarrowed transition as a pass" "rc=0" "rc=${status}"
record_contains "reports the block WRAPS was seen at, not the caveat line" \
    "Schnorr -> WRAPS at block 1" "${output}"

# ============================================================================
# monitor-block-proofs.sh's own max_block*2 deadline (real monitor, fake grpcurl)
# ============================================================================
# max_block 2 puts the monitor's deadline at 4s; the grace keeps the wrapper, when
# present, well clear of it so the monitor's own deadline is what fires. A stride of 1
# is needed to enter the scan loop at all with a max_block that small.
SCRIPT_DIR="${TEST_DIR}/.."
# shellcheck disable=SC2034 # consumed by the sourced runner
SIGNATURE_TRANSITION_GRACE_SECONDS=30
export MONITOR_BLOCK_STEP=1

# A fixture missing from the checkout otherwise surfaces only as "PROTO_PATH invalid".
for fixture in "${TEST_DIR}"/fixtures/monitor-*/grpcurl; do
    [[ -x "${fixture}" ]] || { echo "  FAIL  fixture missing: ${fixture}"; failed=$((failed + 1)); }
done

echo ""
echo "monitor-block-proofs.sh hitting its own deadline with no WRAPS seen"

PROTO_PATH="${TEST_DIR}/fixtures/monitor-hard-timeout"
PATH="${PROTO_PATH}:${REAL_PATH}"
started=${SECONDS}
status=0
output=$(assert_signature_transition block-node-1 2 2>/dev/null) || status=$?
elapsed=$((SECONDS - started))
PATH="${REAL_PATH}"

record "fails instead of reporting a transition" "rc=1" "rc=${status}"
record "returns rather than waiting for blocks forever" "under 20s" "$(under "${elapsed}" 20)"
# Exit 3, not the 143 a bare SIGTERM death produces and not the wrapper's 124.
record_contains "names the monitor's own deadline" \
    "hit its own 4s deadline (max_block * 2)" "${output}"


echo ""
echo "Passed: ${passed}, Failed: ${failed}, Skipped: ${skipped}"
[[ ${failed} -eq 0 ]]
