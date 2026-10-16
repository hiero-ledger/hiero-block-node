#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Fixture-based unit test for the tss-block-proofs assertion:
#
#   - assert_tss_block_proofs in solo-test-runner.sh, against stand-in monitors that
#     hang, leak a descendant, fail immediately, or report an unnarrowed transition. The
#     hang and leak scenarios need GNU `timeout`; they skip when it is absent (stock
#     macOS) rather than hanging for 300s.
#   - the real monitor-block-proofs.sh against fake grpcurl fixtures: its own max_block*2
#     deadline, the pre-Nova Schnorr then WRAPS transition, the Nova layout on every
#     sampled block, a mix of the two, and an unknown signature size. These need no
#     `timeout`: the monitor bounds itself.
#
# Runs without a cluster. Exit 0 on all-pass, 1 on any failure.

set -u -o pipefail
TEST_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REAL_PATH="${PATH}"

# The runner parses its options at source time, so hand it a real test file; main()
# itself stays unrun (it is guarded on BASH_SOURCE). Sourcing overwrites SCRIPT_DIR
# with the runner's own directory, so the fixture override has to come after.
# shellcheck disable=SC1091
source "${TEST_DIR}/../solo-test-runner.sh" --test "${TEST_DIR}/../../tests/tss-block-proofs.yaml"

# assert_tss_block_proofs pre-flights its target with assert_block_available_single,
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

# record_contains <name> <substring> <output>: on mismatch reports the last output line,
# which is the summary assert_tss_block_proofs ends with.
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

# under <seconds> <ceiling>: an "elapsed" value record can compare for equality.
function under {
    if [[ "$1" -lt "$2" ]]; then echo "under $2s"; else echo "$1s"; fi
}

# ============================================================================
# assert_tss_block_proofs's `timeout` wrapper
# ============================================================================

# Any existing directory satisfies validate_proto_path; the fake monitors ignore it.
# max_block 1 plus 1s of grace gives a 3s ceiling.
# shellcheck disable=SC2034 # consumed by the sourced runner
TSS_BLOCK_PROOFS_GRACE_SECONDS=1

if [[ -z "${TIMEOUT_BIN}" ]]; then
    echo "assert_tss_block_proofs's timeout wrapper"
    skip "hung monitor / leaked descendant scenarios (no timeout or gtimeout on PATH)"
else
    echo "assert_tss_block_proofs with a hung monitor-block-proofs.sh"

    SCRIPT_DIR="${TEST_DIR}/fixtures/tss-block-proofs"
    # shellcheck disable=SC2034 # consumed by the sourced runner
    PROTO_PATH="${SCRIPT_DIR}"
    started=${SECONDS}
    status=0
    output=$(assert_tss_block_proofs block-node-1 1 2>/dev/null) || status=$?
    elapsed=$((SECONDS - started))

    record "fails instead of reporting a result" "rc=1" "rc=${status}"
    # The 3s ceiling plus process teardown; anything near the fixture's 300s sleep means
    # the wrapper did not fire.
    record "returns rather than hanging" "under 10s" "$(under "${elapsed}" 10)"
    record_contains "names the timeout rather than 'no conclusive result'" \
        "exceeded 3s and was killed by the wrapper" "${output}"

    echo ""
    echo "assert_tss_block_proofs with a monitor-block-proofs.sh that leaks a descendant"

    SCRIPT_DIR="${TEST_DIR}/fixtures/tss-block-proofs-leaked-descendant"
    # shellcheck disable=SC2034 # consumed by the sourced runner
    PROTO_PATH="${SCRIPT_DIR}"
    started=${SECONDS}
    status=0
    output=$(assert_tss_block_proofs block-node-1 1 2>/dev/null) || status=$?
    elapsed=$((SECONDS - started))

    record "fails instead of reporting a result" "rc=1" "rc=${status}"
    # The leaked helper sleeps 300s. Anything near that means the runner waited on the
    # descendant's copy of a pipe rather than on the child `timeout` actually killed.
    record "returns despite the leaked descendant" "under 10s" "$(under "${elapsed}" 10)"
    record_contains "names the timeout rather than 'no conclusive result'" \
        "exceeded 3s and was killed by the wrapper" "${output}"
fi

echo ""
echo "assert_tss_block_proofs with a monitor-block-proofs.sh that fails immediately"

SCRIPT_DIR="${TEST_DIR}/fixtures/tss-block-proofs-failure"
# shellcheck disable=SC2034 # consumed by the sourced runner
PROTO_PATH="${SCRIPT_DIR}"
status=0
output=$(assert_tss_block_proofs block-node-1 1 2>/dev/null) || status=$?

record "fails instead of reporting a result" "rc=1" "rc=${status}"
record_contains "reports 'no conclusive result' with the exit status, not a timeout" \
    "no conclusive TSS block proof result within 1 blocks (exit 1)" "${output}"

echo ""
echo "assert_tss_block_proofs with a monitor-block-proofs.sh that found WRAPS imprecisely"

SCRIPT_DIR="${TEST_DIR}/fixtures/tss-block-proofs-wraps-imprecise"
# shellcheck disable=SC2034 # consumed by the sourced runner
PROTO_PATH="${SCRIPT_DIR}"
status=0
output=$(assert_tss_block_proofs block-node-1 1 2>/dev/null) || status=$?

# Observing WRAPS satisfies the assertion; the deadline only cost the exact block number.
record "treats an unnarrowed transition as a pass" "rc=0" "rc=${status}"
record_contains "reports the block WRAPS was seen at, not the caveat line" \
    "Schnorr then WRAPS at block 1" "${output}"

# ============================================================================
# The real monitor-block-proofs.sh against fake grpcurl fixtures
# ============================================================================
SCRIPT_DIR="${TEST_DIR}/.."
# shellcheck disable=SC2034 # consumed by the sourced runner
TSS_BLOCK_PROOFS_GRACE_SECONDS=30

# A fixture missing from the checkout otherwise surfaces only as "PROTO_PATH invalid".
for fixture in "${TEST_DIR}"/fixtures/monitor-*/grpcurl "${TEST_DIR}/fixtures/fake-block-json.sh"; do
    [[ -x "${fixture}" ]] || { echo "  FAIL  fixture missing or not executable: ${fixture}"; failed=$((failed + 1)); }
done

# run_monitor <fixture-dir-name> <max_block>: runs the assertion with the fixture's fake
# grpcurl first on PATH and leaves the result in status, output and elapsed.
function run_monitor {
    # shellcheck disable=SC2034 # consumed by the sourced runner
    PROTO_PATH="${TEST_DIR}/fixtures/$1"
    PATH="${PROTO_PATH}:${REAL_PATH}"
    local started=${SECONDS}
    status=0
    output=$(assert_tss_block_proofs block-node-1 "$2" 2>/dev/null) || status=$?
    elapsed=$((SECONDS - started))
    PATH="${REAL_PATH}"
}

# max_block 2 puts the monitor's deadline at 4s; the grace keeps the wrapper, when
# present, well clear of it so the monitor's own deadline is what fires. A stride of 1
# is needed to enter the scan loop at all with a max_block that small.
export MONITOR_BLOCK_STEP=1

echo ""
echo "monitor-block-proofs.sh hitting its own deadline with no signed block seen"

run_monitor monitor-hard-timeout 2

record "fails instead of reporting a result" "rc=1" "rc=${status}"
record "returns rather than waiting for blocks forever" "under 20s" "$(under "${elapsed}" 20)"
# Exit 3, not the 143 a bare SIGTERM death produces and not the wrapper's 124.
record_contains "names the monitor's own deadline" \
    "hit its own 4s deadline (max_block * 2)" "${output}"

# A stride of 10 with max_block 100 samples blocks 10, 20, 30 and so on, well inside the
# monitor's 200s deadline.
export MONITOR_BLOCK_STEP=10

echo ""
echo "monitor-block-proofs.sh on a pre-Nova chain (Schnorr below block 23, WRAPS from 23)"

run_monitor monitor-pre-nova-transition 100

record "passes on the Schnorr then WRAPS transition" "rc=0" "rc=${status}"
record_contains "narrows the transition to the exact block" \
    "Schnorr then WRAPS at block 23" "${output}"
record_contains "reports the pre-Nova era" "Era: pre-Nova (Schnorr then WRAPS)" "${output}"

echo ""
echo "monitor-block-proofs.sh on a Nova chain (block state proofs on multiples of 20)"

run_monitor monitor-nova 100

record "passes without waiting for a transition" "rc=0" "rc=${status}"
record_contains "reports the Nova layout on the minimum number of samples" \
    "Nova layout on 3 sampled blocks (first at block 10)" "${output}"
record_contains "skips the blocks without a TSS signature instead of counting them" \
    "Nova blocks sampled: 10 30 50" "${output}"

echo ""
echo "monitor-block-proofs.sh on a Nova chain that ends before enough samples"

# Samples 10 (Nova) and 20 (block state proof, skipped); 30 is past max_block 25.
run_monitor monitor-nova 25

record "fails instead of passing on a single sample" "rc=1" "rc=${status}"
record_contains "reports max-block reached without a conclusive result" \
    "no conclusive TSS block proof result within 25 blocks (exit 1)" "${output}"

echo ""
echo "monitor-block-proofs.sh on a chain mixing the Schnorr and Nova layouts"

run_monitor monitor-mixed-layouts 100

record "fails on the first Nova block after an older layout" "rc=1" "rc=${status}"
record_contains "names the mixed layout and the block" \
    "unexpected TSS block proof layout: block 30 carries the Nova layout (13712 bytes) after older layouts were seen" \
    "${output}"

echo ""
echo "monitor-block-proofs.sh on a chain with a signature of an unknown size"

run_monitor monitor-unknown-layout 100

record "fails on the first sampled block" "rc=1" "rc=${status}"
record_contains "names the unknown size and the block" \
    "unexpected TSS block proof layout: block 10 carries a 4000 byte TSS signature, which matches no known layout" \
    "${output}"

echo ""
echo "Passed: ${passed}, Failed: ${failed}, Skipped: ${skipped}"
[[ ${failed} -eq 0 ]]
