#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Fixture-based unit tests for assert_no_errors in solo-test-runner.sh, covering the
# max_verify_failed argument that flavor tests use to tolerate TSS warm-up failures.
#
# Runs without a cluster: overrides curl to serve a canned /metrics body.
# Exit 0 on all-pass, 1 on any failure.

set -u -o pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# The runner parses its options at source time, so hand it a real test file; main()
# itself stays unrun (it is guarded on BASH_SOURCE).
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/../solo-test-runner.sh" --test "${SCRIPT_DIR}/../../tests/bn-flavors.yaml"

METRICS=""
function curl { echo "${METRICS}"; }
function get_bn_metrics_port { echo "9999"; }

passed=0
failed=0

# check <name> <expected-rc> <max_verify_failed> <verify_failed> <verify_errors> <stream_errors>
function check {
    local name="$1" expected="$2" max="$3"
    METRICS="blocknode_verification_blocks_failed_total $4
blocknode_verification_blocks_error_total $5
blocknode_publisher_stream_errors_total $6"
    local actual=0
    assert_no_errors block-node-1 "${max}" > /dev/null || actual=1
    if [[ "${actual}" == "${expected}" ]]; then
        echo "  PASS  ${name}"
        passed=$((passed + 1))
    else
        echo "  FAIL  ${name} (expected rc=${expected}, got rc=${actual})"
        failed=$((failed + 1))
    fi
}

check "clean node passes strictly"              0 0   0.0 0.0 0.0
check "verify_failed fails strictly"            1 0   7.0 0.0 0.0
check "verify_failed within budget passes"      0 100 7.0 0.0 0.0
check "verify_failed at budget passes"          0 100 100.0 0.0 0.0
check "verify_failed over budget fails"         1 100 101.0 0.0 0.0
check "verify_errors still fail within budget"  1 100 7.0 1.0 0.0
check "stream_errors still fail within budget"  1 100 0.0 0.0 1.0
check "default arg is strict"                   1 ""  7.0 0.0 0.0

echo ""
echo "${passed} passed, ${failed} failed"
[[ ${failed} -eq 0 ]]
