#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Regression check for execute_load_start/execute_load_stop's NLG_TEST_TYPE
# wiring in solo-test-runner.sh.
#
# A test definition's `load-start`/`load-stop` events accept a `test_class`
# arg (e.g. HCSLoadTest); both execute_load_start and execute_load_stop read
# it, defaulted it, and even echoed it in their log lines — but neither
# exported NLG_TEST_TYPE before invoking solo-load-generate.sh, which selects
# its class from `${NLG_TEST_TYPE:-CryptoTransferLoadTest}`. So test_class was
# silently discarded on both ends: every in-test load-start always ran
# CryptoTransferLoadTest regardless of what a test YAML asked for (no matter
# what workflow_dispatch's nlg-test-type input was set to — that input only
# reaches the separate, standalone pre-test NLG step, not this one), and even
# after fixing load-start alone, load-stop would still target the wrong class
# and fail to actually stop whatever non-default load was started.
#
# Runs without a cluster: extracts each function via sed (same approach as
# test-topology-decisions.sh) rather than sourcing the whole runner script,
# since the runner has top-level arg parsing that exits under `set -u` with no
# --test flag. Mocks the invoked solo-load-generate.sh with a stub that
# captures the environment it was actually given.
#
# Exit 0 on all-pass, 1 on any failure.

set -u -o pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUNNER_SCRIPT="${SCRIPT_DIR}/../solo-test-runner.sh"

passed=0
failed=0
function pass { echo "  PASS  $1"; passed=$((passed+1)); }
function fail { echo "  FAIL  $1"; failed=$((failed+1)); }

# Reuse the real functions rather than copies, so this test tracks the
# implementation instead of drifting from it.
eval "$(sed -n '/^function execute_load_start/,/^}/p' "${RUNNER_SCRIPT}")"
eval "$(sed -n '/^function execute_load_stop/,/^}/p' "${RUNNER_SCRIPT}")"

# execute_load_stop now shells out to `kctl logs` to capture the NLG pod's
# stdout before tearing it down; mock it the same way test-chaos-assertions.sh
# does. The nlg-logs/ dir it creates as a CWD-relative side effect (mirrors
# bn-logs/mn-logs in the CI workflow) is cleaned up by the trap below.
function kctl { return 0; }

if ! declare -f execute_load_start >/dev/null; then
    echo "FATAL: could not extract execute_load_start from ${RUNNER_SCRIPT}"
    exit 1
fi
if ! declare -f execute_load_stop >/dev/null; then
    echo "FATAL: could not extract execute_load_stop from ${RUNNER_SCRIPT}"
    exit 1
fi

# execute_load_start backgrounds "${SCRIPT_DIR}/solo-load-generate.sh" and
# sleeps 5s afterward. Mock both: point SCRIPT_DIR at a stub that dumps the
# environment it received, and no-op the sleep so the test runs instantly.
mock_dir="$(mktemp -d)"
trap 'rm -rf "${mock_dir}" nlg-logs' EXIT
capture_file="${mock_dir}/captured-env"
cat > "${mock_dir}/solo-load-generate.sh" <<'EOF'
#!/usr/bin/env bash
{
  echo "NLG_TEST_TYPE=${NLG_TEST_TYPE:-<unset>}"
  echo "NLG_ARGS=${NLG_ARGS:-<unset>}"
  echo "NLG_MAX_TPS=${NLG_MAX_TPS:-<unset>}"
} > "${CAPTURE_FILE}"
EOF
chmod +x "${mock_dir}/solo-load-generate.sh"
function sleep { :; }
export CAPTURE_FILE="${capture_file}" DEPLOYMENT="deployment-solo" NAMESPACE="solo-network"

# ----------------------------------------------------------------------------
echo "[1] execute_load_start exports NLG_TEST_TYPE from its test_class arg"
for tc in \
    "HCSLoadTest 5 10 60 '' '' HCSLoadTest" \
    "'' 5 10 60 '' '' CryptoTransferLoadTest" \
  ; do
    # shellcheck disable=SC2086
    eval "set -- $tc"
    test_class=$1; concurrency=$2; accounts=$3; duration=$4; max_tps=$5; extra_args=$6; expected=$7
    unset NLG_TEST_TYPE NLG_ARGS NLG_MAX_TPS
    SCRIPT_DIR="${mock_dir}" execute_load_start "$test_class" "$concurrency" "$accounts" "$duration" "$max_tps" "$extra_args" >/dev/null
    wait
    got="$(grep '^NLG_TEST_TYPE=' "${capture_file}" | cut -d= -f2)"
    label="test_class='${test_class:-<default>}'"
    if [[ "${got}" == "${expected}" ]]; then
        pass "${label} -> NLG_TEST_TYPE=${expected}"
    else
        fail "${label} -> expected NLG_TEST_TYPE=${expected}, got '${got}'"
    fi
    rm -f "${capture_file}"
done

# ----------------------------------------------------------------------------
echo "[2] execute_load_start still builds NLG_ARGS and NLG_MAX_TPS correctly"
unset NLG_TEST_TYPE NLG_ARGS NLG_MAX_TPS
SCRIPT_DIR="${mock_dir}" execute_load_start "HCSLoadTest" 7 20 90 250 "--extra flag" >/dev/null
wait
args_got="$(grep '^NLG_ARGS=' "${capture_file}" | cut -d= -f2-)"
maxtps_got="$(grep '^NLG_MAX_TPS=' "${capture_file}" | cut -d= -f2-)"
if [[ "${args_got}" == "-c 7 -a 20 -tt 90 --extra flag" ]]; then
    pass "NLG_ARGS assembled from concurrency/accounts/duration/extra_args"
else
    fail "NLG_ARGS expected '-c 7 -a 20 -tt 90 --extra flag', got '${args_got}'"
fi
if [[ "${maxtps_got}" == "250" ]]; then
    pass "NLG_MAX_TPS exported when max_tps provided"
else
    fail "NLG_MAX_TPS expected '250', got '${maxtps_got}'"
fi
rm -f "${capture_file}"

# ----------------------------------------------------------------------------
echo "[3] execute_load_stop exports NLG_TEST_TYPE from its test_class arg"
for tc in \
    "HCSLoadTest HCSLoadTest" \
    "'' CryptoTransferLoadTest" \
  ; do
    # shellcheck disable=SC2086
    eval "set -- $tc"
    test_class=$1; expected=$2
    unset NLG_TEST_TYPE NLG_ARGS NLG_MAX_TPS
    SCRIPT_DIR="${mock_dir}" execute_load_stop "$test_class" >/dev/null
    got="$(grep '^NLG_TEST_TYPE=' "${capture_file}" | cut -d= -f2)"
    label="test_class='${test_class:-<default>}'"
    if [[ "${got}" == "${expected}" ]]; then
        pass "${label} -> NLG_TEST_TYPE=${expected}"
    else
        fail "${label} -> expected NLG_TEST_TYPE=${expected}, got '${got}'"
    fi
    rm -f "${capture_file}"
done

echo
echo "RESULT: ${passed} passed, ${failed} failed"
[[ $failed -eq 0 ]]
