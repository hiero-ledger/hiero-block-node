#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Fixture-based unit tests for wait-for-bn-artifacts.sh.
#
# Runs without network: overrides probe_chart_values, probe_maven_jar and sleep with stubs
# driven by the variables below.
# Exit 0 on all-pass, 1 on any failure.

set -u -o pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# shellcheck disable=SC1091
source "${SCRIPT_DIR}/../wait-for-bn-artifacts.sh"

# The stubs run inside command substitutions, so they record into files, not variables.
STATE_DIR=$(mktemp -d)
trap 'rm -rf "${STATE_DIR}"' EXIT

CHART_PRESENT=true
PLUGIN_NAMES="backfill,block-access-service"
MISSING_JARS=""       # space-separated jar names the Maven probe reports missing
MISSING_ONCE=false    # when true, MISSING_JARS are missing only on the first chart probe

function probe_chart_values {
  echo "$1" >> "${STATE_DIR}/chart-probes"
  [[ "${CHART_PRESENT}" == true ]] || return 1
  printf 'plugins:\n'
  # An empty PLUGIN_NAMES stands for a chart whose names entry the sed pattern cannot read.
  [[ -z "${PLUGIN_NAMES}" ]] || printf '  names: "%s"\n' "${PLUGIN_NAMES}"
  printf '  mavenImage: "maven"\n'
}

function probe_maven_jar {
  echo "$1:$2" >> "${STATE_DIR}/jar-probes"
  local chart_probes
  chart_probes=$(wc -l < "${STATE_DIR}/chart-probes")
  if [[ " ${MISSING_JARS} " == *" $1 "* ]] && [[ "${MISSING_ONCE}" != true || ${chart_probes} -le 1 ]]; then
    return 1
  fi
  return 0
}

function sleep { :; }

function reset_stubs {
  CHART_PRESENT=true
  PLUGIN_NAMES="backfill,block-access-service"
  MISSING_JARS=""
  MISSING_ONCE=false
  : > "${STATE_DIR}/chart-probes"
  : > "${STATE_DIR}/jar-probes"
}

passed=0
failed=0
function pass { echo "  PASS  $1"; passed=$((passed+1)); }
function fail { echo "  FAIL  $1"; failed=$((failed+1)); }

# expect <name> <expected-rc> <actual-rc>
function expect_rc {
    if [[ "$3" == "$2" ]]; then pass "$1"; else fail "$1 (expected rc=$2, got rc=$3)"; fi
}

# expect_contains <name> <haystack> <needle>
function expect_contains {
    if [[ "$2" == *"$3"* ]]; then pass "$1"; else fail "$1 (missing '$3' in: $2)"; fi
}

echo "[1] SNAPSHOT versions are skipped without probing"
reset_stubs
rc=0
wait_for_bn_artifacts 0.43.0-SNAPSHOT 0 0 2> /dev/null || rc=1
expect_rc "SNAPSHOT returns 0" 0 "${rc}"
expect_rc "SNAPSHOT makes no probe calls" 0 "$(cat "${STATE_DIR}/chart-probes" "${STATE_DIR}/jar-probes" | wc -l)"

echo "[2] Chart and jars present"
reset_stubs
rc=0
wait_for_bn_artifacts v0.41.0-rc1 0 0 2> /dev/null || rc=1
expect_rc "all present returns 0" 0 "${rc}"
expect_contains "leading v is stripped" "$(cat "${STATE_DIR}/chart-probes")" "0.41.0-rc1"
if grep -q '^v' "${STATE_DIR}/chart-probes"; then fail "chart probed with leading v"; fi
expect_contains "every default plugin is probed" "$(cat "${STATE_DIR}/jar-probes")" "block-access-service:0.41.0-rc1"

echo "[3] Chart missing"
reset_stubs
CHART_PRESENT=false
rc=0
out=$(wait_for_bn_artifacts v0.41.0-rc1 0 0 2> /dev/null) || rc=1
expect_rc "chart missing returns 1" 1 "${rc}"
expect_contains "chart missing says not published" "${out}" "not published"
expect_contains "chart missing names the chart" "${out}" "block-node-server:0.41.0-rc1"

echo "[4] Jar missing"
reset_stubs
MISSING_JARS="backfill"
rc=0
out=$(wait_for_bn_artifacts v0.41.0-rc1 0 0 2> /dev/null) || rc=1
expect_rc "jar missing returns 1" 1 "${rc}"
expect_contains "jar missing names the coordinate" "${out}" "org.hiero.block-node:backfill:0.41.0-rc1"

echo "[5] Jar missing on the first poll only"
reset_stubs
MISSING_JARS="backfill"
MISSING_ONCE=true
rc=0
wait_for_bn_artifacts v0.41.0-rc1 5 0 2> /dev/null || rc=1
expect_rc "retry succeeds" 0 "${rc}"
expect_rc "chart probed twice" 2 "$(wc -l < "${STATE_DIR}/chart-probes")"

echo "[6] Pinned name:version entries are not probed"
reset_stubs
PLUGIN_NAMES="backfill,foo:1.2.3"
rc=0
wait_for_bn_artifacts v0.41.0-rc1 0 0 2> /dev/null || rc=1
expect_rc "pinned entry returns 0" 0 "${rc}"
if grep -q '^foo' "${STATE_DIR}/jar-probes"; then fail "pinned entry was probed"; else pass "pinned entry not probed"; fi

echo "[7] Unreadable plugin list is treated as missing"
reset_stubs
PLUGIN_NAMES=""
rc=0
out=$(wait_for_bn_artifacts v0.41.0-rc1 0 0 2> /dev/null) || rc=1
expect_rc "unreadable names returns 1" 1 "${rc}"
expect_contains "unreadable names is reported" "${out}" "plugins.names in chart"
expect_rc "no jar is probed" 0 "$(wc -l < "${STATE_DIR}/jar-probes")"

echo
echo "RESULT: ${passed} passed, ${failed} failed"
[[ $failed -eq 0 ]]
