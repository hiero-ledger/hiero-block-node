#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Checks the Nova WRAPS TSS library decisions: cn_is_nova_wraps and cn_needs_wraps_proving_key
# (lib/cn-application-properties.sh), and the compatibility tier and CN floor that
# resolve-versions.sh derives from them.
#
# The Consensus Node carries the Nova WRAPS library from release 0.79.0, while the CN 0.79.0 and
# 0.80.0 pre-releases published so far are on the earlier library although 0.80.0-alpha.N sorts
# above 0.79.0. A plain version comparison would treat those pre-releases as Nova WRAPS, skip
# the WRAPS proving key and ask for a Mirror Node that does not exist yet. Runs without a cluster.
#
# Exit 0 on all-pass, 1 on any failure.

set -u -o pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LIB="${SCRIPT_DIR}/../lib/cn-application-properties.sh"
RESOLVE_SCRIPT="${SCRIPT_DIR}/../resolve-versions.sh"

passed=0
failed=0
function pass { echo "  PASS  $1"; passed=$((passed+1)); }
function fail { echo "  FAIL  $1"; failed=$((failed+1)); }

unset CN_NOVA_WRAPS CN_NOVA_WRAPS_MIN_VERSION
# shellcheck source=../lib/cn-application-properties.sh
source "${LIB}"

# Reuse the real resolve-versions.sh functions and constants rather than copies, so this test
# tracks the implementation. The script runs main when executed, so it is not sourced.
constants=$(sed -n -E '/^readonly (NOVA_WRAPS_MN_MIN_VERSION|COMPAT_CN_MINS|COMPAT_MN_MINS|COMPAT_BN_MINS|NOVA_WRAPS_BN_MIN_VERSION)=/p' "${RESOLVE_SCRIPT}")
eval "${constants}"
for fn in log_line enforce_minimum_version enforce_nova_wraps_cn_floor resolve_compatibility_mins; do
    pattern="/^function ${fn} [{]/,/^[}]/p"
    definition=$(sed -n "${pattern}" "${RESOLVE_SCRIPT}")
    eval "${definition}"
    if ! declare -f "${fn}" >/dev/null; then
        echo "FATAL: could not extract ${fn} from ${RESOLVE_SCRIPT}"
        exit 1
    fi
done

# expect_nova <version> <true|false> [label]
function expect_nova {
    local version="$1" expected="$2" actual
    if cn_is_nova_wraps "${version}"; then actual="true"; else actual="false"; fi
    if [[ "${actual}" == "${expected}" ]]; then
        pass "cn_is_nova_wraps '${version}' is ${expected}${3:+ (${3})}"
    else
        fail "cn_is_nova_wraps '${version}' expected ${expected}, got ${actual}${3:+ (${3})}"
    fi
}

# expect_equal <label> <expected> <actual>
function expect_equal {
    if [[ "$2" == "$3" ]]; then
        pass "$1: $3"
    else
        fail "$1: expected '$2', got '$3'"
    fi
}

echo "[1] default minimum ${CN_NOVA_WRAPS_MIN_VERSION}: releases from it are Nova WRAPS, pre-releases are not"
expect_nova "0.78.1" false
expect_nova "0.79.0" true
expect_nova "v0.79.0" true
expect_nova "0.79.1" true
expect_nova "0.80.0" true
expect_nova "0.79.0-alpha.2" false "published pre-release on the earlier library"
expect_nova "0.80.0-alpha.3" false "published pre-release on the earlier library, sorts above 0.79.0"
expect_nova "0.79.0-rc.1" false
expect_nova "0.79.0-SNAPSHOT" false "a branch build needs CN_NOVA_WRAPS"
expect_nova "rc" false "a keyword is not a version"
expect_nova "" false

echo "[2] CN_NOVA_WRAPS overrides the version"
CN_NOVA_WRAPS=true expect_nova "0.78.1" true
CN_NOVA_WRAPS=true expect_nova "0.79.0-SNAPSHOT" true
CN_NOVA_WRAPS=false expect_nova "0.80.0" false

echo "[3] a minimum lowered to a pre-release counts the versions from it, pre-releases included"
CN_NOVA_WRAPS_MIN_VERSION=0.79.0-rc.3 expect_nova "0.79.0-rc.2" false
CN_NOVA_WRAPS_MIN_VERSION=0.79.0-rc.3 expect_nova "0.79.0-rc.3" true
CN_NOVA_WRAPS_MIN_VERSION=0.79.0-rc.3 expect_nova "0.79.0" true

echo "[4] cn_needs_wraps_proving_key is the opposite of cn_is_nova_wraps"
for version in "0.78.1" "0.79.0" "0.80.0-alpha.3"; do
    if cn_is_nova_wraps "${version}"; then nova="true"; else nova="false"; fi
    if cn_needs_wraps_proving_key "${version}"; then key="true"; else key="false"; fi
    if [[ "${nova}" != "${key}" ]]; then
        pass "'${version}': nova=${nova}, needs key=${key}"
    else
        fail "'${version}': nova=${nova} and needs key=${key} must differ"
    fi
done

echo "[5] resolve_compatibility_mins picks the Nova WRAPS tier only for a Nova WRAPS CN"
expect_equal "CN 0.79.0" "mn_min=0.166.0 bn_min=0.45.0" "$(resolve_compatibility_mins 0.79.0 2>/dev/null | tr '\n' ' ' | sed 's/ $//')"
expect_equal "CN 0.80.0-alpha.3" "mn_min=0.154.0 bn_min=0.35.0" "$(resolve_compatibility_mins 0.80.0-alpha.3 2>/dev/null | tr '\n' ' ' | sed 's/ $//')"
expect_equal "CN 0.77.5" "mn_min=0.154.0 bn_min=0.35.0" "$(resolve_compatibility_mins 0.77.5 2>/dev/null | tr '\n' ' ' | sed 's/ $//')"
expect_equal "CN 0.79.0-SNAPSHOT" "mn_min= bn_min=" "$(resolve_compatibility_mins 0.79.0-SNAPSHOT 2>/dev/null | tr '\n' ' ' | sed 's/ $//')"

echo "[6] enforce_nova_wraps_cn_floor raises the CN only for a Nova WRAPS Block Node release"
expect_equal "BN 0.45.0, CN 0.78.1" "0.79.0" "$(enforce_nova_wraps_cn_floor 0.78.1 0.45.0 2>/dev/null)"
expect_equal "BN 0.45.0, CN 0.79.0" "0.79.0" "$(enforce_nova_wraps_cn_floor 0.79.0 0.45.0 2>/dev/null)"
expect_equal "BN 0.45.0, CN 0.80.0-alpha.3 (kept, with a warning)" "0.80.0-alpha.3" "$(enforce_nova_wraps_cn_floor 0.80.0-alpha.3 0.45.0 2>/dev/null)"
expect_equal "BN 0.45.0-rc2, CN 0.78.1" "0.78.1" "$(enforce_nova_wraps_cn_floor 0.78.1 0.45.0-rc2 2>/dev/null)"
expect_equal "BN 0.44.2, CN 0.78.1" "0.78.1" "$(enforce_nova_wraps_cn_floor 0.78.1 0.44.2 2>/dev/null)"
expect_equal "BN 0.45.0-SNAPSHOT, CN 0.78.1" "0.78.1" "$(enforce_nova_wraps_cn_floor 0.78.1 0.45.0-SNAPSHOT 2>/dev/null)"

echo ""
echo "${passed} passed, ${failed} failed"
[[ ${failed} -eq 0 ]]
