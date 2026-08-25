#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Regression check for topology parsing in solo-deploy-network.sh.
#
# The relay/explorer/mirror deploy decision used `yq '.<section> | keys | length // 0'`,
# which *errors* on a null section — a `relay_nodes:` key whose only child is commented
# out, or a missing key. stderr was discarded and there was no fallback, so the count came
# back as the empty string, both branches of the comparison were false, and the component
# was deployed anyway. Six of nine matrix jobs died on the resulting relay.
#
# This asserts the decision is reachable for every bundled topology without a yq error,
# so the failure mode cannot come back silently. Runs without a cluster.
#
# Exit 0 on all-pass, 1 on any failure.

set -u -o pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TOPOLOGIES_DIR="${SCRIPT_DIR}/../../topologies"
DEPLOY_SCRIPT="${SCRIPT_DIR}/../solo-deploy-network.sh"

passed=0
failed=0
function pass { echo "  PASS  $1"; passed=$((passed+1)); }
function fail { echo "  FAIL  $1"; failed=$((failed+1)); }

# Reuse the real decision function rather than a copy, so this test tracks the
# implementation instead of drifting from it.
eval "$(sed -n '/^function skip_component/,/^}/p' "${DEPLOY_SCRIPT}")"

if ! declare -f skip_component >/dev/null; then
    echo "FATAL: could not extract skip_component from ${DEPLOY_SCRIPT}"
    exit 1
fi

echo "[1] skip_component resolves to true/false for every topology and section"
for topology_file in "${TOPOLOGIES_DIR}"/*.yaml; do
    name=$(basename "${topology_file}")
    for section_pair in "mirror_nodes:mirror_node" "relay_nodes:relay" "explorer_nodes:explorer"; do
        section="${section_pair%%:*}"
        legacy_key="${section_pair##*:}"

        # Capture stderr too: a yq error here is the exact regression being guarded.
        decision=$(skip_component "${topology_file}" "${section}" "${legacy_key}" 2>&1)

        if [[ "${decision}" == "true" || "${decision}" == "false" ]]; then
            pass "${name} ${section} -> skip=${decision}"
        else
            fail "${name} ${section} -> expected true/false, got '${decision}'"
        fi
    done
done

echo "[2] the null-section shapes that caused the regression all resolve to skip"
# yq errors on `keys` for a null section; `length` returns 0. Cover every shape the
# schema allows: absent, present-but-null, present-but-empty-map, and populated.
fixture_dir=$(mktemp -d)
trap 'rm -rf "${fixture_dir}"' EXIT

printf 'name: absent\n' > "${fixture_dir}/absent.yaml"
printf 'name: null-section\nrelay_nodes:\n  # relay-1: {}\n' > "${fixture_dir}/null-section.yaml"
printf 'name: empty-map\nrelay_nodes: {}\n' > "${fixture_dir}/empty-map.yaml"
printf 'name: populated\nrelay_nodes:\n  relay-1: {}\n' > "${fixture_dir}/populated.yaml"

for expectation in "absent:true" "null-section:true" "empty-map:true" "populated:false"; do
    shape="${expectation%%:*}"
    want="${expectation##*:}"
    got=$(skip_component "${fixture_dir}/${shape}.yaml" "relay_nodes" "relay" 2>&1)
    if [[ "${got}" == "${want}" ]]; then
        pass "relay_nodes ${shape} -> skip=${want}"
    else
        fail "relay_nodes ${shape} -> expected skip=${want}, got '${got}'"
    fi
done

echo "[3] legacy components.<key>=true still forces a component on"
printf 'name: legacy\ncomponents:\n  relay: true\n' > "${fixture_dir}/legacy.yaml"
got=$(skip_component "${fixture_dir}/legacy.yaml" "relay_nodes" "relay" 2>&1)
if [[ "${got}" == "false" ]]; then
    pass "components.relay=true -> skip=false"
else
    fail "components.relay=true -> expected skip=false, got '${got}'"
fi

echo "[4] verification_mode resolves to a known value for every topology"
# A typo (rsa_wrb, rsa-WRB) silently falls back to tss, which is how
# wrb-differential-test ended up verifying every block against the wrong scheme.
for topology_file in "${TOPOLOGIES_DIR}"/*.yaml; do
    name=$(basename "${topology_file}")
    mode=$(yq '.verification_mode // "tss"' "${topology_file}" 2>&1)
    case "${mode}" in
        tss|rsa-wrb) pass "${name} verification_mode=${mode}" ;;
        *)           fail "${name} verification_mode='${mode}' is not tss or rsa-wrb" ;;
    esac
done

echo
echo "RESULT: ${passed} passed, ${failed} failed"
[[ $failed -eq 0 ]]
