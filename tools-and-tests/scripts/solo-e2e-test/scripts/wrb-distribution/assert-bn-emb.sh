#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# WRB Distribution E2E (#3125 slice 3) — assert each BN's earliestManagedBlock
# (EMB) matches the topology's declared value.
#
# The EMB is set at deploy time by the per-topology, per-BN static overlays
# under overrides/wrb-distribution-step345/. This script confirms each BN
# actually came up with that value by reading the effective env from the pod.
#
# Expected values (per issue #3125, updated in slice 6):
#   block-node-1  EMB=0  (Tier-0, live GRPC receiver post-cutover)
#   block-node-2  EMB=0  (Tier-1, backfills from BN1)
#   block-node-3  EMB=0  (Tier-1, backfills from BN1; changed from 100_000_000
#                          so BN3 can participate in the step-12 convergence check)
#
# The env var name on the pod is BLOCK_NODE_EARLIEST_MANAGED_BLOCK, matching
# the precedent from charts/block-node-server/values-overrides/lfh-values.yaml
# and rfh-values.yaml.

set -euo pipefail

: "${NAMESPACE:=solo-network}"
: "${CONTEXT:=kind-solo-cluster}"

log() { echo "[wrb-dist-step345-emb] $*"; }
fail() { echo "[wrb-dist-step345-emb] ERROR: $*" >&2; exit 1; }

# bn_name -> expected EMB. Update in lock-step with the topology's overlay files.
# A plain function (not `declare -A`) so this runs on macOS's stock /bin/bash
# 3.2 too, which has no associative arrays (added in bash 4.0).
expected_emb() {
    case "$1" in
        block-node-1) echo 0 ;;
        block-node-2) echo 0 ;;
        block-node-3) echo 0 ;;
        *) echo "" ;;
    esac
}
BN_NAMES=(block-node-1 block-node-2 block-node-3)

# Read the effective BLOCK_NODE_EARLIEST_MANAGED_BLOCK from the BN pod. Prefer
# the Downward API (env of the running container) so we see the effective value
# after helm merged all overlays, not just what a static file said.
read_bn_emb() {
    local bn_name="$1"
    local pod
    pod=$(kubectl --context "${CONTEXT}" --namespace "${NAMESPACE}" \
        get pod -l app.kubernetes.io/instance="${bn_name}" \
        -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")
    if [[ -z "${pod}" ]]; then
        # Fall back to pod name pattern used by the standard Helm chart.
        pod=$(kubectl --context "${CONTEXT}" --namespace "${NAMESPACE}" \
            get pod -l "app.kubernetes.io/name=block-node-server" \
            -o jsonpath="{.items[?(@.metadata.name==\"${bn_name}-0\")].metadata.name}" 2>/dev/null || echo "")
    fi
    if [[ -z "${pod}" ]]; then
        pod="${bn_name}-0"
    fi

    kubectl --context "${CONTEXT}" --namespace "${NAMESPACE}" \
        exec "${pod}" -c block-node-server -- \
        printenv BLOCK_NODE_EARLIEST_MANAGED_BLOCK 2>/dev/null | tr -d '[:space:]' || echo ""
}

failures=0
for bn in "${BN_NAMES[@]}"; do
    expected=$(expected_emb "${bn}")
    actual=$(read_bn_emb "${bn}")
    if [[ -z "${actual}" ]]; then
        log "${bn}: could not read BLOCK_NODE_EARLIEST_MANAGED_BLOCK from pod (expected ${expected})"
        failures=$(( failures + 1 ))
        continue
    fi
    if [[ "${actual}" == "${expected}" ]]; then
        log "${bn}: EMB=${actual} ✓"
    else
        log "${bn}: EMB=${actual} but expected ${expected}"
        failures=$(( failures + 1 ))
    fi
done

if (( failures > 0 )); then
    fail "${failures} BN(s) had an unexpected EMB — check the topology overlay files"
fi

log "All 3 BNs came up with the expected EMB values."
