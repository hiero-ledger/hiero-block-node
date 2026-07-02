#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# WRB Distribution E2E (#3125 slice 3) — add-bn.sh
#
# Adds a single Block Node to an already-deployed Solo network POST-hoc, using
# the same `solo block node add` invocation shape as deploy_block_nodes in
# solo-deploy-network.sh. This lets the test bring up CN+MN (with MinIO) first,
# run the wrb-cli slice-1+2 flow, then layer BN1/BN2/BN3 in — matching the
# operational order of issue #3125 (BNs added after wrb-cli is producing).
#
# Usage:
#     add-bn.sh <bn-index>
#
# Reads (with the harness's own defaults as fallback):
#   DEPLOYMENT        (default "deployment-solo")
#   NAMESPACE         (default "solo-network")
#   CLUSTER_REFERENCE (default "kind-solo-cluster")
#   TOPOLOGY          (used to locate overrides/${TOPOLOGY}/bn-block-node-<i>-values.yaml)
#   BN_VERSION        (optional — passed through as --chart-version if set)
#
# Overlays applied (mirrors deploy_block_nodes for consistency):
#   * overrides/bn-memory.yaml                                — global BN memory sizing
#   * overrides/${TOPOLOGY}/bn-block-node-<i>-values.yaml     — this test's per-BN EMB
#
# Assertions on success:
#   * `solo block node add` exit code is 0
#   * The block-node-<i>-0 pod exists and is Ready within `BN_READY_TIMEOUT` seconds

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOLO_E2E_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

: "${DEPLOYMENT:=deployment-solo}"
: "${NAMESPACE:=solo-network}"
: "${CLUSTER_REFERENCE:=kind-solo-cluster}"
: "${TOPOLOGY:=wrb-distribution-step345}"
BN_READY_TIMEOUT="${BN_READY_TIMEOUT:-300}"

bn_index="${1:?add-bn.sh: BN index required (1|2|3)}"

log() { echo "[wrb-dist-add-bn] $*"; }
fail() { echo "[wrb-dist-add-bn] ERROR: $*" >&2; exit 1; }

bn_name="block-node-${bn_index}"
memory_overlay="${SOLO_E2E_ROOT}/overrides/bn-memory.yaml"
topology_overlay="${SOLO_E2E_ROOT}/overrides/${TOPOLOGY}/bn-block-node-${bn_index}-values.yaml"

overlay_args=""
if [[ -f "${memory_overlay}" ]]; then
    overlay_args="${overlay_args} -f ${memory_overlay}"
    log "  Applying BN memory override"
else
    log "  (skipping BN memory override — file not present)"
fi

if [[ -f "${topology_overlay}" ]]; then
    overlay_args="${overlay_args} -f ${topology_overlay}"
    log "  Applying topology overlay: overrides/${TOPOLOGY}/bn-block-node-${bn_index}-values.yaml"
else
    fail "Topology overlay not found: ${topology_overlay#${SOLO_E2E_ROOT}/}"
fi

chart_args=""
if [[ -n "${BN_VERSION:-}" ]]; then
    chart_args="--chart-version ${BN_VERSION}"
fi

log "Adding ${bn_name} to deployment=${DEPLOYMENT} cluster-ref=${CLUSTER_REFERENCE}..."
# shellcheck disable=SC2086
solo block node add \
    --deployment "${DEPLOYMENT}" \
    --cluster-ref "${CLUSTER_REFERENCE}" \
    ${chart_args} \
    ${overlay_args} \
    || fail "solo block node add failed for ${bn_name}"

log "Waiting for ${bn_name}-0 pod Ready (timeout ${BN_READY_TIMEOUT}s)..."
kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" \
    wait --for=condition=Ready pod/"${bn_name}-0" --timeout="${BN_READY_TIMEOUT}s" \
    || {
        kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" \
            describe pod/"${bn_name}-0" | tail -30 || true
        fail "${bn_name}-0 did not become Ready within ${BN_READY_TIMEOUT}s"
    }

log "${bn_name}-0 is Ready."
