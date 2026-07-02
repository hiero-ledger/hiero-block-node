#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# WRB Distribution E2E (#3125 slice 4) — assert that a Mirror Node's importer
# is consuming blocks from a Block Node source. Applies to:
#   * mirror-1 (step 6) after reconfigure-mn1-to-bn-sources.sh flips
#     HIERO_MIRROR_IMPORTER_BLOCK_ENABLED=true and points at BN2/BN3.
#   * mirror-2 (step 7) after add-mn2.sh installs a fresh MN with block:
#     enabled: true / sourceType: BLOCK_NODE / startBlockNumber: 0.
#
# Assertion signal: the importer container's logs must contain at least one
# BN-source-related line within POLL_WINDOW seconds. Recognized patterns
# (any one is sufficient):
#   * "block node" / "BLOCK_NODE"
#   * "SubscribeBlockStream" (gRPC method the importer calls on BN)
#   * "block-node-<N>" (target hostname prefix appearing in log context)
#
# Usage:
#     assert-mn-consuming-from-bn.sh <mirror-name>
#     assert-mn-consuming-from-bn.sh mirror-1
#     assert-mn-consuming-from-bn.sh mirror-2
#
# Reads:
#   NAMESPACE         (default "solo-network")
#   CLUSTER_REFERENCE (default "kind-solo-cluster")
#   POLL_WINDOW       (default 240 — seconds to wait for BN activity to appear)
#   POLL_INTERVAL     (default 10)

set -euo pipefail

: "${NAMESPACE:=solo-network}"
: "${CLUSTER_REFERENCE:=kind-solo-cluster}"
POLL_WINDOW="${POLL_WINDOW:-240}"
POLL_INTERVAL="${POLL_INTERVAL:-10}"

mirror_name="${1:?assert-mn-consuming-from-bn.sh: mirror name required (e.g. mirror-1, mirror-2)}"
importer_deployment="${mirror_name}-importer"

log() { echo "[wrb-dist-mn-bn-assert] $*"; }
fail() { echo "[wrb-dist-mn-bn-assert] ERROR: $*" >&2; exit 1; }

log "Waiting up to ${POLL_WINDOW}s for ${importer_deployment} to show BN-source activity..."

deadline=$(($(date +%s) + POLL_WINDOW))
while [[ $(date +%s) -lt ${deadline} ]]; do
    # Try to find the importer pod (name changes across rollouts).
    pod=$(kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" \
        get pods -l "app.kubernetes.io/name=importer,app.kubernetes.io/instance=${mirror_name}" \
        -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)

    if [[ -z "${pod}" ]]; then
        # Fallback selector: some Solo chart variants use different labels.
        pod=$(kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" \
            get pods 2>/dev/null | awk -v m="${importer_deployment}" '$1 ~ m {print $1; exit}' || true)
    fi

    if [[ -n "${pod}" ]]; then
        if kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" \
            logs "${pod}" --tail=2000 2>/dev/null \
            | grep -q -E "block[- ]?node|BLOCK_NODE|SubscribeBlockStream|block-node-[0-9]+"; then
            log "${importer_deployment} (pod ${pod}) log shows BN-source activity."
            log "Sample match:"
            kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" \
                logs "${pod}" --tail=2000 2>/dev/null \
                | grep -E "block[- ]?node|BLOCK_NODE|SubscribeBlockStream|block-node-[0-9]+" \
                | head -5 | sed 's/^/    /'
            exit 0
        fi
    fi

    sleep "${POLL_INTERVAL}"
done

log "No BN-source activity observed in ${importer_deployment} logs within ${POLL_WINDOW}s."
kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" \
    get pods -l "app.kubernetes.io/instance=${mirror_name}" -o wide 2>/dev/null || true
kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" \
    logs "${pod:-unknown}" --tail=100 2>/dev/null | tail -40 | sed 's/^/    /' || true
fail "${importer_deployment}: no BN-source activity detected"
