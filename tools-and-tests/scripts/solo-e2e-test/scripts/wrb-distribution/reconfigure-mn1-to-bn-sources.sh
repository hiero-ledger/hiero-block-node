#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# WRB Distribution E2E (#3125 slice 4 — step 6) — reconfigure MN1's importer
# deployment env to pull live blocks from BN2 and BN3 while keeping the
# recordstream bucket downloader on as a records backup.
#
# Mirrors the env-var recipe proven in wrb-sequential-comparison.sh
# (HIERO_MIRROR_IMPORTER_BLOCK_*), applied post-hoc via `kubectl set env`.
# After the env patch, the importer pod is rolled and we wait for Ready.
#
# Reads:
#   NAMESPACE         (default "solo-network")
#   CLUSTER_REFERENCE (default "kind-solo-cluster")
#   BN_HOST_2         (default block-node-2.${NAMESPACE}.svc.cluster.local)
#   BN_HOST_3         (default block-node-3.${NAMESPACE}.svc.cluster.local)

set -euo pipefail

: "${NAMESPACE:=solo-network}"
: "${CLUSTER_REFERENCE:=kind-solo-cluster}"
: "${BN_HOST_2:=block-node-2.${NAMESPACE}.svc.cluster.local}"
: "${BN_HOST_3:=block-node-3.${NAMESPACE}.svc.cluster.local}"
MN_IMPORTER_DEPLOYMENT="${MN_IMPORTER_DEPLOYMENT:-mirror-1-importer}"
READY_TIMEOUT="${READY_TIMEOUT:-300}"

log() { echo "[wrb-dist-mn1-reconfig] $*"; }
fail() { echo "[wrb-dist-mn1-reconfig] ERROR: $*" >&2; exit 1; }

log "Patching ${MN_IMPORTER_DEPLOYMENT} to add BN2 + BN3 as block-node sources..."
log "  Bucket downloader stays on (records backup, per issue text)."

kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" \
    set env deployment/"${MN_IMPORTER_DEPLOYMENT}" \
    HIERO_MIRROR_IMPORTER_BLOCK_ENABLED=true \
    HIERO_MIRROR_IMPORTER_BLOCK_SOURCETYPE=BLOCK_NODE \
    HIERO_MIRROR_IMPORTER_BLOCK_NODES_0_HOST="${BN_HOST_2}" \
    HIERO_MIRROR_IMPORTER_BLOCK_NODES_0_PORT=40840 \
    HIERO_MIRROR_IMPORTER_BLOCK_NODES_1_HOST="${BN_HOST_3}" \
    HIERO_MIRROR_IMPORTER_BLOCK_NODES_1_PORT=40840 \
    HIERO_MIRROR_IMPORTER_BLOCK_VERIFICATION_ENABLED=false \
    || fail "kubectl set env failed for ${MN_IMPORTER_DEPLOYMENT}"

log "Waiting for ${MN_IMPORTER_DEPLOYMENT} rollout (timeout ${READY_TIMEOUT}s)..."
kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" \
    rollout status deployment/"${MN_IMPORTER_DEPLOYMENT}" \
    --timeout="${READY_TIMEOUT}s" \
    || {
        kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" \
            describe deployment/"${MN_IMPORTER_DEPLOYMENT}" | tail -40 || true
        fail "${MN_IMPORTER_DEPLOYMENT} rollout did not complete"
    }

log "${MN_IMPORTER_DEPLOYMENT} is running with BN2 + BN3 as block-node sources."
