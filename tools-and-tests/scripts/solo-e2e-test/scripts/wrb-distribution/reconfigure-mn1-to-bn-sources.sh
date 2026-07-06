#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# WRB Distribution E2E (#3125 slice 4 — step 6) — reconfigure MN1's importer
# workload env to pull live blocks from BN2 and BN3 while keeping the
# recordstream bucket downloader on as a records backup.
#
# Mirrors the env-var recipe proven in wrb-sequential-comparison.sh
# (HIERO_MIRROR_IMPORTER_BLOCK_*), applied post-hoc via `kubectl set env`.
# After the env patch, the importer pod is rolled and we wait for Ready.
#
# The importer's workload name isn't stable across Solo/Hedera Mirror Node
# chart versions (sometimes Deployment/mirror-1-importer, sometimes a chart-
# generated name), so we look it up by label rather than hardcoding.
#
# Reads:
#   NAMESPACE         (default "solo-network")
#   CLUSTER_REFERENCE (default "kind-solo-cluster")
#   BN_HOST_2         (default block-node-2.${NAMESPACE}.svc.cluster.local)
#   BN_HOST_3         (default block-node-3.${NAMESPACE}.svc.cluster.local)
#   MN_INSTANCE       (default "mirror-1")

set -euo pipefail

: "${NAMESPACE:=solo-network}"
: "${CLUSTER_REFERENCE:=kind-solo-cluster}"
: "${BN_HOST_2:=block-node-2.${NAMESPACE}.svc.cluster.local}"
: "${BN_HOST_3:=block-node-3.${NAMESPACE}.svc.cluster.local}"
: "${MN_INSTANCE:=mirror-1}"
READY_TIMEOUT="${READY_TIMEOUT:-300}"

log() { echo "[wrb-dist-mn1-reconfig] $*"; }
fail() { echo "[wrb-dist-mn1-reconfig] ERROR: $*" >&2; exit 1; }

deploy_name=$(kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" \
    get deployment \
    -l "app.kubernetes.io/instance=${MN_INSTANCE},app.kubernetes.io/component=importer" \
    -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)

if [[ -n "${deploy_name}" ]]; then
    target_kind="deployment"
    target_name="${deploy_name}"
else
    # Fallback: some chart versions ship the importer as a StatefulSet.
    ss_name=$(kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" \
        get statefulset \
        -l "app.kubernetes.io/instance=${MN_INSTANCE},app.kubernetes.io/component=importer" \
        -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
    if [[ -z "${ss_name}" ]]; then
        log "No workload matched labels app.kubernetes.io/instance=${MN_INSTANCE},app.kubernetes.io/component=importer."
        log "Full ${MN_INSTANCE} inventory:"
        kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" \
            get all -l "app.kubernetes.io/instance=${MN_INSTANCE}" -o wide 2>&1 | tail -40 || true
        fail "no importer Deployment or StatefulSet found for instance=${MN_INSTANCE}"
    fi
    target_kind="statefulset"
    target_name="${ss_name}"
fi

log "Patching ${target_kind}/${target_name} to add BN2 + BN3 as block-node sources..."
log "  Bucket downloader stays on (records backup, per issue text)."

kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" \
    set env "${target_kind}/${target_name}" \
    HIERO_MIRROR_IMPORTER_BLOCK_ENABLED=true \
    HIERO_MIRROR_IMPORTER_BLOCK_SOURCETYPE=BLOCK_NODE \
    HIERO_MIRROR_IMPORTER_BLOCK_NODES_0_HOST="${BN_HOST_2}" \
    HIERO_MIRROR_IMPORTER_BLOCK_NODES_0_PORT=40840 \
    HIERO_MIRROR_IMPORTER_BLOCK_NODES_1_HOST="${BN_HOST_3}" \
    HIERO_MIRROR_IMPORTER_BLOCK_NODES_1_PORT=40840 \
    HIERO_MIRROR_IMPORTER_BLOCK_VERIFICATION_ENABLED=false \
    || fail "kubectl set env failed for ${target_kind}/${target_name}"

log "Waiting for ${target_kind}/${target_name} rollout (timeout ${READY_TIMEOUT}s)..."
kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" \
    rollout status "${target_kind}/${target_name}" \
    --timeout="${READY_TIMEOUT}s" \
    || {
        kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" \
            describe "${target_kind}/${target_name}" | tail -40 || true
        fail "${target_kind}/${target_name} rollout did not complete"
    }

log "${target_kind}/${target_name} is running with BN2 + BN3 as block-node sources."
