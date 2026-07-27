#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# WRB Distribution E2E (#3125 slice 5 — step 10) — reconfigure an
# already-deployed Block Node to use another BN as a backfill source.
#
# BN2 and BN3 were installed post-hoc by add-bn.sh (slice 3), which does not
# configure `blockNode.backfill.*` — there was no peer to backfill from at
# that point. Solo has no `solo block node update`/`config` subcommand (only
# `add`), so there's no way to re-render the chart for an existing release
# through Solo. Reconfiguring in place means doing by hand what the Helm
# templates would otherwise render, following the same kubectl-patch
# philosophy as reconfigure-mn1-to-bn-sources.sh:
#
#   1. Create/update the "<bn>-block-node-sources" ConfigMap — this is what
#      charts/block-node-server/templates/configmap-block-node-sources.yaml
#      renders when `blockNode.backfill.sources` is set at install time.
#   2. Merge BACKFILL_BLOCK_NODE_SOURCES_PATH / BACKFILL_GREEDY into the
#      existing "<bn>-config" ConfigMap (already wired into the container via
#      `envFrom: configMapRef`, so no env-var patch needed on the container).
#   3. Patch the StatefulSet's pod template to add the "block-node-sources"
#      volume + volumeMount that
#      charts/block-node-server/templates/statefulset.yaml only renders when
#      `blockNode.backfill.sources` is set — this is the piece a ConfigMap
#      change alone can't add, since the mount doesn't exist yet.
#
# Step 3 changes the pod template itself, so the StatefulSet controller rolls
# a new revision on its own — no explicit `kubectl rollout restart` needed,
# same as any other spec.template change. Doing the ConfigMap merge (step 2)
# before the template patch (step 3) ensures the new pod starts with the
# updated env already in place.
#
# Usage:
#     reconfigure-bn-backfill.sh <target-bn-index>
#     reconfigure-bn-backfill.sh 2
#     reconfigure-bn-backfill.sh 3
#
# Reads:
#   NAMESPACE          (default "solo-network")
#   CLUSTER_REFERENCE  (default "kind-solo-cluster")
#   SOURCE_BN_INDEX    (default 1 — BN1 is the backfill source per the issue)
#   BACKFILL_GREEDY    (default "false")
#   READY_TIMEOUT      (default 300)

set -euo pipefail

: "${NAMESPACE:=solo-network}"
: "${CLUSTER_REFERENCE:=kind-solo-cluster}"
: "${SOURCE_BN_INDEX:=1}"
: "${BACKFILL_GREEDY:=false}"
READY_TIMEOUT="${READY_TIMEOUT:-300}"

target_index="${1:?reconfigure-bn-backfill.sh: target BN index required (2|3)}"

log() { echo "[wrb-dist-bn-backfill] $*"; }
fail() { echo "[wrb-dist-bn-backfill] ERROR: $*" >&2; exit 1; }

target_bn="block-node-${target_index}"
source_bn="block-node-${SOURCE_BN_INDEX}"
source_dns="${source_bn}.${NAMESPACE}.svc.cluster.local"
source_port=40840
backfill_path="/opt/hiero/block-node/backfill"
backfill_filename="block-node-sources.json"
sources_configmap="${target_bn}-block-node-sources"
config_configmap="${target_bn}-config"

log "Reconfiguring ${target_bn} to backfill from ${source_bn} (${source_dns}:${source_port})..."

# 1) Create/update the sources ConfigMap (mirrors
#    configmap-block-node-sources.yaml's rendered shape).
sources_file="${TMPDIR:-/tmp}/wrb-dist-${target_bn}-sources.json"
cat > "${sources_file}" <<EOF
{
  "nodes": [
    { "address": "${source_dns}", "port": ${source_port}, "priority": 1 }
  ]
}
EOF

kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" \
    create configmap "${sources_configmap}" \
    --from-file="${backfill_filename}=${sources_file}" \
    --dry-run=client -o yaml \
    | kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" apply -f - \
    || fail "failed to create/update ${sources_configmap}"
log "  ${sources_configmap} applied."

# 2) Merge the backfill env vars into the existing "-config" ConfigMap.
kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" \
    patch configmap "${config_configmap}" --type merge -p "$(cat <<EOF
{
  "data": {
    "BACKFILL_BLOCK_NODE_SOURCES_PATH": "${backfill_path}/${backfill_filename}",
    "BACKFILL_GREEDY": "${BACKFILL_GREEDY}"
  }
}
EOF
)" || fail "failed to patch ${config_configmap}"
log "  ${config_configmap} patched (BACKFILL_BLOCK_NODE_SOURCES_PATH, BACKFILL_GREEDY=${BACKFILL_GREEDY})."

# 3) Patch the StatefulSet's pod template to add the volume + volumeMount.
#    volumes[] and containers[].volumeMounts[] are both merged on their
#    `name` key by a strategic merge patch, so this appends rather than
#    clobbering the chart's existing volumes/mounts.
statefulset_patch="${TMPDIR:-/tmp}/wrb-dist-${target_bn}-sts-patch.yaml"
cat > "${statefulset_patch}" <<EOF
spec:
  template:
    spec:
      volumes:
        - name: block-node-sources
          configMap:
            name: ${sources_configmap}
      containers:
      - name: block-node-server
        volumeMounts:
        - name: block-node-sources
          mountPath: ${backfill_path}
          readOnly: true
EOF

kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" \
    patch statefulset "${target_bn}" \
    --patch-file "${statefulset_patch}" \
    || fail "kubectl patch failed for statefulset/${target_bn}"

log "Waiting for ${target_bn} rollout (timeout ${READY_TIMEOUT}s)..."
kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" \
    rollout status statefulset/"${target_bn}" --timeout="${READY_TIMEOUT}s" \
    || {
        kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" \
            describe pod/"${target_bn}-0" | tail -30 || true
        fail "${target_bn} rollout did not complete"
    }

log "${target_bn} is now backfilling from ${source_bn}."
