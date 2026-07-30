#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# WRB Distribution E2E (#3125 slice 6 — step 11) — reconfigure BN1 to pull
# TssData from BN2/BN3 via the roster-bootstrap-tss plugin.
#
# Why: TssData is only ever extracted while verifying block number 0 (see
# block-verification's BlockHasher). BN1's copy of block 0 arrived via
# bulk-load-historical-to-bn1.sh's direct-to-storage copy (picked up by
# BlockFileHistoricPlugin's startup scan), which never runs the verification
# pipeline — so BN1 can never extract TssData itself. BN2 and BN3 instead
# backfill block 0 from BN1 (reconfigure-bn-backfill.sh, step 10), and
# backfilled blocks DO go through verification, so each independently
# extracts its own TssData. The roster-bootstrap-tss plugin closes the loop:
# it periodically calls serverStatusDetail on configured peers and copies
# whatever TssData they already have, so pointing BN1 at BN2/BN3 lets BN1
# eventually reflect the same TssData too — matching issue #3125 step 11's
# literal expectation that BN1 gets updated (just via peer gossip rather than
# its own block-0 extraction).
#
# roster-bootstrap-tss is in the chart's DEFAULT plugins.names list
# (charts/block-node-server/values.yaml), so every BN in this suite already
# has its JAR loaded from install time — it's simply inactive because
# ROSTER_BOOTSTRAP_TSS_BLOCK_NODE_SOURCES_PATH defaults to "". Solo has no
# `solo block node update`/`config` subcommand (only `add`), so — same
# kubectl-patch philosophy as reconfigure-bn-backfill.sh — this reconfigures
# the already-running BN1 by hand instead of re-rendering the chart:
#
#   1. Create/update the "<bn>-roster-bootstrap-tss-sources" ConfigMap — the
#      same BlockNodeSource JSON shape backfill's sources ConfigMap uses
#      (protobuf-sources/.../internal/block_node_source.proto), just under a
#      different name since this isn't rendered by any existing chart
#      template (unlike backfill's, roster-bootstrap-tss has no dedicated
#      values-driven ConfigMap/volume block in the chart at all yet).
#   2. Merge ROSTER_BOOTSTRAP_TSS_BLOCK_NODE_SOURCES_PATH into the existing
#      "<bn>-config" ConfigMap (already wired into the container via
#      envFrom: configMapRef, so no env-var patch needed on the container).
#   3. Patch the StatefulSet's pod template to add the
#      "roster-bootstrap-tss-sources" volume + volumeMount — the piece a
#      ConfigMap change alone can't add, since the mount doesn't exist yet.
#
# Step 3 changes the pod template itself, so the StatefulSet controller rolls
# a new revision on its own (no explicit `kubectl rollout restart` needed) —
# same as reconfigure-bn-backfill.sh's BN2/BN3 rollout, which is why the test
# definition refreshes port-forwards after this event too (see
# wrb-distribution-steps1-12.yaml's refresh-port-forwards-after-backfill-rollout
# comment for why that's needed at all).
#
# Usage:
#     reconfigure-bn-roster-bootstrap-tss.sh <target-bn-index> <peer-bn-index> [<peer-bn-index> ...]
#     reconfigure-bn-roster-bootstrap-tss.sh 1 2 3
#
# Reads:
#   NAMESPACE          (default "solo-network")
#   CLUSTER_REFERENCE  (default "kind-solo-cluster")
#   READY_TIMEOUT      (default 300)
#
# Each peer BN's gRPC port is read from its own "-config" ConfigMap
# (SERVER_PORT), falling back to 40840 only if that key is absent — same
# derivation as reconfigure-bn-backfill.sh.

set -euo pipefail

: "${NAMESPACE:=solo-network}"
: "${CLUSTER_REFERENCE:=kind-solo-cluster}"
READY_TIMEOUT="${READY_TIMEOUT:-300}"

target_index="${1:?reconfigure-bn-roster-bootstrap-tss.sh: target BN index required (e.g. 1)}"
shift
[[ $# -ge 1 ]] || { echo "reconfigure-bn-roster-bootstrap-tss.sh: at least one peer BN index required (e.g. 2 3)" >&2; exit 1; }

log() { echo "[wrb-dist-bn-roster-tss] $*"; }
fail() { echo "[wrb-dist-bn-roster-tss] ERROR: $*" >&2; exit 1; }

target_bn="block-node-${target_index}"
roster_tss_path="/opt/hiero/block-node/roster-bootstrap-tss"
sources_filename="block-node-sources.json"
sources_configmap="${target_bn}-roster-bootstrap-tss-sources"
config_configmap="${target_bn}-config"

# Build the peer node entries, reading each peer's actual listening port from
# its own "-config" ConfigMap (SERVER_PORT) rather than assuming the chart
# default, so this doesn't silently produce a wrong sources file for
# custom-port topologies.
nodes_json="[]"
for peer_index in "$@"; do
    peer_bn="block-node-${peer_index}"
    peer_dns="${peer_bn}.${NAMESPACE}.svc.cluster.local"
    peer_port=$(kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" \
        get configmap "${peer_bn}-config" -o jsonpath='{.data.SERVER_PORT}' 2>/dev/null || echo "")
    : "${peer_port:=40840}"
    nodes_json=$(echo "${nodes_json}" | jq --arg addr "${peer_dns}" --argjson port "${peer_port}" \
        '. + [{"address": $addr, "port": $port, "priority": 1}]')
    log "  Peer ${peer_bn}: ${peer_dns}:${peer_port}"
done

log "Reconfiguring ${target_bn} to pull TssData from peer(s): $*"

# 1) Create/update the sources ConfigMap.
sources_file="${TMPDIR:-/tmp}/wrb-dist-${target_bn}-roster-tss-sources.json"
jq -n --argjson nodes "${nodes_json}" '{nodes: $nodes}' > "${sources_file}"

kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" \
    create configmap "${sources_configmap}" \
    --from-file="${sources_filename}=${sources_file}" \
    --dry-run=client -o yaml \
    | kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" apply -f - \
    || fail "failed to create/update ${sources_configmap}"
log "  ${sources_configmap} applied."

# 2) Merge the roster-bootstrap-tss sources path into the existing
#    "-config" ConfigMap.
kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" \
    patch configmap "${config_configmap}" --type merge -p "$(cat <<EOF
{
  "data": {
    "ROSTER_BOOTSTRAP_TSS_BLOCK_NODE_SOURCES_PATH": "${roster_tss_path}/${sources_filename}"
  }
}
EOF
)" || fail "failed to patch ${config_configmap}"
log "  ${config_configmap} patched (ROSTER_BOOTSTRAP_TSS_BLOCK_NODE_SOURCES_PATH)."

# 3) Patch the StatefulSet's pod template to add the volume + volumeMount.
#    volumes[] and containers[].volumeMounts[] are both merged on their
#    `name` key by a strategic merge patch, so this appends rather than
#    clobbering the chart's existing volumes/mounts. This depends on the
#    container actually being named "block-node-server" (per
#    charts/block-node-server/templates/statefulset.yaml) — if that ever
#    changes, the volumeMount patch would silently no-op while the volume
#    itself still gets added, so check it explicitly rather than fail later
#    with a mount-less volume and no error.
actual_container=$(kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" \
    get statefulset "${target_bn}" \
    -o jsonpath='{.spec.template.spec.containers[0].name}')
[[ "${actual_container}" == "block-node-server" ]] \
    || fail "expected container named 'block-node-server' in ${target_bn}, got '${actual_container}'"

statefulset_patch="${TMPDIR:-/tmp}/wrb-dist-${target_bn}-roster-tss-sts-patch.yaml"
cat > "${statefulset_patch}" <<EOF
spec:
  template:
    spec:
      volumes:
        - name: roster-bootstrap-tss-sources
          configMap:
            name: ${sources_configmap}
      containers:
      - name: block-node-server
        volumeMounts:
        - name: roster-bootstrap-tss-sources
          mountPath: ${roster_tss_path}
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

log "${target_bn} is now configured to pull TssData from: $*"
