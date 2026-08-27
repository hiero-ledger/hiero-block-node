#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# WRB Distribution E2E (#3125 slice 6 — step 12) — redirect the CNs' GRPC
# stream to BN1 so it is the Tier-0 live receiver when stop-cn-record-
# production restarts HapiApp. This mirrors the 160-workflow's post-TSS_SIGN
# architecture: BN1 (Tier-0) is always the live GRPC receiver, and BN2/BN3
# (Tier-1) always backfill from BN1.
#
# After Phase 2's FREEZE_UPGRADE (cn-upgrade-tss.sh rc), Helm recreates the
# indexed ConfigMaps with all-3-BNs and the CN init-containers copy that into
# block-nodes.json on every pod restart. This script overwrites block-nodes.json
# with BN1-only config on the live pod filesystem so that when stop-cn-record-
# production.sh's `solo consensus node start` boots HapiApp, it reads BN1-only.
#
# ORDERING: must run BEFORE stop-cn-record-production.sh. HapiApp reads
# block-nodes.json only at startup and does not reload it dynamically; writing
# the file after the restart has no effect.
#
# Reads:
#   NAMESPACE   (default "solo-network")
#   CONTEXT     (default "kind-solo-cluster")
#   BN_HOST_1   (default block-node-1.${NAMESPACE}.svc.cluster.local)
#   BN_PORT_1   (default 40840)

set -euo pipefail

: "${NAMESPACE:=solo-network}"
: "${CONTEXT:=kind-solo-cluster}"
: "${BN_HOST_1:=block-node-1.${NAMESPACE}.svc.cluster.local}"
BN_PORT_1="${BN_PORT_1:-40840}"

log() { echo "[wrb-dist-cn-to-bn1] $*"; }
fail() { echo "[wrb-dist-cn-to-bn1] ERROR: $*" >&2; exit 1; }

CONFIG_PATH="/opt/hgcapp/services-hedera/HapiApp2.0/data/config/block-nodes.json"
CONFIG_CONTENT=$(cat <<EOF
{
  "nodes": [
    {
      "address": "${BN_HOST_1}",
      "streamingPort": ${BN_PORT_1},
      "servicePort": ${BN_PORT_1},
      "priority": 1
    }
  ],
  "blockItemBatchSize": 256
}
EOF
)

log "New block-nodes.json (all 3 CNs -> BN1 only):"
log "${CONFIG_CONTENT}"

for cn_name in node1 node2 node3; do
    cn_pod="network-${cn_name}-0"
    log "Writing ${CONFIG_PATH} on ${cn_pod}..."
    echo "${CONFIG_CONTENT}" | kubectl --context "${CONTEXT}" --namespace "${NAMESPACE}" \
        exec -i "${cn_pod}" -c root-container -- bash -c "cat > ${CONFIG_PATH}" \
        || fail "Failed to write block-nodes.json on ${cn_pod}"
done

log "All 3 CNs reconfigured to publish to BN1 (${BN_HOST_1}:${BN_PORT_1})."
