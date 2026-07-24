#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# WRB Distribution E2E (#3125 slice 5 — step 8) — reconfigure the 3 CNs to
# publish blocks to BN3.
#
# Writes a fresh block-nodes.json onto each network-node pod, pointing at BN3
# alone (priority 1). This is a config-file-only change: the same JSON shape
# and path apply_cn_block_nodes_configs already writes at deploy time in
# solo-deploy-network.sh, and the same shape execute_reconfigure_cn_streaming
# in solo-test-runner.sh writes for mid-test reconfiguration — neither restarts
# the CN pod, since the CN picks up block-nodes.json as its dynamic
# block-node-connection routing config rather than only at startup.
#
# Note: BN3 was installed (step 5) with EMB=100_000_000, specifically so it
# won't persist the low block numbers the CN is producing at this point in the
# test — it's there to prove out "future live streaming," not to receive
# blocks yet. So this step only proves the config surface (assert-cn-
# publishing-to-bn3.sh checks each CN's block-nodes.json references BN3);
# actual block flow to BN3 is out of scope here.
#
# Reads:
#   NAMESPACE   (default "solo-network")
#   CONTEXT     (default "kind-solo-cluster")
#   BN_HOST_3   (default block-node-3.${NAMESPACE}.svc.cluster.local)
#   BN_PORT_3   (default 40840)

set -euo pipefail

: "${NAMESPACE:=solo-network}"
: "${CONTEXT:=kind-solo-cluster}"
: "${BN_HOST_3:=block-node-3.${NAMESPACE}.svc.cluster.local}"
BN_PORT_3="${BN_PORT_3:-40840}"

log() { echo "[wrb-dist-cn-to-bn3] $*"; }
fail() { echo "[wrb-dist-cn-to-bn3] ERROR: $*" >&2; exit 1; }

CONFIG_PATH="/opt/hgcapp/services-hedera/HapiApp2.0/data/config/block-nodes.json"
CONFIG_CONTENT=$(cat <<EOF
{
  "nodes": [
    {
      "address": "${BN_HOST_3}",
      "streamingPort": ${BN_PORT_3},
      "servicePort": ${BN_PORT_3},
      "priority": 1
    }
  ],
  "blockItemBatchSize": 256
}
EOF
)

log "New block-nodes.json (all 3 CNs -> BN3 only):"
log "${CONFIG_CONTENT}"

for cn_name in node1 node2 node3; do
    cn_pod="network-${cn_name}-0"
    log "Writing ${CONFIG_PATH} on ${cn_pod}..."
    echo "${CONFIG_CONTENT}" | kubectl --context "${CONTEXT}" --namespace "${NAMESPACE}" \
        exec -i "${cn_pod}" -c root-container -- bash -c "cat > ${CONFIG_PATH}" \
        || fail "Failed to write block-nodes.json on ${cn_pod}"
done

log "All 3 CNs reconfigured to publish to BN3 (${BN_HOST_3}:${BN_PORT_3})."
