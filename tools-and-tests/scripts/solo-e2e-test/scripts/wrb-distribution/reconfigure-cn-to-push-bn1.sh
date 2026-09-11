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

# ── Re-establish BN1 port-forward ────────────────────────────────────────────
# type:port-forward test-runner events kill ALL kubectl port-forward processes
# before starting their own (static topology only), so BN1's nohup forward
# (set up by stage-tss-data-on-bn1.sh) may be dead by this point.
# Re-establish it now so the live-push catch-up below and assert-cutover-sync
# can actually reach BN1.  stop-cn-record-production (the next step) will
# restart HapiApp, which connects to BN1 immediately -- BN1 must be reachable
# via the port-forward before that happens so the live-push can pre-fill it.
BN1_LOCAL_GRPC_PORT="${BN1_GRPC_PORT:-40840}"
pf_log_dir="${TMPDIR:-/tmp}/wrb-dist-add-bn-pf"
mkdir -p "${pf_log_dir}"
setsid_prefix=""
command -v setsid >/dev/null 2>&1 && setsid_prefix="setsid"

pkill -f "port-forward svc/block-node-1.*${BN1_LOCAL_GRPC_PORT}:" 2>/dev/null || true
sleep 1
nohup ${setsid_prefix} kubectl --context "${CONTEXT}" --namespace "${NAMESPACE}" \
    port-forward svc/block-node-1 "${BN1_LOCAL_GRPC_PORT}:40840" \
    >"${pf_log_dir}/block-node-1-grpc.log" 2>&1 </dev/null &

log "Waiting for BN1 port-forward (localhost:${BN1_LOCAL_GRPC_PORT}) to come up..."
for _ in $(seq 1 30); do
    grep -q "Forwarding from" "${pf_log_dir}/block-node-1-grpc.log" 2>/dev/null && break
    sleep 1
done
grep -q "Forwarding from" "${pf_log_dir}/block-node-1-grpc.log" 2>/dev/null \
    || fail "Port-forward for block-node-1 grpc never came up after 30s"
log "BN1 port-forward up."

# ── Live-push catch-up ────────────────────────────────────────────────────────
# BN1 may be stuck at the bulk-loaded block level if the live-push worker was
# failing while its port-forward was dead.  Push all already-wrapped blocks to
# BN1 now so it holds all pre-TSS blocks before stop-cn-record-production
# restarts HapiApp.  Once the CN starts streaming it will open from wherever
# BN1 is; if BN1 already has every pre-TSS wrapped block the CN only needs to
# send new TSS-era blocks, which BN1 can verify via TSSVerifier.
ENV_FILE="${ENV_FILE:-/tmp/wrb-distribution-step12.env}"
if [[ -f "${ENV_FILE}" ]]; then
    # shellcheck disable=SC1090
    source "${ENV_FILE}"
fi
if [[ -n "${WRB_DIST_WORK_DIR:-}" && -n "${CLI_LIB:-}" ]]; then
    wrapped_dir="${WRB_DIST_WORK_DIR}/wrappedBlocks"
    if [[ -d "${wrapped_dir}" ]]; then
        log "Running blocks push to catch up BN1 with all wrapped blocks..."
        java -cp "${CLI_LIB}/*" \
            org.hiero.block.tools.BlockStreamTool blocks push \
                --input-dir "${wrapped_dir}" \
                --bn-host localhost \
                --bn-port "${BN1_LOCAL_GRPC_PORT}" \
            && log "Catch-up push completed." \
            || log "WARNING: blocks push returned non-zero; live-push worker will retry on its next poll."
    else
        log "WARNING: ${wrapped_dir} not found; skipping catch-up push."
    fi
else
    log "WARNING: ENV_FILE not sourced or missing WRB_DIST_WORK_DIR/CLI_LIB; skipping catch-up push."
fi
