#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# WRB Distribution E2E (#3125 slice 6 — step 11) — stage the CLI-generated
# tss-bootstrap-roster.json onto BN1 (Tier-0) and restart it.
#
# Canonical flow this implements (steps 3-4 of 4; see
# detect-tss-enablement.sh for steps 1-2):
#   3. Node operator issues a command on Block Stream CLI to place the file
#      on the Tier-0 BN. In this Kind/Solo test environment BN1 runs in a
#      separate pod from wherever the CLI ran, so "place the file" is a
#      `kubectl cp` onto BN1's pod rather than a same-host file copy.
#   4. BN1 exposes the TSS details on its serverStatusDetail API.
#
# The Block Node only loads app.state.tssBootstrapFilePath on startup
# (BlockNodeApp.loadApplicationState()), so step 4 requires restarting BN1
# after the file lands — same reasoning as bulk-load-historical-to-bn1.sh's
# restart, including re-establishing the port-forwards it tears down.
#
# Usage:
#     stage-tss-data-on-bn1.sh
#
# Reads:
#   NAMESPACE          (default "solo-network")
#   CLUSTER_REFERENCE  (default "kind-solo-cluster")
#   BN1_GRPC_PORT      (default 40840 — matches add-bn.sh's port-forward convention)
#   BN1_METRICS_PORT   (default 16007)
#   READY_TIMEOUT      (default 300)
#   TSS_BOOTSTRAP_JSON_PATH (required — written by detect-tss-enablement.sh
#                            into ENV_FILE)

set -euo pipefail

ENV_FILE="${ENV_FILE:-/tmp/wrb-distribution-step12.env}"
if [[ -f "${ENV_FILE}" ]]; then
    # shellcheck disable=SC1090
    source "${ENV_FILE}"
fi

: "${NAMESPACE:=solo-network}"
: "${CLUSTER_REFERENCE:=kind-solo-cluster}"
BN1_GRPC_PORT="${BN1_GRPC_PORT:-$((40839 + 1))}"
BN1_METRICS_PORT="${BN1_METRICS_PORT:-$((16006 + 1))}"
READY_TIMEOUT="${READY_TIMEOUT:-300}"

: "${TSS_BOOTSTRAP_JSON_PATH:?TSS_BOOTSTRAP_JSON_PATH must be set (written by detect-tss-enablement.sh)}"
[[ -f "${TSS_BOOTSTRAP_JSON_PATH}" ]] || { echo "[wrb-dist-stage-tss] ERROR: ${TSS_BOOTSTRAP_JSON_PATH} does not exist" >&2; exit 1; }

log() { echo "[wrb-dist-stage-tss] $*"; }
fail() { echo "[wrb-dist-stage-tss] ERROR: $*" >&2; exit 1; }

# Same port-forward wait helper as bulk-load-historical-to-bn1.sh.
wait_for_port_forward() {
    local log_file="$1" label="$2"
    for _ in $(seq 1 30); do
        grep -q "Forwarding from" "${log_file}" 2>/dev/null && return 0
        sleep 1
    done
    fail "Port-forward for ${label} never came up after 30s (see ${log_file})"
}

pod="block-node-1-0"
# Matches ApplicationStateConfig's default app.state.tssBootstrapFilePath.
app_state_dir="/opt/hiero/block-node/application-state"
target_path="${app_state_dir}/tss-bootstrap-roster.json"

log "Staging ${TSS_BOOTSTRAP_JSON_PATH} onto ${pod}:${target_path}..."
kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" \
    exec "${pod}" -c block-node-server -- mkdir -p "${app_state_dir}" \
    || fail "Failed to ensure ${app_state_dir} exists on ${pod}"
kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" \
    exec -i "${pod}" -c block-node-server -- tee "${target_path}" < "${TSS_BOOTSTRAP_JSON_PATH}" > /dev/null \
    || fail "Failed to copy ${TSS_BOOTSTRAP_JSON_PATH} onto ${pod}"
log "File staged onto ${pod}'s persistent application-state volume."

log "Rolling ${pod} (statefulset/block-node-1) so it loads the staged TssData on startup..."
kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" \
    rollout restart statefulset/block-node-1 \
    || fail "rollout restart failed for statefulset/block-node-1"
kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" \
    rollout status statefulset/block-node-1 --timeout="${READY_TIMEOUT}s" \
    || fail "statefulset/block-node-1 rollout did not complete"
kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" \
    wait --for=condition=Ready pod/"${pod}" --timeout="${READY_TIMEOUT}s" \
    || fail "${pod} did not become Ready after restart"

log "Re-establishing kubectl port-forwards for block-node-1 (grpc :${BN1_GRPC_PORT}, metrics :${BN1_METRICS_PORT})..."
pkill -f "port-forward svc/block-node-1 ${BN1_GRPC_PORT}:" 2>/dev/null || true
pkill -f "port-forward svc/block-node-1 ${BN1_METRICS_PORT}:" 2>/dev/null || true
sleep 1

pf_log_dir="${TMPDIR:-/tmp}/wrb-dist-add-bn-pf"
mkdir -p "${pf_log_dir}"
# setsid isn't available on macOS (it's a util-linux tool); fall back to plain
# nohup there — the port-forwards still get backgrounded and survive the
# parent shell exiting, just without their own process group.
setsid_prefix=""
command -v setsid >/dev/null 2>&1 && setsid_prefix="setsid"
nohup ${setsid_prefix} kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" \
    port-forward svc/block-node-1 "${BN1_GRPC_PORT}:40840" \
    >"${pf_log_dir}/block-node-1-grpc.log" 2>&1 </dev/null &
nohup ${setsid_prefix} kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" \
    port-forward svc/block-node-1 "${BN1_METRICS_PORT}:16007" \
    >"${pf_log_dir}/block-node-1-metrics.log" 2>&1 </dev/null &
wait_for_port_forward "${pf_log_dir}/block-node-1-grpc.log" "block-node-1 grpc"
wait_for_port_forward "${pf_log_dir}/block-node-1-metrics.log" "block-node-1 metrics"

log "BN1 restarted with the staged TssData; serverStatusDetail should reflect it shortly."
