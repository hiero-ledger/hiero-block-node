#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# WRB Distribution E2E (#3125 slice 5 — step 9, historical half) —
# bulk-load-historical-to-bn1.
#
# Historical WRBs must NOT go through BN1's live publish/gRPC stream: that
# pipeline is a single-slot-per-block-number race (see
# LiveStreamPublisherManager.resolveActionForHeader's nextUnstreamedBlockNumber
# CAS) meant for freshly-produced blocks, and it silently SKIPs anything that
# loses the race rather than persisting it — which is exactly what happened
# when this slice first tried pushing block 0 to BN1 over gRPC. The wrb-cli's
# own `blocks bulk-load` subcommand (PR #3038) exists for precisely this case:
# it copies wrapped block zips directly into a Block Node's historic-storage
# directory, bypassing live-stream verification entirely; BlockFileHistoricPlugin
# picks them up on the BN's next startup scan. This script:
#
#   1. Runs `blocks bulk-load` locally (source=wrappedBlocks, dest=a local
#      staging dir) — reuses the CLI's own file selection (zips only) and
#      resumability bookkeeping instead of hand-rolling a copy.
#   2. Streams the staged result into BN1's pod via `tar | kubectl exec` (more
#      reliable than `kubectl cp`'s directory-copy semantics) at
#      /opt/hiero/block-node/data/historic — the chart's actual configured
#      archive.mountPath (charts/block-node-server/values.yaml), NOT
#      /opt/hiero/block-node/data/archive (a stale path used by the test
#      runner's unrelated clear-block-storage helper).
#   3. Rolls BN1's StatefulSet pod so BlockFileHistoricPlugin re-scans on
#      startup and picks up the newly-copied files. Safe here because nothing
#      else is writing to BN1's storage at this point in the test (CN was
#      redirected to BN3-only in step 8; MN1/MN2 only ever read from BN2/BN3).
#      /opt/hiero/block-node/data is PVC-backed (volumeClaimTemplates in the
#      chart), so the copied files survive the pod restart.
#   4. Re-establishes the grpc/metrics port-forwards the restart tears down
#      (same convention as add-bn.sh).
#
# Reads:
#   NAMESPACE          (default "solo-network")
#   CONTEXT            (default "kind-solo-cluster")
#   BN1_GRPC_PORT      (default 40840 — matches add-bn.sh's port-forward convention)
#   BN1_METRICS_PORT   (default 16007)
#   READY_TIMEOUT      (default 300)

set -euo pipefail

ENV_FILE="${ENV_FILE:-/tmp/wrb-distribution-step12.env}"
if [[ -f "${ENV_FILE}" ]]; then
    # shellcheck disable=SC1090
    source "${ENV_FILE}"
fi

: "${NAMESPACE:=solo-network}"
: "${CONTEXT:=kind-solo-cluster}"
BN1_GRPC_PORT="${BN1_GRPC_PORT:-$((40839 + 1))}"
BN1_METRICS_PORT="${BN1_METRICS_PORT:-$((16006 + 1))}"
READY_TIMEOUT="${READY_TIMEOUT:-300}"

: "${WRB_DIST_WORK_DIR:?WRB_DIST_WORK_DIR must be set (written by install-and-run-wrb-cli.sh)}"
: "${CLI_LIB:?CLI_LIB must be set (written by install-and-run-wrb-cli.sh)}"

log() { echo "[wrb-dist-bulk-load-bn1] $*"; }
fail() { echo "[wrb-dist-bulk-load-bn1] ERROR: $*" >&2; exit 1; }

wrapped_dir="${WRB_DIST_WORK_DIR}/wrappedBlocks"
staging_dir="${WRB_DIST_WORK_DIR}/bn1-bulk-load-staging"
[[ -d "${wrapped_dir}" ]] || fail "Wrapped dir missing: ${wrapped_dir}"

HISTORIC_MOUNT_PATH="/opt/hiero/block-node/data/historic"
pod="block-node-1-0"

log "Staging wrapped blocks via 'blocks bulk-load' (${wrapped_dir} -> ${staging_dir})..."
java -cp "${CLI_LIB}/*" \
    org.hiero.block.tools.BlockStreamTool blocks bulk-load \
        --source "${wrapped_dir}" \
        --dest "${staging_dir}" \
    || fail "'blocks bulk-load' staging failed"

if [[ -z "$(find "${staging_dir}" -name '*.zip' -print -quit 2>/dev/null)" ]]; then
    fail "No .zip files staged in ${staging_dir}; nothing to bulk-load onto BN1"
fi

log "Streaming staged blocks into ${pod}:${HISTORIC_MOUNT_PATH}..."
kubectl --context "${CONTEXT}" --namespace "${NAMESPACE}" \
    exec "${pod}" -c block-node-server -- mkdir -p "${HISTORIC_MOUNT_PATH}" \
    || fail "Failed to ensure ${HISTORIC_MOUNT_PATH} exists on ${pod}"
tar -C "${staging_dir}" -cf - . | kubectl --context "${CONTEXT}" --namespace "${NAMESPACE}" \
    exec -i "${pod}" -c block-node-server -- tar xf - -C "${HISTORIC_MOUNT_PATH}" \
    || fail "Failed to stream staged blocks into ${pod}"
log "Blocks copied onto ${pod}'s persistent historic volume."

log "Rolling ${pod} (statefulset/block-node-1) so BlockFileHistoricPlugin re-scans..."
kubectl --context "${CONTEXT}" --namespace "${NAMESPACE}" \
    rollout restart statefulset/block-node-1 \
    || fail "rollout restart failed for statefulset/block-node-1"
kubectl --context "${CONTEXT}" --namespace "${NAMESPACE}" \
    rollout status statefulset/block-node-1 --timeout="${READY_TIMEOUT}s" \
    || fail "statefulset/block-node-1 rollout did not complete"
kubectl --context "${CONTEXT}" --namespace "${NAMESPACE}" \
    wait --for=condition=Ready pod/"${pod}" --timeout="${READY_TIMEOUT}s" \
    || fail "${pod} did not become Ready after restart"

log "Re-establishing kubectl port-forwards for block-node-1 (grpc :${BN1_GRPC_PORT}, metrics :${BN1_METRICS_PORT})..."
# The restart above deleted the pod add-bn.sh's own port-forwards were
# tunneling to; kill any still-running ones first so the new ones below can
# bind the same local ports (a dead-pod port-forward doesn't always exit
# immediately on its own).
pkill -f "port-forward svc/block-node-1 ${BN1_GRPC_PORT}:" 2>/dev/null || true
pkill -f "port-forward svc/block-node-1 ${BN1_METRICS_PORT}:" 2>/dev/null || true
sleep 1

pf_log_dir="${TMPDIR:-/tmp}/wrb-dist-add-bn-pf"
mkdir -p "${pf_log_dir}"
nohup setsid kubectl --context "${CONTEXT}" --namespace "${NAMESPACE}" \
    port-forward svc/block-node-1 "${BN1_GRPC_PORT}:40840" \
    >"${pf_log_dir}/block-node-1-grpc.log" 2>&1 </dev/null &
nohup setsid kubectl --context "${CONTEXT}" --namespace "${NAMESPACE}" \
    port-forward svc/block-node-1 "${BN1_METRICS_PORT}:16007" \
    >"${pf_log_dir}/block-node-1-metrics.log" 2>&1 </dev/null &
sleep 2

rm -rf "${staging_dir}"
log "Historical backfill complete: block-node-1 restarted with the bulk-loaded WRBs."
