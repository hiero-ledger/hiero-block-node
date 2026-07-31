#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# backfill-wrb-to-bn.sh — Bulk-load a local wrapped-block archive into a
# deployed Block Node's historic-storage directory, then roll the StatefulSet
# so the BN's `BlockFileHistoricPlugin` re-scans and picks up the copied files.
#
# Use this instead of `blocks push` (gRPC publish stream) whenever you're
# seeding an EMPTY BN from historical data. The live publish path is a
# single-slot-per-block-number CAS meant for freshly-produced blocks and
# silently SKIPs anything that loses the race, which will bite on any block
# that a peer producer has already streamed (typical on real networks). The
# `blocks bulk-load` subcommand exists for exactly this case: it copies WRB
# zips directly into the BN's on-disk historic archive, bypassing live-stream
# verification entirely.
#
# Works against any deployed Block Node addressable via kubectl -- Tier 0 or
# Tier 1, mainnet / testnet / previewnet / a local kind cluster / etc. Defaults
# match the shape produced by `sudo solo-provisioner block node install`
# (namespace `block-node`, release `block-node`, chart mount path
# `/opt/hiero/block-node/data/historic`); override via env vars for other
# deployment shapes.
#
# Prerequisites:
#   * `kubectl` on PATH, current context pointed at the target cluster
#     (or override via BN_KUBE_CONTEXT)
#   * `java` on PATH
#   * A shaded wrb-cli jar (`tools-*-all.jar`) on disk -- point at it with CLI_JAR
#   * A local directory of wrapped blocks (the output of `blocks wrap`)
#
# Usage:
#   ./backfill-wrb-to-bn.sh <path-to-wrappedBlocks-dir>
#
# Environment overrides:
#   BN_KUBE_CONTEXT      kubectl context           (default: current context)
#   BN_NAMESPACE         Kubernetes namespace      (default: block-node)
#   BN_STATEFULSET       StatefulSet name          (default: block-node-block-node-server)
#   BN_POD               Pod name                  (default: <BN_STATEFULSET>-0)
#   BN_CONTAINER         Container name in pod     (default: block-node-server)
#   HISTORIC_MOUNT_PATH  On-pod historic dir       (default: /opt/hiero/block-node/data/historic)
#   STAGING_DIR          Local staging dir         (default: /tmp/bn-backfill-<pid>)
#   CLI_JAR              Path to wrb-cli jar       (default: hunts tools-*-all.jar)
#   READY_TIMEOUT        Pod-ready wait (seconds)  (default: 300)
#   SKIP_ROLLOUT         "true" to skip the pod    (default: false)
#                        restart step
#   KEEP_STAGING         "true" to keep the local  (default: false)
#                        staging dir on success
#
# Example (previewnet Tier 1 deployed via Solo Provisioner):
#   ./backfill-wrb-to-bn.sh ~/wrappedBlocks
#
# Example (custom deployment shape, non-standard namespace + container):
#   BN_NAMESPACE=my-bn \
#   BN_STATEFULSET=my-bn-server \
#   BN_CONTAINER=bn \
#   ./backfill-wrb-to-bn.sh /mnt/archive/wrappedBlocks

set -euo pipefail

# --- Config ---
: "${BN_KUBE_CONTEXT:=}"                                              # empty -> use current
: "${BN_NAMESPACE:=block-node}"
: "${BN_STATEFULSET:=block-node-block-node-server}"
: "${BN_POD:=${BN_STATEFULSET}-0}"
: "${BN_CONTAINER:=block-node-server}"
: "${HISTORIC_MOUNT_PATH:=/opt/hiero/block-node/data/historic}"
: "${STAGING_DIR:=/tmp/bn-backfill-$$}"
: "${READY_TIMEOUT:=300}"
: "${SKIP_ROLLOUT:=false}"
: "${KEEP_STAGING:=false}"

# --- Logging ---
log()  { echo "[backfill-wrb-to-bn] $*"; }
fail() { echo "[backfill-wrb-to-bn] ERROR: $*" >&2; exit 1; }

# --- Argument parsing ---
[[ $# -ge 1 ]] || fail "usage: $0 <path-to-wrappedBlocks-dir>"
WRB_SOURCE_DIR="$1"
[[ -d "${WRB_SOURCE_DIR}" ]] || fail "wrappedBlocks dir does not exist: ${WRB_SOURCE_DIR}"

# --- Locate the wrb-cli jar ---
if [[ -z "${CLI_JAR:-}" ]]; then
    # Prefer explicit location next to this script (built via ./gradlew :tools:shadowJar)
    script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    candidate=$(ls "${script_dir}/../build/libs/"tools-*-all.jar 2>/dev/null | head -1 || true)
    if [[ -z "${candidate}" ]]; then
        candidate=$(command -v tools-cli || ls tools-*-all.jar 2>/dev/null | head -1 || true)
    fi
    [[ -n "${candidate}" ]] || fail "wrb-cli jar not found; set CLI_JAR=<path> to override"
    CLI_JAR="${candidate}"
fi
[[ -f "${CLI_JAR}" ]] || fail "CLI_JAR does not exist: ${CLI_JAR}"

# --- kubectl invocation prefix (context + namespace) ---
kctl=(kubectl --namespace "${BN_NAMESPACE}")
[[ -n "${BN_KUBE_CONTEXT}" ]] && kctl+=(--context "${BN_KUBE_CONTEXT}")

log "Config:"
log "  source WRB dir:        ${WRB_SOURCE_DIR}"
log "  staging dir:           ${STAGING_DIR}"
log "  CLI jar:               ${CLI_JAR}"
log "  kubectl context:       ${BN_KUBE_CONTEXT:-<current>}"
log "  namespace:             ${BN_NAMESPACE}"
log "  target pod:            ${BN_POD} (container ${BN_CONTAINER})"
log "  target statefulset:    ${BN_STATEFULSET}"
log "  historic mount path:   ${HISTORIC_MOUNT_PATH}"

# --- Preflight ---
command -v kubectl >/dev/null || fail "kubectl not on PATH"
command -v java >/dev/null || fail "java not on PATH"

log "Preflight: verifying target pod ${BN_POD} is Ready..."
"${kctl[@]}" get pod "${BN_POD}" >/dev/null || fail "pod ${BN_POD} not found in namespace ${BN_NAMESPACE}"
"${kctl[@]}" wait --for=condition=Ready pod/"${BN_POD}" --timeout=60s \
    || fail "pod ${BN_POD} not Ready within 60s; aborting before we stage anything"

# --- Stage via `blocks bulk-load` ---
log "Staging wrapped blocks via 'blocks bulk-load' (${WRB_SOURCE_DIR} -> ${STAGING_DIR})..."
mkdir -p "${STAGING_DIR}"
java -jar "${CLI_JAR}" blocks bulk-load \
    --source "${WRB_SOURCE_DIR}" \
    --dest   "${STAGING_DIR}" \
    || fail "'blocks bulk-load' staging failed"

if [[ -z "$(find "${STAGING_DIR}" -name '*.zip' -print -quit 2>/dev/null)" ]]; then
    fail "no .zip files staged in ${STAGING_DIR}; nothing to backfill"
fi
staged_count=$(find "${STAGING_DIR}" -name '*.zip' | wc -l)
log "Staged ${staged_count} zip file(s) into ${STAGING_DIR}."

# --- Stream into the pod via tar | kubectl exec ---
log "Streaming staged blocks into ${BN_POD}:${HISTORIC_MOUNT_PATH}..."
"${kctl[@]}" exec "${BN_POD}" -c "${BN_CONTAINER}" -- mkdir -p "${HISTORIC_MOUNT_PATH}" \
    || fail "failed to ensure ${HISTORIC_MOUNT_PATH} exists on ${BN_POD}"
tar -C "${STAGING_DIR}" -cf - . | \
    "${kctl[@]}" exec -i "${BN_POD}" -c "${BN_CONTAINER}" -- \
    tar xf - -C "${HISTORIC_MOUNT_PATH}" \
    || fail "failed to stream staged blocks into ${BN_POD}"
log "Blocks copied onto ${BN_POD}'s persistent historic volume."

# --- Roll the StatefulSet so BlockFileHistoricPlugin re-scans ---
if [[ "${SKIP_ROLLOUT}" == "true" ]]; then
    log "SKIP_ROLLOUT=true; not rolling the StatefulSet. The BN will pick up the new"
    log "historic files on its next natural restart. If you don't want to wait, run:"
    log "  kubectl rollout restart statefulset/${BN_STATEFULSET} -n ${BN_NAMESPACE}"
else
    log "Rolling statefulset/${BN_STATEFULSET} so BlockFileHistoricPlugin re-scans..."
    "${kctl[@]}" rollout restart statefulset/"${BN_STATEFULSET}" \
        || fail "rollout restart failed for statefulset/${BN_STATEFULSET}"
    "${kctl[@]}" rollout status statefulset/"${BN_STATEFULSET}" --timeout="${READY_TIMEOUT}s" \
        || fail "statefulset/${BN_STATEFULSET} rollout did not complete within ${READY_TIMEOUT}s"
    "${kctl[@]}" wait --for=condition=Ready pod/"${BN_POD}" --timeout="${READY_TIMEOUT}s" \
        || fail "${BN_POD} did not become Ready after restart"
    log "${BN_POD} is Ready after rollout."
fi

# --- Cleanup ---
if [[ "${KEEP_STAGING}" == "true" ]]; then
    log "KEEP_STAGING=true; leaving staging dir at ${STAGING_DIR}"
else
    rm -rf "${STAGING_DIR}"
    log "Removed staging dir ${STAGING_DIR}."
fi

log "Backfill complete. Verify the BN sees the new blocks with:"
log "  kubectl port-forward -n ${BN_NAMESPACE} svc/${BN_STATEFULSET} 18082:40982 &"
log "  grpcurl -plaintext -emit-defaults \\"
log "    -import-path <path-to-block-node-protobuf> \\"
log "    -proto block-node/api/node_service.proto \\"
log "    -d '{}' localhost:18082 org.hiero.block.api.BlockNodeService/serverStatus"
log "Expect: firstAvailableBlock / lastAvailableBlock now show real block numbers"
log "(no longer UINT64_MAX sentinel 18446744073709551615)."
