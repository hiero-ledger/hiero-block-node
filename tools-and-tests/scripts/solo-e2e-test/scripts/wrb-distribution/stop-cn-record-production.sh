#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# WRB Distribution E2E (#3125 slice 6 — step 12) — stop CN record-file
# production to simulate the real TSS cutover.
#
# Once TSS/WRAPS is verified (step 11), a real cutover retires the
# record-stream file writer entirely and leaves only the native gRPC block
# stream. This sets blockStream.writerMode=GRPC (was FILE_AND_GRPC) on all 3
# CNs' application.properties and restarts them — per
# docs/block-node/operations/consensus-node-to-block-node-configuration.md,
# writerMode cannot be hot-reloaded, so a restart is required for the change
# to take effect.
#
# After this point no new record files are produced for wrb-cli to wrap:
# every BN and MN must get all subsequent blocks purely through the CN -> BN1
# gRPC stream (reconfigure-cn-to-push-bn1.sh, which runs first so HapiApp
# starts with BN1-only config) and its downstream backfill/consumption chain
# (steps 6-7, 9-10) — see assert-cutover-sync.sh.
#
# Reads:
#   NAMESPACE          (default "solo-network")
#   CLUSTER_REFERENCE  (default "kind-solo-cluster")
#   DEPLOYMENT         (default "deployment-solo")
#   NODE_ALIASES       (default "node1,node2,node3")
#   READY_TIMEOUT      (default 300)

set -euo pipefail

: "${NAMESPACE:=solo-network}"
: "${CLUSTER_REFERENCE:=kind-solo-cluster}"
: "${DEPLOYMENT:=deployment-solo}"
: "${NODE_ALIASES:=node1,node2,node3}"
READY_TIMEOUT="${READY_TIMEOUT:-300}"

log() { echo "[wrb-dist-stop-cn-records] $*"; }
fail() { echo "[wrb-dist-stop-cn-records] ERROR: $*" >&2; exit 1; }

CONFIG_PATH="/opt/hgcapp/services-hedera/HapiApp2.0/data/config/application.properties"

# Same WRAPS v1.0.0 cache as cn-upgrade-tss.sh / solo-deploy-network.sh's
# ensure_wraps_keys_cached. `solo consensus node start` on a TSS-enabled,
# post-v0.76-upgrade network needs --wraps-key-path on every start (not just
# at genesis) — omitting it here caused CN logs to show "WrapsProvingKeyVerification
# - Failed to extract WRAPS proving key archive data/keys/wraps.tar.gz"
# immediately after this script's restart in CI.
WRAPS_DOWNLOAD_URL="https://builds.hedera.com/tss/hiero/wraps/v1.0/wraps-v1.0.0.tar.gz"
WRAPS_KEYS_DIR="${HOME}/.solo/cache/wraps-v1.0.0-keys"
WRAPS_KEY_FILES="decider_pp.bin decider_vp.bin nova_pp.bin nova_vp.bin"

ensure_wraps_keys_cached() {
    local tarball="${WRAPS_KEYS_DIR}/wraps-v1.0.0.tar.gz"
    local f need_extract="false"
    for f in ${WRAPS_KEY_FILES}; do
        [[ -f "${WRAPS_KEYS_DIR}/${f}" ]] || need_extract="true"
    done

    if [[ ! -f "${tarball}" ]]; then
        log "Caching WRAPS v1.0.0 proving key tarball (one-time download, ~2 GB)..."
        mkdir -p "${WRAPS_KEYS_DIR}"
        curl -fSL "${WRAPS_DOWNLOAD_URL}" -o "${tarball}" || fail "Failed to download WRAPS v1.0.0"
        need_extract="true"
    fi
    if [[ "${need_extract}" == "true" ]]; then
        tar -xzf "${tarball}" -C "${WRAPS_KEYS_DIR}" || fail "Failed to extract WRAPS v1.0.0 archive"
    fi
    for f in ${WRAPS_KEY_FILES}; do
        [[ -f "${WRAPS_KEYS_DIR}/${f}" ]] || fail "WRAPS v1.0.0 key ${f} missing after extract"
    done
}

ensure_wraps_keys_cached

IFS=',' read -ra aliases <<< "${NODE_ALIASES}"

log "Stopping consensus nodes (${NODE_ALIASES}) to edit writerMode..."
solo consensus node stop \
    --deployment "${DEPLOYMENT}" \
    --node-aliases "${NODE_ALIASES}" \
    -q \
    || fail "solo consensus node stop failed"

for alias in "${aliases[@]}"; do
    pod="network-${alias}-0"
    log "Setting blockStream.writerMode=GRPC on ${pod}..."
    kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" \
        exec "${pod}" -c root-container -- bash -c \
        "sed -i 's/^blockStream.writerMode=.*/blockStream.writerMode=GRPC/' '${CONFIG_PATH}' && grep '^blockStream.writerMode=' '${CONFIG_PATH}'" \
        || fail "Failed to edit ${CONFIG_PATH} on ${pod}"
done

log "Starting consensus nodes back up..."
solo consensus node start \
    --deployment "${DEPLOYMENT}" \
    --node-aliases "${NODE_ALIASES}" \
    --wraps-key-path "${WRAPS_KEYS_DIR}" \
    -q \
    || fail "solo consensus node start failed"

log "Waiting for CN pods to be Ready again (timeout ${READY_TIMEOUT}s)..."
for alias in "${aliases[@]}"; do
    pod="network-${alias}-0"
    kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" \
        wait --for=condition=Ready "pod/${pod}" --timeout="${READY_TIMEOUT}s" \
        || {
            kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" \
                describe "pod/${pod}" | tail -30 || true
            fail "${pod} did not become Ready after restart"
        }
done

log "All 3 CNs restarted with blockStream.writerMode=GRPC; record-file production stopped."
