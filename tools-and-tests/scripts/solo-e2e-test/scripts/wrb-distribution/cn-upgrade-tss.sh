#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# WRB Distribution E2E (#3125 slice 6 — step 11) — upgrade the 3 CNs to a
# TSS/WRAPS-native software version via a real Hedera FREEZE_UPGRADE, using
# Solo's `consensus network upgrade` (the same primitive a real operator uses
# for a mainnet/testnet TSS cutover).
#
# TSS/WRAPS is already enabled at genesis for this suite (deploy_consensus_nodes
# in solo-deploy-network.sh passes --tss true plus the WRAPS v1.0.0 proving
# keys via --wraps-key-path, needed by CN v0.75.x's TSS_LIB_WRAPS_ARTIFACTS_PATH
# loading path — see that script's comment on WRAPS_DOWNLOAD_URL). This step
# upgrades the running network to CN_UPGRADE_VERSION (default "rc", resolved
# to the latest published release-candidate tag — currently v0.77.0-rc.3, but
# this drifts forward as new rc's ship). v0.76 was the first release where the
# WRAPS proving key loads natively at genesis via tss.wrapsProvingKeyPath
# instead of the v0.75.x workaround — i.e. any CN_UPGRADE_VERSION >= v0.76 is
# the real "TSS cutover" issue #3125 step 11 describes, not a fresh TSS
# enablement.
#
# `solo consensus network upgrade` handles the whole sequence itself (prepares
# an upgrade zip, sends the FREEZE_UPGRADE transaction, waits for the freeze,
# restarts each node on the new version) — no separate `consensus network
# freeze` call is needed here.
#
# Usage:
#     cn-upgrade-tss.sh [<upgrade-version>]
#     cn-upgrade-tss.sh rc
#     cn-upgrade-tss.sh v0.76.0-rc.6
#
# Reads:
#   NAMESPACE           (default "solo-network")
#   CLUSTER_REFERENCE   (default "kind-solo-cluster")
#   DEPLOYMENT          (default "deployment-solo")
#   NODE_ALIASES        (default "node1,node2,node3")
#   CN_UPGRADE_VERSION  (default "rc"; overridden by $1 if given)
#   READY_TIMEOUT       (default 300)

set -euo pipefail

: "${NAMESPACE:=solo-network}"
: "${CLUSTER_REFERENCE:=kind-solo-cluster}"
: "${DEPLOYMENT:=deployment-solo}"
: "${NODE_ALIASES:=node1,node2,node3}"
# Default "rc": as of writing, v0.76.0 GA has not been published to
# builds.hedera.com (Solo's actual artifact source for `network upgrade` —
# only v0.76.0-rc.1..rc.6 exist there, confirmed via `solo consensus network
# upgrade --upgrade-version v0.76.0` failing with SOLO-5068 "Upgrade version
# v0.76.0 does not exist"). A hardcoded rc tag would go stale the moment a
# newer rc ships or v0.76.0 GA is finally cut, so this resolves the same way
# resolve-versions.sh resolves the initial CN deploy version, below.
CN_UPGRADE_VERSION="${1:-${CN_UPGRADE_VERSION:-rc}}"
READY_TIMEOUT="${READY_TIMEOUT:-300}"

log() { echo "[wrb-dist-cn-upgrade] $*"; }
fail() { echo "[wrb-dist-cn-upgrade] ERROR: $*" >&2; exit 1; }

# Resolve 'rc'/'latest' keywords to an actual published tag by reusing
# resolve-versions.sh's CN resolution (get_latest_rc_release / get_latest_release)
# instead of duplicating that logic. The dummy v0.1.0 values for mn/bn/relay/tck
# take the "already a valid tag" fast path in resolve_version (no network call),
# so this only costs the one GitHub API lookup this script actually needs.
if [[ "${CN_UPGRADE_VERSION}" == "rc" || "${CN_UPGRADE_VERSION}" == "latest" ]]; then
    RESOLVE_SCRIPT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/resolve-versions.sh"
    [[ -x "${RESOLVE_SCRIPT}" ]] || fail "resolve-versions.sh not found at ${RESOLVE_SCRIPT}"
    resolved=$("${RESOLVE_SCRIPT}" "${CN_UPGRADE_VERSION}" v0.1.0 v0.1.0 v0.1.0 v0.1.0 2>/dev/null | grep '^cn_version=' | cut -d= -f2)
    [[ -n "${resolved}" ]] || fail "Could not resolve CN_UPGRADE_VERSION keyword '${CN_UPGRADE_VERSION}'"
    log "Resolved CN_UPGRADE_VERSION '${CN_UPGRADE_VERSION}' -> ${resolved}"
    CN_UPGRADE_VERSION="${resolved}"
fi

# Same WRAPS v1.0.0 cache location/contents as solo-deploy-network.sh's
# ensure_wraps_keys_cached, duplicated here since this script runs standalone
# as a test-definition command event rather than sourcing that file. If the
# deploy already ran with TSS enabled (this suite's default), this is a cache
# hit and the ~2 GB tarball is not re-downloaded.
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

log "Upgrading consensus network (deployment=${DEPLOYMENT}, nodes=${NODE_ALIASES}) to ${CN_UPGRADE_VERSION}..."
solo consensus network upgrade \
    --deployment "${DEPLOYMENT}" \
    --node-aliases "${NODE_ALIASES}" \
    --upgrade-version "${CN_UPGRADE_VERSION}" \
    --wraps-key-path "${WRAPS_KEYS_DIR}" \
    -q \
    || fail "solo consensus network upgrade failed"

log "Waiting for CN pods to be Ready again (timeout ${READY_TIMEOUT}s)..."
IFS=',' read -ra aliases <<< "${NODE_ALIASES}"
for alias in "${aliases[@]}"; do
    pod="network-${alias}-0"
    kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" \
        wait --for=condition=Ready "pod/${pod}" --timeout="${READY_TIMEOUT}s" \
        || {
            kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" \
                describe "pod/${pod}" | tail -30 || true
            fail "${pod} did not become Ready after the upgrade"
        }
done

log "CN upgrade to ${CN_UPGRADE_VERSION} complete; all nodes Ready."
