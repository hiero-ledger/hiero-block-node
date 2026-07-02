#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# WRB Distribution E2E (#3125 slice 4 — step 7) — install MN2 (mirror-2)
# post-hoc via `solo mirror node add`, applying overrides/wrb-distribution-step67/
# mn2-values.yaml so the new MN pulls blocks from BN2 and BN3 with
# startBlockNumber=0 (genesis) and no recordstream-bucket downloader.
#
# Reads (with the harness's defaults as fallback):
#   DEPLOYMENT        (default "deployment-solo")
#   NAMESPACE         (default "solo-network")
#   CLUSTER_REFERENCE (default "kind-solo-cluster")
#   TOPOLOGY          (used to locate overrides/${TOPOLOGY}/mn2-values.yaml)
#   MN_VERSION        (optional — passed through as --mirror-node-version)
#
# Assertions on success:
#   * `solo mirror node add` exit code is 0
#   * A mirror-2-* pod exists and is Ready within MN_READY_TIMEOUT seconds
#
# Naming: Solo derives the MN name from the deployment context. Since mirror-1
# already exists, the second `solo mirror node add` call is expected to install
# as mirror-2. If Solo's behaviour turns out to require an explicit name flag,
# adjust this script (Solo CLI does not expose --name on this subcommand as of
# 0.79.x).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOLO_E2E_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

: "${DEPLOYMENT:=deployment-solo}"
: "${NAMESPACE:=solo-network}"
: "${CLUSTER_REFERENCE:=kind-solo-cluster}"
: "${TOPOLOGY:=wrb-distribution-step67}"
MN_READY_TIMEOUT="${MN_READY_TIMEOUT:-600}"

log() { echo "[wrb-dist-add-mn2] $*"; }
fail() { echo "[wrb-dist-add-mn2] ERROR: $*" >&2; exit 1; }

values_overlay="${SOLO_E2E_ROOT}/overrides/${TOPOLOGY}/mn2-values.yaml"
if [[ ! -f "${values_overlay}" ]]; then
    fail "MN2 values overlay not found: ${values_overlay#${SOLO_E2E_ROOT}/}"
fi
log "Applying overlay: overrides/${TOPOLOGY}/mn2-values.yaml"

version_args=""
if [[ -n "${MN_VERSION:-}" ]]; then
    version_args="--mirror-node-version ${MN_VERSION}"
fi

log "Adding MN2 to deployment=${DEPLOYMENT} cluster-ref=${CLUSTER_REFERENCE}..."
# shellcheck disable=SC2086
solo mirror node add \
    --deployment "${DEPLOYMENT}" \
    --cluster-ref "${CLUSTER_REFERENCE}" \
    --pinger \
    --enable-ingress \
    ${version_args} \
    -f "${values_overlay}" \
    || fail "solo mirror node add failed for MN2"

log "Waiting for mirror-2 pods Ready (timeout ${MN_READY_TIMEOUT}s)..."
kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" \
    wait --for=condition=Ready pod \
    -l "app.kubernetes.io/instance=mirror-2" \
    --timeout="${MN_READY_TIMEOUT}s" \
    || {
        kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" \
            get pods -l "app.kubernetes.io/instance=mirror-2" -o wide || true
        kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" \
            describe pods -l "app.kubernetes.io/instance=mirror-2" | tail -60 || true
        fail "mirror-2 pods did not become Ready within ${MN_READY_TIMEOUT}s"
    }

log "MN2 is Ready."
