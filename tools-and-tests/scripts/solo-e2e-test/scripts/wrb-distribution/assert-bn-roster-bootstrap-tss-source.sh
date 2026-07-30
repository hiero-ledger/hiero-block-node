#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# WRB Distribution E2E (#3125 slice 6) — assert that a Block Node has been
# reconfigured (by reconfigure-bn-roster-bootstrap-tss.sh) to pull TssData
# from its configured peer(s) via the roster-bootstrap-tss plugin.
#
# What this asserts, per target BN:
#   1. ROSTER_BOOTSTRAP_TSS_BLOCK_NODE_SOURCES_PATH is set on the running
#      container to the expected mount path (proves the "-config" ConfigMap
#      merge landed and the pod picked it up).
#   2. The file at that path (mounted from the
#      "-roster-bootstrap-tss-sources" ConfigMap) contains every expected
#      peer's address (proves the volume + volumeMount patch landed, not
#      just the env var).
#
# This only confirms the plugin is CONFIGURED to query its peers — it does
# not confirm TssData has actually propagated yet. See assert-bn-tss-data.sh
# for that (functional) check.
#
# Usage:
#     assert-bn-roster-bootstrap-tss-source.sh <target-bn-index> <peer-bn-index> [<peer-bn-index> ...]
#     assert-bn-roster-bootstrap-tss-source.sh 1 2 3
#
# Reads:
#   NAMESPACE          (default "solo-network")
#   CLUSTER_REFERENCE  (default "kind-solo-cluster")
#
# Each peer BN's gRPC port is read from its own "-config" ConfigMap
# (SERVER_PORT), falling back to 40840 only if that key is absent — same
# derivation as reconfigure-bn-roster-bootstrap-tss.sh, so this checks
# against the port that was actually written to the sources file.

set -euo pipefail

: "${NAMESPACE:=solo-network}"
: "${CLUSTER_REFERENCE:=kind-solo-cluster}"

target_index="${1:?assert-bn-roster-bootstrap-tss-source.sh: target BN index required (e.g. 1)}"
shift
[[ $# -ge 1 ]] || { echo "assert-bn-roster-bootstrap-tss-source.sh: at least one peer BN index required (e.g. 2 3)" >&2; exit 1; }

log() { echo "[wrb-dist-bn-roster-tss-assert] $*"; }
fail() { echo "[wrb-dist-bn-roster-tss-assert] ERROR: $*" >&2; exit 1; }

target_bn="block-node-${target_index}"
pod="${target_bn}-0"
expected_path="/opt/hiero/block-node/roster-bootstrap-tss/block-node-sources.json"

actual_path=$(kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" \
    exec "${pod}" -c block-node-server -- \
    printenv ROSTER_BOOTSTRAP_TSS_BLOCK_NODE_SOURCES_PATH 2>/dev/null | tr -d '[:space:]' || echo "")

if [[ "${actual_path}" != "${expected_path}" ]]; then
    fail "${target_bn}: ROSTER_BOOTSTRAP_TSS_BLOCK_NODE_SOURCES_PATH='${actual_path:-<unset>}', expected '${expected_path}'"
fi

sources_content=$(kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" \
    exec "${pod}" -c block-node-server -- \
    cat "${expected_path}" 2>/dev/null || echo "")

failures=0
for peer_index in "$@"; do
    peer_bn="block-node-${peer_index}"
    peer_dns="${peer_bn}.${NAMESPACE}.svc.cluster.local"
    peer_port=$(kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" \
        get configmap "${peer_bn}-config" -o jsonpath='{.data.SERVER_PORT}' 2>/dev/null || echo "")
    : "${peer_port:=40840}"

    # Exact address+port match via jq, not a substring check — a substring match
    # on peer_dns would prefix-match e.g. block-node-2 against block-node-20
    # once the suite scales past 9 BNs.
    if ! echo "${sources_content}" | jq -e --arg addr "${peer_dns}" --argjson port "${peer_port}" \
        '.nodes[]? | select(.address == $addr and .port == $port)' >/dev/null 2>&1; then
        log "${target_bn}: sources file does not reference ${peer_dns}:${peer_port}:"
        log "  ${sources_content}"
        failures=$(( failures + 1 ))
        continue
    fi
    log "${target_bn}: roster-bootstrap-tss peer = ${peer_dns}:${peer_port} ✓"
done

if (( failures > 0 )); then
    fail "${target_bn}: ${failures} expected peer(s) missing from the roster-bootstrap-tss sources file"
fi

log "${target_bn} is configured to pull TssData from: $*"
