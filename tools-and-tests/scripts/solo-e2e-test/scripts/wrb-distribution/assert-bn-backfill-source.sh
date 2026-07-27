#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# WRB Distribution E2E (#3125 slice 5) — assert that a Block Node has been
# reconfigured (by reconfigure-bn-backfill.sh) to use another BN as its
# backfill source.
#
# What this asserts, per target BN:
#   1. BACKFILL_BLOCK_NODE_SOURCES_PATH is set on the running container to
#      the expected mount path (proves the "-config" ConfigMap merge landed
#      and the pod picked it up).
#   2. The file at that path (mounted from the "-block-node-sources"
#      ConfigMap) contains the expected source BN's address (proves the
#      volume + volumeMount patch landed, not just the env var).
#
# Usage:
#     assert-bn-backfill-source.sh <target-bn-index> [<target-bn-index> ...]
#     assert-bn-backfill-source.sh 2 3
#
# Reads:
#   NAMESPACE          (default "solo-network")
#   CLUSTER_REFERENCE  (default "kind-solo-cluster")
#   SOURCE_BN_INDEX    (default 1 — must match reconfigure-bn-backfill.sh)

set -euo pipefail

: "${NAMESPACE:=solo-network}"
: "${CLUSTER_REFERENCE:=kind-solo-cluster}"
: "${SOURCE_BN_INDEX:=1}"

[[ $# -ge 1 ]] || { echo "assert-bn-backfill-source.sh: at least one target BN index required (e.g. 2 3)" >&2; exit 1; }

log() { echo "[wrb-dist-bn-backfill-assert] $*"; }
fail() { echo "[wrb-dist-bn-backfill-assert] ERROR: $*" >&2; exit 1; }

source_bn="block-node-${SOURCE_BN_INDEX}"
source_dns="${source_bn}.${NAMESPACE}.svc.cluster.local"
expected_path="/opt/hiero/block-node/backfill/block-node-sources.json"

failures=0
for target_index in "$@"; do
    target_bn="block-node-${target_index}"
    pod="${target_bn}-0"

    actual_path=$(kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" \
        exec "${pod}" -c block-node-server -- \
        printenv BACKFILL_BLOCK_NODE_SOURCES_PATH 2>/dev/null | tr -d '[:space:]' || echo "")

    if [[ "${actual_path}" != "${expected_path}" ]]; then
        log "${target_bn}: BACKFILL_BLOCK_NODE_SOURCES_PATH='${actual_path:-<unset>}', expected '${expected_path}'"
        failures=$(( failures + 1 ))
        continue
    fi

    sources_content=$(kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" \
        exec "${pod}" -c block-node-server -- \
        cat "${expected_path}" 2>/dev/null || echo "")

    if [[ "${sources_content}" != *"${source_dns}"* ]]; then
        log "${target_bn}: sources file does not reference ${source_dns}:"
        log "  ${sources_content}"
        failures=$(( failures + 1 ))
        continue
    fi

    log "${target_bn}: backfill source = ${source_dns} ✓"
done

if (( failures > 0 )); then
    fail "${failures} BN(s) not correctly configured for backfill from ${source_bn}"
fi

log "All target BNs are configured to backfill from ${source_bn}."
