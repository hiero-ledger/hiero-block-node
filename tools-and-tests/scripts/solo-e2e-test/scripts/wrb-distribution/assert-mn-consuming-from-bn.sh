#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# WRB Distribution E2E (#3125 slice 4) — assert that a Mirror Node's importer
# is consuming blocks from a Block Node source. Applies to:
#   * mirror-1 (step 6) after reconfigure-mn1-to-bn-sources.sh flips
#     HIERO_MIRROR_IMPORTER_BLOCK_ENABLED=true and points at BN2/BN3.
#   * mirror-2 (step 7) after add-mn2.sh installs a fresh MN with block:
#     enabled: true / sourceType: BLOCK_NODE / startBlockNumber: 0.
#
# Assertion signal: the importer container's logs must contain evidence of
# a live block-stream subscription attempt to a Block Node. Patterns fall
# into two tiers:
#
# Positive-ingest tier (BN served blocks):
#   * "SubscribeBlockStream"  — importer's outgoing gRPC method call
#   * "Received block"        — importer reports blocks arriving from BN
#   * "block[- ]?stream.*subscri"  — chart client-lib log line
#
# Polling tier (MN actively contacting BN, whether BN had the block or not):
#   * "BlockNodeSubscriber"                        — the importer's BN subscription client
#   * "CompositeBlockSource.*BLOCK_NODE"           — composite source dispatched to BN
#   * "block node can provide block"               — BN answered "don't have it"; still proves the round-trip
#
# The polling tier is included because slice 4's Solo topology has no
# recordstream-bucket backfill on the BN side — BNs came up after CN was
# already producing, so their live-streamed window never overlaps the
# blocks MN1 has already ingested nor block 0 that MN2 asks for. What we
# want to verify is that the reconfigure / fresh-install correctly wires
# MN to BN; the BN-side data gap is not what slice 4 is testing.
# We also require the pod to be Ready before we trust its log content, so a
# CrashLooping importer whose env dump mentioned BN hosts doesn't count.
#
# Usage:
#     assert-mn-consuming-from-bn.sh <mirror-name>
#     assert-mn-consuming-from-bn.sh mirror-1
#     assert-mn-consuming-from-bn.sh mirror-2
#
# Reads:
#   NAMESPACE         (default "solo-network")
#   CLUSTER_REFERENCE (default "kind-solo-cluster")
#   POLL_WINDOW       (default 720 — seconds to wait for BN activity to appear)
#   POLL_INTERVAL     (default 10)
#
# POLL_WINDOW was bumped twice: 240 -> 480 (MN1 needed ~5-6min with its
# existing DB) and then 480 -> 720 (MN2's from-scratch install runs the
# full Flyway migration set before subscribing and needed ~8min in
# slice-4 CI to produce its first CompositeBlockSource BLOCK_NODE log
# line -- again ~40ms after the 480s window elapsed). 720s (12min) gives
# fresh-install MN comfortable headroom for full migration + subscribe +
# multiple polling attempts.

set -euo pipefail

: "${NAMESPACE:=solo-network}"
: "${CLUSTER_REFERENCE:=kind-solo-cluster}"
POLL_WINDOW="${POLL_WINDOW:-720}"
POLL_INTERVAL="${POLL_INTERVAL:-10}"

mirror_name="${1:?assert-mn-consuming-from-bn.sh: mirror name required (e.g. mirror-1, mirror-2)}"
importer_deployment="${mirror_name}-importer"

log() { echo "[wrb-dist-mn-bn-assert] $*"; }
fail() { echo "[wrb-dist-mn-bn-assert] ERROR: $*" >&2; exit 1; }

log "Waiting up to ${POLL_WINDOW}s for ${importer_deployment} to show BN-source activity..."

deadline=$(($(date +%s) + POLL_WINDOW))
while [[ $(date +%s) -lt ${deadline} ]]; do
    # Try to find the importer pod (name changes across rollouts).
    pod=$(kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" \
        get pods -l "app.kubernetes.io/name=importer,app.kubernetes.io/instance=${mirror_name}" \
        -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)

    if [[ -z "${pod}" ]]; then
        # Fallback selector: some Solo chart variants use different labels.
        pod=$(kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" \
            get pods 2>/dev/null | awk -v m="${importer_deployment}" '$1 ~ m {print $1; exit}' || true)
    fi

    if [[ -n "${pod}" ]]; then
        # Require pod Ready before trusting log content — otherwise a
        # CrashLooping importer whose env dump mentioned BN hosts would
        # produce a false-positive pass.
        pod_ready=$(kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" \
            get pod "${pod}" \
            -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || true)
        if [[ "${pod_ready}" == "True" ]] && kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" \
            logs "${pod}" --tail=2000 2>/dev/null \
            | grep -q -E "SubscribeBlockStream|Received block|block[- ]?stream.*subscri|BlockNodeSubscriber|CompositeBlockSource.*BLOCK_NODE|block node can provide block"; then
            log "${importer_deployment} (pod ${pod}) log shows live BN-stream activity."
            log "Sample match:"
            kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" \
                logs "${pod}" --tail=2000 2>/dev/null \
                | grep -E "SubscribeBlockStream|Received block|block[- ]?stream.*subscri|BlockNodeSubscriber|CompositeBlockSource.*BLOCK_NODE|block node can provide block" \
                | head -5 | sed 's/^/    /'
            exit 0
        fi
    fi

    sleep "${POLL_INTERVAL}"
done

log "No BN-source activity observed in ${importer_deployment} logs within ${POLL_WINDOW}s."
kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" \
    get pods -l "app.kubernetes.io/instance=${mirror_name}" -o wide 2>/dev/null || true
kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" \
    logs "${pod:-unknown}" --tail=100 2>/dev/null | tail -40 | sed 's/^/    /' || true
fail "${importer_deployment}: no BN-source activity detected"
