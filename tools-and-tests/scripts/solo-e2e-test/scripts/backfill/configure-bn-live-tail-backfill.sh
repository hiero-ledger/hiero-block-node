#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Backfill-with-live-tail E2E (#3053) -- prepare a wiped Block Node so that it
# ingests live blocks from its Consensus Node while it backfills its history.
#
# Two changes to the target BN's "<bn>-config" ConfigMap, which the chart wires
# into the container with `envFrom: configMapRef` (see
# charts/block-node-server/templates/statefulset.yaml), so the values only take
# effect on the next pod start -- hence the scaled-to-zero precondition below.
#
#   1. BLOCK_NODE_EARLIEST_MANAGED_BLOCK = <current network height> + EMB_OFFSET
#
#      On a store-less start the publisher sets its next expected block to the
#      earliestManagedBlock (LiveStreamPublisherManager#initializeBlockNumbers).
#      Left at the default 0, every block the Consensus Node offers is *above*
#      that, so the BN answers BlockNodeBehind and live streaming stays blocked
#      until backfill has walked the whole chain. Raised above the chain head,
#      the first offered block is *below* it and takes the post-restart pre-EMB
#      accept path (LiveStreamPublisherManager#streamBeforeEmbOrElse): live
#      ingest starts at once and everything under the first live block is left
#      as a HISTORICAL gap for backfill to close in parallel.
#
#      The offset has to cover the blocks the network produces between this
#      patch and the pod actually accepting its first block. If the head passes
#      the EMB first, the BN goes back to answering BlockNodeBehind and never
#      starts live ingest -- raise EMB_OFFSET if the assertion times out on a
#      fast-producing network.
#
#   2. Throttled backfill -- smaller batches with a longer pause between them,
#      plus a shorter scan interval so the gap is picked up promptly rather than
#      up to a minute later. This keeps the historical gap open long enough for
#      a sampling assertion to watch it shrink while live blocks are arriving.
#
# The chosen EMB is written to STATE_FILE so
# assert-backfill-during-live-stream.sh can confirm the running pod came up with
# it rather than asserting against a patch that never landed.
#
# Usage:
#     configure-bn-live-tail-backfill.sh <target-bn-index> [reference-bn-index]
#     configure-bn-live-tail-backfill.sh 1 2
#
# Reads:
#   NAMESPACE                       (default "solo-network")
#   CONTEXT                         (default "kind-solo-cluster")
#   EMB_OFFSET                      (default 100)   blocks above the chain head
#   BACKFILL_FETCH_BATCH_SIZE       (default 5)     chart default is 10
#   BACKFILL_DELAY_BETWEEN_BATCHES  (default 2000)  chart default is 1000 ms
#   BACKFILL_SCAN_INTERVAL          (default 15000) chart default is 60000 ms
#   REFERENCE_PROGRESS_WINDOW       (default 15)    seconds used to confirm the
#                                                   reference BN is still ingesting
#   STATE_FILE                      (default /tmp/backfill-live-tail.state)

set -euo pipefail

LOG_PREFIX="backfill-live-tail-config"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source-path=SCRIPTDIR source=common.sh
source "${SCRIPT_DIR}/common.sh"

: "${EMB_OFFSET:=100}"
: "${BACKFILL_FETCH_BATCH_SIZE:=5}"
: "${BACKFILL_DELAY_BETWEEN_BATCHES:=2000}"
: "${BACKFILL_SCAN_INTERVAL:=15000}"
: "${REFERENCE_PROGRESS_WINDOW:=15}"
: "${STATE_FILE:=/tmp/backfill-live-tail.state}"

target_index="${1:?configure-bn-live-tail-backfill.sh: target BN index required}"
reference_index="${2:-2}"

target_bn="block-node-${target_index}"
config_configmap="${target_bn}-config"

kctl() { kubectl --context "${CONTEXT}" --namespace "${NAMESPACE}" "$@"; }

# The env only reaches the JVM on a fresh pod, so patching a running BN would
# silently do nothing and leave the assertion to fail for the wrong reason.
replicas=$(kctl get statefulset "${target_bn}" -o jsonpath='{.spec.replicas}' 2>/dev/null || echo "")
[[ "${replicas}" == "0" ]] ||
    fail "${target_bn} must be scaled to 0 before configuring it (replicas='${replicas:-unknown}')"

network_height=$(read_bn_height "${reference_index}") ||
    fail "could not read the network height from block-node-${reference_index} (is its port-forward up?)"

# The reference BN's height stands in for the chain head, which is only valid
# while that BN is actually ingesting live blocks. A stalled reference yields a
# stale, too-low EMB, the target then answers BlockNodeBehind for every offered
# block, and live ingest never starts -- the assertion would fail several minutes
# later with no hint that the cause was here. Confirm the reference is advancing
# and fail loudly with the real reason if it is not.
sleep "${REFERENCE_PROGRESS_WINDOW}"
network_height_later=$(read_bn_height "${reference_index}") ||
    fail "could not re-read the network height from block-node-${reference_index}"
((network_height_later > network_height)) ||
    fail "block-node-${reference_index} is not ingesting live blocks (height stuck at ${network_height} over ${REFERENCE_PROGRESS_WINDOW}s); its height cannot stand in for the chain head, so no EMB can be chosen"
network_height="${network_height_later}"

earliest_managed_block=$((network_height + EMB_OFFSET))

log "Network height on block-node-${reference_index} is ${network_height} (advancing)."
log "Configuring ${target_bn}: EMB=${earliest_managed_block} (+${EMB_OFFSET}), fetch batch ${BACKFILL_FETCH_BATCH_SIZE}, ${BACKFILL_DELAY_BETWEEN_BATCHES}ms between batches, ${BACKFILL_SCAN_INTERVAL}ms scan interval."

kctl patch configmap "${config_configmap}" --type merge -p "$(
    cat <<EOF
{
  "data": {
    "BLOCK_NODE_EARLIEST_MANAGED_BLOCK": "${earliest_managed_block}",
    "BACKFILL_FETCH_BATCH_SIZE": "${BACKFILL_FETCH_BATCH_SIZE}",
    "BACKFILL_DELAY_BETWEEN_BATCHES": "${BACKFILL_DELAY_BETWEEN_BATCHES}",
    "BACKFILL_SCAN_INTERVAL": "${BACKFILL_SCAN_INTERVAL}"
  }
}
EOF
)" || fail "failed to patch ${config_configmap}"

printf 'earliest_managed_block=%s\nnetwork_height_at_config=%s\n' \
    "${earliest_managed_block}" "${network_height}" > "${STATE_FILE}"

log "${config_configmap} patched; state written to ${STATE_FILE}."
