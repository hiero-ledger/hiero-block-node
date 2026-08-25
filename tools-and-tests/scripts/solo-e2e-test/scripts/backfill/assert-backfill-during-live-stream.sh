#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Backfill-with-live-tail E2E (#3053) -- assert that the recovering Block Node is
# closing its historical gap at the same time as it is accepting live blocks from
# its Consensus Node.
#
# "At the same time" is the whole point, so a pass requires every one of these to
# hold across a single sampling window rather than merely at some point in the
# run:
#
#   * publisher_open_connections >= 1 -- a Consensus Node is connected
#   * availableRanges holds a range starting at block 0 *and* a separate later
#     range -- the historical gap is still open, so none of this is
#     post-recovery noise
#   * the end of the range starting at 0 grew -- backfill is closing the gap
#   * the end of the last range grew -- live blocks are still landing
#
# Ranges come from BlockNodeService/serverStatusDetail. A node recovering with
# its earliestManagedBlock above the chain head reports exactly two, e.g.
# [{0,120},{648,712}]: history being refilled from genesis, and the live tail.
#
# The window is retried until CONCURRENCY_TIMEOUT because when live ingest starts
# depends on when the Consensus Node reconnects, which the test cannot schedule.
#
# Usage:
#     assert-backfill-during-live-stream.sh <target-bn-index>
#     assert-backfill-during-live-stream.sh 1
#
# Reads:
#   NAMESPACE      (default "solo-network")
#   CONTEXT        (default "kind-solo-cluster")
#   PROTO_PATH     (defaults to whichever of protobuf-sources/proto or
#                  protobuf-sources/build/proto exists -- CI untars the former,
#                  the Taskfile builds the latter)
#   SAMPLE_WINDOW  (default 20)  seconds between the two samples of a window
#   CONCURRENCY_TIMEOUT   (default 300) seconds before giving up
#   STATE_FILE     (default /tmp/backfill-live-tail.state)
#
# grpcurl needs -import-path/-proto explicitly: the Block Node's gRPC server does
# not expose reflection, so a plain `-d '{}' host:port service/method` call
# silently returns nothing to parse.

set -euo pipefail

LOG_PREFIX="backfill-live-tail-assert"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../../../.." && pwd)"
# shellcheck source-path=SCRIPTDIR source=common.sh
source "${SCRIPT_DIR}/common.sh"

: "${SAMPLE_WINDOW:=20}"
: "${CONCURRENCY_TIMEOUT:=300}"
: "${STATE_FILE:=/tmp/backfill-live-tail.state}"

target_index="${1:?assert-backfill-during-live-stream.sh: target BN index required}"
target_bn="block-node-${target_index}"
grpc_port=$(bn_grpc_port "${target_index}")

command -v grpcurl >/dev/null 2>&1 || fail "grpcurl not on PATH; cannot query serverStatusDetail"
command -v jq >/dev/null 2>&1 || fail "jq not on PATH"

if [[ -z "${PROTO_PATH:-}" || ! -d "${PROTO_PATH}" ]]; then
    for candidate in "${REPO_ROOT}/protobuf-sources/proto" "${REPO_ROOT}/protobuf-sources/build/proto"; do
        if [[ -d "${candidate}" ]]; then
            PROTO_PATH="${candidate}"
            break
        fi
    done
fi
[[ -d "${PROTO_PATH:-}" ]] || fail "PROTO_PATH not set and no protobuf sources found under ${REPO_ROOT}/protobuf-sources"

# Confirm the pod really came up with the earliestManagedBlock the configure step
# patched in. Without that, live ingest could not have started and every failure
# below would be reported against the wrong cause.
[[ -f "${STATE_FILE}" ]] || fail "state file ${STATE_FILE} not found; did configure-bn-live-tail-backfill.sh run?"
# shellcheck source=/dev/null
source "${STATE_FILE}"
: "${earliest_managed_block:?state file did not set earliest_managed_block}"

actual_emb=$(kubectl --context "${CONTEXT}" --namespace "${NAMESPACE}" \
    exec "${target_bn}-0" -c block-node-server -- \
    printenv BLOCK_NODE_EARLIEST_MANAGED_BLOCK 2>/dev/null | tr -d '[:space:]' || echo "")
[[ "${actual_emb}" == "${earliest_managed_block}" ]] ||
    fail "${target_bn} came up with EMB='${actual_emb:-unset}', expected '${earliest_managed_block}'"
log "${target_bn} is running with EMB=${actual_emb} (network head was ${network_height_at_config:-unknown} when configured)."

# Echoes "<range-count> <end-of-range-starting-at-0> <end-of-last-range> <ranges-json>".
# The second field is "-" when the node holds no range starting at block 0, i.e.
# backfill has not landed anything from genesis yet.
sample_ranges() {
    local ranges
    ranges=$(grpcurl -plaintext -emit-defaults \
        -import-path "${PROTO_PATH}" \
        -proto block-node/api/node_service.proto \
        -d '{}' "localhost:${grpc_port}" \
        org.hiero.block.api.BlockNodeService/serverStatusDetail 2>/dev/null |
        jq -c '[.availableRanges[]? | {start: (.rangeStart | tonumber), end: (.rangeEnd | tonumber)}]') || return 1
    [[ -n "${ranges}" && "${ranges}" != "null" ]] || return 1
    echo "${ranges}" | jq -r '
        (length | tostring) + " "
        + (if (length > 0 and .[0].start == 0) then (.[0].end | tostring) else "-" end) + " "
        + (if (length > 0) then (.[-1].end | tostring) else "-" end) + " "
        + tojson'
}

log "Sampling ${target_bn} in ${SAMPLE_WINDOW}s windows for up to ${CONCURRENCY_TIMEOUT}s..."

deadline=$(($(date +%s) + CONCURRENCY_TIMEOUT))
last_reason="no sample taken yet"
while true; do
    first=$(sample_ranges) || first=""
    connections_before=$(read_bn_metric_int "${target_index}" "blocknode_publisher_open_connections") || connections_before=0
    sleep "${SAMPLE_WINDOW}"
    second=$(sample_ranges) || second=""
    connections_after=$(read_bn_metric_int "${target_index}" "blocknode_publisher_open_connections") || connections_after=0

    if [[ -z "${first}" || -z "${second}" ]]; then
        last_reason="serverStatusDetail returned no available ranges"
    else
        read -r count_before historical_before live_before ranges_before <<< "${first}"
        read -r count_after historical_after live_after ranges_after <<< "${second}"
        log "  ${ranges_before} -> ${ranges_after} (publisher connections ${connections_before} -> ${connections_after})"

        if ((connections_after < 1)); then
            last_reason="no publisher connected to ${target_bn} (open connections ${connections_after})"
        elif ((count_before < 2)); then
            # Naming the sample matters: reporting the *after* ranges while complaining about
            # the *before* one reads as a contradiction when the after-sample does hold a gap.
            last_reason="${target_bn} held no historical gap at the start of this window, so there was nothing to watch close (ranges ${ranges_before})"
        elif ((count_after < 2)); then
            last_reason="${target_bn} no longer holds two separate ranges; the gap either closed or never opened (ranges ${ranges_after})"
        elif [[ "${historical_before}" == "-" || "${historical_after}" == "-" ]]; then
            last_reason="${target_bn} holds no range starting at block 0; backfill has not landed any history yet"
        elif ((historical_after <= historical_before)); then
            last_reason="historical range did not grow (end stayed at ${historical_after}); backfill is not closing the gap"
        elif ((live_after <= live_before)); then
            last_reason="live range did not grow (end stayed at ${live_after}); no live blocks arriving from the Consensus Node"
        else
            log "OK: ${target_bn} closed history 0..${historical_before} -> 0..${historical_after} (+$((historical_after - historical_before)) blocks) while the live tail advanced ${live_before} -> ${live_after} (+$((live_after - live_before)) blocks) over ${SAMPLE_WINDOW}s, with ${connections_after} publisher connection(s)."
            exit 0
        fi
    fi

    (($(date +%s) < deadline)) || break
    log "  not yet concurrent: ${last_reason}"
done

log "Last observed state: ${ranges_after:-unavailable}"
fail "${target_bn} did not backfill history while ingesting live blocks within ${CONCURRENCY_TIMEOUT}s: ${last_reason}"
