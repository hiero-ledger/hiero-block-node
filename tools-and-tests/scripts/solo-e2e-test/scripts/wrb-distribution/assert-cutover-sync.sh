#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# WRB Distribution E2E (#3125 slice 6 — step 12) — after CN record-file
# production is stopped (stop-cn-record-production.sh, simulating cutover),
# verify every BN and MN in the topology eventually converges on (nearly) the
# same last-available block. This proves the whole distribution chain
# (CN -> BN3 -> [backfill] -> BN1/BN2 -> MN1/MN2, plus the wrb-cli live-push
# -> BN1 path) keeps blocks flowing end to end after cutover, with no BN or MN
# left behind.
#
# Reads:
#   PROTO_PATH      (required — see assert-bn-tss-data.sh)
#   POLL_INTERVAL   (default 20 seconds)
#   POLL_TIMEOUT    (default 300 seconds)
#   CONVERGE_WINDOW (default 2 — max allowed spread between the highest and
#                    lowest last_available_block once polling settles. A live
#                    network keeps producing blocks during the poll itself, so
#                    a byte-for-byte match across 5 nodes queried sequentially
#                    isn't realistic; a small spread is.)

set -euo pipefail

: "${POLL_INTERVAL:=20}"
: "${POLL_TIMEOUT:=300}"
: "${CONVERGE_WINDOW:=2}"

log() { echo "[wrb-dist-cutover-assert] $*"; }
fail() { echo "[wrb-dist-cutover-assert] ERROR: $*" >&2; exit 1; }

[[ -n "${PROTO_PATH:-}" && -d "${PROTO_PATH}" ]] || fail "PROTO_PATH not set or not a directory (got '${PROTO_PATH:-}')"
command -v grpcurl >/dev/null 2>&1 || fail "grpcurl not found"
command -v jq >/dev/null 2>&1 || fail "jq not found"

bn_last_block() {
    local port="$1"
    grpcurl -plaintext -emit-defaults \
        -import-path "${PROTO_PATH}" \
        -proto block-node/api/node_service.proto \
        -d '{}' "localhost:${port}" \
        org.hiero.block.api.BlockNodeService/serverStatus 2>/dev/null \
        | jq -r '.lastAvailableBlock // empty' 2>/dev/null || true
}

mn_last_block() {
    local port="$1"
    curl -s --max-time 10 "http://127.0.0.1:${port}/api/v1/blocks?limit=1&order=desc" 2>/dev/null \
        | jq -r '.blocks[0].number // empty' 2>/dev/null || true
}

names=(block-node-1 block-node-2 block-node-3 mirror-1 mirror-2)
ports=(40840 40841 40842 5551 5552)
kinds=(bn bn bn mn mn)

elapsed=0
while (( elapsed < POLL_TIMEOUT )); do
    values=()
    all_present="true"
    for i in "${!names[@]}"; do
        if [[ "${kinds[$i]}" == "bn" ]]; then
            v=$(bn_last_block "${ports[$i]}")
        else
            v=$(mn_last_block "${ports[$i]}")
        fi
        if [[ -z "${v}" || "${v}" == "null" ]]; then
            all_present="false"
            break
        fi
        values+=("${v}")
    done

    if [[ "${all_present}" == "true" ]]; then
        summary=""
        min="${values[0]}"
        max="${values[0]}"
        for i in "${!names[@]}"; do
            summary="${summary}${names[$i]}=${values[$i]} "
            (( values[i] < min )) && min="${values[i]}"
            (( values[i] > max )) && max="${values[i]}"
        done
        log "Last available block per node: ${summary}"

        if (( max - min <= CONVERGE_WINDOW )); then
            log "All BNs and MNs converged within ${CONVERGE_WINDOW} block(s) (min=${min}, max=${max}). Cutover sync confirmed."
            exit 0
        fi
        log "Spread is $(( max - min )) block(s) (> ${CONVERGE_WINDOW}); retrying in ${POLL_INTERVAL}s..."
    else
        log "Not all nodes reported a last-available-block yet; retrying in ${POLL_INTERVAL}s..."
    fi

    sleep "${POLL_INTERVAL}"
    elapsed=$(( elapsed + POLL_INTERVAL ))
done

fail "BNs/MNs did not converge within ${POLL_TIMEOUT}s (last window > ${CONVERGE_WINDOW} block(s) apart, or a node never reported)"
