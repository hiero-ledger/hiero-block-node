#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# WRB Distribution E2E (#3125 slice 6 — step 11) — assert that a Block Node's
# serverStatusDetail reports populated TSS data (ledger id, WRAPS verification
# key, and a non-empty TSS roster) after cn-upgrade-tss.sh cuts the network
# over to a TSS/WRAPS-native CN version.
#
# Per the issue, BN1 is the primary check (it receives blocks via the
# wrb-cli live-push loop started in step 9); BN2 and BN3 are asserted
# separately with a longer poll window ("eventually" in the issue text),
# since they only see new TSS-bearing blocks once BN-to-BN backfill (step 10)
# or the CN's direct stream to BN3 (step 8) propagates them.
#
# Usage:
#     assert-bn-tss-data.sh <bn-index> [<bn-index> ...]
#     assert-bn-tss-data.sh 1
#     assert-bn-tss-data.sh 2 3
#
# Reads:
#   PROTO_PATH     (required — path to extracted protobuf-sources; see
#                   solo-test-runner.sh --proto-path / Taskfile proto:extract)
#   POLL_INTERVAL  (default 15 seconds between retries)
#   POLL_TIMEOUT   (default 120 seconds total per target BN; bump this via env
#                   for the BN2/BN3 "eventually" case)

set -euo pipefail

: "${POLL_INTERVAL:=15}"
: "${POLL_TIMEOUT:=120}"

[[ $# -ge 1 ]] || { echo "assert-bn-tss-data.sh: at least one BN index required (e.g. 1 or 2 3)" >&2; exit 1; }

log() { echo "[wrb-dist-bn-tss-assert] $*"; }
fail() { echo "[wrb-dist-bn-tss-assert] ERROR: $*" >&2; exit 1; }

[[ -n "${PROTO_PATH:-}" && -d "${PROTO_PATH}" ]] || fail "PROTO_PATH not set or not a directory (got '${PROTO_PATH:-}')"
command -v grpcurl >/dev/null 2>&1 || fail "grpcurl not found"
command -v jq >/dev/null 2>&1 || fail "jq not found"

fetch_bn_status_detail() {
    local port="$1"
    grpcurl -plaintext -emit-defaults \
        -import-path "${PROTO_PATH}" \
        -proto block-node/api/node_service.proto \
        -d '{}' \
        "localhost:${port}" \
        org.hiero.block.api.BlockNodeService/serverStatusDetail 2>/dev/null || true
}

has_tss_data() {
    local resp="$1"
    [[ -n "${resp}" ]] || return 1
    echo "${resp}" | jq -e '
        (.tssData.ledgerId // "") != "" and
        (.tssData.wrapsVerificationKey // "") != "" and
        ((.tssData.currentRoster.rosterEntries // []) | length > 0)
    ' >/dev/null 2>&1
}

failures=0
for bn_index in "$@"; do
    grpc_port=$((40839 + bn_index))
    bn_name="block-node-${bn_index}"

    log "Polling ${bn_name} (localhost:${grpc_port}) for TSS data (timeout ${POLL_TIMEOUT}s)..."
    elapsed=0
    last_resp=""
    ok="false"
    while (( elapsed < POLL_TIMEOUT )); do
        last_resp=$(fetch_bn_status_detail "${grpc_port}")
        if has_tss_data "${last_resp}"; then
            ok="true"
            break
        fi
        sleep "${POLL_INTERVAL}"
        elapsed=$(( elapsed + POLL_INTERVAL ))
    done

    if [[ "${ok}" == "true" ]]; then
        log "${bn_name}: serverStatusDetail reports populated TSS data ✓"
    else
        log "${bn_name}: TSS data not populated after ${POLL_TIMEOUT}s. Last response:"
        echo "${last_resp}" | sed 's/^/    /' || true
        failures=$(( failures + 1 ))
    fi
done

if (( failures > 0 )); then
    fail "${failures} BN(s) did not report TSS data within the poll window"
fi

log "All target BN(s) report populated TSS data."
