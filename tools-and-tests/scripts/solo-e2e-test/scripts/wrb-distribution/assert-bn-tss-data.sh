#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# WRB Distribution E2E (#3125 slice 6 — step 11) — assert that a Block Node's
# serverStatusDetail reports populated TSS data (ledger id, WRAPS verification
# key, and a non-empty TSS roster) after cn-upgrade-tss.sh cuts the network
# over to a TSS/WRAPS-native CN version.
#
# TssData is only ever extracted by the block-verification plugin's
# BlockHasher while processing block number 0 (see BlockHasher.java's
# blockNumber == 0 check) — it is not a general "TSS is active" signal, and it
# is never populated for a block that arrives through a path that skips
# verification. In this suite BN1's copy of block 0 was written directly into
# historic storage by bulk-load-historical-to-bn1.sh (picked up by
# BlockFileHistoricPlugin's startup scan), which never runs the verification
# pipeline, so BN1 can't extract TssData itself this way. BN2 and BN3 instead
# backfill block 0 from BN1 (reconfigure-bn-backfill.sh, step 10); backfilled
# blocks DO go through the same BlockHasher/VerificationServicePlugin
# pipeline as live-published ones (confirmed via VerificationServicePluginTest
# / BlockHasherTest, which parametrize identically over BlockSource.PUBLISHER
# and BlockSource.BACKFILL), so each independently extracts its own TssData
# from that block regardless of BN1's own state. BN1 is instead configured
# (reconfigure-bn-roster-bootstrap-tss.sh, also step 10) to pull TssData from
# BN2/BN3 via the roster-bootstrap-tss plugin's periodic peer-gossip poll, so
# it does eventually populate too — just strictly after BN2/BN3 do. Callers
# should check BN2/BN3 before BN1 — see wrb-distribution-steps1-12.yaml's
# step-11 comment for the full reasoning.
#
# Usage:
#     assert-bn-tss-data.sh <bn-index> [<bn-index> ...]
#     assert-bn-tss-data.sh 2 3
#     assert-bn-tss-data.sh 1
#
# Reads:
#   PROTO_PATH     (required — path to extracted protobuf-sources; see
#                   solo-test-runner.sh --proto-path / Taskfile proto:extract)
#   POLL_INTERVAL  (default 15 seconds between retries)
#   POLL_TIMEOUT   (default 120 seconds total per target BN; bump this via env
#                   since backfill needs time to walk all the way back to
#                   block 0 before BN2/BN3 can extract TssData, and BN1 in
#                   turn needs a roster-bootstrap-tss poll cycle after that)

set -euo pipefail

: "${POLL_INTERVAL:=15}"
: "${POLL_TIMEOUT:=120}"

[[ $# -ge 1 ]] || { echo "assert-bn-tss-data.sh: at least one BN index required (e.g. 1 or 2 3)" >&2; exit 1; }

log() { echo "[wrb-dist-bn-tss-assert] $*"; }
fail() { echo "[wrb-dist-bn-tss-assert] ERROR: $*" >&2; exit 1; }

[[ -n "${PROTO_PATH:-}" && -d "${PROTO_PATH}" ]] || fail "PROTO_PATH not set or not a directory (got '${PROTO_PATH:-}')"
command -v grpcurl >/dev/null 2>&1 || fail "grpcurl not found"
command -v jq >/dev/null 2>&1 || fail "jq not found"

fetch_bn_status_detail_raw() {
    local port="$1"
    # Captured separately from stderr so a connection failure (grpcurl exits
    # non-zero, empty stdout) can be told apart from "reachable but empty
    # response" — both look identical if stderr is discarded.
    grpcurl -plaintext -emit-defaults \
        -import-path "${PROTO_PATH}" \
        -proto block-node/api/node_service.proto \
        -d '{}' \
        "localhost:${port}" \
        org.hiero.block.api.BlockNodeService/serverStatusDetail
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

# One-line classification of a poll attempt, logged every attempt (not just
# on final failure) so a long poll window shows visible progress instead of
# minutes of silence that read as a hang.
classify_attempt() {
    local resp="$1" err="$2"
    if [[ -z "${resp}" ]]; then
        echo "UNREACHABLE (grpcurl: $(echo "${err}" | tr '\n' ' ' | head -c 200))"
        return
    fi
    local range_count tss_present
    range_count=$(echo "${resp}" | jq -r '(.availableRanges // []) | length' 2>/dev/null || echo "?")
    if has_tss_data "${resp}"; then
        echo "OK (availableRanges=${range_count} entries, tssData populated)"
    else
        tss_present=$(echo "${resp}" | jq -r 'if .tssData == null then "null" else "present-but-incomplete" end' 2>/dev/null || echo "?")
        echo "reachable, tssData=${tss_present}, availableRanges=${range_count} entries"
    fi
}

failures=0
for bn_index in "$@"; do
    grpc_port=$((40839 + bn_index))
    bn_name="block-node-${bn_index}"

    log "Polling ${bn_name} (localhost:${grpc_port}) for TSS data (timeout ${POLL_TIMEOUT}s, every ${POLL_INTERVAL}s)..."
    elapsed=0
    attempt=0
    last_resp=""
    ok="false"
    while (( elapsed < POLL_TIMEOUT )); do
        attempt=$(( attempt + 1 ))
        err_output=""
        if ! last_resp=$(fetch_bn_status_detail_raw "${grpc_port}" 2>/tmp/wrb-dist-bn-tss-assert.err); then
            err_output=$(cat /tmp/wrb-dist-bn-tss-assert.err 2>/dev/null || true)
            last_resp=""
        fi
        log "  ${bn_name} attempt ${attempt} (elapsed ${elapsed}s): $(classify_attempt "${last_resp}" "${err_output}")"
        if has_tss_data "${last_resp}"; then
            ok="true"
            break
        fi
        sleep "${POLL_INTERVAL}"
        elapsed=$(( elapsed + POLL_INTERVAL ))
    done
    rm -f /tmp/wrb-dist-bn-tss-assert.err

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
