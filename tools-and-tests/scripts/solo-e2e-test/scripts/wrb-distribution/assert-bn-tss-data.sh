#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# WRB Distribution E2E (#3125 slice 6 — step 11) — assert that a Block Node's
# serverStatusDetail reports populated TSS data (ledger id, WRAPS verification
# key, and a non-empty TSS roster) after cn-upgrade-tss.sh cuts the network
# over to a TSS/WRAPS-native CN version.
#
# BN1 (Tier-0) gets TssData directly: detect-tss-enablement.sh detects the
# CN's post-upgrade LedgerIdPublication transaction and writes
# tss-bootstrap-roster.json, and stage-tss-data-on-bn1.sh stages that file
# onto BN1 and restarts it so BlockNodeApp.loadApplicationState() loads it —
# so BN1 should be checked FIRST, right after that restart.
#
# BN2 and BN3 have no such direct path. They instead pick up the same
# TssData "eventually" via the roster-bootstrap-tss plugin's periodic
# peer-gossip poll of BN1 (reconfigure-bn-roster-bootstrap-tss.sh, step 10),
# so they should be checked SECOND, after BN1 already has it — matching
# issue #3125 step 11's literal wording (BN1 first, then eventually BN2/BN3).
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
#                   — BN2/BN3 need a roster-bootstrap-tss poll cycle
#                   (ROSTER_BOOTSTRAP_TSS_QUERY_PEER_INTERVAL default 60s)
#                   after BN1 already has the data)

set -euo pipefail

: "${POLL_INTERVAL:=15}"
: "${POLL_TIMEOUT:=120}"
: "${NAMESPACE:=solo-network}"
: "${CLUSTER_REFERENCE:=kind-solo-cluster}"

[[ $# -ge 1 ]] || { echo "assert-bn-tss-data.sh: at least one BN index required (e.g. 1 or 2 3)" >&2; exit 1; }

log() { echo "[wrb-dist-bn-tss-assert] $*"; }
fail() { echo "[wrb-dist-bn-tss-assert] ERROR: $*" >&2; exit 1; }

[[ -n "${PROTO_PATH:-}" && -d "${PROTO_PATH}" ]] || fail "PROTO_PATH not set or not a directory (got '${PROTO_PATH:-}')"
command -v grpcurl >/dev/null 2>&1 || fail "grpcurl not found"
command -v jq >/dev/null 2>&1 || fail "jq not found"

# ── Port-forward helpers ──────────────────────────────────────────────────────
# BN1 restarts as part of stage-tss-data-on-bn1.sh; the test-runner's
# type:port-forward event fires at a fixed wall-clock delay that may race the
# pod becoming ready, leaving the port-forward dead.  Refresh it here (and on
# every UNREACHABLE response) so transient pod-restart windows don't fail the
# assertion regardless of cluster speed.
pf_log_dir="${TMPDIR:-/tmp}/wrb-dist-add-bn-pf"
mkdir -p "${pf_log_dir}"
setsid_prefix=""
command -v setsid >/dev/null 2>&1 && setsid_prefix="setsid"

refresh_bn_port_forward() {
    local svc="$1" local_port="$2"
    pkill -f "port-forward svc/${svc}.*${local_port}:" 2>/dev/null || true
    sleep 1
    local pf_log="${pf_log_dir}/${svc}-${local_port}.log"
    nohup ${setsid_prefix} kubectl --context "${CLUSTER_REFERENCE}" \
        --namespace "${NAMESPACE}" \
        port-forward "svc/${svc}" "${local_port}:40840" \
        >"${pf_log}" 2>&1 </dev/null &
    local deadline=$(( $(date +%s) + 15 ))
    until grep -q "Forwarding from" "${pf_log}" 2>/dev/null; do
        if (( $(date +%s) >= deadline )); then
            log "  port-forward for ${svc} (localhost:${local_port}) not yet up — will retry on next poll"
            return
        fi
        sleep 1
    done
    log "  port-forward for ${svc} ready on localhost:${local_port}."
}

fetch_bn_status_detail_raw() {
    local port="$1"
    # Use 127.0.0.1 (IPv4) explicitly: kubectl port-forward binds only IPv4
    # on macOS (127.0.0.1), but grpcurl resolves "localhost" to [::1] (IPv6)
    # via Go's resolver when the system prefers IPv6. This gives connection
    # refused even when the port-forward is running correctly.
    # Captured separately from stderr so a connection failure (grpcurl exits
    # non-zero, empty stdout) can be told apart from "reachable but empty
    # response" — both look identical if stderr is discarded.
    grpcurl -plaintext -emit-defaults \
        -import-path "${PROTO_PATH}" \
        -proto block-node/api/node_service.proto \
        -d '{}' \
        "127.0.0.1:${port}" \
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
    log "  Refreshing port-forward for ${bn_name} before polling..."
    refresh_bn_port_forward "${bn_name}" "${grpc_port}"
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
            # Pod may still be starting — refresh the port-forward on each failure
            # so we pick it up as soon as the pod becomes ready.
            refresh_bn_port_forward "${bn_name}" "${grpc_port}"
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
