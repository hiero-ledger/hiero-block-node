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
: "${NAMESPACE:=solo-network}"
: "${CLUSTER_REFERENCE:=kind-solo-cluster}"

log() { echo "[wrb-dist-cutover-assert] $*"; }
fail() { echo "[wrb-dist-cutover-assert] ERROR: $*" >&2; exit 1; }

[[ -n "${PROTO_PATH:-}" && -d "${PROTO_PATH}" ]] || fail "PROTO_PATH not set or not a directory (got '${PROTO_PATH:-}')"
command -v grpcurl >/dev/null 2>&1 || fail "grpcurl not found"
command -v jq >/dev/null 2>&1 || fail "jq not found"

# ── Port-forward refresh ──────────────────────────────────────────────────────
# test-runner type:port-forward events kill ALL kubectl port-forward processes
# before starting their own (static-topology only), so block-node forwards
# set up by add-bn.sh / stage-tss-data-on-bn1.sh / reconfigure-cn-to-push-bn1.sh
# may be dead by the time this assertion runs.  Refresh each one unconditionally
# before the poll loop so every node is reachable from the first attempt.
pf_log_dir="${TMPDIR:-/tmp}/wrb-dist-add-bn-pf"
mkdir -p "${pf_log_dir}"
setsid_prefix=""
command -v setsid >/dev/null 2>&1 && setsid_prefix="setsid"

refresh_bn_port_forward() {
    local svc="$1" local_port="$2" label="$3"
    pkill -f "port-forward svc/${svc}.*${local_port}:" 2>/dev/null || true
    sleep 1
    local pf_log="${pf_log_dir}/${svc}-${local_port}.log"
    nohup ${setsid_prefix} kubectl --context "${CLUSTER_REFERENCE}" \
        --namespace "${NAMESPACE}" \
        port-forward "svc/${svc}" "${local_port}:40840" \
        >"${pf_log}" 2>&1 </dev/null &
    local deadline=$(( $(date +%s) + 30 ))
    until grep -q "Forwarding from" "${pf_log}" 2>/dev/null; do
        if (( $(date +%s) >= deadline )); then
            log "WARNING: port-forward for ${label} (localhost:${local_port}) did not come up after 30s; continuing anyway"
            return
        fi
        sleep 1
    done
    log "Port-forward for ${label} ready on localhost:${local_port}."
}

log "Refreshing BN port-forwards before polling..."
refresh_bn_port_forward block-node-1 40840 "block-node-1"
refresh_bn_port_forward block-node-2 40841 "block-node-2"
refresh_bn_port_forward block-node-3 40842 "block-node-3"
log "BN port-forward refresh complete."

bn_last_block() {
    local port="$1"
    local resp
    # Use 127.0.0.1 (IPv4) explicitly: kubectl port-forward binds only IPv4
    # on macOS, but grpcurl resolves "localhost" to [::1] (IPv6) via Go's
    # resolver when the system prefers IPv6, causing connection refused.
    if ! resp=$(grpcurl -plaintext -emit-defaults \
        -import-path "${PROTO_PATH}" \
        -proto block-node/api/node_service.proto \
        -d '{}' "127.0.0.1:${port}" \
        org.hiero.block.api.BlockNodeService/serverStatus 2>/tmp/wrb-dist-cutover.err); then
        echo ""
        return
    fi
    echo "${resp}" | jq -r '.lastAvailableBlock // empty' 2>/dev/null || true
}

mn_last_block() {
    local port="$1"
    local body
    body=$(curl -s --max-time 10 "http://127.0.0.1:${port}/api/v1/blocks?limit=1&order=desc" 2>/tmp/wrb-dist-cutover.err) || return 0
    # Distinguish "server reachable but no blocks yet" (return "0") from
    # "server not reachable" (return "").  The /api/v1/blocks response is
    # always a JSON object with a blocks array; an absent or null .blocks[0]
    # means the importer has not yet ingested block 0, not that the pod is
    # down.  Treating 0-block nodes as "missing" rather than "behind" prevents
    # convergence from ever being declared when MN2 is still catching up.
    if echo "${body}" | jq -e 'has("blocks")' >/dev/null 2>&1; then
        echo "${body}" | jq -r '.blocks[0].number // "0"' 2>/dev/null || true
    fi
    # If jq can't parse the body at all the function returns empty (server error).
}

names=(block-node-1 block-node-2 block-node-3 mirror-1 mirror-2)
ports=(40840 40841 40842 5551 5552)
kinds=(bn bn bn mn mn)

elapsed=0
attempt=0
while (( elapsed < POLL_TIMEOUT )); do
    attempt=$(( attempt + 1 ))
    values=()
    missing=()
    for i in "${!names[@]}"; do
        if [[ "${kinds[$i]}" == "bn" ]]; then
            v=$(bn_last_block "${ports[$i]}")
        else
            v=$(mn_last_block "${ports[$i]}")
        fi
        if [[ -z "${v}" || "${v}" == "null" ]]; then
            missing+=("${names[$i]}")
            values+=("")
        else
            values+=("${v}")
        fi
    done

    # Log every attempt (not just on final failure) so a long poll window
    # shows visible progress instead of minutes of silence that read as a
    # hang, and names exactly which node(s) are the problem rather than a
    # generic "not all nodes reported".
    reported=""
    for i in "${!names[@]}"; do
        [[ -n "${values[$i]}" ]] && reported="${reported}${names[$i]}=${values[$i]} "
    done
    if [[ ${#missing[@]} -gt 0 ]]; then
        log "attempt ${attempt} (elapsed ${elapsed}s): missing=[${missing[*]}] reported=[${reported}]"
    else
        min="${values[0]}"
        max="${values[0]}"
        for v in "${values[@]}"; do
            (( v < min )) && min="${v}"
            (( v > max )) && max="${v}"
        done
        log "attempt ${attempt} (elapsed ${elapsed}s): ${reported}(spread=$(( max - min )))"

        if (( max - min <= CONVERGE_WINDOW )); then
            log "All BNs and MNs converged within ${CONVERGE_WINDOW} block(s) (min=${min}, max=${max}). Cutover sync confirmed."
            rm -f /tmp/wrb-dist-cutover.err
            exit 0
        fi
    fi

    sleep "${POLL_INTERVAL}"
    elapsed=$(( elapsed + POLL_INTERVAL ))
done

rm -f /tmp/wrb-dist-cutover.err
fail "BNs/MNs did not converge within ${POLL_TIMEOUT}s (see per-attempt log above for which node(s) never reported or stayed out of range)"
