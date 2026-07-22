#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# WRB Distribution E2E (#3125 slice 5 — step 9) — assert-live-push-produced-new-blocks.
#
# Runs between start-live-push.sh and stop-live-push.sh. This is the "did both
# the historical backfill and the live continuation happen?" check for step 9:
#
#   * Historical: query BN1's serverStatus over the port-forward add-bn.sh set
#     up and assert lastAvailableBlock is present (>= 0) — proves the first
#     `blocks push` iteration landed the backlog wrapped during steps 1-7.
#   * Live / "continue to move over": compare the "push OK" iteration count in
#     the worker log against the snapshot taken at start-live-push.sh time
#     (/tmp/wrb-dist-push.state) and fail if the loop hasn't completed a new
#     iteration — mirrors assert-live-wrap-produced-new-blocks.sh's own
#     liveness check for the wrap loop.
#
# Reads:
#   BN1_GRPC_PORT  (default 40840 — matches add-bn.sh's port-forward convention:
#                  grpc_port = 40839 + bn_index, so BN1 -> 40840)

set -euo pipefail

: "${BN1_GRPC_PORT:=$((40839 + 1))}"

STATE_FILE="/tmp/wrb-dist-push.state"
PID_FILE="/tmp/wrb-dist-push.pid"
LOG_FILE="/tmp/wrb-dist-push.log"

log() { echo "[wrb-dist-push-assert] $*"; }
fail() { echo "[wrb-dist-push-assert] ERROR: $*" >&2; exit 1; }

[[ -f "${STATE_FILE}" ]] || fail "State file ${STATE_FILE} not found; did start-live-push.sh run?"
[[ -f "${PID_FILE}" ]] || fail "PID file ${PID_FILE} not found; did start-live-push.sh run?"

# shellcheck disable=SC1090
source "${STATE_FILE}"
: "${initial_push_ok_count:?state file did not set initial_push_ok_count}"

worker_pid=$(cat "${PID_FILE}")
if kill -0 "${worker_pid}" 2>/dev/null; then
    log "Live-push worker still alive (pid ${worker_pid})"
else
    log "WARNING: worker pid ${worker_pid} is not alive; recent log:"
    tail -30 "${LOG_FILE}" 2>/dev/null | sed 's/^/  /' || true
    fail "Live-push worker died before assertion"
fi

# Historical check: BN1 should report a lastAvailableBlock by now (the first
# `blocks push` iteration backfills everything wrapped so far).
if ! command -v grpcurl >/dev/null 2>&1; then
    fail "grpcurl not on PATH; cannot query BN1 serverStatus"
fi

status_json=$(grpcurl -plaintext -d '{}' "localhost:${BN1_GRPC_PORT}" \
    org.hiero.block.api.BlockNodeService/serverStatus 2>/dev/null || echo '{}')
bn1_last=$(echo "${status_json}" | jq -r '.lastAvailableBlock // empty' 2>/dev/null || echo "")

if [[ -n "${bn1_last}" && "${bn1_last}" =~ ^[0-9]+$ ]]; then
    log "BN1 lastAvailableBlock=${bn1_last} (historical backfill landed)"
else
    fail "Could not read a numeric lastAvailableBlock from BN1 (got '${bn1_last}'); historical push did not land"
fi

# Live / "continue to move over" check: at least one new successful push
# iteration since start-live-push.sh's snapshot.
current_push_ok_count=$( grep -cE '\] push OK' "${LOG_FILE}" 2>/dev/null || echo 0 )
log "push_ok_iterations: initial=${initial_push_ok_count} current=${current_push_ok_count}"

if (( current_push_ok_count > initial_push_ok_count )); then
    log "OK: live-push completed $(( current_push_ok_count - initial_push_ok_count )) new iteration(s) during the observation window"
    exit 0
fi

log "Live-push loop did not complete a new successful iteration. Recent worker log:"
tail -80 "${LOG_FILE}" 2>/dev/null | sed 's/^/  /' || true
fail "No new push iterations between start and assertion (initial=${initial_push_ok_count} current=${current_push_ok_count})"
