#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# WRB Distribution E2E (#3125 slice 5 — step 9, live half) —
# assert-live-push-produced-new-blocks.
#
# Runs between start-live-push.sh and stop-live-push.sh. The historical half of
# step 9 is proven separately by assert-bn1-historical-backfill-landed.sh right
# after bulk-load-historical-to-bn1.sh; this script only proves the "continue
# to move over" half: that the live-push loop (blocks push, wrappedBlocks ->
# BN1) kept completing iterations after the historical backfill, mirroring
# assert-live-wrap-produced-new-blocks.sh's own liveness check for the wrap
# loop.
#
# Reads:
#   (none directly — everything comes from /tmp/wrb-dist-push.state / .pid / .log
#   written by start-live-push.sh)

set -euo pipefail

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

# At least one new successful push iteration since start-live-push.sh's snapshot.
current_push_ok_count=$( grep -cE '\] push OK' "${LOG_FILE}" 2>/dev/null || echo 0 )
log "push_ok_iterations: initial=${initial_push_ok_count} current=${current_push_ok_count}"

if (( current_push_ok_count > initial_push_ok_count )); then
    log "OK: live-push completed $(( current_push_ok_count - initial_push_ok_count )) new iteration(s) during the observation window"
    exit 0
fi

log "Live-push loop did not complete a new successful iteration. Recent worker log:"
tail -80 "${LOG_FILE}" 2>/dev/null | sed 's/^/  /' || true
fail "No new push iterations between start and assertion (initial=${initial_push_ok_count} current=${current_push_ok_count})"
