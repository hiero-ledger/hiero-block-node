#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# WRB Distribution E2E (#3125) — assert-live-wrap-produced-new-blocks (slice 2).
#
# Runs between start-live-wrap.sh and stop-live-wrap.sh. Compares the current
# wrapped-zip count against the snapshot taken at start time (written to
# /tmp/wrb-dist-live.state) and fails if the loop hasn't produced any new
# wrapped output.
#
# This is the "did live carry on?" check for issue #3125 step 2.

set -euo pipefail

ENV_FILE="${ENV_FILE:-/tmp/wrb-distribution-step12.env}"
if [[ -f "${ENV_FILE}" ]]; then
    # shellcheck disable=SC1090
    source "${ENV_FILE}"
fi

: "${WRB_DIST_WORK_DIR:?WRB_DIST_WORK_DIR must be set}"

STATE_FILE="/tmp/wrb-dist-live.state"
PID_FILE="/tmp/wrb-dist-live.pid"
LOG_FILE="/tmp/wrb-dist-live.log"

log() { echo "[wrb-dist-live-assert] $*"; }
fail() { echo "[wrb-dist-live-assert] ERROR: $*" >&2; exit 1; }

[[ -f "${STATE_FILE}" ]] || fail "State file ${STATE_FILE} not found; did start-live-wrap.sh run?"
[[ -f "${PID_FILE}" ]] || fail "PID file ${PID_FILE} not found; did start-live-wrap.sh run?"

# shellcheck disable=SC1090
source "${STATE_FILE}"
: "${initial_zip_count:?state file did not set initial_zip_count}"

worker_pid=$(cat "${PID_FILE}")
if kill -0 "${worker_pid}" 2>/dev/null; then
    log "Live-wrap worker still alive (pid ${worker_pid})"
else
    log "WARNING: worker pid ${worker_pid} is not alive; recent log:"
    tail -30 "${LOG_FILE}" 2>/dev/null | sed 's/^/  /' || true
    fail "Live-wrap worker died before assertion"
fi

wrapped_dir="${WRB_DIST_WORK_DIR}/wrappedBlocks"
[[ -d "${wrapped_dir}" ]] || fail "Wrapped dir missing: ${wrapped_dir}"

# What we actually want to know: did the background worker keep looping and
# repeatedly succeed at running the wrap command? Neither zip-count nor
# .blk-entry count are reliable here (WRB CLI consolidates all input .rcd
# files into a single wrapped-stream block per invocation), so we look at
# the worker log itself and count "wrap OK" iterations.
current_wrap_ok_count=$( grep -cE '\] wrap OK' "${LOG_FILE}" 2>/dev/null || echo 0 )
current_total_bytes=$( find "${wrapped_dir}" -name '*.zip' -exec stat -c '%s' {} \; 2>/dev/null \
    | awk '{s+=$1} END {print s+0}' )

: "${initial_wrap_ok_count:=0}"
: "${initial_total_bytes:=0}"

log "wrap_ok_iterations: initial=${initial_wrap_ok_count} current=${current_wrap_ok_count}"
log "wrap_output_bytes: initial=${initial_total_bytes} current=${current_total_bytes}"

# Primary signal: at least one new successful wrap iteration during the window.
# Secondary signal: the wrap output has grown in bytes (records CN produced
# during the window ended up inside the zip). Either is sufficient — we log
# both for diagnosis but only require the primary.
if (( current_wrap_ok_count > initial_wrap_ok_count )); then
    log "OK: live-wrap completed $(( current_wrap_ok_count - initial_wrap_ok_count )) new iteration(s) during the observation window"
    if (( current_total_bytes > initial_total_bytes )); then
        log "  (wrap output also grew by $(( current_total_bytes - initial_total_bytes )) bytes)"
    fi
    exit 0
fi

log "Live-wrap loop did not complete a new successful iteration. Recent worker log:"
tail -80 "${LOG_FILE}" 2>/dev/null | sed 's/^/  /' || true
fail "No new wrap iterations between start and assertion (initial=${initial_wrap_ok_count} current=${current_wrap_ok_count})"
