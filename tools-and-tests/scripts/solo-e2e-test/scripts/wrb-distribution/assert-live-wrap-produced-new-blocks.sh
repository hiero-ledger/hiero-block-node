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

current_zip_count=$( find "${wrapped_dir}" -name '*.zip' -print 2>/dev/null | wc -l | tr -d ' ' )
log "initial=${initial_zip_count} current=${current_zip_count}"

if (( current_zip_count > initial_zip_count )); then
    log "OK: live-wrap produced $(( current_zip_count - initial_zip_count )) new wrapped-zip file(s)"
    exit 0
fi

# Not enough time may have passed, or no new records arrived. Print recent log
# for diagnosis and fail — the test-definition sizes the sleep so this shouldn't
# happen in a healthy run.
log "Live-wrap did not produce new wrapped-zip files. Recent worker log:"
tail -60 "${LOG_FILE}" 2>/dev/null | sed 's/^/  /' || true
fail "No new wrapped zips between start and assertion (initial=${initial_zip_count} current=${current_zip_count})"
