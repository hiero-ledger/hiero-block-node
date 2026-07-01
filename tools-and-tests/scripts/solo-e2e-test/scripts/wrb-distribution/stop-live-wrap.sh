#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# WRB Distribution E2E (#3125) — stop-live-wrap (slice 2).
#
# Symmetric partner to start-live-wrap.sh. Follows the load-start / load-stop
# pattern: reads the well-known PID file, sends SIGTERM, waits for the process
# tree to exit, and prints the tail of the log for post-hoc inspection.
#
# Idempotent: exits 0 if the PID file is missing or the process is already
# gone, so re-running or running after a crash is safe.

set -euo pipefail

PID_FILE="/tmp/wrb-dist-live.pid"
LOG_FILE="/tmp/wrb-dist-live.log"

log() { echo "[wrb-dist-live-stop] $*"; }

if [[ ! -f "${PID_FILE}" ]]; then
    log "No PID file; live wrap was not running. Nothing to do."
    exit 0
fi

worker_pid=$(cat "${PID_FILE}")
if [[ -z "${worker_pid}" ]]; then
    log "Empty PID file; nothing to stop."
    rm -f "${PID_FILE}"
    exit 0
fi

if ! kill -0 "${worker_pid}" 2>/dev/null; then
    log "Worker pid ${worker_pid} already gone."
    rm -f "${PID_FILE}"
    exit 0
fi

# The worker was started via `setsid` so it has its own process-group id equal
# to its PID. Signal the whole group to catch the wrap subprocess too.
log "Sending SIGTERM to process group ${worker_pid}..."
kill -TERM -"${worker_pid}" 2>/dev/null || kill -TERM "${worker_pid}" 2>/dev/null || true

for _ in $(seq 1 20); do
    kill -0 "${worker_pid}" 2>/dev/null || break
    sleep 0.5
done

if kill -0 "${worker_pid}" 2>/dev/null; then
    log "Worker did not exit; sending SIGKILL"
    kill -KILL -"${worker_pid}" 2>/dev/null || kill -KILL "${worker_pid}" 2>/dev/null || true
fi

rm -f "${PID_FILE}"

log "Tail of ${LOG_FILE}:"
tail -30 "${LOG_FILE}" 2>/dev/null | sed 's/^/  /' || true
log "Live wrap stopped."
