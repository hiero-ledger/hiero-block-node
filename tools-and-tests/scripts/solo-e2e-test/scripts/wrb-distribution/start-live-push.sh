#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# WRB Distribution E2E (#3125 slice 5 — step 9) — start-live-push.
#
# Follows the same fork-a-background-worker pattern as start-live-wrap.sh: a
# `command`-event script that forks a long-running loop into the background,
# writes its PID to a well-known file, and returns immediately so the runner
# can move on. A later stop-live-push.sh reads the PID file and tears it down.
#
# Run AFTER bulk-load-historical-to-bn1.sh, which handles the "historical"
# half of step 9 by copying wrapped blocks directly into BN1's historic
# storage (bypassing the live publish/gRPC stream entirely — that pipeline is
# a single-slot-per-block-number race meant for freshly-produced blocks, not
# historical replay; see bulk-load-historical-to-bn1.sh's header for why).
# This script only covers the "continue to move over" / live half.
#
# What the worker does:
#   * Every LIVE_PUSH_POLL_SECONDS seconds, run `blocks push` (the new wrb-cli
#     subcommand) against the wrappedBlocks output directory that
#     install-and-run-wrb-cli.sh / start-live-wrap.sh are writing into,
#     targeting BN1. `blocks push` re-queries BN1's lastAvailableBlock and the
#     local blockStreamBlockHashes.bin watermark on every invocation and only
#     pushes the gap — since the historical backlog is already on BN1 via
#     bulk-load by the time this runs, that gap is just whatever `blocks wrap`
#     has produced since.
#   * Continues until stop-live-push.sh signals it or `blocks push` hard-fails
#     LIVE_PUSH_MAX_CONSECUTIVE_FAILURES times in a row.
#
# Inputs (env, set by install-and-run-wrb-cli.sh before this runs):
#   WRB_DIST_WORK_DIR    Same shared work dir as start-live-wrap.sh; contains
#                        wrappedBlocks/.
#   CLI_LIB              tools-and-tests/tools/build/install/tools/lib.
#
# Optional overrides:
#   LIVE_PUSH_POLL_SECONDS              (default: 30)
#   LIVE_PUSH_MAX_CONSECUTIVE_FAILURES  (default: 5)
#   BN_HOST_1                           (default localhost)
#   BN1_GRPC_PORT                       (default 40840 — matches add-bn.sh's port-forward
#                                       convention: grpc_port = 40839 + bn_index, so BN1 -> 40840)

set -euo pipefail

ENV_FILE="${ENV_FILE:-/tmp/wrb-distribution-step12.env}"
if [[ -f "${ENV_FILE}" ]]; then
    # shellcheck disable=SC1090
    source "${ENV_FILE}"
fi

: "${NAMESPACE:=solo-network}"
# This worker runs as a plain process on the test runner host, not inside the
# cluster, so it must reach BN1 through the localhost port-forward add-bn.sh
# sets up (grpc :40839+bn_index) rather than BN1's in-cluster service DNS name
# (which only resolves from within a pod, e.g. the CN's own block-nodes.json
# target in reconfigure-cn-to-push-bn3.sh).
: "${BN_HOST_1:=localhost}"
BN1_GRPC_PORT="${BN1_GRPC_PORT:-$((40839 + 1))}"

: "${WRB_DIST_WORK_DIR:?WRB_DIST_WORK_DIR must be set (written by install-and-run-wrb-cli.sh)}"
: "${CLI_LIB:?CLI_LIB must be set (written by install-and-run-wrb-cli.sh)}"

LIVE_PUSH_POLL_SECONDS="${LIVE_PUSH_POLL_SECONDS:-30}"
LIVE_PUSH_MAX_CONSECUTIVE_FAILURES="${LIVE_PUSH_MAX_CONSECUTIVE_FAILURES:-5}"

PID_FILE="/tmp/wrb-dist-push.pid"
LOG_FILE="/tmp/wrb-dist-push.log"
STATE_FILE="/tmp/wrb-dist-push.state"

log() { echo "[wrb-dist-push-start] $*"; }

# Never defer to a pre-existing worker: PID_FILE/LOG_FILE/STATE_FILE live under /tmp, which
# survives `task down`/`task up` (only the Kubernetes cluster gets torn down, not local
# background processes on this host). A worker orphaned by an interrupted/crashed prior run
# (never reaching its own stop-live-push.sh) would otherwise be silently adopted here as "this
# run's" worker -- still alive, but pushing against a stale wrapped_dir/port-forward from a
# cluster that no longer exists, with a stale state-file baseline from whenever IT started.
# Always kill anything still running first (same process-group kill as stop-live-push.sh) so
# every run starts its own fresh worker with a correct, freshly-truncated log.
if [[ -f "${PID_FILE}" ]]; then
    stale_pid=$(cat "${PID_FILE}")
    if [[ -n "${stale_pid}" ]] && kill -0 "${stale_pid}" 2>/dev/null; then
        log "Found a live worker (pid ${stale_pid}) from a previous run; stopping it before starting fresh."
        kill -TERM -"${stale_pid}" 2>/dev/null || kill -TERM "${stale_pid}" 2>/dev/null || true
        for _ in $(seq 1 20); do
            kill -0 "${stale_pid}" 2>/dev/null || break
            sleep 0.5
        done
        if kill -0 "${stale_pid}" 2>/dev/null; then
            kill -KILL -"${stale_pid}" 2>/dev/null || kill -KILL "${stale_pid}" 2>/dev/null || true
        fi
    fi
    rm -f "${PID_FILE}"
fi

wrapped_dir="${WRB_DIST_WORK_DIR}/wrappedBlocks"
[[ -d "${wrapped_dir}" ]] || { echo "Missing prerequisite: ${wrapped_dir}" >&2; exit 1; }

# We only reach here when about to fork a fresh worker (the "already running" case above
# already returned) -- the nohup redirect below truncates LOG_FILE, discarding any "push OK"
# lines left over from a prior run. Snapshotting THAT stale count here (as this used to do)
# would compare the fresh worker's own early iterations against a baseline that no longer
# means anything once the log is wiped, producing a false "no new iterations" failure in
# assert-live-push-produced-new-blocks.sh if the fresh count happens to reach the same number.
# The fresh worker always starts its log from 0, so the baseline is always 0 here.
initial_push_ok_count=0
printf 'initial_push_ok_count=%s\n' "${initial_push_ok_count}" > "${STATE_FILE}"

# Fork the worker into the background and write its PID. Using nohup+setsid so
# the loop survives if the CI shell that started the event goes away. setsid
# isn't available on macOS (it's a util-linux tool); fall back to plain nohup
# there — the worker still gets backgrounded and outlives the parent shell,
# just without its own process group (stop-live-push.sh already falls back to
# a plain `kill <pid>` when the process-group kill fails for this reason).
setsid_prefix=""
command -v setsid >/dev/null 2>&1 && setsid_prefix="setsid"
nohup ${setsid_prefix} bash -c '
    set -uo pipefail

    CLI_LIB='"'${CLI_LIB}'"'
    wrapped_dir='"'${wrapped_dir}'"'
    BN_HOST_1='"'${BN_HOST_1}'"'
    BN1_GRPC_PORT='"'${BN1_GRPC_PORT}'"'
    LIVE_PUSH_POLL_SECONDS='"${LIVE_PUSH_POLL_SECONDS}"'
    LIVE_PUSH_MAX_CONSECUTIVE_FAILURES='"${LIVE_PUSH_MAX_CONSECUTIVE_FAILURES}"'

    consecutive_failures=0
    iteration=0
    while true; do
        iteration=$(( iteration + 1 ))
        echo "[live-push][iter ${iteration}] pushing wrapped blocks to ${BN_HOST_1}:${BN1_GRPC_PORT}..."

        java -cp "${CLI_LIB}/*" \
            org.hiero.block.tools.BlockStreamTool blocks push \
                --input-dir "${wrapped_dir}" \
                --bn-host "${BN_HOST_1}" \
                --bn-port "${BN1_GRPC_PORT}" \
            >> /tmp/wrb-dist-live-push.log 2>&1 \
            && rc=0 || rc=$?

        if [[ "${rc}" -eq 0 ]]; then
            consecutive_failures=0
            echo "[live-push][iter ${iteration}] push OK"
        else
            consecutive_failures=$(( consecutive_failures + 1 ))
            echo "[live-push][iter ${iteration}] push FAILED rc=${rc} (consecutive_failures=${consecutive_failures})"
            if [[ "${consecutive_failures}" -ge "${LIVE_PUSH_MAX_CONSECUTIVE_FAILURES}" ]]; then
                echo "[live-push] giving up after ${consecutive_failures} consecutive failures"
                exit 1
            fi
        fi

        sleep "${LIVE_PUSH_POLL_SECONDS}"
    done
' > "${LOG_FILE}" 2>&1 &

worker_pid=$!
echo "${worker_pid}" > "${PID_FILE}"
log "Live push started (pid ${worker_pid}, log ${LOG_FILE}, poll every ${LIVE_PUSH_POLL_SECONDS}s, target ${BN_HOST_1}:${BN1_GRPC_PORT})"
