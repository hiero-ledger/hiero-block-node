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
# What the worker does:
#   * Every LIVE_PUSH_POLL_SECONDS seconds, run `blocks push` (the new wrb-cli
#     subcommand) against the wrappedBlocks output directory that
#     install-and-run-wrb-cli.sh / start-live-wrap.sh are writing into,
#     targeting BN1. `blocks push` re-queries BN1's lastAvailableBlock and the
#     local blockStreamBlockHashes.bin watermark on every invocation and only
#     pushes the gap, so the first iteration backfills everything wrapped so
#     far (the "historical" half of step 9) and later iterations push whatever
#     has been wrapped since (the "continue to move over" / live half).
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

if [[ -f "${PID_FILE}" ]] && kill -0 "$(cat "${PID_FILE}")" 2>/dev/null; then
    log "Live push already running (pid $(cat "${PID_FILE}")); leaving it in place."
    exit 0
fi

wrapped_dir="${WRB_DIST_WORK_DIR}/wrappedBlocks"
[[ -d "${wrapped_dir}" ]] || { echo "Missing prerequisite: ${wrapped_dir}" >&2; exit 1; }

printf 'initial_push_ok_count=0\n' > "${STATE_FILE}"

# Fork the worker into the background and write its PID. Using nohup+setsid so
# the loop survives if the CI shell that started the event goes away.
nohup setsid bash -c '
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
