#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# WRB Distribution E2E (#3125) — start-live-wrap (slice 2).
#
# Follows the `load-start` / `load-stop` pattern used by basic-load.yaml /
# high-load.yaml: a `command`-event script that forks a long-running worker
# into the background, writes its PID to a well-known file, and returns
# immediately so the runner can move on to the next event. A later
# `stop-live-wrap.sh` reads the PID file and tears the worker down.
#
# What the worker does:
#   * Every LIVE_WRAP_POLL_SECONDS seconds, download the latest record files
#     from the Solo MinIO recordstreams bucket into the shared records dir,
#     add any new files to per-day tar.zstd archives, and re-run
#     `blocks wrap`. The wrap command is idempotent against already-wrapped
#     blocks (wrap-commit.bin watermark), so re-invocations only wrap
#     newly-arrived record files.
#   * Continues until stop-live-wrap.sh signals it or the wrap subprocess
#     hard-fails LIVE_WRAP_MAX_CONSECUTIVE_FAILURES times in a row.
#
# Inputs (env, set by install-and-run-wrb-cli.sh before this runs):
#   WRB_DIST_WORK_DIR    Absolute path shared between the initial wrap and the
#                        background loop. Contains records/, day-archives/,
#                        wrappedBlocks/, network config, block_times.bin, etc.
#   CLI_LIB              tools-and-tests/tools/build/install/tools/lib (already
#                        built by install-and-run-wrb-cli.sh).
#
# Optional overrides:
#   LIVE_WRAP_POLL_SECONDS         (default: 30)
#   LIVE_WRAP_MAX_CONSECUTIVE_FAILURES (default: 5)
#   LIVE_WRAP_MAX_RECORDS_PER_POLL (default: 200)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPARISON_SCRIPT="${SCRIPT_DIR}/../wrb-sequential-comparison.sh"
PYTHON_DIR="${SCRIPT_DIR}/../python"

# Pick up WRB_DIST_WORK_DIR / CLI_LIB / NAMESPACE / CONTEXT written by
# install-and-run-wrb-cli.sh. Each command event runs in its own subshell so
# we can't rely on an in-memory export from a previous event.
ENV_FILE="${ENV_FILE:-/tmp/wrb-distribution-step12.env}"
if [[ -f "${ENV_FILE}" ]]; then
    # shellcheck disable=SC1090
    source "${ENV_FILE}"
fi

: "${NAMESPACE:=solo-network}"
: "${CONTEXT:=kind-solo-cluster}"
export NAMESPACE CONTEXT

: "${WRB_DIST_WORK_DIR:?WRB_DIST_WORK_DIR must be set (write by install-and-run-wrb-cli.sh)}"
: "${CLI_LIB:?CLI_LIB must be set (write by install-and-run-wrb-cli.sh)}"

LIVE_WRAP_POLL_SECONDS="${LIVE_WRAP_POLL_SECONDS:-30}"
LIVE_WRAP_MAX_CONSECUTIVE_FAILURES="${LIVE_WRAP_MAX_CONSECUTIVE_FAILURES:-5}"
LIVE_WRAP_MAX_RECORDS_PER_POLL="${LIVE_WRAP_MAX_RECORDS_PER_POLL:-200}"

PID_FILE="/tmp/wrb-dist-live.pid"
LOG_FILE="/tmp/wrb-dist-live.log"
STATE_FILE="/tmp/wrb-dist-live.state"

log() { echo "[wrb-dist-live-start] $*"; }

if [[ -f "${PID_FILE}" ]] && kill -0 "$(cat "${PID_FILE}")" 2>/dev/null; then
    log "Live wrap already running (pid $(cat "${PID_FILE}")); leaving it in place."
    exit 0
fi

records_dir="${WRB_DIST_WORK_DIR}/records"
days_dir="${WRB_DIST_WORK_DIR}/day-archives"
wrapped_dir="${WRB_DIST_WORK_DIR}/wrappedBlocks"
block_times_file="${WRB_DIST_WORK_DIR}/block_times.bin"
day_blocks_file="${WRB_DIST_WORK_DIR}/day_blocks.json"
network_config_file="${WRB_DIST_WORK_DIR}/network-other.json"

for required in "${records_dir}" "${days_dir}" "${wrapped_dir}" \
                "${block_times_file}" "${day_blocks_file}" "${network_config_file}"; do
    [[ -e "${required}" ]] || { echo "Missing prerequisite: ${required}" >&2; exit 1; }
done

# Snapshot the wrap output at start-time. The assertion later compares against
# this. WRB CLI consolidates all input .rcd files into a single wrapped-stream
# block per invocation (behaviour confirmed by the earlier WRB differential
# test work), so neither zip-count nor block-count is a reliable "did the loop
# make progress" signal — both can stay at 1 while the loop is healthy.
#
# The signal that IS reliable is that the worker log records successful wrap
# iterations. Snapshot the count of "wrap OK" markers so the assertion can
# check that the count grew during the observation window.
initial_zip_count=$( find "${wrapped_dir}" -name '*.zip' -print 2>/dev/null | wc -l | tr -d ' ' )
initial_total_bytes=$( find "${wrapped_dir}" -name '*.zip' -exec stat -c '%s' {} \; 2>/dev/null \
    | awk '{s+=$1} END {print s+0}' )
printf 'initial_zip_count=%s\ninitial_total_bytes=%s\ninitial_wrap_ok_count=0\n' \
    "${initial_zip_count}" "${initial_total_bytes}" > "${STATE_FILE}"
log "Initial wrap output: ${initial_zip_count} zip file(s), ${initial_total_bytes} total byte(s)"

# Fork the worker into the background and write its PID. Using nohup+setsid so
# the loop survives if the CI shell that started the event goes away.
nohup setsid bash -c '
    set -uo pipefail

    SCRIPT_DIR='"'${SCRIPT_DIR}'"'
    COMPARISON_SCRIPT='"'${COMPARISON_SCRIPT}'"'
    PYTHON_DIR='"'${PYTHON_DIR}'"'
    WRB_DIST_WORK_DIR='"'${WRB_DIST_WORK_DIR}'"'
    CLI_LIB='"'${CLI_LIB}'"'
    NAMESPACE='"'${NAMESPACE}'"'
    CONTEXT='"'${CONTEXT}'"'
    LIVE_WRAP_POLL_SECONDS='"${LIVE_WRAP_POLL_SECONDS}"'
    LIVE_WRAP_MAX_CONSECUTIVE_FAILURES='"${LIVE_WRAP_MAX_CONSECUTIVE_FAILURES}"'
    LIVE_WRAP_MAX_RECORDS_PER_POLL='"${LIVE_WRAP_MAX_RECORDS_PER_POLL}"'

    export NAMESPACE CONTEXT

    records_dir="${WRB_DIST_WORK_DIR}/records"
    days_dir="${WRB_DIST_WORK_DIR}/day-archives"
    wrapped_dir="${WRB_DIST_WORK_DIR}/wrappedBlocks"
    block_times_file="${WRB_DIST_WORK_DIR}/block_times.bin"
    day_blocks_file="${WRB_DIST_WORK_DIR}/day_blocks.json"
    network_config_file="${WRB_DIST_WORK_DIR}/network-other.json"

    # Reuse the MinIO discovery + download helper.
    # shellcheck disable=SC1090
    source "${COMPARISON_SCRIPT}"

    consecutive_failures=0
    iteration=0
    while true; do
        iteration=$(( iteration + 1 ))
        echo "[live-wrap][iter ${iteration}] downloading up to ${LIVE_WRAP_MAX_RECORDS_PER_POLL} record files..."

        if download_record_files_from_minio "${records_dir}" "${LIVE_WRAP_MAX_RECORDS_PER_POLL}"; then
            shopt -s nullglob
            gz_files=( "${records_dir}"/*.rcd.gz )
            if (( ${#gz_files[@]} > 0 )); then
                gunzip -f "${gz_files[@]}" 2>/dev/null || true
            fi
            shopt -u nullglob

            # Refresh day archives so wrap sees the newly-arrived records.
            days=$( find "${records_dir}" -name "*.rcd" -exec basename {} \; | cut -d"T" -f1 | sort -u )
            for day in ${days}; do
                archive="${days_dir}/${day}.tar.zstd"
                ( cd "${records_dir}" && tar -cf - "${day}"T*.rcd "${day}"T*.rcd_sig 2>/dev/null | zstd -T0 > "${archive}" )
            done

            # Regenerate metadata so block_times.bin / day_blocks.json cover
            # the new records.
            first_rcd_ts=$( find "${records_dir}" -name "*.rcd" -exec basename {} \; | sort | head -1 | cut -d"." -f1 )
            first_dt=$( echo "${first_rcd_ts}" | sed "s/_/:/g" )
            if date --version >/dev/null 2>&1; then
                first_seconds=$( date -u -d "${first_dt}Z" +%s 2>/dev/null || echo "0" )
            else
                first_seconds=$( date -u -j -f "%Y-%m-%dT%H:%M:%S" "${first_dt}" +%s 2>/dev/null || echo "0" )
            fi
            genesis_epoch_nanos=$(( first_seconds * 1000000000 ))
            python3 "${PYTHON_DIR}/generate_metadata.py" \
                "${records_dir}" "${block_times_file}" "${day_blocks_file}" "${genesis_epoch_nanos}" \
                >/dev/null 2>&1 || true

            HIERO_NETWORK_CONFIG="${network_config_file}" \
            java -cp "${CLI_LIB}/*" \
                org.hiero.block.tools.BlockStreamTool blocks wrap \
                    --network other \
                    --input-dir "${days_dir}" \
                    --output-dir "${wrapped_dir}" \
                    --blocktimes-file "${block_times_file}" \
                    --day-blocks "${day_blocks_file}" \
                    --skip-block-number-validation \
                >> /tmp/wrb-dist-live-wrap.log 2>&1 \
                && rc=0 || rc=$?

            if [[ "${rc}" -eq 0 ]]; then
                consecutive_failures=0
                zip_count=$( find "${wrapped_dir}" -name "*.zip" -print 2>/dev/null | wc -l | tr -d " " )
                echo "[live-wrap][iter ${iteration}] wrap OK; wrapped-zip count now ${zip_count}"
            else
                consecutive_failures=$(( consecutive_failures + 1 ))
                echo "[live-wrap][iter ${iteration}] wrap FAILED rc=${rc} (consecutive_failures=${consecutive_failures})"
                if [[ "${consecutive_failures}" -ge "${LIVE_WRAP_MAX_CONSECUTIVE_FAILURES}" ]]; then
                    echo "[live-wrap] giving up after ${consecutive_failures} consecutive failures"
                    exit 1
                fi
            fi
        else
            consecutive_failures=$(( consecutive_failures + 1 ))
            echo "[live-wrap][iter ${iteration}] download failed (consecutive_failures=${consecutive_failures})"
        fi

        sleep "${LIVE_WRAP_POLL_SECONDS}"
    done
' > "${LOG_FILE}" 2>&1 &

worker_pid=$!
echo "${worker_pid}" > "${PID_FILE}"
log "Live wrap started (pid ${worker_pid}, log ${LOG_FILE}, poll every ${LIVE_WRAP_POLL_SECONDS}s)"
