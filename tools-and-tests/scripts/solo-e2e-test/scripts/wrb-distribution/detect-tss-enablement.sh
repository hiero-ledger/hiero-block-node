#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# WRB Distribution E2E (#3125 slice 6 — step 11) — detect TSS enablement after
# the CN v0.76 upgrade and produce tss-bootstrap-roster.json.
#
# Canonical flow this implements (steps 1-2 of 4; see stage-tss-data-on-bn1.sh
# for steps 3-4):
#   1. CN's v0.76 upgrade creates a LedgerIdPublication transaction in the
#      record stream (cn-upgrade-tss.sh triggers this).
#   2. Block Stream CLI downloads the new record file(s), extracts the TSS
#      details from the LedgerIdPublication transaction, and writes a TSS
#      properties file to disk.
#
# TssEnablementValidation (block-verification) only fires while processing
# block number 0 — it's designed for a network that already has TSS active at
# genesis, not one upgraded mid-life. `blocks validate`'s TssEnablementValidation
# (org.hiero.block.tools.blocks.validation.TssEnablementValidation) is the
# CLI-side equivalent for exactly this scenario: it fires on the FIRST
# LedgerIdPublication transaction found in ANY block of the input, not just
# block 0 (requiresGenesisStart() == false), which is why this script — not
# BN-side backfill/verification — is the correct way to get TssData after a
# mid-life CN upgrade.
#
# Downloads only the post-upgrade record files from MinIO (anything newer than
# the last file install-and-run-wrb-cli.sh already processed), then wraps the
# FULL chain — pre-upgrade records from disk + new post-upgrade records — from
# block 0, exactly as the 160-workflow does. Using the existing pre-upgrade
# records avoids re-downloading the full bucket on each retry.
#
# Reuses the wrb-cli build + working directory install-and-run-wrb-cli.sh
# already set up (via ENV_FILE).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPARISON_SCRIPT="${SCRIPT_DIR}/../wrb-sequential-comparison.sh"
PYTHON_DIR="${SCRIPT_DIR}/../python"

ENV_FILE="${ENV_FILE:-/tmp/wrb-distribution-step12.env}"
[[ -f "${ENV_FILE}" ]] || { echo "[wrb-dist-tss-detect] ERROR: ${ENV_FILE} not found (did install-and-run-wrb-cli.sh run?)" >&2; exit 1; }
# shellcheck disable=SC1090
source "${ENV_FILE}"

: "${WRB_DIST_WORK_DIR:?WRB_DIST_WORK_DIR must be set (written by install-and-run-wrb-cli.sh)}"
: "${CLI_LIB:?CLI_LIB must be set (written by install-and-run-wrb-cli.sh)}"
: "${NAMESPACE:=solo-network}"
: "${CONTEXT:=kind-solo-cluster}"
POST_UPGRADE_MAX_RECORD_FILES="${POST_UPGRADE_MAX_RECORD_FILES:-5000}"

log() { echo "[wrb-dist-tss-detect] $*"; }
fail() { echo "[wrb-dist-tss-detect] ERROR: $*" >&2; exit 1; }

PRE_UPGRADE_RECORDS_DIR="${WRB_DIST_WORK_DIR}/records"
POST_UPGRADE_DIR="${WRB_DIST_WORK_DIR}/post-upgrade-records"
# Full-chain directories: pre + post records combined, wrapped from block 0.
# Using the existing pre-upgrade records already on disk avoids re-downloading
# everything from MinIO and matches what the 160-workflow does (wrap from genesis).
FULL_RECORDS_DIR="${WRB_DIST_WORK_DIR}/tss-detect-records"
FULL_DAYS_DIR="${WRB_DIST_WORK_DIR}/tss-detect-days"
FULL_WRAPPED_DIR="${WRB_DIST_WORK_DIR}/tss-detect-wrapped"
FULL_BLOCK_TIMES="${WRB_DIST_WORK_DIR}/tss-detect-block_times.bin"
FULL_DAY_BLOCKS="${WRB_DIST_WORK_DIR}/tss-detect-day_blocks.json"
rm -rf "${POST_UPGRADE_DIR}" "${FULL_RECORDS_DIR}" "${FULL_DAYS_DIR}" "${FULL_WRAPPED_DIR}"
mkdir -p "${POST_UPGRADE_DIR}" "${FULL_RECORDS_DIR}" "${FULL_DAYS_DIR}" "${FULL_WRAPPED_DIR}"

last_pre_upgrade_file=$(find "${PRE_UPGRADE_RECORDS_DIR}" -maxdepth 1 -name "*.rcd" -exec basename {} \; | sort | tail -1)
[[ -n "${last_pre_upgrade_file}" ]] || fail "No pre-upgrade record files found in ${PRE_UPGRADE_RECORDS_DIR}"
log "Last pre-upgrade record file already processed: ${last_pre_upgrade_file}"


# ---- Download everything currently in MinIO, then keep only files at/after
#      the last pre-upgrade one --------------------------------------------
[[ -f "${COMPARISON_SCRIPT}" ]] || fail "Comparison script not found at ${COMPARISON_SCRIPT}"
# See install-and-run-wrb-cli.sh's identical comment: sourcing clobbers our
# WORK_DIR/RECORDS_DIR/etc file-scope vars, so snapshot and restore. This also
# includes SCRIPT_DIR itself -- wrb-sequential-comparison.sh sets its own
# SCRIPT_DIR (its own file's directory, one level up from wrb-distribution/),
# and since `source` shares the caller's variable scope, that silently
# overwrites ours for the rest of this script. Every later "${SCRIPT_DIR}/.."
# reference (e.g. the extract-solo-ab-and-generate.sh call below) would
# otherwise resolve one directory too high and fail with "No such file or
# directory" -- deterministically, on every single run, not just sometimes.
_SAVED_WORK_DIR="${WRB_DIST_WORK_DIR}"
_SAVED_POST_DIR="${POST_UPGRADE_DIR}"
_SAVED_FULL_RECORDS_DIR="${FULL_RECORDS_DIR}"
_SAVED_FULL_DAYS_DIR="${FULL_DAYS_DIR}"
_SAVED_FULL_WRAPPED_DIR="${FULL_WRAPPED_DIR}"
_SAVED_SCRIPT_DIR="${SCRIPT_DIR}"
log "Sourcing record-download helpers from wrb-sequential-comparison.sh..."
# shellcheck disable=SC1090
source "${COMPARISON_SCRIPT}"
WRB_DIST_WORK_DIR="${_SAVED_WORK_DIR}"
POST_UPGRADE_DIR="${_SAVED_POST_DIR}"
FULL_RECORDS_DIR="${_SAVED_FULL_RECORDS_DIR}"
FULL_DAYS_DIR="${_SAVED_FULL_DAYS_DIR}"
FULL_WRAPPED_DIR="${_SAVED_FULL_WRAPPED_DIR}"
SCRIPT_DIR="${_SAVED_SCRIPT_DIR}"
unset _SAVED_WORK_DIR _SAVED_POST_DIR _SAVED_FULL_RECORDS_DIR _SAVED_FULL_DAYS_DIR _SAVED_FULL_WRAPPED_DIR _SAVED_SCRIPT_DIR

MAX_DETECT_RETRIES="${MAX_DETECT_RETRIES:-20}"
RETRY_INTERVAL="${RETRY_INTERVAL:-60}"
# Set false initially; subsequent loop iterations skip the 120s MinIO wait
# (already waited on attempt 1 — no need to wait again on retries).
MINIO_SKIP_INITIAL_WAIT=false

for attempt in $(seq 1 "${MAX_DETECT_RETRIES}"); do
    if [[ "${attempt}" -gt 1 ]]; then
        log "Waiting ${RETRY_INTERVAL}s before attempt ${attempt}/${MAX_DETECT_RETRIES}..."
        sleep "${RETRY_INTERVAL}"
        MINIO_SKIP_INITIAL_WAIT=true
        # Re-wipe full-chain dirs but keep POST_UPGRADE_DIR to accumulate
        # post-upgrade records across retries (already-downloaded files are
        # skipped by download_record_files_from_minio's own skip logic).
        rm -rf "${FULL_RECORDS_DIR}" "${FULL_DAYS_DIR}" "${FULL_WRAPPED_DIR}"
        mkdir -p "${FULL_RECORDS_DIR}" "${FULL_DAYS_DIR}" "${FULL_WRAPPED_DIR}"
    fi

    log "Attempt ${attempt}/${MAX_DETECT_RETRIES}: downloading up to ${POST_UPGRADE_MAX_RECORD_FILES} record files from MinIO..."
    download_record_files_from_minio "${POST_UPGRADE_DIR}" "${POST_UPGRADE_MAX_RECORD_FILES}" \
        || log "WARNING: MinIO download had issues on attempt ${attempt}, continuing with what was retrieved"

    shopt -s nullglob
    gz_files=( "${POST_UPGRADE_DIR}"/*.rcd.gz )
    if (( ${#gz_files[@]} > 0 )); then
        log "Decompressing ${#gz_files[@]} new .rcd.gz file(s)..."
        gunzip -f "${gz_files[@]}" || true
    fi
    sig_gz_files=( "${POST_UPGRADE_DIR}"/*.rcd_sig.gz )
    if (( ${#sig_gz_files[@]} > 0 )); then
        log "Decompressing ${#sig_gz_files[@]} new .rcd_sig.gz file(s)..."
        gunzip -f "${sig_gz_files[@]}" || true
    fi
    shopt -u nullglob

    removed=0
    for f in "${POST_UPGRADE_DIR}"/*.rcd; do
        [[ -f "${f}" ]] || continue
        name=$(basename "${f}")
        if [[ "${name}" < "${last_pre_upgrade_file}" || "${name}" == "${last_pre_upgrade_file}" ]]; then
            rm -f "${f}" "${f%.rcd}.rcd_sig"
            removed=$(( removed + 1 ))
        fi
    done
    if [[ "${removed}" -gt 0 ]]; then
        log "Discarded ${removed} pre-upgrade record file(s)"
    fi

    new_count=$(find "${POST_UPGRADE_DIR}" -maxdepth 1 -name "*.rcd" | wc -l | tr -d ' ')
    if (( new_count == 0 )); then
        log "Attempt ${attempt}/${MAX_DETECT_RETRIES}: no post-upgrade records yet — CN may still be restarting"
        continue
    fi
    log "Attempt ${attempt}/${MAX_DETECT_RETRIES}: have ${new_count} post-upgrade record file(s)"

    # ---- Build full-chain record set (pre + post) and package into day archives ----
    # Link all pre-upgrade records (and their sig files) into the combined dir.
    # install-and-run-wrb-cli.sh may not have decompressed .rcd_sig.gz files; if
    # they're still compressed, decompress them in-place before linking.
    shopt -s nullglob
    pre_sig_gz=( "${PRE_UPGRADE_RECORDS_DIR}"/*.rcd_sig.gz )
    if (( ${#pre_sig_gz[@]} > 0 )); then
        log "Decompressing ${#pre_sig_gz[@]} pre-upgrade .rcd_sig.gz file(s)..."
        gunzip -f "${pre_sig_gz[@]}" || true
    fi
    shopt -u nullglob
    for f in "${PRE_UPGRADE_RECORDS_DIR}"/*.rcd "${PRE_UPGRADE_RECORDS_DIR}"/*.rcd_sig; do
        [[ -f "${f}" ]] && ln -sf "${f}" "${FULL_RECORDS_DIR}/"
    done
    # Link post-upgrade records and sig files alongside them.
    for f in "${POST_UPGRADE_DIR}"/*.rcd "${POST_UPGRADE_DIR}"/*.rcd_sig; do
        [[ -f "${f}" ]] && ln -sf "${f}" "${FULL_RECORDS_DIR}/"
    done

    total_count=$(find "${FULL_RECORDS_DIR}" -maxdepth 1 -name "*.rcd" | wc -l | tr -d ' ')
    log "Attempt ${attempt}: packaging ${total_count} total record file(s) (pre + post) into day archives..."
    days=$( find "${FULL_RECORDS_DIR}" -maxdepth 1 -name "*.rcd" -exec basename {} \; | cut -d'T' -f1 | sort -u )
    for day in ${days}; do
        archive="${FULL_DAYS_DIR}/${day}.tar.zstd"
        log "  ${day}.tar.zstd"
        # COPYFILE_DISABLE=1 prevents macOS tar from adding AppleDouble resource-fork sidecars.
        # shopt nullglob: *.rcd_sig glob returns empty when MinIO has no sig files.
        # -h: dereference symlinks — FULL_RECORDS_DIR contains symlinks to the actual
        # record files; without -h, tar archives symlink entries (0 bytes) instead of
        # file content, so TarZstdDayReaderUsingExec reads nothing and wraps 0 blocks.
        (
            shopt -s nullglob
            cd "${FULL_RECORDS_DIR}"
            COPYFILE_DISABLE=1 tar -hcf - "${day}"T*.rcd "${day}"T*.rcd_sig 2>/dev/null | zstd -T0 > "${archive}"
        )
    done

    # Derive genesis from the FIRST record in the combined set so the network
    # config passed to wrap reflects the actual block-0 timestamp, not a stale
    # value from a previously written file.
    first_full_record=$(find "${FULL_RECORDS_DIR}" -maxdepth 1 -name "*.rcd" | sort | { head -1; cat >/dev/null; })
    full_genesis_ts=$(basename "${first_full_record}" | sed 's/\(.*\)\.rcd.*/\1/')
    full_genesis_date=$(echo "${full_genesis_ts}" | cut -dT -f1)
    _fdt=$(echo "${full_genesis_ts}" | sed 's/_/:/g' | sed 's/Z$//')
    _fsec_part=$(echo "${_fdt}" | cut -d. -f1)
    _fnano_part=$(echo "${_fdt}" | cut -d. -f2)
    if date --version >/dev/null 2>&1; then
        _fsec=$(date -u -d "${_fsec_part}Z" +%s 2>/dev/null || echo "0")
    else
        _fsec=$(date -u -j -f "%Y-%m-%dT%H:%M:%S" "${_fsec_part}" +%s 2>/dev/null || echo "0")
    fi
    full_genesis_epoch_nanos=$(( _fsec * 1000000000 + 10#${_fnano_part} ))
    log "Attempt ${attempt}: full-chain genesis: ${full_genesis_ts} (${full_genesis_epoch_nanos} ns)"

    # Copy the pre-upgrade address book history if available; it covers genesis through
    # the TSS upgrade and is all wrap needs for RSA signature verification.
    if [[ -f "${WRB_DIST_WORK_DIR}/day-archives/addressBookHistory.json" ]]; then
        cp "${WRB_DIST_WORK_DIR}/day-archives/addressBookHistory.json" "${FULL_DAYS_DIR}/"
    else
        bash "${SCRIPT_DIR}/../extract-solo-ab-and-generate.sh" \
            "${NAMESPACE}" \
            "${_fsec}.${_fnano_part}" \
            "${FULL_DAYS_DIR}/addressBookHistory.json" \
            || log "WARNING: Could not extract address book from CN; wrap falls back to mainnet resource"
    fi

    full_network_config="${WRB_DIST_WORK_DIR}/tss-detect-network-other.json"
    cat > "${full_network_config}" <<EOF
{
  "networkName": "solo",
  "gcsBucketName": "solo-local",
  "bucketPathPrefix": "recordstreams/",
  "mirrorNodeApiUrl": "http://localhost:5551/api/v1/",
  "genesisDate": "${full_genesis_date}",
  "genesisTimestamp": "${full_genesis_ts}",
  "minNodeAccountId": 3,
  "maxNodeAccountId": 3,
  "totalHbarSupplyTinybar": 5000000000000000000,
  "genesisAddressBookResource": "mainnet-genesis-address-book.proto.bin"
}
EOF

    log "Generating block_times.bin and day_blocks.json for the full record set..."
    if ! python3 "${PYTHON_DIR}/generate_metadata.py" \
            "${FULL_RECORDS_DIR}" "${FULL_BLOCK_TIMES}" "${FULL_DAY_BLOCKS}" "${full_genesis_epoch_nanos}"; then
        log "Attempt ${attempt}: generate_metadata failed, will retry"
        continue
    fi

    # ---- Wrap from block 0 ----
    log "Attempt ${attempt}: running blocks wrap on ${total_count} record file(s) from block 0..."
    # Initialize wrap_exit before the java command so that `set -e` does not kill
    # the script on a non-zero java exit before `wrap_exit=$?` can capture it.
    wrap_exit=0
    HIERO_NETWORK_CONFIG="${full_network_config}" \
    java -cp "${CLI_LIB}/*" \
        org.hiero.block.tools.BlockStreamTool blocks wrap \
            --network other \
            --input-dir "${FULL_DAYS_DIR}" \
            --output-dir "${FULL_WRAPPED_DIR}" \
            --blocktimes-file "${FULL_BLOCK_TIMES}" \
            --day-blocks "${FULL_DAY_BLOCKS}" \
        > /tmp/wrb-dist-post-upgrade-wrap.log 2>&1 || wrap_exit=$?
    log "wrap log (attempt ${attempt}, ${total_count} input record file(s)):"
    sed 's/^/    /' /tmp/wrb-dist-post-upgrade-wrap.log || true
    if [[ "${wrap_exit}" -ne 0 ]]; then
        log "Attempt ${attempt}: blocks wrap failed (exit ${wrap_exit}), will retry"
        continue
    fi

    # wrap organizes output into a nested directory tree (e.g. 000/000/000/000/00/00000s.zip).
    wrapped_block_count=$(python3 -c "
import sys, zipfile
from pathlib import Path
total = 0
for zip_path in Path(sys.argv[1]).rglob('*.zip'):
    with zipfile.ZipFile(zip_path) as zf:
        total += sum(1 for n in zf.namelist() if '.blk' in n)
print(total)
" "${FULL_WRAPPED_DIR}")
    log "Wrapped output: ${wrapped_block_count} block(s) from ${total_count} record file(s)"

    # ---- Validate ----
    log "Attempt ${attempt}: running blocks validate to check for TSS enablement..."
    java -cp "${CLI_LIB}/*" \
        org.hiero.block.tools.BlockStreamTool blocks validate \
            "${FULL_WRAPPED_DIR}" \
            --no-resume \
            --skip-signatures \
            --skip-supply \
            --validate-balances=false \
        > /tmp/wrb-dist-post-upgrade-validate.log 2>&1 || true

    if grep -q "TSS ENABLED" /tmp/wrb-dist-post-upgrade-validate.log 2>/dev/null; then
        log "TSS enablement detected on attempt ${attempt}!"
        break
    fi

    # Show the last 30 lines so failures are visible without flooding the log.
    tail -30 /tmp/wrb-dist-post-upgrade-validate.log
    log "Attempt ${attempt}/${MAX_DETECT_RETRIES}: TSS ENABLED not found — LedgerIdPublication may appear in later post-upgrade records"
done

if ! grep -q "TSS ENABLED" /tmp/wrb-dist-post-upgrade-validate.log 2>/dev/null; then
    tail -60 /tmp/wrb-dist-post-upgrade-validate.log 2>/dev/null || true
    fail "TSS enablement not detected after ${MAX_DETECT_RETRIES} attempt(s) — the LedgerIdPublication transaction may not have been produced yet"
fi

tss_json_path="${FULL_WRAPPED_DIR}/tss-bootstrap-roster.json"
[[ -f "${tss_json_path}" ]] || fail "blocks validate reported TSS enabled but ${tss_json_path} was not written"
log "TSS bootstrap file written: ${tss_json_path}"

# Hand off to stage-tss-data-on-bn1.sh
echo "export TSS_BOOTSTRAP_JSON_PATH=\"${tss_json_path}\"" >> "${ENV_FILE}"
log "Recorded TSS_BOOTSTRAP_JSON_PATH in ${ENV_FILE}"
