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
# `download_record_files_from_minio` (sourced from wrb-sequential-comparison.sh,
# same as install-and-run-wrb-cli.sh) always downloads starting from the
# OLDEST file in the bucket and has no "only new since X" option, so this
# re-downloads everything currently in the bucket into a fresh directory and
# locally discards anything at/before the last file install-and-run-wrb-cli.sh
# already processed (record filenames are RFC3339-like timestamps, so lexical
# comparison is chronological comparison).
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
POST_UPGRADE_DAYS_DIR="${WRB_DIST_WORK_DIR}/post-upgrade-days"
POST_UPGRADE_WRAPPED_DIR="${WRB_DIST_WORK_DIR}/post-upgrade-wrapped"
rm -rf "${POST_UPGRADE_DIR}" "${POST_UPGRADE_DAYS_DIR}" "${POST_UPGRADE_WRAPPED_DIR}"
mkdir -p "${POST_UPGRADE_DIR}" "${POST_UPGRADE_DAYS_DIR}" "${POST_UPGRADE_WRAPPED_DIR}"

last_pre_upgrade_file=$(find "${PRE_UPGRADE_RECORDS_DIR}" -maxdepth 1 -name "*.rcd" -exec basename {} \; | sort | tail -1)
[[ -n "${last_pre_upgrade_file}" ]] || fail "No pre-upgrade record files found in ${PRE_UPGRADE_RECORDS_DIR}"
log "Last pre-upgrade record file already processed: ${last_pre_upgrade_file}"

# ---- Download everything currently in MinIO, then keep only files at/after
#      the last pre-upgrade one --------------------------------------------
[[ -f "${COMPARISON_SCRIPT}" ]] || fail "Comparison script not found at ${COMPARISON_SCRIPT}"
# See install-and-run-wrb-cli.sh's identical comment: sourcing clobbers our
# WORK_DIR/RECORDS_DIR/etc file-scope vars, so snapshot and restore.
_SAVED_WORK_DIR="${WRB_DIST_WORK_DIR}"
_SAVED_POST_DIR="${POST_UPGRADE_DIR}"
_SAVED_POST_DAYS_DIR="${POST_UPGRADE_DAYS_DIR}"
_SAVED_POST_WRAPPED_DIR="${POST_UPGRADE_WRAPPED_DIR}"
log "Sourcing record-download helpers from wrb-sequential-comparison.sh..."
# shellcheck disable=SC1090
source "${COMPARISON_SCRIPT}"
WRB_DIST_WORK_DIR="${_SAVED_WORK_DIR}"
POST_UPGRADE_DIR="${_SAVED_POST_DIR}"
POST_UPGRADE_DAYS_DIR="${_SAVED_POST_DAYS_DIR}"
POST_UPGRADE_WRAPPED_DIR="${_SAVED_POST_WRAPPED_DIR}"
unset _SAVED_WORK_DIR _SAVED_POST_DIR _SAVED_POST_DAYS_DIR _SAVED_POST_WRAPPED_DIR

log "Downloading up to ${POST_UPGRADE_MAX_RECORD_FILES} record files from MinIO..."
download_record_files_from_minio "${POST_UPGRADE_DIR}" "${POST_UPGRADE_MAX_RECORD_FILES}" \
    || fail "Failed to download record files from MinIO"

shopt -s nullglob
gz_files=( "${POST_UPGRADE_DIR}"/*.rcd.gz )
if (( ${#gz_files[@]} > 0 )); then
    log "Decompressing ${#gz_files[@]} .rcd.gz files..."
    gunzip -f "${gz_files[@]}" || true
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
log "Discarded ${removed} record file(s) already processed before the upgrade"

new_count=$(find "${POST_UPGRADE_DIR}" -maxdepth 1 -name "*.rcd" | wc -l | tr -d ' ')
(( new_count > 0 )) || fail "No record files at/after ${last_pre_upgrade_file} — CN may not have resumed record production yet after the upgrade"
log "Have ${new_count} post-upgrade record file(s) to wrap"

# ---- Package into day archives + generate metadata (mirrors install-and-run-wrb-cli.sh) ----
log "Packaging post-upgrade records into day archives..."
days=$( find "${POST_UPGRADE_DIR}" -name "*.rcd" -exec basename {} \; | cut -d'T' -f1 | sort -u )
for day in ${days}; do
    archive="${POST_UPGRADE_DAYS_DIR}/${day}.tar.zstd"
    log "  ${day}.tar.zstd"
    ( cd "${POST_UPGRADE_DIR}" && tar -cf - "${day}"T*.rcd "${day}"T*.rcd_sig 2>/dev/null | zstd -T0 > "${archive}" )
done

log "Generating block_times.bin and day_blocks.json for the post-upgrade subset..."
block_times_file="${WRB_DIST_WORK_DIR}/post-upgrade-block_times.bin"
day_blocks_file="${WRB_DIST_WORK_DIR}/post-upgrade-day_blocks.json"

# ---- Network config (mirrors install-and-run-wrb-cli.sh, scoped to this subset) ----
# `head -1` closes its end of the pipe as soon as it has a line, and with
# ~700 filenames sort's full output can exceed one pipe buffer's worth —
# sort then gets SIGPIPE mid-write and, under pipefail, kills the script.
# Draining the rest of sort's output through `cat >/dev/null` avoids that.
first_record_file=$( find "${POST_UPGRADE_DIR}" -maxdepth 1 -name "*.rcd" | sort | { head -1; cat >/dev/null; } )
genesis_timestamp=$(basename "${first_record_file}" | sed 's/\(.*\)\.rcd.*/\1/')
genesis_date=$( echo "${genesis_timestamp}" | cut -d'T' -f1 )

# genesis_epoch_nanos must be derived from the exact same full-precision
# genesis_timestamp used in network-other.json below (not a re-truncated,
# whole-second-only parse of it) — otherwise ToWrappedBlocksCommand's
# NetworkConfig-derived genesis instant and generate_metadata.py's
# relative_nanos baseline disagree by up to a second, corrupting every
# block's recorded time in block_times.bin.
first_dt=$( echo "${genesis_timestamp}" | sed 's/_/:/g' | sed 's/Z$//' )
first_seconds_part=$( echo "${first_dt}" | cut -d'.' -f1 )
first_nanos_part=$( echo "${first_dt}" | cut -d'.' -f2 )
if date --version >/dev/null 2>&1; then
    first_seconds=$( date -u -d "${first_seconds_part}Z" +%s 2>/dev/null || echo "0" )
else
    first_seconds=$( date -u -j -f "%Y-%m-%dT%H:%M:%S" "${first_seconds_part}" +%s 2>/dev/null || echo "0" )
fi
genesis_epoch_nanos=$(( first_seconds * 1000000000 + 10#${first_nanos_part} ))
python3 "${PYTHON_DIR}/generate_metadata.py" \
    "${POST_UPGRADE_DIR}" "${block_times_file}" "${day_blocks_file}" "${genesis_epoch_nanos}" \
    || fail "Failed to generate metadata for post-upgrade subset"

network_config_file="${WRB_DIST_WORK_DIR}/post-upgrade-network-other.json"
cat > "${network_config_file}" <<EOF
{
  "networkName": "solo",
  "gcsBucketName": "solo-local",
  "bucketPathPrefix": "recordstreams/",
  "mirrorNodeApiUrl": "http://localhost:5551/api/v1/",
  "genesisDate": "${genesis_date}",
  "genesisTimestamp": "${genesis_timestamp}",
  "minNodeAccountId": 3,
  "maxNodeAccountId": 3,
  "totalHbarSupplyTinybar": 5000000000000000000,
  "genesisAddressBookResource": "mainnet-genesis-address-book.proto.bin"
}
EOF

# ---- Wrap, then validate to trigger TssEnablementValidation ----
log "Running blocks wrap on the post-upgrade subset..."
HIERO_NETWORK_CONFIG="${network_config_file}" \
java -cp "${CLI_LIB}/*" \
    org.hiero.block.tools.BlockStreamTool blocks wrap \
        --network other \
        --input-dir "${POST_UPGRADE_DAYS_DIR}" \
        --output-dir "${POST_UPGRADE_WRAPPED_DIR}" \
        --blocktimes-file "${block_times_file}" \
        --day-blocks "${day_blocks_file}" \
        --skip-block-number-validation \
    > /tmp/wrb-dist-post-upgrade-wrap.log 2>&1 \
    || { tail -40 /tmp/wrb-dist-post-upgrade-wrap.log; fail "wrb-cli wrap failed on post-upgrade subset"; }

# Diagnostic visibility even on success: this is a brand-new code path (never
# exercised against a mid-life-upgrade record subset before), and a prior run
# proved wrap silently wrote only 1 of 699 expected blocks — a filtered grep
# of a few keywords wasn't enough to see why, so dump the whole log this time.
log "wrap log (from ${new_count} input record file(s)):"
sed 's/^/    /' /tmp/wrb-dist-post-upgrade-wrap.log || true

# wrap organizes output into a nested directory tree (e.g.
# 000/000/000/000/00/00000s.zip, see install-and-run-wrb-cli.sh's own
# structure check), so this must search recursively rather than maxdepth 1.
# Uses python3's zipfile module (always available; `unzip` may not be
# installed on the runner) to count actual .blk* entries across every zip
# found, giving a ground-truth count independent of validate's own reporting.
wrapped_block_count=$(python3 -c "
import sys, zipfile
from pathlib import Path
total = 0
for zip_path in Path(sys.argv[1]).rglob('*.zip'):
    with zipfile.ZipFile(zip_path) as zf:
        total += sum(1 for n in zf.namelist() if '.blk' in n)
print(total)
" "${POST_UPGRADE_WRAPPED_DIR}")
log "Wrapped output contains ${wrapped_block_count} block entry/entries (expected ~${new_count})"
if [[ "${wrapped_block_count}" -lt "${new_count}" ]]; then
    log "WARNING: wrap produced fewer blocks than input record files — see /tmp/wrb-dist-post-upgrade-wrap.log for the full wrap log"
fi

log "Running blocks validate to trigger TSS enablement detection..."
java -cp "${CLI_LIB}/*" \
    org.hiero.block.tools.BlockStreamTool blocks validate \
        "${POST_UPGRADE_WRAPPED_DIR}" \
        --no-resume \
        --skip-signatures \
        --skip-supply \
        --validate-balances=false \
    > /tmp/wrb-dist-post-upgrade-validate.log 2>&1 \
    || { tail -60 /tmp/wrb-dist-post-upgrade-validate.log; fail "wrb-cli validate failed on post-upgrade subset"; }

if ! grep -q "TSS ENABLED" /tmp/wrb-dist-post-upgrade-validate.log; then
    tail -60 /tmp/wrb-dist-post-upgrade-validate.log
    fail "TSS enablement not detected in the post-upgrade record files — the LedgerIdPublication transaction may not have been produced yet"
fi

tss_json_path="${POST_UPGRADE_WRAPPED_DIR}/tss-bootstrap-roster.json"
[[ -f "${tss_json_path}" ]] || fail "blocks validate reported TSS enabled but ${tss_json_path} was not written"
log "TSS bootstrap file written: ${tss_json_path}"

# Hand off to stage-tss-data-on-bn1.sh
echo "export TSS_BOOTSTRAP_JSON_PATH=\"${tss_json_path}\"" >> "${ENV_FILE}"
log "Recorded TSS_BOOTSTRAP_JSON_PATH in ${ENV_FILE}"
