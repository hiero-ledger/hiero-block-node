#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# WRB Distribution E2E (#3125) — install-and-run-wrb-cli (steps 1+2).
#
# This is the first slice of the broader WRB-distribution E2E test (epic #2954).
# Steps 1+2 only: prove the CLI side of the flow in isolation, before BNs are
# layered in subsequent slices.
#
# Flow:
#   1. Build the wrb-cli (`:tools:installDist`).
#   2. Download N record files from the Solo MinIO `recordstreams` bucket
#      (reusing the same MinIO discovery + S3-API pattern as the existing
#      wrb-sequential-comparison.sh).
#   3. Package the records into day archives the wrap command expects
#      (tar.zstd-per-day with block_times.bin + day_blocks.json metadata).
#   4. Run `blocks wrap --network other` against the records.
#   5. Assert the output directory contains at least one zip at the deepest
#      level of the nested directory structure (i.e. the path
#      `000/000/000/000/00/00000s.zip` exists).
#
# Inherits NAMESPACE / CONTEXT from the harness (see solo-e2e-test runner).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPARISON_SCRIPT="${SCRIPT_DIR}/../wrb-sequential-comparison.sh"
PYTHON_DIR="${SCRIPT_DIR}/../python"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../../../.." && pwd)"

NAMESPACE="${NAMESPACE:-solo-network}"
CONTEXT="${CONTEXT:-kind-solo-cluster}"
export NAMESPACE CONTEXT

WORK_DIR="${WORK_DIR:-/tmp/wrb-distribution-step12}"
RECORDS_DIR="${WORK_DIR}/records"
DAYS_DIR="${WORK_DIR}/day-archives"
WRAPPED_DIR="${WORK_DIR}/wrappedBlocks"
MAX_RECORD_FILES="${MAX_RECORD_FILES:-100}"

# Env file consumed by the slice-2 live-wrap scripts. Written once slice-1
# finishes so start-live-wrap / assert / stop can pick up the same paths and
# already-built CLI without re-doing discovery.
ENV_FILE="${ENV_FILE:-/tmp/wrb-distribution-step12.env}"

log() { echo "[wrb-dist-step12] $*"; }
fail() { echo "[wrb-dist-step12] ERROR: $*" >&2; exit 1; }

mkdir -p "${RECORDS_DIR}" "${DAYS_DIR}" "${WRAPPED_DIR}"

# ---- 1. Build the CLI ----------------------------------------------------------
log "Building wrb-cli (:tools:installDist)..."
( cd "${REPO_ROOT}" && ./gradlew :tools:installDist -x test ) > /tmp/wrb-dist-cli-build.log 2>&1 \
    || { tail -40 /tmp/wrb-dist-cli-build.log; fail "Failed to build :tools:installDist"; }
CLI_LIB="${REPO_ROOT}/tools-and-tests/tools/build/install/tools/lib"
[[ -d "${CLI_LIB}" ]] || fail "CLI lib dir not found at ${CLI_LIB}"
log "CLI built: ${CLI_LIB}"

# ---- 2. Download records from MinIO -------------------------------------------
# Reuse `download_record_files_from_minio` from wrb-sequential-comparison.sh
# so we get the same fallback chain (MinIO label probes → MinIO mc client → CN
# pod fallback) without duplicating ~150 lines of pod-discovery logic.
[[ -f "${COMPARISON_SCRIPT}" ]] || fail "Comparison script not found at ${COMPARISON_SCRIPT}"

# Source helpers without executing the script's top-level orchestration.
# wrb-sequential-comparison.sh defines log() / kctl() at file scope and only
# invokes its main() when run directly; sourcing only registers functions.
log "Sourcing record-download helpers from wrb-sequential-comparison.sh..."
# shellcheck disable=SC1090
source "${COMPARISON_SCRIPT}"

log "Downloading up to ${MAX_RECORD_FILES} record files from MinIO..."
download_record_files_from_minio "${RECORDS_DIR}" "${MAX_RECORD_FILES}" \
    || fail "Failed to download record files from MinIO"

# Decompress any .gz files so the wrap command sees raw .rcd files.
shopt -s nullglob
gz_files=( "${RECORDS_DIR}"/*.rcd.gz )
if (( ${#gz_files[@]} > 0 )); then
    log "Decompressing ${#gz_files[@]} .rcd.gz files..."
    gunzip -f "${gz_files[@]}" || true
fi
shopt -u nullglob

rcd_count=$( find "${RECORDS_DIR}" -maxdepth 1 -name "*.rcd" | wc -l | tr -d ' ' )
[[ "${rcd_count}" -gt 0 ]] || fail "No .rcd files in ${RECORDS_DIR} after download"
log "Have ${rcd_count} record files available for wrapping"

# ---- 3. Package records into day archives + generate metadata -----------------
log "Packaging records into day archives..."
days=$( find "${RECORDS_DIR}" -name "*.rcd" -exec basename {} \; | cut -d'T' -f1 | sort -u )
for day in ${days}; do
    archive="${DAYS_DIR}/${day}.tar.zstd"
    log "  ${day}.tar.zstd"
    ( cd "${RECORDS_DIR}" && tar -cf - "${day}"T*.rcd "${day}"T*.rcd_sig 2>/dev/null | zstd -T0 > "${archive}" )
done

log "Generating block_times.bin and day_blocks.json..."
block_times_file="${WORK_DIR}/block_times.bin"
day_blocks_file="${WORK_DIR}/day_blocks.json"
# Genesis epoch nanos: use the timestamp of the first record file as a proxy
first_rcd_ts=$( find "${RECORDS_DIR}" -name "*.rcd" -exec basename {} \; | sort | head -1 | cut -d'.' -f1 )
first_dt=$( echo "${first_rcd_ts}" | sed 's/_/:/g' )
if date --version >/dev/null 2>&1; then
    first_seconds=$( date -u -d "${first_dt}Z" +%s 2>/dev/null || echo "0" )
else
    first_seconds=$( date -u -j -f "%Y-%m-%dT%H:%M:%S" "${first_dt}" +%s 2>/dev/null || echo "0" )
fi
genesis_epoch_nanos=$(( first_seconds * 1000000000 ))
python3 "${PYTHON_DIR}/generate_metadata.py" \
    "${RECORDS_DIR}" "${block_times_file}" "${day_blocks_file}" "${genesis_epoch_nanos}" \
    || fail "Failed to generate metadata"

# ---- 4. Minimal network config so the CLI accepts --network other -------------
# `blocks wrap --network other` reads a network config file pointed to by the
# HIERO_NETWORK_CONFIG env var (the same pattern used by wrb-sequential-comparison.sh).
network_config_file="${WORK_DIR}/network-other.json"
cat > "${network_config_file}" <<EOF
{
  "networkName": "solo-wrb-distribution",
  "hapiVersionAtGenesis": "0.75.0",
  "totalHbarSupplyTinybar": 5000000000000000000,
  "genesisAddressBookResource": "mainnet-genesis-address-book.proto.bin"
}
EOF

# ---- 5. Run blocks wrap -------------------------------------------------------
log "Running blocks wrap..."
HIERO_NETWORK_CONFIG="${network_config_file}" \
java -cp "${CLI_LIB}/*" \
    org.hiero.block.tools.BlockStreamTool blocks wrap \
        --network other \
        --input-dir "${DAYS_DIR}" \
        --output-dir "${WRAPPED_DIR}" \
        --blocktimes-file "${block_times_file}" \
        --day-blocks "${day_blocks_file}" \
        --skip-block-number-validation \
    > /tmp/wrb-dist-wrap.log 2>&1 \
    || { tail -40 /tmp/wrb-dist-wrap.log; fail "wrb-cli wrap failed"; }

# ---- 6. Validate nested directory structure -----------------------------------
log "Validating nested wrap output directory structure..."
deepest_zip=$( find "${WRAPPED_DIR}" -path "*/000/000/000/000/00/*.zip" 2>/dev/null | head -1 )
[[ -n "${deepest_zip}" ]] || {
    log "Wrap output tree:"
    find "${WRAPPED_DIR}" -maxdepth 8 | sed 's|^|  |' || true
    fail "Wrap output does not include a zip at the deepest nested level"
}

log "  Found nested wrap output: ${deepest_zip#${WRAPPED_DIR}/}"

# ---- 7. Hand off shared state to slice-2 (live-wrap) scripts ------------------
cat > "${ENV_FILE}" <<EOF
# Written by install-and-run-wrb-cli.sh for consumption by the slice-2
# start/assert/stop scripts.
export WRB_DIST_WORK_DIR="${WORK_DIR}"
export CLI_LIB="${CLI_LIB}"
export NAMESPACE="${NAMESPACE}"
export CONTEXT="${CONTEXT}"
EOF
log "Wrote shared env for slice-2 scripts to ${ENV_FILE}"
log "Step 1+2 slice complete."
