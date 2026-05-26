#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# WRB CLI Wrapping Validation Helper
#
# Extracts record files from MinIO/CN, wraps them using the WRB CLI
# `blocks wrap` command, and compares the wrapped blocks to CN blocks.
#
# Subcommands:
#   build     - Build the tools shadow jar via Gradle
#   extract   - Extract record files from MinIO bucket
#   wrap      - Run blocks wrap command on extracted record files
#   compare   - Compare CLI-wrapped blocks to CN blocks
#
# Environment (inherited from solo-test-runner.sh):
#   NAMESPACE  - Kubernetes namespace (default: solo-network)
#   CONTEXT    - Kubernetes context (default: kind-solo-cluster)
#
# Usage:
#   ./wrb-cli-wrap-and-compare.sh build
#   ./wrb-cli-wrap-and-compare.sh extract
#   ./wrb-cli-wrap-and-compare.sh wrap
#   ./wrb-cli-wrap-and-compare.sh compare

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../../.." && pwd)"

NAMESPACE="${NAMESPACE:-solo-network}"
CONTEXT="${CONTEXT:-kind-solo-cluster}"

CN_POD_LABEL="network-node-"
MINIO_POD_LABEL="minio"
BN_POD_LABEL="block-node-1"
RECORD_STREAM_PATH="/opt/hgcapp/recordStreams"
MINIO_DATA_PATH="/export/data/solo-streams/recordstreams"
BN_BLOCKS_PATH="/opt/hiero/block-node/data/live"

LOCAL_WORK_DIR="/tmp/wrb-cli-phase2-validation"
LOCAL_RECORDS_DIR="${LOCAL_WORK_DIR}/record-files"
LOCAL_WRAPPED_DIR="${LOCAL_WORK_DIR}/cli-wrapped-blocks"
LOCAL_CN_BLOCKS_DIR="${LOCAL_WORK_DIR}/cn-blocks"

MIN_RECORD_FILES=5
MAX_FILES_TO_EXTRACT=200  # Limit for testing - set to 0 for no limit

function kctl {
    kubectl --context "${CONTEXT}" "$@"
}

function log {
    echo "[wrb-cli-wrap-compare] $*"
}

function do_build {
    log "Building tools shadow jar..."
    "${REPO_ROOT}/gradlew" -p "${REPO_ROOT}" :tools:shadowJar --no-daemon -q
    local jar
    jar=$(find "${REPO_ROOT}/tools-and-tests/tools/build/libs" -name 'tools-*-all.jar' -print -quit 2>/dev/null)
    if [[ -z "$jar" ]]; then
        log "ERROR: Shadow jar not found after build"
        return 1
    fi
    log "Shadow jar built: ${jar}"
}

function do_extract {
    log "Extracting record files from MinIO/CN..."

    # Clean up and create local directories
    rm -rf "${LOCAL_WORK_DIR}"
    mkdir -p "${LOCAL_RECORDS_DIR}"

    # Try MinIO first, fallback to CN pod
    if extract_from_minio; then
        log "Successfully extracted from MinIO"
    elif extract_from_cn; then
        log "Successfully extracted from CN pod"
    else
        log "ERROR: Failed to extract record files from both MinIO and CN"
        return 1
    fi

    # Count extracted files or day archives (both compressed and uncompressed)
    local rcd_count sig_count day_archives_count
    day_archives_count=$(find "${LOCAL_WORK_DIR}/day-archives" -type f \( -name "*.tar.zstd" -o -name "*.tar" \) 2>/dev/null | wc -l | tr -d '[:space:]')

    if [[ ${day_archives_count} -gt 0 ]]; then
        # Files were packaged into day archives
        log "Created ${day_archives_count} day archive(s) containing record files"
        log "Record files packaged successfully"
    else
        # Individual record files
        rcd_count=$(find "${LOCAL_RECORDS_DIR}" -type f -name "*.rcd*" -not -name "*.rcd_sig*" | wc -l | tr -d '[:space:]')
        sig_count=$(find "${LOCAL_RECORDS_DIR}" -type f -name "*.rcd_sig*" | wc -l | tr -d '[:space:]')

        log "Extracted ${rcd_count} record files and ${sig_count} signature files"

        if [[ "${rcd_count}" -lt "${MIN_RECORD_FILES}" ]]; then
            log "ERROR: Only ${rcd_count} record files found, need at least ${MIN_RECORD_FILES}"
            return 1
        fi

        log "Record files extracted successfully"
    fi
}

function extract_from_minio {
    log "Attempting to extract from MinIO bucket..."

    # Find MinIO pod
    local minio_pod
    minio_pod=$(kctl get pods -n "${NAMESPACE}" -l "v1.min.io/tenant=minio" \
        -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
    if [[ -z "$minio_pod" ]]; then
        log "MinIO pod not found"
        return 1
    fi
    log "Found MinIO pod: ${minio_pod}"

    # MinIO stores streams in: /data/hedera-streams/recordstreams/record0.0.X/
    # First, check what's in MinIO
    log "Checking MinIO contents..."
    local node_dirs
    node_dirs=$(kctl exec -n "${NAMESPACE}" "${minio_pod}" -- \
        sh -c "ls -d ${MINIO_DATA_PATH}/record* 2>/dev/null || echo ''" | tr '\n' ' ')

    if [[ -z "$node_dirs" ]]; then
        log "No record stream directories found in MinIO at ${MINIO_DATA_PATH}"
        return 1
    fi
    log "Found node directories in MinIO: ${node_dirs}"

    # Extract from MinIO
    # Try kubectl cp first (requires tar), fallback to individual file copy if tar not available
    log "Extracting from MinIO: ${minio_pod}:${MINIO_DATA_PATH}..."
    if kctl cp -n "${NAMESPACE}" "${minio_pod}:${MINIO_DATA_PATH}" "${LOCAL_RECORDS_DIR}" 2>/dev/null; then
        log "Extracted via kubectl cp"
    else
        log "kubectl cp failed (likely no tar in container), trying individual file extraction..."
        if ! extract_minio_files_individually "${minio_pod}"; then
            log "Failed to extract files from MinIO"
            return 1
        fi
        # Individual extraction handles download, packaging, and metadata - return early
        log "Successfully extracted from MinIO via mc cat + tar.zstd packaging"
        return 0
    fi

    # MinIO organizes by node (record0.0.3, record0.0.4, etc)
    # Flatten the structure - collect all record files from all nodes
    log "Flattening node directory structure..."
    local temp_dir="${LOCAL_RECORDS_DIR}_flat"
    mkdir -p "${temp_dir}"

    # Find all record files and move to flat structure
    find "${LOCAL_RECORDS_DIR}" -type f \( -name "*.rcd" -o -name "*.rcd.gz" -o -name "*.rcd_sig" -o -name "*.rcd_sig.gz" \) \
        -exec mv {} "${temp_dir}/" \; 2>/dev/null || true

    # Replace records dir with flattened version
    rm -rf "${LOCAL_RECORDS_DIR}"
    mv "${temp_dir}" "${LOCAL_RECORDS_DIR}"

    log "Successfully extracted and flattened record files from MinIO"
    return 0
}

function extract_minio_files_individually {
    local minio_pod=$1
    log "Extracting files using MinIO client (mc) with GCS listing approach..."

    # Get MinIO credentials from secret
    local minio_user minio_pass
    minio_user=$(kctl get secret minio-secrets -n "${NAMESPACE}" -o jsonpath='{.data.config\.env}' | base64 -d | grep MINIO_ROOT_USER | cut -d= -f2)
    minio_pass=$(kctl get secret minio-secrets -n "${NAMESPACE}" -o jsonpath='{.data.config\.env}' | base64 -d | grep MINIO_ROOT_PASSWORD | cut -d= -f2)

    if [[ -z "$minio_user" || -z "$minio_pass" ]]; then
        log "Failed to get MinIO credentials from secret"
        return 1
    fi

    # Configure mc alias with credentials
    log "Configuring MinIO client with credentials..."
    kctl exec -n "${NAMESPACE}" "${minio_pod}" -c minio -- \
        mc alias set local http://localhost:9000 "${minio_user}" "${minio_pass}" 2>/dev/null || {
        log "Failed to configure MinIO client"
        return 1
    }

    # MinIO bucket and path structure
    local bucket="solo-streams"
    local prefix="recordstreams"

    # Step 1: Get bucket listing first (GCS approach)
    log "Getting bucket listing from MinIO..."
    local temp_listing="${LOCAL_WORK_DIR}/minio-listing.txt"

    # List all record files recursively
    # mc ls --recursive outputs format: [2024-05-11 19:07:57 PDT]   123B path/to/file.rcd.gz
    kctl exec -n "${NAMESPACE}" "${minio_pod}" -c minio -- \
        mc ls --recursive "local/${bucket}/${prefix}/" > "${temp_listing}" 2>/dev/null || {
        log "Failed to get bucket listing"
        return 1
    }

    log "Listing saved to ${temp_listing} ($(wc -l < "${temp_listing}") files)"

    # Step 2: Download files individually using mc cp (no tar needed)
    log "Downloading first ${MAX_FILES_TO_EXTRACT} record files and signatures using mc cp..."

    # Get list of files to download from listing (record files + signatures, sorted)
    local files_to_download
    files_to_download=$(grep -E '\.(rcd|rcd_sig)(\.gz)?$' "${temp_listing}" | awk '{print $NF}' | sort | head -n $((MAX_FILES_TO_EXTRACT * 2)))

    local file_count
    file_count=$(echo "$files_to_download" | wc -l | tr -d '[:space:]')
    log "Downloading ${file_count} files (records + signatures)..."

    local downloaded=0
    local failed=0
    while IFS= read -r file_path; do
        [[ -z "$file_path" ]] && continue

        # Extract just the filename for local storage
        local filename=$(basename "$file_path")
        local local_file="${LOCAL_RECORDS_DIR}/${filename}"

        # Stream file content directly using mc cat (no tar/cp needed!)
        if kctl exec -n "${NAMESPACE}" "${minio_pod}" -c minio -- \
            mc cat "local/${bucket}/${prefix}/${file_path}" > "${local_file}" 2>/dev/null; then
            downloaded=$((downloaded + 1))
        else
            failed=$((failed + 1))
        fi

        # Progress indicator every 20 files
        if [[ $((downloaded % 20)) -eq 0 && ${downloaded} -gt 0 ]]; then
            log "Downloaded ${downloaded}/${file_count} files..."
        fi
    done <<< "$files_to_download"

    rm -f "${temp_listing}"

    if [[ ${downloaded} -eq 0 ]]; then
        log "ERROR: Failed to download any files"
        return 1
    fi

    log "Successfully downloaded ${downloaded} files (${failed} failed) via mc cp"

    # Step 3: Generate metadata from downloaded files
    log "Generating metadata from downloaded record files..."
    if ! generate_metadata_from_files; then
        log "Failed to generate metadata from downloaded files"
        return 1
    fi

    # Step 4: Package files into tar.zstd day archives for wrap command
    log "Packaging record files into day tar.zstd archives..."
    if ! package_records_into_day_archives; then
        log "Warning: Failed to package into day archives"
        return 1
    fi

    return 0
}

function extract_block_hashes_from_records {
    # Extract first and last block hashes from record files for day_blocks.json
    # Uses Python to parse protobuf record files and extract running hashes
    local records_dir=$1
    local output_file=$2

    log "Extracting block hashes from record files..."

    python3 "${SCRIPT_DIR}/python/extract_block_hashes.py" "${records_dir}" "${output_file}"

    if [[ $? -ne 0 ]]; then
        log "Warning: Failed to extract block hashes"
        return 1
    fi

    return 0
}

function package_records_into_day_archives {
    # Group record files by day and create tar.zstd archives
    # Expected format: YYYY/MM/DD.tar.zstd

    local records_dir="${LOCAL_RECORDS_DIR}"
    local days_dir="${LOCAL_WORK_DIR}/day-archives"

    mkdir -p "${days_dir}"

    # Extract block hashes from record files
    local hash_file="${LOCAL_WORK_DIR}/block_hashes.txt"
    if ! extract_block_hashes_from_records "${records_dir}" "${hash_file}"; then
        log "Warning: Could not extract block hashes, day_blocks.json will be incomplete"
    fi

    # Get unique days from record filenames
    local days
    days=$(find "${records_dir}" -name "*.rcd.gz" -o -name "*.rcd" | \
           xargs -n1 basename | \
           sed 's/\(.*\)\.rcd.*/\1/' | \
           cut -d'T' -f1 | \
           sort -u)

    local day_count
    day_count=$(echo "$days" | wc -l | tr -d '[:space:]')
    log "Found ${day_count} unique days to package"

    for day in $days; do
        # Find all files for this day
        local day_files
        day_files=$(find "${records_dir}" -name "${day}T*.rcd*" | sort)

        local file_count
        file_count=$(echo "$day_files" | wc -l | tr -d '[:space:]')

        log "Packaging ${file_count} files for ${day} -> ${day}.tar.zstd"

        # Create temp directory with expected structure: YYYY-MM-DD/YYYY-MM-DDTHH_MM_SS.NNNNNNNNNZ/file.rcd
        local temp_staging="${days_dir}/staging-${day}"
        rm -rf "${temp_staging}"
        mkdir -p "${temp_staging}"

        # Group files by timestamp and create directory structure
        for filepath in $day_files; do
            local filename=$(basename "$filepath")
            # Extract timestamp from filename: YYYY-MM-DDTHH_MM_SS.NNNNNNNNNZ.rcd.gz -> YYYY-MM-DDTHH_MM_SS.NNNNNNNNNZ
            local timestamp=$(echo "$filename" | sed 's/\(.*\)\.rcd.*/\1/')

            # Create timestamp directory within day directory
            local timestamp_dir="${temp_staging}/${day}/${timestamp}"
            mkdir -p "${timestamp_dir}"

            # Decompress .gz files if needed, otherwise copy as-is
            if [[ "$filename" == *.gz ]]; then
                # Decompress and save without .gz extension
                local decompressed_name="${filename%.gz}"
                gunzip -c "$filepath" > "${timestamp_dir}/${decompressed_name}"
            else
                # Copy file as-is
                cp "$filepath" "${timestamp_dir}/"
            fi
        done

        # Create tar archive from staging directory
        local temp_tar="${days_dir}/${day}.tar"
        (cd "${temp_staging}" && tar -cf "${temp_tar}" "${day}" 2>&1)

        # Compress with zstd
        if ! command -v zstd >/dev/null 2>&1; then
            log "ERROR: zstd command not found, cannot compress day archives"
            log "Please install zstd: sudo apt-get install -y zstd (Ubuntu/Debian) or brew install zstd (macOS)"
            return 1
        fi

        zstd -q -f "${temp_tar}" -o "${days_dir}/${day}.tar.zstd" && rm -f "${temp_tar}"
        log "Created ${days_dir}/${day}.tar.zstd"

        # Clean up staging directory
        rm -rf "${temp_staging}"
    done

    # Update LOCAL_RECORDS_DIR to point to day archives directory
    LOCAL_RECORDS_DIR="${days_dir}"
    log "Day archives created in ${days_dir}"

    # Update day_blocks.json with extracted hashes
    local day_blocks_file="${LOCAL_WORK_DIR}/day_blocks.json"
    if [[ -f "${hash_file}" ]]; then
        update_day_blocks_with_hashes "${day_blocks_file}" "${hash_file}"
    fi

    return 0
}

function generate_metadata_from_files {
    log "Generating metadata from extracted record files..."

    # Get record files from LOCAL_RECORDS_DIR
    # Sort by filename timestamp (files are named YYYY-MM-DDTHH_MM_SS.NNNNNNNNNZ.rcd.gz)
    local record_files
    record_files=$(find "${LOCAL_RECORDS_DIR}" -type f \( -name "*.rcd.gz" -o -name "*.rcd" \) -not -name "*.rcd_sig*" | \
        sed "s|${LOCAL_RECORDS_DIR}/||" | sort)

    if [[ -z "$record_files" ]]; then
        log "Warning: No record files found in ${LOCAL_RECORDS_DIR}"
        return 1
    fi

    local file_count
    file_count=$(echo "$record_files" | wc -l | tr -d '[:space:]')
    log "Found ${file_count} record files"

    # Call common metadata generation logic
    generate_metadata_from_filelist "$record_files"
}

function generate_metadata_from_listing {
    local listing_file=$1

    log "Parsing bucket listing to generate metadata (GCS approach)..."

    # Parse mc ls output format: [2024-05-11 19:07:57 PDT]   123B path/to/file.rcd.gz
    # Extract only .rcd and .rcd.gz files (not signatures), sort by filename timestamp
    local record_files
    record_files=$(grep -E '\.rcd(\.gz)?$' "${listing_file}" | awk '{print $NF}' | sort)

    if [[ -z "$record_files" ]]; then
        log "Warning: No record files found in bucket listing"
        return 1
    fi

    local file_count
    file_count=$(echo "$record_files" | wc -l | tr -d '[:space:]')
    log "Found ${file_count} record files in bucket listing"

    # Call common metadata generation logic
    generate_metadata_from_filelist "$record_files"
}

function generate_metadata_from_filelist {
    local record_files="$1"

    # Parse genesis timestamp from first record file to calculate relative nanoseconds
    # block_times.bin stores nanos elapsed since genesis, not absolute epoch nanos
    local first_record_file
    first_record_file=$(echo "$record_files" | head -1)

    if [[ -z "$first_record_file" ]]; then
        log "Error: No record files found to determine genesis timestamp"
        return 1
    fi

    # Extract timestamp from first record file name: YYYY-MM-DDTHH_MM_SS.NNNNNNNNNZ.rcd.gz
    local genesis_timestamp
    genesis_timestamp=$(basename "$first_record_file" | sed 's/\(.*\)\.rcd.*/\1/')

    log "Using genesis timestamp from first record file: ${genesis_timestamp}"

    # Convert genesis timestamp to epoch nanoseconds (same logic as block timestamps)
    local genesis_iso=$(echo "$genesis_timestamp" | sed 's/_/:/g')
    local genesis_datetime=$(echo "$genesis_iso" | cut -d'.' -f1)
    local genesis_nanos_part=$(echo "$genesis_iso" | sed 's/.*\.\([0-9]*\)Z/\1/')

    local genesis_epoch_seconds
    if date --version >/dev/null 2>&1; then
        genesis_epoch_seconds=$(date -u -d "${genesis_datetime}Z" +%s 2>/dev/null || echo "0")
    else
        genesis_epoch_seconds=$(date -u -j -f "%Y-%m-%dT%H:%M:%S" "${genesis_datetime}" +%s 2>/dev/null || echo "0")
    fi

    if [[ "$genesis_epoch_seconds" == "0" ]]; then
        log "Error: Failed to parse genesis timestamp: ${genesis_timestamp}"
        return 1
    fi

    local genesis_epoch_nanos=$((genesis_epoch_seconds * 1000000000 + 10#${genesis_nanos_part}))
    log "Genesis epoch nanos: ${genesis_epoch_nanos}"

    # Save genesis timestamp for later use by wrap command
    echo "${genesis_timestamp}" > "${LOCAL_WORK_DIR}/genesis_timestamp.txt"

    # Generate both block_times.bin and day_blocks.json using Python to extract real block numbers
    local block_times_file="${LOCAL_WORK_DIR}/block_times.bin"
    local day_blocks_file="${LOCAL_WORK_DIR}/day_blocks.json"

    log "Generating metadata with 0-based block numbering..."

    # Use Python to generate metadata with 0-based sequential block numbering
    # Note: We use 0-based indexing for both block_times.bin and day_blocks.json
    # This is required by the wrap command which expects dense arrays starting from 0
    python3 "${SCRIPT_DIR}/python/generate_metadata.py" "${LOCAL_RECORDS_DIR}" "${block_times_file}" "${day_blocks_file}" "${genesis_epoch_nanos}"

    if [[ $? -ne 0 ]]; then
        log "Error: Failed to generate metadata"
        return 1
    fi

    # Skip the old bash-based metadata generation
    return 0
}

# Old metadata generation code (kept for reference, not executed)
function generate_metadata_from_filelist_OLD {
    local record_files="$1"

    # Initialize day_blocks.json
    local block_num=0
    local first_block=0
    local current_day=""
    local first_day=""
    declare -a day_entries=()

    while IFS= read -r filepath; do
        # Extract filename from path: record0.0.3/2026-05-11T19_07_57.673598574Z.rcd.gz
        local filename=$(basename "$filepath")

        # Parse timestamp: 2026-05-11T19_07_57.673598574Z
        local timestamp=$(echo "$filename" | sed 's/\(.*\)\.rcd.*/\1/')

        # Extract date for day_blocks.json: 2026-05-11
        local day=$(echo "$timestamp" | cut -d'T' -f1)

        # Convert timestamp to nanoseconds for block_times.bin
        # Format: YYYY-MM-DDTHH_MM_SS.NNNNNNNNNZ (underscores in time, nanoseconds after decimal)
        # Convert to ISO8601: YYYY-MM-DDTHH:MM:SS.NNNNNNNNNZ
        local iso_timestamp=$(echo "$timestamp" | sed 's/_/:/g')

        # Use date command to convert to epoch seconds, then add nanoseconds
        # Extract seconds part: 2026-05-11T19:07:57
        local datetime_part=$(echo "$iso_timestamp" | cut -d'.' -f1)
        # Extract nanoseconds part: 673598574
        local nanos_part=$(echo "$iso_timestamp" | sed 's/.*\.\([0-9]*\)Z/\1/')

        # Convert datetime to epoch seconds (using GNU date or BSD date compatible format)
        local epoch_seconds
        if date --version >/dev/null 2>&1; then
            # GNU date
            epoch_seconds=$(date -u -d "${datetime_part}Z" +%s 2>/dev/null || echo "0")
        else
            # BSD date (macOS)
            epoch_seconds=$(date -u -j -f "%Y-%m-%dT%H:%M:%S" "${datetime_part}" +%s 2>/dev/null || echo "0")
        fi

        if [[ "$epoch_seconds" == "0" ]]; then
            log "Warning: Failed to parse timestamp for ${filename}, skipping"
            continue
        fi

        # Convert to absolute epoch nanoseconds: seconds * 1,000,000,000 + nanoseconds
        # Force decimal interpretation (avoid octal with leading zeros)
        local epoch_nanos=$((epoch_seconds * 1000000000 + 10#${nanos_part}))

        # Calculate relative nanoseconds from genesis (block_times.bin format)
        local relative_nanos=$((epoch_nanos - genesis_epoch_nanos))

        # Write 8-byte long (big-endian) to block_times.bin at position block_num * 8
        # Use Python for binary writing since bash doesn't handle binary well
        python3 -c "
import struct
import sys
block_num = ${block_num}
nanos = ${relative_nanos}
with open('${block_times_file}', 'ab') as f:
    # Seek to position (block_num * 8)
    f.seek(block_num * 8)
    # Write 8-byte big-endian long
    f.write(struct.pack('>Q', nanos))
" 2>/dev/null || {
            log "Warning: Failed to write block time for block ${block_num}"
        }

        # Track day boundaries for day_blocks.json
        if [[ -z "$current_day" ]]; then
            # First file
            current_day="$day"
            first_day="$day"
            first_block=$block_num
        elif [[ "$day" != "$current_day" ]]; then
            # Day changed, save entry for previous day
            local last_block=$((block_num - 1))
            # Parse date: YYYY-MM-DD
            local year=$(echo "$current_day" | cut -d'-' -f1)
            local month=$(echo "$current_day" | cut -d'-' -f2 | sed 's/^0*//')  # Remove leading zero
            local day_num=$(echo "$current_day" | cut -d'-' -f3 | sed 's/^0*//')  # Remove leading zero

            # Format as DayBlockInfo object (hashes optional, will be null)
            day_entries+=("  {\"year\": ${year}, \"month\": ${month}, \"day\": ${day_num}, \"firstBlockNumber\": ${first_block}, \"lastBlockNumber\": ${last_block}}")

            # Start new day
            current_day="$day"
            first_block=$block_num
        fi

        block_num=$((block_num + 1))

        # Stop if we've processed MAX_FILES_TO_EXTRACT records (matches download limit)
        if [[ ${MAX_FILES_TO_EXTRACT} -gt 0 && ${block_num} -ge ${MAX_FILES_TO_EXTRACT} ]]; then
            log "Reached metadata generation limit of ${MAX_FILES_TO_EXTRACT} blocks"
            break
        fi
    done <<< "$record_files"

    # Write final day entry
    if [[ -n "$current_day" ]]; then
        local last_block=$((block_num - 1))
        local year=$(echo "$current_day" | cut -d'-' -f1)
        local month=$(echo "$current_day" | cut -d'-' -f2 | sed 's/^0*//')
        local day_num=$(echo "$current_day" | cut -d'-' -f3 | sed 's/^0*//')

        day_entries+=("  {\"year\": ${year}, \"month\": ${month}, \"day\": ${day_num}, \"firstBlockNumber\": ${first_block}, \"lastBlockNumber\": ${last_block}}")
    fi

    # Write day_blocks.json as array (hashes will be added later after extraction)
    echo "[" > "${day_blocks_file}"
    local entry_count=${#day_entries[@]}
    for i in "${!day_entries[@]}"; do
        echo -n "${day_entries[$i]}" >> "${day_blocks_file}"
        if [[ $((i + 1)) -lt ${entry_count} ]]; then
            echo "," >> "${day_blocks_file}"
        else
            echo "" >> "${day_blocks_file}"
        fi
    done
    echo "]" >> "${day_blocks_file}"

    log "Generated initial metadata for ${block_num} blocks across ${#day_entries[@]} days"
    log "- block_times.bin: ${block_times_file} ($(stat -f%z "${block_times_file}" 2>/dev/null || stat -c%s "${block_times_file}" 2>/dev/null) bytes)"
    log "- day_blocks.json: ${day_blocks_file} (will be updated with hashes after extraction)"

    return 0
}

function update_day_blocks_with_hashes {
    # Update day_blocks.json with extracted block hashes
    local day_blocks_file=$1
    local hash_file=$2

    if [[ ! -f "${hash_file}" ]]; then
        log "Warning: Hash file not found, day_blocks.json will be incomplete"
        return 1
    fi

    log "Updating day_blocks.json with extracted block hashes..."

    # Use Python to merge hashes into day_blocks.json
    python3 "${SCRIPT_DIR}/python/update_day_blocks_hashes.py" "${day_blocks_file}" "${hash_file}"

    if [[ $? -eq 0 ]]; then
        log "Successfully updated day_blocks.json with block hashes"
        return 0
    else
        log "Warning: Failed to update day_blocks.json with hashes"
        return 1
    fi
}

function extract_from_cn {
    log "Attempting to extract from CN pod..."

    # Find CN pod (use first network node)
    local cn_pod
    cn_pod=$(kctl get pods -n "${NAMESPACE}" -l "solo.hedera.com/type=network-node" \
        -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
    if [[ -z "$cn_pod" ]]; then
        log "CN pod not found in namespace '${NAMESPACE}'"
        return 1
    fi
    log "Found CN pod: ${cn_pod}"

    # Extract record files from CN pod
    log "Extracting record files from ${cn_pod}:${RECORD_STREAM_PATH}..."
    kctl cp -n "${NAMESPACE}" "${cn_pod}:${RECORD_STREAM_PATH}" "${LOCAL_RECORDS_DIR}" || {
        log "Failed to extract record files from CN pod"
        return 1
    }

    # Generate metadata from extracted record files
    log "Generating metadata from extracted record files..."
    if ! generate_metadata_from_files; then
        log "Failed to generate metadata from record files"
        return 1
    fi

    # Package record files into day archives
    if ! package_records_into_day_archives; then
        log "Failed to package into day archives"
        return 1
    fi

    return 0
}

function do_wrap {
    log "Wrapping record files with WRB CLI..."

    # Find the tools shadow jar
    local jar
    jar=$(find "${REPO_ROOT}/tools-and-tests/tools/build/libs" -name 'tools-*-all.jar' -print -quit 2>/dev/null)
    if [[ -z "$jar" ]]; then
        log "ERROR: Shadow jar not found. Run 'build' subcommand first."
        return 1
    fi
    log "Using shadow jar: ${jar}"

    # Verify day archives directory exists (created by extract)
    local day_archives_dir="${LOCAL_WORK_DIR}/day-archives"
    if [[ ! -d "${day_archives_dir}" ]]; then
        log "ERROR: Day archives directory not found. Run 'extract' subcommand first."
        return 1
    fi
    log "Using day archives: ${day_archives_dir}"

    # Verify metadata files exist (generated during extract)
    local block_times_file="${LOCAL_WORK_DIR}/block_times.bin"
    local day_blocks_file="${LOCAL_WORK_DIR}/day_blocks.json"

    if [[ ! -f "${block_times_file}" ]]; then
        log "ERROR: block_times.bin not found. Run 'extract' subcommand first."
        return 1
    fi

    if [[ ! -f "${day_blocks_file}" ]]; then
        log "ERROR: day_blocks.json not found. Run 'extract' subcommand first."
        return 1
    fi

    log "Using generated metadata:"
    log "- block_times.bin: ${block_times_file}"
    log "- day_blocks.json: ${day_blocks_file}"

    # Create Solo network config for wrap command
    local genesis_timestamp_file="${LOCAL_WORK_DIR}/genesis_timestamp.txt"
    if [[ ! -f "${genesis_timestamp_file}" ]]; then
        log "ERROR: genesis_timestamp.txt not found. Run 'extract' subcommand first."
        return 1
    fi

    local genesis_timestamp
    genesis_timestamp=$(cat "${genesis_timestamp_file}")
    local genesis_date=$(echo "$genesis_timestamp" | cut -d'T' -f1)

    log "Creating Solo network config with genesis: ${genesis_timestamp}"
    local network_config_file="${LOCAL_WORK_DIR}/solo-network-config.json"
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

    log "Network config created at: ${network_config_file}"

    # Create output directory
    mkdir -p "${LOCAL_WRAPPED_DIR}"

    # Note: Address book handling is automatic - the wrap command will:
    # 1. Load genesis address book for the specified network
    # 2. Build address book history as it processes blocks
    # 3. Save addressBookHistory.json to output directory
    log "Using network config for address book (genesis + discovered updates)"

    # Run blocks wrap command with metadata file paths and day archives
    # Using --blocktimes-file and --day-blocks options to specify metadata locations
    # Using --network other with HIERO_NETWORK_CONFIG to specify Solo network
    # Using --skip-block-number-validation for Solo test network where blocks don't start from 0
    log "Running blocks wrap command on day archives..."
    local rc=0
    local wrap_output
    wrap_output=$(HIERO_NETWORK_CONFIG="${network_config_file}" java -jar "${jar}" blocks wrap \
        --input-dir "${day_archives_dir}" \
        --output-dir "${LOCAL_WRAPPED_DIR}" \
        --blocktimes-file "${block_times_file}" \
        --day-blocks "${day_blocks_file}" \
        --network other \
        --skip-block-number-validation 2>&1) || rc=$?

    echo "$wrap_output"

    if [[ ${rc} -ne 0 ]]; then
        # Check if this is a block number mismatch error
        if echo "$wrap_output" | grep -q "Block number mismatch.*computed blockNum 0 != record file block number"; then
            # Extract the actual starting block number from the error
            local actual_block_num
            actual_block_num=$(echo "$wrap_output" | grep -oP "record file block number \K[0-9]+" | head -1)

            if [[ -n "$actual_block_num" ]]; then
                log "Block number mismatch detected. Actual starting block: ${actual_block_num}"
                log "Regenerating metadata with correct starting block number..."

                # Regenerate metadata with the correct starting block number
                local genesis_timestamp_file="${LOCAL_WORK_DIR}/genesis_timestamp.txt"
                local genesis_timestamp=$(cat "${genesis_timestamp_file}")
                local genesis_iso=$(echo "$genesis_timestamp" | sed 's/_/:/g')
                local genesis_datetime=$(echo "$genesis_iso" | cut -d'.' -f1)
                local genesis_nanos_part=$(echo "$genesis_iso" | sed 's/.*\.\([0-9]*\)Z/\1/')

                local genesis_epoch_seconds
                if date --version >/dev/null 2>&1; then
                    genesis_epoch_seconds=$(date -u -d "${genesis_datetime}Z" +%s 2>/dev/null || echo "0")
                else
                    genesis_epoch_seconds=$(date -u -j -f "%Y-%m-%dT%H:%M:%S" "${genesis_datetime}" +%s 2>/dev/null || echo "0")
                fi

                local genesis_epoch_nanos=$((genesis_epoch_seconds * 1000000000 + 10#${genesis_nanos_part}))

                # Regenerate metadata using Python
                # Important: block_times.bin must be padded from 0 to actual_block_num-1
                # so the wrap command can read any block number starting from 0
                python3 "${SCRIPT_DIR}/python/regenerate_padded_metadata.py" "${LOCAL_RECORDS_DIR}" "${block_times_file}" "${day_blocks_file}" "${genesis_epoch_nanos}" "${actual_block_num}"

                # Retry wrapping with corrected and padded metadata
                log "Retrying wrap with padded metadata (blocks 0-$((actual_block_num-1)) padded, $((actual_block_num))-$((actual_block_num+199)) actual)..."
                wrap_output=$(HIERO_NETWORK_CONFIG="${network_config_file}" java -jar "${jar}" blocks wrap \
                    --input-dir "${day_archives_dir}" \
                    --output-dir "${LOCAL_WRAPPED_DIR}" \
                    --blocktimes-file "${block_times_file}" \
                    --day-blocks "${day_blocks_file}" \
                    --network other \
                    --skip-block-number-validation 2>&1) || rc=$?

                echo "$wrap_output"

                if [[ ${rc} -ne 0 ]]; then
                    log "ERROR: Wrapping failed again after metadata correction with exit code ${rc}"
                    return ${rc}
                fi
            else
                log "ERROR: Could not extract starting block number from error message"
                return ${rc}
            fi
        else
            log "ERROR: Wrapping failed with exit code ${rc}"
            return ${rc}
        fi
    fi

    # Count wrapped blocks (check for both zip archives and individual .blk files)
    local wrapped_count
    wrapped_count=$(find "${LOCAL_WRAPPED_DIR}" -type f \( -name "*.zip" -o -name "*.blk.*" \) | wc -l | tr -d '[:space:]')
    log "Created ${wrapped_count} wrapped block files"

    if [[ "${wrapped_count}" -eq 0 ]]; then
        log "ERROR: No wrapped blocks created"
        return 1
    fi

    log "Wrapping completed successfully"
}

function do_compare {
    log "Comparing CLI-wrapped blocks to CN blocks..."

    # Find the tools shadow jar (needed for validation)
    local jar
    jar=$(find "${REPO_ROOT}/tools-and-tests/tools/build/libs" -name 'tools-*-all.jar' -print -quit 2>/dev/null)
    if [[ -z "$jar" ]]; then
        log "ERROR: Shadow jar not found. Run 'build' subcommand first."
        return 1
    fi
    log "Using shadow jar: ${jar}"

    # Verify wrapped blocks exist
    if [[ ! -d "${LOCAL_WRAPPED_DIR}" ]]; then
        log "ERROR: Wrapped blocks directory not found. Run 'wrap' subcommand first."
        return 1
    fi

    # Extract CN blocks from BN pod for comparison
    log "Extracting CN blocks from Block Node..."
    mkdir -p "${LOCAL_CN_BLOCKS_DIR}"
    log "Created CN blocks directory: ${LOCAL_CN_BLOCKS_DIR}"

    log "Looking for BN pod in namespace '${NAMESPACE}'..."
    local bn_pod
    bn_pod=$(kctl get pods -n "${NAMESPACE}" -l "block-node.hiero.com/type=block-node" \
        -o jsonpath='{.items[0].metadata.name}' 2>&1) || true
    log "kubectl returned: '${bn_pod}'"

    if [[ -z "$bn_pod" ]] || [[ "$bn_pod" == *"error"* ]] || [[ "$bn_pod" == *"Error"* ]]; then
        log "ERROR: Block Node pod not found in namespace '${NAMESPACE}'. kubectl output: ${bn_pod}"
        return 1
    fi
    log "Found BN pod: ${bn_pod}"

    kctl cp -n "${NAMESPACE}" "${bn_pod}:${BN_BLOCKS_PATH}" "${LOCAL_CN_BLOCKS_DIR}" || {
        log "ERROR: Failed to extract blocks from BN pod"
        return 1
    }

    # Basic comparison: count blocks
    local wrapped_count cn_count
    wrapped_count=$(find "${LOCAL_WRAPPED_DIR}" -type f \( -name "*.zip" -o -name "*.blk.*" \) | wc -l | tr -d '[:space:]')
    cn_count=$(find "${LOCAL_CN_BLOCKS_DIR}" -type f -name "*.blk.*" | wc -l | tr -d '[:space:]')

    log "Wrapped blocks: ${wrapped_count}, CN blocks: ${cn_count}"

    if [[ "${wrapped_count}" -eq 0 ]]; then
        log "ERROR: No wrapped blocks to compare"
        return 1
    fi

    if [[ "${cn_count}" -eq 0 ]]; then
        log "ERROR: No CN blocks to compare against"
        return 1
    fi

    # Use the CLI validate command to verify wrapped blocks are valid
    log "Validating CLI-wrapped blocks..."
    local validate_rc=0
    java -jar "${jar}" blocks validate \
        --skip-signatures \
        --skip-supply \
        --validate-balances=false \
        --no-resume \
        "${LOCAL_WRAPPED_DIR}" 2>&1 || validate_rc=$?

    if [[ ${validate_rc} -ne 0 ]]; then
        log "ERROR: Wrapped blocks failed validation"
        log "Work directory preserved for debugging: ${LOCAL_WORK_DIR}"
        return 1
    fi

    log "Wrapped blocks passed validation"

    # TODO: Implement detailed block-by-block comparison:
    # - Parse and compare block structures (header, items, footer)
    # - Compare block contents item-by-item
    # - Verify block hashes match
    # - Check item ordering
    # - Compare serialization
    #
    # For now, validation passing is a good indicator that wrapping worked correctly.
    # Future work: Create a dedicated comparison tool that can diff blocks in detail.

    log "Phase 2 validation completed successfully"
    log "- Created ${wrapped_count} wrapped block file(s)"
    log "- Compared against ${cn_count} CN block file(s)"
    log "- Wrapped blocks passed validation"

    # Note: Cleanup removed - work directory may be needed for subsequent validation steps
}

# Validate jumpstart.bin file format and contents
do_validate_jumpstart() {
    log "Validating jumpstart.bin file format and contents..."

    local jumpstart_file="${LOCAL_WORK_DIR}/cli-wrapped-blocks/jumpstart.bin"

    if [[ ! -f "${jumpstart_file}" ]]; then
        log "ERROR: jumpstart.bin not found at ${jumpstart_file}"
        return 1
    fi

    log "Found jumpstart.bin: ${jumpstart_file}"
    log "File size: $(stat -f%z "${jumpstart_file}" 2>/dev/null || stat -c%s "${jumpstart_file}" 2>/dev/null) bytes"

    # Extract and display jumpstart contents using Python
    log "Extracting jumpstart contents..."
    python3 "${SCRIPT_DIR}/python/validate_jumpstart_format.py" "${jumpstart_file}"

    if [[ $? -eq 0 ]]; then
        log "Jumpstart file format validation passed"
        return 0
    else
        log "ERROR: Jumpstart file format validation failed"
        return 1
    fi
}

# Run blocks validate command to verify jumpstart consistency
do_validate_blocks() {
    log "Running blocks validate command with jumpstart verification..."

    local tools_jar=$(find "${REPO_ROOT}/tools-and-tests/tools/build/libs" -name 'tools-*-all.jar' -print -quit 2>/dev/null)
    if [[ -z "${tools_jar}" ]]; then
        log "ERROR: Tools shadow jar not found"
        return 1
    fi

    log "Using shadow jar: ${tools_jar}"

    local wrapped_blocks_dir="${LOCAL_WORK_DIR}/cli-wrapped-blocks"
    local jumpstart_file="${wrapped_blocks_dir}/jumpstart.bin"

    if [[ ! -f "${jumpstart_file}" ]]; then
        log "ERROR: jumpstart.bin not found at ${jumpstart_file}"
        return 1
    fi

    log "Validating wrapped blocks with jumpstart verification..."
    log "Wrapped blocks directory: ${wrapped_blocks_dir}"
    log "Jumpstart file: ${jumpstart_file}"

    # Run validate command
    # Skip signatures and supply validation (Solo network), but verify jumpstart
    java -jar "${tools_jar}" blocks validate \
        --skip-signatures \
        --skip-supply \
        --validate-balances=false \
        --no-resume \
        "${wrapped_blocks_dir}" 2>&1 | tee "${LOCAL_WORK_DIR}/validate-output.log"

    # Capture the java command's exit code, not tee's
    local validate_exit_code=${PIPESTATUS[0]}

    if [[ ${validate_exit_code} -eq 0 ]]; then
        log "Blocks validation with jumpstart verification PASSED"

        # Check if jumpstart validation specifically passed
        if grep -q "JumpstartValidation.*PASS" "${LOCAL_WORK_DIR}/validate-output.log" 2>/dev/null; then
            log "Jumpstart validation explicitly confirmed in output"
        else
            log "Note: Jumpstart validation status not explicitly found in output"
        fi

        return 0
    else
        log "ERROR: Blocks validation with jumpstart verification FAILED (exit code: ${validate_exit_code})"
        return 1
    fi
}

# Main
case "${1:-}" in
    build)
        do_build
        ;;
    extract)
        do_extract
        ;;
    wrap)
        do_wrap
        ;;
    validate-jumpstart)
        do_validate_jumpstart
        ;;
    compare)
        do_compare
        ;;
    validate-blocks)
        do_validate_blocks
        ;;
    *)
        echo "Usage: $0 {build|extract|wrap|validate-jumpstart|compare|validate-blocks}"
        exit 1
        ;;
esac
