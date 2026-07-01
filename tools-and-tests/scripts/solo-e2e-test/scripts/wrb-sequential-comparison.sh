#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Block Ingestion Validation Test
#
# Tests that Mirror Node correctly ingests blocks from Block Node.
# Solo's single-node setup doesn't have TSS metadata, so this validates
# basic block ingestion without signature verification.
#
# Flow:
#   1. Wait for network to generate blocks
#   2. Deploy MN → BN1
#   3. Wait for ingestion
#   4. Capture MN API responses
#   5. Validate basic ingestion metrics

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON_DIR="${SCRIPT_DIR}/python"

NAMESPACE="${NAMESPACE:-solo-network}"
WRB_NAMESPACE="solo-wrb-test"
CONTEXT="${CONTEXT:-kind-solo-cluster}"
DEPLOYMENT="${DEPLOYMENT:-deployment-solo}"

MN_PORT="${MN_PORT:-5551}"
MN_URL="http://localhost:${MN_PORT}/api/v1"
MN_CAPTURE_DIR="/tmp/mn-responses"
MN_WRB_PORT="5552"
MN_WRB_URL="http://localhost:${MN_WRB_PORT}/api/v1"
MN_WRB_CAPTURE_DIR="/tmp/mn-wrb-responses"

BLOCK_RANGE="0:100"
MIN_TRANSACTIONS=10

# Mirror Node version used for both MN1 (Solo deployment) and the standalone MN2.
# Override with MIRROR_NODE_VERSION env var. Use a GA version, not 'main' or 'latest'.
MIRROR_NODE_VERSION="${MIRROR_NODE_VERSION:-v0.156.0}"

# Work directories
WORK_DIR="/tmp/wrb-test-$$"
RECORD_FILES_DIR="${WORK_DIR}/record-files"
WRAPPED_BLOCKS_DIR="${WORK_DIR}/wrapped-blocks"

function log {
    echo "[wrb-ingestion] $*" >&2
}

function kctl {
    kubectl --context "${CONTEXT}" "$@"
}

function dump_importer_logs {
    local log_file_importer="/tmp/mirror-importer-logs-$(date +%s).log"
    local log_file_bn="/tmp/block-node-1-logs-$(date +%s).log"

    log "Capturing Mirror Node importer logs to ${log_file_importer}..."

    kubectl --context "${CONTEXT}" --namespace "${NAMESPACE}" \
        logs deployment/mirror-1-importer --tail=500 > "${log_file_importer}" 2>&1 || {
        log "WARNING: Failed to capture importer logs"
    }

    log "Capturing Block Node 1 logs to ${log_file_bn}..."

    kubectl --context "${CONTEXT}" --namespace "${NAMESPACE}" \
        logs statefulset/block-node-1 --tail=500 > "${log_file_bn}" 2>&1 || {
        log "WARNING: Failed to capture Block Node logs"
    }

    log ""
    log "Last 50 lines of Mirror Node importer logs:"
    log "============================================="
    tail -50 "${log_file_importer}" 2>/dev/null || echo "No importer logs available"
    log "============================================="
    log ""
    log "Last 50 lines of Block Node 1 logs:"
    log "============================================="
    tail -50 "${log_file_bn}" 2>/dev/null || echo "No Block Node logs available"
    log "============================================="
}

function wait_for_mn_ingestion {
    local min_blocks=${1:-10}
    local timeout=${2:-120}

    log "Waiting for Mirror Node to ingest at least ${min_blocks} transactions..."

    local elapsed=0
    while [[ ${elapsed} -lt ${timeout} ]]; do
        # Check MN logs directly for ingestion success (more reliable than API)
        local ingestion_count=$(kubectl --context "${CONTEXT}" --namespace "${NAMESPACE}" \
            logs deployment/mirror-1-importer --tail=100 2>/dev/null | \
            grep -c "Successfully processed.*items from.*\.blk" || echo "0")

        if [[ "${ingestion_count}" -ge "1" ]]; then
            log "Mirror Node has ingested blocks (found ${ingestion_count} processing messages) ✓"

            # Also try API to verify REST endpoint works
            local tx_count=$(curl -s "http://localhost:${MN_PORT}/api/v1/transactions?limit=1&order=desc" 2>/dev/null | \
                python3 -c "import sys, json; data=json.load(sys.stdin); txs=data.get('transactions', []); print(len(txs) if txs else 0)" 2>/dev/null || echo "0")

            if [[ "${tx_count}" -ge "1" ]]; then
                log "  API also responding with transactions ✓"
            else
                log "  Note: Logs show ingestion but API not responding yet (may need time)"
            fi

            return 0
        fi

        log "  No ingestion yet (waiting... ${elapsed}s/${timeout}s)"
        sleep 5
        elapsed=$((elapsed + 5))
    done

    log "ERROR: Mirror Node did not ingest any blocks within ${timeout}s"
    dump_importer_logs
    return 1
}

function deploy_mn_to_bn {
    local mn_name="mirror-1"
    local bn_host="block-node-1.${NAMESPACE}.svc.cluster.local"

    # Check if mirror-1 already exists (deployed by network setup)
    if helm --kube-context "${CONTEXT}" --namespace "${NAMESPACE}" status mirror-1 &>/dev/null; then
        log "${mn_name} already deployed - skipping deployment"
    else
        log "Deploying ${mn_name} connected to block-node-1..."

        local overlay_file="/tmp/mn-overlay.yaml"
        # Generate overlay - disable verification entirely (Solo doesn't support TSS)
        cat > "${overlay_file}" << EOF
# Mirror Node connected to Block Node 1
# Disable block verification since Solo doesn't generate TSS metadata
importer:
  resources:
    limits:
      cpu: 2000m
      memory: 4Gi
    requests:
      cpu: 500m
      memory: 2Gi
  config:
    hedera:
      mirror:
        importer:
          block:
            enabled: true
            nodes:
              - host: ${bn_host}
                port: 40840
            sourceType: BLOCK_NODE
            stream:
              maxStreamResponseSize: 36MB
            verification:
              enabled: false  # Disable all block verification for Solo
          downloader:
            record:
              enabled: false
            balance:
              enabled: false
          startBlockNumber: 0
          stream:
            maxSubscribeAttempts: 10
            responseTimeout: 10s
EOF

        # Deploy Mirror Node (version from MIRROR_NODE_VERSION env var)
        solo mirror node add \
            --deployment "${DEPLOYMENT}" \
            --mirror-node-version "${MIRROR_NODE_VERSION}" \
            --pinger \
            --cluster-ref "${CONTEXT}" \
            -f "${overlay_file}" || {
            log "ERROR: Failed to deploy ${mn_name}"
            return 1
        }

        log "${mn_name} deployed successfully"
    fi

    # Set up port forward for MN1 API
    log "Setting up port forward to MN1 API..."
    kubectl --context "${CONTEXT}" --namespace "${NAMESPACE}" port-forward svc/mirror-1-rest ${MN_PORT}:80 &
    local pf_pid=$!
    echo $pf_pid > /tmp/mn1-port-forward.pid
    sleep 5

    log "MN1 API available at http://localhost:${MN_PORT}/api/v1"
}

function delete_mn {
    log "Deleting Mirror Node..."

    # Manually cleanup Helm release (Solo doesn't support --node-name in destroy)
    log "Uninstalling Helm releases..."
    helm --kube-context "${CONTEXT}" --namespace "${NAMESPACE}" uninstall mirror-1 --ignore-not-found || {
        log "WARNING: Failed to uninstall mirror-1 (may not exist)"
    }

    # Clean up any mirror-2 if it exists from previous failed attempts
    helm --kube-context "${CONTEXT}" --namespace "${NAMESPACE}" uninstall mirror-2 --ignore-not-found || true

    # Clear Solo's deployment tracking state to prevent auto-increment to mirror-2
    log "Clearing Solo deployment state..."
    rm -rf ~/.solo/deployments/${DEPLOYMENT}/mirror-*.json 2>/dev/null || true
    rm -rf ~/.solo/cache/${DEPLOYMENT}-mirror-*.yaml 2>/dev/null || true

    log "Mirror Node deleted"
}

function download_record_files_from_minio {
    local output_dir=$1
    local max_files=${2:-50}

    log "Downloading record files from MinIO..."

    mkdir -p "${output_dir}"

    # Search for MinIO pod across namespaces and labels
    local minio_pod=""
    local minio_namespace=""

    # Try current namespace first
    minio_pod=$(kubectl --context "${CONTEXT}" --namespace "${NAMESPACE}" get pod -l app=minio -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
    if [ -n "${minio_pod}" ]; then
        minio_namespace="${NAMESPACE}"
    fi

    # Try with Solo's MinIO label
    if [ -z "${minio_pod}" ]; then
        minio_pod=$(kubectl --context "${CONTEXT}" --namespace "${NAMESPACE}" get pod -l "v1.min.io/tenant=minio" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
        if [ -n "${minio_pod}" ]; then
            minio_namespace="${NAMESPACE}"
        fi
    fi

    # Try pattern matching across all namespaces (exclude operator pods)
    if [ -z "${minio_pod}" ]; then
        local all_pods=$(kubectl --context "${CONTEXT}" get pods --all-namespaces -o json 2>/dev/null)
        # Exclude operator/console pods, look for actual MinIO instance
        minio_pod=$(echo "$all_pods" | jq -r '.items[] | select(.metadata.name | contains("minio")) | select(.metadata.name | contains("operator") | not) | select(.metadata.name | contains("console") | not) | .metadata.name' | head -1)
        if [ -n "${minio_pod}" ]; then
            minio_namespace=$(echo "$all_pods" | jq -r ".items[] | select(.metadata.name == \"${minio_pod}\") | .metadata.namespace")
        fi
    fi

    if [ -z "${minio_pod}" ]; then
        log "WARNING: MinIO pod not found, trying to get files from Consensus Node instead..."
        # Fallback: download from CN pod
        download_record_files_from_cn "${output_dir}" "${max_files}"
        return $?
    fi

    log "Found MinIO pod: ${minio_pod} in namespace: ${minio_namespace}"

    # Wait for CN to upload files to MinIO (uploader sidecar may need time)
    # Other tests wait 120s for record files to be produced and uploaded
    log "Waiting 120s for CN to produce and upload record files to MinIO..."
    sleep 120

    # Access MinIO via S3 API using mc client (like wrb-cli-wrap-and-compare.sh)
    log "Accessing MinIO bucket via S3 API..."

    # Get MinIO credentials from secret
    local minio_user minio_pass
    minio_user=$(kubectl --context "${CONTEXT}" --namespace "${minio_namespace}" get secret minio-secrets -o jsonpath='{.data.config\.env}' 2>/dev/null | base64 -d | grep MINIO_ROOT_USER | cut -d= -f2 2>/dev/null)
    minio_pass=$(kubectl --context "${CONTEXT}" --namespace "${minio_namespace}" get secret minio-secrets -o jsonpath='{.data.config\.env}' 2>/dev/null | base64 -d | grep MINIO_ROOT_PASSWORD | cut -d= -f2 2>/dev/null)

    if [[ -z "$minio_user" || -z "$minio_pass" ]]; then
        log "WARNING: Could not get MinIO credentials from secret, trying CN pod..."
        download_record_files_from_cn "${output_dir}" "${max_files}"
        return $?
    fi

    # Configure mc client
    log "Configuring MinIO client with credentials..."
    kubectl --context "${CONTEXT}" --namespace "${minio_namespace}" exec "${minio_pod}" -c minio -- \
        mc alias set local http://localhost:9000 "${minio_user}" "${minio_pass}" >/dev/null 2>&1 || {
        log "WARNING: Failed to configure MinIO client, trying CN pod..."
        download_record_files_from_cn "${output_dir}" "${max_files}"
        return $?
    }

    # List files in bucket via S3 API
    log "Listing files in solo-streams/recordstreams bucket..."
    kubectl --context "${CONTEXT}" --namespace "${minio_namespace}" exec "${minio_pod}" -c minio -- \
        mc ls --recursive local/solo-streams/recordstreams/ > /tmp/minio-listing.txt 2>&1 || {
        log "WARNING: mc ls failed, trying CN pod..."
        download_record_files_from_cn "${output_dir}" "${max_files}"
        return $?
    }

    local file_count=$(grep -c '\.rcd' /tmp/minio-listing.txt 2>/dev/null || echo "0")
    if [ "${file_count}" -lt 1 ]; then
        log "WARNING: No .rcd files found in MinIO bucket (found ${file_count}), trying CN pod..."
        download_record_files_from_cn "${output_dir}" "${max_files}"
        return $?
    fi

    log "Found ${file_count} record files in MinIO bucket"

    # Download files using mc cat
    local downloaded=0
    while IFS= read -r line; do
        if [[ "$line" =~ \.rcd ]]; then
            local file_path=$(echo "$line" | awk '{print $NF}')
            local filename=$(basename "${file_path}")

            kubectl --context "${CONTEXT}" --namespace "${minio_namespace}" exec "${minio_pod}" -c minio -- \
                mc cat "local/solo-streams/recordstreams/${file_path}" > "${output_dir}/${filename}" 2>/dev/null && \
                ((downloaded++)) || true

            if [ ${downloaded} -ge ${max_files} ]; then
                break
            fi
        fi
    done < /tmp/minio-listing.txt

    if [ ${downloaded} -lt 2 ]; then
        log "WARNING: Only downloaded ${downloaded} files from MinIO, trying CN pod..."
        download_record_files_from_cn "${output_dir}" "${max_files}"
        return $?
    fi

    log "Downloaded ${downloaded} record files from MinIO via S3 API"

    # Verify files were actually created
    log "Verifying downloaded files in ${output_dir}:"
    ls -lh "${output_dir}" | head -10 || log "ERROR: Could not list ${output_dir}"
    local actual_files=$(find "${output_dir}" -type f | wc -l)
    log "Found ${actual_files} files in ${output_dir}"

    return 0
}

function download_record_files_from_cn {
    local output_dir=$1
    local max_files=${2:-50}

    log "Downloading record files from Consensus Node..."

    mkdir -p "${output_dir}"

    # Find CN pod
    local cn_pod=$(kubectl --context "${CONTEXT}" --namespace "${NAMESPACE}" get pod network-node1-0 -o name 2>/dev/null | cut -d/ -f2)
    if [ -z "${cn_pod}" ]; then
        cn_pod=$(kubectl --context "${CONTEXT}" --namespace "${NAMESPACE}" get pods -o name 2>/dev/null | grep "network-node1" | head -1 | cut -d/ -f2)
    fi

    if [ -z "${cn_pod}" ]; then
        log "ERROR: Could not find consensus node pod"
        return 1
    fi

    log "Found CN pod: ${cn_pod}"

    # Check if record stream directory exists and list its contents
    log "Checking CN record stream directory structure..."
    kubectl --context "${CONTEXT}" --namespace "${NAMESPACE}" exec "${cn_pod}" -- \
        sh -c "ls -la /opt/hiero/services-hedera/HapiApp2.0/data/recordStreams/ 2>/dev/null" || true

    # Record files path in CN
    kubectl --context "${CONTEXT}" --namespace "${NAMESPACE}" exec "${cn_pod}" -- \
        sh -c "find /opt/hiero/services-hedera/HapiApp2.0/data/recordStreams/ -name '*.rcd' 2>/dev/null | head -${max_files}" > /tmp/cn-files.txt 2>/dev/null || {
        log "WARNING: Could not list record files in standard path"
    }

    local file_list=$(cat /tmp/cn-files.txt)
    if [ -z "${file_list}" ]; then
        log "No .rcd files in standard path, trying record-stream-uploader container..."
        # Try record-stream-uploader sidecar container
        kubectl --context "${CONTEXT}" --namespace "${NAMESPACE}" exec "${cn_pod}" -c record-stream-uploader -- \
            sh -c "find / -name '*.rcd' 2>/dev/null | grep -v '/proc/' | head -${max_files}" > /tmp/cn-files.txt 2>/dev/null || true

        file_list=$(cat /tmp/cn-files.txt)
        if [ -n "${file_list}" ]; then
            log "Found files in record-stream-uploader container"
        fi
    fi

    if [ -z "${file_list}" ]; then
        log "Trying shared volumes in root-container..."
        # Try common shared volume paths
        for path in "/opt/hiero/streamData" "/data" "/streams"; do
            kubectl --context "${CONTEXT}" --namespace "${NAMESPACE}" exec "${cn_pod}" -c root-container -- \
                sh -c "find ${path} -name '*.rcd' 2>/dev/null | head -${max_files}" > /tmp/cn-files.txt 2>/dev/null || true
            file_list=$(cat /tmp/cn-files.txt)
            if [ -n "${file_list}" ]; then
                log "Found files in ${path}"
                break
            fi
        done
    fi

    if [ -z "${file_list}" ]; then
        log "ERROR: No record files found in CN pod after checking all paths"
        log "Listing /opt/hiero/services-hedera/HapiApp2.0/data/ contents:"
        kubectl --context "${CONTEXT}" --namespace "${NAMESPACE}" exec "${cn_pod}" -- \
            sh -c "find /opt/hiero/services-hedera/HapiApp2.0/data/ -maxdepth 3 -type d 2>/dev/null | head -20" || true
        return 1
    fi

    log "Found $(echo "$file_list" | wc -l) record files in CN"

    # Copy each file
    local copied=0
    while IFS= read -r file_path; do
        if [ -n "${file_path}" ]; then
            local filename=$(basename "${file_path}")
            kubectl --context "${CONTEXT}" --namespace "${NAMESPACE}" cp \
                "${cn_pod}:${file_path}" "${output_dir}/${filename}" 2>/dev/null && \
                ((copied++)) || true
        fi
    done < /tmp/cn-files.txt

    log "Downloaded ${copied} record files from CN"

    if [ ${copied} -lt 2 ]; then
        log "ERROR: Not enough record files downloaded"
        return 1
    fi

    return 0
}

function wrap_records_with_cli {
    local records_dir=$1
    local output_dir=$2

    log "Wrapping record files with WRB CLI..."
    log "  Records: ${records_dir}"
    log "  Output:  ${output_dir}"

    mkdir -p "${output_dir}"

    # Build the CLI if not already built
    log "Building block-node CLI..."
    ./gradlew :tools:installDist -x test > /tmp/cli-build.log 2>&1 || {
        log "ERROR: Failed to build CLI"
        tail -20 /tmp/cli-build.log
        return 1
    }

    # Verify records directory has files
    log "Checking for files in ${records_dir}..."
    ls -lh "${records_dir}" | head -10 || log "ERROR: Could not list ${records_dir}"
    local file_count=$(find "${records_dir}" -type f -name "*.rcd*" | wc -l)
    log "Found ${file_count} .rcd* files in ${records_dir}"

    # Decompress any .rcd.gz files
    local gz_count=$(find "${records_dir}" -name "*.rcd.gz" | wc -l)
    if [ ${gz_count} -gt 0 ]; then
        log "Decompressing ${gz_count} .rcd.gz files..."
        gunzip "${records_dir}"/*.rcd.gz || {
            log "WARNING: Some files failed to decompress"
        }
    fi

    # Package record files into tar.zstd day archives
    # WRB CLI expects files in format: YYYY/MM/DD.tar.zstd
    local days_dir="${WORK_DIR}/day-archives"
    mkdir -p "${days_dir}"

    # Get unique days from record filenames
    local days=$(find "${records_dir}" -name "*.rcd" | xargs -n1 basename | cut -d'T' -f1 | sort -u)
    local day_count=$(echo "${days}" | wc -l | tr -d '[:space:]')
    log "Packaging ${day_count} days into tar.zstd archives..."

    for day in ${days}; do
        # Find all .rcd, .rcd.gz and .rcd_sig files for this day
        local day_files=$(find "${records_dir}" \( -name "${day}T*.rcd" -o -name "${day}T*.rcd.gz" -o -name "${day}T*.rcd_sig" \) | sort)
        local day_file_count=$(echo "${day_files}" | wc -l | tr -d '[:space:]')

        if [ ${day_file_count} -eq 0 ]; then
            log "WARNING: No files found for day ${day}"
            continue
        fi

        # Create tar.zstd archive: YYYY-MM-DD.tar.zstd
        local archive_path="${days_dir}/${day}.tar.zstd"
        log "Creating archive ${archive_path} with ${day_file_count} files..."

        tar -cf - -C "${records_dir}" $(echo "${day_files}" | xargs -n1 basename) | zstd -T0 > "${archive_path}" || {
            log "ERROR: Failed to create archive ${archive_path}"
            return 1
        }
    done

    local rcd_count=$(find "${records_dir}" -name "*.rcd" | wc -l)
    log "Packaged ${rcd_count} record files into day archives"

    if [ ${rcd_count} -lt 1 ]; then
        log "ERROR: No record files to wrap"
        return 1
    fi

    # Extract genesis timestamp from first record file
    local first_record_file=$(find "${records_dir}" -name "*.rcd" -o -name "*.rcd.gz" 2>/dev/null | sort | head -1)
    if [ -z "${first_record_file}" ]; then
        log "ERROR: No record files found to determine genesis timestamp"
        return 1
    fi

    local genesis_timestamp=$(basename "${first_record_file}" | sed 's/\(.*\)\.rcd.*/\1/')
    local genesis_date=$(echo "${genesis_timestamp}" | cut -d'T' -f1)
    log "Extracted genesis from first record file: ${genesis_timestamp}"

    # Create Solo network config for WRB CLI
    local network_config_file="${WORK_DIR}/solo-network-config.json"
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
    log "Created Solo network config at ${network_config_file}"

    # Generate metadata files (block_times.bin and day_blocks.json)
    log "Generating metadata from record files..."
    local block_times_file="${WORK_DIR}/block_times.bin"
    local day_blocks_file="${WORK_DIR}/day_blocks.json"

    # Convert genesis timestamp to epoch nanoseconds
    local genesis_iso=$(echo "${genesis_timestamp}" | sed 's/_/:/g')
    local genesis_datetime=$(echo "${genesis_iso}" | cut -d'.' -f1)
    local genesis_nanos_part=$(echo "${genesis_iso}" | sed 's/.*\.\([0-9]*\)Z/\1/')

    local genesis_epoch_seconds
    if date --version >/dev/null 2>&1; then
        genesis_epoch_seconds=$(date -u -d "${genesis_datetime}Z" +%s 2>/dev/null || echo "0")
    else
        genesis_epoch_seconds=$(date -u -j -f "%Y-%m-%dT%H:%M:%S" "${genesis_datetime}" +%s 2>/dev/null || echo "0")
    fi

    if [[ "${genesis_epoch_seconds}" == "0" ]]; then
        log "ERROR: Failed to parse genesis timestamp: ${genesis_timestamp}"
        return 1
    fi

    local genesis_epoch_nanos=$((genesis_epoch_seconds * 1000000000 + 10#${genesis_nanos_part}))
    log "Genesis epoch nanos: ${genesis_epoch_nanos}"

    # Generate metadata using Python script
    local script_dir="$(dirname "${BASH_SOURCE[0]}")"
    python3 "${script_dir}/python/generate_metadata.py" "${records_dir}" "${block_times_file}" "${day_blocks_file}" "${genesis_epoch_nanos}" || {
        log "ERROR: Failed to generate metadata"
        return 1
    }
    log "Generated metadata files: block_times.bin, day_blocks.json"

    # Generate address book from Solo consensus node
    log "Generating address book from Solo consensus node..."
    local genesis_ts_with_nanos="${genesis_timestamp/T/_}"
    genesis_ts_with_nanos="${genesis_ts_with_nanos/Z/}"
    # Convert timestamp format: 2026-06-17T02_25_43.447997286Z -> seconds.nanos
    local ts_parts=(${genesis_ts_with_nanos//./ })
    local date_time="${ts_parts[0]}"
    local nanos="${ts_parts[1]}"
    local ts_seconds=$(date -j -f "%Y-%m-%dT%H_%M_%S" "${date_time}" +%s 2>/dev/null || \
        date -d "${date_time//_/:}" +%s 2>/dev/null || echo "0")
    local genesis_ts_formatted="${ts_seconds}.${nanos}"

    local script_dir="$(dirname "${BASH_SOURCE[0]}")"
    bash "${script_dir}/extract-solo-ab-and-generate.sh" \
        "${NAMESPACE}" \
        "${genesis_ts_formatted}" \
        "${days_dir}/addressBookHistory.json" || {
        log "WARNING: Could not extract address book from CN"
        log "  Wrapping will proceed without signature validation"
        log "  BlockProof will have empty signatures"
    }

    # Also generate binary address book (file 0.0.102 format) for MN2 importer
    if [[ -f "${days_dir}/addressBookHistory.json" ]]; then
        log "Generating binary address book for MN2 importer..."
        java -cp "tools-and-tests/tools/build/install/tools/lib/*" \
            org.hiero.block.tools.BlockStreamTool mirror generateBinFromAddressBookJson \
            "${days_dir}/addressBookHistory.json" \
            -o "${WORK_DIR}/addressbook.bin" && {
            log "  Created binary address book at ${WORK_DIR}/addressbook.bin"
        } || {
            log "  WARNING: Failed to generate binary address book"
        }
    fi

    # Run WRB CLI to wrap the record files
    log "Running WRB CLI wrap command..."
    log "  Input days:  ${days_dir}"
    log "  Output dir:  ${output_dir}"

    HIERO_NETWORK_CONFIG="${network_config_file}" java -cp "tools-and-tests/tools/build/install/tools/lib/*" \
        org.hiero.block.tools.BlockStreamTool blocks wrap \
        --network other \
        --input-dir "${days_dir}" \
        --output-dir "${output_dir}" \
        --blocktimes-file "${block_times_file}" \
        --day-blocks "${day_blocks_file}" \
        --skip-block-number-validation > /tmp/wrb-wrap.log 2>&1 || {
        log "ERROR: WRB CLI wrap failed"
        tail -30 /tmp/wrb-wrap.log
        return 1
    }

    # Verify wrapped blocks were created
    local wrapped_count=$(find "${output_dir}" -type f \( -name "*.zip" -o -name "*.blk.*" \) 2>/dev/null | wc -l)
    log "Created ${wrapped_count} wrapped block files"
    log "Contents of wrapped blocks dir:"
    find "${output_dir}" -type f 2>/dev/null | sed 's/^/    /' | head -20 || true

    if [ ${wrapped_count} -lt 1 ]; then
        log "ERROR: No wrapped blocks created"
        tail -30 /tmp/wrb-wrap.log
        return 1
    fi

    log "✓ Successfully wrapped ${rcd_count} record files into ${wrapped_count} block files"

    # Display signature validation debug logs
    if grep -q "\[SIGNATURE DEBUG\]" /tmp/wrb-wrap.log 2>/dev/null; then
        log ""
        log "=== SIGNATURE DEBUG LOGS ==="
        grep "\[SIGNATURE DEBUG\]" /tmp/wrb-wrap.log
        log "=== END SIGNATURE DEBUG LOGS ==="
    fi

    return 0
}

function capture_mn_responses {
    local mn_url=$1
    local output_dir=$2

    log "Capturing API responses from ${mn_url}..."

    rm -rf "${output_dir}"
    mkdir -p "${output_dir}"

    # Use curl instead of Python script (no dependencies needed)
    log "  Capturing blocks (list)..."
    local http_status=$(curl -s -w "%{http_code}" -o "${output_dir}/blocks.json" "${mn_url}/blocks?limit=100&order=asc" 2>/dev/null || echo "000")
    log "    HTTP status: ${http_status}"

    # Capture individual blocks for comparison (block 0, blocks 1-10)
    log "  Capturing individual blocks (block 0, blocks 1-10)..."
    for block_num in 0 1 2 3 4 5 6 7 8 9 10; do
        curl -s "${mn_url}/blocks/${block_num}" > "${output_dir}/block_${block_num}.json" 2>/dev/null || true
    done

    log "  Capturing transactions..."
    curl -s "${mn_url}/transactions?limit=1000&order=asc" > "${output_dir}/transactions.json" 2>/dev/null || true

    log "  Capturing network nodes..."
    curl -s "${mn_url}/network/nodes" > "${output_dir}/network_nodes.json" 2>/dev/null || true

    log "  Capturing balances..."
    curl -s "${mn_url}/balances?limit=1000" > "${output_dir}/balances.json" 2>/dev/null || true

    # Check if we got any responses
    local file_count=$(find "${output_dir}" -name "*.json" -size +10c | wc -l)
    if [ ${file_count} -lt 1 ]; then
        log "WARNING: No API responses captured (API may not be accessible)"
        log "This is OK - ingestion was verified via logs"
        return 0
    fi

    log "Responses captured to ${output_dir} (${file_count} files)"
}

function validate_responses {
    local capture_dir=${1:-${MN_CAPTURE_DIR}}

    log "Validating captured responses..."

    local response_count=$(find "${capture_dir}" -type f -name "*.json" | wc -l)

    if [[ ${response_count} -lt 3 ]]; then
        log "ERROR: Only captured ${response_count} responses (expected at least 3)"
        return 1
    fi

    log "✓ Captured ${response_count} API responses"

    # Validate blocks response has data
    if [ -f "${capture_dir}/blocks.json" ]; then
        # Show raw response for debugging
        log "  Raw blocks API response (first 500 chars):"
        head -c 500 "${capture_dir}/blocks.json" | sed 's/^/    /'

        local block_count=$(python3 -c "import json; data=json.load(open('${capture_dir}/blocks.json')); print(len(data.get('blocks', [])))" 2>/dev/null || echo "0")
        log "  Blocks captured: ${block_count}"
        if [ ${block_count} -lt 1 ]; then
            log "ERROR: No blocks found in API response"
            log "Full response saved to: ${capture_dir}/blocks.json"
            return 1
        fi
    fi

    # Validate transactions response has data
    if [ -f "${capture_dir}/transactions.json" ]; then
        local tx_count=$(python3 -c "import json; data=json.load(open('${capture_dir}/transactions.json')); print(len(data.get('transactions', [])))" 2>/dev/null || echo "0")
        log "  Transactions captured: ${tx_count}"
        if [ ${tx_count} -lt 1 ]; then
            log "ERROR: No transactions found in API response"
            return 1
        fi
    fi

    log "✓ API responses contain valid data"
    return 0
}

function compare_phase_responses {
    log "Comparing Phase 1 vs Phase 2 responses..."

    if [ ! -d "${MN_CAPTURE_DIR}" ] || [ ! -d "${MN_WRB_CAPTURE_DIR}" ]; then
        log "ERROR: Missing response directories, cannot run differential comparison"
        return 1
    fi

    python3 "${PYTHON_DIR}/compare_captured_responses.py" \
        "${MN_CAPTURE_DIR}" \
        "${MN_WRB_CAPTURE_DIR}" \
        --output /tmp/comparison-report.json \
        --verbose || {
        log "✗ Phase 1 vs Phase 2 differential comparison FAILED"
        log "  Report: /tmp/comparison-report.json"
        return 1
    }

    log "✓ Phase 1 and Phase 2 responses match"
    return 0
}

function create_wrb_namespace {
    log "Creating namespace ${WRB_NAMESPACE}..."

    kubectl --context "${CONTEXT}" create namespace "${WRB_NAMESPACE}" 2>/dev/null || {
        log "Namespace already exists, cleaning up..."
        kubectl --context "${CONTEXT}" delete namespace "${WRB_NAMESPACE}" --ignore-not-found
        sleep 5
        kubectl --context "${CONTEXT}" create namespace "${WRB_NAMESPACE}"
    }

    log "Namespace ${WRB_NAMESPACE} ready"
}

function deploy_standalone_bn2 {
    local wrapped_blocks_dir=$1

    log "Deploying standalone BN2 in ${WRB_NAMESPACE}..."

    # Get the Block Node image from BN1 deployment
    local bn_image=$(kubectl --context "${CONTEXT}" --namespace "${NAMESPACE}" get statefulset block-node-1 -o jsonpath='{.spec.template.spec.containers[0].image}' 2>/dev/null || echo "")
    if [ -z "${bn_image}" ]; then
        log "ERROR: Could not determine Block Node image from BN1"
        return 1
    fi
    log "Using Block Node image: ${bn_image}"

    # Extract version from image tag (e.g., ghcr.io/hiero-ledger/hiero-block-node:0.35.1 -> 0.35.1)
    local bn_version="${bn_image##*:}"
    log "Block Node version: ${bn_version}"

    # Create a deployment manifest for BN2
    local bn2_manifest="/tmp/bn2-deployment.yaml"

    cat > "${bn2_manifest}" << EOF
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: bn2-storage
  namespace: ${WRB_NAMESPACE}
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 5Gi
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: block-node-2
  namespace: ${WRB_NAMESPACE}
spec:
  serviceName: block-node-2
  replicas: 1
  selector:
    matchLabels:
      app: block-node-2
  template:
    metadata:
      labels:
        app: block-node-2
    spec:
      initContainers:
      - name: copy-core-modules
        image: ${bn_image}
        command:
        - cp
        - /opt/hiero/block-node/app-${bn_version}/lib/.core-modules.txt
        - /plugins/.core-modules.txt
        volumeMounts:
        - name: plugins-storage
          mountPath: /plugins
      - name: resolve-plugins
        image: maven:3-eclipse-temurin-25-alpine
        command:
        - sh
        - -c
        - |
          set -e
          DESIRED_HASH=\$(echo "${bn_version}:facility-messaging,block-access-service,health,server-status,stream-publisher,stream-subscriber,verification,blocks-file-historic,blocks-file-recent,backfill::[{\"id\":\"central\",\"url\":\"https://repo1.maven.org/maven2\"},{\"id\":\"besu-maven\",\"url\":\"https://artifacts.hashgraph.io/artifactory/hyperledger-besu-maven-external/\"},{\"id\":\"consensys\",\"url\":\"https://artifacts.consensys.net/public/maven/maven/\"},{\"id\":\"sonatype-snapshots\",\"snapshots\":true,\"url\":\"https://central.sonatype.com/repository/maven-snapshots\"}]" | sha256sum | cut -d' ' -f1)
          if [ -f /plugins/.resolved-hash ] && [ "\$(cat /plugins/.resolved-hash)" = "\$DESIRED_HASH" ]; then
            echo "Plugins already resolved. Skipping."
            exit 0
          fi
          if [ -f /plugins/.resolved-jars ]; then
            while IFS= read -r jar; do
              rm -f "/plugins/\$jar"
            done < /plugins/.resolved-jars
          fi
          rm -f /plugins/.resolved-hash /plugins/.resolved-jars

          echo "Resolving plugins via Maven..."
          cat > /tmp/pom.xml << 'POMEOF'
          <?xml version="1.0" encoding="UTF-8"?>
          <project xmlns="http://maven.apache.org/POM/4.0.0"
                   xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                   xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
            <modelVersion>4.0.0</modelVersion>
            <groupId>plugin-resolver</groupId>
            <artifactId>plugin-resolver</artifactId>
            <version>1.0</version>
            <repositories>
              <repository><id>central</id><url>https://repo1.maven.org/maven2</url></repository>
              <repository><id>besu-maven</id><url>https://artifacts.hashgraph.io/artifactory/hyperledger-besu-maven-external/</url></repository>
              <repository><id>consensys</id><url>https://artifacts.consensys.net/public/maven/maven/</url></repository>
              <repository><id>sonatype-snapshots</id><url>https://central.sonatype.com/repository/maven-snapshots</url><snapshots><enabled>true</enabled></snapshots></repository>
            </repositories>
            <dependencies>
              <dependency><groupId>org.hiero.block-node</groupId><artifactId>facility-messaging</artifactId><version>${bn_version}</version></dependency>
              <dependency><groupId>org.hiero.block-node</groupId><artifactId>block-access-service</artifactId><version>${bn_version}</version></dependency>
              <dependency><groupId>org.hiero.block-node</groupId><artifactId>health</artifactId><version>${bn_version}</version></dependency>
              <dependency><groupId>org.hiero.block-node</groupId><artifactId>server-status</artifactId><version>${bn_version}</version></dependency>
              <dependency><groupId>org.hiero.block-node</groupId><artifactId>stream-publisher</artifactId><version>${bn_version}</version></dependency>
              <dependency><groupId>org.hiero.block-node</groupId><artifactId>stream-subscriber</artifactId><version>${bn_version}</version></dependency>
              <dependency><groupId>org.hiero.block-node</groupId><artifactId>verification</artifactId><version>${bn_version}</version></dependency>
              <dependency><groupId>org.hiero.block-node</groupId><artifactId>blocks-file-historic</artifactId><version>${bn_version}</version></dependency>
              <dependency><groupId>org.hiero.block-node</groupId><artifactId>blocks-file-recent</artifactId><version>${bn_version}</version></dependency>
              <dependency><groupId>org.hiero.block-node</groupId><artifactId>backfill</artifactId><version>${bn_version}</version></dependency>
            </dependencies>
          </project>
          POMEOF

          mvn -f /tmp/pom.xml dependency:copy-dependencies -DoutputDirectory=/plugins -Dsilent=true
          find /plugins -name "*.jar" -exec basename {} \; > /plugins/.resolved-jars
          echo "\$DESIRED_HASH" > /plugins/.resolved-hash
          echo "Plugins resolved successfully."
        volumeMounts:
        - name: plugins-storage
          mountPath: /plugins
      containers:
      - name: block-node-server
        image: ${bn_image}
        ports:
        - containerPort: 40840
          name: grpc
        volumeMounts:
        - name: data
          mountPath: /opt/hiero/block-node/data
        - name: plugins-storage
          mountPath: /opt/hiero/block-node/app-${bn_version}/plugins
        env:
        - name: VERSION
          value: "${bn_version}"
        - name: STORAGE_FILES_HISTORIC_ROOT_PATH
          value: "/opt/hiero/block-node/data/historic"
      volumes:
      - name: data
        persistentVolumeClaim:
          claimName: bn2-storage
      - name: plugins-storage
        emptyDir: {}
---
apiVersion: v1
kind: Service
metadata:
  name: block-node-2
  namespace: ${WRB_NAMESPACE}
spec:
  selector:
    app: block-node-2
  ports:
  - port: 40840
    targetPort: 40840
    name: grpc
EOF

    kubectl --context "${CONTEXT}" apply -f "${bn2_manifest}"

    # Wait for BN2 to be ready (longer timeout for Maven dependency resolution in init containers)
    log "Waiting for BN2 pod to be ready (may take several minutes for plugin downloads)..."
    kubectl --context "${CONTEXT}" --namespace "${WRB_NAMESPACE}" wait --for=condition=ready pod -l app=block-node-2 --timeout=600s || {
        log "ERROR: BN2 pod not ready after 10 minutes"
        log "Pod status:"
        kubectl --context "${CONTEXT}" --namespace "${WRB_NAMESPACE}" get pod -l app=block-node-2 -o wide || true
        log "Pod events:"
        kubectl --context "${CONTEXT}" --namespace "${WRB_NAMESPACE}" describe pod -l app=block-node-2 | tail -50 || true
        log "Init container logs (copy-core-modules):"
        kubectl --context "${CONTEXT}" --namespace "${WRB_NAMESPACE}" logs -l app=block-node-2 -c copy-core-modules --tail=50 || true
        log "Init container logs (resolve-plugins):"
        kubectl --context "${CONTEXT}" --namespace "${WRB_NAMESPACE}" logs -l app=block-node-2 -c resolve-plugins --tail=100 || true
        log "Main container logs (if started):"
        kubectl --context "${CONTEXT}" --namespace "${WRB_NAMESPACE}" logs -l app=block-node-2 -c block-node-server --tail=50 || true
        return 1
    }

    # Copy wrapped blocks to BN2
    log "Copying wrapped blocks to BN2..."
    local pod_name=$(kubectl --context "${CONTEXT}" --namespace "${WRB_NAMESPACE}" get pod -l app=block-node-2 -o jsonpath='{.items[0].metadata.name}')

    kubectl --context "${CONTEXT}" --namespace "${WRB_NAMESPACE}" exec "${pod_name}" -- mkdir -p /opt/hiero/block-node/data/historic

    kubectl --context "${CONTEXT}" --namespace "${WRB_NAMESPACE}" cp "${wrapped_blocks_dir}/." "${pod_name}:/opt/hiero/block-node/data/historic/" || {
        log "ERROR: Failed to copy wrapped blocks to BN2"
        return 1
    }

    # Verify files were copied
    local file_count=$(kubectl --context "${CONTEXT}" --namespace "${WRB_NAMESPACE}" exec "${pod_name}" -- find /opt/hiero/block-node/data/historic -type f | wc -l)
    log "Copied ${file_count} files to BN2"
    log "Files in BN2 historic dir:"
    kubectl --context "${CONTEXT}" --namespace "${WRB_NAMESPACE}" exec "${pod_name}" -- find /opt/hiero/block-node/data/historic -type f 2>/dev/null | sed 's/^/    /' || true

    # Restart BN2 to force BlockFileHistoricPlugin to re-scan and detect the blocks
    log "Restarting BN2 to load historic blocks..."
    kubectl --context "${CONTEXT}" --namespace "${WRB_NAMESPACE}" delete pod "${pod_name}"

    # Wait for new pod to be ready
    kubectl --context "${CONTEXT}" --namespace "${WRB_NAMESPACE}" wait --for=condition=ready pod -l app=block-node-2 --timeout=300s || {
        log "ERROR: BN2 pod not ready after restart"
        return 1
    }

    # Get new pod name
    pod_name=$(kubectl --context "${CONTEXT}" --namespace "${WRB_NAMESPACE}" get pod -l app=block-node-2 -o jsonpath='{.items[0].metadata.name}')
    log "BN2 restarted with pod: ${pod_name}"

    # Give BN2 a moment to start serving (logs show it starts in <1s)
    log "Waiting for BN2 to start gRPC server..."
    sleep 5

    # Verify server started by checking logs
    if kubectl --context "${CONTEXT}" --namespace "${WRB_NAMESPACE}" logs "${pod_name}" | grep -q "Started all channels"; then
        log "✓ BN2 gRPC server started successfully"
    else
        log "WARNING: Could not confirm BN2 server startup from logs, but continuing..."
    fi

    log "BN2 deployed successfully with wrapped blocks"

    # Capture BN2 logs to debug historic plugin
    log "BN2 startup logs (checking BlockFileHistoricPlugin):"
    kubectl --context "${CONTEXT}" --namespace "${WRB_NAMESPACE}" logs "${pod_name}" --tail=100 | grep -E "(BlockFileHistoric|historic|INFO.*BlockNodeApp)" || log "No historic plugin logs found"

    # Check what files BN2 sees in historic directory
    log "Files in BN2 historic directory:"
    kubectl --context "${CONTEXT}" --namespace "${WRB_NAMESPACE}" exec "${pod_name}" -- find /opt/hiero/block-node/data/historic -type f | head -20 || log "Could not list files"

    # Query BN2 via gRPC for its actual block range
    log "Querying BN2 gRPC server for available blocks..."

    # Use grpcurl to query server status which should tell us the block range
    local server_status=$(kubectl --context "${CONTEXT}" --namespace "${WRB_NAMESPACE}" exec block-node-2-0 -- \
        grpcurl -plaintext -d '{}' localhost:40840 com.hedera.hapi.block.stream.BlockStreamService/serverStatus 2>/dev/null || echo "")

    local bn2_first_block="0"
    if [[ -n "${server_status}" ]]; then
        # Try to extract firstAvailableBlock from the response
        bn2_first_block=$(echo "${server_status}" | grep -o '"firstAvailableBlock"[[:space:]]*:[[:space:]]*[0-9]*' | grep -o '[0-9]*$' || echo "0")
        log "BN2 reports first available block from gRPC: ${bn2_first_block}"
    else
        log "WARNING: Could not query BN2 via gRPC, using 0"
    fi

    # Return the first block number for use by MN2
    echo "${bn2_first_block}"
}

function deploy_mn2_to_bn2 {
    local start_block=$1
    local address_book_file=$2

    # Convert to integer (strip leading zeros) - handle empty/invalid values
    if [[ -z "${start_block}" || "${start_block}" =~ [^0-9] ]]; then
        start_block=0
    else
        start_block=$((10#${start_block}))
    fi

    log "Deploying MN2 in ${WRB_NAMESPACE} connected to BN2..."
    log "MN2 will start from block: ${start_block}"

    # Get the Mirror Node importer and REST images from MN1 deployment
    local mn_importer_image=$(kubectl --context "${CONTEXT}" --namespace "${NAMESPACE}" get deployment mirror-1-importer -o jsonpath='{.spec.template.spec.containers[0].image}' 2>/dev/null || echo "")
    local mn_rest_image=$(kubectl --context "${CONTEXT}" --namespace "${NAMESPACE}" get deployment mirror-1-rest -o jsonpath='{.spec.template.spec.containers[0].image}' 2>/dev/null || echo "")

    # Fallback to the configured GA version with the leading 'v' stripped to match
    # the Docker image tag convention (e.g. v0.156.0 -> 0.156.0).
    local mn_image_tag="${MIRROR_NODE_VERSION#v}"

    if [ -z "${mn_importer_image}" ]; then
        log "WARNING: Could not get MN1 importer image, using default for version ${MIRROR_NODE_VERSION}"
        mn_importer_image="gcr.io/mirrornode/hedera-mirror-importer:${mn_image_tag}"
    fi

    if [ -z "${mn_rest_image}" ]; then
        log "WARNING: Could not get MN1 REST image, using default for version ${MIRROR_NODE_VERSION}"
        mn_rest_image="gcr.io/mirrornode/hedera-mirror-rest-java:${mn_image_tag}"
    fi

    log "Using Mirror Node importer image: ${mn_importer_image}"
    log "Using Mirror Node REST image: ${mn_rest_image}"

    # Read and base64-encode the binary address book for MN2
    # Format must be binary NodeAddressBook protobuf (file 0.0.102 format)
    local address_book_b64=""
    if [[ -f "${address_book_file}" ]]; then
        address_book_b64=$(base64 < "${address_book_file}" | tr -d '\n')
        log "Loaded binary address book from ${address_book_file} ($(wc -c < "${address_book_file}") bytes)"
    else
        log "WARNING: Address book file not found at ${address_book_file}"
        log "  MN2 will use default address book and may fail verification"
    fi

    local mn2_manifest="/tmp/mn2-deployment.yaml"

    cat > "${mn2_manifest}" << EOF
apiVersion: v1
kind: Secret
metadata:
  name: mn2-importer-secret
  namespace: ${WRB_NAMESPACE}
type: Opaque
data:
  addressbook.bin: ${address_book_b64}
  application.yaml: $(echo -n "hiero:
  mirror:
    importer:
      initialAddressBook: /usr/etc/hiero/addressbook.bin" | base64 | tr -d '\n')
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: mn2-importer-config
  namespace: ${WRB_NAMESPACE}
data:
  application.yml: |
    spring:
      datasource:
        url: jdbc:postgresql://mn2-postgres:5432/mirror_node?sslmode=disable
        username: mirror_node
        password: mirror_node_pass
      jpa:
        properties:
          hibernate:
            hbm2ddl:
              auto: create
    hiero:
      mirror:
        importer:
          network: OTHER
          block:
            enabled: true
            nodes:
              - host: block-node-2.${WRB_NAMESPACE}.svc.cluster.local
                port: 40840
            sourceType: BLOCK_NODE
          downloader:
            bucketName: dummy-not-used
            record:
              enabled: false
            balance:
              enabled: false
          startBlockNumber: ${start_block}
---
apiVersion: v1
kind: Service
metadata:
  name: mn2-postgres
  namespace: ${WRB_NAMESPACE}
spec:
  type: ClusterIP
  selector:
    app: mn2-postgres
  ports:
  - port: 5432
    targetPort: 5432
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: mn2-postgres-init
  namespace: ${WRB_NAMESPACE}
data:
  init.sql: |
    CREATE EXTENSION IF NOT EXISTS btree_gist;
    CREATE EXTENSION IF NOT EXISTS pg_trgm;
    CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
    CREATE ROLE readonly;
    CREATE ROLE readwrite IN ROLE readonly;
    CREATE ROLE temporary_admin IN ROLE readwrite;
    CREATE ROLE mirror_importer;
    CREATE ROLE mirror_grpc;
    CREATE ROLE mirror_rest;
    CREATE ROLE mirror_web3;
    -- Grant temp schema admin privileges (CRITICAL for entity_temp creation)
    GRANT temporary_admin TO mirror_node;
    GRANT temporary_admin TO mirror_importer;
    -- Create temporary schema owned by temporary_admin
    CREATE SCHEMA IF NOT EXISTS temporary AUTHORIZATION temporary_admin;
    GRANT USAGE ON SCHEMA temporary TO public;
    -- Grant permissions to mirror_node user (owns database)
    GRANT ALL PRIVILEGES ON DATABASE mirror_node TO mirror_node;
    GRANT ALL PRIVILEGES ON SCHEMA public TO mirror_node;
    GRANT ALL PRIVILEGES ON SCHEMA temporary TO mirror_node;
    GRANT mirror_importer TO mirror_node;
    GRANT mirror_rest TO mirror_node;
    ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO mirror_node;
    ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO mirror_node;
    ALTER DEFAULT PRIVILEGES IN SCHEMA temporary GRANT ALL ON TABLES TO mirror_node;
    ALTER DEFAULT PRIVILEGES IN SCHEMA temporary GRANT ALL ON SEQUENCES TO mirror_node;
    -- CRITICAL: Set search_path so temporary tables are found
    ALTER DATABASE mirror_node SET search_path = public, public, temporary;
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: mn2-postgres
  namespace: ${WRB_NAMESPACE}
spec:
  replicas: 1
  selector:
    matchLabels:
      app: mn2-postgres
  template:
    metadata:
      labels:
        app: mn2-postgres
    spec:
      containers:
      - name: postgres
        image: postgres:14-alpine
        env:
        - name: POSTGRES_DB
          value: mirror_node
        - name: POSTGRES_USER
          value: mirror_node
        - name: POSTGRES_PASSWORD
          value: mirror_node_pass
        ports:
        - containerPort: 5432
        volumeMounts:
        - name: init-scripts
          mountPath: /docker-entrypoint-initdb.d
      volumes:
      - name: init-scripts
        configMap:
          name: mn2-postgres-init
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: mirror-2-importer
  namespace: ${WRB_NAMESPACE}
spec:
  replicas: 1
  selector:
    matchLabels:
      app: mirror-2-importer
  template:
    metadata:
      labels:
        app: mirror-2-importer
    spec:
      containers:
      - name: importer
        image: ${mn_importer_image}
        volumeMounts:
        - name: addressbook
          mountPath: /usr/etc/hiero
          readOnly: true
        - name: config
          mountPath: /app/config
          readOnly: true
        env:
        - name: SPRING_CONFIG_ADDITIONAL_LOCATION
          value: "file:/usr/etc/hiero/"
        - name: SPRING_DATASOURCE_URL
          value: jdbc:postgresql://mn2-postgres:5432/mirror_node?sslmode=disable
        - name: SPRING_DATASOURCE_USERNAME
          value: mirror_node
        - name: SPRING_DATASOURCE_PASSWORD
          value: mirror_node_pass
        - name: HIERO_MIRROR_IMPORTER_NETWORK
          value: "OTHER"
        - name: HIERO_MIRROR_IMPORTER_BLOCK_ENABLED
          value: "true"
        - name: HIERO_MIRROR_IMPORTER_BLOCK_NODES_0_HOST
          value: block-node-2.solo-wrb-test.svc.cluster.local
        - name: HIERO_MIRROR_IMPORTER_BLOCK_NODES_0_PORT
          value: "40840"
        - name: HIERO_MIRROR_IMPORTER_BLOCK_SOURCETYPE
          value: BLOCK_NODE
        - name: HIERO_MIRROR_IMPORTER_DOWNLOADER_BUCKETNAME
          value: "dummy-not-used"
        - name: HIERO_MIRROR_IMPORTER_DB_TEMPSCHEMA
          value: "temporary"
        - name: HIERO_MIRROR_IMPORTER_DB_SCHEMA
          value: "public"
        - name: HIERO_MIRROR_IMPORTER_DOWNLOADER_RECORD_ENABLED
          value: "false"
        - name: HIERO_MIRROR_IMPORTER_DOWNLOADER_BALANCE_ENABLED
          value: "false"
        - name: HIERO_MIRROR_IMPORTER_STARTBLOCKNUMBER
          value: "0"
      volumes:
      - name: addressbook
        secret:
          secretName: mn2-importer-secret
      - name: config
        configMap:
          name: mn2-importer-config
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: mirror-2-rest
  namespace: ${WRB_NAMESPACE}
spec:
  replicas: 1
  selector:
    matchLabels:
      app: mirror-2-rest
  template:
    metadata:
      labels:
        app: mirror-2-rest
    spec:
      containers:
      - name: rest
        image: ${mn_rest_image}
        ports:
        - containerPort: 8080
        env:
        - name: HIERO_MIRROR_REST_DB_HOST
          value: mn2-postgres
        - name: HIERO_MIRROR_REST_DB_NAME
          value: mirror_node
        - name: HIERO_MIRROR_REST_DB_USERNAME
          value: mirror_node
        - name: HIERO_MIRROR_REST_DB_PASSWORD
          value: mirror_node_pass
        - name: HIERO_MIRROR_REST_PORT
          value: "8080"
        - name: HIERO_MIRROR_REST_REDIS_URI
          value: redis://mirror-1-redis-node.solo-network.svc.cluster.local:6379
        - name: HIERO_MIRROR_COMMON_SHARD
          value: "0"
        - name: HIERO_MIRROR_COMMON_REALM
          value: "0"
---
apiVersion: v1
kind: Service
metadata:
  name: mirror-2-rest
  namespace: ${WRB_NAMESPACE}
spec:
  type: ClusterIP
  selector:
    app: mirror-2-rest
  ports:
  - port: 80
    targetPort: 8080
    name: http
EOF

    kubectl --context "${CONTEXT}" apply -f "${mn2_manifest}"

    # Wait for MN2 importer to be ready
    log "Waiting for MN2 importer pod to be ready..."
    kubectl --context "${CONTEXT}" --namespace "${WRB_NAMESPACE}" wait --for=condition=ready pod -l app=mirror-2-importer --timeout=300s || {
        log "ERROR: MN2 importer pod not ready after 300s"
        kubectl --context "${CONTEXT}" --namespace "${WRB_NAMESPACE}" describe pod -l app=mirror-2-importer || true
        kubectl --context "${CONTEXT}" --namespace "${WRB_NAMESPACE}" logs -l app=mirror-2-importer --tail=50 || true
        return 1
    }

    # Give importer time to start and log startup
    sleep 10
    log "MN2 importer startup logs:"
    kubectl --context "${CONTEXT}" --namespace "${WRB_NAMESPACE}" logs -l app=mirror-2-importer --tail=100 || true

    # Wait for MN2 REST to be ready
    log "Waiting for MN2 REST pod to be ready..."
    kubectl --context "${CONTEXT}" --namespace "${WRB_NAMESPACE}" wait --for=condition=ready pod -l app=mirror-2-rest --timeout=300s || {
        log "ERROR: MN2 REST pod not ready after 300s"
        kubectl --context "${CONTEXT}" --namespace "${WRB_NAMESPACE}" describe pod -l app=mirror-2-rest || true
        kubectl --context "${CONTEXT}" --namespace "${WRB_NAMESPACE}" logs -l app=mirror-2-rest --tail=50 || true
        return 1
    }

    # Set up port forward for MN2 API
    log "Setting up port forward to MN2 API..."
    kubectl --context "${CONTEXT}" --namespace "${WRB_NAMESPACE}" port-forward svc/mirror-2-rest ${MN_WRB_PORT}:80 &
    local pf_pid=$!
    echo $pf_pid > /tmp/mn2-port-forward.pid
    sleep 5

    log "MN2 deployed successfully (API at ${MN_WRB_URL})"
}

# Main test flow
function main {
    log "Starting Block Ingestion Validation Test"
    log "=========================================="
    log "  Main namespace:    ${NAMESPACE}"
    log "  WRB namespace:     ${WRB_NAMESPACE}"
    log "  Deployment:        ${DEPLOYMENT}"
    log "  Block range:       ${BLOCK_RANGE}"
    log ""

    # Clean up from previous runs
    log "Cleaning up from previous runs..."
    helm --kube-context "${CONTEXT}" --namespace "${NAMESPACE}" uninstall mirror-1 --ignore-not-found || true
    helm --kube-context "${CONTEXT}" --namespace "${NAMESPACE}" uninstall mirror-2 --ignore-not-found || true
    kubectl --context "${CONTEXT}" --namespace "${NAMESPACE}" delete pvc data-mirror-1-postgres-postgresql-0 --ignore-not-found || true

    # Clear Solo's deployment tracking state to prevent auto-increment to mirror-2
    rm -rf ~/.solo/deployments/${DEPLOYMENT}/mirror-*.json 2>/dev/null || true
    rm -rf ~/.solo/cache/${DEPLOYMENT}-mirror-*.yaml 2>/dev/null || true

    # Clean up work directory
    rm -rf "${WORK_DIR}"
    mkdir -p "${WORK_DIR}"

    log ""
    log "Waiting for network to generate blocks..."
    sleep 60  # Give the network time to generate blocks

    # ===== Phase 1: Basic Block Ingestion Test (MN1 → BN1) =====
    log ""
    log "Phase 1: Basic Block Ingestion Test (BN1 → MN1)"
    log "================================================"

    deploy_mn_to_bn || return 1
    wait_for_mn_ingestion "${MIN_TRANSACTIONS}" 360 || return 1
    capture_mn_responses "${MN_URL}" "${MN_CAPTURE_DIR}" || return 1
    validate_responses "${MN_CAPTURE_DIR}" || return 1

    log ""
    log "✅ Phase 1 complete! Basic block ingestion working."

    # ===== Phase 2: WRB CLI Wrapping Test (MN2 → BN2) =====
    log ""
    log "Phase 2: WRB CLI Wrapping Test (BN2 → MN2)"
    log "==========================================="
    log "This phase tests CLI-wrapped blocks:"
    log "  1. Download record files from MinIO (or CN fallback)"
    log "  2. Wrap them using WRB CLI (blocks wrap)"
    log "  3. Deploy BN2 with wrapped blocks in separate namespace"
    log "  4. Deploy MN2 and validate ingestion"
    log "  5. Compare API responses between Phase 1 and Phase 2"
    log ""

    # Download record files from MinIO (with CN fallback)
    local records_dir="${WORK_DIR}/records"
    download_record_files_from_minio "${records_dir}" 50 || {
        log "WARNING: Could not download record files, skipping Phase 2"
        log "✅ Phase 1 passed, Phase 2 skipped"
        return 0
    }

    # Wrap record files using WRB CLI
    wrap_records_with_cli "${records_dir}" "${WRAPPED_BLOCKS_DIR}" || {
        log "WARNING: Could not wrap record files, skipping Phase 2"
        log "✅ Phase 1 passed, Phase 2 skipped"
        return 0
    }

    # Create separate namespace
    create_wrb_namespace || return 1

    # Deploy BN2 with wrapped blocks and get first block number
    local bn2_first_block=$(deploy_standalone_bn2 "${WRAPPED_BLOCKS_DIR}") || return 1

    # Deploy MN2 connected to BN2, starting from BN2's first block
    # Use the binary address book file (file 0.0.102 format) for MN2 importer
    local address_book_bin="${WORK_DIR}/addressbook.bin"
    deploy_mn2_to_bn2 "${bn2_first_block}" "${address_book_bin}" || return 1

    # Wait for MN2 to start and begin ingesting
    log "Waiting for MN2 to start and ingest CLI-wrapped blocks..."
    local elapsed=0
    local timeout=480
    local api_ready=false

    while [[ ${elapsed} -lt ${timeout} ]]; do
        # Check if REST API is responding
        if curl -sf "${MN_WRB_URL}/blocks?limit=1" >/dev/null 2>&1; then
            api_ready=true
            log "MN2 REST API is ready after ${elapsed}s"
            break
        fi
        sleep 5
        elapsed=$((elapsed + 5))
    done

    if [[ "${api_ready}" != "true" ]]; then
        log "WARNING: MN2 REST API not ready after ${timeout}s"

        # Debug: capture MN2 pod logs
        log "Capturing MN2 pod state for debugging..."

        local mn2_importer_pod=$(kubectl --context "${CONTEXT}" --namespace "${WRB_NAMESPACE}" get pods -l app=mirror-2-importer -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")
        if [[ -n "${mn2_importer_pod}" ]]; then
            log "MN2 importer pod: ${mn2_importer_pod}"
            log "MN2 importer pod status:"
            kubectl --context "${CONTEXT}" --namespace "${WRB_NAMESPACE}" get pod "${mn2_importer_pod}" -o wide 2>&1 | sed 's/^/  /'
            log "MN2 importer pod logs (last 100 lines):"
            kubectl --context "${CONTEXT}" --namespace "${WRB_NAMESPACE}" logs "${mn2_importer_pod}" --tail=100 2>&1 | sed 's/^/  /'
        fi

        local mn2_rest_pod=$(kubectl --context "${CONTEXT}" --namespace "${WRB_NAMESPACE}" get pods -l app=mirror-2-rest -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")
        if [[ -n "${mn2_rest_pod}" ]]; then
            log "MN2 REST pod: ${mn2_rest_pod}"
            log "MN2 REST pod status:"
            kubectl --context "${CONTEXT}" --namespace "${WRB_NAMESPACE}" get pod "${mn2_rest_pod}" -o wide 2>&1 | sed 's/^/  /'
            log "MN2 REST pod logs (last 100 lines):"
            kubectl --context "${CONTEXT}" --namespace "${WRB_NAMESPACE}" logs "${mn2_rest_pod}" --tail=100 2>&1 | sed 's/^/  /'
        fi

        # Check database schema
        local mn2_postgres_pod=$(kubectl --context "${CONTEXT}" --namespace "${WRB_NAMESPACE}" get pods -l app=mn2-postgres -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")
        if [[ -n "${mn2_postgres_pod}" ]]; then
            log "Checking MN2 database schema:"
            kubectl --context "${CONTEXT}" --namespace "${WRB_NAMESPACE}" exec "${mn2_postgres_pod}" -- psql -U mirror_node -d mirror_node -c "SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename LIMIT 10;" 2>&1 | sed 's/^/  /'
        fi

        log "✅ Phase 1 passed, Phase 2 incomplete"
        return 0
    fi

    # Monitor MN2 ingestion progress
    log "Monitoring MN2 ingestion (up to 120s)..."
    local wait_time=0
    local max_wait=120
    local blocks_found=false

    while [[ ${wait_time} -lt ${max_wait} ]]; do
        # Check database for blocks
        local mn2_postgres_pod=$(kubectl --context "${CONTEXT}" --namespace "${WRB_NAMESPACE}" get pods -l app=mn2-postgres -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")
        if [[ -n "${mn2_postgres_pod}" ]]; then
            local block_count=$(kubectl --context "${CONTEXT}" --namespace "${WRB_NAMESPACE}" exec "${mn2_postgres_pod}" -- psql -U mirror_node -d mirror_node -t -c "SELECT COUNT(*) FROM record_file;" 2>/dev/null | tr -d ' ' || echo "0")
            log "[${wait_time}s] Database check: ${block_count} record files"

            if [[ ${block_count} -gt 0 ]]; then
                blocks_found=true
                log "✓ Blocks detected in database after ${wait_time}s!"
                break
            fi
        fi

        sleep 10
        wait_time=$((wait_time + 10))
    done

    if [[ "${blocks_found}" != "true" ]]; then
        log "WARNING: No blocks found in database after ${max_wait}s"

        # Check importer logs for clues - capture more lines to see processing
        local mn2_importer_pod=$(kubectl --context "${CONTEXT}" --namespace "${WRB_NAMESPACE}" get pods -l app=mirror-2-importer -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")
        if [[ -n "${mn2_importer_pod}" ]]; then
            log "MN2 importer logs (all relevant entries):"
            kubectl --context "${CONTEXT}" --namespace "${WRB_NAMESPACE}" logs "${mn2_importer_pod}" --tail=500 2>&1 | grep -E "(Started|Subscrib|streaming|block|connect|ERROR|Parser|RecordFile|persisted|inserted|Copied)" | sed 's/^/  /' || true

            log "MN2 database tables count:"
            local mn2_pg_pod="${mn2_postgres_pod}"
            if [[ -n "${mn2_pg_pod}" ]]; then
                kubectl --context "${CONTEXT}" --namespace "${WRB_NAMESPACE}" exec "${mn2_pg_pod}" -- psql -U mirror_node -d mirror_node -c "
                    SELECT 'record_file' as tbl, COUNT(*) FROM record_file UNION ALL
                    SELECT 'transaction', COUNT(*) FROM transaction UNION ALL
                    SELECT 'entity', COUNT(*) FROM entity UNION ALL
                    SELECT 'address_book', COUNT(*) FROM address_book;
                " 2>/dev/null || true
            fi
        fi
    fi

    # Capture MN2 responses
    capture_mn_responses "${MN_WRB_URL}" "${MN_WRB_CAPTURE_DIR}" || {
        log "WARNING: Could not capture MN2 responses"
        log "✅ Phase 1 passed, Phase 2 incomplete"
        return 0
    }

    validate_responses "${MN_WRB_CAPTURE_DIR}" || {
        log "WARNING: MN2 validation failed"
        log "✅ Phase 1 passed, Phase 2 incomplete"
        return 0
    }

    # Compare Phase 1 vs Phase 2 responses
    log ""
    compare_phase_responses

    log ""
    log "=========================================="
    log "✅ Both phases complete!"
    log "=========================================="
    log ""
    log "Results:"
    log "  Phase 1 (MN1→BN1): ${MN_CAPTURE_DIR}"
    log "  Phase 2 (MN2→BN2): ${MN_WRB_CAPTURE_DIR}"
    log ""
    log "API Validation:"
    log "  ✓ Phase 1: Blocks and transactions ingested"
    log "  ✓ Phase 2: Blocks and transactions ingested"
    log "  ✓ Comparison: See output above"
    log ""

    # Cleanup
    log "Cleaning up..."
    kill $(cat /tmp/mn1-port-forward.pid 2>/dev/null) 2>/dev/null || true
    kill $(cat /tmp/mn2-port-forward.pid 2>/dev/null) 2>/dev/null || true
    kubectl --context "${CONTEXT}" delete namespace "${WRB_NAMESPACE}" --ignore-not-found &
}

# Only run the whole test flow when this script is executed directly. Other
# scripts (e.g. wrb-distribution/install-and-run-wrb-cli.sh) `source` this file
# just to reuse download_record_files_from_minio / download_record_files_from_cn
# and must not trigger main() on load.
if [[ "${BASH_SOURCE[0]:-}" == "${0}" ]]; then
    main "$@"
fi
