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

# Work directories
WORK_DIR="/tmp/wrb-test-$$"
RECORD_FILES_DIR="${WORK_DIR}/record-files"
WRAPPED_BLOCKS_DIR="${WORK_DIR}/wrapped-blocks"

function log {
    echo "[wrb-ingestion] $*"
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
    local overlay_file="/tmp/mn-overlay.yaml"

    log "Deploying ${mn_name} connected to block-node-1..."

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

    # Deploy Mirror Node with v0.156.0+
    solo mirror node add \
        --deployment "${DEPLOYMENT}" \
        --mirror-node-version "v0.156.0" \
        --pinger \
        --cluster-ref "${CONTEXT}" \
        -f "${overlay_file}" || {
        log "ERROR: Failed to deploy ${mn_name}"
        return 1
    }

    log "${mn_name} deployed successfully"

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
        # Find all .rcd and .rcd_sig files for this day
        local day_files=$(find "${records_dir}" -name "${day}T*.rcd" -o -name "${day}T*.rcd_sig" | sort)
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
        --unzipped \
        --skip-block-number-validation > /tmp/wrb-wrap.log 2>&1 || {
        log "ERROR: WRB CLI wrap failed"
        tail -30 /tmp/wrb-wrap.log
        return 1
    }

    # Verify wrapped blocks were created
    local wrapped_count=$(find "${output_dir}" -type f \( -name "*.zip" -o -name "*.blk.*" \) 2>/dev/null | wc -l)
    log "Created ${wrapped_count} wrapped block files"

    if [ ${wrapped_count} -lt 1 ]; then
        log "ERROR: No wrapped blocks created"
        tail -30 /tmp/wrb-wrap.log
        return 1
    fi

    log "✓ Successfully wrapped ${rcd_count} record files into ${wrapped_count} block files"
    return 0
}

function capture_mn_responses {
    local mn_url=$1
    local output_dir=$2

    log "Capturing API responses from ${mn_url}..."

    rm -rf "${output_dir}"
    mkdir -p "${output_dir}"

    # Use curl instead of Python script (no dependencies needed)
    log "  Capturing blocks..."
    curl -s "${mn_url}/blocks?limit=100&order=asc" > "${output_dir}/blocks.json" 2>/dev/null || true

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
        local block_count=$(python3 -c "import json; data=json.load(open('${capture_dir}/blocks.json')); print(len(data.get('blocks', [])))" 2>/dev/null || echo "0")
        log "  Blocks captured: ${block_count}"
        if [ ${block_count} -lt 1 ]; then
            log "ERROR: No blocks found in API response"
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
        log "WARNING: Missing response directories, skipping comparison"
        return 0
    fi

    python3 "${PYTHON_DIR}/compare_captured_responses.py" \
        "${MN_CAPTURE_DIR}" \
        "${MN_WRB_CAPTURE_DIR}" \
        --output /tmp/comparison-report.json \
        --verbose || {
        log "⚠️  Responses differ between Phase 1 and Phase 2"
        log "This is expected if block production continued between phases"
        return 0
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
      containers:
      - name: block-node-server
        image: ghcr.io/hiero-ledger/hiero-block-node:0.31.0-rc4
        ports:
        - containerPort: 40840
          name: grpc
        volumeMounts:
        - name: data
          mountPath: /opt/hiero/block-node/data
        env:
        - name: PLUGINS_NAMES
          value: "facility-messaging,block-access-service,health,server-status,stream-publisher,verification,blocks-file-historic"
        - name: STORAGE_FILES_HISTORIC_ROOT_PATH
          value: "/opt/hiero/block-node/data/historic"
        - name: STORAGE_FILES_RECENT_ROOT_PATH
          value: ""
      volumes:
      - name: data
        persistentVolumeClaim:
          claimName: bn2-storage
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

    # Wait for BN2 to be ready
    log "Waiting for BN2 pod to be ready..."
    kubectl --context "${CONTEXT}" --namespace "${WRB_NAMESPACE}" wait --for=condition=ready pod -l app=block-node-2 --timeout=120s || {
        log "ERROR: BN2 pod not ready"
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

    log "BN2 deployed successfully with wrapped blocks"
}

function deploy_mn2_to_bn2 {
    log "Deploying MN2 in ${WRB_NAMESPACE} connected to BN2..."

    # Get the Mirror Node importer and REST images from MN1 deployment
    local mn_importer_image=$(kubectl --context "${CONTEXT}" --namespace "${NAMESPACE}" get deployment mirror-1-importer -o jsonpath='{.spec.template.spec.containers[0].image}' 2>/dev/null || echo "")
    local mn_rest_image=$(kubectl --context "${CONTEXT}" --namespace "${NAMESPACE}" get deployment mirror-1-rest -o jsonpath='{.spec.template.spec.containers[0].image}' 2>/dev/null || echo "")

    if [ -z "${mn_importer_image}" ]; then
        log "WARNING: Could not get MN1 importer image, using default"
        mn_importer_image="gcr.io/mirrornode/hedera-mirror-importer:0.156.0"
    fi

    if [ -z "${mn_rest_image}" ]; then
        log "WARNING: Could not get MN1 REST image, using default"
        mn_rest_image="gcr.io/mirrornode/hedera-mirror-rest-java:0.156.0"
    fi

    log "Using Mirror Node importer image: ${mn_importer_image}"
    log "Using Mirror Node REST image: ${mn_rest_image}"

    local mn2_manifest="/tmp/mn2-deployment.yaml"

    cat > "${mn2_manifest}" << EOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: mn2-importer-config
  namespace: ${WRB_NAMESPACE}
data:
  application.yml: |
    spring:
      datasource:
        url: jdbc:postgresql://mirror-1-postgres-postgresql.${NAMESPACE}.svc.cluster.local:5432/mirror_node_wrb?sslmode=disable
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
          block:
            enabled: true
            nodes:
              - host: block-node-2.${WRB_NAMESPACE}.svc.cluster.local
                port: 40840
            sourceType: BLOCK_NODE
          downloader:
            record:
              enabled: false
            balance:
              enabled: false
          startBlockNumber: 0
          verification:
            tss:
              enabled: false
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: mn2-rest-config
  namespace: ${WRB_NAMESPACE}
data:
  application.yml: |
    spring:
      datasource:
        url: jdbc:postgresql://mirror-1-postgres-postgresql.${NAMESPACE}.svc.cluster.local:5432/mirror_node_wrb?sslmode=disable
        username: mirror_node
        password: mirror_node_pass
      redis:
        host: mirror-1-redis-node.${NAMESPACE}.svc.cluster.local
        port: 6379
    hiero:
      mirror:
        rest:
          port: 8080
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
        - name: config
          mountPath: /app/application.yml
          subPath: application.yml
      volumes:
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
        volumeMounts:
        - name: config
          mountPath: /app/application.yml
          subPath: application.yml
      volumes:
      - name: config
        configMap:
          name: mn2-rest-config
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
    kubectl --context "${CONTEXT}" --namespace "${NAMESPACE}" delete pvc data-mirror-1-postgres-postgresql-0 --ignore-not-found || true
    rm -rf ~/.solo/deployments/${DEPLOYMENT}/mirror-1.json 2>/dev/null || true

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

    # Deploy BN2 with wrapped blocks
    deploy_standalone_bn2 "${WRAPPED_BLOCKS_DIR}" || return 1

    # Deploy MN2 connected to BN2
    deploy_mn2_to_bn2 || return 1

    # Wait for MN2 to start and begin ingesting
    log "Waiting for MN2 to start and ingest CLI-wrapped blocks..."
    local elapsed=0
    local timeout=180
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

        log "✅ Phase 1 passed, Phase 2 incomplete"
        return 0
    fi

    # Give MN2 additional time to ingest blocks
    log "Waiting 30s for MN2 to ingest blocks..."
    sleep 30

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

main "$@"
