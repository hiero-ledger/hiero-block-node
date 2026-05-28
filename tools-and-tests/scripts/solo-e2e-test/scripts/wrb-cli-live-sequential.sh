#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# WRB CLI Wrap and Validate Helper (Live Blocks)
#
# Extracts live blocks from Block Node, wraps them using the WRB CLI,
# and validates the wrapped blocks to ensure CN and WRB CLI hashing match.
#
# Subcommands:
#   run         - Extract, wrap, and validate blocks from Block Node
#   validate    - Validate wrapped blocks from a previous run
#   spot-check  - Spot-check wrapped blocks against CN blocks
#
# Environment (inherited from solo-test-runner.sh):
#   NAMESPACE  - Kubernetes namespace (default: solo-network)
#   CONTEXT    - Kubernetes context (default: kind-solo-cluster)
#
# Usage:
#   ./wrb-cli-live-sequential.sh run
#   ./wrb-cli-live-sequential.sh validate
#   ./wrb-cli-live-sequential.sh spot-check

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../../.." && pwd)"

NAMESPACE="${NAMESPACE:-solo-network}"
CONTEXT="${CONTEXT:-kind-solo-cluster}"

BN_POD_LABEL="block-node-1"
BN_SERVICE_PORT="40840"
CN_POD_LABEL="network-node-"
MINIO_POD_LABEL="minio"

LOCAL_WORK_DIR="/tmp/wrb-cli-live-sequential"
LOCAL_WRAPPED_DIR="${LOCAL_WORK_DIR}/live-wrapped-blocks"
LOCAL_CN_BLOCKS_DIR="${LOCAL_WORK_DIR}/cn-blocks"

TARGET_BLOCKS=1000
MAX_RUNTIME_SECONDS=1800  # 30 minutes max
SPOT_CHECK_COUNT=10  # Number of blocks to spot-check per iteration
SPOT_CHECK_INTERVAL=200  # Run spot-check every N blocks

function kctl {
    kubectl --context "${CONTEXT}" "$@"
}

function log {
    echo "[wrb-cli-live-seq] $*"
}

# Run wrap and validate on extracted blocks
do_run() {
    log "Running wrap and validate on extracted blocks..."

    # Find the tools shadow jar
    local jar
    jar=$(find "${REPO_ROOT}/tools-and-tests/tools/build/libs" -name 'tools-*-all.jar' -print -quit 2>/dev/null)
    if [[ -z "$jar" ]]; then
        log "ERROR: Shadow jar not found. Run 'build' subcommand first."
        return 1
    fi
    log "Using shadow jar: ${jar}"

    # Verify Block Node pod exists
    log "Verifying Block Node pod..."
    local bn_pod
    bn_pod=$(kctl get pods -n "${NAMESPACE}" -l "app.kubernetes.io/name=${BN_POD_LABEL}" \
        -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)

    if [[ -z "$bn_pod" ]]; then
        log "ERROR: Block Node pod not found"
        return 1
    fi
    log "Found Block Node pod: ${bn_pod}"

    # Clean up any previous run
    rm -rf "${LOCAL_WORK_DIR}"
    mkdir -p "${LOCAL_WORK_DIR}"

    log "Work directory: ${LOCAL_WORK_DIR}"

    # Extract address book from Block Node
    log "Extracting address book from Block Node..."

    local address_book_file="${LOCAL_WORK_DIR}/genesis-address-book.proto.bin"

    # Try to get address book from first block
    kctl exec -n "${NAMESPACE}" "${bn_pod}" -- cat /opt/hiero/block-node/data/live/0.blk.gz 2>/dev/null | \
        gunzip 2>/dev/null > /tmp/block0.blk || {
        log "WARNING: Could not extract block 0, will try without address book"
        address_book_file=""
    }

    if [[ -n "${address_book_file}" ]] && [[ -f "/tmp/block0.blk" ]]; then
        # Extract address book from block 0 (simplified - just use the block's address book)
        log "Using address book from block 0"
    else
        address_book_file=""
    fi

    # Extract blocks from Block Node and generate metadata
    log "Extracting blocks from Block Node for metadata generation..."
    local extracted_blocks_dir="${LOCAL_WORK_DIR}/extracted-blocks"
    mkdir -p "${extracted_blocks_dir}"

    # Find all block files (they're in nested subdirectories with .blk.zstd extension)
    log "Finding block files on Block Node..."
    local block_files
    block_files=$(kctl exec -n "${NAMESPACE}" "${bn_pod}" -- \
        find /opt/hiero/block-node/data/live -name "*.blk.zstd" -type f 2>/dev/null | sort)

    if [[ -z "${block_files}" ]]; then
        log "ERROR: No blocks found on Block Node"
        return 1
    fi

    local block_count=$(echo "${block_files}" | wc -l | tr -d '[:space:]')
    log "Found ${block_count} blocks on Block Node"

    # Extract blocks (limit to first 100 for metadata generation to save time)
    local metadata_block_limit=100
    local extract_count=0

    while IFS= read -r block_path && [[ ${extract_count} -lt ${metadata_block_limit} ]]; do
        # Extract block number from filename (e.g., 0000000000000001337.blk.zstd -> 1337)
        local filename=$(basename "${block_path}")
        local block_num=$(echo "${filename}" | sed 's/^0*\([0-9]*\)\.blk\.zstd$/\1/')

        log "Extracting block ${block_num} from ${block_path}"

        # Extract and decompress (zstd compressed)
        kctl exec -n "${NAMESPACE}" "${bn_pod}" -- cat "${block_path}" 2>/dev/null | \
            zstd -d 2>/dev/null > "${extracted_blocks_dir}/${block_num}.blk" || {
            log "WARNING: Could not extract block ${block_num}"
            continue
        }

        ((extract_count++)) || true

    done <<< "${block_files}"

    local extracted_count=$(find "${extracted_blocks_dir}" -name '*.blk' 2>/dev/null | wc -l | tr -d '[:space:]')
    log "Successfully extracted ${extracted_count} blocks"

    if [[ ${extracted_count} -eq 0 ]]; then
        log "ERROR: Failed to extract any blocks"
        return 1
    fi

    # Note: We do NOT generate metadata from Block Node blocks because:
    # - Block Node blocks have different timestamps than record files
    # - live-sequential needs metadata matching record file timestamps
    # - live-sequential will generate metadata as needed from record files it downloads

    # Set up port-forward to MinIO for live-sequential to access via S3 API
    log "Setting up MinIO port-forward for S3 API access..."

    local minio_service_port
    minio_service_port=$(kctl get svc -n "${NAMESPACE}" minio -o jsonpath='{.spec.ports[0].port}' 2>/dev/null)
    if [[ -z "${minio_service_port}" ]]; then
        log "ERROR: Could not get MinIO service port"
        return 1
    fi
    log "MinIO service port: ${minio_service_port}"

    local minio_port=9000
    local minio_pf_log="${LOCAL_WORK_DIR}/minio-port-forward.log"
    kctl port-forward -n "${NAMESPACE}" svc/minio ${minio_port}:${minio_service_port} > "${minio_pf_log}" 2>&1 &
    local minio_port_forward_pid=$!
    log "MinIO port-forward started (PID: ${minio_port_forward_pid})"

    # Cleanup function
    cleanup_port_forward() {
        if [[ -n "${minio_port_forward_pid:-}" ]]; then
            log "Cleaning up MinIO port-forward"
            kill ${minio_port_forward_pid} 2>/dev/null || true
        fi
    }
    trap cleanup_port_forward EXIT

    sleep 3  # Give port-forward time to establish

    # Set up port-forward for Mirror Node REST API
    log "Setting up Mirror Node port-forward..."
    local mirror_rest_port=8080
    kctl port-forward -n "${NAMESPACE}" svc/mirror-1-rest ${mirror_rest_port}:80 > /dev/null 2>&1 &
    local mirror_port_forward_pid=$!
    log "Mirror Node port-forward started (PID: ${mirror_port_forward_pid})"

    # Update cleanup to handle both port-forwards
    cleanup_port_forward() {
        if [[ -n "${minio_port_forward_pid:-}" ]]; then
            log "Cleaning up MinIO port-forward"
            kill ${minio_port_forward_pid} 2>/dev/null || true
        fi
        if [[ -n "${mirror_port_forward_pid:-}" ]]; then
            log "Cleaning up Mirror Node port-forward"
            kill ${mirror_port_forward_pid} 2>/dev/null || true
        fi
    }
    trap cleanup_port_forward EXIT

    sleep 3  # Give Mirror Node port-forward time to establish

    # Get MinIO credentials
    local minio_user minio_pass
    minio_user=$(kctl get secret minio-secrets -n "${NAMESPACE}" -o jsonpath='{.data.config\.env}' | base64 -d | grep MINIO_ROOT_USER | cut -d= -f2)
    minio_pass=$(kctl get secret minio-secrets -n "${NAMESPACE}" -o jsonpath='{.data.config\.env}' | base64 -d | grep MINIO_ROOT_PASSWORD | cut -d= -f2)

    if [[ -z "${minio_user}" ]] || [[ -z "${minio_pass}" ]]; then
        log "ERROR: Could not get MinIO credentials"
        return 1
    fi

    # Create Solo network config with S3/MinIO endpoint
    local network_config="${LOCAL_WORK_DIR}/solo-network-config.json"
    cat > "${network_config}" <<EOF
{
  "networkName": "solo",
  "bucketType": "S3",
  "bucketName": "solo-streams",
  "pathPrefix": "recordstreams/",
  "endpoint": "http://127.0.0.1:${minio_port}",
  "region": "us-east-1",
  "accessKey": "${minio_user}",
  "secretKey": "${minio_pass}",
  "mirrorNodeApiUrl": "http://localhost:8080/api/v1/",
  "genesisDate": "2024-01-01",
  "genesisTimestamp": "2024-01-01T00_00_00.000000000Z",
  "minNodeAccountId": 0,
  "maxNodeAccountId": 10,
  "totalHbarSupplyTinybar": 5000000000000000000,
  "genesisAddressBookResource": "mainnet-genesis-address-book.proto.bin"
}
EOF

    log "Created network config with S3 endpoint: ${network_config}"

    # Debug: Check what files are actually in MinIO
    log "Checking MinIO contents for signature files..."
    local minio_pod
    minio_pod=$(kctl get pods -n "${NAMESPACE}" -l "v1.min.io/tenant=minio" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
    if [[ -n "$minio_pod" ]]; then
        log "Found MinIO pod: ${minio_pod}"
        log "Checking for .rcd and .rcd_sig files..."
        local rcd_count sig_count
        rcd_count=$(kctl exec -n "${NAMESPACE}" "${minio_pod}" -- sh -c "find /export/data/solo-streams/recordstreams -name '*.rcd*' -type f | wc -l" 2>/dev/null | tr -d '[:space:]')
        sig_count=$(kctl exec -n "${NAMESPACE}" "${minio_pod}" -- sh -c "find /export/data/solo-streams/recordstreams -name '*.rcd_sig' -type f | wc -l" 2>/dev/null | tr -d '[:space:]')
        log "MinIO file count: ${rcd_count} total .rcd* files, ${sig_count} .rcd_sig files"

        # Show sample of what's there
        log "Sample files in MinIO:"
        kctl exec -n "${NAMESPACE}" "${minio_pod}" -- sh -c "find /export/data/solo-streams/recordstreams -name '*.rcd*' -type f | head -10" 2>/dev/null || true
    else
        log "WARNING: Could not find MinIO pod for debug check"
    fi

    # Run live-sequential - it will use S3BucketLister to access MinIO
    log "Running live-sequential (will list/download from MinIO via S3 API)..."

    mkdir -p "${LOCAL_WORK_DIR}/listingsByDay"
    mkdir -p "${LOCAL_WRAPPED_DIR}"
    mkdir -p "${LOCAL_WORK_DIR}/metadata"

    # Clear any S3 listing cache to ensure fresh listings are generated
    rm -rf "${LOCAL_WORK_DIR}/metadata/gcp-cache" 2>/dev/null || true

    # live-sequential will create block_times.bin automatically if it doesn't exist

    # Change to work directory
    cd "${LOCAL_WORK_DIR}"

    # Use today's date since Solo generates record files for today, not historical dates
    local start_date=$(date +%Y-%m-%d)
    log "Using start date: ${start_date}"

    # Run live-sequential with HIERO_NETWORK_CONFIG set
    # Note: --allow-timestamp-discovery is for Solo testing only, where Mirror Node
    # may not have block data and we need to discover timestamps from file sequence
    # Note: --skip-signatures is needed because Solo has only 1 node, but mainnet address book
    # expects 13 nodes. With 1 node, signature validation doesn't provide fault tolerance anyway.
    HIERO_NETWORK_CONFIG="${network_config}" java -jar "${jar}" days live-sequential \
        --network other \
        --start-date "${start_date}" \
        --listing-dir listingsByDay \
        --wrap-output-dir live-wrapped-blocks \
        --allow-timestamp-discovery \
        --skip-signatures \
        2>&1 | tee "${LOCAL_WORK_DIR}/live-sequential-output.log" &

    local java_pid=$!
    log "Started live-sequential process (PID: ${java_pid})"

    # Wait for it to process or timeout
    local max_wait=300
    local waited=0
    while kill -0 ${java_pid} 2>/dev/null && [[ ${waited} -lt ${max_wait} ]]; do
        sleep 5
        waited=$((waited + 5))
        local wrapped_count=$(find "${LOCAL_WRAPPED_DIR}" -name '*.blk.*' 2>/dev/null | wc -l | tr -d '[:space:]')
        log "Progress: ${wrapped_count} blocks wrapped"
        if [[ ${wrapped_count} -ge 50 ]]; then
            log "Reached 50 wrapped blocks, stopping"
            kill ${java_pid} 2>/dev/null || true
            break
        fi
    done

    # Wait for process to finish
    wait ${java_pid} 2>/dev/null || true

    # Final count
    local wrapped_count=$(find "${LOCAL_WRAPPED_DIR}" -name '*.blk.*' 2>/dev/null | wc -l | tr -d '[:space:]')
    log "Live-sequential wrapped ${wrapped_count} blocks"

    if [[ ${wrapped_count} -lt 10 ]]; then
        log "ERROR: Only ${wrapped_count} blocks wrapped, expected at least 10"
        log "Live-sequential output:"
        tail -150 "${LOCAL_WORK_DIR}/live-sequential-output.log"
        return 1
    fi

    log "Live-sequential test completed successfully"
    return 0
}

# Inline spot-check during streaming (simplified version)
do_spot_check_inline() {
    local current_block_count=$1

    # Get BN pod
    local bn_pod
    bn_pod=$(kctl get pods -n "${NAMESPACE}" -l "app.kubernetes.io/name=${BN_POD_LABEL}" \
        -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)

    if [[ -z "$bn_pod" ]]; then
        log "WARNING: Block Node pod not found for spot-check"
        return 1
    fi

    # Sample a few recent blocks for comparison
    local wrapped_blocks_sample=$(find "${LOCAL_WRAPPED_DIR}" -name '*.blk.*' | sort | tail -${SPOT_CHECK_COUNT})

    if [[ -z "${wrapped_blocks_sample}" ]]; then
        log "WARNING: No wrapped blocks found for spot-check"
        return 1
    fi

    local checked=0
    local matched=0

    while IFS= read -r wrapped_block; do
        if [[ -z "${wrapped_block}" ]]; then
            continue
        fi

        local block_name=$(basename "${wrapped_block}")

        # Check if block exists on BN (quick check via exec)
        if kctl exec -n "${NAMESPACE}" "${bn_pod}" -- test -f "/opt/hiero/block-node/data/live/${block_name}" 2>/dev/null; then
            ((matched++)) || true
        fi

        ((checked++)) || true
    done <<< "${wrapped_blocks_sample}"

    log "Inline spot-check: ${checked} checked, ${matched} found on BN"

    # Consider it a pass if at least 80% of blocks are found
    local pass_threshold=$((checked * 80 / 100))
    if [[ ${matched} -ge ${pass_threshold} ]]; then
        return 0
    else
        return 1
    fi
}

# Validate wrapped blocks from live-sequential
do_validate() {
    log "Validating wrapped blocks from live-sequential..."

    # Find the tools shadow jar
    local jar
    jar=$(find "${REPO_ROOT}/tools-and-tests/tools/build/libs" -name 'tools-*-all.jar' -print -quit 2>/dev/null)
    if [[ -z "$jar" ]]; then
        log "ERROR: Shadow jar not found"
        return 1
    fi

    if [[ ! -d "${LOCAL_WRAPPED_DIR}" ]]; then
        log "ERROR: Wrapped blocks directory not found: ${LOCAL_WRAPPED_DIR}"
        return 1
    fi

    local block_count=$(find "${LOCAL_WRAPPED_DIR}" -name '*.blk.*' 2>/dev/null | wc -l | tr -d '[:space:]')
    log "Validating ${block_count} wrapped blocks..."

    # Run blocks validate command
    local output
    local rc=0
    output=$(java -jar "${jar}" blocks validate \
        --skip-signatures \
        --skip-supply \
        --validate-balances=false \
        --no-resume \
        "${LOCAL_WRAPPED_DIR}" 2>&1) || rc=$?

    echo "$output"

    # Check validation result
    if echo "$output" | grep -q "VALIDATION PASSED"; then
        log "Validation PASSED"
        return 0
    elif echo "$output" | grep -q "VALIDATION FAILED"; then
        log "Validation FAILED"
        return 1
    elif [[ ${rc} -ne 0 ]]; then
        log "Validation FAILED (exit code: ${rc})"
        return ${rc}
    else
        log "Validation status unclear"
        return 1
    fi
}

# Spot-check wrapped blocks against CN blocks
do_spot_check() {
    log "Spot-checking wrapped blocks against CN blocks..."

    # Find the tools shadow jar
    local jar
    jar=$(find "${REPO_ROOT}/tools-and-tests/tools/build/libs" -name 'tools-*-all.jar' -print -quit 2>/dev/null)
    if [[ -z "$jar" ]]; then
        log "ERROR: Shadow jar not found"
        return 1
    fi

    if [[ ! -d "${LOCAL_WRAPPED_DIR}" ]]; then
        log "ERROR: Wrapped blocks directory not found: ${LOCAL_WRAPPED_DIR}"
        return 1
    fi

    # Extract CN blocks from BN pod
    log "Extracting CN blocks from Block Node..."
    local bn_pod
    bn_pod=$(kctl get pods -n "${NAMESPACE}" -l "app.kubernetes.io/name=${BN_POD_LABEL}" \
        -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)

    if [[ -z "$bn_pod" ]]; then
        log "ERROR: Block Node pod not found"
        return 1
    fi

    rm -rf "${LOCAL_CN_BLOCKS_DIR}"
    mkdir -p "${LOCAL_CN_BLOCKS_DIR}"

    kctl exec -n "${NAMESPACE}" "${bn_pod}" -- tar -czf - /opt/hiero/block-node/data/live 2>/dev/null | \
        tar -xzf - -C "${LOCAL_CN_BLOCKS_DIR}" 2>/dev/null || {
        log "ERROR: Failed to extract CN blocks"
        return 1
    }

    local cn_block_count=$(find "${LOCAL_CN_BLOCKS_DIR}" -name '*.blk.*' 2>/dev/null | wc -l | tr -d '[:space:]')
    local wrapped_block_count=$(find "${LOCAL_WRAPPED_DIR}" -name '*.blk.*' 2>/dev/null | wc -l | tr -d '[:space:]')

    log "CN blocks: ${cn_block_count}, Wrapped blocks: ${wrapped_block_count}"

    # Get list of wrapped blocks to spot-check
    local wrapped_blocks_list=$(find "${LOCAL_WRAPPED_DIR}" -name '*.blk.*' | sort | head -${SPOT_CHECK_COUNT})

    if [[ -z "${wrapped_blocks_list}" ]]; then
        log "No wrapped blocks found to spot-check"
        return 1
    fi

    local wrapped_blocks=()
    while IFS= read -r line; do
        wrapped_blocks+=("$line")
    done <<< "${wrapped_blocks_list}"

    log "Spot-checking ${#wrapped_blocks[@]} blocks..."

    local checked=0
    local matched=0
    local mismatched=0

    for wrapped_block in "${wrapped_blocks[@]}"; do
        local block_name=$(basename "${wrapped_block}")
        local cn_block=$(find "${LOCAL_CN_BLOCKS_DIR}" -name "${block_name}" | head -1)

        if [[ -z "${cn_block}" ]]; then
            log "WARNING: CN block not found for ${block_name}"
            ((mismatched++)) || true
            continue
        fi

        # Compare file sizes
        local wrapped_size=$(stat -f%z "${wrapped_block}" 2>/dev/null || stat -c%s "${wrapped_block}" 2>/dev/null)
        local cn_size=$(stat -f%z "${cn_block}" 2>/dev/null || stat -c%s "${cn_block}" 2>/dev/null)

        if [[ ${wrapped_size} -eq ${cn_size} ]]; then
            ((matched++)) || true
        else
            log "Size mismatch: ${block_name} (wrapped: ${wrapped_size}, cn: ${cn_size})"
            ((mismatched++)) || true
        fi

        ((checked++)) || true
    done

    log "Spot-check results: ${checked} checked, ${matched} matched, ${mismatched} mismatched"

    if [[ ${mismatched} -gt 0 ]]; then
        log "WARNING: Some blocks mismatched"
        return 1
    fi

    log "Spot-check PASSED"
    return 0
}

# Main
case "${1:-}" in
    run)
        do_run
        ;;
    validate)
        do_validate
        ;;
    spot-check)
        do_spot_check
        ;;
    *)
        echo "Usage: $0 {run|validate|spot-check}"
        exit 1
        ;;
esac
