#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# WRB Historic BN2 Deployment Helper
#
# Deploys BN2 configured to serve WRB CLI-wrapped blocks from historic storage.
# This script:
#   1. Wraps record files using WRB CLI (calls wrb-cli-wrap-and-compare.sh)
#   2. Copies wrapped blocks into BN2's archive PVC
#   3. Restarts BN2 to discover the blocks
#
# Prerequisites:
#   - Network deployed with wrb-differential-test topology (or 2+ BNs)
#   - WRB CLI wrap completed (run wrb-cli-wrap-and-compare.sh wrap)
#
# Environment (inherited from solo-test-runner.sh):
#   NAMESPACE  - Kubernetes namespace (default: solo-network)
#   CONTEXT    - Kubernetes context (default: kind-solo-cluster)
#
# Usage:
#   ./wrb-deploy-bn2-historic.sh copy-blocks
#   ./wrb-deploy-bn2-historic.sh restart
#   ./wrb-deploy-bn2-historic.sh verify
#   ./wrb-deploy-bn2-historic.sh all

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../../.." && pwd)"

NAMESPACE="${NAMESPACE:-solo-network}"
CONTEXT="${CONTEXT:-kind-solo-cluster}"

BN2_POD_LABEL="block-node.hiero.com/type=block-node"
BN2_POD_NAME_PATTERN="block-node-2"  # Adjust if using different naming

LOCAL_WORK_DIR="/tmp/wrb-cli-phase2-validation"
LOCAL_WRAPPED_DIR="${LOCAL_WORK_DIR}/cli-wrapped-blocks"
LOCAL_RECORDS_DIR="${LOCAL_WORK_DIR}/record-files"

# BN2 paths (inside container)
BN2_HISTORIC_PATH="/opt/hiero/block-node/data/historic"
BN2_ARCHIVE_MOUNT="/opt/hiero/block-node/data/historic"  # Matches persistence.archive.mountPath

function kctl {
    kubectl --context "${CONTEXT}" "$@"
}

function log {
    echo "[wrb-bn2-historic] $*" >&2
}

function find_bn2_pod {
    log "Finding BN2 pod in namespace ${NAMESPACE}..."

    # Find pod with block-node-2 in the name
    local bn2_pod
    bn2_pod=$(kctl get pods -n "${NAMESPACE}" -l "${BN2_POD_LABEL}" \
        -o jsonpath='{.items[*].metadata.name}' 2>/dev/null | \
        tr ' ' '\n' | grep "${BN2_POD_NAME_PATTERN}" | head -1)

    if [[ -z "$bn2_pod" ]]; then
        log "ERROR: BN2 pod not found in namespace '${NAMESPACE}'"
        log "Searched for pods with label '${BN2_POD_LABEL}' and name pattern '${BN2_POD_NAME_PATTERN}'"
        log ""
        log "Available pods:"
        kctl get pods -n "${NAMESPACE}" -l "${BN2_POD_LABEL}" -o name
        return 1
    fi

    echo "${bn2_pod}"
}

function do_copy_blocks {
    log "Copying wrapped blocks to BN2's archive PVC..."

    # Verify wrapped blocks exist
    if [[ ! -d "${LOCAL_WRAPPED_DIR}" ]]; then
        log "ERROR: Wrapped blocks directory not found: ${LOCAL_WRAPPED_DIR}"
        log "Run 'wrb-cli-wrap-and-compare.sh wrap' first"
        return 1
    fi

    # Count wrapped block files (zip archives)
    local wrapped_count
    wrapped_count=$(find "${LOCAL_WRAPPED_DIR}" -type f -name "*.zip" 2>/dev/null | wc -l | tr -d '[:space:]')

    if [[ "${wrapped_count}" -eq 0 ]]; then
        log "ERROR: No wrapped block zip files found in ${LOCAL_WRAPPED_DIR}"
        return 1
    fi

    log "Found ${wrapped_count} wrapped block zip file(s) to copy"

    # Find BN2 pod
    local bn2_pod
    bn2_pod=$(find_bn2_pod) || return 1
    log "Found BN2 pod: ${bn2_pod}"

    # Check BN2 pod status
    local pod_phase
    pod_phase=$(kctl get pod -n "${NAMESPACE}" "${bn2_pod}" -o jsonpath='{.status.phase}')
    log "BN2 pod status: ${pod_phase}"

    if [[ "${pod_phase}" != "Running" ]]; then
        log "WARNING: BN2 pod is not Running (current: ${pod_phase})"
        log "Proceeding anyway, but copy may fail..."
    fi

    # Create historic directory structure in BN2 if it doesn't exist
    log "Creating historic directory structure in BN2..."
    kctl exec -n "${NAMESPACE}" "${bn2_pod}" -- \
        mkdir -p "${BN2_HISTORIC_PATH}" 2>/dev/null || true

    # Copy the entire wrapped blocks directory structure to BN2
    # This preserves the nested directory structure (000/000/000/.../00000s.zip)
    log "Copying wrapped blocks to BN2:${BN2_HISTORIC_PATH}..."
    log "This may take a few minutes for large datasets..."

    # Use kubectl cp to copy the entire directory
    # Source: LOCAL_WRAPPED_DIR/* (contents)
    # Destination: BN2_POD:BN2_HISTORIC_PATH/
    if kctl cp -n "${NAMESPACE}" "${LOCAL_WRAPPED_DIR}/." "${bn2_pod}:${BN2_HISTORIC_PATH}/" 2>&1 | tee /tmp/kubectl-cp-output.log; then
        log "Successfully copied wrapped blocks to BN2"
    else
        log "ERROR: Failed to copy wrapped blocks to BN2"
        log "Check /tmp/kubectl-cp-output.log for details"
        return 1
    fi

    # Also copy signature files (.rcd_sig) from record files directory
    # Mirror Node needs these to verify wrapped record blocks
    log "Copying signature files to BN2..."
    if [[ -d "${LOCAL_RECORDS_DIR}" ]]; then
        local sig_count
        sig_count=$(find "${LOCAL_RECORDS_DIR}" -type f -name "*.rcd_sig*" 2>/dev/null | wc -l | tr -d '[:space:]')

        if [[ "${sig_count}" -gt 0 ]]; then
            log "Found ${sig_count} signature file(s) to copy"

            # Copy signature files to BN2's historic path
            if kctl cp -n "${NAMESPACE}" "${LOCAL_RECORDS_DIR}/." "${bn2_pod}:${BN2_HISTORIC_PATH}/" 2>&1 | tee -a /tmp/kubectl-cp-output.log; then
                log "Successfully copied signature files to BN2"
            else
                log "WARNING: Failed to copy signature files to BN2"
                log "Mirror Node may fail to verify wrapped blocks without signatures"
            fi
        else
            log "WARNING: No signature files found in ${LOCAL_RECORDS_DIR}"
            log "Mirror Node may fail to verify wrapped blocks without signatures"
        fi
    else
        log "WARNING: Record files directory not found: ${LOCAL_RECORDS_DIR}"
        log "Mirror Node may fail to verify wrapped blocks without signatures"
    fi

    # Verify files were copied
    log "Verifying copied files..."
    local copied_count sig_copied_count
    copied_count=$(kctl exec -n "${NAMESPACE}" "${bn2_pod}" -- \
        find "${BN2_HISTORIC_PATH}" -type f -name "*.zip" 2>/dev/null | wc -l | tr -d '[:space:]')
    sig_copied_count=$(kctl exec -n "${NAMESPACE}" "${bn2_pod}" -- \
        find "${BN2_HISTORIC_PATH}" -type f -name "*.rcd_sig*" 2>/dev/null | wc -l | tr -d '[:space:]')

    log "Copied ${copied_count} zip file(s) and ${sig_copied_count} signature file(s) to BN2"

    if [[ "${copied_count}" -eq 0 ]]; then
        log "ERROR: No zip files found in BN2 after copy"
        return 1
    fi

    if [[ "${sig_copied_count}" -eq 0 ]]; then
        log "WARNING: No signature files found in BN2 after copy"
        log "Mirror Node may fail RSA verification without signatures"
    fi

    # List directory structure for verification
    log "BN2 historic directory structure:"
    kctl exec -n "${NAMESPACE}" "${bn2_pod}" -- \
        find "${BN2_HISTORIC_PATH}" -type d -o -name "*.zip" 2>/dev/null | head -20

    log "Block copy completed successfully"
}

function do_restart {
    log "Restarting BN2 to discover wrapped blocks..."

    # Find BN2 pod
    local bn2_pod
    bn2_pod=$(find_bn2_pod) || return 1
    log "Found BN2 pod: ${bn2_pod}"

    # Delete the pod (StatefulSet will recreate it)
    log "Deleting BN2 pod (will be recreated by StatefulSet)..."
    kctl delete pod -n "${NAMESPACE}" "${bn2_pod}" --wait=false

    log "Waiting for new BN2 pod to start..."
    sleep 5

    # Wait for new pod to be ready
    log "Waiting for BN2 to be ready (timeout: 120s)..."
    if kctl wait --for=condition=ready pod -n "${NAMESPACE}" -l "${BN2_POD_LABEL}" \
        --field-selector="metadata.name=${bn2_pod}" --timeout=120s 2>/dev/null; then
        log "BN2 pod is ready"
    else
        # Pod name may have changed, wait for any block-node-2 pod
        log "Original pod not found, waiting for new block-node-2 pod..."
        local new_pod
        for i in {1..24}; do
            new_pod=$(find_bn2_pod 2>/dev/null || echo "")
            if [[ -n "$new_pod" ]]; then
                local pod_ready
                pod_ready=$(kctl get pod -n "${NAMESPACE}" "${new_pod}" -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || echo "False")
                if [[ "${pod_ready}" == "True" ]]; then
                    log "BN2 pod is ready: ${new_pod}"
                    break
                fi
            fi
            log "Waiting for BN2 pod... (${i}/24)"
            sleep 5
        done
    fi

    # Show BN2 pod logs to check for block discovery
    log "Checking BN2 logs for block discovery..."
    local final_pod
    final_pod=$(find_bn2_pod) || {
        log "ERROR: Could not find BN2 pod after restart"
        return 1
    }

    log "Recent BN2 logs:"
    kctl logs -n "${NAMESPACE}" "${final_pod}" --tail=50 | grep -i "block\|historic\|zip" || true

    log "BN2 restart completed"
}

function do_verify {
    log "Verifying BN2 can serve wrapped blocks..."

    # Find BN2 pod
    local bn2_pod
    bn2_pod=$(find_bn2_pod) || return 1
    log "Found BN2 pod: ${bn2_pod}"

    # Check if BN2 pod is ready
    local pod_ready
    pod_ready=$(kctl get pod -n "${NAMESPACE}" "${bn2_pod}" -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}')

    if [[ "${pod_ready}" != "True" ]]; then
        log "ERROR: BN2 pod is not ready (Ready=${pod_ready})"
        return 1
    fi

    log "BN2 pod is ready"

    # Check historic directory contents
    log "Checking BN2 historic directory..."
    local zip_count
    zip_count=$(kctl exec -n "${NAMESPACE}" "${bn2_pod}" -- \
        find "${BN2_HISTORIC_PATH}" -type f -name "*.zip" 2>/dev/null | wc -l | tr -d '[:space:]')

    log "BN2 has ${zip_count} zip file(s) in historic storage"

    if [[ "${zip_count}" -eq 0 ]]; then
        log "ERROR: No zip files found in BN2 historic storage"
        return 1
    fi

    # Check BN2 logs for block discovery
    log "Checking BN2 logs for block discovery messages..."
    if kctl logs -n "${NAMESPACE}" "${bn2_pod}" --tail=100 | \
        grep -i "ZipBlockArchive\|BlockFileHistoricPlugin\|blocks available" >/dev/null 2>&1; then
        log "Found block discovery messages in logs"
    else
        log "WARNING: No block discovery messages found in logs (may not have started yet)"
    fi

    # Try to get BN2 service endpoint
    local bn2_service
    bn2_service=$(kctl get svc -n "${NAMESPACE}" -l "block-node.hiero.com/node-name=block-node-2" \
        -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")

    if [[ -n "${bn2_service}" ]]; then
        log "BN2 service: ${bn2_service}"
        log "BN2 gRPC endpoint: ${bn2_service}.${NAMESPACE}.svc.cluster.local:40840"
    else
        log "WARNING: BN2 service not found (deployment may use different naming)"
    fi

    # TODO: Add gRPC API test to query block 0
    # This would require grpcurl or a test client

    log "Verification completed"
    log ""
    log "Next steps:"
    log "  1. Deploy Mirror Node configured to subscribe to BN2"
    log "  2. Verify Mirror Node ingests blocks from BN2"
    log "  3. Compare Mirror Node database against live-blocks baseline"
}

function do_all {
    log "Running full WRB BN2 historic deployment..."
    log ""

    do_copy_blocks || return 1
    log ""

    do_restart || return 1
    log ""

    do_verify || return 1
    log ""

    log "Full deployment completed successfully"
}

# Main
case "${1:-}" in
    copy-blocks)
        do_copy_blocks
        ;;
    restart)
        do_restart
        ;;
    verify)
        do_verify
        ;;
    all)
        do_all
        ;;
    *)
        echo "Usage: $0 {copy-blocks|restart|verify|all}"
        echo ""
        echo "Commands:"
        echo "  copy-blocks - Copy wrapped blocks to BN2's archive PVC"
        echo "  restart     - Restart BN2 to discover blocks"
        echo "  verify      - Verify BN2 can serve blocks"
        echo "  all         - Run all steps (copy, restart, verify)"
        exit 1
        ;;
esac