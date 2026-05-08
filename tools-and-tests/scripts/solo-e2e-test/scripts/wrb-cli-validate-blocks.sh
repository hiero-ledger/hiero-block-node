#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# WRB CLI Block Hash Validation Helper
#
# Builds the tools shadow jar and validates block hashes extracted from
# a Block Node pod using the WRB CLI `blocks validate` command.
#
# Subcommands:
#   build     - Build the tools shadow jar via Gradle
#   validate  - Extract blocks from BN pod and run validation
#
# Environment (inherited from solo-test-runner.sh):
#   NAMESPACE  - Kubernetes namespace (default: solo-network)
#   CONTEXT    - Kubernetes context (default: kind-solo-cluster)
#
# Usage:
#   ./wrb-cli-validate-blocks.sh build
#   ./wrb-cli-validate-blocks.sh validate

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../../.." && pwd)"

NAMESPACE="${NAMESPACE:-solo-network}"
CONTEXT="${CONTEXT:-kind-solo-cluster}"

BN_POD_LABEL="block-node-1"
BN_DATA_PATH="/opt/hiero/block-node/data/live"
LOCAL_BLOCKS_DIR="/tmp/wrb-cli-validation-blocks"
MIN_BLOCKS=5

function kctl {
    kubectl --context "${CONTEXT}" "$@"
}

function log {
    echo "[wrb-cli-validate] $*"
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

function do_validate {
    # Find the tools shadow jar
    local jar
    jar=$(find "${REPO_ROOT}/tools-and-tests/tools/build/libs" -name 'tools-*-all.jar' -print -quit 2>/dev/null)
    if [[ -z "$jar" ]]; then
        log "ERROR: Shadow jar not found. Run 'build' subcommand first."
        return 1
    fi
    log "Using shadow jar: ${jar}"

    # Verify BN pod is running
    local pod_name
    pod_name=$(kctl get pods -n "${NAMESPACE}" -l "app.kubernetes.io/name=${BN_POD_LABEL}" \
        -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
    if [[ -z "$pod_name" ]]; then
        log "ERROR: Block Node pod '${BN_POD_LABEL}' not found in namespace '${NAMESPACE}'"
        return 1
    fi
    log "Found BN pod: ${pod_name}"

    # Count block files on BN pod (recursively, since blocks are in nested directories)
    local block_count
    block_count=$(kctl exec -n "${NAMESPACE}" "${pod_name}" -- \
        sh -c "find ${BN_DATA_PATH} -type f -name '*.blk.*' 2>/dev/null | wc -l" | tr -d '[:space:]')
    log "Block count on BN: ${block_count}"
    if [[ "${block_count}" -lt "${MIN_BLOCKS}" ]]; then
        log "ERROR: Only ${block_count} blocks found, need at least ${MIN_BLOCKS}"
        return 1
    fi

    # Clean up any previous extraction
    rm -rf "${LOCAL_BLOCKS_DIR}"
    mkdir -p "${LOCAL_BLOCKS_DIR}"

    # Extract blocks from BN pod
    log "Extracting blocks from ${pod_name}:${BN_DATA_PATH} ..."
    kctl cp "${NAMESPACE}/${pod_name}:${BN_DATA_PATH}" "${LOCAL_BLOCKS_DIR}/live"
    if [[ ! -d "${LOCAL_BLOCKS_DIR}/live" ]]; then
        log "ERROR: Block extraction failed - ${LOCAL_BLOCKS_DIR}/live not found"
        rm -rf "${LOCAL_BLOCKS_DIR}"
        return 1
    fi

    local extracted_count
    extracted_count=$(find "${LOCAL_BLOCKS_DIR}/live" -type f -name '*.blk.*' 2>/dev/null | wc -l | tr -d '[:space:]')
    log "Extracted ${extracted_count} blocks to ${LOCAL_BLOCKS_DIR}/live"

    # Run WRB CLI blocks validate on live blocks (skip required items since live blocks don't have RecordFile/BlockProof)
    log "Running: java -jar ${jar} blocks validate ..."
    local rc=0
    local output
    output=$(java -jar "${jar}" blocks validate \
        --skip-signatures \
        --skip-supply \
        --skip-required-items \
        --validate-balances=false \
        --no-resume \
        "${LOCAL_BLOCKS_DIR}/live" 2>&1) || rc=$?

    echo "$output"

    # Clean up extracted blocks
    log "Cleaning up extracted blocks..."
    rm -rf "${LOCAL_BLOCKS_DIR}"

    # Check for validation failure in output (ValidateBlocksCommand returns 0 even on failure)
    if echo "$output" | grep -q "VALIDATION FAILED"; then
        log "VALIDATION FAILED (detected in output)"
        return 1
    elif [[ ${rc} -ne 0 ]]; then
        log "VALIDATION FAILED (exit code: ${rc})"
        return ${rc}
    else
        log "VALIDATION PASSED"
        return 0
    fi
}

# Main
case "${1:-}" in
    build)
        do_build
        ;;
    validate)
        do_validate
        ;;
    *)
        echo "Usage: $0 {build|validate}"
        exit 1
        ;;
esac
