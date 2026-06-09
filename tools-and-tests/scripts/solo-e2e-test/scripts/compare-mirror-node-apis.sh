#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Mirror Node API Comparison Wrapper
#
# Compares two Mirror Node REST APIs to validate identical results
# when ingesting live blocks vs WRB CLI-wrapped blocks.
#
# Usage:
#   ./compare-mirror-node-apis.sh [--block-range START:END] [--output report.json]
#
# Environment (inherited from solo-test-runner.sh):
#   NAMESPACE  - Kubernetes namespace (default: solo-network)
#   CONTEXT    - Kubernetes context (default: kind-solo-cluster)
#
# Examples:
#   ./compare-mirror-node-apis.sh
#   ./compare-mirror-node-apis.sh --block-range 0:100
#   ./compare-mirror-node-apis.sh --output /tmp/comparison-report.json

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../../.." && pwd)"

NAMESPACE="${NAMESPACE:-solo-network}"
CONTEXT="${CONTEXT:-kind-solo-cluster}"

# Mirror Node service names (adjust based on deployment)
MN1_SERVICE="${MN1_SERVICE:-mirror-1-rest}"
MN2_SERVICE="${MN2_SERVICE:-mirror-2-rest}"

# Detect if running in Kubernetes cluster vs local with port-forward
if [[ -n "${KUBERNETES_SERVICE_HOST:-}" ]] || [[ "${CI:-false}" == "true" ]]; then
    # Running in cluster - use service endpoints
    MN1_URL="${MN1_URL:-http://${MN1_SERVICE}.${NAMESPACE}.svc.cluster.local:80/api/v1}"
    MN2_URL="${MN2_URL:-http://${MN2_SERVICE}.${NAMESPACE}.svc.cluster.local:80/api/v1}"
else
    # Running locally - use port-forward endpoints
    MN1_PORT="${MN1_PORT:-5551}"
    MN2_PORT="${MN2_PORT:-5552}"
    MN1_URL="${MN1_URL:-http://localhost:${MN1_PORT}/api/v1}"
    MN2_URL="${MN2_URL:-http://localhost:${MN2_PORT}/api/v1}"
fi

# Output file
OUTPUT_FILE="${OUTPUT_FILE:-/tmp/mirror-node-comparison-report.json}"

function kctl {
    kubectl --context "${CONTEXT}" "$@"
}

function log {
    echo "[compare-mn-apis] $*"
}

function check_python {
    if ! command -v python3 >/dev/null 2>&1; then
        log "ERROR: python3 not found"
        return 1
    fi

    # Check for requests module
    if ! python3 -c "import requests" 2>/dev/null; then
        log "ERROR: Python requests module not found"
        log "Install with: pip3 install requests"
        return 1
    fi

    log "Python environment OK"
}

function check_mirror_nodes {
    log "Checking Mirror Node endpoints..."
    log "  MN1: ${MN1_URL}"
    log "  MN2: ${MN2_URL}"

    # Check MN1
    log "Testing MN1..."
    if curl -s -f --max-time 10 "${MN1_URL}/network/nodes" > /dev/null 2>&1; then
        log "  ✅ MN1 is accessible"
    else
        log "  ❌ MN1 is NOT accessible at ${MN1_URL}"
        if [[ -z "${KUBERNETES_SERVICE_HOST:-}" ]] && [[ "${CI:-false}" != "true" ]]; then
            log "  Make sure port-forward is active: task port-forward"
        else
            log "  Check if Mirror Node pods are running: kubectl get pods -n ${NAMESPACE}"
        fi
        return 1
    fi

    # Check MN2
    log "Testing MN2..."
    if curl -s -f --max-time 10 "${MN2_URL}/network/nodes" > /dev/null 2>&1; then
        log "  ✅ MN2 is accessible"
    else
        log "  ❌ MN2 is NOT accessible at ${MN2_URL}"
        if [[ -z "${KUBERNETES_SERVICE_HOST:-}" ]] && [[ "${CI:-false}" != "true" ]]; then
            log "  Make sure port-forward is active for MN2"
        else
            log "  Check if Mirror Node 2 pods are running: kubectl get pods -n ${NAMESPACE}"
        fi
        return 1
    fi

    log "Both Mirror Nodes are accessible"
}

function wait_for_mirror_node_sync {
    local mn_url=$1
    local min_blocks=${2:-10}
    local timeout=${3:-120}

    log "Waiting for Mirror Node to have at least ${min_blocks} blocks..."

    local elapsed=0
    while [[ ${elapsed} -lt ${timeout} ]]; do
        local block_count
        block_count=$(curl -s "${mn_url}/blocks?limit=1&order=desc" | \
            python3 -c "import sys, json; data=json.load(sys.stdin); print(data.get('blocks', [{}])[0].get('number', 0))" 2>/dev/null || echo "0")

        if [[ "${block_count}" -ge "${min_blocks}" ]]; then
            log "Mirror Node has ${block_count} blocks"
            return 0
        fi

        log "Mirror Node has ${block_count} blocks, waiting... (${elapsed}s/${timeout}s)"
        sleep 5
        elapsed=$((elapsed + 5))
    done

    log "ERROR: Mirror Node did not reach ${min_blocks} blocks within ${timeout}s"
    return 1
}

function do_compare {
    log "Running Mirror Node API comparison..."

    # Check prerequisites
    check_python || return 1
    check_mirror_nodes || return 1

    # Wait for Mirror Nodes to sync
    log "Waiting for Mirror Nodes to sync..."
    wait_for_mirror_node_sync "${MN1_URL}" 10 120 || {
        log "WARNING: MN1 may not have enough blocks yet"
    }
    wait_for_mirror_node_sync "${MN2_URL}" 10 120 || {
        log "WARNING: MN2 may not have enough blocks yet"
    }

    # Run Python comparison script
    log "Running comparison (this may take a few minutes)..."
    log "  MN1: ${MN1_URL}"
    log "  MN2: ${MN2_URL}"
    log "  Output: ${OUTPUT_FILE}"
    log ""

    local python_script="${SCRIPT_DIR}/python/compare_mirror_nodes.py"

    if [[ ! -f "${python_script}" ]]; then
        log "ERROR: Comparison script not found: ${python_script}"
        return 1
    fi

    # Parse command line arguments for pass-through
    local extra_args=()
    while [[ $# -gt 0 ]]; do
        case $1 in
            --block-range)
                extra_args+=("--block-range" "$2")
                shift 2
                ;;
            --output)
                OUTPUT_FILE="$2"
                extra_args+=("--output" "$2")
                shift 2
                ;;
            --verbose|-v)
                extra_args+=("--verbose")
                shift
                ;;
            --skip-*)
                extra_args+=("$1")
                shift
                ;;
            *)
                log "WARNING: Unknown argument: $1"
                shift
                ;;
        esac
    done

    # Run comparison
    local rc=0
    python3 "${python_script}" \
        "${MN1_URL}" \
        "${MN2_URL}" \
        --output "${OUTPUT_FILE}" \
        --verbose \
        "${extra_args[@]}" || rc=$?

    if [[ ${rc} -eq 0 ]]; then
        log ""
        log "✅ SUCCESS: Mirror Nodes are IDENTICAL"
        log "Detailed report: ${OUTPUT_FILE}"
        return 0
    else
        log ""
        log "❌ FAILURE: Mirror Nodes have differences"
        log "Detailed report: ${OUTPUT_FILE}"
        return 1
    fi
}

# Main
do_compare "$@"