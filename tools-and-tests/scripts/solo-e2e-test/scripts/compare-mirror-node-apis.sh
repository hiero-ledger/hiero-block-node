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

function discover_mirror_node_services {
    log "Discovering Mirror Node services in namespace ${NAMESPACE}..."

    # List all services matching mirror-*-rest pattern
    local services
    services=$(kctl get svc -n "${NAMESPACE}" -o name 2>/dev/null | grep "mirror-.*-rest" || echo "")

    if [[ -z "${services}" ]]; then
        log "  WARNING: No Mirror Node REST services found matching pattern 'mirror-*-rest'"
        log "  Available services:"
        kctl get svc -n "${NAMESPACE}" -o name | grep -i mirror || echo "    (none)"
        return 1
    fi

    log "  Found services: $(echo "${services}" | tr '\n' ' ')"

    # Extract service names and update MN1/MN2 URLs if not explicitly set
    local svc_array=()
    while IFS= read -r svc; do
        [[ -n "$svc" ]] && svc_array+=("$(basename "$svc")")
    done <<< "$services"

    if [[ ${#svc_array[@]} -ge 1 ]] && [[ "${MN1_SERVICE}" == "mirror-1-rest" ]]; then
        MN1_SERVICE="${svc_array[0]}"
        MN1_URL="http://${MN1_SERVICE}.${NAMESPACE}.svc.cluster.local:80/api/v1"
        log "  Using MN1 service: ${MN1_SERVICE}"
    fi

    if [[ ${#svc_array[@]} -ge 2 ]] && [[ "${MN2_SERVICE}" == "mirror-2-rest" ]]; then
        MN2_SERVICE="${svc_array[1]}"
        MN2_URL="http://${MN2_SERVICE}.${NAMESPACE}.svc.cluster.local:80/api/v1"
        log "  Using MN2 service: ${MN2_SERVICE}"
    fi
}

function check_mirror_nodes {
    log "Checking Mirror Node endpoints..."

    # In CI/K8s, discover actual service names
    if [[ -n "${KUBERNETES_SERVICE_HOST:-}" ]] || [[ "${CI:-false}" == "true" ]]; then
        discover_mirror_node_services || log "  WARNING: Service discovery failed, using defaults"
    fi

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

function run_in_kubernetes_pod {
    log "Running comparison inside Kubernetes pod..."

    local python_script="${SCRIPT_DIR}/python/compare_mirror_nodes.py"
    if [[ ! -f "${python_script}" ]]; then
        log "ERROR: Comparison script not found: ${python_script}"
        return 1
    fi

    # Find a running block node pod to use for execution
    local exec_pod
    exec_pod=$(kctl get pods -n "${NAMESPACE}" -l "block-node.hiero.com/type=block-node" \
        -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")

    if [[ -z "${exec_pod}" ]]; then
        log "ERROR: No block node pods found to execute comparison"
        return 1
    fi

    log "Using pod ${exec_pod} for comparison execution"

    # Copy Python script to pod
    log "Copying comparison script to pod..."
    kctl cp -n "${NAMESPACE}" "${python_script}" "${exec_pod}:/tmp/compare_mirror_nodes.py" || {
        log "ERROR: Failed to copy script to pod"
        return 1
    }

    # Install requests module if needed
    log "Installing Python dependencies in pod..."
    kctl exec -n "${NAMESPACE}" "${exec_pod}" -- \
        python3 -m pip install --quiet --user requests 2>&1 | grep -v "Requirement already satisfied" || true

    # Run comparison
    log "Running comparison..."
    log "  MN1: ${MN1_URL}"
    log "  MN2: ${MN2_URL}"

    local rc=0
    kctl exec -n "${NAMESPACE}" "${exec_pod}" -- \
        python3 /tmp/compare_mirror_nodes.py \
        "${MN1_URL}" \
        "${MN2_URL}" \
        --output /tmp/comparison-report.json \
        --verbose \
        --block-range 0:100 || rc=$?

    # Copy result back
    if kctl cp -n "${NAMESPACE}" "${exec_pod}:/tmp/comparison-report.json" "${OUTPUT_FILE}" 2>/dev/null; then
        log "Comparison report saved to ${OUTPUT_FILE}"
    fi

    # Cleanup
    kctl exec -n "${NAMESPACE}" "${exec_pod}" -- rm -f /tmp/compare_mirror_nodes.py /tmp/comparison-report.json 2>/dev/null || true

    if [[ ${rc} -eq 0 ]]; then
        log ""
        log "✅ SUCCESS: Mirror Nodes are IDENTICAL"
        return 0
    else
        log ""
        log "❌ FAILURE: Mirror Nodes have differences"
        return 1
    fi
}

function do_compare {
    log "Running Mirror Node API comparison..."

    # If running in CI/Kubernetes, run comparison inside a pod
    if [[ -n "${KUBERNETES_SERVICE_HOST:-}" ]] || [[ "${CI:-false}" == "true" ]]; then
        log "CI environment detected, running comparison inside Kubernetes pod"
        return run_in_kubernetes_pod
    fi

    # Local execution (with port-forward)
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