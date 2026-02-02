#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Solo E2E Test Runner - Simplified YAML-driven test execution
#
# Executes test definitions with sequential event execution.
# Events run in delay order with sleeps between them.
#
# Usage:
#   ./solo-test-runner.sh --test <test-file.yaml> [options]
#
# Options:
#   --test FILE            Test definition file (required)
#   --topology NAME        Topology to validate against (default: single)
#   --namespace NS         Kubernetes namespace (default: solo-network)
#   --context CTX          Kubernetes context (default: kind-solo-cluster)
#   --topologies-dir DIR   Topologies directory (default: ../topologies)
#   --proto-path PATH      Path to protobuf files (for grpcurl)
#   --output MODE          Output mode: console, github-summary (default: console)
#   --validate             Validate test file only, don't run
#   --help                 Show this help message

set -o pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Defaults
TEST_FILE=""
TOPOLOGY="${TOPOLOGY:-single}"
NAMESPACE="${NAMESPACE:-solo-network}"
CONTEXT="${CONTEXT:-kind-solo-cluster}"
TOPOLOGIES_DIR="${TOPOLOGIES_DIR:-${ROOT_DIR}/topologies}"
PROTO_PATH="${PROTO_PATH:-}"
OUTPUT_MODE="console"
VALIDATE_ONLY=false
DEPLOYMENT="${DEPLOYMENT:-deployment-solo}"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Test metadata
TEST_NAME=""
TEST_DESC=""

# Results
EVENTS_COMPLETED=0
EVENTS_FAILED=0
ASSERTIONS_PASSED=0
ASSERTIONS_FAILED=0


# ============================================================================
# Argument Parsing
# ============================================================================
function show_help {
    cat << 'EOF'
Solo E2E Test Runner - Simplified YAML-driven test execution

Usage:
  ./solo-test-runner.sh --test <test-file.yaml> [options]

Options:
  --test FILE            Test definition file (required)
  --topology NAME        Topology to validate against (default: single)
  --namespace NS         Kubernetes namespace (default: solo-network)
  --context CTX          Kubernetes context (default: kind-solo-cluster)
  --topologies-dir DIR   Topologies directory (default: ../topologies)
  --proto-path PATH      Path to protobuf files (for grpcurl)
  --output MODE          Output mode: console, github-summary (default: console)
  --validate             Validate test file only, don't run
  --help                 Show this help message
EOF
    exit 0
}

while [[ $# -gt 0 ]]; do
    case $1 in
        --test)       TEST_FILE="$2"; shift 2 ;;
        --topology)   TOPOLOGY="$2"; shift 2 ;;
        --namespace)  NAMESPACE="$2"; shift 2 ;;
        --context)    CONTEXT="$2"; shift 2 ;;
        --topologies-dir) TOPOLOGIES_DIR="$2"; shift 2 ;;
        --proto-path) PROTO_PATH="$2"; shift 2 ;;
        --output)     OUTPUT_MODE="$2"; shift 2 ;;
        --validate)   VALIDATE_ONLY=true; shift ;;
        --help|-h)    show_help ;;
        *)            echo "Unknown option: $1"; exit 1 ;;
    esac
done

if [[ -z "${TEST_FILE}" ]]; then
    echo "ERROR: --test is required"
    show_help
fi

# Resolve test file path
if [[ ! -f "${TEST_FILE}" ]]; then
    if [[ -f "${ROOT_DIR}/${TEST_FILE}" ]]; then
        TEST_FILE="${ROOT_DIR}/${TEST_FILE}"
    else
        echo "ERROR: Test file not found: ${TEST_FILE}"
        exit 1
    fi
fi

if ! command -v yq >/dev/null 2>&1; then
    echo "ERROR: yq is required. Install from: https://github.com/mikefarah/yq"
    exit 1
fi

# ============================================================================
# Utility Functions
# ============================================================================
function format_time {
    local seconds=$1
    printf "%02d:%02d" $((seconds / 60)) $((seconds % 60))
}

function log {
    local level="$1"
    local msg="$2"
    local elapsed="${3:-0}"
    local time_str
    time_str=$(format_time "$elapsed")

    case "$level" in
        INFO)  echo -e "[${time_str}] ${BLUE}INFO${NC} $msg" ;;
        EVENT) echo -e "[${time_str}] ${YELLOW}EVENT${NC} $msg" ;;
        PASS)  echo -e "[${time_str}] ${GREEN}PASS${NC} $msg" ;;
        FAIL)  echo -e "[${time_str}] ${RED}FAIL${NC} $msg" ;;
        *)     echo -e "[${time_str}] $msg" ;;
    esac
}

function kctl {
    kubectl --context "${CONTEXT}" "$@"
}

# Validates that PROTO_PATH is set and points to a valid directory
# Returns: 0 if valid, 1 if invalid (with error message to stdout)
function validate_proto_path {
    local context="${1:-}"
    if [[ -z "${PROTO_PATH}" || ! -d "${PROTO_PATH}" ]]; then
        [[ -n "$context" ]] && echo "${context}: PROTO_PATH not set or invalid" || echo "PROTO_PATH not set or invalid"
        return 1
    fi
    return 0
}

# ============================================================================
# Event Execution Functions
# ============================================================================
function execute_load_start {
    local test_class="${1:-CryptoTransferLoadTest}"
    local concurrency="${2:-5}"
    local accounts="${3:-10}"
    local duration="${4:-300}"
    local extra_args="${5:-}"

    # Construct NLG_ARGS from test definition parameters
    local nlg_args="-c $concurrency -a $accounts -tt $duration"
    [[ -n "$extra_args" ]] && nlg_args="$nlg_args $extra_args"

    echo "Starting NLG (async): class=$test_class args='$nlg_args'"

    export DEPLOYMENT NAMESPACE
    export NLG_ARGS="$nlg_args"

    # Run in background so test can continue
    "${SCRIPT_DIR}/solo-load-generate.sh" start &
    local pid=$!
    echo "NLG started in background (PID: $pid)"

    # Brief wait for Solo to initialize
    sleep 5
}

function execute_load_stop {
    local test_class="${1:-CryptoTransferLoadTest}"
    echo "Stopping NLG: class=$test_class"

    export DEPLOYMENT NAMESPACE
    "${SCRIPT_DIR}/solo-load-generate.sh" stop
}

function execute_print_metrics {
    local target="${1:-all}"
    local topology_file="${TOPOLOGIES_DIR}/${TOPOLOGY}.yaml"

    if [[ "$target" == "all" ]]; then
        local block_nodes="block-node-1"
        if [[ -f "$topology_file" ]]; then
            block_nodes=$(grep -E '^[[:space:]]+block-node-[0-9]+:' "$topology_file" | sed 's/://g' | awk '{print $1}' || echo "block-node-1")
        fi

        local port=16007
        for bn in $block_nodes; do
            echo "=== Metrics for $bn (port $port) ==="
            "${SCRIPT_DIR}/solo-metrics-summary.sh" "$port" text || echo "Metrics unavailable"
            port=$((port + 1))
        done
    else
        local node_num
        node_num=$(echo "$target" | grep -oE '[0-9]+$' || echo "1")
        local port=$((16006 + node_num))
        echo "=== Metrics for $target (port $port) ==="
        "${SCRIPT_DIR}/solo-metrics-summary.sh" "$port" text || echo "Metrics unavailable"
    fi
}

function execute_network_status {
    "${SCRIPT_DIR}/solo-network-status.sh" \
        --namespace "${NAMESPACE}" \
        --topology "${TOPOLOGY}" \
        --topologies-dir "${TOPOLOGIES_DIR}" \
        --context "${CONTEXT}" \
        --proto-path "${PROTO_PATH:-}" \
        --output console || echo "Network status unavailable"
}

function execute_restart {
    local target="$1"
    echo "Restarting $target..."

    local resource_type="deployment"
    if kctl get statefulset -n "${NAMESPACE}" "${target}" >/dev/null 2>&1; then
        resource_type="statefulset"
    fi

    kctl rollout restart "${resource_type}/${target}" -n "${NAMESPACE}"
    kctl rollout status "${resource_type}/${target}" -n "${NAMESPACE}" --timeout=300s
    echo "$target restart completed"
}

function execute_node_down {
    local target="$1"
    echo "Scaling down $target..."

    local resource_type="deployment"
    if kctl get statefulset -n "${NAMESPACE}" "${target}" >/dev/null 2>&1; then
        resource_type="statefulset"
    fi

    kctl scale "${resource_type}/${target}" -n "${NAMESPACE}" --replicas=0
    kctl wait --for=delete pod -l "app.kubernetes.io/name=${target}" -n "${NAMESPACE}" --timeout=120s 2>/dev/null || true
    echo "$target is down"
}

function execute_node_up {
    local target="$1"
    echo "Scaling up $target..."

    local resource_type="deployment"
    if kctl get statefulset -n "${NAMESPACE}" "${target}" >/dev/null 2>&1; then
        resource_type="statefulset"
    fi

    kctl scale "${resource_type}/${target}" -n "${NAMESPACE}" --replicas=1
    kctl wait --for=condition=ready pod -l "app.kubernetes.io/name=${target}" -n "${NAMESPACE}" --timeout=300s
    echo "$target is up"
}

function execute_port_forward {
    echo "Refreshing port forwards..."
    # Kill existing port-forwards
    pkill -f "kubectl.*port-forward.*${NAMESPACE}" 2>/dev/null || true
    sleep 2
    # Restart port forwards (script only takes --namespace)
    "${SCRIPT_DIR}/solo-port-forward.sh" --namespace "${NAMESPACE}" &
    sleep 10
    echo "Port forwards refreshed"
}

function execute_clear_block_storage {
    local target="$1"
    echo "Clearing block storage on $target..."

    local pod_name="${target}-0"

    # Clear live storage (recent blocks)
    echo "Clearing live storage..."
    kctl exec "${pod_name}" -n "${NAMESPACE}" -- \
        sh -c "rm -rf /opt/hiero/block-node/data/live/* 2>/dev/null || true"

    # Clear archive storage (historic blocks)
    echo "Clearing archive storage..."
    kctl exec "${pod_name}" -n "${NAMESPACE}" -- \
        sh -c "rm -rf /opt/hiero/block-node/data/archive/* 2>/dev/null || true"

    # Clear verification state
    echo "Clearing verification state..."
    kctl exec "${pod_name}" -n "${NAMESPACE}" -- \
        sh -c "rm -rf /opt/hiero/block-node/data/verification/* 2>/dev/null || true"

    echo "Block storage cleared on $target"
}

function execute_scale_down {
    local target="$1"
    echo "Scaling down $target..."

    local resource_type="statefulset"
    if ! kctl get statefulset -n "${NAMESPACE}" "${target}" >/dev/null 2>&1; then
        resource_type="deployment"
    fi

    kctl scale "${resource_type}/${target}" -n "${NAMESPACE}" --replicas=0
    kctl wait --for=delete pod -l "app.kubernetes.io/name=${target}" -n "${NAMESPACE}" --timeout=120s 2>/dev/null || true
    echo "$target scaled down"
}

function execute_deploy_blocknode {
    local args="$1"
    local bn_name bn_sources greedy chart_version

    bn_name=$(echo "$args" | yq '.name // "block-node-3"')
    bn_sources=$(echo "$args" | yq '.backfill_sources // []')
    greedy=$(echo "$args" | yq '.greedy // true')
    chart_version=$(echo "$args" | yq '.chart_version // ""')

    echo "Deploying new block node: ${bn_name}..."

    # Generate backfill values file
    local values_file="/tmp/bn-${bn_name}-values.yaml"
    local sources_yaml=""
    local priority=1

    for source in $(echo "$bn_sources" | yq -r '.[]'); do
        local source_dns="${source}.${NAMESPACE}.svc.cluster.local"
        sources_yaml="${sources_yaml}
      - address: \"${source_dns}\"
        port: 40840
        priority: ${priority}"
        priority=$((priority + 1))
    done

    # Enable greedy backfill mode for catching up from peers
    cat > "${values_file}" << EOF
# Generated by solo-test-runner.sh for dynamic block node deployment
blockNode:
  config:
    BACKFILL_BLOCK_NODE_SOURCES_PATH: "/opt/hiero/block-node/backfill/block-node-sources.json"
    BACKFILL_GREEDY: "${greedy}"
  backfill:
    path: "/opt/hiero/block-node/backfill"
    filename: "block-node-sources.json"
    sources:${sources_yaml}
EOF

    echo "Generated values file:"
    cat "${values_file}"

    # Get cluster ref from context
    local cluster_ref
    cluster_ref="kind-${CONTEXT#kind-}"
    [[ "${CONTEXT}" == "${cluster_ref}" ]] && cluster_ref="${CONTEXT}"

    # Build chart version args if specified
    local version_args=""
    if [[ -n "${chart_version}" && "${chart_version}" != "null" ]]; then
        version_args="--chart-version ${chart_version}"
    fi

    # Try Solo CLI first, fall back to Helm if it fails (e.g., when other BNs are down)
    # shellcheck disable=SC2086
    if ! solo block node add \
        --deployment "${DEPLOYMENT}" \
        --cluster-ref "${cluster_ref}" \
        ${version_args} \
        -f "${values_file}" \
        --quiet-mode 2>&1; then

        echo "Solo CLI failed, falling back to Helm deployment..."

        # Get the chart version from existing deployment (use same version as other BNs)
        local helm_chart_version
        helm_chart_version=$(helm list -n "${NAMESPACE}" -o json 2>/dev/null | \
            jq -r '.[] | select(.name | startswith("block-node-")) | .chart' | \
            head -1 | sed 's/block-node-server-//')

        if [[ -z "${helm_chart_version}" ]]; then
            helm_chart_version="${chart_version:-0.26.1}"
            helm_chart_version="${helm_chart_version#v}"
        fi

        echo "Using chart version: ${helm_chart_version}"

        # Find the chart in Solo's cache
        local chart_path="${HOME}/.solo/cache/charts/block-node-server-${helm_chart_version}.tgz"
        if [[ ! -f "${chart_path}" ]]; then
            chart_path=$(ls -t "${HOME}"/.solo/cache/charts/block-node-server-*.tgz 2>/dev/null | head -1)
        fi

        if [[ -f "${chart_path}" ]]; then
            echo "Using cached chart: ${chart_path}"
            helm install "${bn_name}" "${chart_path}" \
                --namespace "${NAMESPACE}" \
                -f "${values_file}" \
                --wait --timeout 5m
        else
            echo "No cached chart found, pulling from OCI registry..."
            helm install "${bn_name}" oci://ghcr.io/hiero-ledger/hiero-block-node/block-node-server \
                --version "${helm_chart_version}" \
                --namespace "${NAMESPACE}" \
                -f "${values_file}" \
                --wait --timeout 5m
        fi

        echo "Deployed ${bn_name} via Helm"
    fi

    # Wait for pod to be ready
    kctl wait --for=condition=ready pod \
        -l "app.kubernetes.io/name=${bn_name}" \
        -n "${NAMESPACE}" \
        --timeout=300s

    echo "${bn_name} deployed and ready"
}

function execute_reconfigure_cn_streaming {
    local args="$1"
    local cn_name block_nodes_json

    cn_name=$(echo "$args" | yq '.consensus_node // "node1"')
    block_nodes_json=$(echo "$args" | yq -o=json '.block_nodes')

    echo "Reconfiguring CN ${cn_name} to stream to new block nodes..."

    # Build the block-nodes.json content
    local nodes_array=""
    local first=true
    for bn in $(echo "$block_nodes_json" | yq -r '.[]'); do
        local bn_dns="${bn}.${NAMESPACE}.svc.cluster.local"
        [[ "$first" != "true" ]] && nodes_array="${nodes_array},"
        nodes_array="${nodes_array}
    {
      \"address\": \"${bn_dns}\",
      \"streamingPort\": 40840,
      \"servicePort\": 40840,
      \"priority\": 1
    }"
        first=false
    done

    local config_content="{
  \"nodes\": [${nodes_array}
  ],
  \"blockItemBatchSize\": 256
}"

    echo "New block-nodes.json for ${cn_name}:"
    echo "${config_content}"

    # Get the CN pod name
    local cn_pod="network-${cn_name}-0"

    # Write the config to the CN pod
    local config_path="/opt/hgcapp/services-hedera/HapiApp2.0/data/config/block-nodes.json"

    echo "${config_content}" | kctl exec -i "${cn_pod}" -n "${NAMESPACE}" -c root-container -- \
        bash -c "cat > ${config_path}"

    echo "Updated ${config_path} on ${cn_pod}"

    # Verify the config was written
    echo "Verifying config:"
    kctl exec "${cn_pod}" -n "${NAMESPACE}" -c root-container -- cat "${config_path}"

    echo "CN ${cn_name} reconfigured to stream to: $(echo "$block_nodes_json" | yq -r '. | join(", ")')"
}

function execute_event {
    local event_type="$1"
    local target="$2"
    local args="$3"

    case "$event_type" in
        load-start)
            local test_class concurrency accounts duration extra_args
            test_class=$(echo "$args" | yq '.test_class // "CryptoTransferLoadTest"')
            concurrency=$(echo "$args" | yq '.concurrency // 5')
            accounts=$(echo "$args" | yq '.accounts // 10')
            duration=$(echo "$args" | yq '.duration // 300')
            extra_args=$(echo "$args" | yq '.extra_args // ""')
            execute_load_start "$test_class" "$concurrency" "$accounts" "$duration" "$extra_args"
            ;;
        load-stop)
            local test_class
            test_class=$(echo "$args" | yq '.test_class // "CryptoTransferLoadTest"')
            execute_load_stop "$test_class"
            ;;
        print-metrics)
            local metrics_target
            metrics_target=$(echo "$args" | yq '.target // "all"')
            [[ -n "$target" && "$target" != "null" ]] && metrics_target="$target"
            execute_print_metrics "$metrics_target"
            ;;
        network-status)
            execute_network_status
            ;;
        restart)
            [[ -z "$target" || "$target" == "null" ]] && target=$(echo "$args" | yq '.target // ""')
            execute_restart "$target"
            ;;
        node-down)
            [[ -z "$target" || "$target" == "null" ]] && target=$(echo "$args" | yq '.target // ""')
            execute_node_down "$target"
            ;;
        node-up)
            [[ -z "$target" || "$target" == "null" ]] && target=$(echo "$args" | yq '.target // ""')
            execute_node_up "$target"
            ;;
        port-forward)
            execute_port_forward
            ;;
        sleep)
            local seconds
            seconds=$(echo "$args" | yq '.seconds // 10')
            echo "Sleeping for ${seconds}s..."
            sleep "$seconds"
            ;;
        command)
            local script
            script=$(echo "$args" | yq '.script // ""')
            echo "Executing: $script"
            eval "$script"
            ;;
        clear-block-storage)
            [[ -z "$target" || "$target" == "null" ]] && target=$(echo "$args" | yq '.target // ""')
            execute_clear_block_storage "$target"
            ;;
        scale-down)
            [[ -z "$target" || "$target" == "null" ]] && target=$(echo "$args" | yq '.target // ""')
            execute_scale_down "$target"
            ;;
        scale-up)
            [[ -z "$target" || "$target" == "null" ]] && target=$(echo "$args" | yq '.target // ""')
            execute_node_up "$target"
            ;;
        deploy-block-node)
            execute_deploy_blocknode "$args"
            ;;
        reconfigure-cn-streaming)
            execute_reconfigure_cn_streaming "$args"
            ;;
        *)
            echo "ERROR: Unknown event type: $event_type"
            return 1
            ;;
    esac
}

# ============================================================================
# Assertion Functions
# ============================================================================

# Get list of all block nodes from topology file
function get_all_block_nodes {
    local topology_file="${TOPOLOGIES_DIR}/${TOPOLOGY}.yaml"
    if [[ -f "$topology_file" ]]; then
        grep -E '^[[:space:]]+block-node-[0-9]+:' "$topology_file" | sed 's/://g' | awk '{print $1}' || echo "block-node-1"
    else
        echo "block-node-1"
    fi
}

function get_bn_metrics_port {
    local target="$1"
    local node_num
    node_num=$(echo "$target" | grep -oE '[0-9]+$' || echo "1")
    echo $((16006 + node_num))
}

function get_bn_grpc_port {
    local target="$1"
    local node_num
    node_num=$(echo "$target" | grep -oE '[0-9]+$' || echo "1")
    echo $((40839 + node_num))
}

# Single node block availability check
function assert_block_available_single {
    local target="$1"
    local min_block="${2:-0}"
    local max_block_gte="${3:-0}"
    local port
    port=$(get_bn_grpc_port "$target")

    if ! validate_proto_path "${target}"; then
        return 1
    fi

    local import_args="-import-path ${PROTO_PATH}"

    local status_json
    # shellcheck disable=SC2086  # Intentional word splitting for import path argument
    status_json=$(grpcurl -plaintext -emit-defaults \
        ${import_args} \
        -proto block-node/api/node_service.proto \
        -d '{}' "localhost:${port}" \
        org.hiero.block.api.BlockNodeService/serverStatus 2>/dev/null)

    local first_block last_block
    first_block=$(echo "$status_json" | jq -r '.firstAvailableBlock // "null"')
    last_block=$(echo "$status_json" | jq -r '.lastAvailableBlock // "null"')

    if [[ "$first_block" == "null" || "$last_block" == "null" ]]; then
        echo "${target}: No blocks available"
        return 1
    fi

    if [[ "$first_block" -gt "$min_block" ]]; then
        echo "${target}: First block ($first_block) > expected min ($min_block)"
        return 1
    fi

    if [[ "$max_block_gte" -gt 0 && "$last_block" -lt "$max_block_gte" ]]; then
        echo "${target}: Last block ($last_block) < expected ($max_block_gte)"
        return 1
    fi

    echo "${target}: Blocks ${first_block}-${last_block}"
}

function assert_block_available {
    local target="${1:-all}"
    local min_block="${2:-0}"
    local max_block_gte="${3:-0}"

    if [[ "$target" == "all" ]]; then
        local failed=0
        local results=""
        for bn in $(get_all_block_nodes); do
            local result
            if result=$(assert_block_available_single "$bn" "$min_block" "$max_block_gte"); then
                results="${results}${result}\n"
            else
                results="${results}${result}\n"
                failed=1
            fi
        done
        echo -e "${results%\\n}"
        return $failed
    else
        assert_block_available_single "$target" "$min_block" "$max_block_gte"
    fi
}

# Single node health check
function assert_node_healthy_single {
    local target="$1"
    local status
    status=$(kctl get pods -n "${NAMESPACE}" -l "app.kubernetes.io/name=${target}" \
        -o jsonpath='{.items[0].status.phase}' 2>/dev/null)

    if [[ -z "$status" ]]; then
        echo "${target}: Pod not found"
        return 1
    fi

    if [[ "$status" != "Running" ]]; then
        echo "${target}: $status (expected Running)"
        return 1
    fi

    echo "${target}: Running"
}

function assert_node_healthy {
    local target="${1:-all}"

    if [[ "$target" == "all" ]]; then
        local failed=0
        local results=""
        for bn in $(get_all_block_nodes); do
            local result
            if result=$(assert_node_healthy_single "$bn"); then
                results="${results}${result}\n"
            else
                results="${results}${result}\n"
                failed=1
            fi
        done
        echo -e "${results%\\n}"
        return $failed
    else
        assert_node_healthy_single "$target"
    fi
}

# Single node error check
function assert_no_errors_single {
    local target="$1"
    local port
    port=$(get_bn_metrics_port "$target")
    local metrics
    metrics=$(curl -s "http://localhost:${port}/metrics" 2>/dev/null)

    if [[ -z "$metrics" ]]; then
        echo "${target}: Could not fetch metrics"
        return 1
    fi

    local verify_failed verify_errors stream_errors
    verify_failed=$(echo "$metrics" | grep "^hiero_block_node_verification_blocks_failed_total " | awk '{print $2}' | head -1)
    verify_errors=$(echo "$metrics" | grep "^hiero_block_node_verification_blocks_error_total " | awk '{print $2}' | head -1)
    stream_errors=$(echo "$metrics" | grep "^hiero_block_node_publisher_stream_errors_total " | awk '{print $2}' | head -1)

    verify_failed=$(printf "%.0f" "${verify_failed:-0}" 2>/dev/null || echo "0")
    verify_errors=$(printf "%.0f" "${verify_errors:-0}" 2>/dev/null || echo "0")
    stream_errors=$(printf "%.0f" "${stream_errors:-0}" 2>/dev/null || echo "0")

    local total=$((verify_failed + verify_errors + stream_errors))
    if [[ "$total" -gt 0 ]]; then
        echo "${target}: Errors: verify_failed=$verify_failed verify_errors=$verify_errors stream_errors=$stream_errors"
        return 1
    fi

    echo "${target}: 0 errors"
}

function assert_no_errors {
    local target="${1:-all}"

    if [[ "$target" == "all" ]]; then
        local failed=0
        local results=""
        for bn in $(get_all_block_nodes); do
            local result
            if result=$(assert_no_errors_single "$bn"); then
                results="${results}${result}\n"
            else
                results="${results}${result}\n"
                failed=1
            fi
        done
        echo -e "${results%\\n}"
        return $failed
    else
        assert_no_errors_single "$target"
    fi
}

# Helper to get block count from a node with error handling
function get_block_count {
    local target="$1"
    local port
    port=$(get_bn_grpc_port "$target")

    if ! validate_proto_path >/dev/null 2>&1; then
        echo ""
        return 1
    fi

    local import_args="-import-path ${PROTO_PATH}"
    local json block_count

    json=$(grpcurl -plaintext -emit-defaults \
        ${import_args} \
        -proto block-node/api/node_service.proto \
        -d '{}' "localhost:${port}" \
        org.hiero.block.api.BlockNodeService/serverStatus 2>/dev/null)

    block_count=$(echo "$json" | jq -r '.lastAvailableBlock // ""' 2>/dev/null)

    # Return empty if we got nothing valid
    if [[ -z "$block_count" || "$block_count" == "null" ]]; then
        echo ""
        return 1
    fi

    echo "$block_count"
}

# Single node blocks increasing check with retry logic
# Attempts multiple checks to handle backfill gaps and transient failures
# Default: 3 attempts x 60s = 180s total before declaring failure
function assert_blocks_increasing_single {
    local target="$1"
    local wait_seconds="${2:-60}"
    local max_attempts="${3:-3}"

    if ! validate_proto_path "${target}"; then
        return 1
    fi

    local attempt=1
    local baseline_block=""
    local current_block=""
    local last_error=""

    # Get baseline block count (retry up to 3 times if needed)
    for retry in 1 2 3; do
        baseline_block=$(get_block_count "$target")
        if [[ -n "$baseline_block" ]]; then
            break
        fi
        sleep 2
    done

    if [[ -z "$baseline_block" ]]; then
        echo "${target}: Could not get baseline block count"
        return 1
    fi

    # Try multiple times to detect block increase
    while [[ $attempt -le $max_attempts ]]; do
        sleep "$wait_seconds"

        # Get current block count (retry up to 3 times if needed)
        current_block=""
        for _ in 1 2 3; do
            current_block=$(get_block_count "$target")
            if [[ -n "$current_block" ]]; then
                break
            fi
            sleep 2
        done

        if [[ -z "$current_block" ]]; then
            attempt=$((attempt + 1))
            continue
        fi

        # Check if blocks increased
        if [[ "$current_block" -gt "$baseline_block" ]]; then
            local total_wait=$((wait_seconds * attempt))
            echo "${target}: $baseline_block -> $current_block (+$((current_block - baseline_block)) in ${total_wait}s, attempt $attempt/$max_attempts)"
            return 0
        fi

        attempt=$((attempt + 1))
    done

    # All attempts exhausted
    local total_wait=$((wait_seconds * max_attempts))
    echo "${target}: Blocks not increasing after ${max_attempts} attempts (${total_wait}s total): $baseline_block -> ${current_block:-unknown}"
    return 1
}

function assert_blocks_increasing {
    local target="${1:-all}"
    local wait_seconds="${2:-60}"
    local max_attempts="${3:-3}"

    if [[ "$target" == "all" ]]; then
        local failed=0
        local results=""
        for bn in $(get_all_block_nodes); do
            local result
            if result=$(assert_blocks_increasing_single "$bn" "$wait_seconds" "$max_attempts"); then
                results="${results}${result}\n"
            else
                results="${results}${result}\n"
                failed=1
            fi
        done
        echo -e "${results%\\n}"
        return $failed
    else
        assert_blocks_increasing_single "$target" "$wait_seconds" "$max_attempts"
    fi
}

function run_assertion {
    local assert_type="$1"
    local target="$2"
    local args="$3"

    case "$assert_type" in
        block-available)
            [[ -z "$target" || "$target" == "null" ]] && target=$(echo "$args" | yq '.target // "block-node-1"')
            local min_block max_block_gte
            min_block=$(echo "$args" | yq '.min_block // 0')
            max_block_gte=$(echo "$args" | yq '.max_block_gte // 0')
            assert_block_available "$target" "$min_block" "$max_block_gte"
            ;;
        node-healthy)
            [[ -z "$target" || "$target" == "null" ]] && target=$(echo "$args" | yq '.target // ""')
            assert_node_healthy "$target"
            ;;
        no-errors)
            [[ -z "$target" || "$target" == "null" ]] && target=$(echo "$args" | yq '.target // "block-node-1"')
            assert_no_errors "$target"
            ;;
        blocks-increasing)
            [[ -z "$target" || "$target" == "null" ]] && target=$(echo "$args" | yq '.target // "all"')
            local wait_seconds max_attempts
            wait_seconds=$(echo "$args" | yq '.wait_seconds // 60')
            max_attempts=$(echo "$args" | yq '.max_attempts // 3')
            assert_blocks_increasing "$target" "$wait_seconds" "$max_attempts"
            ;;
        *)
            echo "Unknown assertion type: $assert_type"
            return 1
            ;;
    esac
}

# ============================================================================
# Main Logic
# ============================================================================
function load_test_definition {
    TEST_NAME=$(yq '.name' "$TEST_FILE")
    TEST_DESC=$(yq '.description // ""' "$TEST_FILE")

    if [[ "$TEST_NAME" == "null" || -z "$TEST_NAME" ]]; then
        echo "ERROR: Missing 'name' field"; return 1
    fi
}

function validate_topology {
    local topology_file="${TOPOLOGIES_DIR}/${TOPOLOGY}.yaml"

    # Validate topology file exists
    if [[ ! -f "$topology_file" ]]; then
        echo "ERROR: Topology file not found: $topology_file"; return 1
    fi

    # Extract nodes that will be dynamically deployed via deploy-block-node events
    local dynamic_nodes
    dynamic_nodes=$(yq -r '.events[] | select(.type == "deploy-block-node") | .args.name // "block-node-3"' "$TEST_FILE" 2>/dev/null | sort -u)

    # Extract all targets from test YAML (events and assertions)
    local targets
    targets=$(yq -o=json '.events[].target, .assertions[].target' "$TEST_FILE" 2>/dev/null | \
        jq -r 'select(. != null and . != "")' | sort -u)

    # Check each target exists in topology (or is dynamically created)
    local missing=""
    for target in $targets; do
        # Skip non-node targets like "all" or empty/null
        [[ -z "$target" || "$target" == "null" || "$target" == "all" ]] && continue

        # Skip nodes that will be dynamically deployed
        if echo "$dynamic_nodes" | grep -qx "$target"; then
            continue
        fi

        # Check if target is a block-node
        if [[ "$target" == block-node-* ]]; then
            if ! yq -e ".block_nodes.\"$target\"" "$topology_file" >/dev/null 2>&1; then
                missing="$missing $target"
            fi
        # Check if target is a mirror-node
        elif [[ "$target" == mirror-* ]]; then
            if ! yq -e ".mirror_nodes.\"$target\"" "$topology_file" >/dev/null 2>&1; then
                missing="$missing $target"
            fi
        fi
    done

    if [[ -n "$missing" ]]; then
        echo "ERROR: Test references nodes not in topology '${TOPOLOGY}':$missing"
        return 1
    fi

    echo "Topology '${TOPOLOGY}' satisfies test requirements"
}

function run_events {
    local start_time
    start_time=$(date +%s)
    local event_count
    event_count=$(yq '.events | length' "$TEST_FILE")
    [[ "$event_count" == "null" ]] && event_count=0

    if [[ $event_count -eq 0 ]]; then
        log INFO "No events to execute"
        return 0
    fi

    log INFO "Executing $event_count events sequentially"

    # Get events sorted by delay
    local sorted_events
    sorted_events=$(yq -o=json '.events | sort_by(.delay)' "$TEST_FILE")

    echo "$sorted_events" | jq -c '.[]' | while read -r event; do
        local id delay event_type target desc args
        id=$(echo "$event" | jq -r '.id')
        delay=$(echo "$event" | jq -r '.delay // 0')
        event_type=$(echo "$event" | jq -r '.type')
        target=$(echo "$event" | jq -r '.target // ""')
        desc=$(echo "$event" | jq -r '.description // ""')
        args=$(echo "$event" | jq -c '.args // {}')

        [[ -z "$desc" || "$desc" == "null" ]] && desc="$event_type"

        # Wait until scheduled time
        local now elapsed wait_time
        now=$(date +%s)
        elapsed=$((now - start_time))
        wait_time=$((delay - elapsed))

        if [[ $wait_time -gt 0 ]]; then
            log INFO "Waiting ${wait_time}s until event '$id'" "$elapsed"
            sleep "$wait_time"
        fi

        now=$(date +%s)
        elapsed=$((now - start_time))
        log EVENT "$id: $desc" "$elapsed"

        if execute_event "$event_type" "$target" "$args"; then
            log PASS "$id completed" "$(($(date +%s) - start_time))"
            echo "EVENT_SUCCESS" >> /tmp/solo-test-results-$$
        else
            log FAIL "$id failed" "$(($(date +%s) - start_time))"
            echo "EVENT_FAIL" >> /tmp/solo-test-results-$$
        fi
    done

    # Count results
    if [[ -f /tmp/solo-test-results-$$ ]]; then
        EVENTS_COMPLETED=$(grep -c "EVENT_SUCCESS" /tmp/solo-test-results-$$ 2>/dev/null) || EVENTS_COMPLETED=0
        EVENTS_FAILED=$(grep -c "EVENT_FAIL" /tmp/solo-test-results-$$ 2>/dev/null) || EVENTS_FAILED=0
        rm -f /tmp/solo-test-results-$$
    fi
}

function run_assertions {
    local assert_count
    assert_count=$(yq '.assertions | length' "$TEST_FILE")
    [[ "$assert_count" == "null" ]] && assert_count=0
    [[ $assert_count -eq 0 ]] && return 0

    log INFO "Running $assert_count assertions"

    local i=0
    while [[ $i -lt $assert_count ]]; do
        local id assert_type target desc args result
        id=$(yq ".assertions[$i].id" "$TEST_FILE")
        assert_type=$(yq ".assertions[$i].type" "$TEST_FILE")
        target=$(yq ".assertions[$i].target // \"\"" "$TEST_FILE")
        desc=$(yq ".assertions[$i].description // \"\"" "$TEST_FILE")
        args=$(yq -o=json ".assertions[$i].args // {}" "$TEST_FILE")

        [[ -z "$desc" || "$desc" == "null" ]] && desc="$assert_type"

        if result=$(run_assertion "$assert_type" "$target" "$args"); then
            log PASS "$id: $desc ($result)"
            ((ASSERTIONS_PASSED++))
        else
            log FAIL "$id: $desc ($result)"
            ((ASSERTIONS_FAILED++))
        fi

        ((i++))
    done
}

function print_summary {
    local event_count
    event_count=$(yq '.events | length' "$TEST_FILE")
    [[ "$event_count" == "null" ]] && event_count=0

    echo ""
    echo "=== Test Summary: ${TEST_NAME} ==="
    echo ""
    echo "Events:     ${EVENTS_COMPLETED}/${event_count} completed"
    [[ $EVENTS_FAILED -gt 0 ]] && echo "            ${EVENTS_FAILED} failed"
    echo "Assertions: ${ASSERTIONS_PASSED}/$((ASSERTIONS_PASSED + ASSERTIONS_FAILED)) passed"
    echo ""

    if [[ $EVENTS_FAILED -eq 0 && $ASSERTIONS_FAILED -eq 0 ]]; then
        echo -e "Result: ${GREEN}PASS${NC}"

        if [[ "${OUTPUT_MODE}" == "github-summary" && -n "${GITHUB_STEP_SUMMARY}" ]]; then
            {
                echo "## Test: ${TEST_NAME}"
                echo ""
                echo "| Metric | Value |"
                echo "|--------|-------|"
                echo "| Events | ${EVENTS_COMPLETED}/${event_count} |"
                echo "| Assertions | ${ASSERTIONS_PASSED}/$((ASSERTIONS_PASSED + ASSERTIONS_FAILED)) |"
                echo "| Result | :white_check_mark: PASS |"
            } >> "${GITHUB_STEP_SUMMARY}"
        fi
        return 0
    else
        echo -e "Result: ${RED}FAIL${NC}"

        if [[ "${OUTPUT_MODE}" == "github-summary" && -n "${GITHUB_STEP_SUMMARY}" ]]; then
            {
                echo "## Test: ${TEST_NAME}"
                echo ""
                echo "| Metric | Value |"
                echo "|--------|-------|"
                echo "| Events | ${EVENTS_COMPLETED}/${event_count} |"
                echo "| Assertions | ${ASSERTIONS_PASSED}/$((ASSERTIONS_PASSED + ASSERTIONS_FAILED)) |"
                echo "| Result | :x: FAIL |"
            } >> "${GITHUB_STEP_SUMMARY}"
        fi
        return 1
    fi
}

function main {
    echo ""
    echo "=== Solo E2E Test: ${TEST_FILE} ==="
    echo ""

    if ! load_test_definition; then
        exit 1
    fi

    local event_count assert_count
    event_count=$(yq '.events | length' "$TEST_FILE")
    assert_count=$(yq '.assertions | length' "$TEST_FILE")
    [[ "$event_count" == "null" ]] && event_count=0
    [[ "$assert_count" == "null" ]] && assert_count=0

    echo "Test Name:    ${TEST_NAME}"
    echo "Description:  ${TEST_DESC}"
    echo "Topology:     ${TOPOLOGY}"
    echo "Events:       ${event_count}"
    echo "Assertions:   ${assert_count}"
    echo ""

    if ! validate_topology; then
        exit 1
    fi

    if [[ "${VALIDATE_ONLY}" == "true" ]]; then
        echo -e "${GREEN}Test definition is valid${NC}"
        exit 0
    fi

    # Check namespace exists (only when running, not validating)
    if ! kctl get ns "${NAMESPACE}" >/dev/null 2>&1; then
        echo "ERROR: Namespace '${NAMESPACE}' not found"
        exit 1
    fi

    echo "=== Executing Events ==="
    echo ""
    run_events

    echo ""
    echo "=== Running Assertions ==="
    echo ""
    run_assertions

    print_summary
}

main
