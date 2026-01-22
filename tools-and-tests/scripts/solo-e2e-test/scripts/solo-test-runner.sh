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

# ============================================================================
# Event Execution Functions
# ============================================================================
function execute_load_start {
    local test_class="${1:-CryptoTransferLoadTest}"
    local concurrency="${2:-5}"
    local accounts="${3:-10}"
    local duration="${4:-300}"
    local extra_args="${5:-}"

    echo "Starting NLG (async): class=$test_class concurrency=$concurrency accounts=$accounts duration=${duration}s"

    export DEPLOYMENT NAMESPACE
    export NLG_TEST_CLASS="$test_class"
    export NLG_CONCURRENCY="$concurrency"
    export NLG_ACCOUNTS="$accounts"
    export NLG_DURATION="$duration"
    export NLG_EXTRA_ARGS="$extra_args"

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

    export DEPLOYMENT NAMESPACE NLG_TEST_CLASS="$test_class"
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
        *)
            echo "ERROR: Unknown event type: $event_type"
            return 1
            ;;
    esac
}

# ============================================================================
# Assertion Functions
# ============================================================================
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

function assert_block_available {
    local target="${1:-block-node-1}"
    local min_block="${2:-0}"
    local max_block_gte="${3:-0}"
    local port
    port=$(get_bn_grpc_port "$target")

    if [[ -z "${PROTO_PATH}" || ! -d "${PROTO_PATH}" ]]; then
        echo "PROTO_PATH not set or invalid"
        return 1
    fi

    local import_args="-import-path ${PROTO_PATH}"

    local status_json
    status_json=$(grpcurl -plaintext -emit-defaults \
        ${import_args} \
        -proto block-node/api/node_service.proto \
        -d '{}' "localhost:${port}" \
        org.hiero.block.api.BlockNodeService/serverStatus 2>/dev/null)

    local first_block last_block
    first_block=$(echo "$status_json" | jq -r '.firstAvailableBlock // "null"')
    last_block=$(echo "$status_json" | jq -r '.lastAvailableBlock // "null"')

    if [[ "$first_block" == "null" || "$last_block" == "null" ]]; then
        echo "No blocks available"
        return 1
    fi

    if [[ "$first_block" -gt "$min_block" ]]; then
        echo "First block ($first_block) > expected min ($min_block)"
        return 1
    fi

    if [[ "$max_block_gte" -gt 0 && "$last_block" -lt "$max_block_gte" ]]; then
        echo "Last block ($last_block) < expected ($max_block_gte)"
        return 1
    fi

    echo "Blocks: ${first_block}-${last_block}"
}

function assert_node_healthy {
    local target="$1"
    local status
    status=$(kctl get pods -n "${NAMESPACE}" -l "app.kubernetes.io/name=${target}" \
        -o jsonpath='{.items[0].status.phase}' 2>/dev/null)

    if [[ -z "$status" ]]; then
        echo "Pod not found"
        return 1
    fi

    if [[ "$status" != "Running" ]]; then
        echo "Status: $status (expected Running)"
        return 1
    fi

    echo "Running"
}

function assert_no_errors {
    local target="${1:-block-node-1}"
    local port
    port=$(get_bn_metrics_port "$target")
    local metrics
    metrics=$(curl -s "http://localhost:${port}/metrics" 2>/dev/null)

    if [[ -z "$metrics" ]]; then
        echo "Could not fetch metrics"
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
        echo "Errors: verify_failed=$verify_failed verify_errors=$verify_errors stream_errors=$stream_errors"
        return 1
    fi

    echo "0 errors"
}

function assert_blocks_increasing {
    local target="${1:-block-node-1}"
    local wait_seconds="${2:-15}"
    local port
    port=$(get_bn_grpc_port "$target")

    if [[ -z "${PROTO_PATH}" || ! -d "${PROTO_PATH}" ]]; then
        echo "PROTO_PATH not set"
        return 1
    fi

    local import_args="-import-path ${PROTO_PATH}"

    # First check
    local first_json first_block
    first_json=$(grpcurl -plaintext -emit-defaults \
        ${import_args} \
        -proto block-node/api/node_service.proto \
        -d '{}' "localhost:${port}" \
        org.hiero.block.api.BlockNodeService/serverStatus 2>/dev/null)
    first_block=$(echo "$first_json" | jq -r '.lastAvailableBlock // "0"')

    # Wait
    sleep "$wait_seconds"

    # Second check
    local second_json second_block
    second_json=$(grpcurl -plaintext -emit-defaults \
        ${import_args} \
        -proto block-node/api/node_service.proto \
        -d '{}' "localhost:${port}" \
        org.hiero.block.api.BlockNodeService/serverStatus 2>/dev/null)
    second_block=$(echo "$second_json" | jq -r '.lastAvailableBlock // "0"')

    if [[ "$second_block" -le "$first_block" ]]; then
        echo "Blocks not increasing: $first_block -> $second_block (after ${wait_seconds}s)"
        return 1
    fi

    echo "Blocks flowing: $first_block -> $second_block (+$((second_block - first_block)) in ${wait_seconds}s)"
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
            [[ -z "$target" || "$target" == "null" ]] && target=$(echo "$args" | yq '.target // "block-node-1"')
            local wait_seconds
            wait_seconds=$(echo "$args" | yq '.wait_seconds // 15')
            assert_blocks_increasing "$target" "$wait_seconds"
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

    # Extract all targets from test YAML (events and assertions)
    local targets
    targets=$(yq -o=json '.events[].target, .assertions[].target' "$TEST_FILE" 2>/dev/null | \
        jq -r 'select(. != null and . != "")' | sort -u)

    # Check each target exists in topology
    local missing=""
    for target in $targets; do
        # Skip non-node targets like "all" or empty/null
        [[ -z "$target" || "$target" == "null" || "$target" == "all" ]] && continue

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
