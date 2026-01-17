#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Solo E2E Test - NLG Load Generation Script
#
# Usage: solo-load-generate.sh [options]
#
# Options:
#   --deployment NAME       Solo deployment name (required)
#   --namespace NAMESPACE   Kubernetes namespace (default: solo-network)
#   --action start|stop     Action to perform (default: start)
#   --tps TPS               Target TPS (1-20000, default: 10)
#                           Used for RateLimitedQueue and maps to accounts/concurrency
#   --duration DURATION     Duration (e.g., 5m, 1h, 30s, 300, default: 5m)
#   --test-class CLASS      NLG test class (default: CryptoTransferLoadTest)
#   --help                  Show help
#
# Examples:
#   solo-load-generate.sh --deployment my-deploy --tps 100 --duration 10m
#   solo-load-generate.sh --deployment my-deploy --action stop
#
# This script is used by both the CI workflow and local Taskfile for NLG
# load generation. It auto-calculates accounts and concurrency based on TPS.

set -euo pipefail

# Defaults
ACTION="start"
TPS="10"
DURATION="5m"
TEST_CLASS="CryptoTransferLoadTest"
DEPLOYMENT=""
# Store env var before potentially overriding with --namespace param
NAMESPACE_ENV="${NAMESPACE:-}"
NAMESPACE=""

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --deployment)
            DEPLOYMENT="$2"
            shift 2
            ;;
        --namespace)
            NAMESPACE="$2"
            shift 2
            ;;
        --action)
            ACTION="$2"
            shift 2
            ;;
        --tps)
            TPS="$2"
            shift 2
            ;;
        --duration)
            DURATION="$2"
            shift 2
            ;;
        --test-class)
            TEST_CLASS="$2"
            shift 2
            ;;
        --help)
            head -23 "$0" | tail -19
            exit 0
            ;;
        *)
            echo "ERROR: Unknown option: $1" >&2
            exit 1
            ;;
    esac
done

# Validate required arguments
if [[ -z "$DEPLOYMENT" ]]; then
    echo "ERROR: --deployment is required" >&2
    exit 1
fi

# Validate action
if [[ "$ACTION" != "start" && "$ACTION" != "stop" ]]; then
    echo "ERROR: --action must be 'start' or 'stop', got: $ACTION" >&2
    exit 1
fi

# Handle stop action
if [[ "$ACTION" == "stop" ]]; then
    echo "Stopping NLG load generation..."
    echo "  Deployment: $DEPLOYMENT"
    echo "  Test class: $TEST_CLASS"

    # Try Solo's stop command first
    if solo rapid-fire load stop \
        --deployment "$DEPLOYMENT" \
        --test "$TEST_CLASS" \
        --quiet-mode 2>/dev/null; then
        echo "Solo stop command succeeded"
    else
        echo "Solo stop command failed, cleaning up directly..."
        # Fallback: clean up NLG resources directly
        # This handles cases where the NLG container crashed or is unresponsive
        # Use --namespace param if provided, else NAMESPACE env var, else default
        NAMESPACE="${NAMESPACE:-${NAMESPACE_ENV:-solo-network}}"

        # Delete the Helm release if it exists
        if helm status network-load-generator -n "$NAMESPACE" &>/dev/null; then
            echo "  Uninstalling Helm release..."
            helm uninstall network-load-generator -n "$NAMESPACE" 2>/dev/null || true
        fi

        # Force delete any remaining pods
        if kubectl get deployment network-load-generator -n "$NAMESPACE" &>/dev/null; then
            echo "  Deleting deployment..."
            kubectl delete deployment network-load-generator -n "$NAMESPACE" --grace-period=0 --force 2>/dev/null || true
        fi

        # Clean up any orphaned pods
        kubectl delete pod -n "$NAMESPACE" -l app.kubernetes.io/name=network-load-generator --grace-period=0 --force 2>/dev/null || true

        echo "Direct cleanup completed"
    fi

    echo "load_stopped=true"
    exit 0
fi

# Validate TPS range (1-20000)
if ! [[ "$TPS" =~ ^[0-9]+$ ]] || [[ "$TPS" -lt 1 ]] || [[ "$TPS" -gt 20000 ]]; then
    echo "ERROR: --tps must be between 1 and 20000, got: $TPS" >&2
    exit 1
fi

# Validate duration format (e.g., 5m, 1h, 30s, or plain seconds)
if ! [[ "$DURATION" =~ ^[0-9]+[smh]?$ ]]; then
    echo "ERROR: --duration must be in format like 5m, 1h, 30s, or plain seconds, got: $DURATION" >&2
    exit 1
fi

# Convert duration to seconds
# Accepts: 5m, 1h, 30s, or plain seconds (e.g., 300)
parse_duration() {
    local dur="$1"
    if [[ "$dur" =~ ^([0-9]+)m$ ]]; then
        echo $((${BASH_REMATCH[1]} * 60))
    elif [[ "$dur" =~ ^([0-9]+)h$ ]]; then
        echo $((${BASH_REMATCH[1]} * 3600))
    elif [[ "$dur" =~ ^([0-9]+)s$ ]]; then
        echo "${BASH_REMATCH[1]}"
    elif [[ "$dur" =~ ^([0-9]+)$ ]]; then
        echo "$dur"  # Already seconds
    else
        echo "300"  # Default 5 minutes
    fi
}

DURATION_SECONDS=$(parse_duration "$DURATION")

# Validate test class
VALID_CLASSES=("CryptoTransferLoadTest" "HCSLoadTest" "TokenTransferLoadTest")
VALID=false
for class in "${VALID_CLASSES[@]}"; do
    if [[ "$TEST_CLASS" == "$class" ]]; then
        VALID=true
        break
    fi
done
if [[ "$VALID" == "false" ]]; then
    echo "ERROR: --test-class must be one of: ${VALID_CLASSES[*]}, got: $TEST_CLASS" >&2
    exit 1
fi

# Calculate accounts based on TPS (minimum 10, scale with TPS)
# todo, ask alex how this really works and affects the test.
if [[ "$TPS" -lt 100 ]]; then
    ACCOUNTS=10
else
    ACCOUNTS=$((TPS / 10))
fi

# Calculate concurrency
# todo, ask alex how this really works and affects the test.
if [[ "$TPS" -lt 1000 ]]; then
    CONCURRENCY=5
else
    CONCURRENCY=8
fi

echo "Starting NLG load generation..."
echo "  Deployment:  $DEPLOYMENT"
echo "  Test class:  $TEST_CLASS"
echo "  Target TPS:  ~$TPS (via accounts/concurrency)"
echo "  Duration:    $DURATION ($DURATION_SECONDS seconds)"
echo "  Accounts:    $ACCOUNTS"
echo "  Concurrency: $CONCURRENCY"

# Start the load generator
# Note: NLG runs in the background inside the cluster
# Args quoting: Solo requires single quotes outside, double quotes inside
# See: https://github.com/hiero-ledger/solo/blob/main/examples/rapid-fire/Taskfile.yml
# CryptoTransferLoadTest parameters: -c (concurrency), -a (accounts), -t (seconds)
# TODO: Add -Dbenchmark.maxtps support when NLG chart supports JAVA_OPTS env var
solo rapid-fire load start \
    --deployment "$DEPLOYMENT" \
    --test "$TEST_CLASS" \
    --args '"-c '"${CONCURRENCY}"' -a '"${ACCOUNTS}"' -t '"${DURATION_SECONDS}"'"' \
    --quiet-mode

# Output structured data for callers
echo ""
echo "# Load generation started - structured output:"
echo "load_started=true"
echo "tps=$TPS"
echo "duration=$DURATION"
echo "duration_seconds=$DURATION_SECONDS"
echo "test_class=$TEST_CLASS"
echo "accounts=$ACCOUNTS"
echo "concurrency=$CONCURRENCY"
