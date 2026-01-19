#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Solo E2E Test - NLG Load Generation Script (Simplified)
#
# Usage: solo-load-generate.sh [start|stop]
#
# Environment variables:
#   DEPLOYMENT       Solo deployment name (required)
#   NAMESPACE        Kubernetes namespace (default: solo-network)
#   NLG_TEST_CLASS   NLG test class (default: CryptoTransferLoadTest)
#   NLG_CONCURRENCY  -c parameter (default: 5)
#   NLG_ACCOUNTS     -a parameter (default: 10)
#   NLG_DURATION     -t parameter in seconds (default: 300)
#   NLG_EXTRA_ARGS   Extra args for test class (e.g., "-T 5 -K ED25519")
#
# Examples:
#   DEPLOYMENT=my-deploy ./solo-load-generate.sh start
#   DEPLOYMENT=my-deploy NLG_DURATION=60 ./solo-load-generate.sh start
#   DEPLOYMENT=my-deploy ./solo-load-generate.sh stop

set -euo pipefail

# Action (start or stop)
ACTION="${1:-start}"

# Required
DEPLOYMENT="${DEPLOYMENT:?DEPLOYMENT environment variable is required}"

# Defaults
NAMESPACE="${NAMESPACE:-solo-network}"
TEST_CLASS="${NLG_TEST_CLASS:-CryptoTransferLoadTest}"

# Direct NLG parameters (no TPS abstraction)
CONCURRENCY="${NLG_CONCURRENCY:-5}"
ACCOUNTS="${NLG_ACCOUNTS:-10}"
DURATION="${NLG_DURATION:-300}"
EXTRA_ARGS="${NLG_EXTRA_ARGS:-}"

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

        # Delete the Helm release if it exists
        if helm status network-load-generator -n "$NAMESPACE" &>/dev/null; then
            echo "  Uninstalling Helm release..."
            helm uninstall network-load-generator -n "$NAMESPACE" 2>/dev/null || true
        fi

        # Force delete any remaining deployments
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

# Validate action
if [[ "$ACTION" != "start" ]]; then
    echo "ERROR: Action must be 'start' or 'stop', got: $ACTION" >&2
    exit 1
fi

# Build args string
ARGS="-c $CONCURRENCY -a $ACCOUNTS -t $DURATION"
[[ -n "$EXTRA_ARGS" ]] && ARGS="$ARGS $EXTRA_ARGS"

echo "Starting NLG load generation..."
echo "  Deployment:  $DEPLOYMENT"
echo "  Test class:  $TEST_CLASS"
echo "  Concurrency: $CONCURRENCY"
echo "  Accounts:    $ACCOUNTS"
echo "  Duration:    ${DURATION}s"
[[ -n "$EXTRA_ARGS" ]] && echo "  Extra args:  $EXTRA_ARGS"
echo "  Full args:   $ARGS"

# Start the load generator
# Note: NLG runs in the background inside the cluster
# Args quoting: Solo requires single quotes outside, double quotes inside
# See: https://github.com/hiero-ledger/solo/blob/main/examples/rapid-fire/Taskfile.yml
solo rapid-fire load start \
    --deployment "$DEPLOYMENT" \
    --test "$TEST_CLASS" \
    --args '"'"$ARGS"'"' \
    --quiet-mode

echo "load_started=true"
