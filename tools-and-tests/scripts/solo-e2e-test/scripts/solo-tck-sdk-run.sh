#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Run TCK-SDK tests
# Usage: solo-tck-sdk-run.sh --sdk-dir <path> --tck-dir <path> --test-file <file> [--deployment <name>]

set -euo pipefail

SDK_DIR=""
TCK_DIR=""
TEST_FILE="src/tests/crypto-service/test-transfer-transaction.ts"
DEPLOYMENT="${DEPLOYMENT:-deployment-solo}"
NAMESPACE="${NAMESPACE:-solo-network}"
SERVER_PID=""

cleanup() {
  if [[ -n "${SERVER_PID:-}" ]]; then
    echo "Stopping SDK server (PID: $SERVER_PID)"
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT

while [[ $# -gt 0 ]]; do
  case $1 in
    --sdk-dir) SDK_DIR="$2"; shift 2 ;;
    --tck-dir) TCK_DIR="$2"; shift 2 ;;
    --test-file) TEST_FILE="$2"; shift 2 ;;
    --deployment) DEPLOYMENT="$2"; shift 2 ;;
    --namespace) NAMESPACE="$2"; shift 2 ;;
    *) shift ;;
  esac
done

# Validate required args
if [[ -z "$SDK_DIR" ]]; then
  echo "ERROR: --sdk-dir is required"
  exit 1
fi
if [[ -z "$TCK_DIR" ]]; then
  echo "ERROR: --tck-dir is required"
  exit 1
fi

# Validate directories exist
if [[ ! -d "$SDK_DIR" ]]; then
  echo "ERROR: SDK directory not found: $SDK_DIR"
  exit 1
fi
if [[ ! -d "$TCK_DIR" ]]; then
  echo "ERROR: TCK directory not found: $TCK_DIR"
  exit 1
fi

# Convert to absolute paths (handles both relative and absolute inputs)
SDK_DIR="$(cd "$SDK_DIR" && pwd)"
TCK_DIR="$(cd "$TCK_DIR" && pwd)"

# Start SDK server in background
echo "Starting SDK server..."
cd "${SDK_DIR}/tck"
pnpm start &
SERVER_PID=$!
echo "SDK server started with PID: $SERVER_PID"

# Wait for server to be ready
echo "Waiting for server to start..."
sleep 5

# Set up environment for TCK client
export OPERATOR_ACCOUNT_ID="0.0.2"
export OPERATOR_ACCOUNT_PRIVATE_KEY="302e020100300506032b65700422042091132178e72057a1d7528025956fe39b0b847f200ab59b2fdd367017f3087137"
export JSON_RPC_SERVER_URL="http://127.0.0.1:8544"
export NODE_IP="127.0.0.1:50211"
export MIRROR_NODE_REST_URL="http://127.0.0.1:5551"
export MIRROR_NODE_REST_JAVA_URL="http://127.0.0.1:8084"

# Create dev account if solo CLI available
if command -v solo &>/dev/null; then
  echo "Creating dev account..."
  solo ledger account create --dev --ed25519-private-key "$OPERATOR_ACCOUNT_PRIVATE_KEY" \
    --deployment "$DEPLOYMENT" --hbar-amount 1000000 || true
else
  echo "Warning: solo CLI not found, skipping dev account creation"
fi

# Run tests
echo ""
echo "Running TCK test: $TEST_FILE"
cd "${TCK_DIR}"
cp .env.custom_node .env
npm run test:file "$TEST_FILE"

echo ""
echo "TCK-SDK test completed successfully"
