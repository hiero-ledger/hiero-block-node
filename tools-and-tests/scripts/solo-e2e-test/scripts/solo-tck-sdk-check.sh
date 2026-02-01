#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Check if TCK-SDK dependencies are installed at correct versions
# Usage: solo-tck-sdk-check.sh --sdk-dir <path> [--ci]
# Exit 0 = deps ready, Exit 1 = need install

set -euo pipefail

SDK_DIR=""
CI_MODE=false

while [[ $# -gt 0 ]]; do
  case $1 in
    --sdk-dir) SDK_DIR="$2"; shift 2 ;;
    --ci) CI_MODE=true; shift ;;
    *) shift ;;
  esac
done

# Validate required args
if [[ -z "$SDK_DIR" ]]; then
  echo "ERROR: --sdk-dir is required"
  exit 1
fi

# In CI mode, always need fresh install
if [[ "$CI_MODE" == "true" ]]; then
  echo "CI mode: skipping check, will install"
  exit 1
fi

# Check if SDK directory exists
if [[ ! -d "$SDK_DIR" ]]; then
  echo "SDK directory not found: $SDK_DIR"
  exit 1
fi

# Convert to absolute path (handles both relative and absolute inputs)
SDK_DIR="$(cd "$SDK_DIR" && pwd)"

# Check if node_modules exists in tck subdirectory
TCK_NODE_MODULES="${SDK_DIR}/tck/node_modules"
if [[ ! -d "$TCK_NODE_MODULES" ]]; then
  echo "node_modules not found at ${TCK_NODE_MODULES}"
  exit 1
fi

# Get expected proto version from SDK source (consensus-node approach)
PROTO_PKG="${SDK_DIR}/packages/proto/package.json"
if [[ ! -f "$PROTO_PKG" ]]; then
  echo "Proto package.json not found at ${PROTO_PKG}"
  exit 1
fi
EXPECTED_PROTO=$(node -e "console.log(require('${PROTO_PKG}').version)" 2>/dev/null || echo "")

# Check installed proto version
INSTALLED_PROTO=$(node -e "try{console.log(require('${TCK_NODE_MODULES}/@hiero-ledger/proto/package.json').version)}catch(e){console.log('')}" 2>/dev/null || echo "")
if [[ -z "$INSTALLED_PROTO" ]]; then
  echo "Proto package not installed"
  exit 1
fi

if [[ "$INSTALLED_PROTO" != "$EXPECTED_PROTO" ]]; then
  echo "Proto version mismatch: installed=$INSTALLED_PROTO, expected=$EXPECTED_PROTO"
  exit 1
fi

echo "Dependencies are up-to-date (proto version: $INSTALLED_PROTO)"
exit 0
