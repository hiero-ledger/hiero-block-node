#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Install TCK-SDK version-dependent dependencies only
# Usage: solo-tck-sdk-install.sh --sdk-dir <path> --tck-dir <path> --proto-version <ver>

set -euo pipefail

SDK_DIR=""
TCK_DIR=""
PROTO_VERSION=""  # Will be extracted from source
SDK_VERSION=""    # Will be extracted from source
LONG_VERSION=""   # Will be extracted from source

while [[ $# -gt 0 ]]; do
  case $1 in
    --sdk-dir) SDK_DIR="$2"; shift 2 ;;
    --tck-dir) TCK_DIR="$2"; shift 2 ;;
    --proto-version) PROTO_VERSION="$2"; shift 2 ;; # Optional override
    --sdk-version) SDK_VERSION="$2"; shift 2 ;;     # Optional override
    --long-version) LONG_VERSION="$2"; shift 2 ;;   # Optional override
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

# Extract versions from SDK source if not provided (matches consensus-node approach)
SDK_PKG="${SDK_DIR}/package.json"
PROTO_PKG="${SDK_DIR}/packages/proto/package.json"

if [[ ! -f "$SDK_PKG" ]]; then
  echo "ERROR: SDK package.json not found: $SDK_PKG"
  exit 1
fi

if [[ -z "$SDK_VERSION" ]]; then
  SDK_VERSION=$(node -e "console.log(require('${SDK_PKG}').version)" 2>/dev/null)
fi
if [[ -z "$LONG_VERSION" ]]; then
  LONG_VERSION=$(node -e "console.log(require('${SDK_PKG}').dependencies.long)" 2>/dev/null)
fi
if [[ -z "$PROTO_VERSION" ]]; then
  # Extract proto version from packages/proto/package.json (consensus-node approach)
  if [[ -f "$PROTO_PKG" ]]; then
    PROTO_VERSION=$(node -e "console.log(require('${PROTO_PKG}').version)" 2>/dev/null)
  fi
fi

# Validate all versions were extracted
if [[ -z "$SDK_VERSION" ]]; then
  echo "ERROR: Could not extract SDK version from $SDK_PKG"
  exit 1
fi
if [[ -z "$LONG_VERSION" ]]; then
  echo "ERROR: Could not extract long version from $SDK_PKG"
  exit 1
fi
if [[ -z "$PROTO_VERSION" ]]; then
  echo "ERROR: Could not extract proto version from $PROTO_PKG"
  exit 1
fi

echo "Installing version-dependent dependencies:"
echo "  SDK version: $SDK_VERSION"
echo "  Long version: $LONG_VERSION"
echo "  Proto version: $PROTO_VERSION"

# First, install SDK workspace dependencies from root (builds @hiero-ledger/proto and @hiero-ledger/cryptography)
echo ""
echo "=== Installing SDK workspace dependencies ==="
cd "${SDK_DIR}"
pnpm install

# Then install deps in SDK server's tck directory
# Install both @hashgraph and @hiero-ledger packages (code imports from @hiero-ledger/*)
echo ""
echo "=== Installing SDK server TCK dependencies ==="
cd "${SDK_DIR}/tck"
pnpm add "@hashgraph/sdk@^${SDK_VERSION}" "long@${LONG_VERSION}" "@hashgraph/proto@${PROTO_VERSION}"
# Also install Hiero packages from npm (the tck code imports from @hiero-ledger/*)
pnpm add "@hiero-ledger/proto@${PROTO_VERSION}" "@hiero-ledger/sdk@^${SDK_VERSION}"

# Install TCK client dependencies from source
echo ""
echo "=== Installing TCK client dependencies ==="
cd "${TCK_DIR}"
npm cache clean --force
npm install

echo ""
echo "Dependencies installed successfully"
