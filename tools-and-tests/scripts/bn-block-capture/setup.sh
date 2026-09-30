#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# One-time setup for the bn-block-capture Python tooling:
#   - creates a local .venv,
#   - installs grpcio / protobuf / zstandard,
#   - compiles every proto under ../../protobuf-sources/ to Python.
#
# Re-run if proto files change. Idempotent: safe to call again.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
VENV="${SCRIPT_DIR}/.venv"
PROTO_PY="${SCRIPT_DIR}/proto-py"
API_PROTO_DIR="${REPO_ROOT}/protobuf-sources/src/main/proto"
STREAM_PROTO_DIR="${REPO_ROOT}/protobuf-sources/block-node-protobuf"

if [[ ! -d "${API_PROTO_DIR}" || ! -d "${STREAM_PROTO_DIR}" ]]; then
  echo "ERROR: proto directories missing under ${REPO_ROOT}/protobuf-sources/" >&2
  exit 1
fi
if ! command -v grpcurl &>/dev/null; then
  echo "ERROR: grpcurl not found in PATH (brew install grpcurl)" >&2
  exit 1
fi

echo "==> Creating Python venv at ${VENV}"
python3 -m venv "${VENV}"
"${VENV}/bin/pip" install --quiet --upgrade pip
"${VENV}/bin/pip" install --quiet \
  grpcio==1.62.3 grpcio-tools==1.62.3 protobuf==4.25.3 zstandard==0.22.0

echo "==> Compiling protos to ${PROTO_PY}"
rm -rf "${PROTO_PY}"
mkdir -p "${PROTO_PY}"
find "${API_PROTO_DIR}" "${STREAM_PROTO_DIR}" -name "*.proto" -print0 \
  | xargs -0 "${VENV}/bin/python" -m grpc_tools.protoc \
      -I"${API_PROTO_DIR}" \
      -I"${STREAM_PROTO_DIR}" \
      --python_out="${PROTO_PY}" \
      --grpc_python_out="${PROTO_PY}" 2>&1 \
  | grep -v "^.*: warning:" || true

# Python's stdlib `platform` module shadows the generated `platform/` package.
# Rename it and fix the imports so `from _bn_platform.event import ...` works.
echo "==> Renaming generated platform package -> _bn_platform (stdlib collision)"
if [[ -d "${PROTO_PY}/platform" ]]; then
  mv "${PROTO_PY}/platform" "${PROTO_PY}/_bn_platform"
fi
grep -rl --include="*.py" "from platform\.\|import platform\." "${PROTO_PY}" \
  | xargs sed -i.bak \
      -e 's/from platform\./from _bn_platform./g' \
      -e 's/import platform\./import _bn_platform./g'
find "${PROTO_PY}" -name "*.bak" -delete

echo "==> Smoke test"
"${VENV}/bin/python" -c "
import sys; sys.path.insert(0, '${PROTO_PY}')
from block_node.api import block_access_service_pb2, node_service_pb2
from block.stream import block_pb2
print('Generated protobuf imports OK')
"
echo "==> Done. Invoke profiler.py / download.py / verify.py via .venv/bin/python."
