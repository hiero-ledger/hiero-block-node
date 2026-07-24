#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# WRB Distribution E2E (#3125 slice 5 — step 9, historical half) —
# assert-bn1-historical-backfill-landed.
#
# Runs right after bulk-load-historical-to-bn1.sh. Confirms BlockFileHistoricPlugin
# actually picked up the bulk-loaded WRBs on BN1's post-restart startup scan, by
# querying serverStatus and checking firstAvailableBlock=0 / lastAvailableBlock
# is a real (non-empty-sentinel) number.
#
# Reads:
#   BN1_GRPC_PORT  (default 40840 — matches add-bn.sh's port-forward convention:
#                  grpc_port = 40839 + bn_index, so BN1 -> 40840)
#   PROTO_PATH     (default ${REPO_ROOT}/protobuf-sources/proto — the extracted
#                  proto artifact the "Untar Protobuf Sources" CI step produces)
#
# grpcurl needs -import-path/-proto explicitly: the Block Node's gRPC server
# does not expose reflection, so a plain `-d '{}' host:port service/method`
# call silently returns nothing to parse. Matches the working invocation in
# solo-e2e-test.yml's "Get ServerStatus from Block Node" step.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../../../.." && pwd)"

: "${BN1_GRPC_PORT:=$((40839 + 1))}"
: "${PROTO_PATH:=${REPO_ROOT}/protobuf-sources/proto}"

log() { echo "[wrb-dist-bulk-load-assert] $*"; }
fail() { echo "[wrb-dist-bulk-load-assert] ERROR: $*" >&2; exit 1; }

if ! command -v grpcurl >/dev/null 2>&1; then
    fail "grpcurl not on PATH; cannot query BN1 serverStatus"
fi
if [[ ! -d "${PROTO_PATH}" ]]; then
    fail "PROTO_PATH not found: ${PROTO_PATH} (expected the extracted protobuf artifact)"
fi

grpcurl_err="${TMPDIR:-/tmp}/wrb-dist-bulk-load-assert-grpcurl.err"
status_json=$(grpcurl -plaintext -emit-defaults \
    -import-path "${PROTO_PATH}" \
    -proto block-node/api/node_service.proto \
    -d '{}' "localhost:${BN1_GRPC_PORT}" \
    org.hiero.block.api.BlockNodeService/serverStatus 2>"${grpcurl_err}") || {
        log "grpcurl query failed:"
        sed 's/^/  /' "${grpcurl_err}" || true
        fail "Could not query BN1 serverStatus via grpcurl"
    }

first_available=$(echo "${status_json}" | jq -r '.firstAvailableBlock // empty' 2>/dev/null || echo "")
last_available=$(echo "${status_json}" | jq -r '.lastAvailableBlock // empty' 2>/dev/null || echo "")

# An empty BN reports both fields as UINT64_MAX (18446744073709551615), not 0 —
# 0 means "block 0 is available".
NO_BLOCKS_SENTINEL="18446744073709551615"

log "BN1 serverStatus: firstAvailableBlock=${first_available} lastAvailableBlock=${last_available}"

if [[ -z "${last_available}" || ! "${last_available}" =~ ^[0-9]+$ || "${last_available}" == "${NO_BLOCKS_SENTINEL}" ]]; then
    fail "BN1 still has no blocks after bulk-load + restart (lastAvailableBlock='${last_available}')"
fi
if [[ "${first_available}" != "0" ]]; then
    fail "BN1's firstAvailableBlock is '${first_available}', expected 0 (bulk-loaded from genesis)"
fi

log "OK: historical backfill landed (blocks 0..${last_available} available on block-node-1)."
