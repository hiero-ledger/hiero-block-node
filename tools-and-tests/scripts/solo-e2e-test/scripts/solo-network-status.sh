#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Reports network status for all nodes defined in a topology.
#
# Usage:
#   ./solo-network-status.sh [options]
#
# Options:
#   --namespace NAMESPACE      Kubernetes namespace (required)
#   --topology TOPOLOGY        Topology name (required)
#   --topologies-dir DIR       Directory containing topology files (required)
#   --context CONTEXT          kubectl context (default: current context)
#   --output MODE              Output mode: console (default) or github-summary
#   --proto-path PATH          Path to protobuf files (required for grpcurl)
#   --help                     Show this help message
#
# Examples:
#   # Local development (proto sources in repo)
#   ./solo-network-status.sh --namespace solo-network --topology single \
#     --topologies-dir ./topologies --proto-path ./protobuf-sources/src/main/proto
#
#   # CI (proto sources extracted from artifact)
#   ./solo-network-status.sh --namespace solo-network --topology 7cn-3bn-distributed \
#     --topologies-dir ./topologies --proto-path protobuf-sources/proto --output github-summary

set -o pipefail

# is_valid_block_number / NO_BLOCKS_SENTINEL — shared with solo-test-runner.sh so the
# status table and the assertions agree on what "no blocks" looks like.
# shellcheck source=lib/chaos-assertions.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/chaos-assertions.sh"

NAMESPACE=""
TOPOLOGY=""
TOPOLOGIES_DIR=""
CONTEXT=""
OUTPUT_MODE="console"
PROTO_PATH=""

function show_help {
  cat << 'EOF'
Usage: solo-network-status.sh [options]

Reports network status for all nodes defined in a topology.

Options:
  --namespace NAMESPACE      Kubernetes namespace (required)
  --topology TOPOLOGY        Topology name (required)
  --topologies-dir DIR       Directory containing topology files (required)
  --context CONTEXT          kubectl context (default: current context)
  --output MODE              Output mode: console (default) or github-summary
  --proto-path PATH          Path to protobuf files (required for grpcurl)
  --help                     Show this help message

Examples:
  # Local development (proto sources in repo)
  ./solo-network-status.sh --namespace solo-network --topology single \
    --topologies-dir ./topologies --proto-path ./protobuf-sources/src/main/proto

  # CI (proto sources extracted from artifact)
  ./solo-network-status.sh --namespace solo-network --topology 7cn-3bn-distributed \
    --topologies-dir ./topologies --proto-path protobuf-sources/proto --output github-summary
EOF
  exit 0
}

# Parse arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --namespace)
      NAMESPACE="$2"
      shift 2
      ;;
    --topology)
      TOPOLOGY="$2"
      shift 2
      ;;
    --topologies-dir)
      TOPOLOGIES_DIR="$2"
      shift 2
      ;;
    --context)
      CONTEXT="$2"
      shift 2
      ;;
    --output)
      OUTPUT_MODE="$2"
      shift 2
      ;;
    --proto-path)
      PROTO_PATH="$2"
      shift 2
      ;;
    --help|-h)
      show_help
      ;;
    *)
      echo "Unknown option: $1"
      exit 1
      ;;
  esac
done

# Validate required arguments
[[ -z "${NAMESPACE}" ]] && { echo "ERROR: --namespace is required"; exit 1; }
[[ -z "${TOPOLOGY}" ]] && { echo "ERROR: --topology is required"; exit 1; }
[[ -z "${TOPOLOGIES_DIR}" ]] && { echo "ERROR: --topologies-dir is required"; exit 1; }
[[ -z "${PROTO_PATH}" ]] && { echo "ERROR: --proto-path is required"; exit 1; }

TOPOLOGY_FILE="${TOPOLOGIES_DIR}/${TOPOLOGY}.yaml"
[[ ! -f "${TOPOLOGY_FILE}" ]] && { echo "ERROR: Topology file not found: ${TOPOLOGY_FILE}"; exit 1; }
[[ ! -d "${PROTO_PATH}" ]] && { echo "ERROR: Proto path not found: ${PROTO_PATH}"; exit 1; }

# Build kubectl command with optional context
KUBECTL_CMD="kubectl"
[[ -n "${CONTEXT}" ]] && KUBECTL_CMD="kubectl --context ${CONTEXT}"

# Build grpcurl arguments
GRPCURL_ARGS="-plaintext -emit-defaults -import-path ${PROTO_PATH} -proto block-node/api/node_service.proto"

# Output function - writes to console or GITHUB_STEP_SUMMARY
function output_line {
  local line="$1"
  if [[ "${OUTPUT_MODE}" == "github-summary" ]]; then
    echo "$line" >> "${GITHUB_STEP_SUMMARY}"
  else
    echo "$line"
  fi
}

# Start output
output_line "### Network Status"
output_line ""
output_line "| Node | Type | Status | Details |"
output_line "|------|------|--------|---------|"

# Discover block nodes: merge topology-file entries with live pod discovery.
# Block nodes are added dynamically during the test so the deploy-time topology
# often has "block_nodes: {}" — falling back to just block-node-1 misses BN2/BN3.
BLOCK_NODES_TOPO=$(grep -E '^[[:space:]]+block-node-[0-9]+:' "${TOPOLOGY_FILE}" | sed 's/://g' | awk '{print $1}')
BLOCK_NODES_K8S=$(${KUBECTL_CMD} get pods -n "${NAMESPACE}" \
    --no-headers -o custom-columns='NAME:.metadata.name' 2>/dev/null \
    | grep -E '^block-node-[0-9]+-0$' | sed 's/-0$//' | sort || true)
BLOCK_NODES=$(printf '%s\n%s\n' "${BLOCK_NODES_TOPO}" "${BLOCK_NODES_K8S}" \
    | grep -v '^$' | sort -t- -k3 -n | uniq)
[[ -z "${BLOCK_NODES}" ]] && BLOCK_NODES="block-node-1"

# Query each block node
for BN in ${BLOCK_NODES}; do
  # Derive port from the numeric suffix so block-node-2 always maps to 40841
  # regardless of discovery order.
  BN_NUM=$(echo "${BN}" | grep -oE '[0-9]+$')
  PORT=$((40840 + BN_NUM - 1))

  # Check if grpcurl is available
  if command -v grpcurl >/dev/null 2>&1; then
    # shellcheck disable=SC2086  # Intentional word splitting for grpcurl arguments
    STATUS_JSON=$(grpcurl ${GRPCURL_ARGS} \
      -d '{}' \
      "localhost:${PORT}" \
      org.hiero.block.api.BlockNodeService/serverStatus 2>/dev/null || echo '{}')

    FIRST=$(echo "${STATUS_JSON}" | jq -r '.firstAvailableBlock // "N/A"')
    LAST=$(echo "${STATUS_JSON}" | jq -r '.lastAvailableBlock // "N/A"')

    if [[ "${FIRST}" == "N/A" || "${LAST}" == "N/A" ]]; then
      output_line "| ${BN} | Block Node | Unreachable | - |"
    elif ! is_valid_block_number "${FIRST}" || ! is_valid_block_number "${LAST}"; then
      # An empty store reports UINT64_MAX for both fields; printing that as a range
      # reads like a healthy node holding 18 quintillion blocks.
      output_line "| ${BN} | Block Node | Running | No blocks |"
    else
      output_line "| ${BN} | Block Node | Running | Blocks: ${FIRST} - ${LAST} |"
    fi
  else
    output_line "| ${BN} | Block Node | Unknown | grpcurl not available |"
  fi
done

# Check consensus nodes via kubectl.
# Solo labels network-node pods with "network-node" in their name; try the
# component label first and fall back to name-based discovery so this works
# across Solo versions that use different label sets.
CN_PODS=$(${KUBECTL_CMD} get pods -n "${NAMESPACE}" -l 'app.kubernetes.io/component=network-node' \
    -o jsonpath='{.items[*].metadata.name}' 2>/dev/null || true)
if [[ -z "${CN_PODS}" ]]; then
  CN_PODS=$(${KUBECTL_CMD} get pods -n "${NAMESPACE}" --no-headers \
      -o custom-columns='NAME:.metadata.name' 2>/dev/null \
      | grep -E '^network-node[0-9]+-' | sort || true)
fi
for POD in ${CN_PODS}; do
  STATUS=$(${KUBECTL_CMD} get pod "${POD}" -n "${NAMESPACE}" -o jsonpath='{.status.phase}' 2>/dev/null || echo "Unknown")
  if [[ "${STATUS}" == "Running" ]]; then
    output_line "| ${POD} | Consensus Node | Running | - |"
  else
    output_line "| ${POD} | Consensus Node | ${STATUS} | - |"
  fi
done

# Check mirror nodes: merge topology-file entries with live pod discovery.
# MN2 is installed dynamically so it won't be in the deploy-time topology.
MIRROR_NODES_TOPO=""
MIRROR_NODES_SECTION=$(grep -E '^mirror_nodes:' "${TOPOLOGY_FILE}" || true)
if [[ -n "${MIRROR_NODES_SECTION}" ]]; then
  MIRROR_EMPTY=$(grep -E '^mirror_nodes:[[:space:]]*\{\}' "${TOPOLOGY_FILE}" || true)
  if [[ -z "${MIRROR_EMPTY}" ]]; then
    MIRROR_NODES_TOPO=$(grep -E '^[[:space:]]+mirror-[0-9]+:' "${TOPOLOGY_FILE}" | sed 's/://g' | awk '{print $1}')
  fi
fi
MIRROR_NODES_K8S=$(${KUBECTL_CMD} get pods -n "${NAMESPACE}" --no-headers \
    -o custom-columns='NAME:.metadata.name' 2>/dev/null \
    | grep -oE '^mirror-[0-9]+' | sort -u || true)
MIRROR_NODES=$(printf '%s\n%s\n' "${MIRROR_NODES_TOPO}" "${MIRROR_NODES_K8S}" \
    | grep -v '^$' | sort -t- -k2 -n | uniq)

# Mirror REST API ports: mirror-1 on 5551, mirror-2 on 5552
for MN in ${MIRROR_NODES}; do
  MN_NUM=$(echo "${MN}" | grep -oE '[0-9]+$')
  MN_PORT=$((5550 + MN_NUM))
  MN_STATUS=$(${KUBECTL_CMD} get pods -n "${NAMESPACE}" -l "app.kubernetes.io/instance=${MN}" \
      -o jsonpath='{.items[0].status.phase}' 2>/dev/null || echo "Not Found")
  if [[ "${MN_STATUS}" == "Running" ]]; then
    MN_BLOCK=$(curl -s "http://127.0.0.1:${MN_PORT}/api/v1/blocks?limit=1&order=desc" 2>/dev/null \
        | jq -r '.blocks[0].number // "N/A"' 2>/dev/null || echo "N/A")
    output_line "| ${MN} | Mirror Node | Running | Last block: ${MN_BLOCK} |"
  elif [[ "${MN_STATUS}" == "Not Found" || -z "${MN_STATUS}" ]]; then
    output_line "| ${MN} | Mirror Node | Not deployed | - |"
  else
    output_line "| ${MN} | Mirror Node | ${MN_STATUS} | - |"
  fi
done

output_line ""
