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

# Extract block node names from topology file
# Use grep to find lines like "  block-node-1:" under block_nodes section
BLOCK_NODES=$(grep -E '^[[:space:]]+block-node-[0-9]+:' "${TOPOLOGY_FILE}" | sed 's/://g' | awk '{print $1}' || true)

# Query each block node
BN_INDEX=0
for BN in ${BLOCK_NODES}; do
  PORT=$((40840 + BN_INDEX))
  BN_INDEX=$((BN_INDEX + 1))

  # Check if grpcurl is available
  if command -v grpcurl >/dev/null 2>&1; then
    STATUS_JSON=$(grpcurl ${GRPCURL_ARGS} \
      -d '{}' \
      "localhost:${PORT}" \
      org.hiero.block.api.BlockNodeService/serverStatus 2>/dev/null || echo '{}')

    FIRST=$(echo "${STATUS_JSON}" | jq -r '.firstAvailableBlock // "N/A"')
    LAST=$(echo "${STATUS_JSON}" | jq -r '.lastAvailableBlock // "N/A"')

    if [[ "${FIRST}" != "N/A" && "${LAST}" != "N/A" ]]; then
      output_line "| ${BN} | Block Node | Running | Blocks: ${FIRST} - ${LAST} |"
    else
      output_line "| ${BN} | Block Node | Unreachable | - |"
    fi
  else
    output_line "| ${BN} | Block Node | Unknown | grpcurl not available |"
  fi
done

# Check consensus nodes via kubectl
CN_PODS=$(${KUBECTL_CMD} get pods -n "${NAMESPACE}" -l 'app.kubernetes.io/component=network-node' -o jsonpath='{.items[*].metadata.name}' 2>/dev/null || echo "")
for POD in ${CN_PODS}; do
  STATUS=$(${KUBECTL_CMD} get pod "${POD}" -n "${NAMESPACE}" -o jsonpath='{.status.phase}' 2>/dev/null || echo "Unknown")
  if [[ "${STATUS}" == "Running" ]]; then
    output_line "| ${POD} | Consensus Node | Running | - |"
  else
    output_line "| ${POD} | Consensus Node | ${STATUS} | - |"
  fi
done

# Check mirror node - handle topologies without mirror nodes
MIRROR_NODES_SECTION=$(grep -E '^mirror_nodes:' "${TOPOLOGY_FILE}" || true)
if [[ -n "${MIRROR_NODES_SECTION}" ]]; then
  # Check if mirror_nodes section is empty (like "mirror_nodes: {}")
  MIRROR_EMPTY=$(grep -E '^mirror_nodes:[[:space:]]*\{\}' "${TOPOLOGY_FILE}" || true)
  if [[ -z "${MIRROR_EMPTY}" ]]; then
    MN_STATUS=$(${KUBECTL_CMD} get pods -n "${NAMESPACE}" -l 'app.kubernetes.io/instance=mirror-1' -o jsonpath='{.items[0].status.phase}' 2>/dev/null || echo "Not Found")
    if [[ "${MN_STATUS}" == "Running" ]]; then
      # Try to get block info from mirror
      MN_BLOCK=$(curl -s "http://127.0.0.1:5551/api/v1/blocks?limit=1&order=desc" 2>/dev/null | jq -r '.blocks[0].number // "N/A"' 2>/dev/null || echo "N/A")
      output_line "| mirror-1 | Mirror Node | Running | Last block: ${MN_BLOCK} |"
    elif [[ "${MN_STATUS}" == "Not Found" ]]; then
      output_line "| mirror-1 | Mirror Node | Not deployed | - |"
    else
      output_line "| mirror-1 | Mirror Node | ${MN_STATUS} | - |"
    fi
  else
    output_line "| mirror-1 | Mirror Node | Not in topology | - |"
  fi
fi

output_line ""
