#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Generates Helm values overlays from topology YAML files.
# Produces backfill overlays for Block Nodes with peers and
# block node configuration overlays for Mirror Nodes.
#
# Usage:
#   ./generate-chart-values-config-overlays.sh <topology-file> [options]
#
# Arguments:
#   <topology-file>            Path to topology YAML file (required)
#
# Options:
#   --namespace NAMESPACE      Kubernetes namespace for DNS names (default: solo-network)
#   --output-dir DIR           Directory for output files (default: ./out/<topology-name>)
#   --help                     Show this help message
#
# Output Files:
#   bn-<node-id>-values.yaml   Block Node overlay (only for BNs with peers)
#   mn-<node-id>-values.yaml   Mirror Node overlay
#
# Examples:
#   # Generate overlays from a topology file (outputs to ./out/fan-out-3cn-2bn/)
#   ./generate-chart-values-config-overlays.sh topologies/fan-out-3cn-2bn.yaml
#
#   # Specify namespace and output directory
#   ./generate-chart-values-config-overlays.sh topologies/paired-3.yaml --namespace my-ns --output-dir ./overlays
#
#   # Generate and use with Solo CLI
#   ./generate-chart-values-config-overlays.sh topologies/fan-out-3cn-2bn.yaml
#   solo block node add -d my-deploy -f ./out/fan-out-3cn-2bn/bn-block-node-1-values.yaml
#   solo mirror node add -d my-deploy -f ./out/fan-out-3cn-2bn/mn-mirror-1-values.yaml

set -o pipefail
set +e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Default values
TOPOLOGY_FILE=""
NAMESPACE="solo-network"
OUTPUT_DIR=""  # Set after parsing topology file if not specified

function fail {
    printf '%s\n' "$1" >&2
    exit "${2-1}"
}

function log_line {
  local message="${1}"
  shift
  if [[ $# -eq 0 ]]; then
    printf '%s\n' "${message}"
  else
    local formatted
    # shellcheck disable=SC2059
    formatted=$(printf "${message}" "${@}")
    printf '%s\n' "${formatted}"
  fi
}

function show_help {
  cat << 'EOF'
Usage: generate-chart-values-config-overlays.sh <topology-file> [options]

Generates Helm values overlays from topology YAML files.

Arguments:
  <topology-file>            Path to topology YAML file (required)

Options:
  --namespace NAMESPACE      Kubernetes namespace for DNS names (default: solo-network)
  --output-dir DIR           Directory for output files (default: ./out/<topology-name>)
  --help                     Show this help message

Output Files:
  bn-<node-id>-values.yaml   Block Node overlay (only for BNs with peers)
  mn-<node-id>-values.yaml   Mirror Node overlay

Examples:
  # Generate overlays from a topology file (outputs to ./out/fan-out-3cn-2bn/)
  ./generate-chart-values-config-overlays.sh topologies/fan-out-3cn-2bn.yaml

  # Specify namespace and output directory
  ./generate-chart-values-config-overlays.sh topologies/paired-3.yaml --namespace my-ns --output-dir ./overlays

  # Generate and use with Solo CLI
  ./generate-chart-values-config-overlays.sh topologies/fan-out-3cn-2bn.yaml
  solo block node add -d my-deploy -f ./out/fan-out-3cn-2bn/bn-block-node-1-values.yaml
  solo mirror node add -d my-deploy -f ./out/fan-out-3cn-2bn/mn-mirror-1-values.yaml
EOF
  exit 0
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --namespace)
      NAMESPACE="$2"
      shift 2
      ;;
    --output-dir)
      OUTPUT_DIR="$2"
      shift 2
      ;;
    --help|-h)
      show_help
      ;;
    -*)
      fail "Unknown option: $1. Use --help for usage information." 1
      ;;
    *)
      # First positional argument is the topology file
      if [[ -z "${TOPOLOGY_FILE}" ]]; then
        TOPOLOGY_FILE="$1"
        shift
      else
        fail "Unexpected argument: $1. Use --help for usage information." 1
      fi
      ;;
  esac
done

# Validate required arguments
[[ -z "${TOPOLOGY_FILE}" ]] && fail "ERROR: Topology file is required. Use --help for usage." 1

# Handle relative paths - resolve from script directory if file not found
if [[ ! -f "${TOPOLOGY_FILE}" ]]; then
  # Try relative to script directory
  if [[ -f "${SCRIPT_DIR}/${TOPOLOGY_FILE}" ]]; then
    TOPOLOGY_FILE="${SCRIPT_DIR}/${TOPOLOGY_FILE}"
  else
    fail "ERROR: Topology file not found: ${TOPOLOGY_FILE}" 1
  fi
fi

# Extract topology name from filename (for comments in generated files)
TOPOLOGY_NAME=$(basename "${TOPOLOGY_FILE}" .yaml)

# Set default output directory if not specified
if [[ -z "${OUTPUT_DIR}" ]]; then
  OUTPUT_DIR="${SCRIPT_DIR}/out/${TOPOLOGY_NAME}"
fi

# Create output directory if it doesn't exist
mkdir -p "${OUTPUT_DIR}" || fail "ERROR: Cannot create output directory: ${OUTPUT_DIR}" 1

# Generate Kubernetes service DNS name
function get_service_dns {
  local service_name="${1}"
  local namespace="${2}"
  echo "${service_name}.${namespace}.svc.cluster.local"
}

# Generate Block Node overlay for a block node with peers
# Only generates overlay if the block node has peers configured
function generate_bn_overlay {
  local bn_name="${1}"
  local output_file="${2}"

  # Check if this block node has peers (yq returns "null" string if not present)
  local has_peers
  has_peers=$(yq ".block_nodes[\"${bn_name}\"].peers // \"\"" "${TOPOLOGY_FILE}" 2>/dev/null)

  if [[ -z "${has_peers}" ]] || [[ "${has_peers}" == "null" ]]; then
    return 0  # No peers, no overlay needed
  fi

  # Get peer count
  local peer_count
  peer_count=$(yq ".block_nodes[\"${bn_name}\"].peers | length" "${TOPOLOGY_FILE}" 2>/dev/null)

  if [[ "${peer_count}" -eq 0 ]]; then
    return 0  # No peers found
  fi

  # Get peer names - simple extraction (handles string array)
  local peer_names
  peer_names=$(yq -r ".block_nodes[\"${bn_name}\"].peers[]" "${TOPOLOGY_FILE}" 2>/dev/null)

  if [[ -z "${peer_names}" ]]; then
    return 0  # No peers found
  fi

  # Build sources array
  local sources=""
  local priority=1
  while IFS= read -r peer; do
    [[ -z "${peer}" ]] && continue

    local peer_port
    peer_port=$(yq ".block_nodes[\"${peer}\"].port // 40840" "${TOPOLOGY_FILE}")
    local peer_dns
    peer_dns=$(get_service_dns "${peer}" "${NAMESPACE}")

    sources="${sources}
      - address: \"${peer_dns}\"
        port: ${peer_port}
        priority: ${priority}"
    priority=$((priority + 1))
  done <<< "${peer_names}"

  # Read greedy mode from topology (defaults to false if not specified)
  local greedy_mode
  greedy_mode=$(yq ".block_nodes[\"${bn_name}\"].greedy // false" "${TOPOLOGY_FILE}" 2>/dev/null)

  # Write the overlay
  cat > "${output_file}" << EOF
# Generated by generate-chart-values-config-overlays.sh from topology: ${TOPOLOGY_NAME}
# Block Node: ${bn_name}
blockNode:
  config:
    BACKFILL_BLOCK_NODE_SOURCES_PATH: "/opt/hiero/block-node/backfill/block-node-sources.json"
    BACKFILL_GREEDY: "${greedy_mode}"
  backfill:
    path: "/opt/hiero/block-node/backfill"
    filename: "block-node-sources.json"
    sources:${sources}
EOF

  log_line "Generated BN overlay: %s" "${output_file}"
}

# Generate Mirror Node overlay
function generate_mn_overlay {
  local mn_name="${1}"
  local output_file="${2}"

  # Check if mirror node has block_nodes configured
  local has_block_nodes
  has_block_nodes=$(yq ".mirror_nodes[\"${mn_name}\"].block_nodes // \"\"" "${TOPOLOGY_FILE}" 2>/dev/null)

  if [[ -z "${has_block_nodes}" ]] || [[ "${has_block_nodes}" == "null" ]]; then
    return 0  # No block nodes configured
  fi

  # Get block node names - simple extraction (handles string array)
  local block_node_names
  block_node_names=$(yq -r ".mirror_nodes[\"${mn_name}\"].block_nodes[]" "${TOPOLOGY_FILE}" 2>/dev/null)

  if [[ -z "${block_node_names}" ]]; then
    return 0  # No block nodes configured
  fi

  # Build nodes list
  local nodes_config=""
  while IFS= read -r bn; do
    [[ -z "${bn}" ]] && continue

    local bn_port
    bn_port=$(yq ".block_nodes[\"${bn}\"].port // 40840" "${TOPOLOGY_FILE}")
    local bn_dns
    bn_dns=$(get_service_dns "${bn}" "${NAMESPACE}")

    nodes_config="${nodes_config}
              - host: ${bn_dns}
                port: ${bn_port}"
  done <<< "${block_node_names}"

  # Write the overlay
  cat > "${output_file}" << EOF
# Generated by generate-chart-values-config-overlays.sh from topology: ${TOPOLOGY_NAME}
# Mirror Node: ${mn_name}
importer:
  config:
    hiero:
      mirror:
        importer:
          block:
            enabled: true
            nodes:${nodes_config}
            sourceType: BLOCK_NODE
          downloader:
            record:
              enabled: false
          startDate: 1970-01-01T00:00:00Z
          stream:
            maxSubscribeAttempts: 10
            responseTimeout: 10s
EOF

  log_line "Generated MN overlay: %s" "${output_file}"
}

function main {
  log_line "Generating Helm overlays from: %s" "${TOPOLOGY_FILE}"
  log_line "  Namespace: %s" "${NAMESPACE}"
  log_line "  Output dir: %s" "${OUTPUT_DIR}"
  log_line ""

  # Process block nodes - generate overlays for those with peers
  local bn_names
  bn_names=$(yq -r '.block_nodes | keys[]' "${TOPOLOGY_FILE}" 2>/dev/null)

  if [[ -n "${bn_names}" ]]; then
    while IFS= read -r bn_name; do
      [[ -z "${bn_name}" ]] && continue
      generate_bn_overlay "${bn_name}" "${OUTPUT_DIR}/bn-${bn_name}-values.yaml"
    done <<< "${bn_names}"
  fi

  # Process mirror nodes
  local mn_names
  mn_names=$(yq -r '.mirror_nodes | keys[]' "${TOPOLOGY_FILE}" 2>/dev/null)

  if [[ -n "${mn_names}" ]]; then
    while IFS= read -r mn_name; do
      [[ -z "${mn_name}" ]] && continue
      generate_mn_overlay "${mn_name}" "${OUTPUT_DIR}/mn-${mn_name}-values.yaml"
    done <<< "${mn_names}"
  fi

  log_line ""
  log_line "Overlay generation complete."
}

main
