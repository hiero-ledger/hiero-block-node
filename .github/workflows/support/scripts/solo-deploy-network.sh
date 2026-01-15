#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Deploys Hiero network components (Block Nodes, Consensus Nodes, Mirror Node)
# using Solo CLI based on a topology configuration.
#
# Usage:
#   ./solo-deploy-network.sh [options]
#
# Options:
#   --deployment DEPLOYMENT    Solo deployment name (required)
#   --namespace NAMESPACE      Kubernetes namespace (required)
#   --cluster-ref REF          Cluster reference (required)
#   --topology TOPOLOGY        Topology name from topologies/*.yaml (default: single)
#   --topologies-dir DIR       Directory containing topology files (default: SCRIPT_DIR/topologies)
#   --cn-version VERSION       Consensus Node version
#   --mn-version VERSION       Mirror Node version
#   --bn-version VERSION       Block Node version (used for Helm chart version)
#   --help                     Show this help message

set -o pipefail
set +e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

function fail {
    printf '%s\n' "$1" >&2
    exit "${2-1}"
}

function log_line {
  local message="${1}"
  shift
  if [[ $# -eq 0 ]]; then
    # No format args, print message as-is (avoids printf interpreting dashes)
    printf '%s\n' "${message}"
  else
    local formatted
    # shellcheck disable=SC2059
    formatted=$(printf "${message}" "${@}")
    printf '%s\n' "${formatted}"
  fi
}

function start_task {
  local message="${1}"
  shift
  if [[ $# -eq 0 ]]; then
    printf '%s .....\t' "${message}"
  else
    local formatted
    # shellcheck disable=SC2059
    formatted=$(printf "${message}" "${@}")
    printf '%s .....\t' "${formatted}"
  fi
}

function end_task {
  printf "%s\n" "${1:-DONE}"
}

function show_help {
  cat << EOF
Usage: $(basename "$0") [options]

Deploys Hiero network components using Solo CLI.

Options:
  --deployment DEPLOYMENT    Solo deployment name (required)
  --namespace NAMESPACE      Kubernetes namespace (required)
  --cluster-ref REF          Cluster reference (required)
  --topology TOPOLOGY        Topology name from topologies/*.yaml (default: single)
  --topologies-dir DIR       Directory containing topology files (default: SCRIPT_DIR/topologies)
  --cn-version VERSION       Consensus Node version
  --mn-version VERSION       Mirror Node version
  --bn-version VERSION       Block Node version (used for Helm chart version)
  --help                     Show this help message

Available Topologies:
EOF
  for f in "${TOPOLOGIES_DIR}"/*.yaml; do
    if [[ -f "$f" ]]; then
      local name desc
      name=$(grep "^name:" "$f" | sed 's/name:[[:space:]]*//')
      desc=$(grep "^description:" "$f" | sed 's/description:[[:space:]]*//' | tr -d '"')
      printf "  %-20s %s\n" "${name}" "${desc}"
    fi
  done
  echo ""
  echo "Example:"
  echo "  $(basename "$0") --deployment my-deploy --namespace my-ns --cluster-ref kind-solo \\"
  echo "                   --topology paired-3 --cn-version v0.68.6"
  exit 0
}

# Required parameters
DEPLOYMENT=""
NAMESPACE=""
CLUSTER_REF=""

# Default values
TOPOLOGY="single"
TOPOLOGIES_DIR="${SCRIPT_DIR}/topologies"
CN_VERSION=""
MN_VERSION=""
BN_VERSION=""

# Values loaded from topology
CN_COUNT="1"
BN_COUNT="1"
SKIP_MIRROR="false"
SKIP_RELAY="false"
SKIP_EXPLORER="false"

# Parse command line arguments
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
    --cluster-ref)
      CLUSTER_REF="$2"
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
    --cn-version)
      CN_VERSION="$2"
      shift 2
      ;;
    --mn-version)
      MN_VERSION="$2"
      shift 2
      ;;
    --bn-version)
      BN_VERSION="$2"
      shift 2
      ;;
    --help|-h)
      show_help
      ;;
    *)
      fail "Unknown option: $1. Use --help for usage information." 1
      ;;
  esac
done

# Validate required parameters
[[ -z "${DEPLOYMENT}" ]] && fail "ERROR: --deployment is required" 1
[[ -z "${NAMESPACE}" ]] && fail "ERROR: --namespace is required" 1
[[ -z "${CLUSTER_REF}" ]] && fail "ERROR: --cluster-ref is required" 1

# Load topology from YAML file (PR #1834 schema format)
function load_topology {
  local topology_name="${1}"
  local topology_file="${TOPOLOGIES_DIR}/${topology_name}.yaml"

  if [[ ! -f "${topology_file}" ]]; then
    fail "Topology not found: ${topology_name}. Use --help to see available topologies." 1
  fi

  log_line "Loading topology: %s" "${topology_name}"

  # Count consensus nodes using yq (PR #1834 schema: consensus_nodes.<id>)
  CN_COUNT=$(yq '.consensus_nodes | keys | length' "${topology_file}" 2>/dev/null || echo "1")

  # Count block nodes using yq (PR #1834 schema: block_nodes.<id>)
  BN_COUNT=$(yq '.block_nodes | keys | length' "${topology_file}" 2>/dev/null || echo "1")

  # Check for new schema (mirror_nodes, relay_nodes, explorer_nodes sections)
  # Fall back to components section for backward compatibility
  local has_mirror_nodes has_relay_nodes has_explorer_nodes
  has_mirror_nodes=$(yq '.mirror_nodes | keys | length // 0' "${topology_file}" 2>/dev/null)
  has_relay_nodes=$(yq '.relay_nodes | keys | length // 0' "${topology_file}" 2>/dev/null)
  has_explorer_nodes=$(yq '.explorer_nodes | keys | length // 0' "${topology_file}" 2>/dev/null)

  SKIP_MIRROR="false"
  SKIP_RELAY="false"
  SKIP_EXPLORER="false"

  # New schema: if section exists with nodes, deploy; if section is empty or missing, skip
  if [[ "${has_mirror_nodes}" -gt 0 ]]; then
    SKIP_MIRROR="false"
  elif [[ "${has_mirror_nodes}" == "0" ]] && yq -e '.mirror_nodes' "${topology_file}" >/dev/null 2>&1; then
    # Section exists but empty - skip
    SKIP_MIRROR="true"
  else
    # Fall back to components section
    local mirror_enabled
    mirror_enabled=$(yq '.components.mirror_node // true' "${topology_file}" 2>/dev/null)
    [[ "${mirror_enabled}" == "false" ]] && SKIP_MIRROR="true"
  fi

  if [[ "${has_relay_nodes}" -gt 0 ]]; then
    SKIP_RELAY="false"
  elif [[ "${has_relay_nodes}" == "0" ]] && yq -e '.relay_nodes' "${topology_file}" >/dev/null 2>&1; then
    SKIP_RELAY="true"
  else
    local relay_enabled
    relay_enabled=$(yq '.components.relay // true' "${topology_file}" 2>/dev/null)
    [[ "${relay_enabled}" == "false" ]] && SKIP_RELAY="true"
  fi

  if [[ "${has_explorer_nodes}" -gt 0 ]]; then
    SKIP_EXPLORER="false"
  elif [[ "${has_explorer_nodes}" == "0" ]] && yq -e '.explorer_nodes' "${topology_file}" >/dev/null 2>&1; then
    SKIP_EXPLORER="true"
  else
    local explorer_enabled
    explorer_enabled=$(yq '.components.explorer // true' "${topology_file}" 2>/dev/null)
    [[ "${explorer_enabled}" == "false" ]] && SKIP_EXPLORER="true"
  fi

  log_line "  Consensus Nodes: %s" "${CN_COUNT}"
  log_line "  Block Nodes:     %s" "${BN_COUNT}"
  log_line "  Mirror Node:     %s" "$([[ "${SKIP_MIRROR}" == "true" ]] && echo "Disabled" || echo "Enabled")"
  log_line "  Relay:           %s" "$([[ "${SKIP_RELAY}" == "true" ]] && echo "Disabled" || echo "Enabled")"
  log_line "  Explorer:        %s" "$([[ "${SKIP_EXPLORER}" == "true" ]] && echo "Disabled" || echo "Enabled")"
}

# Generate node aliases
function generate_node_aliases {
  local count="${1}"
  local aliases=""
  for ((i = 1; i <= count; i++)); do
    if [[ -n "${aliases}" ]]; then
      aliases="${aliases},"
    fi
    aliases="${aliases}node${i}"
  done
  echo "${aliases}"
}

# Create Mirror Node values override file for Block Node integration
function create_mirror_values_override {
  local bn_count="${1}"

  start_task "Creating Mirror Node values override"

  # Build the nodes list based on BN count
  local nodes_config=""
  for ((i = 1; i <= bn_count; i++)); do
    if [[ -n "${nodes_config}" ]]; then
      nodes_config="${nodes_config}
"
    fi
    nodes_config="${nodes_config}                        - host: block-node-${i}.${NAMESPACE}.svc.cluster.local
                          port: 40840"
  done

  cat > mirror-bn-values.yaml << EOF
config:
  hiero:
    mirror:
      importer:
        block:
          enabled: true
          nodes:
${nodes_config}
          sourceType: BLOCK_NODE
        downloader:
          record:
            enabled: false
        startDate: 1970-01-01T00:00:00Z
        stream:
          maxSubscribeAttempts: 10
          responseTimeout: 10s
EOF

  end_task
}

function deploy_block_nodes {
  log_line ""
  log_line "Deploying Block Nodes"
  log_line "---------------------"

  local bn_args=""
  if [[ -n "${BN_VERSION}" ]]; then
    bn_args="--chart-version ${BN_VERSION}"
  fi
  if [[ -n "${CN_VERSION}" ]]; then
    bn_args="${bn_args} --release-tag ${CN_VERSION}"
  fi

  for ((i = 1; i <= BN_COUNT; i++)); do
    start_task "Deploying Block Node ${i}"
    # shellcheck disable=SC2086
    solo block node add \
      --deployment "${DEPLOYMENT}" \
      --cluster-ref "${CLUSTER_REF}" \
      ${bn_args} || fail "ERROR: Failed to deploy Block Node ${i}" 1
    end_task
  done
}

function deploy_consensus_nodes {
  log_line ""
  log_line "Deploying Consensus Nodes"
  log_line "-------------------------"

  local cn_args=""
  if [[ -n "${CN_VERSION}" ]]; then
    cn_args="--release-tag ${CN_VERSION}"
  fi

  start_task "Generating consensus keys for ${NODE_ALIASES}"
  solo keys consensus generate \
    --gossip-keys \
    --tls-keys \
    --deployment "${DEPLOYMENT}" \
    --node-aliases "${NODE_ALIASES}" || fail "ERROR: Failed to generate consensus keys" 1
  end_task

  start_task "Deploying consensus network"
  # shellcheck disable=SC2086
  solo consensus network deploy \
    --deployment "${DEPLOYMENT}" \
    --pvcs true \
    --node-aliases "${NODE_ALIASES}" \
    ${cn_args} || fail "ERROR: Failed to deploy consensus network" 1
  end_task

  start_task "Setting up consensus nodes"
  # shellcheck disable=SC2086
  solo consensus node setup \
    --node-aliases "${NODE_ALIASES}" \
    --deployment "${DEPLOYMENT}" \
    ${cn_args} || fail "ERROR: Failed to setup consensus nodes" 1
  end_task

  start_task "Starting consensus nodes"
  solo consensus node start \
    --deployment "${DEPLOYMENT}" \
    --node-aliases "${NODE_ALIASES}" || fail "ERROR: Failed to start consensus nodes" 1
  end_task
}

function deploy_mirror_node {
  if [[ "${SKIP_MIRROR}" == "true" ]]; then
    log_line ""
    log_line "Skipping Mirror Node deployment (disabled in topology)"
    return 0
  fi

  log_line ""
  log_line "Deploying Mirror Node"
  log_line "---------------------"

  create_mirror_values_override "${BN_COUNT}"

  local mn_args=""
  if [[ -n "${MN_VERSION}" ]]; then
    mn_args="--mirror-node-version ${MN_VERSION}"
  fi

  start_task "Deploying Mirror Node"
  # shellcheck disable=SC2086
  solo mirror node add \
    --deployment "${DEPLOYMENT}" \
    --pinger \
    --cluster-ref "${CLUSTER_REF}" \
    --enable-ingress \
    ${mn_args} \
    -f mirror-bn-values.yaml || fail "ERROR: Failed to deploy Mirror Node" 1
  end_task

  # Clean up the values file
  rm -f mirror-bn-values.yaml
}

function deploy_relay {
  if [[ "${SKIP_RELAY}" == "true" ]]; then
    log_line ""
    log_line "Skipping Relay deployment (disabled in topology)"
    return 0
  fi

  log_line ""
  log_line "Deploying Relay"
  log_line "---------------"

  start_task "Deploying Relay Node"
  solo relay node add \
    --deployment "${DEPLOYMENT}" \
    --node-aliases "${NODE_ALIASES}" \
    --cluster-ref "${CLUSTER_REF}" || fail "ERROR: Failed to deploy Relay" 1
  end_task
}

function deploy_explorer {
  if [[ "${SKIP_EXPLORER}" == "true" ]]; then
    log_line ""
    log_line "Skipping Explorer deployment (disabled in topology)"
    return 0
  fi

  log_line ""
  log_line "Deploying Explorer"
  log_line "------------------"

  start_task "Deploying Explorer Node"
  solo explorer node add \
    --deployment "${DEPLOYMENT}" \
    --cluster-ref "${CLUSTER_REF}" || fail "ERROR: Failed to deploy Explorer" 1
  end_task
}

function wait_for_pods {
  log_line ""
  log_line "Waiting for Pods"
  log_line "----------------"

  start_task "Waiting for network stabilization"
  sleep 10
  end_task

  log_line ""
  log_line "Pod status:"
  kubectl get pods -n "${NAMESPACE}"

  log_line ""
  log_line "Service status:"
  kubectl get svc -n "${NAMESPACE}"
}

function print_summary {
  log_line ""
  log_line "Deployment Summary"
  log_line "=================="
  log_line ""
  log_line "Network Configuration:"
  log_line "  Deployment:      %s" "${DEPLOYMENT}"
  log_line "  Namespace:       %s" "${NAMESPACE}"
  log_line "  Topology:        %s" "${TOPOLOGY}"
  log_line ""
  log_line "Components:"
  log_line "  Consensus Nodes: %s" "${CN_COUNT}"
  log_line "  Block Nodes:     %s" "${BN_COUNT}"
  log_line "  Mirror Node:     %s" "$([[ "${SKIP_MIRROR}" == "true" ]] && echo "Skipped" || echo "Deployed")"
  log_line "  Relay:           %s" "$([[ "${SKIP_RELAY}" == "true" ]] && echo "Skipped" || echo "Deployed")"
  log_line "  Explorer:        %s" "$([[ "${SKIP_EXPLORER}" == "true" ]] && echo "Skipped" || echo "Deployed")"
  log_line ""
  log_line "Versions:"
  log_line "  CN Version:      %s" "${CN_VERSION:-default}"
  log_line "  MN Version:      %s" "${MN_VERSION:-default}"
  log_line "  BN Version:      %s" "${BN_VERSION:-default}"

  # Output key=value pairs to stdout for capture by caller
  echo ""
  echo "cn_count=${CN_COUNT}"
  echo "bn_count=${BN_COUNT}"
  echo "topology=${TOPOLOGY}"
  echo "node_aliases=${NODE_ALIASES}"
}

# Main execution
function main {
  log_line "Solo Network Deployment"
  log_line "======================="
  log_line ""

  # Load topology first
  load_topology "${TOPOLOGY}"

  # Generate node aliases
  NODE_ALIASES=$(generate_node_aliases "${CN_COUNT}")

  log_line ""
  log_line "Configuration:"
  log_line "  Deployment:     %s" "${DEPLOYMENT}"
  log_line "  Namespace:      %s" "${NAMESPACE}"
  log_line "  Cluster Ref:    %s" "${CLUSTER_REF}"
  log_line "  Node Aliases:   %s" "${NODE_ALIASES}"

  # Deploy in order: BN -> CN -> MN -> Relay -> Explorer
  deploy_block_nodes
  deploy_consensus_nodes
  deploy_mirror_node
  deploy_relay
  deploy_explorer

  wait_for_pods
  print_summary

  log_line ""
  log_line "Network deployment complete!"
}

main
