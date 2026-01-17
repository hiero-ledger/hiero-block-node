#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Sets up a Kind cluster and initializes Solo for Hiero network deployment.
#
# Usage:
#   ./solo-setup-cluster.sh [options]
#
# Options:
#   --cluster-name NAME      Kind cluster name (default: solo-cluster)
#   --namespace NAMESPACE    Kubernetes namespace (default: solo-network)
#   --deployment DEPLOYMENT  Solo deployment name (default: deployment-solo)
#   --topology TOPOLOGY      Topology name from topologies/*.yaml (default: single)
#   --topologies-dir DIR     Directory containing topology files (default: SCRIPT_DIR/topologies)
#   --skip-kind              Skip Kind cluster creation (use existing cluster)
#   --help                   Show this help message
#
# Environment Variables:
#   CLUSTER_NAME    - Alternative to --cluster-name
#   NAMESPACE       - Alternative to --namespace
#   DEPLOYMENT      - Alternative to --deployment
#   TOPOLOGY        - Alternative to --topology
#   TOPOLOGIES_DIR  - Alternative to --topologies-dir

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

Sets up a Kind cluster and initializes Solo for Hiero network deployment.

Options:
  --cluster-name NAME      Kind cluster name (default: solo-cluster)
  --namespace NAMESPACE    Kubernetes namespace (default: solo-network)
  --deployment DEPLOYMENT  Solo deployment name (default: deployment-solo)
  --topology TOPOLOGY      Topology name from topologies/*.yaml (default: single)
  --topologies-dir DIR     Directory containing topology files (default: SCRIPT_DIR/topologies)
  --skip-kind              Skip Kind cluster creation (use existing cluster)
  --help                   Show this help message

Environment Variables:
  CLUSTER_NAME    - Alternative to --cluster-name
  NAMESPACE       - Alternative to --namespace
  DEPLOYMENT      - Alternative to --deployment
  TOPOLOGY        - Alternative to --topology
  TOPOLOGIES_DIR  - Alternative to --topologies-dir

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
  echo "  $(basename "$0") --cluster-name my-cluster --namespace my-ns --topology paired-3"
  exit 0
}

# Default values (can be overridden by env vars or args)
CLUSTER_NAME="${CLUSTER_NAME:-solo-cluster}"
NAMESPACE="${NAMESPACE:-solo-network}"
DEPLOYMENT="${DEPLOYMENT:-deployment-solo}"
TOPOLOGY="${TOPOLOGY:-single}"
TOPOLOGIES_DIR="${TOPOLOGIES_DIR:-${SCRIPT_DIR}/topologies}"
SKIP_KIND="false"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --cluster-name)
      CLUSTER_NAME="$2"
      shift 2
      ;;
    --namespace)
      NAMESPACE="$2"
      shift 2
      ;;
    --deployment)
      DEPLOYMENT="$2"
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
    --skip-kind)
      SKIP_KIND="true"
      shift
      ;;
    --help|-h)
      show_help
      ;;
    *)
      fail "Unknown option: $1. Use --help for usage information." 1
      ;;
  esac
done

# Load topology from YAML file
function load_topology {
  local topology_name="${1}"
  local topology_file="${TOPOLOGIES_DIR}/${topology_name}.yaml"

  if [[ ! -f "${topology_file}" ]]; then
    fail "Topology not found: ${topology_name}. Use --help to see available topologies." 1
  fi

  log_line "Loading topology: %s" "${topology_name}"

  # Count consensus nodes using yq (PR #1834 schema format)
  NODE_COUNT=$(yq '.consensus_nodes | keys | length' "${topology_file}" 2>/dev/null || echo "1")

  log_line "  Consensus Nodes: %s" "${NODE_COUNT}"
}

# Generate node aliases (node1, node2, ...)
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

function check_prerequisites {
  log_line ""
  log_line "Checking Prerequisites"
  log_line "----------------------"

  start_task "Checking for kubectl"
  if ! command -v kubectl &> /dev/null; then
    fail "ERROR: kubectl not found. Please install kubectl." 1
  fi
  end_task "FOUND ($(command -v kubectl))"

  start_task "Checking for helm"
  if ! command -v helm &> /dev/null; then
    fail "ERROR: helm not found. Please install helm." 1
  fi
  end_task "FOUND ($(command -v helm))"

  start_task "Checking for solo"
  if ! command -v solo &> /dev/null; then
    fail "ERROR: solo CLI not found. Install with: npm i @hashgraph/solo -g" 1
  fi
  end_task "FOUND ($(command -v solo))"

  if [[ "${SKIP_KIND}" != "true" ]]; then
    start_task "Checking for kind"
    if ! command -v kind &> /dev/null; then
      fail "ERROR: kind not found. Please install kind." 1
    fi
    end_task "FOUND ($(command -v kind))"
  fi
}

function create_kind_cluster {
  if [[ "${SKIP_KIND}" == "true" ]]; then
    log_line ""
    log_line "Skipping Kind Cluster Creation"
    log_line "-------------------------------"
    log_line "Using existing Kubernetes context"
    return 0
  fi

  log_line ""
  log_line "Creating Kind Cluster"
  log_line "---------------------"

  # Check if cluster already exists
  start_task "Checking for existing cluster '${CLUSTER_NAME}'"
  if kind get clusters 2>/dev/null | grep -q "^${CLUSTER_NAME}$"; then
    end_task "EXISTS (will reuse)"
    log_line "Cluster already exists, skipping creation"
  else
    end_task "NOT FOUND"

    start_task "Creating Kind cluster '${CLUSTER_NAME}'"
    kind create cluster -n "${CLUSTER_NAME}" || fail "ERROR: Failed to create Kind cluster" 1
    end_task
  fi

  start_task "Waiting for cluster to be ready"
  sleep 10
  end_task

  start_task "Setting kubectl context to '${CONTEXT}'"
  kubectl config use-context "${CONTEXT}" || fail "ERROR: Failed to set kubectl context" 1
  end_task
}

function initialize_solo {
  log_line ""
  log_line "Initializing Solo"
  log_line "-----------------"

  start_task "Running solo init"
  solo init || fail "ERROR: solo init failed" 1
  end_task

  start_task "Connecting cluster reference '${CLUSTER_REF}'"
  solo cluster-ref config connect \
    --cluster-ref "${CLUSTER_REF}" \
    --context "${CONTEXT}" || fail "ERROR: solo cluster-ref config connect failed" 1
  end_task

  start_task "Creating deployment '${DEPLOYMENT}' in namespace '${NAMESPACE}'"
  solo deployment config create \
    --deployment "${DEPLOYMENT}" \
    --namespace "${NAMESPACE}" || fail "ERROR: solo deployment config create failed" 1
  end_task

  start_task "Attaching cluster to deployment with ${NODE_COUNT} consensus node(s)"
  solo deployment cluster attach \
    --deployment "${DEPLOYMENT}" \
    --cluster-ref "${CLUSTER_REF}" \
    --num-consensus-nodes "${NODE_COUNT}" || fail "ERROR: solo deployment cluster attach failed" 1
  end_task

  start_task "Setting up cluster reference"
  solo cluster-ref config setup -s "${CLUSTER_REF}" || fail "ERROR: solo cluster-ref config setup failed" 1
  end_task
}

function print_summary {
  log_line ""
  log_line "Cluster Setup Summary"
  log_line "---------------------"
  log_line "  Cluster Name:    %s" "${CLUSTER_NAME}"
  log_line "  Context:         %s" "${CONTEXT}"
  log_line "  Cluster Ref:     %s" "${CLUSTER_REF}"
  log_line "  Namespace:       %s" "${NAMESPACE}"
  log_line "  Deployment:      %s" "${DEPLOYMENT}"
  log_line "  Topology:        %s" "${TOPOLOGY}"
  log_line "  Node Count:      %s" "${NODE_COUNT}"
  log_line "  Node Aliases:    %s" "${NODE_ALIASES}"

  # Output key=value pairs to stdout for capture by caller
  echo ""
  echo "cluster_name=${CLUSTER_NAME}"
  echo "context=${CONTEXT}"
  echo "cluster_ref=${CLUSTER_REF}"
  echo "namespace=${NAMESPACE}"
  echo "deployment=${DEPLOYMENT}"
  echo "topology=${TOPOLOGY}"
  echo "node_count=${NODE_COUNT}"
  echo "node_aliases=${NODE_ALIASES}"
}

# Main execution
function main {
  log_line "Solo Cluster Setup"
  log_line "=================="

  # Load topology first to get node count
  load_topology "${TOPOLOGY}"

  # Derived values
  CONTEXT="kind-${CLUSTER_NAME}"
  CLUSTER_REF="kind-${CLUSTER_NAME}"
  NODE_ALIASES=$(generate_node_aliases "${NODE_COUNT}")

  check_prerequisites
  create_kind_cluster
  initialize_solo
  print_summary

  log_line ""
  log_line "Cluster setup complete! Ready for network deployment."
}

main
