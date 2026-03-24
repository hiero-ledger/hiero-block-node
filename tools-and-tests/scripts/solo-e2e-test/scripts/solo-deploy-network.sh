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
#   --relay-version VERSION    Relay version (default: Solo's built-in default)
#   --tss-enabled true|false   Enable TSS on consensus nodes (default: true)
#   --enable-metrics           Enable observability stack (Prometheus+Grafana) on last block node
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
    formatted=$(printf -- "${message}" "${@}")
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
  --relay-version VERSION    Relay version (default: Solo's built-in default)
  --tss-enabled true|false   Enable TSS on consensus nodes (default: true)
  --enable-metrics           Enable observability stack (Prometheus+Grafana) on last block node
  --verbose, -v              Print detailed output including generated overlay file contents
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
RELAY_VERSION=""
TSS_ENABLED="true"
ENABLE_METRICS="false"
VERBOSE="false"

# Values loaded from topology
CN_COUNT="1"
BN_COUNT="1"
SKIP_MIRROR="false"
SKIP_RELAY="false"
SKIP_EXPLORER="false"

# Generated overlay directory (set by deploy_block_nodes, used by deploy_mirror_node)
OVERLAY_DIR=""

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
    --relay-version)
      RELAY_VERSION="$2"
      shift 2
      ;;
    --tss-enabled)
      TSS_ENABLED="$2"
      shift 2
      ;;
    --enable-metrics)
      ENABLE_METRICS="true"
      shift
      ;;
    --verbose|-v)
      VERBOSE="true"
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

# Validate required parameters
[[ -z "${DEPLOYMENT}" ]] && fail "ERROR: --deployment is required" 1
[[ -z "${NAMESPACE}" ]] && fail "ERROR: --namespace is required" 1
[[ -z "${CLUSTER_REF}" ]] && fail "ERROR: --cluster-ref is required" 1

# Load topology from YAML file
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

  # Generate Helm overlays from topology
  local overlay_dir="${SCRIPT_DIR}/../out"
  mkdir -p "${overlay_dir}"
  local generator_script="${TOPOLOGIES_DIR}/../../network-topology-tool/generate-chart-values-config-overlays.sh"
  local topology_file="${TOPOLOGIES_DIR}/${TOPOLOGY}.yaml"

  [[ ! -x "${generator_script}" ]] && fail "ERROR: Generator script not found: ${generator_script}" 1
  [[ ! -f "${topology_file}" ]] && fail "ERROR: Topology file not found: ${topology_file}" 1

  # Clean output directory to avoid stale overlays from previous topology runs
  rm -f "${overlay_dir}"/bn-*.yaml "${overlay_dir}"/mn-*.yaml \
        "${overlay_dir}"/bn-*-priority-mapping.txt 2>/dev/null

  start_task "Generating Helm overlays from topology"
  local generator_output
  generator_output=$("${generator_script}" "${topology_file}" \
    --namespace "${NAMESPACE}" \
    --output-dir "${overlay_dir}" 2>&1)
  local generator_exit_code=$?

  if [[ ${generator_exit_code} -ne 0 ]]; then
    end_task "FAILED"
    log_line ""
    log_line "Overlay generator failed (exit code ${generator_exit_code}):"
    log_line "${generator_output}"
    log_line ""
    log_line "Diagnostics:"
    log_line "  Generator script: %s" "${generator_script}"
    log_line "  Topology file:    %s" "${topology_file}"
    log_line "  Output directory:  %s" "${overlay_dir}"
    log_line "  yq available:     %s" "$(command -v yq >/dev/null 2>&1 && echo "yes ($(yq --version 2>&1))" || echo "NO - yq is required")"
    log_line ""
    log_line "Try running the generator manually:"
    log_line "  %s %s --namespace %s --output-dir %s" "${generator_script}" "${topology_file}" "${NAMESPACE}" "${overlay_dir}"
    fail "ERROR: Failed to generate Helm overlays" 1
  fi
  end_task

  # Count and validate generated overlay files
  local overlay_count
  overlay_count=$(find "${overlay_dir}" -maxdepth 1 \( -name "*.yaml" -o -name "*.json" -o -name "*.txt" \) -type f 2>/dev/null | wc -l | tr -d ' ')

  if [[ "${overlay_count}" -eq 0 ]]; then
    log_line ""
    log_line "ERROR: Overlay generator succeeded but produced 0 files."
    log_line ""
    log_line "Generator output:"
    log_line "${generator_output}"
    log_line ""
    log_line "Diagnostics:"
    log_line "  Generator script: %s" "${generator_script}"
    log_line "  Topology file:    %s" "${topology_file}"
    log_line "  Output directory:  %s" "$(cd "${overlay_dir}" 2>/dev/null && pwd || echo "${overlay_dir} (does not exist)")"
    log_line "  yq version:       %s" "$(yq --version 2>&1 || echo 'NOT FOUND')"
    log_line "  Topology content:"
    sed 's/^/    /' "${topology_file}" 2>/dev/null
    fail "ERROR: No overlay files generated. Check topology file structure and yq installation." 1
  fi

  # Print overlay details
  if [[ "${VERBOSE}" == "true" ]]; then
    log_line ""
    log_line "Generated overlay files:"
    for overlay_file in "${overlay_dir}"/*.yaml "${overlay_dir}"/*.json "${overlay_dir}"/*.txt; do
      if [[ -f "${overlay_file}" ]]; then
        log_line "--- %s ---" "$(basename "${overlay_file}")"
        cat "${overlay_file}"
        log_line "--- End %s ---" "$(basename "${overlay_file}")"
        log_line ""
      fi
    done
  else
    log_line "  Generated ${overlay_count} overlay files (use --verbose to see contents)"
  fi

  # TODO: Solo 0.63.0 has a built-in TSS overlay (block-node-tss-values.yaml) that provides
  # identical resources, but it only activates when tssEnabled is in the remote config. Since BNs
  # deploy before CNs (which set the TSS flag via --tss), Solo's overlay doesn't apply in time.
  # This mini overlay remains necessary until Solo supports setting TSS state before BN deployment.
  local resource_overlay="${SCRIPT_DIR}/../../../../charts/block-node-server/values-overrides/mini.yaml"

  # Path to observability overlay (relative to scripts dir)
  local observability_overlay="${SCRIPT_DIR}/../../../../charts/block-node-server/values-overrides/enable-observability.yaml"

  for ((i = 1; i <= BN_COUNT; i++)); do
    local bn_overlay="${overlay_dir}/bn-block-node-${i}-values.yaml"
    local overlay_args=""

    # Apply mini resource overlay to all block nodes
    if [[ -f "${resource_overlay}" ]]; then
      overlay_args="-f ${resource_overlay}"
      log_line "  Using resource overlay (mini) for block-node-${i}"
    else
      log_line "  WARNING: Resource overlay not found: ${resource_overlay}"
    fi

    if [[ -f "${bn_overlay}" ]]; then
      overlay_args="${overlay_args} -f ${bn_overlay}"
      log_line "  Using backfill overlay for block-node-${i}"
    fi

    # Read priority mapping for this BN (BN-centric CN routing config)
    local priority_mapping_file="${overlay_dir}/bn-block-node-${i}-priority-mapping.txt"
    local priority_mapping_args=""
    if [[ -f "${priority_mapping_file}" ]]; then
      local priority_mapping
      priority_mapping=$(cat "${priority_mapping_file}")
      if [[ -n "${priority_mapping}" ]]; then
        priority_mapping_args="--priority-mapping ${priority_mapping}"
        log_line "  Priority mapping for block-node-${i}: %s" "${priority_mapping}"
      fi
    fi

    # Enable observability stack only on the last block node if enabled.
    if [[ "${ENABLE_METRICS}" == "true" && "${i}" -eq "${BN_COUNT}" ]]; then
      if [[ -f "${observability_overlay}" ]]; then
        overlay_args="${overlay_args} -f ${observability_overlay}"
        log_line "  Enabling observability stack on block-node-${i}"
        log_line "  --- Observability overlay contents ---"
        cat "${observability_overlay}"
        log_line "  --- End observability overlay ---"
      else
        log_line "  WARNING: Observability overlay not found: ${observability_overlay}"
      fi
    fi

    start_task "Deploying Block Node ${i}"
    # shellcheck disable=SC2086
    solo block node add \
      --deployment "${DEPLOYMENT}" \
      --cluster-ref "${CLUSTER_REF}" \
      ${bn_args} \
      ${overlay_args} \
      ${priority_mapping_args} || fail "ERROR: Failed to deploy Block Node ${i}" 1
    end_task
  done

  # Store overlay directory for mirror node deployment
  OVERLAY_DIR="${overlay_dir}"
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
  local wraps_arg=""
  if [[ "${TSS_ENABLED}" == "true" ]]; then
    wraps_arg="--wraps true"
  fi

  # shellcheck disable=SC2086
  # adding --dev flag so in case it fails we have more information on details
  eval solo consensus network deploy \
    --deployment "${DEPLOYMENT}" \
    --pvcs true \
    --tss "${TSS_ENABLED}" \
    ${wraps_arg} \
    --node-aliases "${NODE_ALIASES}" \
    ${cn_args} --dev || fail "ERROR: Failed to deploy consensus network" 1
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

  local mn_overlay="${OVERLAY_DIR}/mn-mirror-1-values.yaml"
  [[ ! -f "${mn_overlay}" ]] && fail "ERROR: Mirror Node overlay not found: ${mn_overlay}" 1
  log_line "  Using generated overlay: %s" "${mn_overlay}"
  log_line "  --- Mirror Node overlay contents ---"
  cat "${mn_overlay}"
  log_line "  --- End Mirror Node overlay ---"

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
    -f "${mn_overlay}" || fail "ERROR: Failed to deploy Mirror Node" 1
  end_task
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

  local relay_args=""
  if [[ -n "${RELAY_VERSION}" && "${RELAY_VERSION}" != "latest" ]]; then
    relay_args="--relay-release ${RELAY_VERSION}"
  fi

  start_task "Deploying Relay Node"
  # shellcheck disable=SC2086
  solo relay node add \
    --deployment "${DEPLOYMENT}" \
    --node-aliases "${NODE_ALIASES}" \
    --cluster-ref "${CLUSTER_REF}" \
    ${relay_args} || fail "ERROR: Failed to deploy Relay" 1
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
  log_line "  Solo CLI:        %s" "$(solo --version 2>/dev/null | grep 'Version' | awk -F': ' '{print $2}' || echo 'unknown')"
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

readonly SOLO_MIN_VERSION="0.63.0"

function check_prerequisites {
  if ! command -v yq &> /dev/null; then
    fail "ERROR: yq not found. Install from: https://github.com/mikefarah/yq" 1
  fi
  if ! command -v kubectl &> /dev/null; then
    fail "ERROR: kubectl not found. Please install kubectl." 1
  fi
  if ! command -v solo &> /dev/null; then
    fail "ERROR: solo CLI not found. Install with: npm i @hashgraph/solo -g" 1
  fi

  # Enforce minimum Solo version (required for TSS support)
  local solo_version
  solo_version=$(solo --version 2>&1 | grep 'Version' | sed 's/.*: *//' | tr -d '[:space:]')
  if [[ -n "${solo_version}" ]]; then
    # Simple semver comparison: split into major.minor.patch and compare numerically
    IFS='.' read -r cur_major cur_minor cur_patch <<< "${solo_version}"
    IFS='.' read -r min_major min_minor min_patch <<< "${SOLO_MIN_VERSION}"
    local is_outdated="false"
    if (( cur_major < min_major )); then is_outdated="true"
    elif (( cur_major == min_major && cur_minor < min_minor )); then is_outdated="true"
    elif (( cur_major == min_major && cur_minor == min_minor && cur_patch < min_patch )); then is_outdated="true"
    fi
    if [[ "${is_outdated}" == "true" ]]; then
      fail "ERROR: Solo CLI version ${solo_version} is outdated. Minimum supported version is ${SOLO_MIN_VERSION} (required for TSS support). Upgrade with: npm i @hashgraph/solo@${SOLO_MIN_VERSION} -g" 1
    fi
  fi
}

# Main execution
function main {
  log_line "Solo Network Deployment"
  log_line "======================="
  log_line ""

  check_prerequisites

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
