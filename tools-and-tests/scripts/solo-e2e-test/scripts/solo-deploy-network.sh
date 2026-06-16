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

# Block verification mode (loaded from topology). "rsa-wrb" deploys Wrapped Record
# Blocks verified against the RSA roster (TSS disabled); anything else is TSS.
VERIFICATION_MODE="tss"
WRB_RSA="false"

# Full Block Node plugin list with roster-bootstrap-rsa appended (rsa-wrb mode only).
# Mirrors charts/block-node-server/values.yaml plugins.names plus roster-bootstrap-rsa.
readonly WRB_RSA_PLUGIN_NAMES="facility-messaging,block-access-service,health,server-status,stream-publisher,stream-subscriber,verification,blocks-file-historic,blocks-file-recent,backfill,roster-bootstrap-rsa"

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

  # Read verification mode. "rsa-wrb" switches the deployment to Wrapped Record
  # Blocks verified against the RSA roster, which requires TSS to be disabled.
  VERIFICATION_MODE=$(yq '.verification_mode // "tss"' "${topology_file}" 2>/dev/null)
  if [[ "${VERIFICATION_MODE}" == "rsa-wrb" ]]; then
    WRB_RSA="true"
    TSS_ENABLED="false"
  fi

  log_line "  Consensus Nodes: %s" "${CN_COUNT}"
  log_line "  Block Nodes:     %s" "${BN_COUNT}"
  log_line "  Mirror Node:     %s" "$([[ "${SKIP_MIRROR}" == "true" ]] && echo "Disabled" || echo "Enabled")"
  log_line "  Relay:           %s" "$([[ "${SKIP_RELAY}" == "true" ]] && echo "Disabled" || echo "Enabled")"
  log_line "  Explorer:        %s" "$([[ "${SKIP_EXPLORER}" == "true" ]] && echo "Disabled" || echo "Enabled")"
  log_line "  Verification:    %s" "${VERIFICATION_MODE}"
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

# Generate all Helm overlays (BN backfill, MN config, priority mappings) from the
# topology. Runs before any component is deployed so the overlays are available
# regardless of deployment order (rsa-wrb deploys the Mirror Node before Block Nodes).
function generate_overlays {
  log_line ""
  log_line "Generating Helm Overlays"
  log_line "------------------------"

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
    --verification-mode "${VERIFICATION_MODE}" \
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

  # Store overlay directory for block node and mirror node deployment
  OVERLAY_DIR="${overlay_dir}"
}

# Generate the shared Block Node WRB overlay for rsa-wrb mode. Enables the
# roster-bootstrap-rsa plugin and points it at the in-cluster Mirror Node REST
# service. The Mirror Node must already be deployed so its service is discoverable.
function generate_bn_wrb_overlay {
  local output_file="${1}"

  # Block Nodes deploy before the Mirror Node (so the Consensus Node gets the BNs
  # wired as stream sources), so the MN REST service usually does not exist yet.
  # The /api/v1/network/nodes endpoint the roster plugin needs is served by the
  # rest-java service (mirror-<n>-restjava), NOT the JS rest service (which 404s).
  # Solo names it mirror-<n>-restjava (n starts at 1 on a fresh deployment); try to
  # discover a live service first, otherwise fall back to the first-instance name.
  # The roster-bootstrap-rsa plugin polls this URL indefinitely until the Mirror
  # Node is reachable, so the Block Node does not need the MN to be up at startup.
  local mn_rest_svc
  mn_rest_svc=$(kubectl get svc -n "${NAMESPACE}" -o name 2>/dev/null | grep -E "mirror-.*-restjava$" | head -1 | sed 's#^service/##')
  [[ -z "${mn_rest_svc}" ]] && mn_rest_svc="mirror-1-restjava"

  local mn_base_url="http://${mn_rest_svc}.${NAMESPACE}.svc.cluster.local"
  cat > "${output_file}" << EOF
# Generated by solo-deploy-network.sh for rsa-wrb verification mode.
# Enables the roster-bootstrap-rsa plugin and points it at the in-cluster Mirror Node.
plugins:
  names: "${WRB_RSA_PLUGIN_NAMES}"
blockNode:
  config:
    ROSTER_BOOTSTRAP_RSA_MIRROR_NODE_BASE_URL: "${mn_base_url}"
    # Poll the Mirror Node frequently so the roster loads ASAP after the MN is up — this
    # narrows the startup window in which the genesis block (0) can arrive before the roster
    # and be dropped. See wrb-startup-block-gap-issue: the real fix is no-gap-from-genesis on the BN.
    ROSTER_BOOTSTRAP_RSA_INITIAL_QUERY_INTERVAL_MILLIS: "3000"
EOF
  log_line "  Generated BN WRB overlay (roster URL: %s)" "${mn_base_url}"
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

  local overlay_dir="${OVERLAY_DIR}"

  # In rsa-wrb mode generate the shared WRB overlay (enables roster-bootstrap-rsa)
  # and skip the TSS overlay, since verification uses the RSA roster instead of TSS.
  local wrb_overlay_args=""
  local tss_overlay_arg="--block-node-tss-overlay"
  if [[ "${WRB_RSA}" == "true" ]]; then
    local wrb_overlay="${overlay_dir}/bn-wrb-rsa-values.yaml"
    generate_bn_wrb_overlay "${wrb_overlay}"
    wrb_overlay_args="-f ${wrb_overlay}"
    tss_overlay_arg=""
  fi

  # Path to observability overlay (relative to scripts dir)
  local observability_overlay="${SCRIPT_DIR}/../../../../charts/block-node-server/values-overrides/enable-observability.yaml"

  for ((i = 1; i <= BN_COUNT; i++)); do
    local bn_overlay="${overlay_dir}/bn-block-node-${i}-values.yaml"
    local overlay_args=""

    if [[ -f "${bn_overlay}" ]]; then
      overlay_args="${overlay_args} -f ${bn_overlay}"
      log_line "  Using backfill overlay for block-node-${i}"
    fi

    # rsa-wrb: append the shared WRB overlay enabling the roster-bootstrap-rsa plugin.
    if [[ -n "${wrb_overlay_args}" ]]; then
      overlay_args="${overlay_args} ${wrb_overlay_args}"
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
      ${tss_overlay_arg} \
      ${bn_args} \
      ${overlay_args} \
      ${priority_mapping_args} || fail "ERROR: Failed to deploy Block Node ${i}" 1
    end_task
  done
}

function generate_cn_application_properties {
  local output_file="${1}"
  cat > "${output_file}" << 'EOF'
hedera.config.version=0
ledger.id=0x01
netty.mode=TEST
contracts.chainId=298
hedera.recordStream.logPeriod=1
balances.exportPeriodSecs=400
files.maxSizeKb=2048
hedera.recordStream.compressFilesOnCreation=true
balances.compressOnCreation=true
contracts.maxNumWithHapiSigsAccess=0
autoRenew.targetTypes=
nodes.gossipFqdnRestricted=false
hedera.profiles.active=TEST
nodes.updateAccountIdAllowed=true
blockStream.streamMode=BOTH
# TODO: we can remove this after we no longer need less than v0.59.x
networkAdmin.exportCandidateRoster=true
# for v0.59+, write the network.json file when you freeze the network
networkAdmin.diskNetworkExport=ONLY_FREEZE_BLOCK
hedera.realm=0
hedera.shard=0
nodes.webProxyEndpointsEnabled=true
nodes.nodeRewardsEnabled=false

blockStream.writerMode=FILE_AND_GRPC

blockNode.connectionStallThresholdMillis=5000
EOF

  # Verification-mode dependent settings. rsa-wrb streams Wrapped Record Blocks
  # verified against the RSA roster, so TSS is disabled and WRB streaming enabled.
  if [[ "${WRB_RSA}" == "true" ]]; then
    cat >> "${output_file}" << 'EOF'

tss.hintsEnabled=false
tss.historyEnabled=false
tss.forceMockSignatures=false
tss.wrapsEnabled=false

blockStream.streamWrappedRecordBlocks=true
EOF
  else
    cat >> "${output_file}" << 'EOF'

tss.hintsEnabled=true
tss.historyEnabled=true
tss.forceMockSignatures=false
tss.wrapsEnabled=true

blockStream.streamWrappedRecordBlocks=false
EOF
  fi
}

also# WRAPS v1.0.0 proving keys: downloaded + extracted once into a local cache and handed to Solo via
# --wraps-key-path on every deploy (no ~2 GB re-download). Pre-staging is required because CN-side
# runtime download is dropped before genesis on CN v0.75.x — see project-patterns.md (WRAPS on
# solo-e2e). Solo stages these under its cache dir named `wraps-v0.2.0` (default directoryName,
# not overridable here), so that directory holds v1.0.0 contents on this setup.
WRAPS_DOWNLOAD_URL="https://builds.hedera.com/tss/hiero/wraps/v1.0/wraps-v1.0.0.tar.gz"
WRAPS_KEYS_DIR="${HOME}/.solo/cache/wraps-v1.0.0-keys"
WRAPS_KEY_FILES="decider_pp.bin decider_vp.bin nova_pp.bin nova_vp.bin"

## Download and extract the WRAPS v1.0.0 proving keys into `keys_dir` once; later deploys reuse them.
function ensure_wraps_keys_cached {
  local keys_dir="${1}"
  local f=""
  local need_download="false"
  for f in ${WRAPS_KEY_FILES}; do
    [[ -f "${keys_dir}/${f}" ]] || need_download="true"
  done

  if [[ "${need_download}" == "true" ]]; then
    log_line "Caching WRAPS v1.0.0 proving keys (one-time download + extract, ~2 GB)"
    mkdir -p "${keys_dir}"
    local tarball="${keys_dir}/wraps-v1.0.0.tar.gz"
    curl -fSL "${WRAPS_DOWNLOAD_URL}" -o "${tarball}" || fail "ERROR: Failed to download WRAPS v1.0.0" 1
    tar -xzf "${tarball}" -C "${keys_dir}" || fail "ERROR: Failed to extract WRAPS v1.0.0 archive" 1
    rm -f "${tarball}"
  fi

  for f in ${WRAPS_KEY_FILES}; do
    [[ -f "${keys_dir}/${f}" ]] || fail "ERROR: WRAPS v1.0.0 key ${f} missing after extract" 1
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

  local overlay_dir="${SCRIPT_DIR}/../out"
  mkdir -p "${overlay_dir}"
  local cn_app_properties="${overlay_dir}/cn-application.properties"
  generate_cn_application_properties "${cn_app_properties}"

  start_task "Generating consensus keys for ${NODE_ALIASES}"
  solo keys consensus generate \
    --gossip-keys \
    --tls-keys \
    --deployment "${DEPLOYMENT}" \
    --node-aliases "${NODE_ALIASES}" || fail "ERROR: Failed to generate consensus keys" 1
  end_task

  # Pre-stage the WRAPS v1.0.0 keys via --wraps-key-path so the library is ready at genesis (CN
  # runtime download is too late on CN v0.75.x and the WRAPS proof gets dropped). CN-side download
  # stays disabled in cn-application.properties. --tss is kept so Solo still patches
  # block-nodes.json with the TSS message-size limits the large WRAPS genesis block needs.
  local wraps_key_args=""
  if [[ "${TSS_ENABLED}" == "true" ]]; then
    ensure_wraps_keys_cached "${WRAPS_KEYS_DIR}"
    wraps_key_args="--wraps true --wraps-key-path ${WRAPS_KEYS_DIR}"
  fi

  start_task "Deploying consensus network"
  # shellcheck disable=SC2086
  # adding --dev flag so in case it fails we have more information on details
  eval solo consensus network deploy \
    --deployment "${DEPLOYMENT}" \
    --pvcs true \
    --tss "${TSS_ENABLED}" \
    ${wraps_key_args} \
    --node-aliases "${NODE_ALIASES}" \
    --application-properties "${cn_app_properties}" \
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
  log_line "Deploying Mirror Nodes"
  log_line "----------------------"

  # Count Mirror Nodes from topology
  local topology_file="${TOPOLOGIES_DIR}/${TOPOLOGY}.yaml"
  local mn_count
  mn_count=$(yq '.mirror_nodes | keys | length // 0' "${topology_file}" 2>/dev/null || echo "1")

  if [[ "${mn_count}" -eq 0 ]]; then
    mn_count=1  # Fallback to single MN for backward compatibility
  fi

  log_line "  Mirror Nodes to deploy: %s" "${mn_count}"

  local mn_args=""
  if [[ -n "${MN_VERSION}" ]]; then
    mn_args="--mirror-node-version ${MN_VERSION}"
  fi

  # Deploy each Mirror Node
  for ((i = 1; i <= mn_count; i++)); do
    local mn_overlay="${OVERLAY_DIR}/mn-mirror-${i}-values.yaml"

    if [[ ! -f "${mn_overlay}" ]]; then
      log_line "  WARNING: Mirror Node ${i} overlay not found: ${mn_overlay}, skipping"
      continue
    fi

    log_line "  Using generated overlay: %s" "${mn_overlay}"
    if [[ "${VERBOSE}" == "true" ]]; then
      log_line "  --- Mirror Node ${i} overlay contents ---"
      cat "${mn_overlay}"
      log_line "  --- End Mirror Node ${i} overlay ---"
    fi

    start_task "Deploying Mirror Node ${i}"
    # shellcheck disable=SC2086
    solo mirror node add \
      --deployment "${DEPLOYMENT}" \
      --pinger \
      --cluster-ref "${CLUSTER_REF}" \
      --enable-ingress \
      ${mn_args} \
      -f "${mn_overlay}" || fail "ERROR: Failed to deploy Mirror Node ${i}" 1
    end_task
  done
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

  # Generate all overlays up front (used by both Block Node and Mirror Node deploys).
  generate_overlays

  # Deploy in order: BN -> CN -> MN -> Relay -> Explorer.
  # Block Nodes must come first so Solo wires them as stream sources on the
  # Consensus Nodes. In rsa-wrb mode the Block Node's roster-bootstrap-rsa plugin
  # polls the Mirror Node indefinitely, so it tolerates the MN starting later.
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
