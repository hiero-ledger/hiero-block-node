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
#   bn-<node-id>-values.yaml           Block Node overlay (only for BNs with peers)
#   mn-<node-id>-values.yaml           Mirror Node overlay
#   bn-<node-id>-priority-mapping.txt  BN priority mapping for Solo --priority-mapping
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
VERIFICATION_MODE="tss"  # "tss" (default) or "rsa-wrb" (WRB cutover Mirror Node config)

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
  --verification-mode MODE   Block verification mode: tss (default) or rsa-wrb
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
    --verification-mode)
      VERIFICATION_MODE="$2"
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

# Generate Block Node overlay for a block node with peers and/or plugin_ports.
# Only generates the overlay file when there is something to emit.
function generate_bn_overlay {
  local bn_name="${1}"
  local output_file="${2}"

  # Determine if this block node has peers
  local has_peers
  has_peers=$(yq ".block_nodes[\"${bn_name}\"].peers // \"\"" "${TOPOLOGY_FILE}" 2>/dev/null)
  local peer_count=0
  if [[ -n "${has_peers}" ]] && [[ "${has_peers}" != "null" ]]; then
    peer_count=$(yq ".block_nodes[\"${bn_name}\"].peers | length" "${TOPOLOGY_FILE}" 2>/dev/null)
  fi

  # Determine if this block node has plugin_ports
  local raw_plugin_ports
  raw_plugin_ports=$(yq ".block_nodes[\"${bn_name}\"].plugin_ports // \"\"" "${TOPOLOGY_FILE}" 2>/dev/null)
  local has_plugin_ports="false"
  if [[ -n "${raw_plugin_ports}" ]] && [[ "${raw_plugin_ports}" != "null" ]]; then
    has_plugin_ports="true"
  fi

  # Nothing to generate
  if [[ "${peer_count}" -eq 0 ]] && [[ "${has_plugin_ports}" == "false" ]]; then
    return 0
  fi

  # Build backfill sources array (only when peers are configured)
  local sources=""
  local greedy_mode="false"

  if [[ "${peer_count}" -gt 0 ]]; then
    local peer_names
    peer_names=$(yq -r ".block_nodes[\"${bn_name}\"].peers[]" "${TOPOLOGY_FILE}" 2>/dev/null)

    if [[ -z "${peer_names}" ]]; then
      peer_count=0  # Treat as no peers if names could not be read
    else
      local priority=1
      while IFS= read -r peer; do
        [[ -z "${peer}" ]] && continue

        local peer_port
        peer_port=$(yq ".block_nodes[\"${peer}\"].port // 40840" "${TOPOLOGY_FILE}")
        local peer_dns
        peer_dns=$(get_service_dns "${peer}" "${NAMESPACE}")

        # Read gRPC tuning from the peer node (if configured)
        local grpc_tuning=""
        local max_frame_size
        max_frame_size=$(yq ".block_nodes[\"${peer}\"].grpc_tuning.max_frame_size // 0" "${TOPOLOGY_FILE}" 2>/dev/null)
        local initial_window_size
        initial_window_size=$(yq ".block_nodes[\"${peer}\"].grpc_tuning.initial_window_size // 0" "${TOPOLOGY_FILE}" 2>/dev/null)
        local initial_buffer_size
        initial_buffer_size=$(yq ".block_nodes[\"${peer}\"].grpc_tuning.initial_buffer_size // 0" "${TOPOLOGY_FILE}" 2>/dev/null)
        local connect_timeout
        connect_timeout=$(yq ".block_nodes[\"${peer}\"].grpc_tuning.connect_timeout // 0" "${TOPOLOGY_FILE}" 2>/dev/null)
        local read_timeout
        read_timeout=$(yq ".block_nodes[\"${peer}\"].grpc_tuning.read_timeout // 0" "${TOPOLOGY_FILE}" 2>/dev/null)

        if [[ "${max_frame_size}" -gt 0 ]] || [[ "${initial_window_size}" -gt 0 ]] || \
           [[ "${initial_buffer_size}" -gt 0 ]] || [[ "${connect_timeout}" -gt 0 ]] || \
           [[ "${read_timeout}" -gt 0 ]]; then
          grpc_tuning="
        grpc_webclient_tuning:"
          [[ "${max_frame_size}" -gt 0 ]] && grpc_tuning="${grpc_tuning}
          max_frame_size: ${max_frame_size}"
          [[ "${initial_window_size}" -gt 0 ]] && grpc_tuning="${grpc_tuning}
          initial_window_size: ${initial_window_size}"
          [[ "${initial_buffer_size}" -gt 0 ]] && grpc_tuning="${grpc_tuning}
          initial_buffer_size: ${initial_buffer_size}"
          [[ "${connect_timeout}" -gt 0 ]] && grpc_tuning="${grpc_tuning}
          connect_timeout: ${connect_timeout}"
          [[ "${read_timeout}" -gt 0 ]] && grpc_tuning="${grpc_tuning}
          read_timeout: ${read_timeout}"
        fi

        sources="${sources}
      - address: \"${peer_dns}\"
        port: ${peer_port}
        priority: ${priority}${grpc_tuning}"
        priority=$((priority + 1))
      done <<< "${peer_names}"

      greedy_mode=$(yq ".block_nodes[\"${bn_name}\"].greedy // false" "${TOPOLOGY_FILE}" 2>/dev/null)
    fi
  fi

  # Write the overlay with a single blockNode: root (avoids duplicate-key issues in YAML)
  {
    echo "# Generated by generate-chart-values-config-overlays.sh from topology: ${TOPOLOGY_NAME}"
    echo "# Block Node: ${bn_name}"
    echo "blockNode:"

    # Per-plugin port overrides (blockNode.ports). Keys are passed through as-is — use
    # camelCase to match the Helm chart (publisher, subscriber, blockAccess, health, serverStatus).
    if [[ "${has_plugin_ports}" == "true" ]]; then
      echo "  ports:"
      while IFS="=" read -r key value; do
        [[ -z "${key}" ]] && continue
        echo "    ${key}: ${value}"
      done < <(yq -r ".block_nodes[\"${bn_name}\"].plugin_ports | to_entries[] | .key + \"=\" + (.value | tostring)" "${TOPOLOGY_FILE}" 2>/dev/null)
    fi

    # Backfill config (blockNode.config + blockNode.backfill)
    if [[ "${peer_count}" -gt 0 ]]; then
      echo "  config:"
      echo "    BACKFILL_BLOCK_NODE_SOURCES_PATH: \"/opt/hiero/block-node/backfill/block-node-sources.json\""
      echo "    BACKFILL_GREEDY: \"${greedy_mode}\""
      echo "  backfill:"
      echo "    path: \"/opt/hiero/block-node/backfill\""
      echo "    filename: \"block-node-sources.json\""
      echo "    sources:${sources}"
    fi
  } > "${output_file}"

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
              - endpoints:
                  - host: ${bn_dns}
                    port: ${bn_port}"
  done <<< "${block_node_names}"

  # Image overrides (workaround, ALL topologies): the default GraalVM-native Mirror Node
  # images crash on startup due to missing reflection hints for several classes. Override
  # the affected modules to the JVM images (gcr.io/mirrornode/*); their native-sized memory
  # limits are too small for the JVM, so bump them. web3 native and pinger (already JVM)
  # are fine and left as-is.
  # TODO: remove these overrides once MN ships fully fixed native images.
  local importer_jvm_override="  image:
    registry: gcr.io
    repository: mirrornode/hedera-mirror-importer
  resources:
    limits:
      memory: 4Gi"
  local grpc_jvm_override="grpc:
  image:
    registry: gcr.io
    repository: mirrornode/hedera-mirror-grpc
  resources:
    limits:
      memory: 2Gi"
  local restjava_jvm_override="restjava:
  image:
    registry: gcr.io
    repository: mirrornode/hedera-mirror-rest-java
  resources:
    limits:
      memory: 1Gi"

  # Write the overlay. rsa-wrb uses the WRB cutover config (record-stream downloader
  # kept as fallback) plus DISABLE_IMPORTER_SPRING_PROFILES; the default mode reads
  # blocks only from the Block Node with the record downloader disabled.
  if [[ "${VERIFICATION_MODE}" == "rsa-wrb" ]]; then
    cat > "${output_file}" << EOF
# Generated by generate-chart-values-config-overlays.sh from topology: ${TOPOLOGY_NAME}
# Mirror Node: ${mn_name} (rsa-wrb / WRB cutover)
importer:
${importer_jvm_override}
  env:
    DISABLE_IMPORTER_SPRING_PROFILES: "true"
  config:
    hiero:
      mirror:
        importer:
          block:
            cutover:
              enabled: true
              firstStage:
                enabled: true
            nodes:${nodes_config}
            stream:
                maxStreamResponseSize: 36MB
${grpc_jvm_override}
${restjava_jvm_override}
EOF
  else
    cat > "${output_file}" << EOF
# Generated by generate-chart-values-config-overlays.sh from topology: ${TOPOLOGY_NAME}
# Mirror Node: ${mn_name}
importer:
${importer_jvm_override}
  config:
    hiero:
      mirror:
        importer:
          block:
            enabled: true
            nodes:${nodes_config}
            sourceType: BLOCK_NODE
            stream:
                maxStreamResponseSize: 36MB
          downloader:
            record:
              enabled: false
          startDate: 1970-01-01T00:00:00Z
          stream:
            maxSubscribeAttempts: 10
            responseTimeout: 10s
${grpc_jvm_override}
${restjava_jvm_override}
EOF
  fi

  log_line "Generated MN overlay: %s" "${output_file}"
}

# Generate BN-centric priority mappings for Solo --priority-mapping parameter
# Inverts the CN-centric topology to BN-centric view
# Output: One file per BN with format "node1=1,node2=2,node3=1"
function generate_bn_priority_mappings {
  local output_dir="${1}"

  # Check if consensus_nodes section exists
  local has_cn_section
  has_cn_section=$(yq '.consensus_nodes | keys | length // 0' "${TOPOLOGY_FILE}" 2>/dev/null)

  if [[ "${has_cn_section}" -eq 0 ]]; then
    return 0  # No consensus_nodes section
  fi

  # Get all block node names
  local bn_names
  bn_names=$(yq -r '.block_nodes | keys[]' "${TOPOLOGY_FILE}" 2>/dev/null)

  if [[ -z "${bn_names}" ]]; then
    return 0
  fi

  # For each block node, find which CNs route to it and with what priority
  while IFS= read -r bn_name; do
    [[ -z "${bn_name}" ]] && continue

    local mapping_entries=""

    # Check each CN's block_nodes array
    local cn_names
    cn_names=$(yq -r '.consensus_nodes | keys[]' "${TOPOLOGY_FILE}")

    while IFS= read -r cn_name; do
      [[ -z "${cn_name}" ]] && continue

      # Get this CN's block_nodes array and find position of current BN
      local bn_list
      bn_list=$(yq -r ".consensus_nodes[\"${cn_name}\"].block_nodes[]" "${TOPOLOGY_FILE}" 2>/dev/null)

      local priority=1
      while IFS= read -r bn_ref; do
        [[ -z "${bn_ref}" ]] && continue

        if [[ "${bn_ref}" == "${bn_name}" ]]; then
          # This CN routes to this BN with this priority
          [[ -n "${mapping_entries}" ]] && mapping_entries="${mapping_entries},"
          mapping_entries="${mapping_entries}${cn_name}=${priority}"
          break
        fi
        priority=$((priority + 1))
      done <<< "${bn_list}"
    done <<< "${cn_names}"

    # Write mapping file for this BN
    if [[ -n "${mapping_entries}" ]]; then
      local output_file="${output_dir}/bn-${bn_name}-priority-mapping.txt"
      echo "${mapping_entries}" > "${output_file}"
      log_line "Generated BN priority mapping: %s" "${output_file}"
    fi
  done <<< "${bn_names}"
}

# Generate CN block-nodes.json files for consensus nodes whose block nodes have non-default
# publisher (streamingPort) or serverStatus (servicePort) plugin ports.
# streamingPort = plugin_ports.publisher // topology port // 40840
# servicePort   = plugin_ports.serverStatus // topology port // 40840
function generate_cn_block_nodes_json {
  local output_dir="${1}"

  local has_cn_section
  has_cn_section=$(yq '.consensus_nodes | keys | length // 0' "${TOPOLOGY_FILE}" 2>/dev/null)
  [[ "${has_cn_section}" -eq 0 ]] && return 0

  local cn_names
  cn_names=$(yq -r '.consensus_nodes | keys[]' "${TOPOLOGY_FILE}" 2>/dev/null)
  [[ -z "${cn_names}" ]] && return 0

  while IFS= read -r cn_name; do
    [[ -z "${cn_name}" ]] && continue

    local bn_list
    bn_list=$(yq -r ".consensus_nodes[\"${cn_name}\"].block_nodes[]" "${TOPOLOGY_FILE}" 2>/dev/null)
    [[ -z "${bn_list}" ]] && continue

    # Only generate if at least one BN in this CN's list has a non-default publisher or serverStatus port
    local needs_custom_config="false"
    while IFS= read -r bn_name; do
      [[ -z "${bn_name}" ]] && continue
      local pub_port ss_port
      pub_port=$(yq ".block_nodes[\"${bn_name}\"].plugin_ports.publisher // \"\"" "${TOPOLOGY_FILE}" 2>/dev/null)
      ss_port=$(yq ".block_nodes[\"${bn_name}\"].plugin_ports.serverStatus // \"\"" "${TOPOLOGY_FILE}" 2>/dev/null)
      if [[ -n "${pub_port}" && "${pub_port}" != "null" ]] || \
         [[ -n "${ss_port}" && "${ss_port}" != "null" ]]; then
        needs_custom_config="true"
        break
      fi
    done <<< "${bn_list}"
    [[ "${needs_custom_config}" == "false" ]] && continue

    local nodes_json=""
    local priority=1
    local first="true"

    while IFS= read -r bn_name; do
      [[ -z "${bn_name}" ]] && continue

      local bn_dns
      bn_dns=$(get_service_dns "${bn_name}" "${NAMESPACE}")

      local default_port
      default_port=$(yq ".block_nodes[\"${bn_name}\"].port // 40840" "${TOPOLOGY_FILE}" 2>/dev/null)

      local streaming_port service_port
      streaming_port=$(yq ".block_nodes[\"${bn_name}\"].plugin_ports.publisher // ${default_port}" "${TOPOLOGY_FILE}" 2>/dev/null)
      service_port=$(yq ".block_nodes[\"${bn_name}\"].plugin_ports.serverStatus // ${default_port}" "${TOPOLOGY_FILE}" 2>/dev/null)

      [[ "${first}" != "true" ]] && nodes_json="${nodes_json},"
      nodes_json="${nodes_json}
    {
      \"address\": \"${bn_dns}\",
      \"streamingPort\": ${streaming_port},
      \"servicePort\": ${service_port},
      \"priority\": ${priority}
    }"
      first="false"
      priority=$((priority + 1))
    done <<< "${bn_list}"

    local output_file="${output_dir}/cn-${cn_name}-block-nodes.json"
    printf '{\n  "nodes": [%s\n  ],\n  "blockItemBatchSize": 256\n}\n' "${nodes_json}" > "${output_file}"
    log_line "Generated CN block-nodes.json: %s" "${output_file}"
  done <<< "${cn_names}"
}

function validate_prerequisites {
  if ! command -v yq >/dev/null 2>&1; then
    fail "ERROR: yq is required but not found. Install from: https://github.com/mikefarah/yq" 1
  fi

  if ! yq -e '.block_nodes' "${TOPOLOGY_FILE}" >/dev/null 2>&1; then
    fail "ERROR: Topology file is missing 'block_nodes' section or is not valid YAML: ${TOPOLOGY_FILE}" 1
  fi
}

function main {
  log_line "Generating Helm overlays from: %s" "${TOPOLOGY_FILE}"
  log_line "  Namespace: %s" "${NAMESPACE}"
  log_line "  Output dir: %s" "${OUTPUT_DIR}"
  log_line ""

  validate_prerequisites

  local generated_count=0

  # Process block nodes - generate overlays for those with peers
  local bn_names
  bn_names=$(yq -r '.block_nodes | keys[]' "${TOPOLOGY_FILE}" 2>/dev/null)

  if [[ -n "${bn_names}" ]]; then
    while IFS= read -r bn_name; do
      [[ -z "${bn_name}" ]] && continue
      generate_bn_overlay "${bn_name}" "${OUTPUT_DIR}/bn-${bn_name}-values.yaml"
      if [[ -f "${OUTPUT_DIR}/bn-${bn_name}-values.yaml" ]]; then
        generated_count=$((generated_count + 1))
      fi
    done <<< "${bn_names}"
  else
    # Zero-BN topologies are legitimate for tests where the MN ingests via
    # the legacy record-stream path (e.g. WRB Distribution E2E #3125 slice 1,
    # which brings up CNs+MN but no BN so the CLI-side flow can be validated
    # in isolation before BNs are layered in later slices). Warn but continue
    # — the generator's job here is BN peer/plugin config, which is trivially
    # empty when there are no BNs.
    log_line "  No block nodes in topology; skipping BN overlay generation."
  fi

  # Process mirror nodes
  local mn_names
  mn_names=$(yq -r '.mirror_nodes | keys[]' "${TOPOLOGY_FILE}" 2>/dev/null)

  if [[ -n "${mn_names}" ]]; then
    while IFS= read -r mn_name; do
      [[ -z "${mn_name}" ]] && continue
      generate_mn_overlay "${mn_name}" "${OUTPUT_DIR}/mn-${mn_name}-values.yaml"
      if [[ -f "${OUTPUT_DIR}/mn-${mn_name}-values.yaml" ]]; then
        generated_count=$((generated_count + 1))
      fi
    done <<< "${mn_names}"
  fi

  # Generate BN priority mappings for Solo --priority-mapping
  generate_bn_priority_mappings "${OUTPUT_DIR}"

  # Generate CN block-nodes.json overrides when publisher/serverStatus use non-default ports
  generate_cn_block_nodes_json "${OUTPUT_DIR}"

  # Count priority mapping files
  local mapping_count
  mapping_count=$(find "${OUTPUT_DIR}" -maxdepth 1 -name "bn-*-priority-mapping.txt" -type f 2>/dev/null | wc -l | tr -d ' ')
  generated_count=$((generated_count + mapping_count))

  log_line ""
  if [[ "${generated_count}" -eq 0 ]]; then
    # A zero-overlay outcome is legitimate whenever the topology has no
    # relationships the generator would translate into helm values:
    #   * BN peers (backfill sources) — not set on any BN
    #   * BN plugin_ports              — not set on any BN
    #   * MN.block_nodes references    — not set on any MN
    #
    # Examples:
    #   * WRB Distribution E2E (#3125) slice 1 — no BNs at all; MN ingests
    #     via record-stream path.
    #   * WRB Distribution E2E (#3125) slice 3 — 3 standalone BNs, no peers,
    #     no MN→BN wiring yet (that arrives in step 8+).
    #
    # The deploy step downstream still requires at least SOMEONE (BN, MN, or
    # CN) in the topology; that's checked separately.
    log_line "No overlay files needed (no BN peers, no BN plugin_ports, no MN→BN wiring in this topology)."
  else
    log_line "Overlay generation complete. Generated %s file(s)." "${generated_count}"
  fi
}

main
