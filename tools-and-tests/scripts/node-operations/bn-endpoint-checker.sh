#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# =============================================================================
# bn-endpoint-checker.sh — Block Node endpoint health checker
# =============================================================================
#
# Checks one or more Block Node gRPC endpoints by:
#   1. Verifying TCP reachability  (nc)
#   2. Calling serverStatus        → reports first/last available block,
#                                    only_latest_state flag
#   3. Calling serverStatusDetail  → reports BN software version, stream proto
#                                    version, available block ranges, registered
#                                    plugins, and TSS data presence
#
# Endpoints are supplied as positional arguments in <host:port> form.
# A proto package is resolved in the following priority order:
#   --proto-dir flag  >  PROTO_DIR env var  >  downloaded via --version flag
#
# USAGE
#   bn-endpoint-checker.sh [OPTIONS] <host:port> [<host:port> ...]
#
# OPTIONS
#   -v, --version VERSION   BN protobuf version to download when no proto
#                           directory is available (e.g. 0.32.0).
#                           Downloads from the GitHub releases page and
#                           extracts next to this script. Skipped if the
#                           directory already exists.
#   -p, --proto-dir DIR     Path to an already-extracted proto directory.
#                           Overrides the PROTO_DIR environment variable.
#       --tls               Use TLS for gRPC connections.
#                           Default: plaintext (-plaintext flag to grpcurl).
#   -h, --help              Print this help text and exit.
#
# ENVIRONMENT
#   PROTO_DIR               Alternative to --proto-dir.
#                           --proto-dir takes precedence if both are set.
#
# EXAMPLES
#   # Two nodes — download proto pack automatically
#   bn-endpoint-checker.sh --version 0.32.0 \
#     node1.example.com:40840 node2.example.com:40840
#
#   # One node — use an existing local proto directory
#   bn-endpoint-checker.sh --proto-dir /opt/bn-proto \
#     mynode.example.com:40840
#
#   # Via environment variable, with TLS
#   PROTO_DIR=/opt/bn-proto bn-endpoint-checker.sh --tls \
#     mynode.example.com:443
#
#   # Previewnet convenience example
#   bn-endpoint-checker.sh --version 0.32.0 \
#     lfh01.previewnet.blocknode.hashgraph-devops.com:40840 \
#     lfh02.previewnet.blocknode.hashgraph-devops.com:40840
#
# REQUIREMENTS
#   grpcurl   Auto-installed if missing (you will be prompted for the
#             install location).
#   nc        Netcat — used for the TCP reachability check.
#   jq        JSON processor — used for pretty-printing gRPC responses.
#   curl      Used to download grpcurl and proto archives when needed.
#
# GRPCURL AUTO-INSTALL
#   If grpcurl is not found on PATH or in $SCRIPT_DIR/bin/, the script
#   will offer three choices:
#     1) Local install  → $SCRIPT_DIR/bin/grpcurl   (no sudo required)
#     2) System install → /usr/local/bin/grpcurl     (requires sudo)
#     q) Quit           → manual install instructions are printed
#
# PROTO PACKAGE LAYOUT
#   The downloaded archive is extracted into:
#     $SCRIPT_DIR/block-node-protobuf-<VERSION>/
#   That directory must contain the path:
#     block-node/api/node_service.proto
#   and all transitive imports (e.g. services/basic_types.proto).
#   These are all present in the official hiero-block-node release archive.
#
# EXIT CODES
#   0   All endpoints passed TCP and serverStatus checks.
#   1   One or more endpoints failed (TCP unreachable or serverStatus error).
#   2   Usage / configuration error (bad arguments, missing dependencies).
# =============================================================================

set -euo pipefail

# ── Script-level constants ────────────────────────────────────────────────────

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# grpcurl version to install when auto-installing.
GRPCURL_VERSION="1.9.1"

# GitHub release URLs (no trailing slash).
PROTO_ARCHIVE_BASE_URL="https://github.com/hiero-ledger/hiero-block-node/releases/download"
GRPCURL_RELEASE_BASE_URL="https://github.com/fullstorydev/grpcurl/releases/download"

# Proto file path relative to the extracted proto directory root.
NODE_SERVICE_PROTO="block-node/api/node_service.proto"

# Fully-qualified gRPC service name (matches the proto package + service).
GRPC_SERVICE="org.hiero.block.api.BlockNodeService"

# ── ANSI colours (disabled automatically when stdout is not a terminal) ───────

if [[ -t 1 ]]; then
  C_GREEN='\033[0;32m'
  C_RED='\033[0;31m'
  C_YELLOW='\033[0;33m'
  C_BLUE='\033[0;34m'
  C_BOLD='\033[1m'
  C_RESET='\033[0m'
else
  C_GREEN='' C_RED='' C_YELLOW='' C_BLUE='' C_BOLD='' C_RESET=''
fi

log_info()    { printf "%b\n"    "${C_BLUE}${*}${C_RESET}"; }
log_success() { printf "%b\n"    "${C_GREEN}${*}${C_RESET}"; }
log_warn()    { printf "%b\n"    "${C_YELLOW}${*}${C_RESET}" >&2; }
log_err()     { printf "%b\n"    "${C_RED}${*}${C_RESET}" >&2; }

# ── Usage / help ──────────────────────────────────────────────────────────────

usage() {
  # Print every line that starts with '#' (excluding the shebang) from this file.
  grep '^#' "$0" | grep -v '^#!/' | sed 's/^# \{0,1\}//'
  exit "${1:-0}"
}

# ── Argument parsing ──────────────────────────────────────────────────────────

BN_VERSION=""
PROTO_DIR_ARG=""
USE_TLS=false
ENDPOINTS=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    -v|--version)
      [[ -n "${2:-}" ]] || { log_err "--version requires a value"; usage 2; }
      BN_VERSION="$2"; shift 2 ;;
    -p|--proto-dir)
      [[ -n "${2:-}" ]] || { log_err "--proto-dir requires a value"; usage 2; }
      PROTO_DIR_ARG="$2"; shift 2 ;;
    --tls)
      USE_TLS=true; shift ;;
    -h|--help)
      usage 0 ;;
    -*)
      log_err "Unknown option: $1"; usage 2 ;;
    *)
      ENDPOINTS+=("$1"); shift ;;
  esac
done

# ── Input validation ──────────────────────────────────────────────────────────

if [[ ${#ENDPOINTS[@]} -eq 0 ]]; then
  log_err "Error: at least one <host:port> endpoint is required."
  usage 2
fi

# Resolve proto directory: flag > env var > derived from --version.
RESOLVED_PROTO_DIR="${PROTO_DIR_ARG:-${PROTO_DIR:-}}"

if [[ -z "$RESOLVED_PROTO_DIR" && -z "$BN_VERSION" ]]; then
  log_err "Error: supply --proto-dir (or PROTO_DIR env var) or --version to download the proto package."
  usage 2
fi

# When only --version is given, derive the local directory name.
if [[ -z "$RESOLVED_PROTO_DIR" && -n "$BN_VERSION" ]]; then
  RESOLVED_PROTO_DIR="${SCRIPT_DIR}/block-node-protobuf-${BN_VERSION}"
fi

# Basic <host:port> format validation.
for ep in "${ENDPOINTS[@]}"; do
  if [[ "$ep" != *:* ]]; then
    log_err "Error: endpoint '${ep}' must be in <host:port> form."
    exit 2
  fi
done

# ── Dependency checks ─────────────────────────────────────────────────────────

check_nc() {
  if ! command -v nc &>/dev/null; then
    log_err "Error: 'nc' (netcat) is required for TCP reachability checks but was not found."
    log_err "Install it via your package manager:"
    log_err "  Debian/Ubuntu : apt install netcat-openbsd"
    log_err "  macOS         : nc is included in the base system"
    exit 2
  fi
}

check_jq() {
  if ! command -v jq &>/dev/null; then
    log_err "Error: 'jq' is required for JSON parsing but was not found."
    log_err "Install it via your package manager:"
    log_err "  Debian/Ubuntu : apt install jq"
    log_err "  macOS         : brew install jq"
    exit 2
  fi
}

# ── grpcurl: platform detection ───────────────────────────────────────────────

# Returns the grpcurl release archive platform string, e.g. "linux_x86_64" or "osx_arm64".
detect_grpcurl_platform() {
  local os_raw arch_raw os_tag arch_tag
  os_raw="$(uname -s)"
  arch_raw="$(uname -m)"

  case "$os_raw" in
    Linux)  os_tag="linux" ;;
    Darwin) os_tag="osx"   ;;
    *)
      log_err "Unsupported OS: ${os_raw}. Install grpcurl manually:"
      log_err "  ${GRPCURL_RELEASE_BASE_URL}/v${GRPCURL_VERSION}/"
      exit 2 ;;
  esac

  case "$arch_raw" in
    x86_64)        arch_tag="x86_64" ;;
    arm64|aarch64) arch_tag="arm64"  ;;
    *)
      log_err "Unsupported architecture: ${arch_raw}. Install grpcurl manually:"
      log_err "  ${GRPCURL_RELEASE_BASE_URL}/v${GRPCURL_VERSION}/"
      exit 2 ;;
  esac

  echo "${os_tag}_${arch_tag}"
}

# ── grpcurl: auto-install ─────────────────────────────────────────────────────

install_grpcurl() {
  local platform archive url

  platform="$(detect_grpcurl_platform)"
  archive="grpcurl_${GRPCURL_VERSION}_${platform}.tar.gz"
  url="${GRPCURL_RELEASE_BASE_URL}/v${GRPCURL_VERSION}/${archive}"

  log_warn ""
  log_warn "grpcurl ${GRPCURL_VERSION} not found — it is required for gRPC calls."
  log_warn ""
  log_warn "Where would you like to install it?"
  log_warn "  1) Local  — ${SCRIPT_DIR}/bin/grpcurl  (no sudo required)"
  log_warn "  2) System — /usr/local/bin/grpcurl      (requires sudo)"
  log_warn "  q) Quit   — install manually from:"
  log_warn "              ${GRPCURL_RELEASE_BASE_URL}/v${GRPCURL_VERSION}/"
  log_warn ""
  read -rp "Choice [1/2/q]: " install_choice

  case "$install_choice" in
    1)
      local install_dir="${SCRIPT_DIR}/bin"
      mkdir -p "$install_dir"
      log_info "Downloading grpcurl ${GRPCURL_VERSION} for ${platform} → ${install_dir}/grpcurl …"
      curl -fsSL "$url" | tar -xz -C "$install_dir" grpcurl
      chmod +x "${install_dir}/grpcurl"
      export PATH="${install_dir}:${PATH}"
      log_success "grpcurl installed at ${install_dir}/grpcurl"
      log_info "Tip: add '${install_dir}' to your PATH to avoid this prompt in future sessions."
      ;;
    2)
      local tmp_dir
      tmp_dir="$(mktemp -d)"
      log_info "Downloading grpcurl ${GRPCURL_VERSION} for ${platform} …"
      curl -fsSL "$url" | tar -xz -C "$tmp_dir" grpcurl
      log_info "Installing to /usr/local/bin/grpcurl (sudo) …"
      sudo mv "${tmp_dir}/grpcurl" /usr/local/bin/grpcurl
      sudo chmod +x /usr/local/bin/grpcurl
      rm -rf "$tmp_dir"
      log_success "grpcurl installed at /usr/local/bin/grpcurl"
      ;;
    q|Q)
      log_err "Aborted. Install grpcurl manually:"
      log_err "  ${GRPCURL_RELEASE_BASE_URL}/v${GRPCURL_VERSION}/"
      exit 2
      ;;
    *)
      log_err "Invalid choice '${install_choice}'. Exiting."
      exit 2
      ;;
  esac
}

check_grpcurl() {
  # Check the local script-adjacent bin first (installed by a previous run).
  local local_bin="${SCRIPT_DIR}/bin"
  if [[ -x "${local_bin}/grpcurl" ]]; then
    export PATH="${local_bin}:${PATH}"
  fi

  if ! command -v grpcurl &>/dev/null; then
    install_grpcurl
  fi
}

# ── Proto package: download and extract ───────────────────────────────────────

ensure_proto_dir() {
  if [[ -d "$RESOLVED_PROTO_DIR" ]]; then
    log_info "Proto directory : ${RESOLVED_PROTO_DIR}"
    # Sanity-check that the expected proto file exists.
    if [[ ! -f "${RESOLVED_PROTO_DIR}/${NODE_SERVICE_PROTO}" ]]; then
      log_err "Error: proto directory exists but is missing the expected file:"
      log_err "  ${RESOLVED_PROTO_DIR}/${NODE_SERVICE_PROTO}"
      log_err "Verify the directory contents or re-download with --version."
      exit 2
    fi
    return 0
  fi

  # Directory does not exist — download required.
  if [[ -z "$BN_VERSION" ]]; then
    log_err "Error: proto directory not found: ${RESOLVED_PROTO_DIR}"
    log_err "Provide --version to download it automatically."
    exit 2
  fi

  local archive="block-node-protobuf-${BN_VERSION}.tgz"
  local url="${PROTO_ARCHIVE_BASE_URL}/v${BN_VERSION}/${archive}"
  local tmp_archive="${SCRIPT_DIR}/${archive}"

  log_info "Downloading proto package v${BN_VERSION} …"
  log_info "  URL : ${url}"
  if ! curl -fsSL -o "$tmp_archive" "$url"; then
    log_err "Error: failed to download proto package."
    log_err "  Check that version v${BN_VERSION} exists on the releases page:"
    log_err "  https://github.com/hiero-ledger/hiero-block-node/releases"
    rm -f "$tmp_archive"
    exit 2
  fi

  log_info "Extracting → ${RESOLVED_PROTO_DIR} …"
  mkdir -p "$RESOLVED_PROTO_DIR"
  tar -xzf "$tmp_archive" -C "$RESOLVED_PROTO_DIR"
  rm -f "$tmp_archive"

  if [[ ! -f "${RESOLVED_PROTO_DIR}/${NODE_SERVICE_PROTO}" ]]; then
    log_err "Error: archive extracted but expected proto file is missing:"
    log_err "  ${RESOLVED_PROTO_DIR}/${NODE_SERVICE_PROTO}"
    exit 2
  fi

  log_success "Proto package ready : ${RESOLVED_PROTO_DIR}"
}

# ── gRPC call helper ──────────────────────────────────────────────────────────

# Invokes grpcurl for the given target and method name.
# Passes -plaintext when TLS is disabled (the default).
# All output (stdout + stderr) is returned to the caller.
grpc_call() {
  local target="$1" method="$2"
  local -a flags=(
    -emit-defaults
    -import-path "${RESOLVED_PROTO_DIR}"
    -proto        "${NODE_SERVICE_PROTO}"
    -d            '{}'
  )
  # grpcurl uses TLS by default; -plaintext opts out.
  [[ "$USE_TLS" == "false" ]] && flags=("-plaintext" "${flags[@]}")

  grpcurl "${flags[@]}" "${target}" "${GRPC_SERVICE}/${method}"
}

# ── TCP reachability check ────────────────────────────────────────────────────

tcp_check() {
  local host="$1" port="$2"
  nc -z -w 3 "$host" "$port" &>/dev/null
}

# ── Output formatting ─────────────────────────────────────────────────────────

# Prints a two-column key/value line indented by 5 spaces.
print_field() {
  local key="$1" value="$2"
  printf "     %-30s %s\n" "${key}:" "$value"
}

# Formats a SemanticVersion protobuf JSON object as "major.minor.patch[-pre][+build]".
# Accepts the JSON object as a string argument.
format_semver() {
  local json="$1"
  echo "$json" | jq -r '
    if . == null or . == {} then "unknown"
    else
      ((.major // 0 | tostring) + "." +
       (.minor // 0 | tostring) + "." +
       (.patch // 0 | tostring)) +
      (if (.pre  // "") != "" then "-" + .pre  else "" end) +
      (if (.build // "") != "" then "+" + .build else "" end)
    end
  '
}

# Pretty-prints a ServerStatusResponse JSON object.
print_server_status() {
  local json="$1"
  local first last only_latest
  first="$(       echo "$json" | jq -r '.firstAvailableBlock // "0"')"
  last="$(        echo "$json" | jq -r '.lastAvailableBlock  // "0"')"
  only_latest="$( echo "$json" | jq -r '.onlyLatestState     // false')"

  print_field "first_available_block" "$first"
  print_field "last_available_block"  "$last"
  print_field "only_latest_state"     "$only_latest"
}

# Pretty-prints a ServerStatusDetailResponse JSON object.
print_server_status_detail() {
  local json="$1"

  # Software version
  local bn_ver_json bn_ver
  bn_ver_json="$(echo "$json" | jq '.versionInformation.blockNodeVersion // {}')"
  bn_ver="$(format_semver "$bn_ver_json")"

  # Stream proto version
  local stream_ver_json stream_ver
  stream_ver_json="$(echo "$json" | jq '.versionInformation.streamProtoVersion // {}')"
  stream_ver="$(format_semver "$stream_ver_json")"

  # Available ranges
  local range_count
  range_count="$(echo "$json" | jq '.availableRanges | length')"

  local range_summary
  if [[ "$range_count" -gt 0 ]]; then
    range_summary="$(echo "$json" | jq -r '
      .availableRanges |
      to_entries |
      map("       range \(.key + 1): blocks \(.value.rangeStart) – \(.value.rangeEnd)") |
      join("\n")
    ')"
  else
    range_summary=""
  fi

  # Installed plugins
  local plugin_count plugin_list
  plugin_count="$(echo "$json" | jq '.versionInformation.installedPluginVersions | length')"
  if [[ "$plugin_count" -gt 0 ]]; then
    plugin_list="$(echo "$json" | jq -r '
      .versionInformation.installedPluginVersions[] |
      "       \(.pluginId) \(
        if .pluginSoftwareVersion then
          ((.pluginSoftwareVersion.major // 0 | tostring) + "." +
           (.pluginSoftwareVersion.minor // 0 | tostring) + "." +
           (.pluginSoftwareVersion.patch // 0 | tostring))
        else "?"
        end
      )"
    ')"
  else
    plugin_list=""
  fi

  # TSS data presence — check ledger_id field (non-empty bytes)
  local tss_status
  local ledger_id
  ledger_id="$(echo "$json" | jq -r '.tssData.ledgerId // ""')"
  if [[ -n "$ledger_id" && "$ledger_id" != "null" && "$ledger_id" != "" ]]; then
    tss_status="present"
  else
    tss_status="not configured"
  fi

  print_field "BN version"            "$bn_ver"
  print_field "stream_proto_version"  "$stream_ver"
  print_field "available_ranges"      "${range_count} range(s)"

  if [[ -n "$range_summary" ]]; then
    echo "$range_summary"
  fi

  print_field "installed_plugins"     "${plugin_count} registered"
  if [[ -n "$plugin_list" ]]; then
    echo "$plugin_list"
  fi

  print_field "tss_data"             "$tss_status"
}

# ── Per-endpoint check ────────────────────────────────────────────────────────

# Returns 0 if TCP + serverStatus both pass; 1 otherwise.
# serverStatusDetail failure is reported as a warning (non-fatal).
check_endpoint() {
  local endpoint="$1"
  local host port
  host="${endpoint%:*}"
  port="${endpoint##*:}"

  echo ""
  printf "%b\n" "${C_BOLD}──────────────────────────────────────────────────────────────${C_RESET}"
  printf "%b\n" "${C_BOLD}  ${endpoint}${C_RESET}"
  printf "%b\n" "${C_BOLD}──────────────────────────────────────────────────────────────${C_RESET}"

  # 1. TCP reachability ───────────────────────────────────────────────────────
  if tcp_check "$host" "$port"; then
    log_success "  🟢 TCP reachable"
  else
    log_err "  🔴 TCP FAIL — cannot reach ${host}:${port}"
    return 1
  fi

  # 2. serverStatus ──────────────────────────────────────────────────────────
  # Reports: first_available_block, last_available_block, only_latest_state
  local status_json
  if status_json="$(grpc_call "$endpoint" "serverStatus" 2>&1)"; then
    log_success "  🟢 serverStatus"
    print_server_status "$status_json"
  else
    log_err "  🔴 serverStatus FAILED"
    echo "$status_json" | sed 's/^/     /'
    return 1
  fi

  # 3. serverStatusDetail ────────────────────────────────────────────────────
  # Reports: BN version, stream proto version, block ranges, plugins, TSS.
  # Failure here is treated as a warning — some nodes may serve serverStatus
  # only (e.g. behind an HTTP/1.1 proxy that does not support streaming RPCs).
  local detail_json
  if detail_json="$(grpc_call "$endpoint" "serverStatusDetail" 2>&1)"; then
    log_success "  📦 serverStatusDetail"
    print_server_status_detail "$detail_json"
  else
    log_warn "  🟠 serverStatusDetail WARN (method unavailable or proxy limitation)"
    echo "$detail_json" | sed 's/^/     /'
  fi

  return 0
}

# ── Main ──────────────────────────────────────────────────────────────────────

main() {
  # Check hard dependencies (nc, jq) before doing any network work.
  check_nc
  check_jq

  # Check / install grpcurl.
  check_grpcurl

  # Ensure the proto directory is present (download if needed).
  ensure_proto_dir

  local overall_fail=0
  local tls_label
  tls_label="$( [[ "$USE_TLS" == "true" ]] && echo "TLS" || echo "plaintext" )"

  echo ""
  log_info "Checking ${#ENDPOINTS[@]} endpoint(s)"
  log_info "  Proto dir : ${RESOLVED_PROTO_DIR}"
  log_info "  Transport : ${tls_label}"

  for ep in "${ENDPOINTS[@]}"; do
    if ! check_endpoint "$ep"; then
      overall_fail=1
    fi
  done

  echo ""
  printf "%b\n" "${C_BOLD}══════════════════════════════════════════════════════════════${C_RESET}"
  if [[ "$overall_fail" -eq 0 ]]; then
    log_success "  ✅  All ${#ENDPOINTS[@]} endpoint(s) passed"
  else
    log_err     "  ❌  One or more endpoints failed — see output above"
  fi
  printf "%b\n" "${C_BOLD}══════════════════════════════════════════════════════════════${C_RESET}"
  echo ""

  exit "$overall_fail"
}

main
