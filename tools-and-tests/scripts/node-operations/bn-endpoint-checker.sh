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
#                                    (only when --detailed-server-status is passed)
#   4. Fetching the latest block   → reports the block proof type observed in the
#                                    most recent block: WRB/RSA (Phase 2a),
#                                    TSS hinTS + WRAPS proof, or TSS hinTS +
#                                    Aggregate Schnorr signature (Phase 2b)
#                                    (only when --latest-block-proof is passed)
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
#       --detailed-server-status
#                           Also call serverStatusDetail per endpoint, which
#                           returns BN software version, stream proto version,
#                           available block ranges, registered plugins, and TSS
#                           data presence. Omitted by default because this RPC
#                           can be slow on nodes with many block ranges.
#       --latest-block-proof
#                           Fetch the latest block from the node and report the
#                           proof type observed in it. Useful for confirming that
#                           a node can serve blocks and for tracking the network's
#                           current proof phase:
#                             WRB/RSA            — Phase 2a (SignedRecordFileProof)
#                             TSS WRAPS proof    — Phase 2b, WRAPS-based hinTS
#                             TSS Agg. Schnorr   — Phase 2b, Aggregate Schnorr
#                           Requires the proto package to contain the
#                           block_access_service.proto file (same archive as the
#                           node_service.proto already required).
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
#   # Check latest block proof type (e.g. TSS vs WRB/RSA) on mainnet nodes
#   bn-endpoint-checker.sh --version 0.32.0 --latest-block-proof \
#     node1.mainnet.blocknode.hashgraph-devops.com:40840 \
#     node2.mainnet.blocknode.hashgraph-devops.com:40840
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

# Exit immediately on unhandled error, treat unset variables as errors, and
# propagate failures through pipes (e.g. cmd1 | cmd2 fails if cmd1 fails).
set -euo pipefail

# ── Script-level constants ────────────────────────────────────────────────────

# Resolve the directory that contains this script at runtime so that relative
# paths (proto dirs, local bin/) are stable regardless of the caller's cwd.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# grpcurl version to pin when auto-installing. Bump this when a newer release
# fixes a bug or adds a feature the script depends on.
GRPCURL_VERSION="1.9.1"

# Base URLs for release artifacts (no trailing slash).
PROTO_ARCHIVE_BASE_URL="https://github.com/hiero-ledger/hiero-block-node/releases/download"
GRPCURL_RELEASE_BASE_URL="https://github.com/fullstorydev/grpcurl/releases/download"

# Path to the node service proto file, relative to the proto directory root.
# grpcurl uses this as the entry point and resolves all transitive imports from
# the same root via -import-path.
NODE_SERVICE_PROTO="block-node/api/node_service.proto"

# Fully-qualified gRPC service name as declared in the proto package.
# Used to construct the grpcurl target: <service>/<method>.
GRPC_SERVICE="org.hiero.block.api.BlockNodeService"

# Proto file and service name for the block access service, used by
# --latest-block-proof to call getBlock with retrieve_latest=true.
BLOCK_ACCESS_PROTO="block-node/api/block_access_service.proto"
BLOCK_ACCESS_SERVICE="org.hiero.block.api.BlockAccessService"

# ── TSS block_signature sub-type byte-length constants ───────────────────────
#
# TssSignedBlockProof.block_signature is a flat byte concatenation:
#   hints_verification_key  (1096 B)
#   hints_signature         (1632 B)
#   [wraps_proof | aggregate_schnorr_signature]
#
# The sub-type is identified by the total byte length alone — there is no
# delimiter or header byte between the three components.
#
# These constants come from the hinTS protocol specification. If the network
# upgrades and changes roster size or cryptographic parameters, the totals
# will shift and the "unknown sub-type" branch in jq will surface the new
# length for investigation.
readonly _TSS_HINTS_VK_LEN=1096     # HINTS_VERIFICATION_KEY_LENGTH
readonly _TSS_HINTS_SIG_LEN=1632    # HINTS_SIGNATURE_LENGTH
readonly _TSS_WRAPS_LEN=704         # COMPRESSED_WRAPS_PROOF_LENGTH
readonly _TSS_SCHNORR_LEN=192       # AGGREGATE_SCHNORR_SIGNATURE_LENGTH
# Totals passed as --argjson to jq so the values live in bash, not in the
# jq string, keeping the filter readable and avoiding magic numbers.
readonly TSS_WRAPS_TOTAL=$(( _TSS_HINTS_VK_LEN + _TSS_HINTS_SIG_LEN + _TSS_WRAPS_LEN ))     # 3432
readonly TSS_SCHNORR_TOTAL=$(( _TSS_HINTS_VK_LEN + _TSS_HINTS_SIG_LEN + _TSS_SCHNORR_LEN )) # 2920

# ── ANSI colours (disabled automatically when stdout is not a terminal) ───────
# Check file descriptor 1 (stdout) with -t; when the script is piped or
# redirected the colour codes are suppressed to avoid polluting log files.

if [[ -t 1 ]]; then
  C_GREEN='\033[0;32m'
  C_RED='\033[0;31m'
  C_YELLOW='\033[0;33m'
  C_BLUE='\033[0;34m'
  C_BOLD='\033[1m'
  C_RESET='\033[0m'
else
  # Non-interactive: set all colour variables to empty strings.
  C_GREEN='' C_RED='' C_YELLOW='' C_BLUE='' C_BOLD='' C_RESET=''
fi

# Logging helpers — each wraps printf with the appropriate colour and a newline.
# log_warn and log_err write to stderr so they don't pollute stdout captures.
log_info()    { printf "%b\n" "${C_BLUE}${*}${C_RESET}"; }
log_success() { printf "%b\n" "${C_GREEN}${*}${C_RESET}"; }
log_warn()    { printf "%b\n" "${C_YELLOW}${*}${C_RESET}" >&2; }
log_err()     { printf "%b\n" "${C_RED}${*}${C_RESET}" >&2; }

# ── Usage / help ──────────────────────────────────────────────────────────────

usage() {
  # Extract the inline help text from the comment block at the top of this
  # file. Every line starting with '#' (except the shebang) is printed with
  # the leading '# ' stripped, so the source file is the single source of truth
  # for help text — no separate man page or heredoc needed.
  grep '^#' "$0" | grep -v '^#!/' | sed 's/^# \{0,1\}//'
  exit "${1:-0}"
}

# ── Argument parsing ──────────────────────────────────────────────────────────

BN_VERSION=""            # Requested BN protobuf version for auto-download.
PROTO_DIR_ARG=""         # --proto-dir flag value (overrides PROTO_DIR env var).
USE_TLS=false            # When true, grpcurl uses TLS; default is plaintext.
DETAILED_STATUS=false    # When true, also call serverStatusDetail per endpoint.
LATEST_BLOCK_PROOF=false # When true, fetch the latest block and report proof type.
ENDPOINTS=()             # Positional <host:port> arguments collected here.

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
    --detailed-server-status)
      DETAILED_STATUS=true; shift ;;
    --latest-block-proof)
      LATEST_BLOCK_PROOF=true; shift ;;
    -h|--help)
      usage 0 ;;
    -*)
      log_err "Unknown option: $1"; usage 2 ;;
    *)
      # Everything that is not a recognised flag is treated as an endpoint.
      ENDPOINTS+=("$1"); shift ;;
  esac
done

# ── Input validation ──────────────────────────────────────────────────────────

if [[ ${#ENDPOINTS[@]} -eq 0 ]]; then
  log_err "Error: at least one <host:port> endpoint is required."
  usage 2
fi

# Proto directory resolution order: CLI flag > env var > derived from --version.
# The env var fallback allows operators to export PROTO_DIR once in their shell
# profile without needing to pass --proto-dir on every invocation.
RESOLVED_PROTO_DIR="${PROTO_DIR_ARG:-${PROTO_DIR:-}}"

if [[ -z "$RESOLVED_PROTO_DIR" && -z "$BN_VERSION" ]]; then
  log_err "Error: supply --proto-dir (or PROTO_DIR env var) or --version to download the proto package."
  usage 2
fi

# When only --version is given (no explicit directory path), derive the
# expected extraction path from the script's own directory so repeated runs
# re-use the already-downloaded package without re-downloading.
if [[ -z "$RESOLVED_PROTO_DIR" && -n "$BN_VERSION" ]]; then
  RESOLVED_PROTO_DIR="${SCRIPT_DIR}/block-node-protobuf-${BN_VERSION}"
fi

# Validate that every endpoint looks like host:port before hitting the network.
# Also verifies the port portion is a valid integer in the range 1–65535 so that
# nc and grpcurl receive well-formed arguments and produce useful error messages.
for ep in "${ENDPOINTS[@]}"; do
  local_port="${ep##*:}"
  if [[ "$ep" != *:* ]] \
      || ! [[ "$local_port" =~ ^[0-9]+$ ]] \
      || (( local_port < 1 || local_port > 65535 )); then
    log_err "Error: endpoint '${ep}' must be in <host:port> form with a valid port (1–65535)."
    exit 2
  fi
done

# ── Dependency checks ─────────────────────────────────────────────────────────

# Verify nc (netcat) is present. nc is used for the fast TCP reachability probe
# before we attempt a full gRPC call. On most Linux distros it ships with
# netcat-openbsd; on macOS it is part of the base system.
check_nc() {
  if ! command -v nc &>/dev/null; then
    log_err "Error: 'nc' (netcat) is required for TCP reachability checks but was not found."
    log_err "Install it via your package manager:"
    log_err "  Debian/Ubuntu : apt install netcat-openbsd"
    log_err "  macOS         : nc is included in the base system"
    exit 2
  fi
}

# Verify jq is present. jq is used to extract and format fields from the JSON
# responses returned by grpcurl.
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

# Maps the current OS and CPU architecture to the platform string used in
# grpcurl GitHub release archive names, e.g. "linux_x86_64" or "osx_arm64".
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

# Interactively downloads and installs grpcurl when it is not already on PATH.
# Offers a local install (no sudo, scoped to this script's bin/ directory) or
# a system-wide install (/usr/local/bin, requires sudo).
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
      # Install into a bin/ directory alongside this script. This directory is
      # automatically prepended to PATH by check_grpcurl on future runs, so the
      # user will not be prompted again after the first install.
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
      # Download to a temp directory first, then move into /usr/local/bin with
      # sudo so the curl pipe never runs as root.
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

# Verifies grpcurl is available, prepending the script-local bin/ directory to
# PATH first so a previously auto-installed binary is found without the user
# needing to update their shell profile.
check_grpcurl() {
  local local_bin="${SCRIPT_DIR}/bin"
  if [[ -x "${local_bin}/grpcurl" ]]; then
    export PATH="${local_bin}:${PATH}"
  fi

  if ! command -v grpcurl &>/dev/null; then
    install_grpcurl
  fi
}

# ── Proto package: download and extract ───────────────────────────────────────

# Ensures RESOLVED_PROTO_DIR exists and contains the expected node_service.proto
# file. If the directory is absent and --version was supplied, downloads the
# release archive from GitHub and extracts it in place.
ensure_proto_dir() {
  if [[ -d "$RESOLVED_PROTO_DIR" ]]; then
    log_info "Proto directory : ${RESOLVED_PROTO_DIR}"
    # Verify the key entry-point file is present so we fail fast with a clear
    # message rather than a cryptic grpcurl error later.
    if [[ ! -f "${RESOLVED_PROTO_DIR}/${NODE_SERVICE_PROTO}" ]]; then
      log_err "Error: proto directory exists but is missing the expected file:"
      log_err "  ${RESOLVED_PROTO_DIR}/${NODE_SERVICE_PROTO}"
      log_err "Verify the directory contents or re-download with --version."
      exit 2
    fi
    return 0
  fi

  # Directory does not exist — download is required, but only if a version was
  # specified. Without --version we have no URL to derive.
  if [[ -z "$BN_VERSION" ]]; then
    log_err "Error: proto directory not found: ${RESOLVED_PROTO_DIR}"
    log_err "Provide --version to download it automatically."
    exit 2
  fi

  local archive="block-node-protobuf-${BN_VERSION}.tgz"
  local url="${PROTO_ARCHIVE_BASE_URL}/v${BN_VERSION}/${archive}"
  # Use mktemp so the archive lands in the system temp directory rather than
  # next to this script, and so a partial download is never mistaken for a
  # complete file on the next run. The file is cleaned up in every exit path.
  local tmp_archive
  tmp_archive="$(mktemp "${TMPDIR:-/tmp}/bn-proto-XXXXXX.tgz")"

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
  if ! tar -xzf "$tmp_archive" -C "$RESOLVED_PROTO_DIR"; then
    log_err "Error: failed to extract proto archive."
    rm -f "$tmp_archive"
    exit 2
  fi
  rm -f "$tmp_archive"

  # Final integrity check: confirm the archive contained the expected file.
  if [[ ! -f "${RESOLVED_PROTO_DIR}/${NODE_SERVICE_PROTO}" ]]; then
    log_err "Error: archive extracted but expected proto file is missing:"
    log_err "  ${RESOLVED_PROTO_DIR}/${NODE_SERVICE_PROTO}"
    exit 2
  fi

  log_success "Proto package ready : ${RESOLVED_PROTO_DIR}"
}

# ── gRPC call helper ──────────────────────────────────────────────────────────

# Invokes grpcurl for the given target endpoint and RPC method name.
#
# Key grpcurl flags used:
#   -emit-defaults   Include proto3 fields that hold their default (zero) value
#                    in the JSON output. Without this, fields like block=0 are
#                    silently omitted, making the output ambiguous.
#   -import-path     Root directory from which proto imports are resolved.
#   -proto           Entry-point proto file (relative to -import-path).
#   -d '{}'          Send an empty request body (all fields at default).
#   -plaintext       Disable TLS. grpcurl uses TLS by default; this flag opts
#                    out for nodes that do not terminate TLS themselves.
#
# stdout and stderr are both captured by the caller via $() so that error
# messages from grpcurl are available for display on failure.
grpc_call() {
  local target="$1" method="$2"
  local -a flags=(
    -emit-defaults
    -import-path "${RESOLVED_PROTO_DIR}"
    -proto        "${NODE_SERVICE_PROTO}"
    -d            '{}'
  )
  # Prepend -plaintext when TLS is disabled (the common case for internal nodes).
  [[ "$USE_TLS" == "false" ]] && flags=("-plaintext" "${flags[@]}")

  grpcurl "${flags[@]}" "${target}" "${GRPC_SERVICE}/${method}"
}

# ── Block proof gRPC call ─────────────────────────────────────────────────────

# Calls BlockAccessService/getBlock with retrieve_latest=true and returns the
# raw JSON response on stdout.
#
# Important: -emit-defaults is deliberately NOT used here. For proto3 oneof
# fields, emitting defaults causes grpcurl to output all three proof variants
# with empty/zero values, which breaks the has("fieldName") discriminator used
# in print_block_proof. Without -emit-defaults, only the field that is actually
# set in the oneof appears in the JSON output.
grpc_call_block_proof() {
  local target="$1"
  local -a flags=(
    -import-path "${RESOLVED_PROTO_DIR}"
    -proto        "${BLOCK_ACCESS_PROTO}"
    -d            '{"retrieve_latest": true}'
  )
  [[ "$USE_TLS" == "false" ]] && flags=("-plaintext" "${flags[@]}")

  grpcurl "${flags[@]}" "${target}" "${BLOCK_ACCESS_SERVICE}/getBlock"
}

# Parses a getBlock JSON response and prints the block number and proof type.
#
# Proof type detection logic:
#   BlockProof carries a oneof proof field with three possible variants:
#     signedRecordFileProof  → WRB / RSA (Phase 2a)
#     signedBlockProof       → TSS (Phase 2b) — sub-type determined by byte length
#     blockStateProof        → State proof
#
#   For TSS blocks, TssSignedBlockProof.blockSignature is a flat byte sequence:
#     hints_verification_key (1096 B) || hints_signature (1632 B) || sub-type payload
#   The sub-type payload is either:
#     wraps_proof (704 B)         → total 3432 B  (TSS_WRAPS_TOTAL)
#     aggregate_schnorr (192 B)   → total 2920 B  (TSS_SCHNORR_TOTAL)
#
#   grpcurl encodes bytes fields as standard base64 strings. The byte count is
#   recovered without decoding:
#     byte_length = floor( len(base64_without_padding) × 3 / 4 )
print_block_proof() {
  local json="$1"

  echo "$json" | jq -r \
    --argjson wraps   "${TSS_WRAPS_TOTAL}" \
    --argjson schnorr "${TSS_SCHNORR_TOTAL}" \
    '
      # Extract block number from the blockHeader item (if present).
      (.block.items[]? | select(has("blockHeader")) | .blockHeader.number // "?") as $num |

      # Find the blockProof item. Use an array + [0] so we get null rather than
      # an error when no proof item exists (e.g. a partial/streaming response).
      ([ .block.items[]? | select(has("blockProof")) | .blockProof ][0]) as $p |

      if $p == null then
        "block \($num): NO PROOF ITEM — block may be incomplete"

      elif ($p | has("signedRecordFileProof")) then
        # WRB / RSA Phase 2a: SignedRecordFileProof carries per-node RSA signatures.
        ( $p.signedRecordFileProof |
          "block \($num): WRB/RSA  — \(.recordFileSignatures | length) node signature(s)"
        )

      elif ($p | has("signedBlockProof")) then
        # TSS Phase 2b: Determine sub-type from blockSignature byte length.
        # Strip base64 padding ("=" chars) before computing length so that
        # padded and unpadded encodings give the same result.
        ( $p.signedBlockProof.blockSignature // ""
          | gsub("=+$"; "") | length * 3 / 4 | floor ) as $n |
        if   $n == $wraps   then
          "block \($num): TSS — hinTS + WRAPS proof  (\($n) B)"
        elif $n == $schnorr then
          "block \($num): TSS — hinTS + Aggregate Schnorr  (\($n) B)"
        else
          "block \($num): TSS — unknown sub-type  (\($n) B — check hinTS constants)"
        end

      elif ($p | has("blockStateProof")) then
        "block \($num): STATE PROOF"

      else
        "block \($num): UNKNOWN proof type — keys: \($p | keys)"
      end
    '
}

# ── Timing helpers ────────────────────────────────────────────────────────────

# Returns the current wall-clock time in milliseconds since the Unix epoch.
#
# Why not just use `date +%s%3N` everywhere?
# GNU date (Linux) supports the %3N nanoseconds-truncated-to-milliseconds
# format specifier. BSD date (macOS) does not — it treats %3N as a literal
# "3N" and outputs a string ending in "N". We detect that case and use the
# first available alternative in priority order:
#   1. gdate (GNU coreutils, installed via `brew install coreutils` on macOS —
#      the same package that provides gtimeout used in tcp_check).
#   2. perl  (ships on all macOS versions without additional tooling; starts
#      faster than python3 and Time::HiRes is always present).
#   3. python3 (last resort; available on modern macOS but slower to start).
now_ms() {
  local t
  t="$(date +%s%3N 2>/dev/null)"
  if [[ "$t" =~ N$ ]]; then
    # BSD date detected (macOS). Use the fastest available alternative.
    if command -v gdate &>/dev/null; then
      gdate +%s%3N
    elif command -v perl &>/dev/null; then
      perl -MTime::HiRes -e 'printf "%d\n", Time::HiRes::time() * 1000'
    else
      python3 -c "import time; print(int(time.time() * 1000))"
    fi
  else
    echo "$t"
  fi
}

# Converts a raw millisecond count into a human-readable string.
# Values under one second are shown as "NNN ms"; values of one second or more
# are shown as "N.N s" with one decimal place.
# awk is used instead of bc for the floating-point division because bc is not
# available on all systems (e.g. minimal Docker images), while awk is POSIX.
format_elapsed_ms() {
  local ms="$1"
  if (( ms < 1000 )); then
    echo "${ms} ms"
  else
    awk -v ms="$ms" 'BEGIN { printf "%.1f s\n", ms / 1000 }'
  fi
}

# ── TCP reachability check ────────────────────────────────────────────────────

# Probes whether host:port is accepting TCP connections.
#
# What nc -z does:
#   -z   "Zero I/O mode" — opens a TCP socket and immediately closes it without
#        sending any data. It simply confirms the port is open and listening.
#        This is much faster than a full gRPC handshake and gives an early
#        signal when a node is completely unreachable (wrong IP, firewall, etc.).
#
# The -w 3 timeout problem:
#   nc's -w flag is documented as a timeout in seconds, but its behaviour
#   differs across implementations (BSD nc on macOS vs netcat-openbsd on Linux
#   vs netcat-traditional). Crucially, when a firewall silently DROPs packets
#   (no TCP RST is sent back), -w may not cut off the connection attempt —
#   instead the OS-level TCP SYN retransmission backoff runs to completion,
#   which can take 75–127 seconds depending on the kernel's tcp_syn_retries
#   setting. This has been observed causing 75-second hangs on unreachable nodes.
#
# The fix — wrapping with `timeout`:
#   The shell's `timeout` command sends SIGALRM to the child process after the
#   specified interval regardless of what the process is doing, providing a
#   reliable hard deadline. We prefer `timeout` (GNU coreutils, present on
#   Linux by default) and fall back to `gtimeout` (the same tool installed by
#   `brew install coreutils` on macOS). If neither is available we fall back to
#   plain nc with -w 3, accepting the risk of the longer hang.
#
# The -w 3 is kept even when timeout is available as belt-and-suspenders: if
# the connection is actively refused (RST received), nc exits immediately and
# the timeout wrapper adds no overhead.
tcp_check() {
  local host="$1" port="$2"

  # Resolve whichever timeout command is available on this system.
  # An array is used rather than a string so word-splitting is never needed and
  # the empty-array case ("${timeout_cmd[@]}") expands to nothing cleanly.
  local -a timeout_cmd=()
  if   command -v timeout  &>/dev/null; then timeout_cmd=(timeout  3)
  elif command -v gtimeout &>/dev/null; then timeout_cmd=(gtimeout 3)
  fi
  # Note: if neither is found, timeout_cmd remains empty and we rely on -w 3
  # alone. This is safe for reachable nodes; unreachable DROP-firewalled nodes
  # may still hang up to the OS TCP timeout in that case.

  # "${timeout_cmd[@]+...}" expands to the array elements when the array is
  # non-empty and to nothing when it is empty, safely handling the set -u case
  # where a bare "${timeout_cmd[@]}" on an empty array is treated as unbound.
  "${timeout_cmd[@]+"${timeout_cmd[@]}"}" nc -z -w 3 "$host" "$port" &>/dev/null
}

# ── Output formatting ─────────────────────────────────────────────────────────

# Prints a key/value pair indented by 5 spaces with the key left-padded to a
# fixed width, producing a consistent column-aligned table.
print_field() {
  local key="$1" value="$2"
  printf "     %-30s %s\n" "${key}:" "$value"
}

# Parses a SemanticVersion protobuf JSON object (major/minor/patch/pre/build)
# and formats it as the standard "major.minor.patch[-pre][+build]" string.
# Returns "unknown" when the input is null or an empty object — this happens
# when the server is running a version of BN that does not yet populate the
# field.
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

# Extracts and prints the key fields from a ServerStatusResponse JSON object.
# Fields:
#   firstAvailableBlock  — lowest block number the node can serve.
#   lastAvailableBlock   — highest block number currently stored.
#   onlyLatestState      — true when the node retains only the most recent block
#                          and does not serve historical blocks.
print_server_status() {
  local json="$1"
  local first last only_latest
  # Single jq invocation extracts all three fields as tab-separated values,
  # avoiding two extra subshells compared to calling jq once per field.
  IFS=$'\t' read -r first last only_latest < <(echo "$json" | jq -r '
    [
      (.firstAvailableBlock // "0" | tostring),
      (.lastAvailableBlock  // "0" | tostring),
      (.onlyLatestState     // false | tostring)
    ] | join("\t")
  ')

  print_field "first_available_block" "$first"
  print_field "last_available_block"  "$last"
  print_field "only_latest_state"     "$only_latest"
}

# Extracts and prints the extended fields from a ServerStatusDetailResponse.
# Fields:
#   BN version             — semantic version of the Block Node software.
#   stream_proto_version   — version of the block stream protobuf schema the
#                            node is currently using.
#   available_ranges       — number of contiguous block ranges the node holds,
#                            with each range's start and end block printed below.
#   installed_plugins      — plugins registered in the node, with their versions.
#   tss_data               — whether TSS (Threshold Signature Scheme) ledger
#                            configuration data is present on this node.
print_server_status_detail() {
  local json="$1"

  # ── Extract all scalar fields in a single jq pass ─────────────────────────
  # Inlines the semver formatter as a jq function (identical logic to
  # format_semver) to avoid spawning separate subshells for each version string.
  # Outputs five tab-separated fields so one read assigns all variables at once,
  # reducing five separate jq invocations to one.
  local bn_ver stream_ver range_count plugin_count ledger_id
  IFS=$'\t' read -r bn_ver stream_ver range_count plugin_count ledger_id < <(
    echo "$json" | jq -r '
      def semver:
        if . == null or . == {} then "unknown"
        else
          ((.major // 0 | tostring) + "." +
           (.minor // 0 | tostring) + "." +
           (.patch // 0 | tostring)) +
          (if (.pre   // "") != "" then "-" + .pre   else "" end) +
          (if (.build // "") != "" then "+" + .build else "" end)
        end;
      [
        (.versionInformation.blockNodeVersion   // {} | semver),
        (.versionInformation.streamProtoVersion // {} | semver),
        (.availableRanges | length | tostring),
        (.versionInformation.installedPluginVersions | length | tostring),
        (.tssData.ledgerId // "")
      ] | join("\t")
    '
  )

  # ── Available block ranges ────────────────────────────────────────────────
  # A node may hold multiple non-contiguous ranges if it was offline during
  # parts of the chain history (gaps). Each range has rangeStart and rangeEnd.
  local range_summary=""
  if [[ "$range_count" -gt 0 ]]; then
    range_summary="$(echo "$json" | jq -r '
      .availableRanges |
      to_entries |
      map("       range \(.key + 1): blocks \(.value.rangeStart) – \(.value.rangeEnd)") |
      join("\n")
    ')"
  fi

  # ── Installed plugins ─────────────────────────────────────────────────────
  # Plugins extend the node with capabilities such as cloud storage, metrics
  # exporters, etc. Each entry has a pluginId and an optional pluginSoftwareVersion.
  local plugin_list=""
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
  fi

  # ── TSS data presence ─────────────────────────────────────────────────────
  # ledgerId is a bytes field; grpcurl encodes it as a base64 string when
  # non-empty. jq -r with // "" never returns the string "null" for an absent
  # field, so a plain non-empty test is sufficient.
  local tss_status
  if [[ -n "$ledger_id" ]]; then
    tss_status="present"
  else
    tss_status="not configured"
  fi

  # ── Print all collected fields ────────────────────────────────────────────
  print_field "BN version"            "$bn_ver"
  print_field "stream_proto_version"  "$stream_ver"
  print_field "available_ranges"      "${range_count} range(s)"
  [[ -n "$range_summary" ]] && echo "$range_summary"

  print_field "installed_plugins"     "${plugin_count} registered"
  [[ -n "$plugin_list" ]] && echo "$plugin_list"

  print_field "tss_data"             "$tss_status"
}

# ── Per-endpoint check ────────────────────────────────────────────────────────

# Runs the full check sequence for a single endpoint: TCP probe → serverStatus
# gRPC call → (optional) serverStatusDetail gRPC call.
#
# Returns 0 if TCP and serverStatus both succeed; 1 if either fails.
# serverStatusDetail failure is non-fatal (reported as a warning) because some
# nodes sit behind HTTP/1.1 proxies that do not support streaming RPCs.
#
# Each step is individually timed and the per-endpoint total is printed at the
# end. Elapsed times help operators identify slow or high-latency nodes at a
# glance without needing a separate profiling tool.
check_endpoint() {
  local endpoint="$1"
  local host port
  # Split "host:port" — %:* strips from the last colon rightward (host),
  # ##*: strips everything up to and including the last colon (port).
  host="${endpoint%:*}"
  port="${endpoint##*:}"

  local ep_start_ms t0 elapsed
  ep_start_ms="$(now_ms)"

  echo ""
  printf "%b\n" "${C_BOLD}──────────────────────────────────────────────────────────────${C_RESET}"
  printf "%b\n" "${C_BOLD}  ${endpoint}${C_RESET}"
  printf "%b\n" "${C_BOLD}──────────────────────────────────────────────────────────────${C_RESET}"

  # 1. TCP reachability ───────────────────────────────────────────────────────
  # A fast port-open check before attempting a full gRPC handshake. If TCP
  # fails we skip the gRPC calls entirely — there is no point waiting for them
  # when the node is not even network-reachable.
  t0="$(now_ms)"
  if tcp_check "$host" "$port"; then
    elapsed="$(format_elapsed_ms "$(( $(now_ms) - t0 ))")"
    log_success "  🟢 TCP reachable  (${elapsed})"
  else
    elapsed="$(format_elapsed_ms "$(( $(now_ms) - t0 ))")"
    log_err "  🔴 TCP FAIL — cannot reach ${host}:${port}  (${elapsed})"
    return 1
  fi

  # 2. serverStatus ──────────────────────────────────────────────────────────
  # The primary health signal. A successful response confirms the node's gRPC
  # stack is up and it is actively serving blocks. The response includes the
  # first and last available block numbers and the only_latest_state flag.
  local status_json
  t0="$(now_ms)"
  if status_json="$(grpc_call "$endpoint" "serverStatus" 2>&1)"; then
    elapsed="$(format_elapsed_ms "$(( $(now_ms) - t0 ))")"
    log_success "  🟢 serverStatus  (${elapsed})"
    print_server_status "$status_json"
  else
    elapsed="$(format_elapsed_ms "$(( $(now_ms) - t0 ))")"
    log_err "  🔴 serverStatus FAILED  (${elapsed})"
    echo "$status_json" | sed 's/^/     /'
    return 1
  fi

  # 3. serverStatusDetail (opt-in via --detailed-server-status) ──────────────
  # Returns richer metadata: software version, stream proto version, available
  # block ranges, installed plugins, and TSS configuration presence. Omitted by
  # default because this RPC can be noticeably slower on nodes that hold many
  # block ranges (the response payload is larger and server-side assembly takes
  # more time). Pass --detailed-server-status when version or plugin information
  # is needed.
  if [[ "$DETAILED_STATUS" == "true" ]]; then
    local detail_json
    t0="$(now_ms)"
    if detail_json="$(grpc_call "$endpoint" "serverStatusDetail" 2>&1)"; then
      elapsed="$(format_elapsed_ms "$(( $(now_ms) - t0 ))")"
      log_success "  📦 serverStatusDetail  (${elapsed})"
      print_server_status_detail "$detail_json"
    else
      elapsed="$(format_elapsed_ms "$(( $(now_ms) - t0 ))")"
      log_warn "  🟠 serverStatusDetail WARN (method unavailable or proxy limitation)  (${elapsed})"
      echo "$detail_json" | sed 's/^/     /'
    fi
  fi

  # 4. Latest block proof (opt-in via --latest-block-proof) ────────────────────
  # Fetches the most recent block from the node and reports the proof type.
  # This confirms two things at once:
  #   a) The node can serve block data via BlockAccessService/getBlock.
  #   b) The current network proof phase (WRB/RSA vs TSS WRAPS vs TSS Schnorr).
  #
  # Failure is non-fatal: a node may have serverStatus working before it has
  # stored any blocks (e.g. freshly started or syncing). The warning is still
  # printed so the operator knows the block fetch was attempted.
  if [[ "$LATEST_BLOCK_PROOF" == "true" ]]; then
    local proof_json proof_line
    t0="$(now_ms)"
    if proof_json="$(grpc_call_block_proof "$endpoint" 2>&1)"; then
      elapsed="$(format_elapsed_ms "$(( $(now_ms) - t0 ))")"
      proof_line="$(print_block_proof "$proof_json")"
      log_success "  🔏 latest block proof  (${elapsed})"
      print_field "proof_type" "$proof_line"
    else
      elapsed="$(format_elapsed_ms "$(( $(now_ms) - t0 ))")"
      log_warn "  🟠 latest block proof WARN (getBlock unavailable or no blocks stored)  (${elapsed})"
      echo "$proof_json" | sed 's/^/     /'
    fi
  fi

  # Print the wall-clock time spent on this endpoint in total (TCP + all gRPC
  # calls). Useful for spotting outliers when checking many nodes at once.
  elapsed="$(format_elapsed_ms "$(( $(now_ms) - ep_start_ms ))")"
  printf "     %b\n" "${C_BLUE}endpoint total: ${elapsed}${C_RESET}"

  return 0
}

# ── Main ──────────────────────────────────────────────────────────────────────

main() {
  # Record the script start time before any setup work so the grand total
  # includes dependency checks and proto downloads, not just the network probes.
  local SCRIPT_START_MS
  SCRIPT_START_MS="$(now_ms)"

  # Verify hard dependencies before touching the network. Fail fast with clear
  # install instructions rather than cryptic errors mid-run.
  check_nc
  check_jq
  check_grpcurl

  # Ensure the proto directory is present, downloading it if needed.
  ensure_proto_dir

  local overall_fail=0

  # Build human-readable labels for the run summary header.
  # Explicit if/else avoids the &&/|| ternary idiom, which is fragile: if the
  # true-branch command ever fails, the false-branch fires incorrectly.
  local tls_label detail_label proof_label
  if [[ "$USE_TLS"             == "true" ]]; then tls_label="TLS";     else tls_label="plaintext"; fi
  if [[ "$DETAILED_STATUS"     == "true" ]]; then detail_label="enabled"; else detail_label="disabled (pass --detailed-server-status to enable)"; fi
  if [[ "$LATEST_BLOCK_PROOF"  == "true" ]]; then proof_label="enabled"; else proof_label="disabled (pass --latest-block-proof to enable)"; fi

  echo ""
  log_info "Checking ${#ENDPOINTS[@]} endpoint(s)"
  log_info "  Proto dir       : ${RESOLVED_PROTO_DIR}"
  log_info "  Transport       : ${tls_label}"
  log_info "  Detailed status : ${detail_label}"
  log_info "  Block proof     : ${proof_label}"

  # Iterate over every supplied endpoint. Failures are accumulated rather than
  # stopping immediately so the operator gets a complete picture of all nodes in
  # a single run.
  for ep in "${ENDPOINTS[@]}"; do
    if ! check_endpoint "$ep"; then
      overall_fail=1
    fi
  done

  # Compute the total elapsed time for the entire script run.
  local total_elapsed
  total_elapsed="$(format_elapsed_ms "$(( $(now_ms) - SCRIPT_START_MS ))")"

  echo ""
  printf "%b\n" "${C_BOLD}══════════════════════════════════════════════════════════════${C_RESET}"
  if [[ "$overall_fail" -eq 0 ]]; then
    log_success "  ✅  All ${#ENDPOINTS[@]} endpoint(s) passed"
  else
    log_err     "  ❌  One or more endpoints failed — see output above"
  fi
  log_info      "  ⏱  Total time: ${total_elapsed}"
  printf "%b\n" "${C_BOLD}══════════════════════════════════════════════════════════════${C_RESET}"
  echo ""

  exit "$overall_fail"
}

main
