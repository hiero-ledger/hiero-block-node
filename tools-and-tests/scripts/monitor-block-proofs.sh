#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Finds the Schnorr → WRAPS signature transition in block proofs.
#
# Scans blocks every 50, waits if the network hasn't produced them yet,
# and binary-searches for the exact transition block once WRAPS is detected.
#
# Proof sizes (TssSignedBlockProof.block_signature):
#   Schnorr: ~2,920 bytes (pre-settled TSS)
#   WRAPS:   ~3,432 bytes (post-settled TSS)
#
# Usage:
#   ./monitor-block-proofs.sh <proto-path> [grpc-endpoint] [max-block]
#
# Arguments:
#   proto-path      Path to extracted protobuf files (required)
#   grpc-endpoint   Block Node gRPC endpoint (default: localhost:40840)
#   max-block       Stop and fail if this block is reached without WRAPS
#                   (default: 0 = no limit, wait forever)
#
# Exit codes:
#   0  WRAPS transition found
#   1  max-block reached without WRAPS, or error

set -euo pipefail

PROTO_DIR="${1:?Usage: $0 <proto-path> [grpc-endpoint] [max-block]}"
BN_ENDPOINT="${2:-localhost:40840}"
MAX_BLOCK="${3:-0}"

STEP=50

if ! command -v grpcurl &>/dev/null; then
  echo "ERROR: grpcurl not found" >&2
  exit 1
fi
if [[ ! -d "${PROTO_DIR}" ]]; then
  echo "ERROR: Proto path not found: ${PROTO_DIR}" >&2
  exit 1
fi

# Returns "<TYPE> <sig_bytes> <block_size>"
# e.g. "SCHNORR 2920 95432", "WRAPS 3432 94521320", "NOT_AVAILABLE 0 0"
# block_size is the approximate binary protobuf size (sum of base64-decoded fields)
function get_sig_type {
  local block_number="$1"
  local raw
  raw=$(cd "${PROTO_DIR}" && grpcurl -plaintext -import-path . \
    -proto "block-node/api/block_access_service.proto" \
    -max-msg-sz 150000000 \
    -d "{\"block_number\": ${block_number}}" \
    "${BN_ENDPOINT}" org.hiero.block.api.BlockAccessService/getBlock 2>&1) || true

  if echo "${raw}" | grep -q '"NOT_FOUND"\|"NOT_AVAILABLE"'; then
    echo "NOT_AVAILABLE 0 0"
    return
  fi

  # If grpcurl returned an error (not valid JSON), report as NOT_AVAILABLE
  if ! echo "${raw}" | python3 -c "import sys,json; json.loads(sys.stdin.read())" 2>/dev/null; then
    echo "NOT_AVAILABLE 0 0"
    return
  fi

  local parsed
  parsed=$(echo "${raw}" | python3 -c "
import sys, json, base64, re
raw_text = sys.stdin.read()
d = json.loads(raw_text)
# Estimate binary protobuf size: decode all base64 values, sum their lengths
# plus a small overhead per field for protobuf framing
b64_pattern = re.compile(r'\"([A-Za-z0-9+/]{4,}={0,2})\"')
binary_size = 0
for match in b64_pattern.finditer(raw_text):
    try:
        binary_size += len(base64.b64decode(match.group(1)))
    except Exception:
        pass
# Add ~10% for protobuf field tags, varints, and non-binary fields
binary_size = int(binary_size * 1.1)
sig_bytes = 0
for item in d.get('block',{}).get('items',[]):
    if 'blockProof' in item:
        bp = item['blockProof']
        sbp = bp.get('signedBlockProof',{})
        if sbp and sbp.get('blockSignature'):
            sig_bytes = len(base64.b64decode(sbp['blockSignature']))
        elif bp.get('previousBlockRootHash'):
            sig_bytes = len(base64.b64decode(bp['previousBlockRootHash']))
        break
print(f'{sig_bytes} {binary_size}')
" 2>/dev/null) || parsed="0 0"

  local sig_bytes block_size
  sig_bytes="${parsed%% *}"
  block_size="${parsed##* }"

  local sig_type
  case "${sig_bytes}" in
    292[0-9]) sig_type="SCHNORR" ;;
    343[0-9]) sig_type="WRAPS" ;;
    *)        sig_type="UNKNOWN" ;;
  esac

  echo "${sig_type} ${sig_bytes} ${block_size}"
}

function format_size {
  local bytes="$1"
  if [[ "$bytes" -ge 1048576 ]]; then
    printf "%.1fMB" "$(echo "$bytes" | awk '{printf "%.1f", $1/1048576}')"
  elif [[ "$bytes" -ge 1024 ]]; then
    printf "%.1fKB" "$(echo "$bytes" | awk '{printf "%.1f", $1/1024}')"
  else
    echo "${bytes}B"
  fi
}

function binary_search_transition {
  local low="$1"
  local high="$2"

  while [[ $((high - low)) -gt 1 ]]; do
    local mid=$(( (low + high) / 2 ))
    local result
    result=$(get_sig_type "$mid")
    local sig_type="${result%% *}"
    if [[ "$sig_type" == "SCHNORR" ]]; then
      low=$mid
    else
      # WRAPS, UNKNOWN, or NOT_AVAILABLE — treat conservatively as potential WRAPS
      # (the ~99MB genesis WRAPS block may fail to fetch on resource-constrained CI)
      high=$mid
    fi
  done

  echo "$high"
}

function find_schnorr_lower_bound {
  local wraps_block="$1"
  local low=$(( wraps_block > 100 ? wraps_block - 100 : 0 ))

  while [[ $low -gt 0 ]]; do
    local result
    result=$(get_sig_type "$low")
    if [[ "${result%% *}" == "SCHNORR" ]]; then
      echo "$low"
      return
    fi
    low=$(( low > 100 ? low - 100 : 0 ))
  done

  echo "$low"
}

function format_block_line {
  local block_number="$1"
  local result="$2"
  local sig_type="${result%% *}"

  if [[ "$sig_type" == "NOT_AVAILABLE" || "$sig_type" == "UNKNOWN" ]]; then
    echo "  Block ${block_number}: NOT_AVAILABLE (BN unable to serve — likely oversized WRAPS genesis block)"
  else
    local sig_bytes block_size
    sig_bytes=$(echo "$result" | awk '{print $2}')
    block_size=$(echo "$result" | awk '{print $3}')
    echo "  Block ${block_number}: ${sig_type} (sig: ${sig_bytes} bytes, block: $(format_size "$block_size"))"
  fi
}

function print_result {
  local transition_block="$1"

  local pre_result post_result next_result
  pre_result=$(get_sig_type "$((transition_block - 1))")
  post_result=$(get_sig_type "$transition_block")
  next_result=$(get_sig_type "$((transition_block + 1))")

  echo ""
  echo "=== Signature Transition ==="
  echo "  First WRAPS block: ${transition_block}"
  echo "  Last Schnorr block: $((transition_block - 1))"
  format_block_line "$((transition_block - 1))" "$pre_result"
  format_block_line "$transition_block" "$post_result"
  format_block_line "$((transition_block + 1))" "$next_result"
}

# Main: scan every STEP blocks
echo "Scanning for WRAPS signature (every ${STEP} blocks)..."
block=0
while true; do
  block=$(( block + STEP ))

  # Enforce max-block limit
  if [[ "$MAX_BLOCK" -gt 0 && "$block" -gt "$MAX_BLOCK" ]]; then
    echo ""
    echo "=== Signature Transition ==="
    echo "  Status: WRAPS NOT DETECTED"
    echo "  Max block limit: ${MAX_BLOCK}"
    exit 1
  fi

  result=$(get_sig_type "$block")
  sig_type="${result%% *}"
  sig_bytes=$(echo "$result" | awk '{print $2}')
  block_size=$(echo "$result" | awk '{print $3}')

  if [[ "$sig_type" == "NOT_AVAILABLE" ]]; then
    if [[ "${waiting_for:-0}" -ne "$block" ]]; then
      waiting_for=$block
      wait_count=0
      printf "  Block %d: waiting" "$block"
    fi
    (( wait_count++ )) || true
    printf "."
    sleep 5
    block=$(( block - STEP ))
    continue
  fi

  # End the waiting dots line if we were waiting
  if [[ "${waiting_for:-0}" -gt 0 ]]; then
    printf " (%ds)\n" $((wait_count * 5))
    waiting_for=0
  fi

  echo "  Block ${block}: ${sig_type} (sig: ${sig_bytes} bytes, block: $(format_size "$block_size"))"

  if [[ "$sig_type" == "WRAPS" ]]; then
    echo "  WRAPS detected, binary searching for exact transition..."
    low=$(find_schnorr_lower_bound "$block")
    transition=$(binary_search_transition "$low" "$block")
    print_result "$transition"
    exit 0
  fi
done
