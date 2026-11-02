#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Checks the TSS block proofs a Block Node serves, for Consensus Nodes on either TSS
# library era.
#
# Scans blocks every STEP, waits if the network hasn't produced them yet, and classifies
# each signed TSS block proof by the size of its signature
# (TssSignedBlockProof.block_signature):
#
#   NOVA     13,712 bytes            Nova WRAPS library: one layout on every signed block,
#                                    block 0 included
#   SCHNORR  2,920 to 2,929 bytes    older library, before the TSS keys settle
#   WRAPS    3,430 to 3,439 bytes    older library, after the TSS keys settle
#   UNKNOWN  any other size
#
# Blocks without a TSS signature (block state proofs, signed record file proofs) are
# skipped. The first signed proof sampled decides the era:
#
#   older library  passes once the Schnorr then WRAPS transition is found; the exact
#                  transition block is then narrowed with a binary search.
#   Nova library   passes once MONITOR_MIN_NOVA_SAMPLES sampled blocks all carry the Nova
#                  layout; there is no transition to wait for.
#
# A mix of the Nova layout and an older one, or a signature of any other size, fails at
# once. Reaching max-block before either pass condition holds fails too.
#
# Usage:
#   ./monitor-block-proofs.sh <proto-path> [grpc-endpoint] [max-block] [port-forward-cmd]
#
# Arguments:
#   proto-path        Path to extracted protobuf files (required)
#   grpc-endpoint     Block Node gRPC endpoint (default: localhost:40840)
#   max-block         Stop and fail if this block is reached before the check is conclusive.
#                     Blocks are produced every ~2s; the script waits patiently
#                     for each block to become available. Also enforces a hard
#                     wall-clock timeout of max_block*2 seconds. (default: 0 = no limit)
#   port-forward-cmd  Optional shell command to restart port-forwards on connection error.
#                     When provided, a connection error triggers this command and retries
#                     instead of failing immediately (max 3 restarts).
#
# Environment:
#   MONITOR_BLOCK_STEP        Scan stride in blocks (default: 50)
#   MONITOR_MIN_NOVA_SAMPLES  Sampled blocks that must carry the Nova layout before the
#                             Nova era passes (default: 3)
#
# Exit codes:
#   0  older library: Schnorr then WRAPS transition found (including a hard-timeout hit
#      after WRAPS was observed: the transition is real, only its exact block number went
#      unnarrowed); Nova library: MONITOR_MIN_NOVA_SAMPLES sampled blocks carry the Nova layout
#   1  max-block reached before the check was conclusive, or error
#   2  connection error with no port-forward-cmd provided
#   3  hard wall-clock timeout (max_block*2) hit before the check was conclusive
#   4  unexpected layout: the Nova layout mixed with an older one, or a signature of an
#      unknown size

set -euo pipefail

PROTO_DIR="${1:?Usage: $0 <proto-path> [grpc-endpoint] [max-block] [port-forward-cmd]}"
BN_ENDPOINT="${2:-localhost:40840}"
MAX_BLOCK="${3:-0}"
PORT_FORWARD_CMD="${4:-}"

# Signature size of a TSS block proof made by the Nova WRAPS library.
NOVA_SIGNATURE_BYTES=13712

# Scan stride. Env-overridable so the unit suite can pair a tiny max_block with a
# stride small enough to enter the scan loop at all; production always uses 50.
STEP="${MONITOR_BLOCK_STEP:-50}"
# Nova-layout samples needed before the Nova era passes. Several samples spread STEP
# blocks apart show the layout holds along the chain, not just on one block.
MIN_NOVA_SAMPLES="${MONITOR_MIN_NOVA_SAMPLES:-3}"
# Max gRPC message size to accept. Normal blocks are ~40KB; the TSS genesis
# block can be ~100MB. Downloading 100MB through kubectl port-forward crashes
# the SPDY tunnel. A 10MB limit rejects oversized blocks early (via RST_STREAM)
# before the data flood kills the port-forward. Rejected blocks are treated as
# NOT_AVAILABLE, which the binary search handles correctly.
MAX_MSG_SIZE=10000000
# Per-call wall-clock ceiling for grpcurl. A Service whose pods are gone still
# accepts a kubectl port-forward connection and then stalls forever, so without
# this a single call can block the whole script past the max_block*2
# deadline before check_hard_timeout gets a chance to run. 60s is well clear of a legitimate
# fetch: observed blocks are ~40KB, the largest seen 983.9KB, and MAX_MSG_SIZE
# caps a download at 10MB. On expiry grpcurl reports "context deadline
# exceeded", which the CONNECTION_ERROR pattern below already handles.
MAX_CALL_SECONDS=60

if [[ ! "${STEP}" =~ ^[1-9][0-9]*$ ]]; then
  echo "ERROR: MONITOR_BLOCK_STEP must be a positive integer, got '${STEP}'" >&2
  exit 1
fi
if [[ ! "${MIN_NOVA_SAMPLES}" =~ ^[1-9][0-9]*$ ]]; then
  echo "ERROR: MONITOR_MIN_NOVA_SAMPLES must be a positive integer, got '${MIN_NOVA_SAMPLES}'" >&2
  exit 1
fi

# Hard timeout: max_block * 2s (block production interval).
# If max_block=1000, timeout=2000s (~33 min). 0 means no timeout.
if [[ "$MAX_BLOCK" -gt 0 ]]; then
  HARD_TIMEOUT=$(( MAX_BLOCK * 2 ))
else
  HARD_TIMEOUT=0
fi
START_TIME=$(date +%s)

# Era state, only ever changed in the main shell (record_layout). Empty until the first
# signed TSS proof is classified, then PRE_NOVA or NOVA.
ERA=""
# Nova-layout samples seen so far, and their block numbers (space separated).
NOVA_COUNT=0
NOVA_BLOCKS=""
# Set once WRAPS is observed. Everything after that only sharpens the reported
# block number, so a deadline hit with this set is imprecision, not a failure.
WRAPS_SEEN_BLOCK=""
# Results of find_schnorr_lower_bound and binary_search_transition. Globals rather than
# command substitutions so that record_layout runs in the main shell and its exit ends
# the script.
LOWER_BOUND=0
TRANSITION_BLOCK=0

# Without a trap the TERM from check_hard_timeout exits 143, indistinguishable
# from any other abnormal death. 3 names the self-imposed deadline.
function on_hard_timeout {
  if [[ -n "${WRAPS_SEEN_BLOCK}" ]]; then
    exit 0
  fi
  exit 3
}
trap on_hard_timeout TERM

# One line describing how far the era check got, for the not-conclusive summaries.
function print_era_progress {
  case "${ERA}" in
    NOVA)
      echo "  Era: Nova, ${NOVA_COUNT} of ${MIN_NOVA_SAMPLES} required samples seen (blocks:${NOVA_BLOCKS})"
      ;;
    PRE_NOVA)
      if [[ -n "${WRAPS_SEEN_BLOCK}" ]]; then
        echo "  Era: pre-Nova, WRAPS first sampled at block ${WRAPS_SEEN_BLOCK}"
      else
        echo "  Era: pre-Nova, no WRAPS block sampled yet"
      fi
      ;;
    *)
      echo "  Era: undetermined, no signed TSS block proof sampled yet"
      ;;
  esac
}

# Checked from get_sig_type_safe (every gRPC call site: scan loop, binary
# search, lower-bound search, print_transition_result) rather than only between scan
# iterations, so post-detection work can't run unbounded past the deadline.
function check_hard_timeout {
  [[ "$HARD_TIMEOUT" -gt 0 ]] || return 0
  local elapsed=$(( $(date +%s) - START_TIME ))
  if [[ "$elapsed" -ge "$HARD_TIMEOUT" ]]; then
    {
      echo ""
      echo "=== TSS Block Proofs ==="
      if [[ -n "${WRAPS_SEEN_BLOCK}" ]]; then
        echo "  Era: pre-Nova (Schnorr then WRAPS)"
        echo "  First WRAPS block: ${WRAPS_SEEN_BLOCK}"
        echo "  (upper bound only: exact transition block not narrowed, ${HARD_TIMEOUT}s deadline hit during binary search)"
      else
        echo "  Status: NOT CONCLUSIVE"
        print_era_progress
        echo "  Hard timeout: ${HARD_TIMEOUT}s elapsed (max_block=${MAX_BLOCK} * 2s)"
      fi
    } >&2
    # set -e doesn't reliably cross nested $(...) layers; signal the top-level script.
    # The exit below lets the enclosing $(...) finish so the parent can run its trap.
    kill -TERM "$$"
    exit 1
  fi
}

if ! command -v grpcurl &>/dev/null; then
  echo "ERROR: grpcurl not found" >&2
  exit 1
fi
if [[ ! -d "${PROTO_DIR}" ]]; then
  echo "ERROR: Proto path not found: ${PROTO_DIR}" >&2
  exit 1
fi

# Returns "<TYPE> <sig_bytes> <block_size>"
# e.g. "NOVA 13712 95432", "SCHNORR 2920 95432", "WRAPS 3432 94521320",
#      "STATE_PROOF 0 41230", "UNKNOWN 4000 95432", "NOT_AVAILABLE 0 0"
# TYPE is NOVA, SCHNORR, WRAPS or UNKNOWN for a signed TSS proof; STATE_PROOF,
# RECORD_FILE or NONE for a block without a TSS signature; NOT_AVAILABLE or
# CONNECTION_ERROR when the block could not be read.
# block_size is the approximate binary protobuf size (sum of base64-decoded fields)
function get_sig_type {
  local block_number="$1"
  local raw
  raw=$(cd "${PROTO_DIR}" && grpcurl -plaintext -import-path . \
    -proto "block-node/api/block_access_service.proto" \
    -max-msg-sz "${MAX_MSG_SIZE}" \
    -max-time "${MAX_CALL_SECONDS}" \
    -d "{\"block_number\": ${block_number}}" \
    "${BN_ENDPOINT}" org.hiero.block.api.BlockAccessService/getBlock 2>&1) || true

  if echo "${raw}" | grep -q '"NOT_FOUND"\|"NOT_AVAILABLE"'; then
    echo "NOT_AVAILABLE 0 0"
    return
  fi

  # Connection errors (port-forward down, refused, timeout): fail fast
  if echo "${raw}" | grep -qiE "connection refused|failed to dial|context deadline|transport:"; then
    echo "CONNECTION_ERROR 0 0"
    return
  fi

  # Oversized block: grpcurl rejected it before downloading (port-forward survives)
  if echo "${raw}" | grep -qiE "received message larger than max"; then
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
proof_kind = 'NONE'
sig_bytes = 0
for item in d.get('block',{}).get('items',[]):
    if 'blockProof' in item:
        bp = item['blockProof']
        sbp = bp.get('signedBlockProof',{})
        if sbp and sbp.get('blockSignature'):
            proof_kind = 'SIGNED'
            sig_bytes = len(base64.b64decode(sbp['blockSignature']))
        elif bp.get('blockStateProof'):
            proof_kind = 'STATE_PROOF'
        elif bp.get('signedRecordFileProof'):
            proof_kind = 'RECORD_FILE'
        break
print(f'{proof_kind} {sig_bytes} {binary_size}')
" 2>/dev/null) || parsed="NONE 0 0"

  local proof_kind sig_bytes block_size
  read -r proof_kind sig_bytes block_size <<< "${parsed}"

  local sig_type
  if [[ "${proof_kind}" == "SIGNED" ]]; then
    case "${sig_bytes}" in
      "${NOVA_SIGNATURE_BYTES}") sig_type="NOVA" ;;
      292[0-9])                 sig_type="SCHNORR" ;;
      343[0-9])                 sig_type="WRAPS" ;;
      *)                        sig_type="UNKNOWN" ;;
    esac
  else
    # STATE_PROOF, RECORD_FILE or NONE: no TSS signature to classify on this block
    sig_type="${proof_kind}"
  fi

  echo "${sig_type} ${sig_bytes} ${block_size}"
}

# Wrapper around get_sig_type that recovers from port-forward crashes.
# If a CONNECTION_ERROR is detected and PORT_FORWARD_CMD is set:
#   1. Restarts port-forward
#   2. Retries the request once
#   3. If retry also fails (oversized block crashing port-forward), returns NOT_AVAILABLE
# Without PORT_FORWARD_CMD, CONNECTION_ERROR is returned as-is for the caller to handle.
function get_sig_type_safe {
  check_hard_timeout
  local block_number="$1"
  local result
  result=$(get_sig_type "${block_number}")
  local sig_type="${result%% *}"

  if [[ "${sig_type}" == "CONNECTION_ERROR" && -n "${PORT_FORWARD_CMD}" ]]; then
    echo "  Port-forward lost at block ${block_number}, restarting..." >&2
    eval "${PORT_FORWARD_CMD}" >&2 2>/dev/null || true
    sleep 3
    result=$(get_sig_type "${block_number}")
    sig_type="${result%% *}"
    if [[ "${sig_type}" == "CONNECTION_ERROR" ]]; then
      # Block itself is crashing the port-forward (likely oversized); recover and skip
      echo "  Block ${block_number} crashes port-forward (likely oversized), recovering..." >&2
      eval "${PORT_FORWARD_CMD}" >&2 2>/dev/null || true
      sleep 3
      echo "NOT_AVAILABLE 0 0"
      return
    fi
  fi

  echo "${result}"
}

# Prints the unexpected-layout summary and ends the script with exit 4.
function fail_layout {
  local detail="$1"
  echo ""
  echo "=== TSS Block Proofs ==="
  echo "  Status: UNEXPECTED LAYOUT"
  echo "  Detail: ${detail}"
  print_era_progress
  exit 4
}

# record_layout <block> <type> <sig_bytes>
# Applies the era rules to one classified block. Must run in the main shell, never inside
# a command substitution, so that fail_layout's exit ends the script.
function record_layout {
  local block_number="$1"
  local sig_type="$2"
  local sig_bytes="$3"
  case "${sig_type}" in
    NOVA)
      if [[ "${ERA}" == "PRE_NOVA" ]]; then
        fail_layout "block ${block_number} carries the Nova layout (${sig_bytes} bytes) after older layouts were seen"
      else
        ERA="NOVA"
      fi
      ;;
    SCHNORR|WRAPS)
      if [[ "${ERA}" == "NOVA" ]]; then
        fail_layout "block ${block_number} carries the older ${sig_type} layout (${sig_bytes} bytes) after the Nova layout was seen"
      else
        ERA="PRE_NOVA"
      fi
      ;;
    UNKNOWN)
      fail_layout "block ${block_number} carries a ${sig_bytes} byte TSS signature, which matches no known layout"
      ;;
    *)
      # STATE_PROOF, RECORD_FILE, NONE, NOT_AVAILABLE or CONNECTION_ERROR: nothing to classify
      ;;
  esac
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

function format_block_line {
  local block_number="$1"
  local result="$2"
  local sig_type sig_bytes block_size
  read -r sig_type sig_bytes block_size <<< "${result}"

  case "${sig_type}" in
    NOT_AVAILABLE|CONNECTION_ERROR|"")
      echo "  Block ${block_number}: NOT_AVAILABLE (BN unable to serve it, likely the oversized WRAPS genesis block)"
      ;;
    STATE_PROOF|RECORD_FILE|NONE)
      echo "  Block ${block_number}: ${sig_type} (no TSS signature, block: $(format_size "${block_size}")), skipped"
      ;;
    *)
      echo "  Block ${block_number}: ${sig_type} (sig: ${sig_bytes} bytes, block: $(format_size "${block_size}"))"
      ;;
  esac
}

# Steps back from the first sampled WRAPS block, 100 blocks at a time, to a block that
# still carries Schnorr. Sets LOWER_BOUND (0 when none is found).
function find_schnorr_lower_bound {
  local wraps_block="$1"
  local low=$(( wraps_block > 100 ? wraps_block - 100 : 0 ))
  local result sig_type sig_bytes block_size

  LOWER_BOUND=0
  while [[ $low -gt 0 ]]; do
    result=$(get_sig_type_safe "$low")
    read -r sig_type sig_bytes block_size <<< "${result}"
    record_layout "$low" "${sig_type}" "${sig_bytes}"
    if [[ "${sig_type}" == "SCHNORR" ]]; then
      LOWER_BOUND=$low
      return
    fi
    low=$(( low > 100 ? low - 100 : 0 ))
  done
}

# Narrows the first WRAPS block between a Schnorr (or 0) lower bound and a WRAPS upper
# bound. Sets TRANSITION_BLOCK.
function binary_search_transition {
  local low="$1"
  local high="$2"
  local mid result sig_type sig_bytes block_size

  while [[ $((high - low)) -gt 1 ]]; do
    mid=$(( (low + high) / 2 ))
    result=$(get_sig_type_safe "$mid")
    read -r sig_type sig_bytes block_size <<< "${result}"
    record_layout "$mid" "${sig_type}" "${sig_bytes}"
    if [[ "$sig_type" == "SCHNORR" ]]; then
      low=$mid
    else
      # WRAPS, NOT_AVAILABLE or no TSS signature: treat conservatively as potential WRAPS
      # (the ~99MB genesis WRAPS block may fail to fetch on resource-constrained CI)
      high=$mid
    fi
  done

  TRANSITION_BLOCK=$high
}

function print_transition_result {
  local transition_block="$1"
  local pre_result post_result next_result sig_type sig_bytes block_size

  pre_result=$(get_sig_type_safe "$((transition_block - 1))")
  read -r sig_type sig_bytes block_size <<< "${pre_result}"
  record_layout "$((transition_block - 1))" "${sig_type}" "${sig_bytes}"
  post_result=$(get_sig_type_safe "$transition_block")
  read -r sig_type sig_bytes block_size <<< "${post_result}"
  record_layout "$transition_block" "${sig_type}" "${sig_bytes}"
  next_result=$(get_sig_type_safe "$((transition_block + 1))")
  read -r sig_type sig_bytes block_size <<< "${next_result}"
  record_layout "$((transition_block + 1))" "${sig_type}" "${sig_bytes}"

  echo ""
  echo "=== TSS Block Proofs ==="
  echo "  Era: pre-Nova (Schnorr then WRAPS)"
  echo "  First WRAPS block: ${transition_block}"
  echo "  Last Schnorr block: $((transition_block - 1))"
  format_block_line "$((transition_block - 1))" "$pre_result"
  format_block_line "$transition_block" "$post_result"
  format_block_line "$((transition_block + 1))" "$next_result"
}

function print_nova_result {
  local last_result="$1"
  local last_block="$2"

  echo ""
  echo "=== TSS Block Proofs ==="
  echo "  Era: Nova (one block proof layout on every signed block)"
  echo "  Nova blocks sampled:${NOVA_BLOCKS}"
  format_block_line "${last_block}" "${last_result}"
}

# Main: scan every STEP blocks
echo "Scanning TSS block proofs (every ${STEP} blocks, ${MIN_NOVA_SAMPLES} Nova samples needed for the Nova era)..."
block=0
waiting_for=0
wait_count=0
while true; do
  block=$(( block + STEP ))

  # Enforce max-block limit
  if [[ "$MAX_BLOCK" -gt 0 && "$block" -gt "$MAX_BLOCK" ]]; then
    echo ""
    echo "=== TSS Block Proofs ==="
    echo "  Status: NOT CONCLUSIVE"
    print_era_progress
    echo "  Max block limit: ${MAX_BLOCK}"
    exit 1
  fi

  result=$(get_sig_type_safe "$block")
  read -r sig_type sig_bytes block_size <<< "${result}"

  # get_sig_type_safe handles port-forward recovery when PORT_FORWARD_CMD is set.
  # If it still returns CONNECTION_ERROR, there's nothing more we can do.
  if [[ "$sig_type" == "CONNECTION_ERROR" ]]; then
    echo ""
    echo "ERROR: Cannot connect to Block Node at ${BN_ENDPOINT} (port-forward may be down)" >&2
    exit 2
  fi

  if [[ "$sig_type" == "NOT_AVAILABLE" ]]; then
    if [[ "${waiting_for}" -ne "$block" ]]; then
      waiting_for=$block
      wait_count=0
      printf "  Block %d: waiting" "$block"
    fi
    (( wait_count++ )) || true
    printf "."
    sleep 5
    # Block production is every ~2s. Wait patiently, the block will be produced.
    # max_block and the hard wall-clock timeout are what bound the wait.
    block=$(( block - STEP ))
    continue
  fi

  # End the waiting dots line if we were waiting
  if [[ "${waiting_for}" -gt 0 ]]; then
    printf " (available after %ds)\n" $((wait_count * 5))
    waiting_for=0
  fi

  format_block_line "$block" "$result"
  record_layout "$block" "$sig_type" "$sig_bytes"

  if [[ "$sig_type" == "NOVA" ]]; then
    NOVA_COUNT=$(( NOVA_COUNT + 1 ))
    NOVA_BLOCKS="${NOVA_BLOCKS} ${block}"
    if [[ "${NOVA_COUNT}" -ge "${MIN_NOVA_SAMPLES}" ]]; then
      print_nova_result "$result" "$block"
      exit 0
    fi
  elif [[ "$sig_type" == "WRAPS" ]]; then
    # Record it before narrowing: from here on the assertion is already satisfied.
    WRAPS_SEEN_BLOCK=$block
    echo "  WRAPS detected, binary searching for exact transition..."
    find_schnorr_lower_bound "$block"
    binary_search_transition "$LOWER_BOUND" "$block"
    print_transition_result "$TRANSITION_BLOCK"
    exit 0
  fi
  # SCHNORR or a block without a TSS signature: keep scanning
done
