#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Finds any block on a Block Node whose BlockProof carries a `block_state_proof`
# (indirect / state-based proof) — as fast as possible.
#
# Architecture:
#   - A generator emits block numbers on stdout.
#   - xargs -P PARALLEL fans them out to concurrent workers; the next available
#     worker grabs the next block, so a slow/retrying worker does not block faster
#     ones. No per-batch sync point.
#   - Transient failures (NOT_AVAILABLE, FETCH_ERROR, CONNECTION_ERROR) are
#     retried inside the worker with exponential backoff so a flaky block does
#     not get silently skipped.
#   - When a worker thinks it found a StateProof, it re-fetches the block and
#     re-verifies before writing the block number into a shared result file
#     (atomic rename). The main script polls that file and tears the whole
#     pipeline down as soon as a verified hit appears.
#
# Note: because workers run in parallel, the discovered block is "the first one
# any worker confirmed", not necessarily the lowest-numbered hit. That's the
# deliberate trade for speed; goal is to find a state-proof block ASAP.
#
# Usage:
#   ./find-state-proof-block.sh <bn-endpoint> [start-block] [max-block]
#
# Arguments:
#   bn-endpoint   Block Node gRPC endpoint, e.g. localhost:40840 (required)
#   start-block   Block number to start scanning from (default: 0).
#                 Clamped up to firstAvailableBlock if the BN doesn't have it.
#   max-block     Stop and fail if the generator reaches this block without
#                 finding a state proof (default: 0 = no limit).
#
# Environment:
#   PARALLEL      Number of concurrent getBlock workers (default: 10)
#   MAX_RETRIES   Per-block retries on transient failures (default: 5).
#                 Failures after this many retries are skipped.
#   RETRY_DELAY   Base seconds between retries; doubles up to 8s (default: 1)
#
# Output:
#   On success, prints the matching block number to stdout.
#   Per-block status goes to stderr.
#
# Exit codes:
#   0  Block with state proof found; block number printed to stdout
#   1  max-block reached without finding a state proof, or fatal error
#   2  Persistent connection error reaching the Block Node

set -uo pipefail
# Enable job control in this non-interactive script so each `&` background
# job becomes its own process group leader. Without this, Ctrl+C can only kill
# the foreground poll loop, leaving xargs/workers/grpcurl orphaned.
set -m

BN_ENDPOINT="${1:?Usage: $0 <bn-endpoint> [start-block] [max-block]}"
START_BLOCK="${2:-0}"
MAX_BLOCK="${3:-0}"
PARALLEL="${PARALLEL:-10}"
MAX_RETRIES="${MAX_RETRIES:-5}"
RETRY_DELAY="${RETRY_DELAY:-1}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
API_PROTO_DIR="${REPO_ROOT}/protobuf-sources/src/main/proto"
STREAM_PROTO_DIR="${REPO_ROOT}/protobuf-sources/block-node-protobuf"

BN_ENDPOINT="${BN_ENDPOINT#tcp://}"
BN_ENDPOINT="${BN_ENDPOINT#http://}"
BN_ENDPOINT="${BN_ENDPOINT#https://}"
BN_ENDPOINT="${BN_ENDPOINT%/}"

MAX_MSG_SIZE=209715200
TIP_POLL_SECONDS=2

if ! command -v grpcurl &>/dev/null; then
  echo "ERROR: grpcurl not found in PATH" >&2
  exit 1
fi
if ! command -v python3 &>/dev/null; then
  echo "ERROR: python3 not found in PATH" >&2
  exit 1
fi
if [[ ! -d "${API_PROTO_DIR}" || ! -d "${STREAM_PROTO_DIR}" ]]; then
  echo "ERROR: proto directories missing." >&2
  echo "  Expected: ${API_PROTO_DIR}" >&2
  echo "  Expected: ${STREAM_PROTO_DIR}" >&2
  exit 1
fi
if [[ "${PARALLEL}" -lt 1 ]]; then
  echo "ERROR: PARALLEL must be >= 1" >&2
  exit 1
fi
if [[ "${MAX_RETRIES}" -lt 0 ]]; then
  echo "ERROR: MAX_RETRIES must be >= 0" >&2
  exit 1
fi

# Shared run-state directory. Workers signal a confirmed hit by atomically
# renaming a tmp file to RESULT_FILE. We also write STOP_FILE to tell the
# generator and other workers to exit immediately.
RUN_DIR="$(mktemp -d -t find-state-proof-block.XXXXXX)"
RESULT_FILE="${RUN_DIR}/result"
STOP_FILE="${RUN_DIR}/stop"
export RUN_DIR RESULT_FILE STOP_FILE

pipeline_pid=""
cleanup_done=""
function shutdown {
  # Idempotent — trap fires on EXIT after INT/TERM too.
  [[ -n "${cleanup_done}" ]] && return 0
  cleanup_done=1
  : > "${STOP_FILE}" 2>/dev/null || true
  if [[ -n "${pipeline_pid}" ]]; then
    # Kill the whole process group of the background pipeline so xargs,
    # all bash workers, and any in-flight grpcurl children are torn down.
    kill -TERM -- "-${pipeline_pid}" 2>/dev/null || \
      kill -TERM "${pipeline_pid}" 2>/dev/null || true
    # Brief grace period before SIGKILL.
    for _ in 1 2 3 4; do
      kill -0 "${pipeline_pid}" 2>/dev/null || break
      sleep 0.25
    done
    kill -KILL -- "-${pipeline_pid}" 2>/dev/null || \
      kill -KILL "${pipeline_pid}" 2>/dev/null || true
  fi
  rm -rf "${RUN_DIR}"
}
function on_interrupt {
  echo "" >&2
  echo "Interrupted — stopping workers and cleaning up..." >&2
  shutdown
  exit 130
}
trap shutdown EXIT
trap on_interrupt INT TERM HUP

# Echoes "<first_available> <last_available>" or "ERROR".
function query_server_status {
  local raw
  raw=$(grpcurl -plaintext \
    -import-path "${API_PROTO_DIR}" \
    -import-path "${STREAM_PROTO_DIR}" \
    -proto "block-node/api/node_service.proto" \
    -max-msg-sz "${MAX_MSG_SIZE}" \
    -d '{}' \
    "${BN_ENDPOINT}" org.hiero.block.api.BlockNodeService/serverStatus 2>&1) || true

  if ! echo "${raw}" | python3 -c "import sys,json; json.loads(sys.stdin.read())" 2>/dev/null; then
    echo "ERROR"
    return
  fi
  echo "${raw}" | python3 -c "
import sys, json
d = json.loads(sys.stdin.read())
print(f\"{int(d.get('firstAvailableBlock', 0))} {int(d.get('lastAvailableBlock', 0))}\")
"
}

# Classifies one block; echoes a result token to stdout.
# Result: STATE_PROOF | TSS_PROOF | RSA_PROOF | UNKNOWN_PROOF
#       | NO_PROOF | NOT_AVAILABLE | FETCH_ERROR | CONNECTION_ERROR
function inspect_block_once {
  local block_number="$1"
  local raw
  raw=$(grpcurl -plaintext \
    -import-path "${API_PROTO_DIR}" \
    -import-path "${STREAM_PROTO_DIR}" \
    -proto "block-node/api/block_access_service.proto" \
    -max-msg-sz "${MAX_MSG_SIZE}" \
    -d "{\"block_number\": ${block_number}}" \
    "${BN_ENDPOINT}" org.hiero.block.api.BlockAccessService/getBlock 2>&1) || true

  if echo "${raw}" | grep -q '"status": *"NOT_FOUND"\|"status": *"NOT_AVAILABLE"'; then
    echo "NOT_AVAILABLE"
    return
  fi
  if echo "${raw}" | grep -qiE "connection refused|failed to dial|name resolution|no such host|context deadline"; then
    echo "CONNECTION_ERROR"
    return
  fi
  if echo "${raw}" | grep -qiE "code: *internal|code: *unknown|code: *resourceexhausted|received message larger than max|unmarshal|unexpected eof"; then
    echo "FETCH_ERROR"
    return
  fi
  if ! echo "${raw}" | python3 -c "import sys,json; json.loads(sys.stdin.read())" 2>/dev/null; then
    echo "FETCH_ERROR"
    return
  fi

  # Strict detection: require `block.items` to be a list of dicts; require each
  # candidate `blockProof` to be a dict; require the matching proof key's value
  # to also be a dict (the message body), not a string or a primitive that
  # Python's `in` could substring-match by accident.
  echo "${raw}" | python3 -c "
import sys, json
d = json.loads(sys.stdin.read())
block = d.get('block') if isinstance(d, dict) else None
items = block.get('items') if isinstance(block, dict) else None
if not isinstance(items, list):
    print('NO_PROOF'); sys.exit(0)

has_proof_item = False
for item in items:
    if not isinstance(item, dict):
        continue
    bp = item.get('blockProof')
    if not isinstance(bp, dict):
        continue
    has_proof_item = True
    if isinstance(bp.get('blockStateProof'), dict):
        print('STATE_PROOF'); sys.exit(0)
    if isinstance(bp.get('signedBlockProof'), dict):
        print('TSS_PROOF'); sys.exit(0)
    if isinstance(bp.get('signedRecordFileProof'), dict):
        print('RSA_PROOF'); sys.exit(0)
print('UNKNOWN_PROOF' if has_proof_item else 'NO_PROOF')
"
}

# Worker entry point used by xargs -I {}.
# - Retries transient failures with exponential backoff (capped at 8s).
# - On a putative STATE_PROOF: re-fetch and re-verify before signalling success
#   (defensive against transient malformed responses).
# - On a confirmed STATE_PROOF: write block number to RESULT_FILE atomically
#   (via mv from a tmp path in the same dir), touch STOP_FILE, and return 0.
# - On any STOP_FILE existence check, exit immediately so other workers don't
#   keep doing useless work after a hit.
function worker_main {
  local n="$1"
  local attempt=0
  local delay="${RETRY_DELAY}"
  local result

  # Cheap early-out: another worker already won.
  [[ -e "${STOP_FILE}" ]] && return 0

  while true; do
    result=$(inspect_block_once "${n}")
    [[ -e "${STOP_FILE}" ]] && return 0

    case "${result}" in
      STATE_PROOF)
        # Defensive re-verification: a single bad response should not pass.
        local verify
        verify=$(inspect_block_once "${n}")
        if [[ "${verify}" != "STATE_PROOF" ]]; then
          printf "  Block %d: STATE_PROOF (1st look) but re-verify said %s — treating as %s\n" \
            "${n}" "${verify}" "${verify}" >&2
          result="${verify}"
          # Fall through to the matching case below by looping with the new result.
          continue
        fi
        if [[ "${attempt}" -gt 0 ]]; then
          printf "  Block %d: StateProof (BlockStateProof) [verified, after %d retr%s]\n" \
            "${n}" "${attempt}" "$([[ ${attempt} -eq 1 ]] && echo y || echo ies)" >&2
        else
          printf "  Block %d: StateProof (BlockStateProof) [verified]\n" "${n}" >&2
        fi
        # Atomically publish the result. Two workers writing the same value is
        # harmless; mv is atomic within the same filesystem.
        local tmp="${RUN_DIR}/result.$$"
        printf "%d" "${n}" > "${tmp}"
        mv "${tmp}" "${RESULT_FILE}" 2>/dev/null || true
        : > "${STOP_FILE}"
        return 0
        ;;
      TSS_PROOF)
        if [[ "${attempt}" -gt 0 ]]; then
          printf "  Block %d: TSS (SignedBlockProof) [after %d retr%s]\n" \
            "${n}" "${attempt}" "$([[ ${attempt} -eq 1 ]] && echo y || echo ies)" >&2
        else
          printf "  Block %d: TSS (SignedBlockProof)\n" "${n}" >&2
        fi
        return 0
        ;;
      RSA_PROOF)
        printf "  Block %d: RSA (SignedRecordFileProof)\n" "${n}" >&2
        return 0
        ;;
      UNKNOWN_PROOF)
        printf "  Block %d: BlockProof present, no recognised key\n" "${n}" >&2
        return 0
        ;;
      NO_PROOF)
        printf "  Block %d: no BlockProof item\n" "${n}" >&2
        return 0
        ;;
      NOT_AVAILABLE|FETCH_ERROR|CONNECTION_ERROR)
        if [[ "${attempt}" -ge "${MAX_RETRIES}" ]]; then
          printf "  Block %d: %s (gave up after %d retr%s)\n" \
            "${n}" "${result}" "${attempt}" \
            "$([[ ${attempt} -eq 1 ]] && echo y || echo ies)" >&2
          return 0
        fi
        attempt=$(( attempt + 1 ))
        printf "  Block %d: %s — retry %d/%d in %ds\n" \
          "${n}" "${result}" "${attempt}" "${MAX_RETRIES}" "${delay}" >&2
        sleep "${delay}"
        if [[ "${delay}" -lt 8 ]]; then
          delay=$(( delay * 2 ))
        fi
        ;;
      *)
        printf "  Block %d: %s (unexpected)\n" "${n}" "${result}" >&2
        return 0
        ;;
    esac
  done
}

# Emits block numbers on stdout. Stops when STOP_FILE appears or when MAX_BLOCK
# is reached. Waits on the BN tip when caught up.
function emit_block_numbers {
  local start="$1"
  local last="$2"
  local b="${start}"
  while true; do
    [[ -e "${STOP_FILE}" ]] && return 0
    if [[ "${MAX_BLOCK}" -gt 0 && "${b}" -gt "${MAX_BLOCK}" ]]; then
      return 0
    fi
    if [[ "${b}" -gt "${last}" ]]; then
      printf "  Generator: at tip (last_available=%d), waiting" "${last}" >&2
      local waited=0
      while [[ "${b}" -gt "${last}" ]]; do
        [[ -e "${STOP_FILE}" ]] && { printf "\n" >&2; return 0; }
        sleep "${TIP_POLL_SECONDS}"
        waited=$(( waited + TIP_POLL_SECONDS ))
        printf "." >&2
        local status
        status=$(query_server_status)
        if [[ "${status}" == "ERROR" ]]; then
          printf "\n  Generator: lost connection (workers will retry)\n" >&2
          sleep "${TIP_POLL_SECONDS}"
          continue
        fi
        last="${status##* }"
      done
      printf " (tip now %d, waited %ds)\n" "${last}" "${waited}" >&2
    fi
    printf "%d\n" "${b}"
    b=$(( b + 1 ))
  done
}

if [[ "${MAX_BLOCK}" -gt 0 && "${START_BLOCK}" -gt "${MAX_BLOCK}" ]]; then
  echo "ERROR: start-block (${START_BLOCK}) is greater than max-block (${MAX_BLOCK})" >&2
  exit 1
fi

echo "Block Node:     ${BN_ENDPOINT}" >&2
echo "Parallelism:    ${PARALLEL}" >&2
echo "Retries/block:  ${MAX_RETRIES} (base delay ${RETRY_DELAY}s, exp backoff cap 8s)" >&2
echo "Querying serverStatus..." >&2
status=$(query_server_status)
if [[ "${status}" == "ERROR" ]]; then
  echo "ERROR: cannot reach Block Node at ${BN_ENDPOINT}" >&2
  exit 2
fi
first_available="${status%% *}"
last_available="${status##* }"
echo "Available:      [${first_available}, ${last_available}]" >&2

block="${START_BLOCK}"
if [[ "${block}" -lt "${first_available}" ]]; then
  echo "Note:           start-block ${START_BLOCK} < firstAvailable ${first_available}; advancing to ${first_available}" >&2
  block="${first_available}"
fi
if [[ "${MAX_BLOCK}" -gt 0 ]]; then
  echo "Scan range:     [${block}, ${MAX_BLOCK}]" >&2
else
  echo "Scan range:     [${block}, unlimited]" >&2
fi
echo "Searching for any STATE_PROOF (fastest wins; results are verified)..." >&2
echo "" >&2

export BN_ENDPOINT API_PROTO_DIR STREAM_PROTO_DIR MAX_MSG_SIZE
export MAX_RETRIES RETRY_DELAY
export -f inspect_block_once worker_main

# Run the pipeline in the background. With `set -m` enabled above, the
# subshell `(...) &` becomes its own process group leader, so the cleanup
# trap can kill the entire tree (xargs + bash workers + grpcurl) at once.
(
  emit_block_numbers "${block}" "${last_available}" \
    | xargs -n 1 -P "${PARALLEL}" -I {} bash -c 'worker_main "$@"' _ {}
) &
pipeline_pid=$!

# Poll for the result file. When it appears (or the pipeline exits without
# a hit), drop out of the loop and let the EXIT trap tear things down.
while true; do
  if [[ -e "${RESULT_FILE}" ]]; then
    break
  fi
  if ! kill -0 "${pipeline_pid}" 2>/dev/null; then
    break
  fi
  sleep 0.5
done

if [[ ! -e "${RESULT_FILE}" ]]; then
  echo "" >&2
  if [[ "${MAX_BLOCK}" -gt 0 ]]; then
    echo "No StateProof found in blocks [${START_BLOCK}, ${MAX_BLOCK}]." >&2
  else
    echo "Pipeline ended without finding a StateProof." >&2
  fi
  exit 1
fi

found_block="$(cat "${RESULT_FILE}")"
echo "" >&2
echo "================================================================" >&2
printf "  >>> FOUND: block %s carries a StateProof (BlockStateProof) <<<\n" "${found_block}" >&2
echo "================================================================" >&2
echo "${found_block}"
exit 0
