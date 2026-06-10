#!/usr/bin/env bash
#
# generate-rsa-roster-bootstrap.sh
#
# Build a hiero-block-node rsa-bootstrap-roster.json by querying a Hedera
# Mirror Node (MN) REST API. This is the operator-side, deploy-time equivalent
# of the runtime fallback in RsaRosterBootstrapPlugin.fetchFromMirrorNode():
# it lets an operator generate the bootstrap roster file in advance so the
# Block Node has it on disk at startup and never has to call the Mirror Node.
#
#   Source (MN):  GET <endpoint>/api/v1/network/nodes
#                 -> nodes[].{node_id, public_key, timestamp:{from,to}}
#                    + links.next (pagination)
#
#   Output (BN):  rsa-bootstrap-roster.json
#                 -> { nodeAddress: [ { RSAPubKey, nodeId } ] }
#                    where RSAPubKey is the lowercase hex public key with any
#                    leading "0x" stripped, sorted ascending by nodeId.
#
# Only the *active* roster is captured: entries whose timestamp.to is blank
# (null/empty) are the currently-active address-book entries; superseded
# historical entries (timestamp.to set) are excluded. Nodes without a
# public_key are excluded — they cannot participate in RSA WRB verification.
#
# This output matches NodeAddressBook.JSON as parsed by RsaRosterBootstrapPlugin at startup.
#
# The script makes no assumptions about its own location or the current working
# directory. It writes to stdout by default, or to the path given via -o/--output.
#
# ===========================================================================
# RUNBOOK — generating the RSA bootstrap roster before a Phase 2a cutover
# ===========================================================================
#
# Before Consensus Nodes begin streaming Wrapped Record Blocks (WRBs), each
# Block Node must know the current node_id -> RSA-public-key mapping so it can
# verify SignedRecordFileProof block proofs. Pre-staging the file avoids a
# Mirror Node round-trip (and its failure modes) at BN startup.
#
#   1. Pick the network (or an explicit MN URL):
#        generate-rsa-roster-bootstrap.sh --network mainnet \
#          --output /opt/hiero/block-node/node/rsa-bootstrap-roster.json
#
#      or point at a specific Mirror Node:
#        generate-rsa-roster-bootstrap.sh \
#          --mirror-node-url https://mainnet-public.mirrornode.hedera.com \
#          --output /opt/hiero/block-node/node/rsa-bootstrap-roster.json
#
#   2. Inspect the result:
#        python3 -m json.tool < /opt/hiero/block-node/node/rsa-bootstrap-roster.json | head -40
#      Confirm the entry count matches the expected number of active nodes.
#
#   3. Deploy the file to the configured bootstrap path
#      (roster.bootstrap.rsa.* config; default rsa-bootstrap-roster.json in the
#      node data directory) and (re)start the Block Node.
#
#   4. Verify on startup: the blocknode_roster_entries_loaded gauge should equal
#      the number of entries in the file, and the BN must NOT log a Mirror Node
#      fetch ("RSA roster available: N entries obtained from Mirror Node" means
#      it fell back to the network instead of using the file).
#
#   5. The roster is loaded once at startup. If the address book changes (nodes
#      added/removed/re-keyed), regenerate this file and restart the BN.
# ===========================================================================

set -euo pipefail
# Locale-fixed for deterministic byte handling in sort/jq.
export LC_ALL=C

SCRIPT_NAME="$(basename "$0")"

usage() {
  cat <<'EOF'
Usage:
  generate-rsa-roster-bootstrap.sh (--network <net> | --mirror-node-url <url>) [options]

Builds a BN rsa-bootstrap-roster.json from a Mirror Node /api/v1/network/nodes query.

Source selection (exactly one required):
  -n, --network <net>          Convenience selector. One of:
                                 mainnet    -> https://mainnet-public.mirrornode.hedera.com
                                 testnet    -> https://testnet.mirrornode.hedera.com
                                 previewnet -> https://previewnet.mirrornode.hedera.com
  -u, --mirror-node-url <url>  Explicit Mirror Node base endpoint, e.g.
                                 https://mainnet-public.mirrornode.hedera.com
                               A trailing "/api/v1/network/nodes" is accepted and
                               normalized away.

Options:
  -o, --output <path>          Absolute path to write rsa-bootstrap-roster.json.
                               Default: write JSON to stdout.
  -l, --page-size <1..100>     Mirror Node page size (limit). Default: 100.
  -h, --help                   Show this help and exit.

Output shape:
  { "nodeAddress": [ { "RSAPubKey": "<lowercase-hex>", "nodeId": <int> }, ... ] }
  Sorted ascending by nodeId. Only active entries (timestamp.to blank) with a
  non-empty public_key are included; the leading "0x" is stripped.

Examples:
  generate-rsa-roster-bootstrap.sh --network mainnet \
    --output /opt/hiero/block-node/node/rsa-bootstrap-roster.json

  generate-rsa-roster-bootstrap.sh \
    --mirror-node-url https://testnet.mirrornode.hedera.com > roster.json

Required tools: curl, jq.
EOF
}

# --- arg parsing ------------------------------------------------------------

NETWORK=""
MN_URL=""
OUTPUT=""
PAGE_SIZE="100"

while [[ $# -gt 0 ]]; do
  case "$1" in
    -n|--network)        [[ $# -ge 2 ]] || { echo "Error: $1 requires an argument" >&2; usage >&2; exit 2; }; NETWORK="$2"; shift 2 ;;
    -u|--mirror-node-url)[[ $# -ge 2 ]] || { echo "Error: $1 requires an argument" >&2; usage >&2; exit 2; }; MN_URL="$2"; shift 2 ;;
    -o|--output)         [[ $# -ge 2 ]] || { echo "Error: $1 requires an argument" >&2; usage >&2; exit 2; }; OUTPUT="$2"; shift 2 ;;
    -l|--page-size)      [[ $# -ge 2 ]] || { echo "Error: $1 requires an argument" >&2; usage >&2; exit 2; }; PAGE_SIZE="$2"; shift 2 ;;
    -h|--help)           usage; exit 0 ;;
    --) shift; break ;;
    -*) echo "Error: unknown option $1" >&2; usage >&2; exit 2 ;;
    *)  echo "Error: unexpected positional argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done
if [[ $# -gt 0 ]]; then
  echo "Error: unexpected positional argument(s): $*" >&2
  usage >&2
  exit 2
fi

# --- arg validation ---------------------------------------------------------

if [[ -n "$NETWORK" && -n "$MN_URL" ]]; then
  echo "Error: provide only one of --network or --mirror-node-url, not both" >&2
  usage >&2
  exit 2
fi
if [[ -z "$NETWORK" && -z "$MN_URL" ]]; then
  echo "Error: one of --network or --mirror-node-url is required" >&2
  usage >&2
  exit 2
fi

if [[ -n "$NETWORK" ]]; then
  case "$NETWORK" in
    mainnet)    MN_URL="https://mainnet-public.mirrornode.hedera.com" ;;
    testnet)    MN_URL="https://testnet.mirrornode.hedera.com" ;;
    previewnet) MN_URL="https://previewnet.mirrornode.hedera.com" ;;
    *) echo "Error: unknown --network '$NETWORK' (expected mainnet|testnet|previewnet)" >&2; exit 2 ;;
  esac
fi

if [[ "$MN_URL" != http://* && "$MN_URL" != https://* ]]; then
  echo "Error: Mirror Node URL must start with http:// or https://, got: $MN_URL" >&2
  exit 2
fi

if [[ -n "$OUTPUT" && "$OUTPUT" != /* ]]; then
  echo "Error: --output must be an absolute path, got: $OUTPUT" >&2
  exit 2
fi

if ! [[ "$PAGE_SIZE" =~ ^[0-9]+$ ]] || (( PAGE_SIZE < 1 || PAGE_SIZE > 100 )); then
  echo "Error: --page-size must be an integer in [1,100], got: $PAGE_SIZE" >&2
  exit 2
fi

# --- pre-flight checks ------------------------------------------------------

for bin in curl jq; do
  if ! command -v "$bin" >/dev/null 2>&1; then
    echo "Error: required tool '$bin' not found in PATH" >&2
    exit 1
  fi
done

# Normalize: strip trailing slash, then a trailing /api/v1/network/nodes if the
# caller included the full path. BASE_URL is the scheme://host[:port][/prefix]
# used both to build the first request and to resolve a relative links.next.
BASE_URL="${MN_URL%/}"
BASE_URL="${BASE_URL%/api/v1/network/nodes}"
# scheme://host[:port] only — relative links.next from MN are root-absolute
# (e.g. "/api/v1/network/nodes?...") so they must resolve against the origin.
ORIGIN="$(printf '%s' "$BASE_URL" | sed -E 's#^(https?://[^/]+).*#\1#')"

# --- fetch + paginate -------------------------------------------------------

# curl with conservative timeouts and retries, matching the plugin's intent
# (connect 5s, read/total bounded, 3 retries with backoff).
curl_get() {
  curl -fsS \
    --connect-timeout 5 \
    --max-time 30 \
    --retry 3 \
    --retry-delay 1 \
    --retry-connrefused \
    -H 'Accept: application/json' \
    "$1"
}

next_url="${BASE_URL}/api/v1/network/nodes?limit=${PAGE_SIZE}&order=desc"
all_entries="[]"
page_count=0

while [[ -n "$next_url" ]]; do
  page_count=$((page_count + 1))

  if ! body="$(curl_get "$next_url")"; then
    echo "Error: Mirror Node request failed: $next_url" >&2
    exit 1
  fi

  if ! jq -e . >/dev/null 2>&1 <<<"$body"; then
    echo "Error: Mirror Node returned non-JSON for: $next_url" >&2
    exit 1
  fi

  # Per-page extraction:
  #   * keep entries where timestamp.to is null OR blank (the active entry),
  #   * require a non-null, non-blank public_key,
  #   * strip a leading 0x and lowercase the hex,
  #   * emit {RSAPubKey, nodeId}.
  page_entries="$(jq -c '
    [ .nodes[]
      | select((.timestamp == null) or (.timestamp.to == null) or (.timestamp.to == ""))
      | select(.public_key != null and (.public_key | tostring | length) > 0)
      | { RSAPubKey: (.public_key | tostring | ltrimstr("0x") | ascii_downcase),
          nodeId:    .node_id }
    ]' <<<"$body")"

  all_entries="$(jq -c -n --argjson a "$all_entries" --argjson b "$page_entries" '$a + $b')"

  # Resolve links.next: null -> stop; "http..." -> absolute; "/path" -> origin-relative.
  raw_next="$(jq -r '.links.next // empty' <<<"$body")"
  if [[ -z "$raw_next" ]]; then
    next_url=""
  elif [[ "$raw_next" == http://* || "$raw_next" == https://* ]]; then
    next_url="$raw_next"
  else
    next_url="${ORIGIN}${raw_next}"
  fi
done

# --- assemble result --------------------------------------------------------

# De-duplicate by nodeId (defensive; keep first occurrence) and sort ascending.
result="$(jq -c '
  { nodeAddress:
      ( reduce .[] as $e ({seen:{}, out:[]};
          if .seen[($e.nodeId|tostring)] then .
          else .seen[($e.nodeId|tostring)] = true | .out += [$e] end)
        | .out
        | sort_by(.nodeId) )
  }' <<<"$all_entries")"

entry_count="$(jq '.nodeAddress | length' <<<"$result")"

if [[ "$entry_count" -eq 0 ]]; then
  echo "Error: no active nodes with a public_key found at ${BASE_URL} (queried ${page_count} page(s))." >&2
  echo "       Confirm the Mirror Node URL is reachable and serving /api/v1/network/nodes." >&2
  exit 1
fi

# --- write ------------------------------------------------------------------

if [[ -n "$OUTPUT" ]]; then
  out_dir="$(dirname "$OUTPUT")"
  if [[ ! -d "$out_dir" ]]; then
    echo "Error: output directory does not exist: $out_dir" >&2
    exit 1
  fi
  # Stage to a temp file and atomically move on success so a partial failure
  # never leaves a half-written file in place.
  tmp_out="$(mktemp "${OUTPUT}.XXXXXX")"
  trap '[[ -n "${tmp_out:-}" && -f "$tmp_out" ]] && rm -f "$tmp_out"; true' EXIT
  printf '%s\n' "$result" | jq . > "$tmp_out"
  mv -f "$tmp_out" "$OUTPUT"
  tmp_out=""  # disarm cleanup; mv already consumed it
  file_size="$(wc -c < "$OUTPUT" | tr -d ' ')"
  echo "Wrote ${entry_count} node entries to ${OUTPUT} (${file_size} bytes)." >&2
else
  printf '%s\n' "$result" | jq .
  echo "Wrote ${entry_count} node entries to stdout." >&2
fi
