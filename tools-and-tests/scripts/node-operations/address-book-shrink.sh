#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# address-book-shrink.sh
#
# Shrinks a Block Node RSA address book history file (rsa-bootstrap-roster.json, a JSON-encoded
# RangedAddressBookHistory). The whole history is returned by the serverStatusDetail API, so a large
# file makes every call slow (#3901).
#
# 1. Optional trim. When a block height n is given, every era ending before n is dropped. The era
#    containing n and all later eras are kept. Given eras [0,x], [x+1,y], [y+1,z] and
#    x+1 <= n <= y, era [0,x] is dropped. Only trim below blocks the node will never need to
#    verify again: blocks in dropped eras can no longer be verified.
# 2. Merge. Adjacent eras with identical address books (node order ignored) are collapsed into one
#    era covering both ranges, keeping the newer era's book. This loses no information.
#
# Before the file is replaced, every era boundary block at or above n is checked to resolve to the
# same address book as in the original. On any failure the original file is left untouched.
# A timestamped backup (<file>.bak-YYYYmmddHHMMSS) is written next to the file.
#
# Usage:
#   address-book-shrink.sh <address-book-file> [block-height]
#
# Requires: jq
#
# The Block Node reads this file only at startup, so restart it after shrinking the file:
#   1. Copy the file from the application-state volume, e.g.
#        kubectl cp <ns>/<pod>:/opt/hiero/block-node/application-state/rsa-bootstrap-roster.json ./rsa-bootstrap-roster.json
#   2. address-book-shrink.sh ./rsa-bootstrap-roster.json [block-height]
#   3. Copy the shrunk file back to the same path and restart the Block Node.
#   4. Check that the blocknode_roster_eras_loaded metric matches the era count printed by this script.

set -euo pipefail

usage() {
  sed -n '/^# Usage:/,/^# Requires:/p' "$0" | sed '$d; s/^# \{0,1\}//'
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi
if [[ $# -lt 1 || $# -gt 2 ]]; then
  usage >&2
  exit 1
fi

file="$1"
height="${2:-0}"

command -v jq >/dev/null || { echo "jq is required" >&2; exit 1; }
[[ -f "$file" ]] || { echo "File not found: $file" >&2; exit 1; }
[[ "$height" =~ ^[0-9]+$ ]] || { echo "Block height must be a non-negative integer: $height" >&2; exit 1; }

# Eras must be non-empty, ordered and contiguous, with only the last one open-ended (-1).
jq -e '
  .addressBooks as $a
  | ($a | type == "array" and length > 0)
    and all(range(0; $a | length - 1); ($a[.].endBlock | tonumber) != -1)
    and all(range(1; $a | length);
        ($a[.].startBlock | tonumber) == ($a[. - 1].endBlock | tonumber) + 1)
' "$file" >/dev/null 2>&1 || { echo "addressBooks missing, unordered or not contiguous in $file" >&2; exit 1; }

# endBlock -1 is the open-ended sentinel. Block numbers may be JSON strings (PBJ int64) or numbers.
shrink_filter='
  def book: .addressBook.nodeAddress | sort_by(.nodeId | tonumber);
  .addressBooks |= (
    map(select((.endBlock | tonumber) == -1 or (.endBlock | tonumber) >= $h))
    | reduce .[] as $e ([];
        if length > 0 and ((.[-1] | book) == ($e | book))
        then .[-1] as $prev | .[-1] = ($e | .startBlock = $prev.startBlock)
        else . + [$e] end)
  )'

# Lists the boundary blocks >= $h whose address book differs between $orig and $new.
verify_filter='
  def lookup($hist; $n):
    [$hist.addressBooks[]
      | select((.startBlock | tonumber) <= $n
          and ((.endBlock | tonumber) == -1 or (.endBlock | tonumber) >= $n))]
    | if length == 1 then .[0].addressBook.nodeAddress | sort_by(.nodeId | tonumber) else "MISS" end;
  [$orig[0].addressBooks[] | (.startBlock | tonumber), (.endBlock | tonumber)]
  | map(select(. >= $h)) + [$h]
  | map(select(lookup($orig[0]; .) != lookup($new[0]; .)))'

tmp="$(mktemp "$(dirname "$file")/.address-book-shrink.XXXXXX")"
trap 'rm -f "$tmp"' EXIT
jq --argjson h "$height" "$shrink_filter" "$file" > "$tmp"

mismatches=$(jq -n -c --argjson h "$height" --slurpfile orig "$file" --slurpfile new "$tmp" "$verify_filter")
if [[ "$mismatches" != "[]" ]]; then
  echo "Verification failed, blocks resolving to a different address book: $mismatches (file untouched)" >&2
  exit 1
fi

backup="${file}.bak-$(date +%Y%m%d%H%M%S)"
cp -p "$file" "$backup"
echo "Backup written: $backup"

before_eras=$(jq '.addressBooks | length' "$file")
before_bytes=$(wc -c < "$file" | tr -d ' ')
# Write in place (not mv) so the file keeps its owner and permissions.
cat "$tmp" > "$file"
after_eras=$(jq '.addressBooks | length' "$file")
after_bytes=$(wc -c < "$file" | tr -d ' ')

echo "Eras:  $before_eras -> $after_eras"
echo "Bytes: $before_bytes -> $after_bytes"
echo "First era: $(jq -c '.addressBooks[0] | {startBlock, endBlock}' "$file")"
echo "Last era:  $(jq -c '.addressBooks[-1] | {startBlock, endBlock}' "$file")"
