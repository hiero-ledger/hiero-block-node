#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Tests for address-book-shrink.sh. Plain bash plus jq: run it directly.
#   tools-and-tests/scripts/node-operations/tests/test-address-book-shrink.sh

set -uo pipefail

SCRIPT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/address-book-shrink.sh"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
PASS=0 FAIL=0

# Eras: [0,10] book A, [11,20] book A with nodes reordered, [21,30] book B, [31,-1] book B.
fixture() {
  cat > "$1" <<'JSON'
{"addressBooks":[
 {"addressBook":{"nodeAddress":[{"RSAPubKey":"a3","nodeId":"3"},{"RSAPubKey":"a4","nodeId":"4"}]},"startBlock":"0","endBlock":"10"},
 {"addressBook":{"nodeAddress":[{"RSAPubKey":"a4","nodeId":"4"},{"RSAPubKey":"a3","nodeId":"3"}]},"startBlock":"11","endBlock":"20"},
 {"addressBook":{"nodeAddress":[{"RSAPubKey":"b3","nodeId":"3"},{"RSAPubKey":"b5","nodeId":"5"}]},"startBlock":"21","endBlock":"30"},
 {"addressBook":{"nodeAddress":[{"RSAPubKey":"b3","nodeId":"3"},{"RSAPubKey":"b5","nodeId":"5"}]},"startBlock":"31","endBlock":"-1"}]}
JSON
}

# edit <file> <jq filter>: rewrites the fixture in place.
edit() { jq "$2" "$1" > "$1.tmp" && mv "$1.tmp" "$1"; }

# ranges <file>: era ranges as a compact JSON array, e.g. ["0-20","21--1"].
ranges() { jq -c '[.addressBooks[] | "\(.startBlock)-\(.endBlock)"]' "$1"; }

# check <name> <condition result 0|1> <detail on failure>
check() {
  if [[ $2 -eq 0 ]]; then PASS=$((PASS + 1)); else FAIL=$((FAIL + 1)); echo "FAIL $1: $3"; fi
}

# expect_ranges <name> <file> <expected ranges> [script args after the file...]
expect_ranges() {
  local name="$1" f="$2" want="$3"; shift 3
  "$SCRIPT" "$f" "$@" > "$WORK/out" 2>&1
  local code=$? got; got=$(ranges "$f")
  [[ $code -eq 0 && "$got" == "$want" ]]
  check "$name" $? "code=$code want=$want got=$got out='$(cat "$WORK/out")'"
}

# expect_reject <name> <file> <expected error substring> [script args...]
expect_reject() {
  local name="$1" f="$2" want="$3"; shift 3
  local before; before=$(cat "$f" 2>/dev/null || true)
  "$SCRIPT" "$@" > /dev/null 2> "$WORK/err"
  local code=$?
  [[ $code -ne 0 ]] && grep -qF -- "$want" "$WORK/err" \
    && [[ "$(cat "$f" 2>/dev/null || true)" == "$before" ]] && ! ls "$f".bak-* > /dev/null 2>&1
  check "$name" $? "code=$code want error '$want' got '$(cat "$WORK/err")' (file must be unchanged, no backup)"
}

# --- merge without trim -----------------------------------------------------------------------
f="$WORK/merge.json"; fixture "$f"
expect_ranges "identical neighbours merge, node order ignored" "$f" '["0-20","21--1"]'
[[ $(jq -r '.addressBooks[0].addressBook.nodeAddress[0].nodeId' "$f") == 4 ]]
check "merged era keeps the newer book" $? "older book kept"

f="$WORK/zero.json"; fixture "$f"
expect_ranges "block height 0 only merges" "$f" '["0-20","21--1"]'

f="$WORK/desc.json"; fixture "$f"
edit "$f" '.addressBooks[1].addressBook.nodeAddress[0].description = "changed"'
expect_ranges "books differing in a non-key field are not merged" "$f" '["0-10","11-20","21--1"]'

f="$WORK/numbers.json"; fixture "$f"
edit "$f" '.addressBooks[].startBlock |= tonumber | .addressBooks[].endBlock |= tonumber'
expect_ranges "numeric block numbers are accepted" "$f" '["0-20","21--1"]'

f="$WORK/again.json"; fixture "$f"; "$SCRIPT" "$f" > /dev/null; cp "$f" "$WORK/again.once"
"$SCRIPT" "$f" > /dev/null
cmp -s "$f" "$WORK/again.once"
check "running twice changes nothing more" $? "second run changed the file"

# --- trim at era boundaries -------------------------------------------------------------------
# Eras [0,10], [11,20], [21,30], [31,-1]: an era is dropped only when it ends before the height.
for case in '10 ["0-20","21--1"]' '11 ["11-20","21--1"]' '20 ["11-20","21--1"]' \
  '21 ["21--1"]' '1000 ["31--1"]'; do
  h="${case%% *}" want="${case#* }"
  f="$WORK/trim-$h.json"; fixture "$f"
  expect_ranges "trim at $h" "$f" "$want" "$h"
done

# --- backup and file metadata -----------------------------------------------------------------
f="$WORK/backup.json"; fixture "$f"; chmod 640 "$f"; cp "$f" "$WORK/backup.orig"
"$SCRIPT" "$f" 11 > /dev/null
bak=$(ls "$f".bak-* 2> /dev/null | head -1)
[[ -n "$bak" ]] && cmp -s "$bak" "$WORK/backup.orig" && [[ $(ls -l "$f" | cut -c1-10) == "-rw-r-----" ]]
check "backup matches the original and permissions are kept" $? "bak='$bak' mode=$(ls -l "$f" | cut -c1-10)"

# --- input validation -------------------------------------------------------------------------
f="$WORK/bad.json"; fixture "$f"
expect_reject "no arguments" "$f" "Usage:"
expect_reject "too many arguments" "$f" "Usage:" "$f" 1 2
expect_reject "missing file" "$WORK/nope.json" "File not found" "$WORK/nope.json"
expect_reject "non-numeric height" "$f" "must be a non-negative integer" "$f" abc
expect_reject "negative height" "$f" "must be a non-negative integer" "$f" -5

f="$WORK/gap.json"; fixture "$f"; edit "$f" '.addressBooks[2].startBlock = "22"'
expect_reject "gap between eras" "$f" "not contiguous" "$f"
f="$WORK/overlap.json"; fixture "$f"; edit "$f" '.addressBooks[2].startBlock = "20"'
expect_reject "overlapping eras" "$f" "not contiguous" "$f"
f="$WORK/open.json"; fixture "$f"; edit "$f" '.addressBooks[1].endBlock = "-1"'
expect_reject "open-ended era that is not last" "$f" "not contiguous" "$f"
f="$WORK/empty.json"; echo '{"addressBooks":[]}' > "$f"
expect_reject "empty history" "$f" "addressBooks missing" "$f"
f="$WORK/single.json"; echo '{"nodeAddress":[{"RSAPubKey":"a3","nodeId":"3"}]}' > "$f"
expect_reject "legacy single address book file" "$f" "addressBooks missing" "$f"
f="$WORK/notjson.json"; echo 'not json' > "$f"
expect_reject "invalid JSON" "$f" "addressBooks missing" "$f"

# --- verification safety net -----------------------------------------------------------------
# A correct merge never trips the verification, so a fake jq first on PATH stands in for a faulty
# one: it runs the real jq and applies $CORRUPT to the output of the shrink step only (the call
# whose filter merges with "reduce"). Every other jq call is passed through unchanged.
mkdir -p "$WORK/fake-jq"
cat > "$WORK/fake-jq/jq" <<EOF
#!/usr/bin/env bash
for arg in "\$@"; do
  if [[ "\$arg" == *"reduce .[] as \\\$e"* ]]; then
    "$(command -v jq)" "\$@" | "$(command -v jq)" "\$CORRUPT"
    exit "\${PIPESTATUS[0]}"
  fi
done
exec "$(command -v jq)" "\$@"
EOF
chmod +x "$WORK/fake-jq/jq"

f="$WORK/safety-key.json"; fixture "$f"
PATH="$WORK/fake-jq:$PATH" CORRUPT='.addressBooks[-1].addressBook.nodeAddress[0].RSAPubKey = "x"' \
  expect_reject "verification rejects a changed RSA key" "$f" "Verification failed" "$f"
f="$WORK/safety-dropped.json"; fixture "$f"
PATH="$WORK/fake-jq:$PATH" CORRUPT='.addressBooks |= .[1:]' \
  expect_reject "verification rejects a dropped era" "$f" "Verification failed" "$f" 0
f="$WORK/safety-shortened.json"; fixture "$f"
PATH="$WORK/fake-jq:$PATH" CORRUPT='.addressBooks[0].endBlock = "15"' \
  expect_reject "verification rejects a shortened era" "$f" "Verification failed" "$f" 5
f="$WORK/safety-control.json"; fixture "$f"
PATH="$WORK/fake-jq:$PATH" CORRUPT='.' expect_ranges "fake jq passes a correct merge through" \
  "$f" '["0-20","21--1"]'

"$SCRIPT" --help | grep -qF "address-book-shrink.sh <address-book-file> [block-height]"
check "--help prints usage" $? "usage missing"

echo "passed=$PASS failed=$FAIL"
[[ $FAIL -eq 0 ]]
