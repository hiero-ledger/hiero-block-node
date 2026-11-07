#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Tests for compute-release-version.sh. Plain bash, no extra tooling: run it directly.
#   tools-and-tests/scripts/release/test-compute-release-version.sh

set -uo pipefail

SCRIPT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/compute-release-version.sh"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
PASS=0 FAIL=0

# State shared by the scenarios below. ON_BRANCH_TAGS (tags reachable from the dispatching
# branch) defaults to TAGS unless a scenario sets ON_BRANCH_SET.
TAGS=() BRANCHES=() ON_BRANCH_TAGS=() ON_BRANCH_SET=""
# NEW_COMMITS ("<tag> <count>" lines) defaults to 0 for rc tags and 1 for release tags unless set.
NEW_COMMITS=() NEW_COMMITS_SET=""

# run <release-type> <ref> <version.txt> [extra script args...]; sets OUT (stdout) and CODE.
run() {
  printf '%s\n' "${TAGS[@]:-}" > "$WORK/tags"
  printf '%s\n' "${BRANCHES[@]:-}" > "$WORK/branches"
  if [[ -n "$NEW_COMMITS_SET" ]]; then printf '%s\n' "${NEW_COMMITS[@]:-}" > "$WORK/new-commits"
  else
    : > "$WORK/new-commits"
    for t in "${TAGS[@]:-}"; do
      [[ -z "$t" ]] || { [[ "$t" == *-rc* ]] && echo "$t 0" || echo "$t 1"; } >> "$WORK/new-commits"
    done
  fi
  if [[ -n "$ON_BRANCH_SET" ]]; then printf '%s\n' "${ON_BRANCH_TAGS[@]:-}" > "$WORK/branch-tags"
  else printf '%s\n' "${TAGS[@]:-}" > "$WORK/branch-tags"; fi
  OUT=$("$SCRIPT" --release-type "$1" --ref-name "$2" --current-version "$3" \
    --tags-file "$WORK/tags" --branches-file "$WORK/branches" --branch-tags-file "$WORK/branch-tags" \
    --new-commits-file "$WORK/new-commits" "${@:4}" 2>"$WORK/err")
  CODE=$?
}

# expect_ok <name> <expected output line...>
expect_ok() {
  local name="$1"; shift
  local want; want="$(printf '%s\n' "$@")"
  if [[ $CODE -eq 0 && "$OUT" == "$want" ]]; then PASS=$((PASS + 1)); else
    FAIL=$((FAIL + 1)); echo "FAIL $name: code=$CODE want='$want' got='$OUT' err='$(cat "$WORK/err")'"
  fi
}

# expect_fail <name> <substring expected in the error>
expect_fail() {
  if [[ $CODE -ne 0 ]] && grep -qF -- "$2" "$WORK/err"; then PASS=$((PASS + 1)); else
    FAIL=$((FAIL + 1)); echo "FAIL $1: code=$CODE want error containing '$2' got '$(cat "$WORK/err")'"
  fi
}

# --- rc from a release branch ---------------------------------------------------------------
# The reported incident: version.txt stale at rc1 while rc2-rc4 exist only as tags.
TAGS=(v0.45.0-rc1 v0.45.0-rc2 v0.45.0-rc3 v0.45.0-rc4 v0.46.0-rc3) BRANCHES=(main release/0.45 release/0.46)
run rc release/0.45 0.45.0-rc1
expect_ok "rc uses highest tag, not version.txt" COMPUTED_VERSION=0.45.0-rc5

TAGS=(v0.45.0-rc1 v0.46.0-rc3) BRANCHES=(main release/0.45 release/0.46)
run rc release/0.45 0.45.0-rc1
expect_ok "rc ignores tags of other release lines" COMPUTED_VERSION=0.45.0-rc2

TAGS=() BRANCHES=(main release/0.45)
run rc release/0.45 0.45.0-SNAPSHOT
expect_ok "rc with no tags starts at rc1" COMPUTED_VERSION=0.45.0-rc1

run rc release/0.46 0.45.0-rc1
expect_fail "rc rejects version.txt from another line" "does not match branch"

run rc feature/foo 0.45.0-rc1
expect_fail "rc rejects non-release branch" "must be dispatched from a release/X.Y branch"

# --- rc from main -----------------------------------------------------------------------------
TAGS=() BRANCHES=(main)
run rc main 0.45.0-SNAPSHOT
expect_ok "rc1 from main" COMPUTED_VERSION=0.45.0-rc1

BRANCHES=(main release/0.45)
run rc main 0.45.0-SNAPSHOT
expect_fail "rc from main rejected once release branch exists" "release/0.45 already exists"

TAGS=(v0.45.0-rc1) BRANCHES=(main)
run rc main 0.45.0-rc1
expect_fail "rc from main rejected once rc tags exist" "rc tags for 0.45.0 already exist"

# --- alpha ------------------------------------------------------------------------------------
TAGS=(v0.46.0-alpha1 v0.46.0-alpha2 v0.46.0-alpha10 v0.45.0-alpha7) BRANCHES=(main)
run alpha main 0.46.0-SNAPSHOT
expect_ok "alpha uses highest numeric alpha tag of its base" COMPUTED_VERSION=0.46.0-alpha11

TAGS=()
run alpha main 0.46.0-SNAPSHOT
expect_ok "first alpha" COMPUTED_VERSION=0.46.0-alpha1

run alpha release/0.45 0.45.0-rc1
expect_fail "alpha rejected off main" "alpha must be dispatched from main"

# --- GA ---------------------------------------------------------------------------------------
TAGS=(v0.45.0-rc1 v0.45.0-rc2 v0.45.0-rc4 v0.46.0-rc9) BRANCHES=(main release/0.45)
run GA release/0.45 0.45.0-rc1
expect_ok "GA builds from highest rc tag" COMPUTED_VERSION=0.45.0 SOURCE_TAG=v0.45.0-rc4

TAGS=()
run GA release/0.45 0.45.0-SNAPSHOT
expect_ok "GA without rc tags falls back to branch tip" COMPUTED_VERSION=0.45.0

TAGS=()
run GA main 0.45.0-SNAPSHOT
expect_fail "GA rejected off main" "GA must be dispatched from a release/X.Y branch"

# An rc cut off-branch (the main-based v0.45.0-rc2 incident) must never be the GA source,
# and an older rc must never be used silently in place of a newer one.
TAGS=(v0.45.0-rc1 v0.45.0-rc2 v0.45.0-rc3) ON_BRANCH_TAGS=(v0.45.0-rc1 v0.45.0-rc3) ON_BRANCH_SET=1
run GA release/0.45 0.45.0-rc3
expect_ok "GA ignores a lower off-branch rc" COMPUTED_VERSION=0.45.0 SOURCE_TAG=v0.45.0-rc3

ON_BRANCH_TAGS=(v0.45.0-rc1)
run GA release/0.45 0.45.0-rc1
expect_fail "GA fails when the newest rc is off-branch" "v0.45.0-rc3 is the newest rc but is not on 'release/0.45'"

TAGS=(v0.45.0-rc2) ON_BRANCH_TAGS=()
run GA release/0.45 0.45.0-rc1
expect_fail "GA fails when every rc is off-branch" "v0.45.0-rc2 is the newest rc but is not on 'release/0.45'"
ON_BRANCH_TAGS=() ON_BRANCH_SET=""

TAGS=(v0.45.0-rc1)
printf '%s\n' "${TAGS[@]}" > "$WORK/tags"; printf 'release/0.45\n' > "$WORK/branches"
OUT=$("$SCRIPT" --release-type GA --ref-name release/0.45 --current-version 0.45.0-rc1 \
  --tags-file "$WORK/tags" --branches-file "$WORK/branches" 2>"$WORK/err"); CODE=$?
expect_fail "GA requires --branch-tags-file" "--branch-tags-file is required for GA"

# --- patch releases on a line that already has a release ---------------------------------------
# version.txt is stale at 0.45.0 because custom patch tags (v0.45.1) never update it.
TAGS=(v0.45.0-rc5 v0.45.0 v0.45.1) BRANCHES=(main release/0.45)
run GA release/0.45 0.45.0
expect_ok "GA after a release targets the next patch" COMPUTED_VERSION=0.45.2
run rc release/0.45 0.45.0
expect_ok "rc after a release targets the next patch" COMPUTED_VERSION=0.45.2-rc1

TAGS=(v0.45.0 v0.45.1 v0.45.2)
run GA release/0.45 0.45.2
expect_ok "GA counts every released patch" COMPUTED_VERSION=0.45.3

TAGS=(v0.44.3)
run GA release/0.44 0.44.3-SNAPSHOT
expect_ok "GA after a SNAPSHOT of a released version targets the next patch" COMPUTED_VERSION=0.44.4

TAGS=(v0.45.0 v0.45.1 v0.45.2-rc1)
run rc release/0.45 0.45.2-rc1
expect_ok "next patch rc counts its own rc tags" COMPUTED_VERSION=0.45.2-rc2
TAGS=(v0.45.0 v0.45.1 v0.45.2-rc1)
run GA release/0.45 0.45.2-rc1
expect_ok "patch GA builds from its rc" COMPUTED_VERSION=0.45.2 SOURCE_TAG=v0.45.2-rc1

TAGS=(v0.45.0 v0.46.0)
run GA release/0.45 0.45.0
expect_ok "patch numbering ignores other release lines" COMPUTED_VERSION=0.45.1

TAGS=(v0.45.0 v0.45.1) NEW_COMMITS=("v0.45.0 5" "v0.45.1 0") NEW_COMMITS_SET=1
run GA release/0.45 0.45.0
expect_fail "GA fails with nothing to release" "Nothing to release: 'release/0.45' has no new commits since v0.45.1"
run rc release/0.45 0.45.0
expect_fail "rc fails with nothing to release" "Nothing to release"

NEW_COMMITS=("v0.45.0 5")
run GA release/0.45 0.45.0
expect_fail "GA fails without a commit count for the last release" "No commit count for v0.45.1"
NEW_COMMITS_SET=""

# Cherry-picks after the source rc would be dropped by a GA built from it.
TAGS=(v0.45.0 v0.45.1 v0.45.2-rc1) NEW_COMMITS=("v0.45.1 4" "v0.45.2-rc1 2") NEW_COMMITS_SET=1
run GA release/0.45 0.45.2-rc1
expect_fail "GA fails with commits after the source rc" "has commits after v0.45.2-rc1"
NEW_COMMITS=("v0.45.1 4" "v0.45.2-rc1 0")
run GA release/0.45 0.45.2-rc1
expect_ok "GA allowed when nothing follows the source rc" COMPUTED_VERSION=0.45.2 SOURCE_TAG=v0.45.2-rc1
NEW_COMMITS=() NEW_COMMITS_SET=""

# First release of a line: nothing to compare against, so no commit guards apply.
TAGS=(v0.46.0-rc1) BRANCHES=(main release/0.46) NEW_COMMITS=("v0.46.0-rc1 0") NEW_COMMITS_SET=1
run GA release/0.46 0.46.0-rc1
expect_ok "first GA of a line" COMPUTED_VERSION=0.46.0 SOURCE_TAG=v0.46.0-rc1
NEW_COMMITS=() NEW_COMMITS_SET=""

# --- custom -----------------------------------------------------------------------------------
TAGS=(v0.45.0-rc4) BRANCHES=(main release/0.45)
run custom release/0.45 0.45.0-rc1 --custom-version 0.45.0-rc5
expect_ok "custom rc with release branch" COMPUTED_VERSION=0.45.0-rc5
TAGS=(v0.45.0-rc4 v0.45.0-rc5)
run rc release/0.45 0.45.0-rc1
expect_ok "next rc counts custom rc tags" COMPUTED_VERSION=0.45.0-rc6

# A plain X.Y.Z publishes a final release, so it must come from release/X.Y.
TAGS=() BRANCHES=(main release/0.45)
run custom main 0.45.0-SNAPSHOT --custom-version 0.45.0
expect_fail "custom X.Y.Z rejected from main" "must be dispatched from release/0.45, not 'main'"
run custom release/0.44 0.44.0-rc1 --custom-version 0.45.0
expect_fail "custom X.Y.Z rejected from another release line" "must be dispatched from release/0.45, not 'release/0.44'"
run custom release/0.45 0.45.0-rc1 --custom-version 0.45.0
expect_ok "custom X.Y.Z from its release branch" COMPUTED_VERSION=0.45.0
run custom main 0.45.0-SNAPSHOT --custom-version 0.45.0 --allow-custom-without-release-branch true
expect_ok "custom X.Y.Z override flag waives the branch check" COMPUTED_VERSION=0.45.0

# The rc2 incident: custom rc from main while release/0.45 exists.
TAGS=(v0.45.0-rc1) BRANCHES=(main release/0.45)
run custom main 0.46.0-SNAPSHOT --custom-version 0.45.0-rc2
expect_fail "custom rc rejected from main even when release branch exists" "must be dispatched from release/0.45, not 'main'"
run custom release/0.46 0.46.0-rc1 --custom-version 0.45.0-rc2
expect_fail "custom rc rejected from another release line" "must be dispatched from release/0.45, not 'release/0.46'"
run custom main 0.46.0-SNAPSHOT --custom-version 0.45.0-rc2 --allow-custom-without-release-branch true
expect_ok "custom rc override flag waives the branch check" COMPUTED_VERSION=0.45.0-rc2

TAGS=() BRANCHES=(main)
run custom main 0.47.0-SNAPSHOT --custom-version 0.47.0-rc1
expect_fail "custom rc without release branch needs the flag" "allow_custom_without_release_branch"
run custom main 0.47.0-SNAPSHOT --custom-version 0.47.0-rc1 --allow-custom-without-release-branch false
expect_fail "custom rc flag=false still rejected" "allow_custom_without_release_branch"
run custom main 0.47.0-SNAPSHOT --custom-version 0.47.0-rc1 --allow-custom-without-release-branch true
expect_ok "custom rc without release branch with the flag" COMPUTED_VERSION=0.47.0-rc1

run custom main 0.47.0-SNAPSHOT --custom-version 0.47.0-alpha3
expect_ok "custom alpha, and non-rc shapes, need no release branch" COMPUTED_VERSION=0.47.0-alpha3
TAGS=(v0.47.0-alpha3 v0.47.0-alpha4)
run alpha main 0.47.0-SNAPSHOT
expect_ok "next alpha counts custom alpha tags" COMPUTED_VERSION=0.47.0-alpha5

TAGS=(v0.47.0-alpha3)
run custom main 0.47.0-SNAPSHOT --custom-version 0.47.0-alpha3
expect_fail "custom rejected when tag exists" "Tag v0.47.0-alpha3 already exists"

run custom main 0.47.0-SNAPSHOT
expect_fail "custom requires custom_version" "custom_version input is required"

run custom main 0.47.0-SNAPSHOT --custom-version not-a-version
expect_fail "custom rejects malformed version" "is not a valid version"

# Other custom shapes can be tagged from any branch.
TAGS=() BRANCHES=(main release/0.45)
run custom feature/foo 0.45.0-SNAPSHOT --custom-version 0.45.2-hotfix.1
expect_ok "custom hotfix shape from any branch" COMPUTED_VERSION=0.45.2-hotfix.1
TAGS=(v0.45.2)
run custom release/0.45 0.45.0 --custom-version 0.45.2
expect_fail "custom X.Y.Z rejected when tag exists" "Tag v0.45.2 already exists"

# --- full lifecycle: each step feeds its tag into the next, as the real workflow would ----------
# rc1 from main -> rc2 -> GA -> custom patch (tag-only, version.txt stays stale) -> GA patches.
TAGS=() BRANCHES=(main)
run rc main 0.45.0-SNAPSHOT
expect_ok "lifecycle: rc1 from main" COMPUTED_VERSION=0.45.0-rc1
TAGS=(v0.45.0-rc1) BRANCHES=(main release/0.45)
run rc release/0.45 0.45.0-rc1
expect_ok "lifecycle: rc2 from the release branch" COMPUTED_VERSION=0.45.0-rc2
TAGS=(v0.45.0-rc1 v0.45.0-rc2)
run GA release/0.45 0.45.0-rc2
expect_ok "lifecycle: first GA builds from the newest rc" COMPUTED_VERSION=0.45.0 SOURCE_TAG=v0.45.0-rc2
TAGS=(v0.45.0-rc1 v0.45.0-rc2 v0.45.0)
run custom release/0.45 0.45.0 --custom-version 0.45.1
expect_ok "lifecycle: custom patch after the GA" COMPUTED_VERSION=0.45.1
TAGS=(v0.45.0-rc1 v0.45.0-rc2 v0.45.0 v0.45.1)
run GA release/0.45 0.45.0
expect_ok "lifecycle: GA after a custom patch skips to the next patch" COMPUTED_VERSION=0.45.2
TAGS=(v0.45.0-rc1 v0.45.0-rc2 v0.45.0 v0.45.1 v0.45.2)
run GA release/0.45 0.45.2
expect_ok "lifecycle: second GA patch" COMPUTED_VERSION=0.45.3
run rc release/0.45 0.45.2
expect_ok "lifecycle: rc for the next patch" COMPUTED_VERSION=0.45.3-rc1
run rc main 0.46.0-SNAPSHOT
expect_ok "lifecycle: the next minor still starts on main" COMPUTED_VERSION=0.46.0-rc1

# --- input validation -------------------------------------------------------------------------
# A flag with no value must be a clean ::error::, not an unbound-variable crash under set -u.
for flag in --release-type --ref-name --current-version --custom-version --tags-file \
            --branches-file --branch-tags-file --allow-custom-without-release-branch; do
  OUT=$("$SCRIPT" "$flag" 2>"$WORK/err"); CODE=$?
  expect_fail "missing value for $flag" "Missing value for '$flag'"
done
OUT=$("$SCRIPT" --bogus x 2>"$WORK/err"); CODE=$?
expect_fail "unknown argument" "Unknown argument '--bogus'"

run rc release/0.45 garbage
expect_fail "unparseable version.txt" "Unrecognised version format"

run nonsense main 0.45.0-SNAPSHOT
expect_fail "unknown release type" "Unknown release type"

# --- list-branch-tags.sh against a real git history ------------------------------------------
# Reproduces release/0.45: rc1 on the branch, rc2 tagged from main, rc3 a `custom` tag-only rc
# (its bump commit is never on the branch, only its parent is).
LIST="$(cd "$(dirname "$SCRIPT")" && pwd)/list-branch-tags.sh"
REPO="$WORK/repo"
git init -q -b main "$REPO"
g() { git -C "$REPO" -c user.name=t -c user.email=t@t -c commit.gpgsign=false -c tag.gpgsign=false "$@"; }
g commit -q --allow-empty -m base
g checkout -q -b release/0.45
g commit -q --allow-empty -m rc1; g tag v0.45.0-rc1
g checkout -q main
g commit -q --allow-empty -m main-work
g commit -q --allow-empty -m "bump rc2"; g tag v0.45.0-rc2
g checkout -q release/0.45
g commit -q --allow-empty -m cherry-pick
g checkout -q --detach
g commit -q --allow-empty -m "bump rc3"; g tag v0.45.0-rc3
g checkout -q release/0.45
g tag v0.45.0-alpha1
g commit -q --allow-empty -m "Bump versions for v0.45.0"; g tag v0.45.0
g commit -q --allow-empty -m "cherry-pick after GA"
g checkout -q --detach
g commit -q --allow-empty -m "Bump versions for v0.45.1"; g tag v0.45.1
g checkout -q release/0.45
g commit -q --allow-empty -m "second cherry-pick"
OUT=$(cd "$REPO" && "$LIST" release/0.45 | sort | tr '\n' ' ')
if [[ "$OUT" == "v0.45.0 v0.45.0-rc1 v0.45.0-rc3 v0.45.1 " ]]; then PASS=$((PASS + 1)); else
  FAIL=$((FAIL + 1)); echo "FAIL list-branch-tags: want 'v0.45.0 v0.45.0-rc1 v0.45.0-rc3 v0.45.1 ' got '$OUT'"; fi

# count-new-commits.sh: counted from the tag's parent, ignoring bump and merge commits. The
# tag-only v0.45.1 points at an off-branch bump commit whose parent (the cherry-pick) is on it.
COUNT="$(cd "$(dirname "$SCRIPT")" && pwd)/count-new-commits.sh"
OUT=$(cd "$REPO" && printf 'v0.45.0-rc1\nv0.45.0-rc3\nv0.45.0\nv0.45.1\n' | "$COUNT" release/0.45 | tr '\n' ',')
if [[ "$OUT" == "v0.45.0-rc1 4,v0.45.0-rc3 2,v0.45.0 2,v0.45.1 1," ]]; then PASS=$((PASS + 1)); else
  FAIL=$((FAIL + 1)); echo "FAIL count-new-commits: got '$OUT'"; fi

echo "passed=$PASS failed=$FAIL"
[[ $FAIL -eq 0 ]]
