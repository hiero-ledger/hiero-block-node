#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Computes the next release version for .github/workflows/release-automation.yaml.
# Pure function of its inputs (no git or network access) so it can be unit tested.
#
# Usage:
#   compute-release-version.sh --release-type rc|alpha|GA|custom \
#       --ref-name <dispatching branch> --current-version <version.txt on that branch> \
#       --tags-file <file, one existing tag per line> \
#       --branches-file <file, one existing remote branch per line> \
#       [--custom-version <version>] [--allow-custom-without-release-branch true|false]
#
# Prints KEY=VALUE lines on stdout (COMPUTED_VERSION, and SOURCE_TAG for GA when an rc tag exists).
# Errors are reported as ::error:: on stderr with exit code 1.
#
# Rules:
#   rc      main: only rc1, and only if no release/X.Y branch or vX.Y.Z-rcN tag exists for the base version.
#           release/X.Y: highest existing vX.Y.Z-rcN tag + 1 (tags of other release lines are ignored).
#   alpha   main only: highest existing vX.Y.Z-alphaN tag + 1.
#   GA      release/X.Y only: built from the highest existing vX.Y.Z-rcN tag, if any.
#   custom  any branch. An rc-shaped version needs an existing release/X.Y branch unless
#           --allow-custom-without-release-branch is true.
# The computed tag must not already exist.

set -euo pipefail

fail() {
  echo "::error::$*" >&2
  exit 1
}

RELEASE_TYPE="" REF_NAME="" CURRENT="" CUSTOM_VERSION="" TAGS_FILE="" BRANCHES_FILE="" ALLOW_CUSTOM="false"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --release-type) RELEASE_TYPE="$2" ;;
    --ref-name) REF_NAME="$2" ;;
    --current-version) CURRENT="$2" ;;
    --custom-version) CUSTOM_VERSION="$2" ;;
    --tags-file) TAGS_FILE="$2" ;;
    --branches-file) BRANCHES_FILE="$2" ;;
    --allow-custom-without-release-branch) ALLOW_CUSTOM="$2" ;;
    *) fail "Unknown argument '$1'" ;;
  esac
  shift 2 || fail "Missing value for '$1'"
done

[[ -n "$RELEASE_TYPE" && -n "$REF_NAME" && -f "$TAGS_FILE" && -f "$BRANCHES_FILE" ]] \
  || fail "--release-type, --ref-name, --tags-file and --branches-file are required"

has_line() { grep -qxF -- "$1" "$2"; }

# highest_n <base> <rc|alpha>: highest N among existing v<base>-<kind>N tags, empty if none.
highest_n() {
  { grep -E "^v${1//./\\.}-$2[0-9]+$" "$TAGS_FILE" || true; } | sed -E "s/^v.*-$2//" | sort -n | tail -1
}

# Sets BASE, MAJOR and MINOR from the version.txt content of the dispatching branch.
parse_current() {
  local re='^([0-9]+)\.([0-9]+)\.([0-9]+)(-SNAPSHOT|-rc[0-9]+)?$'
  [[ "$CURRENT" =~ $re ]] || fail "Unrecognised version format '$CURRENT' in version.txt on $REF_NAME"
  MAJOR="${BASH_REMATCH[1]}" MINOR="${BASH_REMATCH[2]}" BASE="${BASH_REMATCH[1]}.${BASH_REMATCH[2]}.${BASH_REMATCH[3]}"
}

# On a release/X.Y branch, version.txt must belong to the same X.Y line.
require_release_branch() { # $1 = what is being run
  local re='^release/([0-9]+)\.([0-9]+)$'
  [[ "$REF_NAME" =~ $re ]] || fail "$1 must be dispatched from a release/X.Y branch, not '$REF_NAME'"
  local branch_line="${BASH_REMATCH[1]}.${BASH_REMATCH[2]}"
  parse_current
  [[ "$branch_line" == "$MAJOR.$MINOR" ]] \
    || fail "version.txt ($CURRENT) does not match branch '$REF_NAME'"
}

SOURCE_TAG=""
case "$RELEASE_TYPE" in
  custom)
    [[ -n "$CUSTOM_VERSION" ]] || fail "custom_version input is required when release_type is 'custom'"
    re='^([0-9]+)\.([0-9]+)\.[0-9]+(-[0-9A-Za-z.-]+)?$'
    [[ "$CUSTOM_VERSION" =~ $re ]] || fail "custom_version '$CUSTOM_VERSION' is not a valid version"
    VERSION="$CUSTOM_VERSION"
    if [[ "$VERSION" =~ -rc[0-9]+$ && "$ALLOW_CUSTOM" != "true" ]]; then
      [[ "$VERSION" =~ $re ]] # re-match to get major/minor (the -rc test above overwrote BASH_REMATCH)
      line="release/${BASH_REMATCH[1]}.${BASH_REMATCH[2]}"
      has_line "$line" "$BRANCHES_FILE" \
        || fail "custom rc '$VERSION' has no $line branch. Set allow_custom_without_release_branch to confirm this is intended."
    fi
    ;;
  alpha)
    [[ "$REF_NAME" == "main" ]] || fail "alpha must be dispatched from main, not '$REF_NAME'"
    parse_current
    VERSION="${BASE}-alpha$(($(highest_n "$BASE" alpha || true) + 1))"
    ;;
  rc)
    if [[ "$REF_NAME" == "main" ]]; then
      parse_current
      has_line "release/$MAJOR.$MINOR" "$BRANCHES_FILE" \
        && fail "release/$MAJOR.$MINOR already exists. Run rc from that branch; only rc1 may be tagged from main."
      [[ -z "$(highest_n "$BASE" rc)" ]] \
        || fail "rc tags for $BASE already exist. Run rc from release/$MAJOR.$MINOR; only rc1 may be tagged from main."
      VERSION="${BASE}-rc1"
    else
      require_release_branch "rc"
      VERSION="${BASE}-rc$(($(highest_n "$BASE" rc || true) + 1))"
    fi
    ;;
  GA)
    require_release_branch "GA"
    VERSION="$BASE"
    RC_N="$(highest_n "$BASE" rc)"
    [[ -z "$RC_N" ]] || SOURCE_TAG="v${BASE}-rc${RC_N}"
    ;;
  *) fail "Unknown release type '$RELEASE_TYPE'" ;;
esac

has_line "v$VERSION" "$TAGS_FILE" && fail "Tag v$VERSION already exists"

echo "COMPUTED_VERSION=$VERSION"
[[ -z "$SOURCE_TAG" ]] || echo "SOURCE_TAG=$SOURCE_TAG"
