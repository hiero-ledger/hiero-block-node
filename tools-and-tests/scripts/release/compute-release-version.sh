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
#       [--branch-tags-file <file, rc/release tags on the dispatching branch, see list-branch-tags.sh>] \
#       [--new-commits-file <file of '<tag> <count>', see count-new-commits.sh>] \
#       [--custom-version <version>] [--allow-custom-without-release-branch true|false]
#
# Prints KEY=VALUE lines on stdout (COMPUTED_VERSION, and SOURCE_TAG for GA when an rc tag exists).
# Errors are reported as ::error:: on stderr with exit code 1.
#
# Rules:
#   rc      main: only rc1, and only if no release/X.Y branch or vX.Y.Z-rcN tag exists for the base version.
#           release/X.Y: highest existing vX.Y.Z-rcN tag + 1 (tags of other release lines are ignored).
#   alpha   main only: highest existing vX.Y.Z-alphaN tag + 1.
#   GA      release/X.Y only: built from the highest vX.Y.Z-rcN tag, or from the branch tip if there
#           is none. Fails if that tag is not in --branch-tags-file (see list-branch-tags.sh), so an
#           rc cut from another branch (e.g. main) can never become the GA source, and an older rc
#           is never used silently. Also fails if the branch has commits after that rc.
#   Patch   On release/X.Y, once a vX.Y.N release exists, rc and GA target the next patch
#           (highest vX.Y.N + 1, or version.txt if that is higher). rc and GA fail if the branch has
#           no commits since the last release tag.
#   custom  any branch. A plain X.Y.Z (final release) or an rc-shaped version must be dispatched
#           from release/X.Y. Both are waived by --allow-custom-without-release-branch true.
#           Other shapes (alpha, hotfix...) can be tagged from any branch.
# The computed tag must not already exist.

set -euo pipefail

fail() {
  echo "::error::$*" >&2
  exit 1
}

RELEASE_TYPE="" REF_NAME="" CURRENT="" CUSTOM_VERSION="" TAGS_FILE="" BRANCHES_FILE="" BRANCH_TAGS_FILE="" NEW_COMMITS_FILE="" ALLOW_CUSTOM="false"
while [[ $# -gt 0 ]]; do
  [[ $# -ge 2 ]] || fail "Missing value for '$1'"
  case "$1" in
    --release-type) RELEASE_TYPE="$2" ;;
    --ref-name) REF_NAME="$2" ;;
    --current-version) CURRENT="$2" ;;
    --custom-version) CUSTOM_VERSION="$2" ;;
    --tags-file) TAGS_FILE="$2" ;;
    --branches-file) BRANCHES_FILE="$2" ;;
    --branch-tags-file) BRANCH_TAGS_FILE="$2" ;;
    --new-commits-file) NEW_COMMITS_FILE="$2" ;;
    --allow-custom-without-release-branch) ALLOW_CUSTOM="$2" ;;
    *) fail "Unknown argument '$1'" ;;
  esac
  shift 2
done

[[ -n "$RELEASE_TYPE" && -n "$REF_NAME" && -f "$TAGS_FILE" && -f "$BRANCHES_FILE" ]] \
  || fail "--release-type, --ref-name, --tags-file and --branches-file are required"

has_line() { grep -qxF -- "$1" "$2"; }

# highest_n <base> <rc|alpha> [tags file]: highest N among v<base>-<kind>N tags, empty if none.
highest_n() {
  { grep -E "^v${1//./\\.}-$2[0-9]+$" "${3:-$TAGS_FILE}" || true; } | sed -E "s/^v.*-$2//" | sort -n | tail -1
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

# new_commits <tag>: commits on the branch since <tag> (see count-new-commits.sh).
new_commits() {
  [[ -f "$NEW_COMMITS_FILE" ]] || fail "--new-commits-file is required for this release"
  local n
  n="$(awk -v t="$1" '$1 == t { print $2; exit }' "$NEW_COMMITS_FILE")"
  [[ -n "$n" ]] || fail "No commit count for $1: it is not on '$REF_NAME'"
  echo "$n"
}

# On a release/X.Y branch, rc and GA target the next patch once a release exists (BASE becomes
# X.Y.<highest released patch + 1>, or version.txt's patch if higher). Fails with nothing to release.
resolve_patch() {
  local last patch="${BASE##*.}"
  last="$({ grep -E "^v${MAJOR}\.${MINOR}\.[0-9]+$" "$TAGS_FILE" || true; } | sed -E 's/^v[0-9]+\.[0-9]+\.//' | sort -n | tail -1)"
  if [[ -n "$last" ]]; then
    (( last + 1 > patch )) && patch=$((last + 1))
    [[ "$(new_commits "v$MAJOR.$MINOR.$last")" -gt 0 ]] \
      || fail "Nothing to release: '$REF_NAME' has no new commits since v$MAJOR.$MINOR.$last. Cherry-pick the fixes first."
  fi
  BASE="$MAJOR.$MINOR.$patch"
}

SOURCE_TAG=""
case "$RELEASE_TYPE" in
  custom)
    [[ -n "$CUSTOM_VERSION" ]] || fail "custom_version input is required when release_type is 'custom'"
    re='^([0-9]+)\.([0-9]+)\.[0-9]+(-[0-9A-Za-z.-]+)?$'
    [[ "$CUSTOM_VERSION" =~ $re ]] || fail "custom_version '$CUSTOM_VERSION' is not a valid version"
    VERSION="$CUSTOM_VERSION"
    line="release/${BASH_REMATCH[1]}.${BASH_REMATCH[2]}"
    if [[ "$ALLOW_CUSTOM" != "true" ]]; then
      pre="${BASH_REMATCH[3]}"
      if [[ -z "$pre" || "$VERSION" =~ -rc[0-9]+$ ]]; then
        [[ "$REF_NAME" == "$line" ]] \
          || fail "custom '$VERSION' must be dispatched from $line, not '$REF_NAME'. Set allow_custom_without_release_branch to confirm this is intended."
      fi
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
      resolve_patch
      VERSION="${BASE}-rc$(($(highest_n "$BASE" rc || true) + 1))"
    fi
    ;;
  GA)
    require_release_branch "GA"
    resolve_patch
    VERSION="$BASE"
    [[ -f "$BRANCH_TAGS_FILE" ]] || fail "--branch-tags-file is required for GA"
    RC_N="$(highest_n "$BASE" rc)"
    if [[ -n "$RC_N" ]]; then
      SOURCE_TAG="v${BASE}-rc${RC_N}"
      has_line "$SOURCE_TAG" "$BRANCH_TAGS_FILE" \
        || fail "$SOURCE_TAG is the newest rc but is not on '$REF_NAME'. Refusing to build GA from it or from an older rc. Cut a new rc from $REF_NAME first."
      [[ "$(new_commits "$SOURCE_TAG")" -eq 0 ]] \
        || fail "'$REF_NAME' has commits after $SOURCE_TAG, which a GA built from it would drop. Cut a new rc from $REF_NAME first."
    fi
    ;;
  *) fail "Unknown release type '$RELEASE_TYPE'" ;;
esac

has_line "v$VERSION" "$TAGS_FILE" && fail "Tag v$VERSION already exists"

echo "COMPUTED_VERSION=$VERSION"
[[ -z "$SOURCE_TAG" ]] || echo "SOURCE_TAG=$SOURCE_TAG"
