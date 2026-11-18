#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Print the base version the node-operator lifecycle test installs before upgrading to <snapshot-version>.
#
# The base must be older than the snapshot, otherwise the upgrade is a downgrade that solo-provisioner rejects.
# On a release branch the newest release overall is usually a later minor line (e.g. 0.46.0 for release/0.44),
# so the newest release is only a valid base when it is below the snapshot.
#
# Candidates are the releases whose MAJOR.MINOR.PATCH is strictly below the snapshot's, in this order:
#   1. the highest published GA release,
#   2. the highest draft GA release (the tag push publishes the image before the release is published),
#   3. the highest published RC or alpha release.
#
# Usage: gh release list --limit 100 --json tagName,isDraft --jq '.[] | "\(.tagName) \(.isDraft)"' \
#          | select-base-version.sh <snapshot-version>
# Input lines are "<tag> <isDraft>", e.g. "v0.44.2 false". Exits non-zero with a ::error:: when nothing qualifies.
set -euo pipefail

SNAPSHOT_VERSION="${1:?snapshot version required}"
SNAPSHOT_VERSION="${SNAPSHOT_VERSION#v}"
SNAPSHOT_CORE="${SNAPSHOT_VERSION%%-*}"

if [[ ! "${SNAPSHOT_CORE}" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "::error::snapshot version '${SNAPSHOT_VERSION}' is not a MAJOR.MINOR.PATCH[-PRE_RELEASE] version" >&2
  exit 1
fi

is_below_snapshot() { # $1 = MAJOR.MINOR.PATCH
  [[ "$1" != "${SNAPSHOT_CORE}" ]] \
    && [[ "$(printf '%s\n%s\n' "$1" "${SNAPSHOT_CORE}" | sort -V | head -n 1)" == "$1" ]]
}

PUBLISHED_GA=()
DRAFT_GA=()
PUBLISHED_PRE_RELEASE=()
while read -r tag is_draft; do
  [[ -z "${tag}" ]] && continue
  version="${tag#v}"
  if [[ "${version}" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    is_below_snapshot "${version}" || continue
    if [[ "${is_draft}" == "true" ]]; then DRAFT_GA+=("${version}"); else PUBLISHED_GA+=("${version}"); fi
  elif [[ "${version}" =~ ^([0-9]+\.[0-9]+\.[0-9]+)-(rc|alpha)[0-9]+$ ]]; then
    is_below_snapshot "${BASH_REMATCH[1]}" || continue
    [[ "${is_draft}" == "true" ]] || PUBLISHED_PRE_RELEASE+=("${version}")
  fi
done

highest() { # prints the highest of the given versions
  printf '%s\n' "$@" | sort -V | tail -n 1
}

if [[ ${#PUBLISHED_GA[@]} -gt 0 ]]; then
  highest "${PUBLISHED_GA[@]}"
elif [[ ${#DRAFT_GA[@]} -gt 0 ]]; then
  highest "${DRAFT_GA[@]}"
elif [[ ${#PUBLISHED_PRE_RELEASE[@]} -gt 0 ]]; then
  highest "${PUBLISHED_PRE_RELEASE[@]}"
else
  echo "::error::no release below ${SNAPSHOT_VERSION} to use as the base version; set the base-version input" >&2
  exit 1
fi
