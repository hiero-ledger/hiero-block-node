#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# For each tag read from stdin, prints "<tag> <count>": the commits on <ref> since the tag,
# for compute-release-version.sh --new-commits-file.
#   list-branch-tags.sh <ref> | count-new-commits.sh <ref>
#
# Counted from the tag's parent, because tag-only releases (custom) point at a bump commit that is
# not on the branch. Merge commits and "Bump versions" commits are not counted.

set -euo pipefail

ref="${1:-HEAD}"
while read -r tag; do
  [[ -n "$tag" ]] || continue
  echo "$tag $(git rev-list --count --no-merges --invert-grep --grep='^Bump versions' "$tag^..$ref")"
done
