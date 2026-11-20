#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Lists the release and rc tags (vX.Y.Z, vX.Y.Z-rcN) that belong to a branch, for
# compute-release-version.sh --branch-tags-file.
#   list-branch-tags.sh <ref, default HEAD>
#
# A tag belongs to the branch when it, or its parent, is an ancestor of <ref>. The parent rule is
# needed because `custom` pushes only the tag: the bump commit it points at is never on the
# branch, but its parent is. A tag cut from another branch (e.g. an rc from main) has neither.

set -euo pipefail

ref="${1:-HEAD}"
git tag -l 'v*' | grep -E '^v[0-9]+\.[0-9]+\.[0-9]+(-rc[0-9]+)?$' | while read -r tag; do
  if git merge-base --is-ancestor "$tag" "$ref" || git merge-base --is-ancestor "$tag^" "$ref"; then
    echo "$tag"
  fi
done
