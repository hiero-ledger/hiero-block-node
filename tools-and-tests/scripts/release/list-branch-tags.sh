#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Lists the rc tags that belong to a branch, for compute-release-version.sh --branch-tags-file.
#   list-branch-tags.sh <ref, default HEAD>
#
# A tag belongs to the branch when it, or its parent, is an ancestor of <ref>. The parent rule is
# needed because `custom` pushes only the tag: the bump commit it points at is never on the
# branch, but its parent is. An rc cut from another branch (e.g. main) has neither.

set -euo pipefail

ref="${1:-HEAD}"
git tag -l 'v*-rc*' | while read -r tag; do
  if git merge-base --is-ancestor "$tag" "$ref" || git merge-base --is-ancestor "$tag^" "$ref"; then
    echo "$tag"
  fi
done
