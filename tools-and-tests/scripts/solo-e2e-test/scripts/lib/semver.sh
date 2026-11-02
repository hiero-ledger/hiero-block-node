#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Shared semantic version comparison for the Solo E2E scripts.
# Sourced by resolve-versions.sh (compatibility floors) and
# lib/cn-application-properties.sh (TSS library era of a Consensus Node), so both
# order versions, including pre-release suffixes such as -rc.2 or -SNAPSHOT, the same way.

# Compares two semver strings (with optional pre-release suffix).
# Strips leading 'v', compares major.minor.patch numerically,
# then treats pre-release (e.g., -rc1, -rc.2, -SNAPSHOT) as lower than GA.
# Both arguments must start with a numeric major.minor.patch; callers check that first.
# Arguments:
#   $1 - Version A
#   $2 - Version B
# Returns (via stdout):
#   -1 if A < B, 0 if A == B, 1 if A > B
function compare_semver {
  local ver_a="${1#v}"
  local ver_b="${2#v}"

  # Split into base version and pre-release suffix
  local base_a="${ver_a%%-*}"
  local base_b="${ver_b%%-*}"
  local pre_a="" pre_b=""
  [[ "${ver_a}" == *-* ]] && pre_a="${ver_a#*-}"
  [[ "${ver_b}" == *-* ]] && pre_b="${ver_b#*-}"

  # Compare major.minor.patch
  local a_major a_minor a_patch b_major b_minor b_patch pair left right
  IFS='.' read -r a_major a_minor a_patch <<< "${base_a}"
  IFS='.' read -r b_major b_minor b_patch <<< "${base_b}"

  for pair in "${a_major:-0}:${b_major:-0}" "${a_minor:-0}:${b_minor:-0}" "${a_patch:-0}:${b_patch:-0}"; do
    left="${pair%%:*}"
    right="${pair##*:}"
    if (( left < right )); then echo "-1"; return; fi
    if (( left > right )); then echo "1"; return; fi
  done

  # Base versions are equal: compare pre-release.
  # No pre-release (GA) is higher than any pre-release.
  if [[ -z "${pre_a}" && -n "${pre_b}" ]]; then echo "1"; return; fi
  if [[ -n "${pre_a}" && -z "${pre_b}" ]]; then echo "-1"; return; fi
  if [[ -z "${pre_a}" && -z "${pre_b}" ]]; then echo "0"; return; fi

  # Both have pre-release: lexicographic with numeric awareness.
  # Extract numeric suffix from pre-release (e.g., rc1 is 1, rc.2 is 2)
  local pre_a_label="${pre_a%%[0-9]*}" pre_b_label="${pre_b%%[0-9]*}"
  local pre_a_num pre_b_num
  pre_a_num=$(echo "${pre_a}" | grep -o '[0-9]\+$') || pre_a_num="0"
  pre_b_num=$(echo "${pre_b}" | grep -o '[0-9]\+$') || pre_b_num="0"

  # Compare labels first (alpha < beta < rc)
  if [[ "${pre_a_label}" < "${pre_b_label}" ]]; then echo "-1"; return; fi
  if [[ "${pre_a_label}" > "${pre_b_label}" ]]; then echo "1"; return; fi

  # Same label, compare numeric suffix
  if (( pre_a_num < pre_b_num )); then echo "-1"; return; fi
  if (( pre_a_num > pre_b_num )); then echo "1"; return; fi

  echo "0"
}
