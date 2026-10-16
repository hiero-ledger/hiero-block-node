#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Resolves version keywords to actual release versions.
# Supports: 'latest' (GA release), 'rc' (Release Candidate), 'main' (SNAPSHOT from main branch), or specific version tags.
#
# Usage:
#   ./resolve-versions.sh [cn_version] [mn_version] [bn_version] [relay_version] [tck_version]
#
# Arguments:
#   cn_version    - Consensus Node version ('latest', 'rc', 'main' for SNAPSHOT, or tag like 'v0.68.6')
#   mn_version    - Mirror Node version ('latest', 'rc', 'main' for SNAPSHOT, or tag like 'v0.146.0')
#   bn_version    - Block Node version ('latest', 'rc', 'main' for SNAPSHOT, or tag like 'v0.21.2')
#   relay_version - Relay version ('latest', 'rc', or tag like 'v0.75.0')
#   tck_version   - TCK-SDK version ('latest', 'rc', or tag like 'v1.2.3')
#
# Output:
#   Outputs key=value pairs to stdout that can be captured by the caller:
#     cn_version=<resolved_version>
#     mn_version=<resolved_version>
#     bn_version=<resolved_version>
#     relay_version=<resolved_version>
#     tck_version=<resolved_version>

set -o pipefail
set +e

readonly CN_REPO="hiero-ledger/hiero-consensus-node"
readonly MN_REPO="hiero-ledger/hiero-mirror-node"
readonly BN_REPO="hiero-ledger/hiero-block-node"
readonly RELAY_REPO="hiero-ledger/hiero-json-rpc-relay"
readonly TCK_REPO="hiero-ledger/hiero-sdk-tck"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/semver.sh
source "${SCRIPT_DIR}/lib/semver.sh"

# Compatibility matrix: maps CN version ranges to required MN and BN minimums.
# Tiers are evaluated highest-first; first matching tier wins.
# CN is also floored at the lowest tier's cn_min.
#
# | CN Version        | MN Minimum    | BN Minimum   | Notes                                      |
# |-------------------|---------------|--------------|--------------------------------------------|
# | >= 0.80.0         | 0.166.0       | 0.46.0       | Nova WRAPS TSS library: one block proof     |
# |                   |               |              | layout on every block; older MN and BN      |
# |                   |               |              | cannot verify these TSS block proofs        |
# | >= 0.75.0-rc.4    | 0.154.0       | 0.35.0       | WRB streaming + RSA roster                  |
#
# TODO(#3689): confirm the 0.80.0 tier once the CN release carrying hiero-consensus-node#27506
# is cut, and the MN minimum (0.166.0, hiero-mirror-node#14399) once that MN release exists.
#
# Note: 0.154.0 is the floor for WRB cutover support (added in 0.153.0); newer MN is fine.
# Separately, MN 0.155.x/0.156.x ship broken GraalVM-native importer/grpc images; that is
# worked around in the Mirror Node overlay by overriding those modules to the JVM images
# (see generate-chart-values-config-overlays.sh), NOT by pinning the MN version here.
readonly COMPAT_CN_MINS=("0.80.0" "0.75.0-rc.4")
readonly COMPAT_MN_MINS=("0.166.0" "0.154.0")
readonly COMPAT_BN_MINS=("0.46.0" "0.35.0")

# The same pairing seen from the Block Node: a BN from NOVA_WRAPS_BN_MIN_VERSION verifies
# only the TSS block proof layout of the Nova WRAPS library, which Consensus Nodes ship from
# NOVA_WRAPS_CN_MIN_VERSION, so such a BN raises the CN floor.
# TODO(#3689): confirm the CN version once the CN release carrying hiero-consensus-node#27506
# is cut.
readonly NOVA_WRAPS_BN_MIN_VERSION="0.46.0"
readonly NOVA_WRAPS_CN_MIN_VERSION="0.80.0"

# Lowest tier CN floor (used to enforce CN minimum before tier selection)
# Note: bash 3.2 (macOS default) does not support negative array indices — use explicit last index
readonly CN_MIN_VERSION="${COMPAT_CN_MINS[${#COMPAT_CN_MINS[@]}-1]}"

function fail {
    printf '%s\n' "$1" >&2
    exit "${2-1}"
}

function log_line {
  local message="${1}"
  shift
  if [[ $# -eq 0 ]]; then
    # No format args, print message as-is (avoids printf interpreting dashes)
    printf '%s\n' "${message}" >&2
  else
    local formatted
    # shellcheck disable=SC2059
    formatted=$(printf "${message}" "${@}")
    printf '%s\n' "${formatted}" >&2
  fi
}

function start_task {
  local message="${1}"
  shift
  if [[ $# -eq 0 ]]; then
    printf '%s .....\t' "${message}" >&2
  else
    local formatted
    # shellcheck disable=SC2059
    formatted=$(printf "${message}" "${@}")
    printf '%s .....\t' "${formatted}" >&2
  fi
}

function end_task {
  printf "%s\n" "${1:-DONE}" >&2
}

# Enforces a minimum version floor for a component.
# If the resolved version is below the minimum, logs a warning and returns the minimum version.
# If the resolved version meets the minimum, returns the resolved version unchanged.
# Arguments:
#   $1 - Resolved version string
#   $2 - Minimum version string (without 'v' prefix)
#   $3 - Component name for log messages
# Returns (via stdout):
#   The effective version to use (either resolved or minimum floor)
function enforce_minimum_version {
  local resolved="${1}"
  local minimum="${2}"
  local component="${3}"

  # Skip enforcement for SNAPSHOT versions (from 'main' keyword)
  if [[ "${resolved}" == *-SNAPSHOT ]]; then
    log_line "Skipping minimum version enforcement for ${component} (SNAPSHOT build: ${resolved})"
    echo "${resolved}"
    return 0
  fi

  local cmp
  cmp=$(compare_semver "${resolved}" "${minimum}")

  if [[ "${cmp}" == "-1" ]]; then
    log_line "WARNING: ${component} resolved to ${resolved}, below minimum ${minimum} (required for TSS/hinTS support). Using ${minimum} instead."
    echo "${minimum}"
  else
    echo "${resolved}"
  fi
}

# Raises the CN floor for a Block Node that verifies only the Nova WRAPS TSS block proof layout.
# SNAPSHOT versions on either side skip enforcement (unreleased builds may mix freely), the same
# rule resolve_compatibility_mins applies.
# Arguments:
#   $1 - Resolved CN version string
#   $2 - Resolved BN version string
# Returns (via stdout):
#   The effective CN version to use
function enforce_nova_wraps_cn_floor {
  local cn_version="${1}"
  local bn_version="${2}"

  if [[ "${cn_version}" == *-SNAPSHOT || "${bn_version}" == *-SNAPSHOT ]]; then
    echo "${cn_version}"
  elif [[ "$(compare_semver "${bn_version}" "${NOVA_WRAPS_BN_MIN_VERSION}")" == "-1" ]]; then
    echo "${cn_version}"
  elif [[ "$(compare_semver "${cn_version}" "${NOVA_WRAPS_CN_MIN_VERSION}")" == "-1" ]]; then
    log_line "Block Node ${bn_version} verifies only Nova WRAPS TSS block proofs, which need CN >=${NOVA_WRAPS_CN_MIN_VERSION}"
    enforce_minimum_version "${cn_version}" "${NOVA_WRAPS_CN_MIN_VERSION}" "Consensus Node"
  else
    echo "${cn_version}"
  fi
}

# Resolves the MN and BN minimum versions required for a given CN version.
# Evaluates the compatibility matrix tiers from highest to lowest.
# SNAPSHOT CN versions skip enforcement (unreleased builds may mix freely).
# Arguments:
#   $1 - Resolved CN version string
# Returns (via stdout):
#   Two lines: "mn_min=<version>" and "bn_min=<version>", or empty strings if no tier matched.
function resolve_compatibility_mins {
  local cn_version="${1}"

  if [[ "${cn_version}" == *-SNAPSHOT ]]; then
    log_line "Skipping compatibility matrix enforcement (CN is SNAPSHOT: ${cn_version})"
    echo "mn_min="
    echo "bn_min="
    return
  fi

  for i in "${!COMPAT_CN_MINS[@]}"; do
    if [[ "$(compare_semver "${cn_version}" "${COMPAT_CN_MINS[$i]}")" != "-1" ]]; then
      log_line "Compatibility tier: CN >=${COMPAT_CN_MINS[$i]} -> MN >=${COMPAT_MN_MINS[$i]}, BN >=${COMPAT_BN_MINS[$i]}"
      echo "mn_min=${COMPAT_MN_MINS[$i]}"
      echo "bn_min=${COMPAT_BN_MINS[$i]}"
      return
    fi
  done

  echo "mn_min="
  echo "bn_min="
}

# Fetches the version from the main branch of a GitHub repository.
# Tries version.txt first, then falls back to gradle.properties.
# Arguments:
#   $1 - Repository in format "owner/repo"
# Returns:
#   The version string (e.g., "0.27.0-SNAPSHOT")
function get_main_branch_version {
  local repo="${1}"
  local version=""

  # Try version.txt first (used by Block Node and Consensus Node)
  local version_txt_url="https://raw.githubusercontent.com/${repo}/main/version.txt"
  local response
  response=$(curl -s -f "${version_txt_url}" 2>/dev/null)

  if [[ $? -eq 0 && -n "${response}" ]]; then
    version=$(echo "${response}" | tr -d '[:space:]')
  fi

  # Fall back to gradle.properties (used by Mirror Node)
  if [[ -z "${version}" || ! "${version}" =~ ^[0-9]+\.[0-9]+\.[0-9]+ ]]; then
    local gradle_props_url="https://raw.githubusercontent.com/${repo}/main/gradle.properties"
    response=$(curl -s -f "${gradle_props_url}" 2>/dev/null)

    if [[ $? -eq 0 && -n "${response}" ]]; then
      # Extract version=X.Y.Z from gradle.properties
      version=$(echo "${response}" | grep -E "^version=" | sed 's/version=//' | tr -d '[:space:]')
    fi
  fi

  if [[ -z "${version}" || ! "${version}" =~ ^[0-9]+\.[0-9]+\.[0-9]+ ]]; then
    log_line "ERROR: Could not find version in version.txt or gradle.properties"
    return 1
  fi

  echo "${version}"
}

# Fetches the latest GA release tag from a GitHub repository
# Arguments:
#   $1 - Repository in format "owner/repo"
# Returns:
#   The tag_name of the latest release
function get_latest_release {
  local repo="${1}"
  local url="https://api.github.com/repos/${repo}/releases/latest"
  local response
  local tag_name

  response=$(curl -s -H "Accept: application/vnd.github+json" "${url}") || return 1

  tag_name=$(echo "${response}" | grep -o '"tag_name": *"[^"]*"' | head -1 | sed 's/"tag_name": *"\([^"]*\)"/\1/')

  if [[ -z "${tag_name}" ]]; then
    log_line "ERROR: Could not parse tag_name from response"
    return 1
  fi

  echo "${tag_name}"
}

# Fetches the latest RC (Release Candidate) tag from a GitHub repository
# First checks releases, then falls back to tags if no RC releases found
# Looks for tags containing '-rc', '-alpha', or '-beta' (case-insensitive)
# Arguments:
#   $1 - Repository in format "owner/repo"
# Returns:
#   The tag_name of the latest RC release/tag, or falls back to latest GA
function get_latest_rc_release {
  local repo="${1}"
  local rc_tag=""

  # First, try to find RC in releases
  local releases_url="https://api.github.com/repos/${repo}/releases?per_page=30"
  local response
  response=$(curl -s -H "Accept: application/vnd.github+json" "${releases_url}") || return 1

  # Find the first (most recent) release with -rc, -alpha, or -beta in the tag name
  rc_tag=$(echo "${response}" | grep -o '"tag_name": *"[^"]*"' | sed 's/"tag_name": *"\([^"]*\)"/\1/' | grep -iE '(-rc|-alpha|-beta)' | head -1)

  # If no RC releases found, check tags (some repos use tags without releases for RCs)
  if [[ -z "${rc_tag}" ]]; then
    local tags_url="https://api.github.com/repos/${repo}/tags?per_page=50"
    response=$(curl -s -H "Accept: application/vnd.github+json" "${tags_url}") || return 1

    # Find the first (most recent) tag with -rc, -alpha, or -beta
    rc_tag=$(echo "${response}" | grep -o '"name": *"[^"]*"' | sed 's/"name": *"\([^"]*\)"/\1/' | grep -iE '(-rc|-alpha|-beta)' | head -1)
  fi

  if [[ -z "${rc_tag}" ]]; then
    log_line "WARNING: No RC release/tag found for ${repo}, falling back to latest GA"
    get_latest_release "${repo}"
    return
  fi

  echo "${rc_tag}"
}

# Resolves a version string to an actual version
# Arguments:
#   $1 - Input version ('latest', 'main', or specific tag)
#   $2 - Repository for 'latest' resolution
#   $3 - Component name for logging
# Returns:
#   Resolved version string
function resolve_version {
  local input="${1}"
  local repo="${2}"
  local component="${3}"

  case "${input}" in
    latest|LATEST)
      start_task "Resolving 'latest' for ${component}"
      local resolved
      resolved=$(get_latest_release "${repo}") || fail "ERROR: Failed to fetch latest release for ${component}" 1
      end_task "${resolved}"
      echo "${resolved}"
      ;;
    rc|RC)
      start_task "Resolving 'rc' (Release Candidate) for ${component}"
      local resolved
      resolved=$(get_latest_rc_release "${repo}") || fail "ERROR: Failed to fetch RC release for ${component}" 1
      end_task "${resolved}"
      echo "${resolved}"
      ;;
    main|MAIN)
      start_task "Resolving 'main' branch version for ${component}"
      local resolved
      resolved=$(get_main_branch_version "${repo}") || fail "ERROR: Failed to fetch main branch version for ${component}" 1
      end_task "${resolved}"
      echo "${resolved}"
      ;;
    *)
      # Validate it looks like a version tag
      if [[ ! "${input}" =~ ^v?[0-9]+\.[0-9]+\.[0-9]+ ]]; then
        fail "ERROR: Invalid version '${input}' for ${component}. Use 'latest', 'main', 'rc', or a version tag (e.g., v0.68.6)" 1
      fi
      start_task "Using specified version for ${component}"
      end_task "${input}"
      echo "${input}"
      ;;
  esac
}

# Main execution
function main {
  local cn_input="${1:-latest}"
  local mn_input="${2:-latest}"
  local bn_input="${3:-latest}"
  local relay_input="${4:-latest}"
  local tck_input="${5:-latest}"

  log_line "Resolving Component Versions"
  log_line "============================"
  log_line ""
  log_line "Input versions:"
  log_line "  Consensus Node: %s" "${cn_input}"
  log_line "  Mirror Node:    %s" "${mn_input}"
  log_line "  Block Node:     %s" "${bn_input}"
  log_line "  Relay:          %s" "${relay_input}"
  log_line "  TCK-SDK:        %s" "${tck_input}"
  log_line ""

  # Resolve each version
  local cn_resolved mn_resolved bn_resolved relay_resolved tck_resolved

  cn_resolved=$(resolve_version "${cn_input}" "${CN_REPO}" "Consensus Node")
  mn_resolved=$(resolve_version "${mn_input}" "${MN_REPO}" "Mirror Node")
  bn_resolved=$(resolve_version "${bn_input}" "${BN_REPO}" "Block Node")

  # Enforce CN minimum floor (lowest compatibility tier) and the floor a Nova WRAPS Block Node
  # needs, then derive MN/BN minimums from CN
  cn_resolved=$(enforce_minimum_version "${cn_resolved}" "${CN_MIN_VERSION}" "Consensus Node")
  cn_resolved=$(enforce_nova_wraps_cn_floor "${cn_resolved}" "${bn_resolved}")
  local compat_out mn_min bn_min
  compat_out=$(resolve_compatibility_mins "${cn_resolved}")
  mn_min=$(echo "${compat_out}" | grep "^mn_min=" | cut -d= -f2)
  bn_min=$(echo "${compat_out}" | grep "^bn_min=" | cut -d= -f2)
  if [[ -n "${mn_min}" ]]; then
    mn_resolved=$(enforce_minimum_version "${mn_resolved}" "${mn_min}" "Mirror Node")
  fi
  if [[ -n "${bn_min}" ]]; then
    bn_resolved=$(enforce_minimum_version "${bn_resolved}" "${bn_min}" "Block Node")
  fi

  relay_resolved=$(resolve_version "${relay_input}" "${RELAY_REPO}" "Relay")
  tck_resolved=$(resolve_version "${tck_input}" "${TCK_REPO}" "TCK-SDK")

  log_line ""
  log_line "Resolved versions:"
  # Output key=value pairs to stdout for capture by caller
  echo "cn_version=${cn_resolved}"
  echo "mn_version=${mn_resolved}"
  echo "bn_version=${bn_resolved}"
  echo "relay_version=${relay_resolved}"
  echo "tck_version=${tck_resolved}"
}

main "$@"
