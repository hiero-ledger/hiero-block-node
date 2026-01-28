#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Resolves version keywords to actual release versions.
# Supports: 'latest' (GA release), 'rc' (Release Candidate), 'main' (snapshot), or specific version tags.
#
# Usage:
#   ./resolve-versions.sh [cn_version] [mn_version] [bn_version]
#
# Arguments:
#   cn_version - Consensus Node version ('latest', 'rc', 'main', or tag like 'v0.68.6')
#   mn_version - Mirror Node version ('latest', 'rc', 'main', or tag like 'v0.146.0')
#   bn_version - Block Node version ('latest', 'rc', 'main', or tag like 'v0.21.2')
#
# Output:
#   Outputs key=value pairs to stdout that can be captured by the caller:
#     cn_version=<resolved_version>
#     mn_version=<resolved_version>
#     bn_version=<resolved_version>

set -o pipefail
set +e

readonly CN_REPO="hiero-ledger/hiero-consensus-node"
readonly MN_REPO="hiero-ledger/hiero-mirror-node"
readonly BN_REPO="hiero-ledger/hiero-block-node"

# WORKAROUND: CN v0.68.x and v0.69.x have compatibility issues with Block Node.
# When these versions are resolved, we override with the latest v0.70.x RC.
# TODO: Remove this workaround once v0.70.0 GA or higher is available.
readonly CN_BROKEN_VERSION_PATTERN="^v?0\.(68|69)\."
readonly CN_FALLBACK_VERSION_PATTERN="v0\.70\."

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

# Checks if a version matches the broken CN version pattern (v0.68.x or v0.69.x)
# Arguments:
#   $1 - Version string to check
# Returns:
#   0 if version is broken, 1 otherwise
function is_broken_cn_version {
  local version="${1}"
  [[ "${version}" =~ ${CN_BROKEN_VERSION_PATTERN} ]]
}

# Fetches the latest v0.70.x RC tag from consensus node repository
# Returns:
#   The latest v0.70.x RC tag, or empty string if not found
function get_cn_fallback_version {
  local tags_url="https://api.github.com/repos/${CN_REPO}/tags?per_page=100"
  local response
  response=$(curl -s -H "Accept: application/vnd.github+json" "${tags_url}") || return 1

  # Find the first (most recent) v0.70.x tag (RC or GA)
  local fallback_tag
  fallback_tag=$(echo "${response}" | grep -o '"name": *"[^"]*"' | sed 's/"name": *"\([^"]*\)"/\1/' | grep -E "${CN_FALLBACK_VERSION_PATTERN}" | head -1)

  echo "${fallback_tag}"
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
      start_task "Using 'main' branch for ${component}"
      end_task "main (will use -SNAPSHOT)"
      echo "main"
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

  log_line "Resolving Component Versions"
  log_line "============================"
  log_line ""
  log_line "Input versions:"
  log_line "  Consensus Node: %s" "${cn_input}"
  log_line "  Mirror Node:    %s" "${mn_input}"
  log_line "  Block Node:     %s" "${bn_input}"
  log_line ""

  # Resolve each version
  local cn_resolved mn_resolved bn_resolved

  cn_resolved=$(resolve_version "${cn_input}" "${CN_REPO}" "Consensus Node")

  # WORKAROUND: CN v0.69.x is broken, override with v0.70.x RC if needed
  if is_broken_cn_version "${cn_resolved}"; then
    log_line ""
    log_line "WARNING: CN %s is broken (incompatible with Block Node)" "${cn_resolved}"
    local fallback_version
    fallback_version=$(get_cn_fallback_version)
    if [[ -n "${fallback_version}" ]]; then
      log_line "WARNING: Overriding CN version to %s" "${fallback_version}"
      cn_resolved="${fallback_version}"
    else
      fail "ERROR: Could not find fallback CN version (v0.70.x)" 1
    fi
  fi

  mn_resolved=$(resolve_version "${mn_input}" "${MN_REPO}" "Mirror Node")
  bn_resolved=$(resolve_version "${bn_input}" "${BN_REPO}" "Block Node")

  log_line ""
  log_line "Resolved versions:"
  # Output key=value pairs to stdout for capture by caller
  echo "cn_version=${cn_resolved}"
  echo "mn_version=${mn_resolved}"
  echo "bn_version=${bn_resolved}"
}

main "$@"
