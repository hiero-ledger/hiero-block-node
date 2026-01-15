#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Resolves 'latest' version keywords to actual GA release versions.
# Supports: 'latest' (GA release), 'main' (snapshot), or specific version tags.
#
# Usage:
#   ./resolve-versions.sh [cn_version] [mn_version] [bn_version]
#
# Arguments:
#   cn_version - Consensus Node version ('latest', 'main', or tag like 'v0.68.6')
#   mn_version - Mirror Node version ('latest', 'main', or tag like 'v0.146.0')
#   bn_version - Block Node version ('latest', 'main', or tag like 'v0.21.2')
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
    main|MAIN)
      start_task "Using 'main' branch for ${component}"
      end_task "main (will use -SNAPSHOT)"
      echo "main"
      ;;
    *)
      # Validate it looks like a version tag
      if [[ ! "${input}" =~ ^v?[0-9]+\.[0-9]+\.[0-9]+ ]]; then
        fail "ERROR: Invalid version '${input}' for ${component}. Use 'latest', 'main', or a version tag (e.g., v0.68.6)" 1
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
  mn_resolved=$(resolve_version "${mn_input}" "${MN_REPO}" "Mirror Node")
  bn_resolved=$(resolve_version "${bn_input}" "${BN_REPO}" "Block Node")

  log_line ""
  log_line "Resolved versions:"
  log_line "  cn_version = %s" "${cn_resolved}"
  log_line "  mn_version = %s" "${mn_resolved}"
  log_line "  bn_version = %s" "${bn_resolved}"

  # Output key=value pairs to stdout for capture by caller
  echo "cn_version=${cn_resolved}"
  echo "mn_version=${mn_resolved}"
  echo "bn_version=${bn_resolved}"
}

main "$@"
