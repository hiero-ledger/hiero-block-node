#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Installs the Solo CLI globally, either from npm (default) or by building an
# approved fork/branch from source. Single entry point shared by the Taskfile
# and the solo-e2e-test CI workflow.
#
# Usage:
#   ./solo-install.sh
#
# Environment Variables:
#   SOLO_SOURCE    - 'npm' (default) or 'git'
#
#   npm mode:
#     SOLO_VERSION   - npm dist-tag or version (default: latest)
#
#   git mode:
#     SOLO_GIT_REPO  - owner/repo on GitHub; must be in the approved allowlist
#     SOLO_GIT_REF   - branch, tag, or commit SHA to build
#     SOLO_SRC_DIR   - clone directory (default: <script-dir>/../.solo-build)
#
# Security boundary:
#   git mode clones and executes third-party code (npm ci runs lifecycle
#   scripts). Only repositories in APPROVED_REPOS are permitted. This gate is
#   what makes it safe to expose SOLO_GIT_REPO as a CI input.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

SOLO_SOURCE="${SOLO_SOURCE:-npm}"
SOLO_VERSION="${SOLO_VERSION:-latest}"
SOLO_GIT_REPO="${SOLO_GIT_REPO:-}"
SOLO_GIT_REF="${SOLO_GIT_REF:-}"
SOLO_SRC_DIR="${SOLO_SRC_DIR:-${SCRIPT_DIR}/../.solo-build}"

# Repositories permitted as a git source. This is the security boundary:
# git mode builds and runs code from these repos, so the list is intentionally
# short and explicit.
readonly APPROVED_REPOS=(
  "hiero-ledger/solo"
  "hashgraph/solo"
  "AlfredoG87/solo"
)

function fail {
  printf '%s\n' "${1}" >&2
  exit "${2-1}"
}

function is_approved_repo {
  local candidate="${1}"
  local repo
  for repo in "${APPROVED_REPOS[@]}"; do
    if [[ "${repo}" == "${candidate}" ]]; then
      return 0
    fi
  done
  return 1
}

function print_resolved_version {
  local source_label="${1}"
  local resolved
  resolved="$(solo --version 2>/dev/null | grep 'Version' | sed 's/.*: *//' | tr -d '[:space:]')"
  echo ""
  echo "Solo installed successfully"
  echo "  Source:  ${source_label}"
  echo "  Version: ${resolved:-unknown}"
}

function install_from_npm {
  echo "Installing Solo from npm: @hashgraph/solo@${SOLO_VERSION}"
  npm i "@hashgraph/solo@${SOLO_VERSION}" -g
  print_resolved_version "npm @hashgraph/solo@${SOLO_VERSION}"
}

function install_from_git {
  if [[ -z "${SOLO_GIT_REPO}" ]]; then
    fail "ERROR: SOLO_SOURCE=git requires SOLO_GIT_REPO (owner/repo)." 1
  fi
  if [[ -z "${SOLO_GIT_REF}" ]]; then
    fail "ERROR: SOLO_SOURCE=git requires SOLO_GIT_REF (branch, tag, or SHA)." 1
  fi
  if ! is_approved_repo "${SOLO_GIT_REPO}"; then
    fail "ERROR: SOLO_GIT_REPO '${SOLO_GIT_REPO}' is not in the approved allowlist: ${APPROVED_REPOS[*]}" 1
  fi

  echo "Building Solo from source: ${SOLO_GIT_REPO}@${SOLO_GIT_REF}"
  echo "  Clone dir: ${SOLO_SRC_DIR}"

  rm -rf "${SOLO_SRC_DIR}"
  git clone --depth 1 --branch "${SOLO_GIT_REF}" \
    "https://github.com/${SOLO_GIT_REPO}.git" "${SOLO_SRC_DIR}"

  # Build mirrors solo's own 'build:compile': install deps, transpile, then run
  # the post-build script that stages the runtime resources into dist/. A bare
  # 'npm i github:owner/repo#ref -g' does NOT work: solo's 'prepare' script is
  # not a build, so dist/ would be missing.
  (
    cd "${SOLO_SRC_DIR}"
    npm ci
    npx tsc
    node resources/post-build-script.js
    npm i -g .
  )

  print_resolved_version "git ${SOLO_GIT_REPO}@${SOLO_GIT_REF}"
}

function main {
  case "${SOLO_SOURCE}" in
    npm)
      install_from_npm
      ;;
    git)
      install_from_git
      ;;
    *)
      fail "ERROR: SOLO_SOURCE must be 'npm' or 'git' (got '${SOLO_SOURCE}')." 1
      ;;
  esac
}

main
