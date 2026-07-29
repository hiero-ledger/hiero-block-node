#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Validates a chaos selector by counting matching pods. Returns 0 if at least
# one pod matches; non-zero otherwise. Used to fail-fast before applying a
# NetworkChaos that would silently no-op against zero pods.
#
# Usage:
#   ./chaos-dryrun.sh --namespace NS --selector LABEL=VALUE[,LABEL=VALUE...]
#                     [--label TAG] [--quiet]
#
# Options:
#   --namespace NS    Kubernetes namespace (required)
#   --selector SEL    Label selector in kubectl form (required)
#   --label TAG       Optional human label for log lines (e.g. 'source')
#   --quiet           Suppress matched-pod listing
#   --help            Show this help message
#
# Exit codes:
#   0 — at least one pod matches
#   1 — argument error
#   2 — zero pods match

set -o pipefail

NAMESPACE=""
SELECTOR=""
TAG="selector"
QUIET=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --namespace) NAMESPACE="$2"; shift 2 ;;
    --selector)  SELECTOR="$2"; shift 2 ;;
    --label)     TAG="$2"; shift 2 ;;
    --quiet)     QUIET=true; shift ;;
    --help|-h)   sed -n '2,/^$/p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
done

[[ -z "${NAMESPACE}" ]] && { echo "ERROR: --namespace is required"; exit 1; }
[[ -z "${SELECTOR}"  ]] && { echo "ERROR: --selector is required"; exit 1; }

command -v kubectl >/dev/null 2>&1 || { echo "ERROR: kubectl not found"; exit 1; }

names=$(kubectl get pods -n "${NAMESPACE}" -l "${SELECTOR}" -o name 2>/dev/null || true)
count=0
[[ -n "${names}" ]] && count=$(echo "${names}" | wc -l | tr -d ' ')

if [[ "${count}" -eq 0 ]]; then
  echo "ERROR: ${TAG} matched 0 pods in ns=${NAMESPACE} selector='${SELECTOR}'"
  exit 2
fi

if [[ "${QUIET}" != "true" ]]; then
  echo "${TAG}: matched ${count} pod(s) in ns=${NAMESPACE} selector='${SELECTOR}'"
  echo "${names}" | sed 's/^/  /'
fi
