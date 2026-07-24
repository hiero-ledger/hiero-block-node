#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Deletes chaos resources (NetworkChaos, PodChaos) without uninstalling
# Chaos Mesh itself. Tolerates the case where Chaos Mesh CRDs are not
# installed (returns success silently) so it is safe to wire into 'task down'
# even when chaos was never enabled.
#
# Usage:
#   ./chaos-cleanup.sh [options]
#
# Options:
#   --label-selector SEL   Only delete resources matching this label selector
#                          (e.g. solo-e2e-test/test-id=my-test). If omitted,
#                          deletes ALL chaos resources in the cluster.
#   --kinds KINDS          Comma-separated chaos kinds to delete
#                          (default: networkchaos,podchaos)
#   --quiet                Suppress per-kind logging
#   --help                 Show this help message

set -o pipefail

LABEL_SELECTOR=""
KINDS="networkchaos,podchaos"
QUIET=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --label-selector) LABEL_SELECTOR="$2"; shift 2 ;;
    --kinds)          KINDS="$2"; shift 2 ;;
    --quiet)          QUIET=true; shift ;;
    --help|-h)        sed -n '2,/^$/p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
done

command -v kubectl >/dev/null 2>&1 || { echo "ERROR: kubectl not found"; exit 1; }

# If chaos-mesh CRDs are not installed, nothing to do.
if ! kubectl api-resources --api-group=chaos-mesh.org -o name >/dev/null 2>&1; then
  [[ "${QUIET}" == "true" ]] || echo "Chaos Mesh CRDs not installed; nothing to clean."
  exit 0
fi

failed=0
IFS=',' read -ra KIND_LIST <<< "${KINDS}"
for kind in "${KIND_LIST[@]}"; do
  kind_trimmed="${kind// /}"
  [[ -z "${kind_trimmed}" ]] && continue
  if ! kubectl api-resources --api-group=chaos-mesh.org -o name 2>/dev/null \
       | grep -qx "${kind_trimmed}.chaos-mesh.org"; then
    [[ "${QUIET}" == "true" ]] || echo "Kind '${kind_trimmed}' not present; skipping."
    continue
  fi
  [[ "${QUIET}" == "true" ]] || echo "Deleting ${kind_trimmed} resources${LABEL_SELECTOR:+ (selector: ${LABEL_SELECTOR})}..."
  # --all and -l (label selector) are mutually exclusive in kubectl delete;
  # use the selector path when one is given, the --all path otherwise.
  if [[ -n "${LABEL_SELECTOR}" ]]; then
    delete_args=(--all-namespaces -l "${LABEL_SELECTOR}" --ignore-not-found)
  else
    delete_args=(--all-namespaces --all --ignore-not-found)
  fi
  if ! kubectl delete "${kind_trimmed}" "${delete_args[@]}" 2>&1 | sed 's/^/  /'; then
    failed=1
  fi
done

exit "${failed}"
