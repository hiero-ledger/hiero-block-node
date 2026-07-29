#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Shows active chaos resources and Chaos Mesh component health.
#
# Usage:
#   ./chaos-status.sh [--help]

set -o pipefail

[[ "${1:-}" == "--help" || "${1:-}" == "-h" ]] && {
  sed -n '2,/^$/p' "$0" | sed 's/^# \{0,1\}//'
  exit 0
}

command -v kubectl >/dev/null 2>&1 || { echo "ERROR: kubectl not found"; exit 1; }

echo "=== Chaos Mesh components ==="
if kubectl get ns chaos-mesh >/dev/null 2>&1; then
  kubectl -n chaos-mesh get pods --no-headers 2>/dev/null | sed 's/^/  /' || echo "  (no pods)"
else
  echo "  chaos-mesh namespace not present (install with: task chaos:install)"
  exit 0
fi

if ! kubectl api-resources --api-group=chaos-mesh.org -o name >/dev/null 2>&1; then
  echo
  echo "Chaos Mesh CRDs not registered."
  exit 0
fi

echo
echo "=== Active NetworkChaos ==="
out=$(kubectl get networkchaos --all-namespaces 2>&1)
if [[ -z "${out}" || "${out}" == *"No resources found"* ]]; then
  echo "  (none)"
else
  echo "${out}" | sed 's/^/  /'
fi

echo
echo "=== Active PodChaos ==="
out=$(kubectl get podchaos --all-namespaces 2>&1)
if [[ -z "${out}" || "${out}" == *"No resources found"* ]]; then
  echo "  (none)"
else
  echo "${out}" | sed 's/^/  /'
fi
