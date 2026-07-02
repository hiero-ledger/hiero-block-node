#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Uninstalls Chaos Mesh and removes its namespace.
#
# Usage:
#   ./chaos-uninstall.sh [--keep-namespace]
#
# Options:
#   --keep-namespace   Keep the chaos-mesh namespace (only uninstall the helm release)
#   --help             Show this help message

set -o pipefail

KEEP_NAMESPACE=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --keep-namespace) KEEP_NAMESPACE=true; shift ;;
    --help|-h)        sed -n '2,/^$/p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
done

command -v helm >/dev/null 2>&1 || { echo "ERROR: helm not found"; exit 1; }
command -v kubectl >/dev/null 2>&1 || { echo "ERROR: kubectl not found"; exit 1; }

if helm status chaos-mesh -n chaos-mesh >/dev/null 2>&1; then
  echo "Uninstalling chaos-mesh helm release..."
  helm uninstall chaos-mesh -n chaos-mesh
else
  echo "No chaos-mesh release found (already uninstalled)."
fi

if [[ "${KEEP_NAMESPACE}" != "true" ]]; then
  if kubectl get ns chaos-mesh >/dev/null 2>&1; then
    echo "Deleting chaos-mesh namespace..."
    kubectl delete namespace chaos-mesh --grace-period=30 --timeout=60s
  else
    echo "chaos-mesh namespace not present."
  fi
fi

echo "Chaos Mesh uninstall complete."
