#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Idempotently installs Chaos Mesh v2.7.2 (or another configurable version) into
# the current Kubernetes cluster, in the 'chaos-mesh' namespace. Used by the
# latency-injection test infrastructure.
#
# Idempotent: re-running with an existing release upgrades-in-place rather than
# failing. Adds the chaos-mesh helm repo if not already present.
#
# Usage:
#   ./chaos-install.sh [options]
#
# Options:
#   --version VERSION    Chaos Mesh chart version (default: 2.7.2)
#   --runtime RUNTIME    Container runtime (default: containerd)
#   --socket-path PATH   Container runtime socket
#                        (default: /run/containerd/containerd.sock)
#   --timeout DURATION   Helm wait timeout (default: 5m)
#   --help               Show this help message

set -o pipefail

VERSION="2.7.2"
RUNTIME="containerd"
SOCKET_PATH="/run/containerd/containerd.sock"
TIMEOUT="5m"

function show_help {
  sed -n '2,/^$/p' "$0" | sed 's/^# \{0,1\}//'
  exit 0
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --version)     VERSION="$2"; shift 2 ;;
    --runtime)     RUNTIME="$2"; shift 2 ;;
    --socket-path) SOCKET_PATH="$2"; shift 2 ;;
    --timeout)     TIMEOUT="$2"; shift 2 ;;
    --help|-h)     show_help ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
done

command -v helm >/dev/null 2>&1 || { echo "ERROR: helm not found"; exit 1; }
command -v kubectl >/dev/null 2>&1 || { echo "ERROR: kubectl not found"; exit 1; }

echo "=== Chaos Mesh installer ==="
echo "  version: ${VERSION}"
echo "  runtime: ${RUNTIME}"
echo "  socket:  ${SOCKET_PATH}"
echo

if ! helm repo list 2>/dev/null | awk '{print $1}' | grep -qx "chaos-mesh"; then
  echo "Adding helm repo chaos-mesh..."
  helm repo add chaos-mesh https://charts.chaos-mesh.org
fi
helm repo update chaos-mesh >/dev/null

if helm status chaos-mesh -n chaos-mesh >/dev/null 2>&1; then
  echo "Chaos Mesh release already exists; running upgrade for idempotency..."
  ACTION=upgrade
else
  echo "Installing Chaos Mesh release..."
  ACTION=install
fi

helm "${ACTION}" chaos-mesh chaos-mesh/chaos-mesh \
  -n chaos-mesh --create-namespace \
  --version "${VERSION}" \
  --set chaosDaemon.runtime="${RUNTIME}" \
  --set chaosDaemon.socketPath="${SOCKET_PATH}" \
  --set chaosDaemon.hostPID=true \
  --set chaosDaemon.privileged=true \
  --set chaosDaemon.mountHostLibModules=true \
  --wait --timeout "${TIMEOUT}"

echo
echo "Chaos Mesh pods:"
kubectl -n chaos-mesh get pods --no-headers 2>/dev/null | sed 's/^/  /'
echo
echo "Chaos Mesh ${ACTION} complete."
