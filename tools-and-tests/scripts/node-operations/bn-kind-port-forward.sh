#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Wait for the kind-deployed Block Node to be Ready, then (re)establish a local port-forward.
# Used by .github/workflows/e2e-node-operator.yaml after each lifecycle phase that restarts the pod
# (install, upgrade, reset).
#
# The chart names objects "<release>-block-node-server"; the Helm instance label is the release name.
#
# Usage: bn-kind-port-forward.sh [release] [namespace] [local_port] [svc_port]
#   release    (default: block-node)
#   namespace  (default: default)
#   local_port (default: 40840)
#   svc_port   (default: 40840)
# -x traces the kubectl wait / port-forward / readiness commands into the CI run log.
set -euxo pipefail

RELEASE="${1:-block-node}"
NAMESPACE="${2:-default}"
LOCAL_PORT="${3:-40840}"
SVC_PORT="${4:-40840}"
SVC="svc/${RELEASE}-block-node-server"
SELECTOR="app.kubernetes.io/instance=${RELEASE}"

# Drop any prior port-forward so we bind cleanly to the restarted pod. Match the kubectl process
# specifically — a broad "port-forward.*block-node" pattern also matches THIS script's own command line
# (its filename contains "port-forward" and the release arg is "block-node"), which would SIGTERM itself.
pkill -f "kubectl.*port-forward.*${SVC#svc/}" 2>/dev/null || true
sleep 1

echo "Waiting for ${SELECTOR} to be Ready in namespace ${NAMESPACE}..."
kubectl wait pod -n "${NAMESPACE}" -l "${SELECTOR}" --for=condition=Ready --timeout=120s

kubectl port-forward -n "${NAMESPACE}" "${SVC}" "${LOCAL_PORT}:${SVC_PORT}" >/tmp/bn-pf.log 2>&1 &

# Wait for the forwarded port to accept TCP connections.
for _ in $(seq 1 30); do
  if nc -z localhost "${LOCAL_PORT}" 2>/dev/null; then
    break
  fi
  sleep 2
done

# Gate on the health endpoint so callers know the BN is actually serving.
curl -sf --retry 10 --retry-delay 3 "http://localhost:${LOCAL_PORT}/healthz/readyz"
echo "Block Node is ready and port-forwarded on localhost:${LOCAL_PORT}"
