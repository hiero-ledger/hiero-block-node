#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Wait for the kind-deployed Block Node to be Ready, then (re)establish a local port-forward. Used by
# e2e-node-operator.yaml after each phase that restarts the pod (install/upgrade/reset).
#
# Usage: bn-kind-port-forward.sh [release=block-node] [namespace=default] [local_port=40840] [svc_port=40840]
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

# Readiness is already gated by `kubectl wait condition=Ready` above (the pod's readiness probe hits the
# BN health port). Here we only confirm the port-forward we use for grpcurl accepts connections, and fail
# if it never does. Do NOT curl /healthz on ${LOCAL_PORT}: health endpoints live on a separate health
# port, not the server port, so they aren't served here.
for _ in $(seq 1 30); do
  if nc -z localhost "${LOCAL_PORT}" 2>/dev/null; then
    echo "Block Node is ready and port-forwarded on localhost:${LOCAL_PORT}"
    exit 0
  fi
  sleep 2
done
echo "ERROR: port-forward to localhost:${LOCAL_PORT} never became reachable" >&2
cat /tmp/bn-pf.log >&2 2>/dev/null || true
exit 1
