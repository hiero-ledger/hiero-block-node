#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Needs ghcr.io/hiero-ledger/hiero-block-node-full:<version> loaded into the current cluster.
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"
CHART=charts/block-node-server
OVERRIDES="${CHART}/values-overrides"
RELEASE=bn-full-test
NAMESPACE=default
VERSION=$(cat version.txt)

fail() { echo "FAIL: $*"; exit 1; }

helm_release() {
  local verb=$1 profile=$2
  shift 2
  helm "${verb}" "${RELEASE}" "${CHART}" --namespace "${NAMESPACE}" \
    --values "${OVERRIDES}/full.yaml" \
    --values "${OVERRIDES}/plugin-profile-${profile}.yaml" \
    --set image.tag="${VERSION}" "$@"
}

sts_field() {
  kubectl get statefulset -n "${NAMESPACE}" -l "app.kubernetes.io/instance=${RELEASE}" -o "jsonpath={.items[0].$1}"
}

assert_plugin_configs() {
  local pod logs
  pod=$(kubectl get pod -n "${NAMESPACE}" -l "app.kubernetes.io/instance=${RELEASE}" -o jsonpath='{.items[0].metadata.name}')
  logs=$(kubectl logs -n "${NAMESPACE}" "${pod}")
  for config in $1; do
    grep -q "${config}" <<< "${logs}" || fail "${config} not loaded"
  done
  for config in $2; do
    grep -q "${config}" <<< "${logs}" && fail "${config} loaded but not selected"
  done
  echo "OK: plugin configs match"
}

kubectl apply -n "${NAMESPACE}" -f - <<EOF
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: ${RELEASE}-deny-egress
spec:
  podSelector:
    matchLabels:
      app.kubernetes.io/instance: ${RELEASE}
  policyTypes: [Egress]
  egress: []
EOF

echo "--- install with plugin-profile-lfh"
helm_release install lfh --wait --timeout 300s
[[ " $(sts_field 'spec.template.spec.initContainers[*].name') " == *" resolve-plugins "* ]] \
  && fail "resolve-plugins init container rendered with plugins.source=image"
assert_plugin_configs "PublisherConfig SubscriberConfig FilesHistoricConfig" "CloudStorageArchiveConfig ExpandedCloudStorageConfig"
uid=$(sts_field metadata.uid)

echo "--- upgrade to plugin-profile-minimal"
helm_release upgrade minimal --wait --timeout 300s
assert_plugin_configs "MessagingConfig ServerStatusConfig BlockAccessConfig" "PublisherConfig BackfillConfiguration"
[[ "$(sts_field metadata.uid)" == "${uid}" ]] || fail "StatefulSet was recreated"

echo "--- server-side dry-run of switching back to plugins.source=maven"
helm_release template minimal --set plugins.source=maven --show-only templates/statefulset.yaml \
  | kubectl apply -n "${NAMESPACE}" --dry-run=server -f -

echo "PASS"
