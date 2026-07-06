#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# WRB Distribution E2E (#3125 slice 4 — step 6) — reconfigure MN1's importer
# to pull live blocks from BN2 and BN3 while keeping the recordstream bucket
# downloader on as a records backup.
#
# Spring Boot cannot bind a List<BlockNodeProperties> from env vars alone,
# and helm upgrade needs the exact chart source (which Solo doesn't cache
# under a predictable name for the OCI URI we know). Instead we:
#
#   1. Locate Solo's importer ConfigMap by label
#      (app.kubernetes.io/instance=mirror-1, app.kubernetes.io/component=importer).
#   2. Extract its current application.yaml payload.
#   3. Merge in our block: {enabled, sourceType, nodes, verification} keys
#      using yq's deep-merge, preserving whatever else was there.
#   4. kubectl apply the updated ConfigMap.
#   5. `kubectl rollout restart deployment/mirror-1-importer` and wait for Ready.
#
# Reads:
#   NAMESPACE         (default "solo-network")
#   CLUSTER_REFERENCE (default "kind-solo-cluster")
#   BN_HOST_2         (default block-node-2.${NAMESPACE}.svc.cluster.local)
#   BN_HOST_3         (default block-node-3.${NAMESPACE}.svc.cluster.local)
#   MN_INSTANCE       (default "mirror-1")
#   READY_TIMEOUT     (default 300)

set -euo pipefail

: "${NAMESPACE:=solo-network}"
: "${CLUSTER_REFERENCE:=kind-solo-cluster}"
: "${BN_HOST_2:=block-node-2.${NAMESPACE}.svc.cluster.local}"
: "${BN_HOST_3:=block-node-3.${NAMESPACE}.svc.cluster.local}"
: "${MN_INSTANCE:=mirror-1}"
READY_TIMEOUT="${READY_TIMEOUT:-300}"

log() { echo "[wrb-dist-mn1-reconfig] $*"; }
fail() { echo "[wrb-dist-mn1-reconfig] ERROR: $*" >&2; exit 1; }

# 1) Locate the importer ConfigMap.
importer_cm=$(kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" \
    get configmap \
    -l "app.kubernetes.io/instance=${MN_INSTANCE},app.kubernetes.io/component=importer" \
    -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)

if [[ -z "${importer_cm}" ]]; then
    log "No ConfigMap matched labels instance=${MN_INSTANCE}, component=importer."
    log "All ${MN_INSTANCE} ConfigMaps:"
    kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" \
        get configmap -l "app.kubernetes.io/instance=${MN_INSTANCE}" -o wide 2>&1 | tail -20 || true
    fail "importer ConfigMap not found"
fi

log "Found importer ConfigMap: ${importer_cm}"

# 2) Extract application.yaml. Solo's chart typically stores it under a key
#    named "application.yaml" or "application.yml"; support both.
config_key=""
for key in application.yaml application.yml; do
    if kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" \
            get configmap "${importer_cm}" \
            -o jsonpath="{.data.${key/./\\.}}" 2>/dev/null \
            | grep -q .; then
        config_key="${key}"
        break
    fi
done

if [[ -z "${config_key}" ]]; then
    log "Neither 'application.yaml' nor 'application.yml' key found in ${importer_cm}. Contents:"
    kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" \
        get configmap "${importer_cm}" -o yaml 2>&1 | tail -40 || true
    fail "importer ConfigMap has no application yaml key"
fi
log "Using ConfigMap key: ${config_key}"

current_yaml_file="${TMPDIR:-/tmp}/wrb-dist-mn1-current.yaml"
kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" \
    get configmap "${importer_cm}" \
    -o jsonpath="{.data.${config_key/./\\.}}" > "${current_yaml_file}"

log "Current application yaml (${config_key}), size: $(wc -c < "${current_yaml_file}") bytes"

# 3) Merge in our block config using yq. Deep merge preserves everything else.
overlay_yaml_file="${TMPDIR:-/tmp}/wrb-dist-mn1-overlay.yaml"
cat > "${overlay_yaml_file}" <<EOF
hiero:
  mirror:
    importer:
      block:
        enabled: true
        sourceType: BLOCK_NODE
        nodes:
          - host: ${BN_HOST_2}
            port: 40840
          - host: ${BN_HOST_3}
            port: 40840
        verification:
          enabled: false
EOF

merged_yaml_file="${TMPDIR:-/tmp}/wrb-dist-mn1-merged.yaml"
yq eval-all '. as $item ireduce ({}; . * $item)' \
    "${current_yaml_file}" "${overlay_yaml_file}" > "${merged_yaml_file}"

log "Merged application yaml, size: $(wc -c < "${merged_yaml_file}") bytes"

# 4) Apply the updated ConfigMap. Use create + apply so we replace only the
#    data.<config_key> key, keeping the CM's labels/annotations intact.
kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" \
    create configmap "${importer_cm}" \
    "--from-file=${config_key}=${merged_yaml_file}" \
    --dry-run=client -o yaml \
    | kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" apply -f - \
    || fail "failed to patch ${importer_cm}"

log "ConfigMap ${importer_cm} patched. Rolling importer deployment..."

# 5) Restart the importer Deployment and wait for the new pod.
importer_deploy=$(kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" \
    get deployment \
    -l "app.kubernetes.io/instance=${MN_INSTANCE},app.kubernetes.io/component=importer" \
    -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)

if [[ -z "${importer_deploy}" ]]; then
    fail "no importer Deployment found for instance=${MN_INSTANCE}"
fi

kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" \
    rollout restart deployment/"${importer_deploy}" \
    || fail "kubectl rollout restart failed for ${importer_deploy}"

kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" \
    rollout status deployment/"${importer_deploy}" --timeout="${READY_TIMEOUT}s" \
    || {
        kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" \
            describe deployment/"${importer_deploy}" | tail -40 || true
        kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" \
            logs -l "app.kubernetes.io/instance=${MN_INSTANCE},app.kubernetes.io/component=importer" \
            --tail=80 || true
        fail "${importer_deploy} rollout did not complete"
    }

log "${MN_INSTANCE} importer is running with BN2 + BN3 as block-node sources."
