#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# WRB Distribution E2E (#3125 slice 4 — step 6) — reconfigure MN1's importer
# to pull live blocks from BN2 and BN3.
#
# Combines the two ingredients that make Spring's List<BlockNodeProperties>
# binding work on the mirror-node importer, matching the proven recipe in
# scripts/wrb-sequential-comparison.sh's deploy_mn2_to_bn2 function:
#
#   1. JVM importer image (gcr.io/mirrornode/hedera-mirror-importer:<tag>).
#      Solo's default ghcr.io/hiero-ledger/hiero-mirror-node/importer image is
#      GraalVM-native and can't reflect into List<BlockNodeProperties>.
#   2. Block config supplied as env vars (HIERO_MIRROR_IMPORTER_BLOCK_*) set
#      on the container spec. Spring's env-var → indexed-list binder resolves
#      HIERO_MIRROR_IMPORTER_BLOCK_NODES_0/1_HOST|PORT into the list correctly
#      when applied at container-creation time.
#
# Earlier iterations failed because they used only one of the two: either
# env-vars-on-graalvm-image (kubectl set env with no image swap) or
# jvm-image-with-SPRING_CONFIG_ADDITIONAL_LOCATION overlay (which never gets
# bindMethod resolved for List<Bean> element types).
#
# Reads:
#   NAMESPACE         (default "solo-network")
#   CLUSTER_REFERENCE (default "kind-solo-cluster")
#   BN_HOST_2         (default block-node-2.${NAMESPACE}.svc.cluster.local)
#   BN_HOST_3         (default block-node-3.${NAMESPACE}.svc.cluster.local)
#   MN_INSTANCE       (default "mirror-1")
#   MN_VERSION        (default v0.157.1)
#   READY_TIMEOUT     (default 300)

set -euo pipefail

: "${NAMESPACE:=solo-network}"
: "${CLUSTER_REFERENCE:=kind-solo-cluster}"
: "${BN_HOST_2:=block-node-2.${NAMESPACE}.svc.cluster.local}"
: "${BN_HOST_3:=block-node-3.${NAMESPACE}.svc.cluster.local}"
: "${MN_INSTANCE:=mirror-1}"
: "${MN_VERSION:=v0.157.1}"
READY_TIMEOUT="${READY_TIMEOUT:-300}"

log() { echo "[wrb-dist-mn1-reconfig] $*"; }
fail() { echo "[wrb-dist-mn1-reconfig] ERROR: $*" >&2; exit 1; }

importer_deploy=$(kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" \
    get deployment \
    -l "app.kubernetes.io/instance=${MN_INSTANCE},app.kubernetes.io/component=importer" \
    -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)

if [[ -z "${importer_deploy}" ]]; then
    log "All ${MN_INSTANCE} deployments:"
    kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" \
        get deployment -l "app.kubernetes.io/instance=${MN_INSTANCE}" -o wide 2>&1 | tail -20 || true
    fail "no importer Deployment found for instance=${MN_INSTANCE}"
fi
log "Found importer Deployment: ${importer_deploy}"

jvm_tag="${MN_VERSION#v}"
jvm_image="gcr.io/mirrornode/hedera-mirror-importer:${jvm_tag}"

patch_file="${TMPDIR:-/tmp}/wrb-mn1-deployment-patch.yaml"
cat > "${patch_file}" <<EOF
spec:
  template:
    spec:
      containers:
      - name: importer
        image: ${jvm_image}
        env:
        - name: HIERO_MIRROR_IMPORTER_BLOCK_ENABLED
          value: "true"
        - name: HIERO_MIRROR_IMPORTER_BLOCK_SOURCETYPE
          value: "BLOCK_NODE"
        - name: HIERO_MIRROR_IMPORTER_BLOCK_NODES_0_HOST
          value: "${BN_HOST_2}"
        - name: HIERO_MIRROR_IMPORTER_BLOCK_NODES_0_PORT
          value: "40840"
        - name: HIERO_MIRROR_IMPORTER_BLOCK_NODES_1_HOST
          value: "${BN_HOST_3}"
        - name: HIERO_MIRROR_IMPORTER_BLOCK_NODES_1_PORT
          value: "40840"
        - name: HIERO_MIRROR_IMPORTER_BLOCK_VERIFICATION_ENABLED
          value: "false"
EOF

log "Patching ${importer_deploy} (JVM image + block-source env vars)..."
kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" \
    patch deployment "${importer_deploy}" \
    --patch-file "${patch_file}" \
    || fail "kubectl patch failed for ${importer_deploy}"

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

log "${MN_INSTANCE} importer is running (JVM image, BN2 + BN3 as block-node sources)."