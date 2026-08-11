#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Deploys MinIO via Helm into a Kubernetes namespace and creates the
# minio-cloud-storage-creds Secret used by cloud-storage-archive and
# cloud-storage-expanded block node plugins.
#
# Uses the official MinIO Helm chart (https://charts.min.io/) whose image lives
# on quay.io — more reliably available in CI than the Bitnami chart whose Docker
# Hub images are periodically rotated off.
#
# Usage:
#   ./solo-deploy-minio.sh --namespace NS [--release-name NAME] [--buckets LIST]
#
# Options:
#   --namespace NS        Kubernetes namespace (required)
#   --release-name NAME   Helm release name (default: minio)
#   --buckets LIST        Comma-separated bucket names to create on startup
#                         (default: block-archive-tar,block-archive-expanded)

set -eo pipefail

NAMESPACE=""
RELEASE_NAME="minio"
BUCKETS="block-archive-tar,block-archive-expanded"

while [[ $# -gt 0 ]]; do
  case $1 in
    --namespace)    NAMESPACE="$2"; shift 2 ;;
    --release-name) RELEASE_NAME="$2"; shift 2 ;;
    --buckets)      BUCKETS="$2"; shift 2 ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
done

[[ -z "${NAMESPACE}" ]] && { echo "ERROR: --namespace is required"; exit 1; }

echo "Deploying Minio"
echo "  Release:   ${RELEASE_NAME}"
echo "  Namespace: ${NAMESPACE}"
echo "  Buckets:   ${BUCKETS}"

# Add MinIO Helm repo (idempotent)
helm repo add minio-official https://charts.min.io/ 2>/dev/null || true
helm repo update minio-official

# Build --set bucket args from comma-separated BUCKETS variable.
# MinIO chart buckets format: buckets[N].name, buckets[N].policy, buckets[N].purge
bucket_args=""
idx=0
IFS=',' read -ra bucket_list <<< "${BUCKETS}"
for bucket in "${bucket_list[@]}"; do
    bucket_args="${bucket_args} --set buckets[${idx}].name=${bucket} --set buckets[${idx}].policy=none --set buckets[${idx}].purge=false"
    idx=$((idx + 1))
done

# Install or upgrade minio — idempotent, safe to re-run.
# The MinIO chart defaults to a 16Gi memory request which exhausts the Kind cluster.
# Cap it to 512Mi request / 1Gi limit; ample for the small block counts in CI tests.
# shellcheck disable=SC2086
helm upgrade --install "${RELEASE_NAME}" minio-official/minio \
  --namespace "${NAMESPACE}" \
  --set rootUser=minioadmin \
  --set rootPassword=minioadmin123 \
  --set mode=standalone \
  --set persistence.size=5Gi \
  --set resources.requests.memory=512Mi \
  --set resources.limits.memory=1Gi \
  ${bucket_args} \
  --wait --timeout 5m

echo "Waiting for MinIO pod readiness..."
kubectl wait --for=condition=ready pod \
  -l "app=${RELEASE_NAME}" \
  -n "${NAMESPACE}" \
  --timeout=300s

# Wait for the bucket-creation job (runs mc to create buckets after minio starts)
echo "Waiting for bucket creation job..."
kubectl wait --for=condition=complete job \
  -l "app=minio-make-bucket-job" \
  -n "${NAMESPACE}" \
  --timeout=120s 2>/dev/null || true

# Create credentials Secret used by both cloud-storage plugins.
# --dry-run=client -o yaml | apply -f - is idempotent (no error if secret exists).
echo "Creating minio-cloud-storage-creds secret..."
kubectl create secret generic minio-cloud-storage-creds \
  --namespace "${NAMESPACE}" \
  --from-literal=CLOUD_STORAGE_ARCHIVE_ACCESS_KEY=minioadmin \
  --from-literal=CLOUD_STORAGE_ARCHIVE_SECRET_KEY=minioadmin123 \
  --from-literal=CLOUD_STORAGE_EXPANDED_ACCESS_KEY=minioadmin \
  --from-literal=CLOUD_STORAGE_EXPANDED_SECRET_KEY=minioadmin123 \
  --dry-run=client -o yaml | kubectl apply -f -

echo "Minio deployment complete"
