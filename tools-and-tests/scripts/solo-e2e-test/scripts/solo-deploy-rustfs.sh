#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Deploys RustFS via Helm into a Kubernetes namespace and creates the
# cloud-storage-creds Secret used by cloud-storage-archive and
# cloud-storage-expanded block node plugins.
#
# The RustFS chart has no bucket-creation hook, so buckets are created
# post-startup via a one-shot Job running the aws-cli client, which works
# unmodified against RustFS's S3-compatible API.
#
# Usage:
#   ./solo-deploy-rustfs.sh --namespace NS [--release-name NAME] [--buckets LIST]
#
# Options:
#   --namespace NS        Kubernetes namespace (required)
#   --release-name NAME   Helm release name (default: rustfs)
#   --buckets LIST        Comma-separated bucket names to create on startup
#                         (default: block-archive-tar,block-archive-expanded)

set -eo pipefail

NAMESPACE=""
RELEASE_NAME="rustfs"
BUCKETS="block-archive-tar,block-archive-expanded"
# Must differ from the well-known "rustfsadmin" default: the chart refuses to
# render with default credentials (accidental-insecure-deploy guard).
ACCESS_KEY="blocknodeciaccess"
SECRET_KEY="blocknodecisecret123"

while [[ $# -gt 0 ]]; do
  case $1 in
    --namespace)    NAMESPACE="$2"; shift 2 ;;
    --release-name) RELEASE_NAME="$2"; shift 2 ;;
    --buckets)      BUCKETS="$2"; shift 2 ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
done

[[ -z "${NAMESPACE}" ]] && { echo "ERROR: --namespace is required"; exit 1; }

echo "Deploying RustFS"
echo "  Release:   ${RELEASE_NAME}"
echo "  Namespace: ${NAMESPACE}"
echo "  Buckets:   ${BUCKETS}"

# Add RustFS Helm repo (idempotent — --force-update avoids "already exists" error on re-runs)
helm repo add rustfs https://charts.rustfs.com --force-update
helm repo update rustfs

# Standalone mode (single pod, single PVC) — plenty for CI's small block counts.
# The chart defaults to distributed mode (4 pods / 16 PVCs), which would blow
# past the Kind cluster's resource budget for what is just a CI test double.
helm upgrade --install "${RELEASE_NAME}" rustfs/rustfs \
  --namespace "${NAMESPACE}" \
  --set mode.standalone.enabled=true \
  --set mode.distributed.enabled=false \
  --set secret.rustfs.access_key="${ACCESS_KEY}" \
  --set secret.rustfs.secret_key="${SECRET_KEY}" \
  --set resources.requests.memory=512Mi \
  --set resources.limits.memory=1Gi \
  --set storageclass.name="" \
  --set storageclass.dataStorageSize=5Gi \
  --set ingress.enabled=false \
  --wait --timeout 5m

echo "Waiting for RustFS pod readiness..."
kubectl wait --for=condition=ready pod \
  -l "app.kubernetes.io/instance=${RELEASE_NAME}" \
  -n "${NAMESPACE}" \
  --timeout=300s

# Create credentials Secret before bucket init so block nodes can start even if the
# bucket job is slow (large aws-cli image pull on a fresh cloud node). The block
# node plugins need the secret at startup; they will retry failed uploads once
# buckets exist.
# --dry-run=client -o yaml | apply -f - is idempotent (no error if secret exists).
echo "Creating cloud-storage-creds secret..."
kubectl create secret generic cloud-storage-creds \
  --namespace "${NAMESPACE}" \
  --from-literal=CLOUD_STORAGE_ARCHIVE_ACCESS_KEY="${ACCESS_KEY}" \
  --from-literal=CLOUD_STORAGE_ARCHIVE_SECRET_KEY="${SECRET_KEY}" \
  --from-literal=CLOUD_STORAGE_EXPANDED_ACCESS_KEY="${ACCESS_KEY}" \
  --from-literal=CLOUD_STORAGE_EXPANDED_SECRET_KEY="${SECRET_KEY}" \
  --dry-run=client -o yaml | kubectl apply -f -

# Build the aws-cli script that creates every requested bucket.
# "aws s3 mb" errors if the bucket already exists — ignore that to stay idempotent.
s3_endpoint="http://${RELEASE_NAME}-svc.${NAMESPACE}.svc.cluster.local:9000"
aws_script=""
IFS=',' read -ra bucket_list <<< "${BUCKETS}"
for bucket in "${bucket_list[@]}"; do
    aws_script="${aws_script} aws --endpoint-url ${s3_endpoint} s3 mb s3://${bucket} 2>/dev/null || true;"
done

# Run bucket creation as a one-shot Job (RustFS's own image bundles no S3 client).
# Delete any stale Job first — Job pod templates are immutable, so a plain
# apply would fail on re-run.
# Timeout is 5m to allow for amazon/aws-cli image pull on a fresh cloud node.
echo "Creating buckets..."
job_name="${RELEASE_NAME}-bucket-init"
kubectl delete job "${job_name}" -n "${NAMESPACE}" --ignore-not-found >/dev/null 2>&1

cat <<EOF | kubectl apply -f -
apiVersion: batch/v1
kind: Job
metadata:
  name: ${job_name}
  namespace: ${NAMESPACE}
  labels:
    app: ${job_name}
spec:
  backoffLimit: 3
  template:
    metadata:
      labels:
        app: ${job_name}
    spec:
      restartPolicy: Never
      containers:
        - name: aws-cli
          image: amazon/aws-cli
          env:
            - name: AWS_ACCESS_KEY_ID
              value: "${ACCESS_KEY}"
            - name: AWS_SECRET_ACCESS_KEY
              value: "${SECRET_KEY}"
            - name: AWS_DEFAULT_REGION
              value: "us-east-1"
          command: ["/bin/sh", "-c", "${aws_script}"]
EOF

kubectl wait --for=condition=complete job \
  -l "app=${job_name}" \
  -n "${NAMESPACE}" \
  --timeout=300s

echo "RustFS deployment complete"
