# SPDX-License-Identifier: Apache-2.0
#
# Shared by solo-deploy-network.sh (static BNs) and solo-test-runner.sh (dynamically
# deployed BNs, e.g. archive-backfill's BN3) so the cloud-storage archive overlay is
# defined in exactly one place.

# Generate a cloud-storage archive overlay pointing to the in-namespace RustFS service.
# Does NOT set plugins.names — that comes from plugin-profile-cloud.yaml.
# Requires NAMESPACE to be set by the caller.
function generate_s3_archive_overlay {
  local output_file="$1"
  local s3_service="rustfs-svc.${NAMESPACE}.svc.cluster.local"
  cat > "${output_file}" << EOF
blockNode:
  secretRef: "cloud-storage-creds"
  config:
    CLOUD_STORAGE_ARCHIVE_ENDPOINT_URL: "http://${s3_service}:9000"
    CLOUD_STORAGE_ARCHIVE_REGION_NAME: "us-east-1"
    CLOUD_STORAGE_ARCHIVE_BUCKET_NAME: "block-archive-tar"
    CLOUD_STORAGE_ARCHIVE_GROUPING_LEVEL: "1"
    CLOUD_STORAGE_ARCHIVE_STORAGE_CLASS: "STANDARD"
    CLOUD_STORAGE_EXPANDED_ENDPOINT_URL: "http://${s3_service}:9000"
    CLOUD_STORAGE_EXPANDED_REGION_NAME: "us-east-1"
    CLOUD_STORAGE_EXPANDED_BUCKET_NAME: "block-archive-expanded"
    CLOUD_STORAGE_EXPANDED_STORAGE_CLASS: "STANDARD"
    CLOUD_STORAGE_EXPANDED_OBJECT_KEY_PREFIX: ""
EOF
  echo "Generated cloud archive overlay (endpoint: http://${s3_service}:9000)"
}
