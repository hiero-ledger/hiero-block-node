// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.config;

/**
 * S3-specific configuration for accessing record streams from S3-compatible storage.
 *
 * <p>Used for networks that store record streams in Amazon S3 or S3-compatible
 * storage like MinIO, Ceph, or other private cloud storage.
 *
 * @param endpoint    S3 endpoint URL (e.g., "http://minio.solo-network.svc.cluster.local:9000")
 * @param region      AWS region or arbitrary region for S3-compatible storage (e.g., "us-east-1")
 * @param accessKey   S3 access key ID
 * @param secretKey   S3 secret access key
 * @param bucketName  S3 bucket name containing record streams
 * @param pathPrefix  path prefix within the bucket (e.g., "recordstreams/")
 */
public record S3Config(
        String endpoint, String region, String accessKey, String secretKey, String bucketName, String pathPrefix) {

    /**
     * Creates an S3Config for MinIO with default credentials.
     *
     * @param endpoint   MinIO endpoint URL
     * @param bucketName bucket name
     * @return S3Config for MinIO
     */
    public static S3Config forMinIO(final String endpoint, final String bucketName) {
        return new S3Config(
                endpoint,
                "us-east-1", // MinIO default region
                "minioadmin",
                "minioadmin",
                bucketName,
                "recordstreams/");
    }
}
