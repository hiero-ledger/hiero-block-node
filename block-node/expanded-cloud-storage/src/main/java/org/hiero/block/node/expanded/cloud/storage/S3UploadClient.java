// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.expanded.cloud.storage;

import java.io.IOException;
import java.util.Iterator;

/**
 * Minimal abstraction over S3 upload operations used by this plugin.
 *
 * <p>The production instance is obtained via {@link #forConfig}, which wraps
 * {@code com.hedera.bucky.S3Client} directly. Package-private subclasses in the test
 * source tree override {@link #uploadFile} to capture or throw as needed — no Docker
 * or real S3 endpoint required for unit tests.
 */
abstract class S3UploadClient implements AutoCloseable {

    /**
     * Uploads content to S3 using multipart upload.
     *
     * @param objectKey       the key for the object in S3
     * @param storageClass    the S3 storage class (e.g., {@code "STANDARD"})
     * @param contentIterable an iterator of byte-array chunks representing the file content
     * @param contentType     the MIME content type (e.g., {@code "application/octet-stream"})
     * @throws com.hedera.bucky.S3ClientException if the S3 service returns an error
     * @throws IOException                         if an I/O error occurs
     */
    abstract void uploadFile(String objectKey, String storageClass,
            Iterator<byte[]> contentIterable, String contentType)
            throws com.hedera.bucky.S3ClientException, IOException;

    /** Releases resources held by this client. Must be idempotent. */
    @Override
    public abstract void close();

    /**
     * Creates a production {@link S3UploadClient} backed by {@code com.hedera.bucky.S3Client}.
     *
     * @param config the plugin configuration supplying endpoint, bucket, region, and credentials
     * @return a new upload client
     * @throws com.hedera.bucky.S3ClientInitializationException if the underlying client cannot be initialised
     */
    static S3UploadClient forConfig(final ExpandedCloudStorageConfig config)
            throws com.hedera.bucky.S3ClientInitializationException {
        final com.hedera.bucky.S3Client bucky = new com.hedera.bucky.S3Client(
                config.regionName(), config.endpointUrl(), config.bucketName(),
                config.accessKey(), config.secretKey());
        return new S3UploadClient() {
            @Override
            void uploadFile(final String objectKey, final String storageClass,
                    final Iterator<byte[]> contentIterable, final String contentType)
                    throws com.hedera.bucky.S3ClientException, IOException {
                bucky.uploadFile(objectKey, storageClass, contentIterable, contentType);
            }

            @Override
            public void close() {
                bucky.close();
            }
        };
    }
}
