// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.expanded.cloud.storage;

import java.io.IOException;
import java.util.Iterator;

/**
 * Minimal abstraction over an S3-compatible object-store client, covering only the
 * operations used by this plugin.
 *
 * <p>The production implementation is {@link BuckyS3ClientAdapter}, which wraps the
 * {@code final} {@code com.hedera.bucky.S3Client} class. {@code NoOpS3Client} (in
 * {@code src/test/java}) is available for unit testing.
 */
public interface S3Client extends AutoCloseable {

    /**
     * Uploads a file to S3 using multipart upload.
     *
     * @param objectKey       the key for the object in S3
     * @param storageClass    the S3 storage class (e.g., {@code "STANDARD"})
     * @param contentIterable an iterator of byte-array chunks representing the file content
     * @param contentType     the MIME content type (e.g., {@code "application/octet-stream"})
     * @throws com.hedera.bucky.S3ClientException if the S3 service returns an error
     * @throws IOException                         if an I/O error occurs
     */
    void uploadFile(
            String objectKey,
            String storageClass,
            Iterator<byte[]> contentIterable,
            String contentType)
            throws com.hedera.bucky.S3ClientException, IOException;

    /** Releases resources held by this client. Must be idempotent. */
    @Override
    void close();
}
