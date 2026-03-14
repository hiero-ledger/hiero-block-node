// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.expanded.cloud.storage;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Abstraction over an S3-compatible object-store client. The method signatures mirror
 * hedera-bucky's public API so that a future {@code HederaBuckyS3ClientAdapter} can implement
 * this interface as a thin delegation wrapper — no changes to plugin logic required.
 *
 * <p>The current production implementation is {@link BaseS3ClientAdapter}, which wraps the
 * {@code org.hiero.block.node.base.s3.S3Client} concrete class. When hedera-bucky is available
 * on Maven Central, swap the factory in {@link ExpandedCloudStoragePlugin} for a
 * {@code HederaBuckyS3ClientAdapter} and delete or retain {@link NoOpS3Client} for testing.
 */
public interface S3Client extends AutoCloseable {

    /**
     * Uploads a file to S3 using multipart upload.
     *
     * @param objectKey     the key for the object in S3 (e.g., {@code "blocks/0000000000001234567.blk.zstd"})
     * @param storageClass  the storage class (e.g., {@code "STANDARD"})
     * @param contentIterable an iterator of byte-array chunks representing the file content
     * @param contentType   the MIME content type (e.g., {@code "application/octet-stream"})
     * @throws S3ClientException if the S3 service returns an error response
     * @throws IOException       if an I/O error occurs
     */
    void uploadFile(
            String objectKey,
            String storageClass,
            Iterator<byte[]> contentIterable,
            String contentType)
            throws S3ClientException, IOException;

    /**
     * Uploads a UTF-8 text file to S3 using a single-part PUT.
     *
     * @param objectKey    the key for the object in S3
     * @param storageClass the storage class
     * @param content      the text content to upload
     * @throws S3ClientException if the S3 service returns an error response
     * @throws IOException       if an I/O error occurs
     */
    void uploadTextFile(String objectKey, String storageClass, String content)
            throws S3ClientException, IOException;

    /**
     * Downloads a text file from S3.
     *
     * @param key the object key
     * @return the file contents as a {@code String}, or {@code null} if the object does not exist (404)
     * @throws S3ClientException if the S3 service returns a non-404 error response
     * @throws IOException       if an I/O error occurs
     */
    String downloadTextFile(String key) throws S3ClientException, IOException;

    /**
     * Lists objects in the S3 bucket with the given prefix.
     *
     * @param prefix     the key prefix to filter by; use an empty string to list all objects
     * @param maxResults maximum number of results to return (1–1000)
     * @return list of object keys matching the prefix
     * @throws S3ClientException if the S3 service returns an error response
     * @throws IOException       if an I/O error occurs
     */
    List<String> listObjects(String prefix, int maxResults) throws S3ClientException, IOException;

    /**
     * Lists all in-progress multipart uploads in the bucket.
     *
     * @return a map of object key → list of upload IDs; empty map if none
     * @throws S3ClientException if the S3 service returns an error response
     * @throws IOException       if an I/O error occurs
     */
    Map<String, List<String>> listMultipartUploads() throws S3ClientException, IOException;

    /**
     * Aborts an in-progress multipart upload.
     *
     * @param key      the object key
     * @param uploadId the multipart upload ID to abort
     * @throws S3ClientException if the S3 service returns an error response
     * @throws IOException       if an I/O error occurs
     */
    void abortMultipartUpload(String key, String uploadId) throws S3ClientException, IOException;

    /**
     * Releases resources held by this client. Must be idempotent.
     */
    @Override
    void close();
}
