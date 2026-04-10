// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.cloud.expanded;

import java.io.IOException;
import java.util.Iterator;

/// Minimal abstraction over S3 upload operations used by this plugin.
///
/// The production instance is obtained via {@link #getInstance}, which wraps
/// `com.hedera.bucky.S3Client` directly. Package-private subclasses in the test
/// source tree override {@link #uploadFile} to capture or throw as needed — no Docker
/// or real S3 endpoint required for unit tests.
abstract class S3UploadClient implements AutoCloseable {

    /// Uploads content to S3 using multipart upload.
    ///
    /// @param objectKey       the key for the object in S3
    /// @param storageClass    the S3 storage class (e.g., `"STANDARD"`)
    /// @param contentIterable an iterator of byte-array chunks representing the file content
    /// @param contentType     the MIME content type (e.g., `"application/octet-stream"`)
    /// @throws com.hedera.bucky.S3ClientException if the S3 service returns an error
    /// @throws IOException                         if an I/O error occurs
    abstract void uploadFile(
            String objectKey, String storageClass, Iterator<byte[]> contentIterable, String contentType)
            throws com.hedera.bucky.S3ClientException, IOException;


    /// Creates a production {@link S3UploadClient} backed by `com.hedera.bucky.S3Client`.
    ///
    /// Returns a {@link BuckyS3UploadClient} instance.
    ///
    /// @param config the plugin configuration supplying endpoint, bucket, region, and credentials
    /// @return a new upload client
    /// @throws com.hedera.bucky.S3ClientInitializationException if the underlying client cannot be initialised
    static S3UploadClient getInstance(final ExpandedCloudStorageConfig config)
            throws com.hedera.bucky.S3ClientInitializationException {
        return new BuckyS3UploadClient(config);
    }
}
