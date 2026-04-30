// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.cloud.storage.expanded;

import java.io.IOException;
import java.util.Iterator;

/// Production implementation of {@link S3UploadClient} backed by `com.hedera.bucky.S3Client`.
///
/// Created using plugin configuration. This is a named concrete class rather than an anonymous
/// inner class so that it appears by name in stack traces and heap dumps, making debugging easier.
final class BuckyS3UploadClient implements S3UploadClient {

    /// Underlying bucky S3 client that performs the actual HTTP multipart upload.
    ///
    /// The fully-qualified name is intentional: importing `com.hedera.bucky.S3Client`
    /// would conflict with this package's own {@link S3UploadClient} in error messages
    /// and IDE navigation. The FQN keeps the two types visually distinct.
    private final com.hedera.bucky.S3Client bucky;

    /// Constructs a new client from plugin configuration.
    ///
    /// Wraps {@code com.hedera.bucky.S3ClientInitializationException} in {@link UploadException}
    /// so that bucky initialisation failures are expressed through the package abstraction.
    ///
    /// @param config the plugin configuration supplying endpoint, bucket, region, and credentials
    /// @throws UploadException if the underlying bucky client cannot be initialised
    ///         (e.g. blank required fields, unreachable endpoint)
    BuckyS3UploadClient(final ExpandedCloudStorageConfig config) throws UploadException {
        try {
            this.bucky = new com.hedera.bucky.S3Client(
                    config.regionName(),
                    config.endpointUrl(),
                    config.bucketName(),
                    config.accessKey(),
                    config.secretKey());
        } catch (final com.hedera.bucky.S3ClientInitializationException e) {
            throw new UploadException(e.getMessage(), e);
        }
    }

    /// {@inheritDoc}
    ///
    /// Translates {@code com.hedera.bucky.S3ClientException} into {@link UploadException}
    /// so that bucky types stay contained within this class and do not leak into the
    /// abstract {@link S3UploadClient} interface or any callers.
    @Override
    public void uploadFile(
            final String objectKey,
            final String storageClass,
            final Iterator<byte[]> contentIterable,
            final String contentType)
            throws UploadException, IOException {
        try {
            bucky.uploadFile(objectKey, storageClass, contentIterable, contentType);
        } catch (final com.hedera.bucky.S3ClientException e) {
            throw new UploadException(e.getMessage(), e);
        }
    }

    /// Closes the underlying bucky S3 client and releases its connection pool.
    @Override
    public void close() {
        bucky.close();
    }
}
