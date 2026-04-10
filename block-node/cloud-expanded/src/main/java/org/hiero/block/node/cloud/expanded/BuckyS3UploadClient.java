// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.cloud.expanded;

import java.io.IOException;
import java.util.Iterator;

/// Production implementation of {@link S3UploadClient} backed by `com.hedera.bucky.S3Client`.
///
/// Created via {@link S3UploadClient#getInstance} using plugin configuration. This is a
/// named concrete class rather than an anonymous inner class so that it appears by name in
/// stack traces and heap dumps, making debugging easier.
final class BuckyS3UploadClient extends S3UploadClient {

    private final com.hedera.bucky.S3Client bucky;

    /// Constructs a new client from plugin configuration.
    ///
    /// @param config the plugin configuration supplying endpoint, bucket, region, and credentials
    /// @throws com.hedera.bucky.S3ClientInitializationException if the underlying bucky client
    ///         cannot be initialised (e.g. invalid credentials, unreachable endpoint)
    BuckyS3UploadClient(final ExpandedCloudStorageConfig config)
            throws com.hedera.bucky.S3ClientInitializationException {
        this.bucky = new com.hedera.bucky.S3Client(
                config.regionName(), config.endpointUrl(), config.bucketName(),
                config.accessKey(), config.secretKey());
    }

    @Override
    void uploadFile(
            final String objectKey,
            final String storageClass,
            final Iterator<byte[]> contentIterable,
            final String contentType)
            throws com.hedera.bucky.S3ClientException, IOException {
        bucky.uploadFile(objectKey, storageClass, contentIterable, contentType);
    }

    @Override
    public void close() {
        bucky.close();
    }
}
