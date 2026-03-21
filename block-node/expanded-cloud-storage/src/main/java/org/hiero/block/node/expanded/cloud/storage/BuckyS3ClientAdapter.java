// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.expanded.cloud.storage;

import com.hedera.bucky.S3ClientInitializationException;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.util.Iterator;

/**
 * Production implementation of {@link S3Client} that delegates to
 * {@code com.hedera.bucky.S3Client} ({@code bucky-client} on Maven Central).
 *
 * <p>Bucky's {@code S3Client} is {@code final}, so this adapter implements the local
 * {@link S3Client} interface to provide a test seam. No exception translation is needed —
 * bucky throws {@link com.hedera.bucky.S3ResponseException} (a subtype of
 * {@link com.hedera.bucky.S3ClientException}), which satisfies the interface's declared
 * throws clause directly.
 */
class BuckyS3ClientAdapter implements S3Client {

    private final com.hedera.bucky.S3Client delegate;

    /**
     * Constructs a new adapter from the given plugin configuration.
     *
     * @param config provides the S3 endpoint, bucket, region, and credentials
     * @throws S3ClientInitializationException if the bucky client cannot be initialised
     */
    BuckyS3ClientAdapter(@NonNull final ExpandedCloudStorageConfig config)
            throws S3ClientInitializationException {
        this.delegate = new com.hedera.bucky.S3Client(
                config.regionName(),
                config.endpointUrl(),
                config.bucketName(),
                config.accessKey(),
                config.secretKey());
    }

    /** {@inheritDoc} */
    @Override
    public void uploadFile(
            final String objectKey,
            final String storageClass,
            final Iterator<byte[]> contentIterable,
            final String contentType)
            throws com.hedera.bucky.S3ClientException, IOException {
        delegate.uploadFile(objectKey, storageClass, contentIterable, contentType);
    }

    /** {@inheritDoc} */
    @Override
    public void close() {
        delegate.close();
    }
}
