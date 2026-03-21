// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.expanded.cloud.storage;

import com.hedera.bucky.S3ClientInitializationException;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Production implementation of {@link S3Client} that delegates to the
 * {@code com.hedera.bucky.S3Client} library ({@code bucky-client} on Maven Central).
 *
 * <p>This is a thin delegation wrapper around the {@code final} bucky client class. No exception
 * translation is performed — bucky throws {@link com.hedera.bucky.S3ResponseException} (a subtype
 * of {@link com.hedera.bucky.S3ClientException}), which satisfies the interface's declared throws
 * clause directly.
 *
 * <p>This replaces {@link BaseS3ClientAdapter} as the production S3 client factory used by
 * {@link ExpandedCloudStoragePlugin}.
 */
public class BuckyS3ClientAdapter implements S3Client {

    private final com.hedera.bucky.S3Client delegate;

    /**
     * Constructs a new adapter using the given expanded cloud storage configuration.
     *
     * @param config the plugin configuration providing S3 credentials and endpoint
     * @throws S3ClientInitializationException if the bucky client cannot be initialised
     */
    public BuckyS3ClientAdapter(@NonNull final ExpandedCloudStorageConfig config)
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
    public void uploadTextFile(final String objectKey, final String storageClass, final String content)
            throws com.hedera.bucky.S3ClientException, IOException {
        delegate.uploadTextFile(objectKey, storageClass, content);
    }

    /** {@inheritDoc} */
    @Override
    public String downloadTextFile(final String key) throws com.hedera.bucky.S3ClientException, IOException {
        return delegate.downloadTextFile(key);
    }

    /** {@inheritDoc} */
    @Override
    public List<String> listObjects(final String prefix, final int maxResults)
            throws com.hedera.bucky.S3ClientException, IOException {
        return delegate.listObjects(prefix, maxResults);
    }

    /** {@inheritDoc} */
    @Override
    public Map<String, List<String>> listMultipartUploads() throws com.hedera.bucky.S3ClientException, IOException {
        return delegate.listMultipartUploads();
    }

    /** {@inheritDoc} */
    @Override
    public void abortMultipartUpload(final String key, final String uploadId)
            throws com.hedera.bucky.S3ClientException, IOException {
        delegate.abortMultipartUpload(key, uploadId);
    }

    /** {@inheritDoc} */
    @Override
    public void close() {
        delegate.close();
    }
}
