// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.expanded.cloud.storage;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.hiero.block.node.base.s3.S3ClientInitializationException;

/**
 * Adapts the {@code org.hiero.block.node.base.s3.S3Client} concrete class to the
 * {@link S3Client} interface defined by this module.
 *
 * <p>This is the production implementation used until hedera-bucky is available on Maven Central.
 * When hedera-bucky ships, replace this adapter with {@code HederaBuckyS3ClientAdapter} — no
 * changes to plugin logic or tests are needed.
 */
public class BaseS3ClientAdapter implements S3Client {

    private final org.hiero.block.node.base.s3.S3Client delegate;

    /**
     * Constructs a new adapter using the given expanded cloud storage configuration.
     *
     * @param config the plugin configuration providing S3 credentials and endpoint
     * @throws S3ClientException if the underlying client cannot be initialised
     */
    public BaseS3ClientAdapter(@NonNull final ExpandedCloudStorageConfig config) throws S3ClientException {
        try {
            this.delegate = new org.hiero.block.node.base.s3.S3Client(
                    config.regionName(),
                    config.endpointUrl(),
                    config.bucketName(),
                    config.accessKey(),
                    config.secretKey());
        } catch (final S3ClientInitializationException e) {
            throw new S3ClientException("Failed to initialise base S3 client", e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void uploadFile(
            final String objectKey,
            final String storageClass,
            final Iterator<byte[]> contentIterable,
            final String contentType)
            throws S3ClientException, IOException {
        try {
            delegate.uploadFile(objectKey, storageClass, contentIterable, contentType);
        } catch (final org.hiero.block.node.base.s3.S3ClientException e) {
            throw new S3ClientException(e.getMessage(), e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void uploadTextFile(final String objectKey, final String storageClass, final String content)
            throws S3ClientException, IOException {
        try {
            delegate.uploadTextFile(objectKey, storageClass, content);
        } catch (final org.hiero.block.node.base.s3.S3ClientException e) {
            throw new S3ClientException(e.getMessage(), e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public String downloadTextFile(final String key) throws S3ClientException, IOException {
        try {
            return delegate.downloadTextFile(key);
        } catch (final org.hiero.block.node.base.s3.S3ClientException e) {
            throw new S3ClientException(e.getMessage(), e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public List<String> listObjects(final String prefix, final int maxResults) throws S3ClientException, IOException {
        try {
            return delegate.listObjects(prefix, maxResults);
        } catch (final org.hiero.block.node.base.s3.S3ClientException e) {
            throw new S3ClientException(e.getMessage(), e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public Map<String, List<String>> listMultipartUploads() throws S3ClientException, IOException {
        try {
            return delegate.listMultipartUploads();
        } catch (final org.hiero.block.node.base.s3.S3ClientException e) {
            throw new S3ClientException(e.getMessage(), e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void abortMultipartUpload(final String key, final String uploadId) throws S3ClientException, IOException {
        try {
            delegate.abortMultipartUpload(key, uploadId);
        } catch (final org.hiero.block.node.base.s3.S3ClientException e) {
            throw new S3ClientException(e.getMessage(), e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void close() {
        delegate.close();
    }
}
