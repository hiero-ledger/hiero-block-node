// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.expanded.cloud.storage;

import static java.lang.System.Logger.Level.INFO;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * A no-operation {@link S3Client} implementation for testing only. Every method logs at INFO
 * level and returns an empty/null result. No exceptions are ever thrown.
 */
public class NoOpS3Client implements S3Client {

    private final System.Logger LOGGER = System.getLogger(getClass().getName());

    /** Constructs a new {@code NoOpS3Client}. */
    public NoOpS3Client() {}

    /** {@inheritDoc} */
    @Override
    public void uploadFile(
            final String objectKey,
            final String storageClass,
            final Iterator<byte[]> contentIterable,
            final String contentType)
            throws com.hedera.bucky.S3ClientException, IOException {
        LOGGER.log(
                INFO,
                "NoOpS3Client.uploadFile called: key={0}, storageClass={1}, contentType={2}",
                objectKey,
                storageClass,
                contentType);
    }

    /** {@inheritDoc} */
    @Override
    public void uploadTextFile(final String objectKey, final String storageClass, final String content)
            throws com.hedera.bucky.S3ClientException, IOException {
        LOGGER.log(
                INFO,
                "NoOpS3Client.uploadTextFile called: key={0}, storageClass={1}",
                objectKey,
                storageClass);
    }

    /** {@inheritDoc} */
    @Override
    public String downloadTextFile(final String key) throws com.hedera.bucky.S3ClientException, IOException {
        LOGGER.log(INFO, "NoOpS3Client.downloadTextFile called: key={0}", key);
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public List<String> listObjects(final String prefix, final int maxResults) throws com.hedera.bucky.S3ClientException, IOException {
        LOGGER.log(INFO, "NoOpS3Client.listObjects called: prefix={0}, maxResults={1}", prefix, maxResults);
        return Collections.emptyList();
    }

    /** {@inheritDoc} */
    @Override
    public Map<String, List<String>> listMultipartUploads() throws com.hedera.bucky.S3ClientException, IOException {
        LOGGER.log(INFO, "NoOpS3Client.listMultipartUploads called");
        return Collections.emptyMap();
    }

    /** {@inheritDoc} */
    @Override
    public void abortMultipartUpload(final String key, final String uploadId) throws com.hedera.bucky.S3ClientException, IOException {
        LOGGER.log(INFO, "NoOpS3Client.abortMultipartUpload called: key={0}, uploadId={1}", key, uploadId);
    }

    /** {@inheritDoc} */
    @Override
    public void close() {
        LOGGER.log(INFO, "NoOpS3Client.close called");
    }
}
