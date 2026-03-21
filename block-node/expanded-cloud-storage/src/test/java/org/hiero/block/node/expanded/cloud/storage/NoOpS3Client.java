// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.expanded.cloud.storage;

import java.io.IOException;
import java.util.Iterator;

/**
 * A no-operation {@link S3Client} for unit tests. Both methods are silent no-ops.
 * Override {@link #uploadFile} in anonymous subclasses to capture or throw as needed.
 */
class NoOpS3Client implements S3Client {

    /** {@inheritDoc} */
    @Override
    public void uploadFile(
            final String objectKey,
            final String storageClass,
            final Iterator<byte[]> contentIterable,
            final String contentType)
            throws com.hedera.bucky.S3ClientException, IOException {}

    /** {@inheritDoc} */
    @Override
    public void close() {}
}
