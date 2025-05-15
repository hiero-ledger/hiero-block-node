// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.base.s3;

/**
 * A checked exception to act as a base for all S3 client exceptions.
 */
public class S3ClientException extends Exception {
    /**
     * {@inheritDoc}
     */
    public S3ClientException() {
        super();
    }

    /**
     * {@inheritDoc}
     */
    public S3ClientException(final String message) {
        super(message);
    }

    /**
     * {@inheritDoc}
     */
    public S3ClientException(final Throwable cause) {
        super(cause);
    }

    /**
     * {@inheritDoc}
     */
    public S3ClientException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
