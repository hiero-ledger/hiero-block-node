// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.expanded.cloud.storage;

/**
 * Checked base exception for S3 client operations. Mirrors hedera-bucky's S3ResponseException
 * so that a future {@code HederaBuckyS3ClientAdapter} can map exceptions 1:1 without changing
 * plugin logic.
 */
public class S3ClientException extends Exception {

    /** Constructs an exception with no message or cause. */
    public S3ClientException() {
        super();
    }

    /**
     * Constructs an exception with the given message.
     *
     * @param message the detail message
     */
    public S3ClientException(final String message) {
        super(message);
    }

    /**
     * Constructs an exception with the given message and cause.
     *
     * @param message the detail message
     * @param cause the cause
     */
    public S3ClientException(final String message, final Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructs an exception with the given cause.
     *
     * @param cause the cause
     */
    public S3ClientException(final Throwable cause) {
        super(cause);
    }
}
