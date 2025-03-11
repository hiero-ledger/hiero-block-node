// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.persistence.storage.archive;

/**
 * A simple exception class to represent an error during block archiving.
 */
public final class BlockArchivingException extends RuntimeException {
    public BlockArchivingException() {
        super();
    }

    public BlockArchivingException(final String message) {
        super(message);
    }

    public BlockArchivingException(final Throwable cause) {
        super(cause);
    }

    public BlockArchivingException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
