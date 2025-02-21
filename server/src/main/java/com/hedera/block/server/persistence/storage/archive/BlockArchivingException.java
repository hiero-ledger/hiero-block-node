// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.server.persistence.storage.archive;

/**
 * A simple exception class to represent an error during block archiving.
 */
public final class BlockArchivingException extends RuntimeException {
    BlockArchivingException(final Throwable cause) {
        super(cause);
    }
}
