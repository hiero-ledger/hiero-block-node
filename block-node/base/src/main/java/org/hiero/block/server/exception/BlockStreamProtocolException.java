// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.exception;

/**
 * Use this checked exception to represent a Block Node protocol exception encountered while
 * processing block items.
 */
public class BlockStreamProtocolException extends Exception {

    /**
     * Constructs a new exception with the specified detail message.
     *
     * @param message the detail message
     */
    public BlockStreamProtocolException(String message) {
        super(message);
    }

    public BlockStreamProtocolException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
