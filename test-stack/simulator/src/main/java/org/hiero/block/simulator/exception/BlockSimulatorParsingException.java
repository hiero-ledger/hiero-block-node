// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.exception;

/** Use this checked exception to represent a Block Simulator parsing exception. */
public class BlockSimulatorParsingException extends Exception {
    /**
     * Constructs a new parsing exception with the specified detail message.
     *
     * @param message the detail message
     */
    public BlockSimulatorParsingException(final String message) {
        super(message);
    }

    /**
     * Constructs a new parsing exception with the specified cause.
     *
     * @param cause the cause of the exception, could be null, see super javadoc
     */
    public BlockSimulatorParsingException(final Throwable cause) {
        super(cause);
    }
}
