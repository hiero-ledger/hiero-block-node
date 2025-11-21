// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.records.model.parsed;

/**
 * Exception thrown when validation of a block fails.
 */
public class ValidationException extends Exception {
    public ValidationException(String message) {
        super(message);
    }
}
