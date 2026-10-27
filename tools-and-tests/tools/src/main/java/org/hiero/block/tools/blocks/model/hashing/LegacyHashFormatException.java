// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.model.hashing;

import java.io.IOException;

/**
 * Thrown when a hash state file has the layout of SHA-384 output from an older CLI version. Unlike a corrupt file,
 * it must not be deleted or silently replaced with fresh state.
 */
public class LegacyHashFormatException extends IOException {
    /**
     * Creates a new exception with the given message.
     *
     * @param message the detail message
     */
    public LegacyHashFormatException(String message) {
        super(message);
    }
}
