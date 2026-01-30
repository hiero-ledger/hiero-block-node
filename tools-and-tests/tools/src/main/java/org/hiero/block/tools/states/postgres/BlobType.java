// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.states.postgres;

/**
 * Enum of blob types stored in the blob storage.
 */
public enum BlobType {
    FILE_DATA,
    FILE_METADATA,
    CONTRACT_STORAGE,
    CONTRACT_BYTECODE,
    SYSTEM_DELETED_ENTITY_EXPIRY;

    /**
     * Returns the type corresponding to a legacy character code.
     *
     * @param code the legacy blob code
     * @return the blob type
     */
    public static BlobType typeFromCharCode(final char code) {
        return switch (code) {
            case 'f' -> FILE_DATA;
            case 'k' -> FILE_METADATA;
            case 's' -> CONTRACT_BYTECODE;
            case 'd' -> CONTRACT_STORAGE;
            case 'e' -> SYSTEM_DELETED_ENTITY_EXPIRY;
            default -> throw new IllegalArgumentException("Invalid legacy code '" + code + "'");
        };
    }
}
