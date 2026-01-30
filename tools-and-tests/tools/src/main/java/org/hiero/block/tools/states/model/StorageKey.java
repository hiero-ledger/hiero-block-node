// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.states.model;

import java.io.DataInputStream;
import java.io.IOException;
import org.hiero.block.tools.states.postgres.BlobType;
import org.hiero.block.tools.states.utils.Utils;

public record StorageKey(String path) {
    private static final long CURRENT_VERSION = 1;
    private static final long OBJECT_ID = 15487002;

    public static StorageKey copyFrom(DataInputStream inStream) throws IOException {
        long version = inStream.readLong();
        if (version != CURRENT_VERSION) {
            throw new IOException("Unsupported StorageKey version: " + version);
        }
        long objectId = inStream.readLong();
        if (objectId != OBJECT_ID) {
            throw new IOException("Unexpected StorageKey object ID: " + objectId);
        }

        String path = Utils.readNormalisedString(inStream);
        return new StorageKey(path);
    }

    /**
     * Returns the ID extracted from the path.
     * The path is expected to be in the format <pre>/0/&lt;type_char&gt;&lt;id&gt;</pre>.
     *
     * @return the ID as a long
     */
    public long getId() {
        return Long.parseLong(path.substring(4));
    }

    /**
     * Returns the BlobType based on the character at position 3 in the path.
     * The path is expected to be in the format <pre>/0/&lt;type_char&gt;&lt;id&gt;</pre>.
     *
     * @return the BlobType corresponding to the character at position 3
     * @throws IllegalArgumentException if the path format is invalid
     */
    public BlobType getBlobType() {
        if (path.length() < 5) {
            throw new IllegalArgumentException("Invalid path format: " + path);
        }
        char typeChar = path.charAt(3);
        return BlobType.typeFromCharCode(typeChar);
    }
}
