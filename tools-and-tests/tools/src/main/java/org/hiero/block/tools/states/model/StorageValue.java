// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.states.model;

import java.io.IOException;
import org.hiero.block.tools.states.utils.FCDataInputStream;

public record StorageValue(BinaryObject data) {
    private static final long LEGACY_VERSION = 1;
    private static final long CURRENT_VERSION = 2;
    private static final long OBJECT_ID = 15487003;

    public static StorageValue copyFrom(FCDataInputStream inStream) throws IOException {
        long version = inStream.readLong(); // read version
        if (version < LEGACY_VERSION || version > CURRENT_VERSION) {
            throw new IOException("Unsupported StorageValue version: " + version);
        }
        long objectId = inStream.readLong(); // read object id
        if (objectId != OBJECT_ID) {
            throw new IOException("Unexpected StorageValue object ID: " + objectId);
        }

        BinaryObject data = null;
        if (version == LEGACY_VERSION) {
            int length = inStream.readInt();
            if (length > 0) {
                byte[] newData = new byte[length];
                inStream.readFully(newData);
                data = new BinaryObject(newData);
            }
        } else {
            final boolean hasData = inStream.readBoolean();
            if (hasData) {
                data = new BinaryObject();
                data.copyFrom(inStream);
            }
        }

        return new StorageValue(data);
    }
}
