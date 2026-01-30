// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.states.model;

import java.io.IOException;
import org.hiero.block.tools.states.utils.FCDataInputStream;

public record MapKey(long realmId, long shardId, long accountId) {
    private static final long CURRENT_VERSION = 1;
    private static final long OBJECT_ID = 15486487;

    public static MapKey copyFrom(FCDataInputStream inStream) throws IOException {
        long version = inStream.readLong();
        if (version != CURRENT_VERSION) {
            throw new IOException("Unsupported MapKey version: " + version);
        }
        long objectId = inStream.readLong();
        if (objectId != OBJECT_ID) {
            throw new IOException("Unexpected MapKey object ID: " + objectId);
        }
        long realmId = inStream.readLong();
        long shardId = inStream.readLong();
        long accountId = inStream.readLong();

        return new MapKey(realmId, shardId, accountId);
    }
}
