// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.server.persistence.storage.archive;

import com.hedera.block.server.persistence.storage.PersistenceStorageConfig;
import java.lang.System.Logger.Level;

/**
 * An {@link AsyncLocalBlockArchiver} that utilizes the
 * {@link PersistenceStorageConfig.StorageType#NO_OP}
 * persistence type.
 */
public final class AsyncNoOpArchiver implements AsyncLocalBlockArchiver {
    private static final System.Logger LOGGER = System.getLogger(AsyncNoOpArchiver.class.getName());
    private final long blockNumberThreshold;

    AsyncNoOpArchiver(final long blockNumberThreshold) {
        this.blockNumberThreshold = blockNumberThreshold;
    }

    /**
     * No Op archiver, does nothing. No preconditions either.
     */
    @Override
    public void run() {
        LOGGER.log(
                Level.DEBUG,
                "No Op Archiver started because block number threshold [%d] passed".formatted(blockNumberThreshold));
    }
}
