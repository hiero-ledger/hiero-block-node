// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.server.persistence.storage.archive;

import static java.lang.System.Logger.Level.TRACE;

import java.lang.System.Logger;

/**
 * A no-op implementation of {@link LocalBlockArchiver}.
 */
public class NoOpArchiver implements LocalBlockArchiver {
    private static final Logger LOGGER = System.getLogger(NoOpArchiver.class.getName());

    /**
     * No-op implementation. Does nothing.
     */
    @Override
    public void notifyBlockPersisted(long blockNumber) {
        // no-op
        LOGGER.log(TRACE, "No-op archiver invoked for block number threshold [%d]".formatted(blockNumber));
    }
}
