// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.server.persistence.storage.archive;

/**
 * An interface that defines an asynchronous local block archiver.
 */
public interface LocalBlockArchiver {
    /**
     * This method will submit a passed threshold. The archiver then will
     * proceed to archive the blocks lower than 1 order of magnitude below the
     * threshold, based on archive group size configuration.
     *
     * @param blockNumberThreshold the block number threshold passed
     */
    void submitThresholdPassed(final long blockNumberThreshold);
}
