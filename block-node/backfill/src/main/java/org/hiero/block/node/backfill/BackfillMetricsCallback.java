// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.backfill;

/**
 * Callback interface for reporting backfill metrics.
 * Tasks use this interface to report their progress to the orchestrator
 * without needing to know about metric internals.
 */
public interface BackfillMetricsCallback {

    /**
     * Called when a block has been fetched from a peer node.
     *
     * @param blockNumber the block number that was fetched
     */
    void onBlockFetched(long blockNumber);

    /**
     * Called when a block has been dispatched to the messaging facility.
     *
     * @param blockNumber the block number that was dispatched
     */
    void onBlockDispatched(long blockNumber);

    /**
     * Called when an error occurs during fetching.
     *
     * @param error the error that occurred
     */
    void onFetchError(Throwable error);
}
