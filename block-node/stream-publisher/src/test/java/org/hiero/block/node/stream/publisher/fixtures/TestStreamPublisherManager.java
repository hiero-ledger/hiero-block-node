// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.stream.publisher.fixtures;

import com.hedera.hapi.block.stream.BlockProof;
import com.hedera.pbj.runtime.grpc.Pipeline;
import edu.umd.cs.findbugs.annotations.NonNull;
import org.hiero.block.api.PublishStreamResponse;
import org.hiero.block.node.stream.publisher.PublisherHandler;
import org.hiero.block.node.stream.publisher.PublisherHandler.MetricsHolder;
import org.hiero.block.node.stream.publisher.StreamPublisherManager;

/**
 * @todo add documentation
 */
public class TestStreamPublisherManager implements StreamPublisherManager {
    /** The BlockAction to return when querying for next action for a block. */
    private BlockAction blockAction;
    /** The latest block number to be returned. */
    private long latestBlockNumber = -1L;

    @Override
    public PublisherHandler addHandler(
            @NonNull final Pipeline<? super PublishStreamResponse> replies,
            @NonNull final MetricsHolder handlerMetrics) {
        // do nothing, implement when needed
        return null;
    }

    @Override
    public void removeHandler(final long handlerId) {
        // do nothing, implement when needed
    }

    @Override
    public BlockAction getActionForBlock(
            final long blockNumber, final BlockAction previousAction, final long handlerId) {
        return blockAction;
        // @todo consider if we should reset the action here so we know that
        //    the action returned is always set separately for each message.
    }

    @Override
    public void closeBlock(final BlockProof blockEndProof, final long handlerId) {
        // do nothing, implement when needed
    }

    @Override
    public long getLatestBlockNumber() {
        return latestBlockNumber;
    }

    /**
     * Fixture method to set the block action.
     * <p>
     * This method will set the action to be returned by
     * {@link StreamPublisherManager#getActionForBlock(long, BlockAction, long)}.<br/>
     * Overwritable.<br/>
     * If this method has not been called, the default return from getActionForBlock is null.<br/>
     * We use null so as to always be explicit about the action to be returned in tests,
     * otherwise we might think tests are covering cases that are not covered.
     *
     * @param blockAction The action to return. This value is returned until
     *     this method is called again.
     */
    public void setBlockAction(final BlockAction blockAction) {
        this.blockAction = blockAction;
    }

    /**
     * Fixture method. This method will set the latest block number to be returned
     * by {@link #getLatestBlockNumber()}. Overwritable. If not set, it will
     * return -1L, which is a best effort to ensure no false positives.
     *
     * @param latestBlockNumber to set
     */
    public void setLatestBlockNumber(final long latestBlockNumber) {
        this.latestBlockNumber = latestBlockNumber;
    }
}
