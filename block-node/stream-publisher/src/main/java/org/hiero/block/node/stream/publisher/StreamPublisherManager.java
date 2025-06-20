// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.stream.publisher;

import com.hedera.pbj.runtime.grpc.Pipeline;
import edu.umd.cs.findbugs.annotations.NonNull;
import org.hiero.block.api.PublishStreamResponse;
import org.hiero.block.node.spi.blockmessaging.BlockNotificationHandler;

/**
 * TODO: add documentation
 */
public interface StreamPublisherManager extends BlockNotificationHandler {
    /**
     * TODO add documentation
     */
    PublisherHandler addHandler(
            @NonNull final Pipeline<? super PublishStreamResponse> replies,
            @NonNull final PublisherHandler.MetricsHolder handlerMetrics);

    /**
     * TODO add documentation
     */
    void removeHandler(final long handlerId);

    /**
     * Given a block number, determine the action to take for that block.
     * <p>
     * This method is used to determine how to handle a block number
     * when it is received from a publisher.<br/>
     * This method must be checked for each batch of block items, the action
     * can change at any time due to the actions of other plugins or publishers.
     *
     * @param blockNumber the block number to evaluate
     * @param previousAction the previous action returned by this method for
     *     the same block number, but an earlier batch.  This helps to ensure
     *     we don't update manager state incorrectly and also helps determine
     *     specific corner cases, including when RESEND is permitted.
     * @return the action to take for the given block number
     */
    BlockAction getActionForBlock(final long blockNumber, final BlockAction previousAction);

    /**
     * TODO add documentation
     */
    long getLatestBlockNumber();

    /**
     * The action to take within the PublisherHandler for a block.
     */
    public static enum BlockAction {
        ACCEPT,
        SKIP,
        RESEND,
        END_BEHIND,
        END_DUPLICATE,
        END_ERROR // Something has gone wrong, stop this publisher and tell them to start a new connection.
    }
}
