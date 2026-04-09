// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.stream.publisher;

import com.hedera.pbj.runtime.grpc.Pipeline;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.Deque;
import org.hiero.block.api.PublishStreamResponse;
import org.hiero.block.internal.BlockItemSetUnparsed;
import org.hiero.block.node.spi.blockmessaging.BlockNotificationHandler;

/// todo(1420) add documentation
public interface StreamPublisherManager extends BlockNotificationHandler {
    /// todo(1420) add documentation
    PublisherHandler addHandler(
            @NonNull final Pipeline<? super PublishStreamResponse> replies,
            @NonNull final PublisherHandler.MetricsHolder handlerMetrics,
            @NonNull final String correlationId);

    /// todo(1420) add documentation
    void removeHandler(final long handlerId);

    /// Given a block number, determine the action to take for that block.
    ///
    /// This method is used to determine how to handle a block number
    /// when it is received from a publisher.
    /// This method must be checked for each batch of block items, the action
    /// can change at any time due to the actions of other plugins or publishers.
    ///
    /// @param blockNumber the block number to evaluate
    /// @param previousAction the previous action returned by this method for
    ///         the same block number, but an earlier batch.  This helps to ensure
    ///         we don't update manager state incorrectly and also helps determine
    ///         specific corner cases, including when RESEND is permitted.
    /// @param handlerId The ID of the handler calling this method.
    ///
    /// @return the action to take for the given block number
    BlockAction getActionForBlock(
            final long blockNumber, @Nullable final BlockAction previousAction, final long handlerId);

    /// This method registers a queue for a block by number, to which items will be transferred to the manager from
    /// a publisher.
    void registerQueueForBlock(final long handlerId, final Deque<BlockItemSetUnparsed> queue, final long blockNumber);

    /// Close a block for a handler.
    void closeBlock(final long handlerId);

    /// Calling this method indicates that the end of block message is received for said block.
    /// @return a block action to be handled by the handler
    ActionForBlock endOfBlock(final long blockNumber);

    /// Return the latest known valid and persisted block number.
    ///
    /// Mostly called by handlers when returning `EndOfStream` to a publisher.
    /// @return the latest known valid and persisted block number.
    long getLatestBlockNumber();

    /// Notify the publisher manager that they are too far behind the latest block number.
    ///
    /// This is used to notify the system that they are too far behind the latest
    /// block number and should take appropriate action.
    ///
    /// @param newestKnownBlockNumber the newest known block number
    void notifyTooFarBehind(final long newestKnownBlockNumber);

    /// This method is called when a block is ending in an unfinished state.
    /// This means that the block, currently streamed by this handler is not yet
    /// streamed in full.
    ///
    /// @param blockNumber the block number that has not finished streaming
    /// @param handlerId the id of the handler that is ending the block
    void blockIsEnding(final long blockNumber, final long handlerId);

    /// Shut down the publisher manager and all of its handlers.
    void shutdown();

    /// Signal the data ready condition.
    ///
    /// This method is called to indicate that data \_might\_ be available to be
    /// sent to the messaging facility.
    ///
    /// The messaging thread may wait on this condition to limit spin cycles
    /// and still have a low impact on latency.
    void signalDataReady();

    /// The action to take within the PublisherHandler for a block.
    enum BlockAction {
        /// todo(1420) add documentation
        ACCEPT,
        /// todo(1420) add documentation
        SKIP,
        /// todo(1420) add documentation
        RESEND,
        /// todo(1420) add documentation
        SEND_BEHIND,
        /// todo(1420) add documentation
        END_DUPLICATE,
        /// todo(1420) add documentation
        END_ERROR // Something has gone wrong, stop this publisher and tell them to start a new connection.
    }

    /// A record that holds a [BlockAction] that needs to be done for a specified block.
    /// @param action to be taken
    /// @param blockNumber of the block to take the action upon
    record ActionForBlock(BlockAction action, long blockNumber) {}
}
