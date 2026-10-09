// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.stream.publisher;

import com.hedera.pbj.runtime.grpc.Pipeline;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.Deque;
import java.util.concurrent.ScheduledFuture;
import org.hiero.block.api.PublishStreamResponse;
import org.hiero.block.internal.BlockItemSetUnparsed;
import org.hiero.block.node.app.config.ServerConfig;
import org.hiero.block.node.spi.blockmessaging.BlockNotificationHandler;

/// todo(1420) add documentation
public interface StreamPublisherManager extends BlockNotificationHandler {
    /// todo(1420) add documentation
    PublisherHandler addHandler(
            @NonNull final Pipeline<? super PublishStreamResponse> replies,
            @NonNull final PublisherHandler.MetricsHolder handlerMetrics,
            final String correlationId);

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

    /// Schedule a task to run with a fixed delay.
    ///
    /// @param taskToSchedule a runnable task to schedule to run after a short delay
    /// @param delayInNanoseconds The length of delay, this should always be less than 1 second.
    ///
    /// @return a ScheduledFuture representing the scheduled task.
    ScheduledFuture<?> scheduleAfterDelay(Runnable taskToSchedule, final long delayInNanoseconds);

    /// Get the current configuration of the publisher plugin.
    /// @return The plugin configuration data.
    PublisherConfig configuration();

    /// Get the current configuration of the server.
    /// @return The server configuration data.
    ServerConfig serverConfiguration();

    /// Return the block root hash recorded for a previously acknowledged block, or
    /// [Bytes#EMPTY] when no hash is available (either not acknowledged yet, or
    /// evicted from the bounded cache). Populated best-effort by the manager when
    /// it receives a successful verification notification.
    ///
    /// @param blockNumber the block number to look up
    /// @return the recorded root hash, or [Bytes#EMPTY] when unknown
    default Bytes getCachedBlockRootHash(final long blockNumber) {
        return Bytes.EMPTY;
    }

    /// Return the latest block number that has been acknowledged (i.e. verified and
    /// persisted) by this Block-Node. Used by the publisher handler to decide whether
    /// to immediately acknowledge an [org.hiero.block.api.PublishStreamRequest.AcknowledgeOnly]
    /// request.
    ///
    /// @return the latest acknowledged block number, or a value less than zero when
    ///     nothing has been acknowledged yet
    default long getLatestAckedBlockNumber() {
        return getLatestBlockNumber();
    }

    /// Record that a passive handler reported progress at the given block number via
    /// [org.hiero.block.api.PublishStreamRequest.AcknowledgeOnly]. Used by the manager
    /// to feed stall detection and connected-publisher progress tracking.
    ///
    /// @param handlerId the id of the handler that sent the AcknowledgeOnly request
    /// @param blockNumber the block number the handler claims to be at or past
    default void recordPassiveHandlerAck(final long handlerId, final long blockNumber) {
        // default: no-op
    }

    /// Notify the manager that a handler has transitioned between active and passive mode.
    /// Used to maintain per-mode gauges and other mode-aware bookkeeping.
    ///
    /// @param handlerId the id of the handler
    /// @param passive `true` if the handler is now passive, `false` if active
    default void notifyHandlerModeChange(final long handlerId, final boolean passive) {
        // default: no-op
    }

    /// The action to take within the PublisherHandler for a block.
    enum BlockAction {
        /// todo(1420) add documentation
        ACCEPT,
        /// todo(1420) add documentation
        SKIP,
        /// Send a SKIP followed by an ACKNOWLEDGE for a block that is already
        /// persisted, but recent enough to be within the "soft duplicate" window.
        SKIP_AND_ACK,
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
