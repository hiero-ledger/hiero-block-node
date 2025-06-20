// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.stream.publisher;

import static java.lang.System.Logger.Level.TRACE;
import static org.hiero.block.node.spi.BlockNodePlugin.METRICS_CATEGORY;

import com.hedera.pbj.runtime.grpc.Pipeline;
import com.swirlds.metrics.api.Counter;
import com.swirlds.metrics.api.LongGauge;
import com.swirlds.metrics.api.Metrics;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.atomic.AtomicLong;
import org.hiero.block.api.PublishStreamResponse;
import org.hiero.block.internal.BlockItemSetUnparsed;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.blockmessaging.PersistedNotification;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;

public final class LiveStreamPublisherManager implements StreamPublisherManager {
    private static final String QUEUE_ID_FORMAT = "Q%016d";
    private final System.Logger LOGGER = System.getLogger(getClass().getName());
    private final MetricsHolder metrics;
    private final BlockNodeContext serverContext;
    private final Map<Long, PublisherHandler> handlers;
    private final AtomicLong nextHandlerId;
    private final Map<String, BlockingQueue<BlockItemSetUnparsed>> transferQueueMap;

    private final AtomicLong currentStreamingBlockNumber;
    private final AtomicLong nextUnstreamedBlockNumber;
    private final AtomicLong lastPersistedBlockNumber;

    public LiveStreamPublisherManager(
            @NonNull final BlockNodeContext context, @NonNull final MetricsHolder metricsHolder) {
        serverContext = Objects.requireNonNull(context);
        metrics = Objects.requireNonNull(metricsHolder);
        handlers = new ConcurrentSkipListMap<>();
        nextHandlerId = new AtomicLong(0);
        transferQueueMap = new ConcurrentSkipListMap<>();
        currentStreamingBlockNumber = new AtomicLong(-1);
        nextUnstreamedBlockNumber = new AtomicLong(-1);
        lastPersistedBlockNumber = new AtomicLong(-1);
        updateBlockNumbers(serverContext);
    }

    @Override
    public PublisherHandler addHandler(
            @NonNull final Pipeline<? super PublishStreamResponse> replies,
            @NonNull final PublisherHandler.MetricsHolder handlerMetrics) {
        final long handlerId = nextHandlerId.getAndIncrement();
        final PublisherHandler newHandler =
                new PublisherHandler(handlerId, replies, handlerMetrics, this, registerTransferQueue(handlerId));
        handlers.put(handlerId, newHandler);
        metrics.currentPublisherCount().set(handlers.size());
        return newHandler;
    }

    @Override
    public void removeHandler(final long handlerId) {
        handlers.remove(handlerId);
        final String queueId = getQueueNameForHandlerId(handlerId);
        transferQueueMap.remove(queueId);
        LOGGER.log(TRACE, "Removed handler {0} and its transfer queue {1}", handlerId, queueId);
        metrics.currentPublisherCount().set(handlers.size());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BlockAction getActionForBlock(final long blockNumber, final BlockAction previousAction) {
        return switch (previousAction) {
            case null -> getActionForHeader(blockNumber, previousAction);
            case ACCEPT -> getActionForCurrentlyStreaming(blockNumber, previousAction);
            case END_ERROR, END_DUPLICATE, END_BEHIND -> BlockAction.END_ERROR; // This should not happen because the Handler should have shut down.
            case SKIP, RESEND -> BlockAction.END_ERROR; // This should not happen because the Handler should have reset the previous action.
        };
    }

    private BlockAction getActionForCurrentlyStreaming(final long blockNumber, final BlockAction previousAction) {
        return BlockAction.END_ERROR; // @todo return values based on the table below.
        /*
         Current State	<= last known	> next unstreamed	== next unstreamed	== current streaming	> last known && < current streaming		> current streaming && < next unstreamed
         accept			END_DUPLICATE	END_BEHIND			END_ERROR			ACCEPT					SKIP									ACCEPT
         */
    }

    private BlockAction getActionForHeader(final long blockNumber, final BlockAction previousAction) {
        return BlockAction.END_ERROR; // @todo return values based on the table below.
        /*
          Current State	<= last known	> next unstreamed	== next unstreamed	== current streaming	> last known && < current streaming		> current streaming && < next unstreamed
          null			END_DUPLICATE	END_BEHIND			ACCEPT				SKIP					SKIP									SKIP
         */
    }

    /**
     * Return the latest known valid and persisted block number.
     * <p>
     * Mostly called by handlers when returning `EndOfStream` to a publisher.
     * @return the latest known valid and persisted block number.
     */
    @Override
    public long getLatestBlockNumber() {
        return lastPersistedBlockNumber.get();
    }

    @Override
    public void handleVerification(@NonNull final VerificationNotification notification) {
        // Need to check, but should only handle the "failed" case.
        // on success we should probably do nothing.
    }

    @Override
    public void handlePersisted(@NonNull final PersistedNotification notification) {
        // update the latest known verified and persisted block number
        // and signal all handlers to send acknowledgements
    }

    private BlockingQueue<BlockItemSetUnparsed> registerTransferQueue(final long handlerId) {
        final String queueId = getQueueNameForHandlerId(handlerId);
        transferQueueMap.put(queueId, new LinkedTransferQueue<>());
        LOGGER.log(TRACE, "Registered new transfer queue: {0}", queueId);
        return transferQueueMap.get(queueId);
    }

    private static String getQueueNameForHandlerId(final long handlerId) {
        return QUEUE_ID_FORMAT.formatted(handlerId);
    }

    private void updateBlockNumbers(final BlockNodeContext serverContext) {
        // @todo update the three block number fields based on the block provider state, if available.
    }

    // Add methods for
    //   Determining if a particular block number should result
    //     in skip, resend, acknowledgement, stream, behind, duplicate, etc...
    //     Note, for each of these, there is the possibility that we will re-check
    //         the block provider state to find updates to the three block number fields.
    //   (speculative) have a thread that blocks on the transfer queue(s) so we don't
    //       need a "send now" method called by each handler for each update to
    //       the transfer queues (which is hard to keep thread-safe).
    //       The tricky part: The thread has to check _all_ of the transfer queues
    //       on each wake, so it might need a "combined" wait mechanism...
    //   Sending data from a transfer queue to the messaging facility
    //       This is triggered when a block proof is seen, and should send not only the
    //       current block, but also any blocks that were previously unstreamed but
    //       buffered in another handler's queue.
    //   Notifying handlers of persisted blocks.
    //   Notifying handlers of failed verification.

    /**
     * Metrics for tracking publisher handler activity:
     * lowestBlockNumber - Lowest incoming block number
     * currentBlockNumber - Current incoming block number
     * highestBlockNumber - Highest incoming block number
     * latestBlockNumberAcknowledged - The latest block number acknowledged
     */
    public record MetricsHolder(
            Counter blockItemsMessaged,
            LongGauge currentPublisherCount,
            LongGauge lowestBlockNumber,
            LongGauge currentBlockNumber,
            LongGauge highestBlockNumber,
            LongGauge latestBlockNumberAcknowledged) {
        static MetricsHolder createMetrics(@NonNull final Metrics metrics) {
            final Counter blockItemsMessaged =
                    metrics.getOrCreate(new Counter.Config(METRICS_CATEGORY, "publisher_block_items_messaged")
                            .withDescription("Live block items messaged to the messaging service"));
            final LongGauge numberOfProducers =
                    metrics.getOrCreate(new LongGauge.Config(METRICS_CATEGORY, "publisher_open_connections")
                            .withDescription("Connected publishers"));
            final LongGauge lowestBlockNumber =
                    metrics.getOrCreate(new LongGauge.Config(METRICS_CATEGORY, "publisher_lowest_block_number_inbound")
                            .withDescription("Oldest inbound block number"));
            final LongGauge currentBlockNumber =
                    metrics.getOrCreate(new LongGauge.Config(METRICS_CATEGORY, "publisher_current_block_number_inbound")
                            .withDescription("Current block number from handled publisher"));
            final LongGauge highestBlockNumber =
                    metrics.getOrCreate(new LongGauge.Config(METRICS_CATEGORY, "publisher_highest_block_number_inbound")
                            .withDescription("Newest inbound block number"));
            final LongGauge latestBlockNumberAcknowledged = metrics.getOrCreate(
                    new LongGauge.Config(METRICS_CATEGORY, "publisher_latest_block_number_acknowledged")
                            .withDescription("Latest block number acknowledged"));
            return new MetricsHolder(
                    blockItemsMessaged,
                    numberOfProducers,
                    lowestBlockNumber,
                    currentBlockNumber,
                    highestBlockNumber,
                    latestBlockNumberAcknowledged);
        }
    }
}
