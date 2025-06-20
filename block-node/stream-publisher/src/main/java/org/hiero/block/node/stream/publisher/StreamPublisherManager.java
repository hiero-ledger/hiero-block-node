package org.hiero.block.node.stream.publisher;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.TRACE;
import static org.hiero.block.node.spi.BlockNodePlugin.METRICS_CATEGORY;

import com.hedera.pbj.runtime.grpc.Pipeline;
import com.swirlds.metrics.api.Counter;
import com.swirlds.metrics.api.LongGauge;
import com.swirlds.metrics.api.Metrics;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TransferQueue;
import java.util.concurrent.atomic.AtomicLong;
import org.hiero.block.api.PublishStreamResponse;
import org.hiero.block.internal.BlockItemSetUnparsed;
import org.hiero.block.node.spi.BlockNodeContext;

public class StreamPublisherManager {
    private static final String QUEUE_ID_FORMAT = "Q%016d";
    private final System.Logger LOGGER = System.getLogger(getClass().getName());
    private final MetricsHolder metrics;
    private final BlockNodeContext serverContext;
    private final Map<Long, PublisherHandler> handlers;
    private final AtomicLong nextHandlerId;
    private final Map<String, TransferQueue<BlockItemSetUnparsed>> transferQueueMap;

    private final AtomicLong currentStreamingBlockNumber;
    private final AtomicLong nextUnstreamedBlockNumber;
    private final AtomicLong lastPersistedBlockNumber;

    public StreamPublisherManager(final BlockNodeContext context, MetricsHolder metricsHolder) {
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

    public PublisherHandler addHandler(final Pipeline<? super PublishStreamResponse> replies,
            final PublisherHandler.MetricsHolder handlerMetrics) {
        long handlerId = nextHandlerId.getAndIncrement();
        final PublisherHandler newHandler =
                new PublisherHandler(handlerId, replies, handlerMetrics, this, registerTransferQueue(handlerId));
        handlers.put(handlerId, newHandler);
        metrics.currentPublisherCount().set(handlers.size());
        return newHandler;
    }

    public void removeHandler(final long handlerId) {
        handlers.remove(handlerId);
        final String queueId = getQueueNameForHandlerId(handlerId);
        transferQueueMap.remove(queueId);
        LOGGER.log(TRACE, "Removed handler {0} and its transfer queue {1}", handlerId, queueId);
        metrics.currentPublisherCount().set(handlers.size());
    }

    private TransferQueue<BlockItemSetUnparsed> registerTransferQueue(final long handlerId) {
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
            LongGauge latestBlockNumberAcknowledged
    ) {
        static MetricsHolder createMetrics(Metrics metrics) {
            Counter blockItemsMessaged =
                    metrics.getOrCreate(new Counter.Config(METRICS_CATEGORY, "publisher_block_items_messaged")
                            .withDescription("Live block items messaged to the messaging service"));
            LongGauge numberOfProducers = metrics.getOrCreate(new LongGauge.Config(METRICS_CATEGORY, "publisher_open_connections")
                    .withDescription("Connected publishers"));
            LongGauge lowestBlockNumber = metrics.getOrCreate(
                    new LongGauge.Config(METRICS_CATEGORY, "publisher_lowest_block_number_inbound")
                            .withDescription("Oldest inbound block number"));
            LongGauge currentBlockNumber = metrics.getOrCreate(
                    new LongGauge.Config(METRICS_CATEGORY, "publisher_current_block_number_inbound")
                            .withDescription("Current block number from handled publisher"));
            LongGauge highestBlockNumber = metrics.getOrCreate(
                    new LongGauge.Config(METRICS_CATEGORY, "publisher_highest_block_number_inbound")
                            .withDescription("Newest inbound block number"));
            LongGauge latestBlockNumberAcknowledged = metrics.getOrCreate(
                    new LongGauge.Config(METRICS_CATEGORY, "publisher_latest_block_number_acknowledged")
                            .withDescription("Latest block number acknowledged"));
            return new MetricsHolder(
                    blockItemsMessaged,
                    numberOfProducers,
                    lowestBlockNumber,
                    currentBlockNumber,
                    highestBlockNumber,
                    latestBlockNumberAcknowledged
            );
        }
    }
}
