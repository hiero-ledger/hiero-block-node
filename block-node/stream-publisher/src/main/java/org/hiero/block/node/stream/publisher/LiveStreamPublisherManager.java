// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.stream.publisher;

import static java.lang.System.Logger.Level.TRACE;
import static java.util.concurrent.locks.LockSupport.parkNanos;
import static org.hiero.block.node.spi.BlockNodePlugin.METRICS_CATEGORY;
import static org.hiero.block.node.spi.BlockNodePlugin.UNKNOWN_BLOCK_NUMBER;

import com.hedera.hapi.block.stream.BlockProof;
import com.hedera.pbj.runtime.grpc.Pipeline;
import com.swirlds.metrics.api.Counter;
import com.swirlds.metrics.api.LongGauge;
import com.swirlds.metrics.api.Metrics;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.atomic.AtomicLong;
import org.hiero.block.api.PublishStreamResponse;
import org.hiero.block.internal.BlockItemSetUnparsed;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.BlockMessagingFacility;
import org.hiero.block.node.spi.blockmessaging.PersistedNotification;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import org.hiero.block.node.spi.threading.ThreadPoolManager;

/**
 * todo(1420) add documentation
 */
public final class LiveStreamPublisherManager implements StreamPublisherManager {
    private static final String QUEUE_ID_FORMAT = "Q%016d";
    // @todo(1413) utilize the logger
    private final System.Logger LOGGER = System.getLogger(getClass().getName());
    private final MetricsHolder metrics;
    private final BlockNodeContext serverContext;
    private final ThreadPoolManager threadManager;
    private final Map<Long, PublisherHandler> handlers;
    private final AtomicLong nextHandlerId;
    private final ConcurrentMap<String, BlockingQueue<BlockItemSetUnparsed>> transferQueueMap;
    private final ConcurrentMap<Long, BlockingQueue<BlockItemSetUnparsed>> queueByBlockMap;

    /** Future tracking the queue forwarder task.
     * <p>
     * This will run until it encounters an exception or reaches a reasonable
     * run time limit. When handlers encounter a block proof, a method is called
     * to check and restart the task if it is not yet running or has completed.
     * <p>
     * This is initially null so that the first accepted block will initiate
     * sending and each completed block provides a chance to restart the
     * process, if needed.
     */
    private Future<Long> queueForwarderResult = null;

    private final AtomicLong currentStreamingBlockNumber;
    private final AtomicLong nextUnstreamedBlockNumber;
    private final AtomicLong lastPersistedBlockNumber;

    /**
     * todo(1420) add documentation
     */
    public LiveStreamPublisherManager(
            @NonNull final BlockNodeContext context, @NonNull final MetricsHolder metricsHolder) {
        serverContext = Objects.requireNonNull(context);
        metrics = Objects.requireNonNull(metricsHolder);
        threadManager = serverContext.threadPoolManager();
        handlers = new ConcurrentSkipListMap<>();
        nextHandlerId = new AtomicLong(0);
        transferQueueMap = new ConcurrentSkipListMap<>();
        queueByBlockMap = new ConcurrentHashMap<>();
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
        var queueRemoved = transferQueueMap.remove(queueId);
        // It takes just as long to loop the map as to call `containsValue`.
        // so just loop through and remove the entry if it's found.
        for (final var nextEntry : queueByBlockMap.entrySet()) {
            if (nextEntry.getValue() == queueRemoved) {
                // Remove the entry from the Map
                queueByBlockMap.remove(nextEntry.getKey());
                break; // There will only be one entry with this queue.
            }
        }
        LOGGER.log(TRACE, "Removed handler {0} and its transfer queue {1}", handlerId, queueId);
        metrics.currentPublisherCount().set(handlers.size());
    }

    @Override
    public BlockAction getActionForBlock(
            final long blockNumber, final BlockAction previousAction, final long handlerId) {
        return switch (previousAction) {
            case null -> getActionForHeader(blockNumber, handlerId);
            case ACCEPT -> getActionForCurrentlyStreaming(blockNumber);
            case END_ERROR, END_DUPLICATE, END_BEHIND ->
                // This should not happen because the Handler should have shut down.
                BlockAction.END_ERROR;
            case SKIP, RESEND ->
                // This should not happen because the Handler should have reset the previous action.
                BlockAction.END_ERROR;
        };
    }

    /**
     * todo(1420) add documentation
     */
    private BlockAction getActionForHeader(final long blockNumber, final long handlerId) {
        if (blockNumber <= lastPersistedBlockNumber.get()) {
            return BlockAction.END_DUPLICATE;
        } else if (blockNumber > lastPersistedBlockNumber.get() && blockNumber < nextUnstreamedBlockNumber.get()) {
            // current streaming number will always be within the range tested here.
            return BlockAction.SKIP;
        } else if (blockNumber == nextUnstreamedBlockNumber.get()) {
            return addHandlerQueueForBlock(blockNumber, handlerId);
        } else if (blockNumber > nextUnstreamedBlockNumber.get()) {
            return BlockAction.END_BEHIND;
        } else {
            // This should not be possible, all cases that could reach here are
            // already handled above.
            return BlockAction.END_ERROR;
        }
    }

    /**
     * todo(1420) add documentation
     */
    private BlockAction addHandlerQueueForBlock(final long blockNumber, final long handlerId) {
        if (nextUnstreamedBlockNumber.compareAndSet(blockNumber, blockNumber + 1)) {
            final String handlerQueueName = getQueueNameForHandlerId(handlerId);
            // Exception, using var here for an expected null value to avoid excessive wrapping.
            final var previousValue = queueByBlockMap.put(blockNumber, transferQueueMap.get(handlerQueueName));
            if (previousValue != null) {
                // Another handler jumped in front of the calling handler.
                // Undo the change
                queueByBlockMap.put(blockNumber, previousValue);
            } else {
                // special case, we just started a new block, so make sure we
                // have a queue forwarder thread.
                if (queueForwarderResult == null) {
                    queueForwarderResult = launchQueueForwarder();
                }
                return BlockAction.ACCEPT;
            }
        }
        // Return the correct action if another handler jumped in front of the caller.
        return blockNumber < nextUnstreamedBlockNumber.get() ? BlockAction.SKIP : BlockAction.END_BEHIND;
    }

    /**
     * todo(1420) add documentation
     */
    private BlockAction getActionForCurrentlyStreaming(final long blockNumber) {
        if (blockNumber <= lastPersistedBlockNumber.get()) {
            return BlockAction.END_DUPLICATE;
        } else if (blockNumber > lastPersistedBlockNumber.get() && blockNumber < currentStreamingBlockNumber.get()) {
            // Somehow another handler snuck in and is streaming _ahead_ of us.
            // We'll have to skip the rest of this block.
            return BlockAction.SKIP;
        } else if (blockNumber >= currentStreamingBlockNumber.get() && blockNumber < nextUnstreamedBlockNumber.get()) {
            // We're one of the handlers currently streaming, keep going.
            return BlockAction.ACCEPT;
        } else if (blockNumber == nextUnstreamedBlockNumber.get()) {
            // We're checking for a handler that is currently streaming, why error here?
            // That is because next unstreamed is _after_ the block we're streaming.
            // A handler that's currently streaming should always have a block number
            // that is >= current streaming and < next unstreamed (the test above this one).
            return BlockAction.END_ERROR;
        } else if (blockNumber > nextUnstreamedBlockNumber.get()) {
            // Something weird happened, we were streaming this block, but now
            // the block node is behind. The most likely cause here is a block
            // that failed to verify, or got stuck and did not finish, and was
            // parallel streaming a block earlier than the calling handler.
            return BlockAction.END_BEHIND;
        } else {
            // This should not be possible, all cases that could reach here are
            // already handled above.
            return BlockAction.END_ERROR;
        }
    }

    // Note, we may call this method with `null` if the block proof
    // fails to parse. This _is not an error_ and we should still forward
    // the block to messaging and treat the block as completed, we just
    // won't do anything that requires parsing the block proof. It is
    // possible the parsing failed in the publisher but will still
    // succeed in the verification plugin.
    @Override
    public void closeBlock(final BlockProof blockEndProof, final long handlerId) {
        // check the queue forwarder result and start, or restart, if it is
        // null or completed, respectively.
        // For now, restart if null or completed, but @todo(1413) log exceptions
        if (queueForwarderResult == null || queueForwarderResult.isDone()) {
            queueForwarderResult = launchQueueForwarder();
        }
        // @todo(1416) complete tasks that do not require the block proof data here.
        if (blockEndProof == null) {
            // No point logging here, as the handler would have done that.
            // here we just update metrics.
        } else {
            // @todo(1413) Also log completed blocks metric and any other relevant
            //     actions. Also check if we have incomplete blocks lower than the
            //     block that completed, and possibly enter the resend process to
            //     have handlers go back and get the block that was too slow resent
            //     from a different publisher (don't forget to keep/track last
            //     completed block, and retain data in queue(s) for
            //     completed-but-not-forwarded blocks).
        }
    }

    /**
     * Launch the queue forwarder task.
     * <p>
     * This method is called when the first block is accepted, or when a block
     * proof is received and the forwarder task is not running.
     * <p>
     * The task will run until it encounters an exception or reaches a reasonable
     * run time limit. When handlers encounter a block proof, a method is called
     * to check and restart the task if it is not yet running or has completed.
     *
     * @return a Future representing pending completion of the task
     */
    private Future<Long> launchQueueForwarder() {
        return threadManager
                .getVirtualThreadExecutor()
                .submit(new MessagingForwarderTask(serverContext, this, queueByBlockMap));
    }

    @Override
    public long getLatestBlockNumber() {
        return lastPersistedBlockNumber.get();
    }

    @Override
    public void handleVerification(@NonNull final VerificationNotification notification) {
        // Need to check, but should only handle the "failed" case.
        // on success we should probably do nothing.
        // @todo(1422) implement
    }

    @Override
    public void handlePersisted(@NonNull final PersistedNotification notification) {
        // update the latest known verified and persisted block number
        // and signal all handlers to send acknowledgements
        // @todo(1417) is the below correct/sufficient? Is it ok to block here?
        //     Also, need to set persisted block number _first_ and
        //     need to check that new persisted is > last persisted
        //     before doing more.
        final long newLastPersistedBlock = notification.endBlockNumber();
        // @note it is _not_ OK to block here. What we need is a forkJoin task
        // that forks for each handler and sends acks for all in thread(s)
        // then joins at the end. That can be added next sprint, however.
        if (newLastPersistedBlock >= lastPersistedBlockNumber.get()) {
            for (final PublisherHandler handler : handlers.values()) {
                // _Important_, we only need the last persisted block number
                //     all previous blocks are implicitly acknowledged.
                handler.sendAcknowledgement(newLastPersistedBlock);
            }
        } // otherwise, just ignore this, we already acknowledged this block
        lastPersistedBlockNumber.set(newLastPersistedBlock);
    }

    /**
     * Register a new transfer queue for the given handler ID.
     * <p>
     * This method creates a new transfer queue and registers it in the
     * transferQueueMap. The queue is used to transfer block items from
     * the handler to the messaging facility.
     *
     * @param handlerId the ID of the handler for which to register the queue
     * @return a BlockingQueue for transferring BlockItemSetUnparsed items
     */
    private BlockingQueue<BlockItemSetUnparsed> registerTransferQueue(final long handlerId) {
        final String queueId = getQueueNameForHandlerId(handlerId);
        transferQueueMap.put(queueId, new LinkedTransferQueue<>());
        LOGGER.log(TRACE, "Registered new transfer queue: {0}", queueId);
        return transferQueueMap.get(queueId);
    }

    /**
     * todo(1420) add documentation
     */
    private static String getQueueNameForHandlerId(final long handlerId) {
        return QUEUE_ID_FORMAT.formatted(handlerId);
    }

    // Somewhere we were supposed to set the first block number supported by
    // the block node. I don't know what happened to that config, but it seems
    // to be missing. I asked the question on the backfill PR as it's also
    // relevant there. The current streaming should be the next block to be
    // streamed, but _only_ on startup. After that there should always be
    // a delta (next unstreamed must always be strictly greater than the current
    // streaming block number).
    private void updateBlockNumbers(final BlockNodeContext serverContext) {
        final long latestKnownBlock =
                serverContext.historicalBlockProvider().availableBlocks().max();
        // Always set the last persisted block number, even if there are no
        // known blocks.
        lastPersistedBlockNumber.set(latestKnownBlock);
        if (UNKNOWN_BLOCK_NUMBER == latestKnownBlock) {
            // if we have entered here, then we have no blocks available
            // @todo(1416) get below values from hiero config.
            currentStreamingBlockNumber.set(0L);
            nextUnstreamedBlockNumber.set(0L);
        } else {
            // if we have entered here, we know what the latest known block is,
            // so we can set the next unstreamed block number to one greater
            // pretend the next unstreamed block is streaming initially, that
            // ensures that the first block accepted is correctly handled.
            currentStreamingBlockNumber.set(latestKnownBlock + 1L);
            nextUnstreamedBlockNumber.set(latestKnownBlock + 1L);
        }
    }

    /**
     * todo(1420) add documentation
     */
    private static class MessagingForwarderTask implements Callable<Long> {
        private final BlockNodeContext serverContext;
        private final LiveStreamPublisherManager publisherManager;
        private final ConcurrentMap<Long, BlockingQueue<BlockItemSetUnparsed>> queueByBlockMap;
        private final BlockMessagingFacility messaging;
        private final PublisherConfig publisherConfiguration;

        /**
         * todo(1420) add documentation
         */
        public MessagingForwarderTask(
                final BlockNodeContext serverContext,
                final LiveStreamPublisherManager liveStreamPublisherManager,
                final ConcurrentMap<Long, BlockingQueue<BlockItemSetUnparsed>> queueByBlockMap) {
            this.serverContext = Objects.requireNonNull(serverContext);
            this.publisherManager = Objects.requireNonNull(liveStreamPublisherManager);
            this.queueByBlockMap = Objects.requireNonNull(queueByBlockMap);
            messaging = serverContext.blockMessaging();
            publisherConfiguration = serverContext.configuration().getConfigData(PublisherConfig.class);
        }

        // This needs more work, particularly handling the next block if it's already
        // queued up from a different handler, until we run out of queued up batches.
        /**
         * todo(1420) add documentation
         */
        @Override
        public Long call() {
            long batchesSent = 0L;
            boolean forwardingLimitReached = false;
            while (!forwardingLimitReached) {
                final long currentBlockNumber = publisherManager.currentStreamingBlockNumber.get();
                BlockingQueue<BlockItemSetUnparsed> queueToForward = queueByBlockMap.get(currentBlockNumber);
                if (queueToForward != null) {
                    List<BlockItemSetUnparsed> availableBatches = new LinkedList<>();
                    queueToForward.drainTo(availableBatches);
                    for (BlockItemSetUnparsed currentBatch : availableBatches) {
                        // send the batch to the messaging facility
                        messaging.sendBlockItems(new BlockItems(currentBatch.blockItems(), currentBlockNumber));
                        // forward the batches for the _current_ block first,
                        batchesSent++;
                        forwardingLimitReached = batchesSent >= publisherConfiguration.batchForwardLimit();
                        if (currentBatch.blockItems().getLast().hasBlockProof()) {
                            // If the last item in the batch is a block proof,
                            // then potentially increment the current streaming
                            // block number.
                            publisherManager.currentStreamingBlockNumber.compareAndSet(
                                    currentBlockNumber, currentBlockNumber + 1);
                        }
                    }
                    // If the current block number has no batches to send, then
                    // block on a count down latch until more data is available.
                    // @todo(1416) need to figure out how to reset and set the countdown
                    //     latch...  Until then, just park for 1/2 millisecond.
                    // Park for 500 microseconds if there is no data available,
                    // but not if the current block is completed (i.e. we just
                    // sent a block proof).
                    if (publisherManager.currentStreamingBlockNumber.get() == currentBlockNumber
                            && availableBatches.isEmpty()) {
                        parkNanos(500_000); // Park for 500 microseconds
                    }
                }
            }
            return batchesSent;
        }
    }

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
        /**
         * todo(1420) add documentation
         */
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
