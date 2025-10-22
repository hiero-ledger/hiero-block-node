// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.stream.publisher;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.TRACE;
import static java.lang.System.Logger.Level.WARNING;
import static org.hiero.block.node.spi.BlockNodePlugin.METRICS_CATEGORY;
import static org.hiero.block.node.spi.BlockNodePlugin.UNKNOWN_BLOCK_NUMBER;

import com.hedera.hapi.block.stream.BlockProof;
import com.hedera.pbj.runtime.grpc.Pipeline;
import com.swirlds.metrics.api.Counter;
import com.swirlds.metrics.api.LongGauge;
import com.swirlds.metrics.api.Metrics;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import org.hiero.block.api.PublishStreamResponse;
import org.hiero.block.internal.BlockItemSetUnparsed;
import org.hiero.block.node.app.config.node.NodeConfig;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.BlockMessagingFacility;
import org.hiero.block.node.spi.blockmessaging.NewestBlockKnownToNetworkNotification;
import org.hiero.block.node.spi.blockmessaging.PersistedNotification;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import org.hiero.block.node.spi.threading.ThreadPoolManager;

/**
 * todo(1420) add documentation
 */
public final class LiveStreamPublisherManager implements StreamPublisherManager {
    private static final String QUEUE_ID_FORMAT = "Q%016d";
    private static final int DATA_READY_WAIT_MICROSECONDS = 5000;
    private final System.Logger LOGGER = System.getLogger(LiveStreamPublisherManager.class.getName());
    private final MetricsHolder metrics;
    private final BlockNodeContext serverContext;
    private final ThreadPoolManager threadManager;
    private final Map<Long, PublisherHandler> handlers;
    private final AtomicLong nextHandlerId;
    private final ConcurrentMap<String, BlockingDeque<BlockItemSetUnparsed>> transferQueueMap;
    private final ConcurrentMap<Long, BlockingDeque<BlockItemSetUnparsed>> queueByBlockMap;
    private final Condition dataReadyLatch;
    private final ReentrantLock dataReadyLock;
    private final long earliestManagedBlock;

    /**
     * Future tracking the queue forwarder task.
     * <p>
     * This will run until it encounters an exception or reaches a reasonable
     * run time limit. When handlers encounter a block proof, a method is called
     * to check and restart the task if it is not yet running or has completed.
     * <p>
     * This is initially null so that the first accepted block will initiate
     * sending and each completed block provides a chance to restart the
     * process, if needed.
     */
    private final AtomicReference<Future<Long>> queueForwarderResult = new AtomicReference<>(null);

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
        dataReadyLock = new ReentrantLock();
        dataReadyLatch = dataReadyLock.newCondition();
        NodeConfig nodeConfiguration = serverContext.configuration().getConfigData(NodeConfig.class);
        earliestManagedBlock = nodeConfiguration.earliestManagedBlock();
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
        LOGGER.log(TRACE, "Added new handler {0}", handlerId);
        return newHandler;
    }

    @Override
    public void removeHandler(final long handlerId) {
        handlers.remove(handlerId);
        final String queueId = getQueueNameForHandlerId(handlerId);
        final BlockingDeque<BlockItemSetUnparsed> queueRemoved = transferQueueMap.remove(queueId);
        // Note: for queueByBlockMap, we need to remove an entry only if the
        // block contained in the queue is incomplete!
        // It takes just as long to loop the map as to call `containsValue`.
        // so just loop through and remove the entry if it's found.
        // @todo(1239): we need to redo the loop below. We could walk the
        //    queueByBlockMap in reverse order and only remove the first
        //    time we encounter an incomplete block that matches this handler.
        for (final Map.Entry<Long, BlockingDeque<BlockItemSetUnparsed>> nextEntry : queueByBlockMap.entrySet()) {
            final BlockingDeque<BlockItemSetUnparsed> queueByBlock = nextEntry.getValue();
            // Note: we are using ConcurrentMap which does not allow any null
            // values in order to unambiguously assert that a value is not
            // present when queried, as specified in the javadoc. This being
            // said, if queueByBlock is null, we need not take any action, as it
            // means the queue was never present in the first place.
            if (queueByBlock == queueRemoved) {
                // Remove the entry from the Map if the queue is incomplete.
                // We can afford to be naive for now and check if the last item
                // is not a proof, which means the queue is incomplete.
                if (queueByBlock != null) {
                    // We should remove the queue only if it holds an incomplete
                    // block, i.e. the last item in the queue is not a block
                    // proof, or the queue is empty (if there are no items,
                    // obviously there is no block supplied). Also, we can
                    // assert that block item sets will not be empty because
                    // that check is done when the request was received and if
                    // the item set was empty, that would have been an invalid
                    // request.
                    final BlockItemSetUnparsed last = queueByBlock.peekLast();
                    if (last == null || !last.blockItems().getLast().hasBlockProof()) {
                        queueByBlockMap.remove(nextEntry.getKey());
                        discardIncompleteTrailingBlock(queueByBlock);
                    }
                }
                break; // There will only be one entry with this queue.
            }
        }
        LOGGER.log(TRACE, "Removed handler {0} and its transfer queue {1}", handlerId, queueId);
        metrics.currentPublisherCount().set(handlers.size());
    }

    /**
     * Discard an incomplete trailing block from the given queue, if present.
     *
     * @param queue the queue to check and clean up.
     * @return true if and only if items were discarded.
     */
    private boolean discardIncompleteTrailingBlock(final BlockingDeque<BlockItemSetUnparsed> queue) {
        int itemsRemoved = 0;
        // use peek to non-destructively check the last item.
        BlockItemSetUnparsed last = queue.peekLast();
        // Remove items until we find a block proof or the queue is empty.
        while (last != null && !(last.blockItems().getLast().hasBlockProof())) {
            queue.removeLast();
            itemsRemoved++;
            last = queue.peekLast();
        }
        // @todo(1416) add an "items discarded" metric.
        return itemsRemoved > 0;
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

    @Override
    public void shutdown() {
        // Shut down all handlers and clear the queues.
        // Order is important here, we want to stop the handlers before
        // we stop the forwarding task and that must happen before we clear the
        // queue maps.
        for (final Long nextKey : handlers.keySet()) {
            final PublisherHandler value = handlers.remove(nextKey);
            if (value != null) {
                value.closeCommunication();
                value.onComplete();
            }
        }
        handlers.clear();
        // Cancel the queue forwarder task if it is running.
        final var lastResult = queueForwarderResult.getAndSet(null);
        if (lastResult != null) {
            lastResult.cancel(true);
        }
        transferQueueMap.clear();
        queueByBlockMap.clear();
    }

    /**
     * todo(1420) add documentation
     */
    private BlockAction getActionForHeader(final long blockNumber, final long handlerId) {
        if (blockNumber <= lastPersistedBlockNumber.get()) {
            return BlockAction.END_DUPLICATE;
        } else if (blockNumber > lastPersistedBlockNumber.get() && blockNumber < nextUnstreamedBlockNumber.get()) {
            return streamBeforeEMBOrElse(blockNumber, handlerId, BlockAction.SKIP);
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
    private BlockAction streamBeforeEMBOrElse(
            final long blockNumber, final long handlerId, final BlockAction elseAction) {
        // current streaming number will always be within the range tested here.
        // Except when we're awaiting the first block after restart and earliest
        // managed block is higher than what the publisher offered here.
        if (blockNumber < earliestManagedBlock
                && nextUnstreamedBlockNumber.get() == currentStreamingBlockNumber.get()
                // Handle an edge case where we need to accept a block before the earliest
                // managed block right after the node (re)started.
                && nextUnstreamedBlockNumber.compareAndSet(earliestManagedBlock, blockNumber)) {
            currentStreamingBlockNumber.set(blockNumber);
            metrics.lowestBlockNumber.set(blockNumber);
            return addHandlerQueueForBlock(blockNumber, handlerId);
        } else {
            return elseAction;
        }
    }

    /**
     * todo(1420) add documentation
     */
    private BlockAction addHandlerQueueForBlock(final long blockNumber, final long handlerId) {
        if (nextUnstreamedBlockNumber.compareAndSet(blockNumber, blockNumber + 1L)) {
            final String handlerQueueName = getQueueNameForHandlerId(handlerId);
            // Exception, using var here for an expected null value to avoid excessive wrapping.
            final BlockingDeque<BlockItemSetUnparsed> previousTransferQueueOfHandler =
                    transferQueueMap.get(handlerQueueName);
            if (previousTransferQueueOfHandler != null) {
                final var previousValue = queueByBlockMap.put(blockNumber, previousTransferQueueOfHandler);
                if (previousValue != null) {
                    // Another handler jumped in front of the calling handler.
                    // Undo the change
                    queueByBlockMap.put(blockNumber, previousValue);
                } else {
                    checkLogAndRestartForwarderTask();
                    // This should result in new data being available, so we
                    // count down the data ready latch.
                    signalDataReady();
                    return BlockAction.ACCEPT;
                }
            } else {
                // This should not happen, an active handler should always
                // have a transfer queue registered.
                LOGGER.log(WARNING, "No transfer queue found for handler %d".formatted(handlerId));
                return BlockAction.END_ERROR;
            }
        }
        // Return the correct action if another handler jumped in front of the caller.
        return blockNumber < nextUnstreamedBlockNumber.get() ? BlockAction.SKIP : BlockAction.END_BEHIND;
    }

    /*
     * Signal the data ready condition.
     * <p>
     * This method is called to indicate that data _might_ be available to be
     * sent to the messaging facility.<br/>
     * The messaging thread may wait on this condition to limit spin cycles
     * and still have a low impact on latency.
     */
    private void signalDataReady() {
        dataReadyLock.lock();
        try {
            dataReadyLatch.signal();
        } finally {
            dataReadyLock.unlock();
        }
    }

    /**
     * Wait for data to be ready.
     * <p>
     * This method will block until the data ready condition is signaled or
     * the timeout is reached.<br/>
     * This method is used (with {@link #signalDataReady()}) to limit spin
     * cycles and still have a low impact on latency.
     * <p>
     * When this method returns data _might_ be available to send to the
     * messaging facility, but it is not guaranteed.
     * <p>
     * Note
     * <blockquote>This method ignored interrupted exceptions as a specific
     * optimization to avoid unnecessarily ending a thread or causing failures
     * when interrupt is used as a signal rather than signaling the `Condition`
     * variable.</blockquote>
     */
    private void waitForDataReady() {
        dataReadyLock.lock();
        try {
            dataReadyLatch.await(DATA_READY_WAIT_MICROSECONDS, TimeUnit.MICROSECONDS);
        } catch (InterruptedException e) {
            // just ignore interruption in this specific case.
        } finally {
            dataReadyLock.unlock();
        }
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
            // This should result in new data being available, so we
            // count down the data ready latch.
            signalDataReady();
            metrics.highestBlockNumber.set(blockNumber);
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
        checkLogAndRestartForwarderTask();
        // @todo(1416) complete tasks that do not require the block proof data here (before this line).
        if (blockEndProof == null) {
            // No point logging here, as the handler would have done that.
            // here we just update metrics.
            metrics.blocksClosedIncomplete.increment();
        } else {
            metrics.blocksClosedComplete.increment();
            // @todo(1416) Also log completed blocks metric and any other relevant
            //     actions. Also check if we have incomplete blocks lower than the
            //     block that completed, and possibly enter the resend process to
            //     have handlers go back and get the block that was too slow resent
            //     from a different publisher (don't forget to keep/track last
            //     completed block, and retain data in queue(s) for
            //     completed-but-not-forwarded blocks).

            // @todo(1415) Remove this log when the related tickets are done.
            LOGGER.log(
                    DEBUG,
                    "Completed blocks: {0}, Incompleted blocks: {1}",
                    metrics.blocksClosedComplete.get(),
                    metrics.blocksClosedIncomplete.get());
        }
    }

    /// Check the queue forwarder task and start, or restart, if it is
    /// null or completed, respectively.
    private void checkLogAndRestartForwarderTask() {
        final Future<Long> currentResult = queueForwarderResult.get();
        if (currentResult != null && currentResult.isDone()) {
            try {
                final long blocksForwarded = currentResult.get();
                metrics.blockBatchesMessaged().add(blocksForwarded);
                final String formatString = "Queue forwarder task completed normally after forwarding %d blocks.";
                LOGGER.log(DEBUG, formatString.formatted(blocksForwarded));
            } catch (CancellationException | ExecutionException e) {
                LOGGER.log(INFO, "Queue forwarder task completed exceptionally.", e);
            } catch (InterruptedException e) {
                LOGGER.log(DEBUG, "Interrupted retrieving queue forwarder task result.");
            }
        }
        if (queueForwarderResult.get() == null) {
            queueForwarderResult.compareAndSet(null, launchQueueForwarder());
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
    public void notifyTooFarBehind(final long newestKnownBlockNumber) {
        // create a NewestBlockKnownToNetwork and sent it to the messaging facility
        final NewestBlockKnownToNetworkNotification notification =
                new NewestBlockKnownToNetworkNotification(newestKnownBlockNumber);
        // send the notification
        serverContext.blockMessaging().sendNewestBlockKnownToNetwork(notification);
    }

    @Override
    public long getLatestBlockNumber() {
        return lastPersistedBlockNumber.get();
    }

    @Override
    public void handlerIsEnding(final long blockNumber, final long handlerId) {
        // Preserve the initial state of current and next streaming in order to
        // ensure correct logging as these atomic longs can be asynchronously
        // updated. Any other query of these values needs to be made again!
        final long currentStreaming = currentStreamingBlockNumber.get();
        final long nextUnstreamed = nextUnstreamedBlockNumber.get();
        if (blockNumber >= currentStreaming && blockNumber < nextUnstreamed) {
            // decrement the next unstreamed value, but only if the block number
            // provided by the ending handler is the latest started block.
            nextUnstreamedBlockNumber.compareAndSet(blockNumber + 1, blockNumber);
            transferQueueMap.remove(getQueueNameForHandlerId(handlerId));
            // Also (potentially) remove this block from the queueByBlockMap.
            // and clear the queue if it is removed here.
            // Note, we know the last block must be incomplete _if_ it was started
            // because the handler would not have passed a valid block number in
            // here if it wasn't currently streaming a block.
            final BlockingDeque<BlockItemSetUnparsed> queue = queueByBlockMap.remove(blockNumber);
            if (queue != null) {
                discardIncompleteTrailingBlock(queue);
            }
        } else {
            // this should never happen
            final String message =
                    "Invalid state detected for handler %d when ending mid-block %d. Current Streaming Block Number: %d, Next Unstreamed Block Number: %d"
                            .formatted(handlerId, blockNumber, currentStreaming, nextUnstreamed);
            LOGGER.log(WARNING, message);
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * This method handles verification notifications from the block messaging
     * facility. It checks if the notification is from a publisher and if the
     * verification was unsuccessful. If so, it forks each handler and calls
     * the {@link PublisherHandler#handleFailedVerification(long)} on each.
     * After each invocation, it clears the transfer queue for that handler
     * if it exists.
     * <blockquote>
     * NOTE: The handler that was responsible for sending the block that failed
     * verification will proceed to send an {@link PublishStreamResponse.EndOfStream}
     * with {@link PublishStreamResponse.EndOfStream.Code#BAD_BLOCK_PROOF}. The
     * handler will then shut down. All other handlers will send
     * a {@link PublishStreamResponse.ResendBlock}, if the resend was
     * successfully sent, then the handlers will continue to operate normally.
     * Otherwise, the handlers will also shut down.
     * <br>
     * After we exit from the fork, we will clear all transfer queues.
     * Naturally, handlers that have shut down will not have the queue
     * available, i.e. it will be {@code null}.
     * </blockquote>
     * We will handle only if the verification has failed, notification block
     * number is greater than the {@link #lastPersistedBlockNumber}
     * (which has also passed verification naturally), and less than the
     * {@link #nextUnstreamedBlockNumber} Blocks will not be known unless they have
     * passed verification and have been persisted. There is always expected
     * to be a gap between the {@link #lastPersistedBlockNumber} and the
     * {@link #nextUnstreamedBlockNumber} because a block cannot start
     * verification until streamed in full, and the
     * {@link #nextUnstreamedBlockNumber} is incremented at the moment we start
     * * streaming the block.
     * <br>
     * In practice, if the {@link #lastPersistedBlockNumber} is 100, the
     * {@link #nextUnstreamedBlockNumber} will be at least 101. When we start
     * streaming block 101, the {@link #nextUnstreamedBlockNumber} will be
     * incremented to 102, and the {@link #lastPersistedBlockNumber} will be
     * 100. If the block 101 fails verification, it will be captured by
     * (block greater than 100 and block lower than 102).
     */
    @Override
    public void handleVerification(@NonNull final VerificationNotification notification) {
        final long blockNumber = notification.blockNumber();
        // Critical note: Only handle _failed_ verifications.
        // If we ever handle successful verifications, we must add conditions
        // to handle if the block is the first block received after a
        // restart.
        final boolean shouldHandle = !notification.success()
                && blockNumber > lastPersistedBlockNumber.get()
                && blockNumber < nextUnstreamedBlockNumber.get();
        if (shouldHandle) {
            nextUnstreamedBlockNumber.set(blockNumber);
            currentStreamingBlockNumber.set(blockNumber);
            // @todo(1514): We have an extremely rare race condition here.
            //    We need a condition wait to prevent one publisher from
            //    starting to resend before all handler have been notified.
            // We need to clear the queueByBlockMap because we are discarding
            // all blocks due to failed verification.
            queueByBlockMap.clear();
            handlers.values().parallelStream().unordered().forEach(handler -> {
                final long handlerId = handler.handleFailedVerification(blockNumber);
                final String qId = getQueueNameForHandlerId(handlerId);
                final BlockingQueue<BlockItemSetUnparsed> queue = transferQueueMap.get(qId);
                if (queue != null) {
                    // If a queue exists for the handler, we need to flush it
                    queue.clear();
                }
            });
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Here, we check the new persisted block number against the last persisted
     * block number, and only send acknowledgements if the new persisted block
     * number is greater than the last persisted block number. Otherwise, the
     * block is already acknowledged and we can ignore this notification.
     * <p>
     * This implementation uses fork/join processing to fork a task for each
     * publisher handler to send acknowledgements for their connected publishers
     * in parallel and then joins the tasks to ensure all are completed.
     * <p>
     * The messaging handler thread is not released until all acknowledgements
     * are sent.
     * <p>Note
     * <blockquote>It is OK to block here, but not overly long. What we
     * need is a fork/join process that forks for each handler and sends acks
     * for all in thread(s) then joins at the end. Fortunately the Streams API
     * offers a convenient parallelStream() method that does exactly that. We
     * further declare the set explicitly unordered to further limit
     * unnecessary ties between handlers. The overall time blocked should not
     * significantly exceed the time to send a single acknowledgement.
     * </blockquote>
     */
    @Override
    public void handlePersisted(@NonNull final PersistedNotification notification) {
        if (notification != null) {
            final long newLastPersistedBlock = notification.blockNumber();
            if (notification.succeeded()) {
                if (newLastPersistedBlock > lastPersistedBlockNumber.get()) {
                    handlers.values().parallelStream().unordered().forEach(handler -> {
                        // _Important_, we only need the last persisted block number
                        // all previous blocks are implicitly acknowledged.
                        handler.sendAcknowledgement(newLastPersistedBlock);
                    });
                    lastPersistedBlockNumber.set(newLastPersistedBlock);
                    metrics.latestBlockNumberAcknowledged.set(newLastPersistedBlock);
                }
            } else {
                queueByBlockMap.clear();
                final long blockNumber = notification.blockNumber();
                // @todo(1514): We have an extremely rare race condition here
                //    similar to the one in handleVerification.
                nextUnstreamedBlockNumber.set(blockNumber);
                currentStreamingBlockNumber.set(blockNumber);

                handlers.values().parallelStream().unordered().forEach(handler -> {
                    final long handlerId = handler.handleFailedPersistence();
                    final String qId = getQueueNameForHandlerId(handlerId);
                    final BlockingQueue<BlockItemSetUnparsed> queue = transferQueueMap.get(qId);
                    if (queue != null) {
                        // If a queue exists for the handler, we need to flush it
                        queue.clear();
                    }
                });
            }
        }
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
        transferQueueMap.put(queueId, new LinkedBlockingDeque<>());
        LOGGER.log(TRACE, "Registered new transfer queue: {0}", queueId);
        return transferQueueMap.get(queueId);
    }

    /**
     * todo(1420) add documentation
     */
    private static String getQueueNameForHandlerId(final long handlerId) {
        return QUEUE_ID_FORMAT.formatted(handlerId);
    }

    // The current streaming should be the next block to be
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
            // if we have entered here, then we have no blocks available.
            // treat anything up to the earliest managed block as the "next"
            // block.
            currentStreamingBlockNumber.set(earliestManagedBlock);
            nextUnstreamedBlockNumber.set(earliestManagedBlock);
        } else if (latestKnownBlock < earliestManagedBlock) {
            // We haven't caught up to the earliest block the operator wants
            // to treat as the start of history, so accept anything up to that
            // block as the "next" block.
            currentStreamingBlockNumber.set(earliestManagedBlock);
            nextUnstreamedBlockNumber.set(earliestManagedBlock);
        } else {
            // if we have entered here, we know what the latest known block is,
            // and we must prevent gaps after that block, so we can set the
            // next unstreamed block number to one greater pretend the next
            // unstreamed block is streaming initially, that ensures that the
            // first block accepted is correctly handled.
            currentStreamingBlockNumber.set(latestKnownBlock + 1L);
            nextUnstreamedBlockNumber.set(latestKnownBlock + 1L);
        }
    }

    /**
     * todo(1420) add documentation
     */
    private static class MessagingForwarderTask implements Callable<Long> {

        private final System.Logger LOGGER = System.getLogger(MessagingForwarderTask.class.getName());
        private final BlockNodeContext serverContext;
        private final LiveStreamPublisherManager publisherManager;
        private final ConcurrentMap<Long, BlockingDeque<BlockItemSetUnparsed>> queueByBlockMap;
        private final BlockMessagingFacility messaging;
        private final PublisherConfig publisherConfiguration;

        /**
         * todo(1420) add documentation
         */
        public MessagingForwarderTask(
                final BlockNodeContext serverContext,
                final LiveStreamPublisherManager liveStreamPublisherManager,
                final ConcurrentMap<Long, BlockingDeque<BlockItemSetUnparsed>> queueByBlockMap) {
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
            // End this thread after some number of batches, so we don't have
            // a thread that becomes overly "old" and experiences "senescence".
            while (!forwardingLimitReached) {
                final long currentBlockNumber = publisherManager.currentStreamingBlockNumber.get();
                final BlockingQueue<BlockItemSetUnparsed> queueToForward = queueByBlockMap.get(currentBlockNumber);
                if (queueToForward != null) {
                    boolean moreToSend = queueToForward.peek() != null;
                    while (moreToSend) {
                        // We MUST remove each batch from the queue before sending, otherwise we end
                        // up sending the same block over and over, forever, while the queue grows
                        // without bounds.  Use `poll` not `remove` because `remove` throws a
                        // runtime exception if the queue is empty.
                        final BlockItemSetUnparsed currentBatch = queueToForward.poll();
                        if (currentBatch != null) {
                            // log the batch being forwarded to messaging facility from publisher plugin
                            LOGGER.log(
                                    TRACE,
                                    "Forwarding batch for block {0} with {1} items",
                                    currentBlockNumber,
                                    currentBatch.blockItems().size());
                            // send the batch to the messaging facility
                            messaging.sendBlockItems(new BlockItems(currentBatch.blockItems(), currentBlockNumber));
                            // limit how many batches we send in a single task.
                            batchesSent++;
                            forwardingLimitReached = batchesSent >= publisherConfiguration.batchForwardLimit();
                            // If we reach end of block, update counters and stop sending this block.
                            if (currentBatch.blockItems().getLast().hasBlockProof()) {
                                // If the last item in the batch is a block proof,
                                // we need to remove the queue from the queueByBlockMap.
                                queueByBlockMap.remove(currentBlockNumber);
                                // Then potentially increment the current streaming
                                // block number.
                                publisherManager.currentStreamingBlockNumber.compareAndSet(
                                        currentBlockNumber, currentBlockNumber + 1);
                                // exit this while loop so we do not accidentally
                                // send a block out of order.
                                moreToSend = false;
                            }
                        } else {
                            moreToSend = false;
                        }
                    }
                    // If the current block number has no more batches to send,
                    // then block on a condition variable until more data is
                    // _probably_ available, or until a timeout elapses.
                    if (publisherManager.currentStreamingBlockNumber.get() == currentBlockNumber
                            && queueToForward.isEmpty()) {
                        publisherManager.waitForDataReady();
                    }
                } else {
                    // We have no queue for this block, so wait for an
                    // indication that more data might be available.
                    publisherManager.waitForDataReady();
                }
            }
            return batchesSent;
        }
    }

    /**
     * Metrics for tracking publisher handler activity:
     * blockItemsMessaged - Number of block items delivered to the messaging service
     * currentPublisherCount - Number of currently connected publishers
     * lowestBlockNumber - Lowest incoming block number
     * highestBlockNumber - Highest incoming block number
     * latestBlockNumberAcknowledged - The latest block number acknowledged
     * blocksClosedComplete - Number of blocks received complete (with both header and proof)
     * blocksClosedIncomplete - Number of blocks received incomplete (missing header or proof)
     */
    public record MetricsHolder(
            Counter blockItemsMessaged,
            Counter blockBatchesMessaged,
            LongGauge currentPublisherCount,
            LongGauge lowestBlockNumber,
            LongGauge highestBlockNumber,
            LongGauge latestBlockNumberAcknowledged,
            Counter blocksClosedComplete,
            Counter blocksClosedIncomplete) {
        /**
         * todo(1420) add documentation
         */
        static MetricsHolder createMetrics(@NonNull final Metrics metrics) {
            final Counter blockItemsMessaged =
                    metrics.getOrCreate(new Counter.Config(METRICS_CATEGORY, "publisher_block_items_messaged")
                            .withDescription("Live block items messaged to the messaging service"));
            final Counter blockBatchesMessaged =
                    metrics.getOrCreate(new Counter.Config(METRICS_CATEGORY, "publisher_block_batches_messaged")
                            .withDescription("Live block batches processed and sent to the messaging service"));
            final Counter blocksClosedComplete =
                    metrics.getOrCreate(new Counter.Config(METRICS_CATEGORY, "publisher_blocks_closed_complete")
                            .withDescription("Blocks received complete (with both header and proof) by any Handler"));
            final Counter blocksClosedIncomplete =
                    metrics.getOrCreate(new Counter.Config(METRICS_CATEGORY, "publisher_blocks_closed_incomplete")
                            .withDescription("Blocks received incomplete (missing header or proof) by any Handler"));
            final LongGauge numberOfProducers =
                    metrics.getOrCreate(new LongGauge.Config(METRICS_CATEGORY, "publisher_open_connections")
                            .withDescription("Connected publishers"));
            final LongGauge lowestBlockNumber =
                    metrics.getOrCreate(new LongGauge.Config(METRICS_CATEGORY, "publisher_lowest_block_number_inbound")
                            .withDescription("Oldest inbound block number"));
            final LongGauge highestBlockNumber =
                    metrics.getOrCreate(new LongGauge.Config(METRICS_CATEGORY, "publisher_highest_block_number_inbound")
                            .withDescription("Newest inbound block number"));
            final LongGauge latestBlockNumberAcknowledged = metrics.getOrCreate(
                    new LongGauge.Config(METRICS_CATEGORY, "publisher_latest_block_number_acknowledged")
                            .withDescription("Latest block number acknowledged"));
            return new MetricsHolder(
                    blockItemsMessaged,
                    blockBatchesMessaged,
                    numberOfProducers,
                    lowestBlockNumber,
                    highestBlockNumber,
                    latestBlockNumberAcknowledged,
                    blocksClosedComplete,
                    blocksClosedIncomplete);
        }
    }
}
