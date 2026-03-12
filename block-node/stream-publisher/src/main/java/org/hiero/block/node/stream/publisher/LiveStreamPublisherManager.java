// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.stream.publisher;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.TRACE;
import static org.hiero.block.node.spi.BlockNodePlugin.METRICS_CATEGORY;
import static org.hiero.block.node.spi.BlockNodePlugin.UNKNOWN_BLOCK_NUMBER;

import com.hedera.pbj.runtime.grpc.Pipeline;
import com.swirlds.metrics.api.Counter;
import com.swirlds.metrics.api.IntegerGauge;
import com.swirlds.metrics.api.LongGauge;
import com.swirlds.metrics.api.Metrics;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import org.hiero.block.api.PublishStreamResponse;
import org.hiero.block.internal.BlockItemSetUnparsed;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.node.app.config.node.NodeConfig;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.BlockMessagingFacility;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.NewestBlockKnownToNetworkNotification;
import org.hiero.block.node.spi.blockmessaging.PersistedNotification;
import org.hiero.block.node.spi.blockmessaging.PublisherStatusUpdateNotification;
import org.hiero.block.node.spi.blockmessaging.PublisherStatusUpdateNotification.UpdateType;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import org.hiero.block.node.spi.threading.ThreadPoolManager;

/// todo(1420) add documentation
public final class LiveStreamPublisherManager implements StreamPublisherManager {
    private static final int DATA_READY_WAIT_MICROSECONDS = 5000;
    private final System.Logger LOGGER = System.getLogger(LiveStreamPublisherManager.class.getName());
    private final MetricsHolder metrics;
    private final BlockNodeContext serverContext;
    private final PublisherConfig publisherConfig;
    private final ThreadPoolManager threadManager;
    private final ConcurrentNavigableMap<Long, PublisherHandler> handlers;
    private final AtomicLong nextHandlerId;
    private final ConcurrentNavigableMap<Long, Deque<BlockItemSetUnparsed>> queueByBlockMap;
    private final Condition dataReadyLatch;
    private final ReentrantLock dataReadyLock;
    private final long earliestManagedBlock;
    private final ScheduledExecutorService scheduledExecutor;
    private volatile ScheduledFuture<Boolean> publisherUnavailabilityTimeoutFuture;

    /// Future tracking the queue forwarder task.
    ///
    /// This will run until it encounters an exception or reaches a reasonable
    /// run time limit. When handlers encounter a block proof, a method is called
    /// to check and restart the task if it is not yet running or has completed.
    ///
    /// This is initially null so that the first accepted block will initiate
    /// sending and each completed block provides a chance to restart the
    /// process, if needed.
    private final AtomicReference<Future<Long>> queueForwarderResult = new AtomicReference<>(null);

    private final AtomicLong currentStreamingBlockNumber;
    private final AtomicLong lastStreamedBlockNumber;
    private final AtomicLong nextUnstreamedBlockNumber;
    private final AtomicLong lastPersistedBlockNumber;

    private final ConcurrentNavigableMap<Long, BlockItemUnparsed> blockProofs;
    private final NavigableSet<Long> endBlocksReceived;
    private final NavigableSet<Long> blocksToResend;

    /// todo(1420) add documentation
    public LiveStreamPublisherManager(
            @NonNull final BlockNodeContext context, @NonNull final MetricsHolder metricsHolder) {
        serverContext = Objects.requireNonNull(context);
        metrics = Objects.requireNonNull(metricsHolder);
        threadManager = serverContext.threadPoolManager();
        handlers = new ConcurrentSkipListMap<>();
        nextHandlerId = new AtomicLong(0);
        queueByBlockMap = new ConcurrentSkipListMap<>();
        currentStreamingBlockNumber = new AtomicLong(-1);
        lastStreamedBlockNumber = new AtomicLong(-1);
        nextUnstreamedBlockNumber = new AtomicLong(-1);
        lastPersistedBlockNumber = new AtomicLong(-1);
        dataReadyLock = new ReentrantLock();
        dataReadyLatch = dataReadyLock.newCondition();
        NodeConfig nodeConfiguration = serverContext.configuration().getConfigData(NodeConfig.class);
        earliestManagedBlock = nodeConfiguration.earliestManagedBlock();
        scheduledExecutor = threadManager.createVirtualThreadScheduledExecutor(
                1,
                null,
                (t, e) -> LOGGER.log(
                        INFO, "Scheduled task thread exception occurred in Live Stream Publisher Manager", e));
        publisherConfig = serverContext.configuration().getConfigData(PublisherConfig.class);
        publisherUnavailabilityTimeoutFuture = schedulePublisherUnavailabilityTimeout();
        blockProofs = new ConcurrentSkipListMap<>();
        endBlocksReceived = new ConcurrentSkipListSet<>();
        blocksToResend = new ConcurrentSkipListSet<>();
        updateBlockNumbers(serverContext);
    }

    @Override
    public PublisherHandler addHandler(
            @NonNull final Pipeline<? super PublishStreamResponse> replies,
            @NonNull final PublisherHandler.MetricsHolder handlerMetrics) {
        final long handlerId = nextHandlerId.getAndIncrement();
        final PublisherHandler newHandler = new PublisherHandler(handlerId, replies, handlerMetrics, this);
        // If there is an active unavailability timeout task, cancel it
        // because we now have a new publisher.
        // The cancel of the existing future must happen immediately prior to updating the active handlers map!
        cancelExistingFuture();
        handlers.put(handlerId, newHandler);
        // Now we can safely update the metrics and send the notification
        // for the new publisher.
        metrics.currentPublisherCount().set(handlers.size());
        sendPublisherStatusUpdate(UpdateType.PUBLISHER_CONNECTED, handlers);
        LOGGER.log(TRACE, "Added new handler {0}", handlerId);
        return newHandler;
    }

    @Override
    public void removeHandler(final long handlerId) {
        final PublisherHandler handlerRemoved = handlers.remove(handlerId);
        // If there are no more active publishers, schedule the
        // unavailability timeout task.
        if (handlerRemoved != null && handlers.isEmpty()) {
            publisherUnavailabilityTimeoutFuture = schedulePublisherUnavailabilityTimeout();
        }
        // Update metrics and publish the status update
        metrics.currentPublisherCount().set(handlers.size());
        sendPublisherStatusUpdate(UpdateType.PUBLISHER_DISCONNECTED, handlers);
        LOGGER.log(TRACE, "Removed handler {0}", handlerId);
    }

    @Override
    public BlockAction getActionForBlock(
            final long blockNumber, final BlockAction previousAction, final long handlerId) {
        return switch (previousAction) {
            case null -> getActionForHeader(blockNumber);
            case ACCEPT -> getActionForCurrentlyStreaming(blockNumber);
            case END_ERROR, END_DUPLICATE ->
                // This should not happen because the Handler should have shut down.
                BlockAction.END_ERROR;
            case SKIP, RESEND, SEND_BEHIND ->
                // This should not happen because the Handler should have reset the previous action.
                BlockAction.END_ERROR;
        };
    }

    @Override
    public void registerQueueForBlock(
            final long handlerId, final Deque<BlockItemSetUnparsed> queue, final long blockNumber) {
        final Deque<BlockItemSetUnparsed> previousValue = queueByBlockMap.put(blockNumber, queue);
        if (previousValue != null) {
            // This is not expected to happen
            final String message = "Handler {0} registered a queue for block {1}, but it was already registered";
            LOGGER.log(INFO, message, handlerId, blockNumber);
        }
    }

    @Override
    public long getLatestBlockNumber() {
        return lastPersistedBlockNumber.get();
    }

    @Override
    public ActionForBlock endOfBlock(final long blockNumber) {
        endBlocksReceived.add(blockNumber);
        final long blockToResend = nextBlockToResend();
        if (blockToResend != UNKNOWN_BLOCK_NUMBER) {
            return new ActionForBlock(BlockAction.RESEND, blockToResend);
        } else {
            return new ActionForBlock(BlockAction.ACCEPT, blockNumber);
        }
    }

    @Override
    public void blockIsEnding(final long blockNumber, final long handlerId) {
        // @todo(2344) we can further improve this
        LOGGER.log(INFO, "Handler {0} is ending mid-block {1}", handlerId, blockNumber);
        final Deque<BlockItemSetUnparsed> deque = queueByBlockMap.remove(blockNumber);
        if (deque != null) {
            if (blockNumber > lastPersistedBlockNumber.get() && blockNumber < nextUnstreamedBlockNumber.get()) {
                final String message = "Block {0} will be resent due to handler {1} ending mid block";
                LOGGER.log(DEBUG, message, blockNumber, handlerId);
                blocksToResend.add(blockNumber);
            }
        }
    }

    @Override
    public void closeBlock(final long handlerId) {
        checkLogAndRestartForwarderTask();
        metrics.blocksClosedComplete.increment();
        // @todo(1415) Remove this log when the related tickets are done.
        LOGGER.log(DEBUG, "Completed blocks {0}", metrics.blocksClosedComplete.get());
    }

    @Override
    public void notifyTooFarBehind(final long newestKnownBlockNumber) {
        // create a NewestBlockKnownToNetwork and sent it to the messaging facility
        final NewestBlockKnownToNetworkNotification notification =
                new NewestBlockKnownToNetworkNotification(newestKnownBlockNumber);
        // send the notification
        serverContext.blockMessaging().sendNewestBlockKnownToNetwork(notification);
    }

    /// {@inheritDoc}
    ///
    /// This method handles verification notifications from the block messaging
    /// facility. Only in cases of failed verification, given that the block
    /// number of the failed block satisfies the conditions of it being
    /// higher than the last persisted and lower than the next unstreamed block,
    /// we will attempt to handle that failure. That includes allowing
    /// individual handlers to handle that block, but also scheduling the failed
    /// block to be resent.
    @Override
    public void handleVerification(@NonNull final VerificationNotification notification) {
        final long blockNumber = notification.blockNumber();
        // Critical note: Only handle _failed_ verifications.
        // If we ever handle successful verifications, we must add conditions
        // to handle if the block is the first block received after a
        // restart.
        final boolean shouldHandle = !notification.success()
                && notification.source() == BlockSource.PUBLISHER
                && blockNumber > lastPersistedBlockNumber.get()
                && blockNumber < nextUnstreamedBlockNumber.get();
        if (shouldHandle) {
            // Iterate over all handlers and attempt to send the
            // bad block proof message.
            // @todo(2339) improve the loop below, find the handler that should send the bad block proof code
            //    and then let it handle the failed verification.
            for (final PublisherHandler handler : handlers.values()) {
                if (handler.handleFailedVerification(blockNumber)) {
                    // There will always be only one handler that will send the
                    // bad block proof message. Once we have, we can break.
                    break;
                }
            }
            // Now finally schedule the block that failed verification to be
            // resent.
            blocksToResend.add(blockNumber);
        }
    }

    /// {@inheritDoc}
    ///
    /// Here, we check the new persisted block number against the last persisted
    /// block number, and only send acknowledgements if the new persisted block
    /// number is greater than the last persisted block number. Otherwise, the
    /// block is already acknowledged and we can ignore this notification.
    ///
    /// This implementation uses fork/join processing to fork a task for each
    /// publisher handler to send acknowledgements for their connected publishers
    /// in parallel and then joins the tasks to ensure all are completed.
    ///
    /// The messaging handler thread is not released until all acknowledgements
    /// are sent.
    ///
    /// Note
    /// > It is OK to block here, but not overly long. What we
    /// need is a fork/join process that forks for each handler and sends acks
    /// for all in thread(s) then joins at the end. Fortunately the Streams API
    /// offers a convenient parallelStream() method that does exactly that. We
    /// further declare the set explicitly unordered to further limit
    /// unnecessary ties between handlers. The overall time blocked should not
    /// significantly exceed the time to send a single acknowledgement.
    @Override
    public void handlePersisted(@NonNull final PersistedNotification notification) {
        if (notification != null) {
            final long newLastPersistedBlock = notification.blockNumber();
            if (notification.succeeded()) {
                // If we are currently streaming any blocks < newLastPersistedBlock, then do not send acknowledgement
                if (newLastPersistedBlock > lastPersistedBlockNumber.get()
                        && isBeforeEarliestActiveBlock(newLastPersistedBlock)) {
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
                // @todo(2198): We have an extremely rare race condition here
                nextUnstreamedBlockNumber.set(blockNumber);
                currentStreamingBlockNumber.set(blockNumber);
                handlers.values().parallelStream().unordered().forEach(PublisherHandler::handleFailedPersistence);
            }
        }
    }

    /// Determine if the given block number is before the earliest active block.
    /// @param blockNumber to check
    /// @return `true` if, and only if, there are no blocks currently streaming or `blockNumber` is strictly less
    ///     than the earliest block currently waiting to be sent to messaging
    private boolean isBeforeEarliestActiveBlock(final long blockNumber) {
        try {
            return queueByBlockMap.isEmpty() || blockNumber < queueByBlockMap.firstKey();
        } catch (final NoSuchElementException e) {
            return true;
        }
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
        queueByBlockMap.clear();
        // We can safely shut down the scheduled executor abruptly. The timeout
        // tasks can be ignored completely as we shut down the manager.
        scheduledExecutor.shutdownNow();
    }

    private void updateBlockNumbers(final BlockNodeContext serverContext) {
        // The current streaming should be the next block to be
        // streamed, but _only_ on startup. After that there should always be
        // a delta (next unstreamed must always be strictly greater than the current
        // streaming block number).
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
        // initially, the latest streaming block number is always the same as the current streaming block number
        lastStreamedBlockNumber.set(currentStreamingBlockNumber.get());
    }

    /// This method schedules the publisher unavailability timeout task.
    ///
    /// @return the [ScheduledFuture] representing the scheduled task
    @NonNull
    private ScheduledFuture<Boolean> schedulePublisherUnavailabilityTimeout() {
        cancelExistingFuture();
        return scheduledExecutor.schedule(
                new PublisherUnavailabilityTimeoutCallable(serverContext.blockMessaging(), handlers),
                publisherConfig.publisherUnavailabilityTimeout(),
                TimeUnit.SECONDS);
    }

    /// This method sends a publisher status update notification.
    private synchronized void sendPublisherStatusUpdate(
            final UpdateType type, final ConcurrentMap<Long, PublisherHandler> activeHandlers) {
        final PublisherStatusUpdateNotification notification =
                new PublisherStatusUpdateNotification(type, activeHandlers.size());
        // send a publisher update
        serverContext.blockMessaging().sendPublisherStatusUpdate(notification);
    }

    /// This method cancels the publisher unavailability timeout
    /// [ScheduledFuture] task if it is not null or done. The method will
    /// gracefully handle the future if it is done.
    private void cancelExistingFuture() {
        final ScheduledFuture<Boolean> localFuture = publisherUnavailabilityTimeoutFuture;
        if (localFuture != null) {
            // Remove the main reference to this future.
            publisherUnavailabilityTimeoutFuture = null;
            // Cancel first, if possible
            if (!localFuture.isCancelled() && !localFuture.isDone()) {
                // If not canceled, we can cancel it.
                localFuture.cancel(true);
            }
            // If done and not cancelled, get the result so we can log
            // any exceptions
            else if (localFuture.isDone() && !localFuture.isCancelled()) {
                try {
                    localFuture.get();
                } catch (final InterruptedException e) {
                    LOGGER.log(
                            TRACE, "Interrupted while waiting for publisher unavailability timeout task to complete");
                } catch (final ExecutionException e) {
                    LOGGER.log(INFO, "Publisher unavailability timeout task completed exceptionally", e);
                }
            }
        }
    }

    /// This method will return the next block number for the next block that must be resent.
    /// This method could also return {@link org.hiero.block.node.spi.BlockNodePlugin#UNKNOWN_BLOCK_NUMBER} if no
    /// more blocks are awaiting resend.
    private long nextBlockToResend() {
        long nextBlock;
        if (!blocksToResend.isEmpty()) {
            try {
                // The blocksToResend set is a SortedSet, so first item will be the lowest one.
                nextBlock = blocksToResend.first();
            } catch (final NoSuchElementException e) {
                // do nothing; we have no more blocks to resend.
                nextBlock = UNKNOWN_BLOCK_NUMBER;
            }
        } else {
            nextBlock = UNKNOWN_BLOCK_NUMBER;
        }
        return nextBlock;
    }

    /// todo(1420) add documentation
    private BlockAction getActionForHeader(final long blockNumber) {
        if (blocksToResend.contains(blockNumber)) {
            // If we expect a block to be resent, we must check if this publisher is currently publishing that block
            if (blocksToResend.remove(blockNumber)) {
                // Only one publisher can enter here and start publishing the expected block to be resent
                return BlockAction.ACCEPT;
            } else {
                return BlockAction.SKIP;
            }
        } else if (blockNumber <= lastPersistedBlockNumber.get()) {
            return BlockAction.END_DUPLICATE;
        } else if (blockNumber > lastPersistedBlockNumber.get() && blockNumber < nextUnstreamedBlockNumber.get()) {
            return streamBeforeEMBOrElse(blockNumber, BlockAction.SKIP);
        } else if (blockNumber == nextUnstreamedBlockNumber.get()) {
            return resolveActionForHeader(blockNumber);
        } else if (blockNumber > nextUnstreamedBlockNumber.get()) {
            return BlockAction.SEND_BEHIND;
        } else {
            // This should not be possible, all cases that could reach here are
            // already handled above.
            return BlockAction.END_ERROR;
        }
    }

    /// todo(1420) add documentation
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
            if (blockNumber > metrics.highestBlockNumber.get()) {
                metrics.highestBlockNumber.set(blockNumber);
            }
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
            return BlockAction.SEND_BEHIND;
        } else {
            // This should not be possible, all cases that could reach here are
            // already handled above.
            return BlockAction.END_ERROR;
        }
    }

    /// todo(1420) add documentation
    private BlockAction streamBeforeEMBOrElse(final long blockNumber, final BlockAction elseAction) {
        // current streaming number will always be within the range tested here.
        // Except when we're awaiting the first block after restart and earliest
        // managed block is higher than what the publisher offered here.
        if (blockNumber < earliestManagedBlock
                && nextUnstreamedBlockNumber.get() == currentStreamingBlockNumber.get()
                // Handle an edge case where we need to accept a block before the earliest
                // managed block right after the node (re)started.
                && nextUnstreamedBlockNumber.compareAndSet(earliestManagedBlock, blockNumber)) {
            currentStreamingBlockNumber.set(blockNumber);
            lastStreamedBlockNumber.set(blockNumber);
            metrics.lowestBlockNumber.set(blockNumber);
            return resolveActionForHeader(blockNumber);
        } else {
            return elseAction;
        }
    }

    /// todo(1420) add documentation
    private BlockAction resolveActionForHeader(final long blockNumber) {
        if (nextUnstreamedBlockNumber.compareAndSet(blockNumber, blockNumber + 1L)) {
            return BlockAction.ACCEPT;
        } else {
            // If the CAS does not succeed, we have either a SKIP or SEND_BEHIND
            return blockNumber < nextUnstreamedBlockNumber.get() ? BlockAction.SKIP : BlockAction.SEND_BEHIND;
        }
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

    /// Wait for data to be ready.
    ///
    /// This method will block until the data ready condition is signaled or
    /// the timeout is reached.
    /// This method is used (with [#signalDataReady()]) to limit spin
    /// cycles and still have a low impact on latency.
    ///
    /// When this method returns data _might_ be available to send to the
    /// messaging facility, but it is not guaranteed.
    ///
    /// Note
    /// > This method ignored interrupted exceptions as a specific
    /// optimization to avoid unnecessarily ending a thread or causing failures
    /// when interrupt is used as a signal rather than signaling the `Condition`
    /// variable.
    @SuppressWarnings("AwaitNotInLoop")
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

    /// Launch the queue forwarder task.
    ///
    /// This method is called when the first block is accepted, or when a block
    /// proof is received and the forwarder task is not running.
    ///
    /// The task will run until it encounters an exception or reaches a reasonable
    /// run time limit. When handlers encounter a block proof, a method is called
    /// to check and restart the task if it is not yet running or has completed.
    ///
    /// @return a Future representing pending completion of the task
    private Future<Long> launchQueueForwarder() {
        return threadManager.getVirtualThreadExecutor().submit(new MessagingForwarderTask(this));
    }

    /// todo(1420) add documentation
    private static class MessagingForwarderTask implements Callable<Long> {
        private final System.Logger LOGGER = System.getLogger(MessagingForwarderTask.class.getName());
        private final LiveStreamPublisherManager publisherManager;

        /// todo(1420) add documentation
        public MessagingForwarderTask(final LiveStreamPublisherManager liveStreamPublisherManager) {
            this.publisherManager = Objects.requireNonNull(liveStreamPublisherManager);
        }

        // This needs more work, particularly handling the next block if it's already
        // queued up from a different handler, until we run out of queued up batches.
        /// todo(1420) add documentation
        @Override
        public Long call() {
            long batchesSent = 0L;
            boolean forwardingLimitReached = false;
            // End this thread after some number of batches, so we don't have
            // a thread that becomes overly "old" and experiences "senescence".
            while (!forwardingLimitReached) {
                // @todo(2347) we can improve the loop because we have new way of doing resends, we can also
                //   improve on the way we determine the current block number below, but also the way we decide
                //   which items and how to hold them for a bit, to await for the end of block message so we can
                //   send them to messaging as the end of the block.
                final long currentBlockNumber = determineCurrentBlockNumber();
                final Deque<BlockItemSetUnparsed> queueToForward =
                        publisherManager.queueByBlockMap.get(currentBlockNumber);
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
                                    "Forwarding batch for block={0} with blockItemSize={1}",
                                    currentBlockNumber,
                                    currentBatch.blockItems().size());
                            // send the batch to the messaging facility
                            // @todo(2200) when we add support for the end of block message we might change
                            //    the way we send the block items
                            final boolean hasBlockProof =
                                    currentBatch.blockItems().getLast().hasBlockProof();
                            final int itemsSent;
                            if (hasBlockProof) {
                                final List<BlockItemUnparsed> currentItems = currentBatch.blockItems();
                                publisherManager.blockProofs.put(currentBlockNumber, currentItems.getLast());
                                if (currentItems.size() > 1) {
                                    // send all but the block proof
                                    final List<BlockItemUnparsed> blocksToSend =
                                            new ArrayList<>(currentItems.subList(0, currentItems.size() - 1));
                                    final BlockItemSetUnparsed itemSetToSend = BlockItemSetUnparsed.newBuilder()
                                            .blockItems(blocksToSend)
                                            .build();
                                    sendBlockItems(itemSetToSend, currentBlockNumber, false);
                                    itemsSent = blocksToSend.size();
                                } else {
                                    itemsSent = 0;
                                }
                                // If the last item in the batch is a block proof,
                                // we need to remove the queue from the queueByBlockMap.
                                publisherManager.queueByBlockMap.remove(currentBlockNumber);
                                // exit this while loop so we do not accidentally
                                // send a block out of order.
                                moreToSend = false;
                            } else {
                                sendBlockItems(currentBatch, currentBlockNumber, false);
                                itemsSent = currentBatch.blockItems().size();
                            }
                            if (itemsSent > 0) {
                                publisherManager.metrics.blockItemsMessaged().add(itemsSent);
                            }
                            // limit how many batches we send in a single task.
                            batchesSent++;
                            forwardingLimitReached =
                                    batchesSent >= publisherManager.publisherConfig.batchForwardLimit();
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
                    if (publisherManager.endBlocksReceived.contains(currentBlockNumber)) {
                        // Send the proof
                        final BlockItemUnparsed proof = publisherManager.blockProofs.remove(currentBlockNumber);
                        if (proof != null) {
                            publisherManager.endBlocksReceived.remove(currentBlockNumber);
                            final BlockItemSetUnparsed itemSet = BlockItemSetUnparsed.newBuilder()
                                    .blockItems(proof)
                                    .build();
                            sendBlockItems(itemSet, currentBlockNumber, true);
                            publisherManager
                                    .metrics
                                    .blockItemsMessaged()
                                    .add(itemSet.blockItems().size());
                            // Then potentially increment the current streaming
                            // block number.
                            publisherManager.currentStreamingBlockNumber.compareAndSet(
                                    currentBlockNumber, currentBlockNumber + 1);
                        } else {
                            // @todo(2200) if we have received the end of block message, but we do not have the proof,
                            //    should we take any action? In theory the end of block message should arrive after the
                            //    proof, but also we have the condition where we are registering the queue for the
                            //    block expected here again (i.e. starting to stream it again)
                        }
                    }
                    // We have no queue for this block, so wait for an
                    // indication that more data might be available.
                    publisherManager.waitForDataReady();
                }
            }
            return batchesSent;
        }

        private long determineCurrentBlockNumber() {
            final long result;
            final long publisherCurrentStreamingNumber = publisherManager.currentStreamingBlockNumber.get();
            final long publisherLatestStreamingNumber = publisherManager.lastStreamedBlockNumber.get();
            if (publisherManager.queueByBlockMap.containsKey(publisherLatestStreamingNumber)) {
                // If we are still streaming the current block, we need to
                // continue.
                result = publisherLatestStreamingNumber;
            } else {
                // Else, we either continue with the next one in line or proceed to
                // supply a block that was resent.
                final Entry<Long, Deque<BlockItemSetUnparsed>> firstEntry =
                        publisherManager.queueByBlockMap.firstEntry();
                if (firstEntry != null) {
                    // Note: If we have a first entry in the map, we need to see
                    // if it is lower than or equal (would be equal in the case
                    // where the block that just finished streaming is also just
                    // resent) to the publisher's current streaming value.
                    // If lower or equal, we need to return the lower block
                    // number, otherwise we need to return the publisher's
                    // current streaming value. Else it means that we have
                    // received a block that was resent.
                    final long lowestBlockByNumber = firstEntry.getKey();
                    result = Math.min(lowestBlockByNumber, publisherCurrentStreamingNumber);
                } else {
                    result = publisherCurrentStreamingNumber;
                }
            }
            // Always set the latest streaming block number to the result here
            publisherManager.lastStreamedBlockNumber.set(result);
            return result;
        }

        private void sendBlockItems(
                final BlockItemSetUnparsed currentBatch, final long currentBlockNumber, final boolean endOfBLock) {
            final List<BlockItemUnparsed> items = currentBatch.blockItems();
            final BlockItems toSend =
                    new BlockItems(items, currentBlockNumber, items.getFirst().hasBlockHeader(), endOfBLock);
            publisherManager.serverContext.blockMessaging().sendBlockItems(toSend);
        }
    }

    /// Metrics for tracking publisher handler activity:
    /// blockItemsMessaged - Number of block items delivered to the messaging service
    /// currentPublisherCount - Number of currently connected publishers
    /// lowestBlockNumber - Lowest incoming block number
    /// highestBlockNumber - Highest incoming block number
    /// latestBlockNumberAcknowledged - The latest block number acknowledged
    /// blocksClosedComplete - Number of blocks received complete (with both header and end of block)
    public record MetricsHolder(
            Counter blockItemsMessaged,
            Counter blockBatchesMessaged,
            IntegerGauge currentPublisherCount,
            LongGauge lowestBlockNumber,
            LongGauge highestBlockNumber,
            LongGauge latestBlockNumberAcknowledged,
            Counter blocksClosedComplete) {
        /// todo(1420) add documentation
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
            final IntegerGauge numberOfProducers =
                    metrics.getOrCreate(new IntegerGauge.Config(METRICS_CATEGORY, "publisher_open_connections")
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
                    blocksClosedComplete);
        }
    }

    /// A simple callable to send a timeout notification to the node's messaging
    /// in case of publisher timeout, meaning no publishers have connected within
    /// the configured timeout period.
    private static final class PublisherUnavailabilityTimeoutCallable implements Callable<Boolean> {
        private final BlockMessagingFacility messaging;
        private final ConcurrentMap<Long, PublisherHandler> activePublishers;

        private PublisherUnavailabilityTimeoutCallable(
                final BlockMessagingFacility messaging, final ConcurrentMap<Long, PublisherHandler> activeHandlers) {
            this.messaging = Objects.requireNonNull(messaging);
            this.activePublishers = Objects.requireNonNull(activeHandlers);
        }

        /// Sends a [PublisherStatusUpdateNotification] indicating that no publishers are currently connected and
        /// active.
        ///
        /// @return `true` if the timeout notification was sent, `false` otherwise.
        @Override
        public Boolean call() {
            if (!Thread.currentThread().isInterrupted() && activePublishers.isEmpty()) {
                // last chance to send the notification if we are not interrupted and if we have no active publishers
                // note, activePublishers.size() can definitely change between the isEmpty check and the call below
                final PublisherStatusUpdateNotification notification = new PublisherStatusUpdateNotification(
                        UpdateType.PUBLISHER_UNAVAILABILITY_TIMEOUT, activePublishers.size());
                messaging.sendPublisherStatusUpdate(notification);
                return true;
            } else {
                return false;
            }
        }
    }
}
