// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.stream.publisher;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.TRACE;
import static java.lang.System.Logger.Level.WARNING;
import static org.hiero.block.api.PublishStreamResponse.EndOfStream.Code.TIMEOUT;
import static org.hiero.block.node.spi.BlockNodePlugin.UNKNOWN_BLOCK_NUMBER;
import static org.hiero.block.node.stream.publisher.StreamPublisherPlugin.METRIC_PUBLISHER_BLOCKS_CLOSED_COMPLETE;
import static org.hiero.block.node.stream.publisher.StreamPublisherPlugin.METRIC_PUBLISHER_BLOCK_BATCHES_MESSAGED;
import static org.hiero.block.node.stream.publisher.StreamPublisherPlugin.METRIC_PUBLISHER_BLOCK_ITEMS_MESSAGED;
import static org.hiero.block.node.stream.publisher.StreamPublisherPlugin.METRIC_PUBLISHER_HIGHEST_BLOCK_NUMBER_INBOUND;
import static org.hiero.block.node.stream.publisher.StreamPublisherPlugin.METRIC_PUBLISHER_LATEST_BLOCK_NUMBER_ACKNOWLEDGED;
import static org.hiero.block.node.stream.publisher.StreamPublisherPlugin.METRIC_PUBLISHER_LOWEST_BLOCK_NUMBER_INBOUND;
import static org.hiero.block.node.stream.publisher.StreamPublisherPlugin.METRIC_PUBLISHER_OPEN_CONNECTIONS;
import static org.hiero.block.node.stream.publisher.StreamPublisherPlugin.METRIC_PUBLISHER_STALL_TIMEOUTS_SENT;

import com.hedera.pbj.runtime.grpc.Pipeline;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.TreeSet;
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
import org.hiero.block.api.PublishStreamResponse.EndOfStream.Code;
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
import org.hiero.metrics.LongCounter;
import org.hiero.metrics.LongGauge;
import org.hiero.metrics.core.MetricRegistry;

/// todo(1420) add documentation
public final class LiveStreamPublisherManager implements StreamPublisherManager {
    private static final int DATA_READY_WAIT_MICROSECONDS = 5000;
    private static final String STALL_DETECTED_LOG_MESSAGE =
            "[{0}] Stall detected: handler {1} has not completed block {2}, another handler completed block {3}";
    private final System.Logger LOGGER = System.getLogger(LiveStreamPublisherManager.class.getName());
    private final MetricsHolder metrics;
    private final BlockNodeContext serverContext;
    private final PublisherConfig publisherConfig;
    private final ThreadPoolManager threadManager;
    private final ConcurrentNavigableMap<Long, PublisherHandler> handlers;
    private final AtomicLong nextHandlerId;
    private final ConcurrentNavigableMap<Long, Deque<BlockItemSetUnparsed>> queueByBlockMap;
    private final AtomicLong blocksClosedComplete;
    private final Condition dataReadyLatch;
    private final ReentrantLock dataReadyLock;
    private final long earliestManagedBlock;
    private final int maxBlocksBeforeStalled;
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

    private final AtomicLong lastForwardedBlockNumber;
    private final AtomicLong nextUnstreamedBlockNumber;
    private final AtomicLong lastPersistedBlockNumber;

    // Note, using concrete definitions here to avoid unnecessary virtual calls
    //    for these frequently accessed collections.
    private final ConcurrentSkipListMap<Long, BlockItemUnparsed> blockProofs;
    private final ConcurrentSkipListSet<Long> endBlocksReceived;
    private final ConcurrentSkipListSet<Long> blocksToResend;
    private final ConcurrentSkipListSet<Long> activeResendBlocks;
    /// Maps block number to the ID of the handler that currently holds ACCEPT for that block.
    /// An entry is added when a handler wins ACCEPT via registerQueueForBlock().
    /// An entry is removed when endOfBlock() is called for that block, when blockIsEnding()
    /// fires for a mid-block drop, when stall detection removes it, or on shutdown.
    private final ConcurrentNavigableMap<Long, Long> activeStreamHandlerByBlock;

    /// todo(1420) add documentation
    public LiveStreamPublisherManager(
            @NonNull final BlockNodeContext context, @NonNull final MetricsHolder metricsHolder) {
        serverContext = Objects.requireNonNull(context);
        metrics = Objects.requireNonNull(metricsHolder);
        threadManager = serverContext.threadPoolManager();
        handlers = new ConcurrentSkipListMap<>();
        nextHandlerId = new AtomicLong(0);
        blocksClosedComplete = new AtomicLong(0);
        queueByBlockMap = new ConcurrentSkipListMap<>();
        lastForwardedBlockNumber = new AtomicLong(-1);
        nextUnstreamedBlockNumber = new AtomicLong(-1);
        lastPersistedBlockNumber = new AtomicLong(-1);
        dataReadyLock = new ReentrantLock();
        dataReadyLatch = dataReadyLock.newCondition();
        NodeConfig nodeConfiguration = serverContext.configuration().getConfigData(NodeConfig.class);
        earliestManagedBlock = nodeConfiguration.earliestManagedBlock();
        scheduledExecutor =
                threadManager.createVirtualThreadScheduledExecutor(1, null, this::uncaughtScheduledExecutorException);
        publisherConfig = serverContext.configuration().getConfigData(PublisherConfig.class);
        maxBlocksBeforeStalled = publisherConfig.MaxFutureBlocksBeforeStalled();
        publisherUnavailabilityTimeoutFuture = schedulePublisherUnavailabilityTimeout();
        blockProofs = new ConcurrentSkipListMap<>();
        endBlocksReceived = new ConcurrentSkipListSet<>();
        blocksToResend = new ConcurrentSkipListSet<>();
        activeResendBlocks = new ConcurrentSkipListSet<>();
        activeStreamHandlerByBlock = new ConcurrentSkipListMap<>();
        initializeBlockNumbers(serverContext);
    }

    @Override
    public PublisherHandler addHandler(
            @NonNull final Pipeline<? super PublishStreamResponse> replies,
            @NonNull final PublisherHandler.MetricsHolder handlerMetrics,
            final String correlationId) {
        final long handlerId = nextHandlerId.getAndIncrement();
        final PublisherHandler newHandler =
                new PublisherHandler(handlerId, replies, handlerMetrics, this, correlationId);
        // If there is an active unavailability timeout task, cancel it
        // because we now have a new publisher.
        // The cancel of the existing future must happen immediately prior to updating the active handlers map!
        cancelExistingFuture();
        handlers.put(handlerId, newHandler);
        // Now we can safely update the metrics and send the notification
        // for the new publisher.
        metrics.currentPublisherCount().set(handlers.size());
        sendPublisherStatusUpdate(UpdateType.PUBLISHER_CONNECTED, handlers);
        LOGGER.log(DEBUG, "Added new handler {0}", handlerId);
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
        LOGGER.log(DEBUG, "Removed handler {0}", handlerId);
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
            // If there is a previous value, this means that the publisher that streamed this block is now
            // re-starting it mid-block. We need to remove a potentially collected block proof.
            blockProofs.remove(blockNumber);
        }
        // Record which handler won ACCEPT for this block so stall detection
        // can identify and terminate a silent publisher.
        activeStreamHandlerByBlock.put(blockNumber, handlerId);
    }

    @Override
    public long getLatestBlockNumber() {
        return lastPersistedBlockNumber.get();
    }

    @Override
    public ActionForBlock endOfBlock(final long blockNumber) {
        if (queueByBlockMap.containsKey(blockNumber)) {
            endBlocksReceived.add(blockNumber);
        } else {
            // If the queue is not in the map, this means that the manager no longer
            // needs the block
            // Remove a proof in case it is collected
            blockProofs.remove(blockNumber);
        }
        // Remove from the active resend set, if it's present
        activeResendBlocks.remove(blockNumber);
        // The completing block is no longer an ACCEPT candidate for stall detection.
        activeStreamHandlerByBlock.remove(blockNumber);
        // After removing the completing block, check whether any remaining
        // ACCEPT holders are stalled relative to this completion.
        checkForStalledHandlers(blockNumber);
        // Check for new blocks to resend (previous method might have added one)
        // and send a resend response if there are blocks needing to resend.
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
        // Remove from the active resend set, if it's present
        activeResendBlocks.remove(blockNumber);
        // Remove this block from the ACCEPT winner tracking in all cases;
        // if the handler dropped mid-block there is no stall to detect here.
        activeStreamHandlerByBlock.remove(blockNumber);
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
        LOGGER.log(DEBUG, "Completed blocks {0}", blocksClosedComplete.incrementAndGet());
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
    public void shutdown() {
        // Shut down all handlers and clear the queues.
        // Order is important here, we want to stop the handlers before
        // we stop the forwarding task and that must happen before we clear the
        // queue maps.
        for (final Long nextKey : handlers.keySet()) {
            final PublisherHandler value = handlers.remove(nextKey);
            if (value != null) {
                value.endStreamWithCode(Code.SUCCESS, false);
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
        activeStreamHandlerByBlock.clear();
        // We can safely shut down the scheduled executor abruptly. The timeout
        // tasks can be ignored completely as we shut down the manager.
        scheduledExecutor.shutdownNow();
    }

    @Override
    public void signalDataReady() {
        dataReadyLock.lock();
        try {
            dataReadyLatch.signal();
        } finally {
            dataReadyLock.unlock();
        }
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
            // Schedule a resend for the block before sending the bad block proof message
            blocksToResend.add(blockNumber);
            // Iterate over all handlers and attempt to send the
            // bad block proof message.
            for (final PublisherHandler handler : handlers.values()) {
                if (handler.handleFailedVerification(blockNumber)) {
                    // There will always be only one handler that will send the
                    // bad block proof message. Once we have, we can break.
                    break;
                }
            }
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
                // There was a test, isBeforeEarliestActiveBlock(newLastPersistedBlock) here
                // but this must be removed for now.
                // @todo(#1841) reconsider this conditional in light of other changes.
                if (newLastPersistedBlock > lastPersistedBlockNumber.get()) {
                    // Update internal manager state before sending acknowledgement
                    lastPersistedBlockNumber.set(newLastPersistedBlock);
                    ensureNextGreaterThanPersisted(newLastPersistedBlock);
                    clearObsoleteQueueItems(newLastPersistedBlock);
                    handlers.values().parallelStream().unordered().forEach(handler -> {
                        // _Important_, we only need the last persisted block number
                        // all previous blocks are implicitly acknowledged.
                        handler.sendAcknowledgement(newLastPersistedBlock);
                    });
                    metrics.latestBlockNumberAcknowledged.set(newLastPersistedBlock);
                }
            } else {
                queueByBlockMap.clear();
                final long blockNumber = notification.blockNumber();
                // @todo(2198): We may have an extremely rare race condition here
                nextUnstreamedBlockNumber.set(blockNumber);
                handlers.values().parallelStream()
                        .unordered()
                        .forEach((handler) -> handler.endStreamWithCode(Code.PERSISTENCE_FAILED, false));
            }
        }
    }

    /// Removes queue entries for blocks that are incomplete and
    /// presumed already persisted.
    ///
    /// This method scans all entries in [#queueByBlockMap] whose block number
    /// is strictly less than `blockNumber` and removes any that are confirmed
    /// incomplete — meaning the block has not yet streamed a block proof.
    ///
    /// An entry is considered safe to remove only when _all_ conditions hold:
    /// - The block number is **not** present as a key in [#blockProofs], which
    ///    would indicate that the block is complete and awaiting dispatch
    /// - The last [BlockItemSetUnparsed] batch in the queue does **not** end
    ///   with a `BlockProof` item, which would indicate the proof arrived but
    ///   the block is not yet completed (this should be quite rare).
    /// - The block number is **not** present in current blocks being resent.
    ///   Blocks being actively resent would otherwise always be removed,
    ///   resulting in broken blocks and handler failures.
    ///
    /// Entries that fail either check are left in the map so that in-flight
    /// complete blocks are not lost before they can be forwarded to the
    /// messaging facility.
    ///
    /// This method is called any time the last persisted block is changed.
    /// This will discard buffered data for blocks that will never be forwarded
    /// and prevent the forwarder becoming "stuck" on an incomplete block.
    ///
    /// @param blockNumber the latest persisted block number. Queue entries
    ///     whose key is less than or equal to this value are candidates
    ///     for removal.
    private void clearObsoleteQueueItems(final long blockNumber) {
        if (queueByBlockMap != null && !queueByBlockMap.isEmpty()) {
            final NavigableSet<Long> keysBeforeBlock = new TreeSet<>();
            keysBeforeBlock.addAll(queueByBlockMap.headMap(blockNumber, true).keySet());
            for (final Long candidate : keysBeforeBlock) {
                if (!(blockProofs.containsKey(candidate) || hasBlockProof(queueByBlockMap.get(candidate)))) {
                    queueByBlockMap.remove(candidate);
                    // possibly remove a just collected block proof
                    blockProofs.remove(candidate);
                    // remove from stall-detection tracking; this block is now obsolete
                    activeStreamHandlerByBlock.remove(candidate);
                }
            }
        }
    }

    /// Return `true` iff the provided queue of block item lists ends with
    /// a `BlockProof`.
    ///
    /// One or more block proofs are the final items in a complete block.
    /// The presence of a `BlockProof` signals that all content items for the
    /// block have been received and that the block is ready for verification
    /// and persistence. Callers that need to detect completed blocks — for
    /// example, to decide when to remove an incomplete queue — should use this
    /// method rather than inspecting the raw queue data directly.
    ///
    /// @param activeQueue a [Deque] of Block Item Sets to inspect for a
    ///     `BlockProof`.
    /// @return `true` iff the item list for the _last_ entry in the provided
    ///     queue ends with a `BlockProof` item.
    private boolean hasBlockProof(final Deque<BlockItemSetUnparsed> activeQueue) {
        final BlockItemSetUnparsed lastItem = getLastDequeItem(activeQueue);
        if (lastItem != null
                && lastItem.blockItems() != null
                && !lastItem.blockItems().isEmpty()) {
            return lastItem.blockItems().getLast().hasBlockProof();
        } else {
            return false;
        }
    }

    /// This method will return the last item of a [Deque].
    /// The method does not throw, but will return `null` in cases where the
    /// deque is null or empty.
    /// @param deque the deque to get the last item of
    /// @param <T> type of the items in the deque
    /// @return the last item of the deque or `null` if the deque provided is
    ///    `null` or empty
    @Nullable
    private <T> T getLastDequeItem(final Deque<T> deque) {
        try {
            if (deque != null && !deque.isEmpty()) {
                return deque.getLast();
            } else {
                return null;
            }
        } catch (final NoSuchElementException e) {
            return null;
        }
    }

    /// Checks whether any currently accepted block is stalled relative to the
    /// block that just completed. A handler is considered stalled when it holds
    /// ACCEPT for block N, has not yet called endOfBlock(N), and another handler
    /// has just called endOfBlock(M) where M > N + 2.
    ///
    /// The +2 threshold respects the protocol requirement that a publisher may
    /// reasonably require up to 2 block times to complete a block due to
    /// signature gathering.
    ///
    /// For each stalled block found:
    /// - Its queue and proof entries are removed so the forwarder can advance.
    /// - Its block number is added to blocksToResend.
    /// - The owning handler receives EndStream(TIMEOUT) and is shut down.
    /// - The stall-timeout metric is incremented.
    ///
    /// Thread safety: multiple handler threads may call this concurrently for
    /// different values of completedBlockNumber. The
    /// ConcurrentNavigableMap.remove(key, value) CAS ensures that only one
    /// caller acts on each stalled block even if two threads detect it
    /// simultaneously.
    ///
    /// @param completedBlockNumber the block number that just finished via endOfBlock()
    private void checkForStalledHandlers(final long completedBlockNumber) {
        for (final Map.Entry<Long, Long> entry : activeStreamHandlerByBlock.entrySet()) {
            final long candidateBlock = entry.getKey();
            final long handlerId = entry.getValue();
            if (completedBlockNumber > candidateBlock + maxBlocksBeforeStalled
                    && !endBlocksReceived.contains(candidateBlock)
                    && !isResendingLive(candidateBlock)
                    && activeStreamHandlerByBlock.remove(candidateBlock, handlerId)) {
                // This thread won the CAS — execute the stall action.
                // @todo: Add CorrelationID _for the handler that is stalled_
                //        (not the thread that called this method).
                //     This requires making correlation ID accessible in Handler.
                PublisherHandler stalledHandler = handlers.get(handlerId);
                String correlationId = stalledHandler != null ? stalledHandler.getCorrelationId() : "";
                LOGGER.log(
                        DEBUG,
                        STALL_DETECTED_LOG_MESSAGE,
                        correlationId,
                        handlerId,
                        candidateBlock,
                        completedBlockNumber);
                queueByBlockMap.remove(candidateBlock);
                blockProofs.remove(candidateBlock);
                blocksToResend.add(candidateBlock);
                if (stalledHandler != null) {
                    stalledHandler.endStreamWithCode(TIMEOUT, true);
                }
                metrics.stallTimeoutsSent().increment();
            }
        }
    }

    /// Check if the candidate block is being resent and is newer than the last
    /// persisted block. If both conditions are true, then this block should
    /// be considered a "live" resend and not eligible for stall detection.
    ///
    /// @param candidateBlock the block number to test
    ///
    /// @return true if, and only if, the candidate block is currently
    ///     streaming as a result of a resend request and is still needed by
    ///     this block node.
    ///
    private boolean isResendingLive(final long candidateBlock) {
        return activeResendBlocks.contains(candidateBlock) && candidateBlock > lastPersistedBlockNumber.get();
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

    /// todo(1420) add documentation
    private void initializeBlockNumbers(final BlockNodeContext serverContext) {
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
            nextUnstreamedBlockNumber.set(earliestManagedBlock);
        } else if (latestKnownBlock < earliestManagedBlock) {
            // We haven't caught up to the earliest block the operator wants
            // to treat as the start of history, so accept anything up to that
            // block as the "next" block.
            nextUnstreamedBlockNumber.set(earliestManagedBlock);
        } else {
            // if we have entered here, we know what the latest known block is,
            // and we must prevent gaps after that block, so we can set the
            // next unstreamed block number to one greater pretend the next
            // unstreamed block is streaming initially, that ensures that the
            // first block accepted is correctly handled.
            nextUnstreamedBlockNumber.set(latestKnownBlock + 1L);
        }
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
        }
        final long lastPersisted = lastPersistedBlockNumber.get();
        final long nextUnstreamed = ensureNextGreaterThanPersisted(lastPersisted);
        if (blockNumber <= lastPersisted) {
            return BlockAction.END_DUPLICATE;
        } else if (blockNumber < nextUnstreamed) {
            return streamBeforeEmbOrElse(blockNumber, BlockAction.SKIP);
        } else if (blockNumber == nextUnstreamed) {
            return resolveActionForHeader(blockNumber);
        } else {
            return BlockAction.SEND_BEHIND;
        }
    }

    /// todo(1420) add documentation
    private long ensureNextGreaterThanPersisted(final long lastPersisted) {
        long nextUnstreamed = nextUnstreamedBlockNumber.get();
        if (lastPersisted >= nextUnstreamed) {
            // potentially increment the next unstreamed
            final long newValue = lastPersisted + 1L;
            if (nextUnstreamedBlockNumber.compareAndSet(nextUnstreamed, newValue)) {
                // if cas was successful, we can update the local value as well
                nextUnstreamed = newValue;
            }
        }
        return nextUnstreamed;
    }

    /// todo(1420) add documentation
    private BlockAction getActionForCurrentlyStreaming(final long blockNumber) {
        if (queueByBlockMap.containsKey(blockNumber)) {
            updateHighestBlockMetric(blockNumber);
            // We're one of the handlers currently streaming, keep going.
            return BlockAction.ACCEPT;
        } else {
            // The block is no longer needed by the manager, we can skip it
            return BlockAction.SKIP;
        }
    }

    /// Update the highest block metric.
    ///
    /// This method only updates the metric based on next unstreamed block
    /// number.
    ///
    /// For now, this means checking the block queue map, when we upgrade
    /// to a newer version of the metrics API, we will query the metric
    /// instead (which is more reliable).
    private void updateHighestBlockMetric(final long blockNumber) {
        final long candidateValue;
        if (blockNumber >= nextUnstreamedBlockNumber.get()) {
            candidateValue = blockNumber;
        } else {
            candidateValue = nextUnstreamedBlockNumber.get() - 1;
        }
        metrics.highestBlockNumber.set(candidateValue);
    }

    /// todo(1420) add documentation
    private BlockAction streamBeforeEmbOrElse(final long blockNumber, final BlockAction elseAction) {
        // current streaming number will always be within the range tested here.
        // Except when we're awaiting the first block after restart and earliest
        // managed block is higher than what the publisher offered here.
        if (blockNumber < earliestManagedBlock
                // Handle an edge case where we need to accept a block before the earliest
                // managed block right after the node (re)started.
                && lastForwardedBlockNumber.get() == UNKNOWN_BLOCK_NUMBER
                && nextUnstreamedBlockNumber.compareAndSet(earliestManagedBlock, blockNumber)) {
            metrics.lowestBlockNumber.set(blockNumber);
            return resolveActionForHeader(blockNumber);
        } else {
            return elseAction;
        }
    }

    /// todo(1420) add documentation
    private BlockAction resolveActionForHeader(final long blockNumber) {
        if (nextUnstreamedBlockNumber.compareAndSet(blockNumber, blockNumber + 1L)) {
            updateHighestBlockMetric(blockNumber + 1);
            return BlockAction.ACCEPT;
        } else {
            // If the CAS does not succeed, we have either a SKIP or SEND_BEHIND
            return blockNumber < nextUnstreamedBlockNumber.get() ? BlockAction.SKIP : BlockAction.SEND_BEHIND;
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
                metrics.blockBatchesMessaged().increment(blocksForwarded);
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

    private void uncaughtScheduledExecutorException(Thread t, Throwable e) {
        final String message = "Scheduled task thread exception occurred in Live Stream Publisher Manager";
        LOGGER.log(WARNING, message, e);
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
                if (queueToForward != null && !publisherManager.blockProofs.containsKey(currentBlockNumber)) {
                    boolean moreToSend = queueToForward.peek() != null;
                    while (moreToSend) {
                        // We MUST remove each batch from the queue before sending, otherwise we end
                        // up sending the same block over and over, forever, while the queue grows
                        // without bounds.  Use `poll` not `remove` because `remove` throws a
                        // runtime exception if the queue is empty.
                        final BlockItemSetUnparsed currentBatch = queueToForward.poll();
                        if (currentBatch != null) {
                            // @todo(2347) as an improvement we can opt in to possibly change the
                            //    way we send the last batch of BlockItems
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
                                // exit this while loop so we do not accidentally
                                // send a block out of order.
                                moreToSend = false;
                            } else {
                                sendBlockItems(currentBatch, currentBlockNumber, false);
                                itemsSent = currentBatch.blockItems().size();
                            }
                            if (itemsSent > 0) {
                                publisherManager.metrics.blockItemsMessaged().increment(itemsSent);
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
                    if (queueToForward.isEmpty()) {
                        publisherManager.waitForDataReady();
                    }
                } else {
                    if (publisherManager.endBlocksReceived.contains(currentBlockNumber)) {
                        // Send the proof
                        final BlockItemUnparsed proof = publisherManager.blockProofs.remove(currentBlockNumber);
                        if (proof != null) {
                            final BlockItemSetUnparsed itemSet = BlockItemSetUnparsed.newBuilder()
                                    .blockItems(proof)
                                    .build();
                            // Remove the queue of the block, this will now mark the block as no longer active
                            publisherManager.queueByBlockMap.remove(currentBlockNumber);
                            publisherManager.endBlocksReceived.remove(currentBlockNumber);
                            // Send the last item set to internal messaging
                            sendBlockItems(itemSet, currentBlockNumber, true);
                            // Finally, update metrics
                            publisherManager
                                    .metrics
                                    .blockItemsMessaged()
                                    .increment(itemSet.blockItems().size());
                            // Then potentially increment the current streaming
                            // block number.
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
            final long publisherLastForwardedBlockNumber = publisherManager.lastForwardedBlockNumber.get();
            if (publisherManager.queueByBlockMap.containsKey(publisherLastForwardedBlockNumber)) {
                // If we are still streaming the current block, we need to
                // continue.
                result = publisherLastForwardedBlockNumber;
            } else {
                // Else, we either continue with the next one in line or proceed to
                // supply a block that was resent.
                final Entry<Long, Deque<BlockItemSetUnparsed>> firstEntry =
                        publisherManager.queueByBlockMap.firstEntry();
                if (firstEntry != null) {
                    result = firstEntry.getKey();
                } else {
                    result = UNKNOWN_BLOCK_NUMBER;
                }
            }
            // Always set the latest streaming block number to the result here
            publisherManager.lastForwardedBlockNumber.set(result);
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
            LongCounter.Measurement blockItemsMessaged,
            LongCounter.Measurement blockBatchesMessaged,
            LongGauge.Measurement currentPublisherCount,
            LongGauge.Measurement lowestBlockNumber,
            LongGauge.Measurement highestBlockNumber,
            LongGauge.Measurement latestBlockNumberAcknowledged,
            LongCounter.Measurement blocksClosedComplete,
            LongCounter.Measurement stallTimeoutsSent) {
        /// todo(1420) add documentation
        static MetricsHolder createMetrics(@NonNull final MetricRegistry metricRegistry) {
            final LongCounter.Measurement blockItemsMessaged = metricRegistry
                    .register(LongCounter.builder(METRIC_PUBLISHER_BLOCK_ITEMS_MESSAGED)
                            .setDescription("Live block items messaged to the messaging service"))
                    .getOrCreateLabeled();
            final LongCounter.Measurement blockBatchesMessaged = metricRegistry
                    .register(LongCounter.builder(METRIC_PUBLISHER_BLOCK_BATCHES_MESSAGED)
                            .setDescription("Live block batches processed and sent to the messaging service"))
                    .getOrCreateLabeled();
            final LongCounter.Measurement blocksClosedComplete = metricRegistry
                    .register(LongCounter.builder(METRIC_PUBLISHER_BLOCKS_CLOSED_COMPLETE)
                            .setDescription("Blocks received complete (with both header and proof) by any Handler"))
                    .getOrCreateLabeled();
            final LongCounter.Measurement stallTimeoutsSent = metricRegistry
                    .register(LongCounter.builder(METRIC_PUBLISHER_STALL_TIMEOUTS_SENT)
                            .setDescription("Publishers terminated due to stall detection (silent ACCEPT winner)"))
                    .getOrCreateNotLabeled();
            final LongGauge.Measurement numberOfProducers = metricRegistry
                    .register(
                            LongGauge.builder(METRIC_PUBLISHER_OPEN_CONNECTIONS).setDescription("Connected publishers"))
                    .getOrCreateNotLabeled();
            final LongGauge.Measurement lowestBlockNumber = metricRegistry
                    .register(LongGauge.builder(METRIC_PUBLISHER_LOWEST_BLOCK_NUMBER_INBOUND)
                            .setDescription("Oldest inbound block number"))
                    .getOrCreateNotLabeled();
            final LongGauge.Measurement highestBlockNumber = metricRegistry
                    .register(LongGauge.builder(METRIC_PUBLISHER_HIGHEST_BLOCK_NUMBER_INBOUND)
                            .setDescription("Newest inbound block number"))
                    .getOrCreateNotLabeled();
            final LongGauge.Measurement latestBlockNumberAcknowledged = metricRegistry
                    .register(LongGauge.builder(METRIC_PUBLISHER_LATEST_BLOCK_NUMBER_ACKNOWLEDGED)
                            .setDescription("Latest block number acknowledged"))
                    .getOrCreateNotLabeled();
            return new MetricsHolder(
                    blockItemsMessaged,
                    blockBatchesMessaged,
                    numberOfProducers,
                    lowestBlockNumber,
                    highestBlockNumber,
                    latestBlockNumberAcknowledged,
                    blocksClosedComplete,
                    stallTimeoutsSent);
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
