// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.stream.subscriber;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.locks.LockSupport.parkNanos;
import static org.hiero.block.node.spi.BlockNodePlugin.METRICS_CATEGORY;
import static org.hiero.block.node.spi.BlockNodePlugin.UNKNOWN_BLOCK_NUMBER;

import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.pbj.runtime.OneOf;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.grpc.Pipeline;
import com.swirlds.metrics.api.Counter;
import com.swirlds.metrics.api.Counter.Config;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.hiero.block.api.SubscribeStreamRequest;
import org.hiero.block.api.SubscribeStreamResponse;
import org.hiero.block.internal.BlockItemSetUnparsed;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.internal.SubscribeStreamResponseUnparsed;
import org.hiero.block.internal.SubscribeStreamResponseUnparsed.Builder;
import org.hiero.block.internal.SubscribeStreamResponseUnparsed.ResponseOneOfType;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.NoBackPressureBlockItemHandler;
import org.hiero.block.node.spi.historicalblocks.BlockAccessor;

/**
 * This class is used to represent a session for a single BlockStream subscriber that has connected to the block node.
 * At least to start with we will only support receiving a single SubscribeStreamRequest per session.
 * <p>
 * This session supports two primary modes of operation: live-streaming and historical streaming. Also switching between
 * on demand as needed.<p>
 * #### Threading<br/>
 * This class is called from many threads from web server, the block messaging system and its own background historical
 * fetching thread. To make its state thread safe it uses synchronized methods around all entry points from other
 * threads (which are minimized to avoid potential for deadlock or contention). Currently only the close method is
 * synchronized.
 */
public class BlockStreamSubscriberSession implements Callable<BlockStreamSubscriberSession> {
    /** The logger for this class. */
    private final Logger LOGGER = System.getLogger(getClass().getName());

    /** The maximum time units to wait for a live block to be available */
    private static final long MAX_LIVE_POLL_DELAY = 500;
    /** The time unit for the maximum live poll delay */
    private static final TimeUnit LIVE_POLL_UNITS = TimeUnit.MILLISECONDS;
    /** The "resolution", in ns, to use for a polling delays. */
    private static final long PARK_DELAY_TIME = 100_000;

    /** The client id for this session */
    private final long clientId;
    /** The first block number to stream */
    private final long startBlockNumber;
    /** The last block number to stream, can be {@value BlockNodePlugin#UNKNOWN_BLOCK_NUMBER } to mean infinite */
    private final long endBlockNumber;
    /** The pipeline to send responses to the client */
    private final Pipeline<? super SubscribeStreamResponseUnparsed> responsePipeline;
    /** The context for the block node */
    private final BlockNodeContext context;
    /** The configuration for the "owning" plugin */
    private final SubscriberConfig pluginConfiguration;
    /** The number of historic to live stream transitions metric */
    private final Counter historicToLiveStreamTransitions;
    /** The number of live to historic stream transitions metric */
    private final Counter liveToHistoricStreamTransitions;
    /** The name of this handler, used in toString and for testing */
    private final String handlerName;
    /** A blocking queue to send blocks from the live handler to the session. */
    private final BlockingQueue<BlockItems> liveBlockQueue;
    /** A lock to hold the pipeline thread until this session is ready */
    private final CountDownLatch sessionReadyLatch;
    /** A flag indicating if the session should be interrupted */
    private final AtomicBoolean interruptedStream = new AtomicBoolean(false);
    /** The subscription for the GRPC connection with client */
    private Subscription subscription;
    /** The current block being sent to the client */
    private long nextBlockToSend;
    /** The latest block received from the live stream. */
    private final AtomicLong latestLiveStreamBlock;
    /**
     * A thread that receives live blocks from the messaging facility.
     * Each received block is offered to the session via a blocking queue.
     */
    private final LiveBlockHandler liveBlockHandler;
    /**
     * Exception that caused this session to fail during operation.
     * This is only set if an unexpected exception is thrown in the call method
     * and enables the plugin to remove failed sessions from the open sessions
     * list it maintains, and log the cause of that failure.
     */
    private Exception sessionFailedCause;

    /**
     * Constructor for the BlockStreamSubscriberSession class.
     *
     * @param clientId The client id for this session
     * @param responsePipeline The pipeline to send responses to the client
     * @param context The context for the block node
     */
    public BlockStreamSubscriberSession(
            final long clientId,
            @NonNull final SubscribeStreamRequest request,
            @NonNull final Pipeline<? super SubscribeStreamResponseUnparsed> responsePipeline,
            @NonNull final BlockNodeContext context,
            @NonNull final CountDownLatch sessionReadyLatch) {
        requireNonNull(request);

        LOGGER.log(Level.TRACE, request.toString());
        this.clientId = clientId;
        this.startBlockNumber = request.startBlockNumber();
        this.endBlockNumber = request.endBlockNumber();
        this.responsePipeline = requireNonNull(responsePipeline);
        this.context = requireNonNull(context);
        this.sessionReadyLatch = requireNonNull(sessionReadyLatch);
        latestLiveStreamBlock = new AtomicLong(UNKNOWN_BLOCK_NUMBER - 1);
        pluginConfiguration = context.configuration().getConfigData(SubscriberConfig.class);
        // Next block to send depends on what was requested and what is available.
        nextBlockToSend = startBlockNumber < 0 ? getLatestKnownBlock() : startBlockNumber;
        handlerName = "Live stream client " + clientId;
        // create metrics
        // Note: These two may, or may not, be useful...
        historicToLiveStreamTransitions = context.metrics()
                .getOrCreate(new Config(METRICS_CATEGORY, "historicToLiveStreamTransitions")
                        .withDescription("Historic to Live Stream Transitions"));
        liveToHistoricStreamTransitions = context.metrics()
                .getOrCreate(new Config(METRICS_CATEGORY, "liveToHistoricStreamTransitions")
                        .withDescription("Live to Historic Stream Transitions"));
        liveBlockQueue = new ArrayBlockingQueue<>(pluginConfiguration.liveQueueSize());
        liveBlockHandler = new LiveBlockHandler(liveBlockQueue, latestLiveStreamBlock, handlerName);
    }

    private long getEarliestHistoricalBlock() {
        return context.historicalBlockProvider().availableBlocks().min();
    }

    private long getLatestKnownBlock() {
        return Math.max(getLatestHistoricalBlock(), latestLiveStreamBlock.get());
    }

    private long getLatestHistoricalBlock() {
        return context.historicalBlockProvider().availableBlocks().max();
    }

    @Override
    public BlockStreamSubscriberSession call() {
        try {
            // get latest available blocks
            final long oldestBlockNumber = getEarliestHistoricalBlock();
            final long latestBlockNumber = getLatestKnownBlock();
            // we have just started with a new subscribe request, now we need to work out if we can complete it
            if (validateRequest(
                    oldestBlockNumber, latestBlockNumber, startBlockNumber, endBlockNumber, clientId, LOGGER)) {
                // register us to listen to block items from the block messaging system
                LOGGER.log(Level.TRACE, "Registering a block subscriber handler for " + handlerName);
                context.blockMessaging().registerNoBackpressureBlockItemHandler(liveBlockHandler, false, handlerName);
                sessionReadyLatch.countDown();
                // Send blocks forever if requested, otherwise send until we reach the requested end block
                // or the stream is interrupted.
                while (!(interruptedStream.get() || allRequestedBlocksSent())) {
                    // Note, nextBlockToSend is always the next block we need to send.
                    // start by sending from history if we can.
                    sendHistoricalBlocks();
                    // then process live blocks, if available.
                    sendLiveBlocksIfAvailable();
                }
                if (!interruptedStream.get()) {
                    // We've sent all request blocks. Therefore, we close, according to the protocol.
                    close(SubscribeStreamResponse.Code.READ_STREAM_SUCCESS);
                }
            }
        } catch (RuntimeException | ParseException | InterruptedException e) {
            sessionFailedCause = e;
            interruptedStream.set(true);
            close(SubscribeStreamResponse.Code.READ_STREAM_SUCCESS); // Need an "INCOMPLETE" code...
        }
        // Need to record a metric here with client ID tag, so we can record
        // requested vs sent metrics.
        return this;
    }

    private void sendHistoricalBlocks() throws ParseException {
        // Don't send anything from history if this stream is interrupted, has sent
        // every requested block, or we can send the next block from "live".
        while (isHistoryPermitted()) {
            LOGGER.log(Level.TRACE, "Sending historical block {0} to {1}", nextBlockToSend, handlerName);
            // We need to send historical blocks.
            // We will only send one block at a time to keep things "smooth".
            // Start by getting a block accessor for the next block to send from the historical provider.
            final BlockAccessor nextBlockAccessor =
                    context.historicalBlockProvider().block(nextBlockToSend);
            if (nextBlockAccessor != null) {
                // We have a block to send, so send it.
                sendOneBlockItemSet(nextBlockAccessor.blockUnparsed());
                // Trim the queue if necessary, also increment the next block to send.
                trimBlockItemQueue(++nextBlockToSend);
            } else {
                // Only give up if this is an historical block, otherwise just
                // go back up and see if live has the block.
                if (!(nextBlockToSend < 0 || nextBlockToSend >= getLatestKnownBlock())) {
                    // We cannot get the block needed, something has failed.
                    // close the stream with an "unavailable" response.
                    final String message = "Unable to read historical block {0}.";
                    LOGGER.log(Level.INFO, message, nextBlockToSend);
                    close(SubscribeStreamResponse.Code.READ_STREAM_NOT_AVAILABLE);
                } else {
                    awaitNewLiveEntries();
                }
                break;
            }
            LOGGER.log(Level.TRACE, "Done sending historical block to {1}, {0} up next.", nextBlockToSend, handlerName);
        }
    }

    /**
     * Is sending a block from history permitted?
     * <p>
     * This is true if all of the following are true:
     *     1. The stream is not interrupted
     *     2. We have not run past the latest live block
     *     3. All requested blocks have not been sent
     *     4. The next block to send is not available from the live stream.
     */
    private boolean isHistoryPermitted() {
        return !(interruptedStream.get()
                || latestLiveStreamBlock.get() < nextBlockToSend
                || allRequestedBlocksSent()
                || nextBatchIsLive());
    }

    private boolean nextBatchIsLive() {
        BlockItems queueHead = liveBlockQueue.peek();
        if (queueHead != null && latestLiveStreamBlock.get() >= nextBlockToSend) {
            return queueHead.newBlockNumber() == nextBlockToSend || !queueHead.isStartOfNewBlock();
        } else {
            return false;
        }
    }

    private void awaitNewLiveEntries() {
        final long maximumDelay = (LIVE_POLL_UNITS.toNanos(MAX_LIVE_POLL_DELAY) / PARK_DELAY_TIME);
        for (long i = 0; i < maximumDelay && liveBlockQueue.isEmpty(); i++) {
            // Sleep for a bit to avoid excessive spin overhead.
            parkNanos(PARK_DELAY_TIME);
        }
    }

    private boolean allRequestedBlocksSent() {
        if (endBlockNumber == 0L) { // We are requesting block indefinitely.
            return false;
        }
        return endBlockNumber != UNKNOWN_BLOCK_NUMBER && nextBlockToSend > endBlockNumber;
    }

    /**
     * Process live blocks to send to the client.
     *
     * @throws InterruptedException if the thread is interrupted while waiting for a "live" batch.
     */
    private void sendLiveBlocksIfAvailable() throws InterruptedException {
        // Need to park here waiting for at least one entry on the queue, otherwise
        // we'll spin in the caller thread in an extreme form of spin wait.
        if (liveBlockQueue.isEmpty()) {
            // Wait briefly for a live block to be available.
            awaitNewLiveEntries();
        }
        // Send as much as we can from live. If we have no live blocks, or have sent everything
        // currently available, then we'll go back to the caller's loop.
        // If we run out, get ahead of live, or have to send historical blocks,
        // then we'll also break out of the loop and return to the caller.
        while (!liveBlockQueue.isEmpty()) {
            // take the block item from the queue and process it
            final BlockItems blockItems = liveBlockQueue.poll();
            // Live _might_ be behind the next expected block (particularly if
            // the requested start block is in the future), skip this block, in that case.
            if (blockItems.isStartOfNewBlock() && blockItems.newBlockNumber() != nextBlockToSend) {
                LOGGER.log(Level.TRACE, "Skipping block {0} for client {1}", blockItems.newBlockNumber(), clientId);
                skipCurrentBlockInQueue(blockItems);
            } else {
                if (blockItems != null) {
                    sendOneBlockItemSet(blockItems);
                }
                if (blockItems.isEndOfBlock()) {
                    nextBlockToSend++;
                    trimBlockItemQueue(nextBlockToSend);
                }
            }
            // Note: We depend here on the rule that there are no gaps between blocks.
            //       The header for block N immediately follows the proof for block N-1.
            final BlockItems nextBatch = liveBlockQueue.peek();
            if (nextBatch != null && nextBatch.isStartOfNewBlock()) {
                if (nextBatch.newBlockNumber() != nextBlockToSend) {
                    // Exit this method if we fall behind, the next call will
                    // start by catching up from the historical provider.
                    break;
                }
            }
        }
    }

    /**
     * Trim the head of the block item queue if needed.
     * <p>
     * This is called after each block sent (whether live or historical).
     * The goal is to ensure that the queue doesn't become full, so after
     * each block sent, we try to ensure at least a minimum amount of
     * open capacity (controlled by {@link SubscriberConfig#minimumLiveQueueCapacity()})
     * is available for new live batches.
     * This may delay the point at which we "catch up" and return to streaming
     * live batches, but it also prevents most cases that would stall the
     * messaging threads.
     */
    private void trimBlockItemQueue(final long nextBlockNumber) {
        if (nextBlockNumber > UNKNOWN_BLOCK_NUMBER) {
            // peek at the head of the queue, if it's a header, and the number is later
            // than what we need to send next, and the queue is nearly full (less than
            // the minimum desired capacity is available), then we will remove
            // one or more _whole blocks_ from the queue (we must _never_ remove
            // a partial block).
            BlockItems queueHead = liveBlockQueue.peek();
            if (queueHead != null && queueHead.isStartOfNewBlock()) {
                // After this loop, the head of the queue must be the start of a
                // new block or empty (and it really _should_ never be empty).
                while (queueHead.newBlockNumber() > nextBlockNumber && queueFull()) {
                    removeHeadBlockFromQueue();
                    queueHead = liveBlockQueue.peek();
                }
            }
        }
    }

    private boolean queueFull() {
        return liveBlockQueue.remainingCapacity() <= pluginConfiguration.minimumLiveQueueCapacity();
    }

    /**
     * Remove the remainder of a block from the queue.
     * <p>
     * This method assumes that a possible header batch has already been removed
     * from the queue (and is provided). The provided item is checked, and if it
     * is a header block, the remainder of that block, up to the next header
     * batch (which might be the next item) is removed from the queue.<br/>
     * Note, the item provided and the next item are not removed from the queue,
     * so it is important to only call this method after polling and item, and
     * when this method returns, the next item in the queue will be the start of
     * a new block (or else the queue will be empty).
     */
    private void skipCurrentBlockInQueue(BlockItems queueHead) {
        // The "head" entry is already removed, remove the rest of its block if it's a block header.
        if (queueHead != null && queueHead.isStartOfNewBlock()) {
            queueHead = liveBlockQueue.peek();
            // Now remove "head" entries until the _next_ item is a block header
            while (queueHead != null && !(queueHead.isStartOfNewBlock())) {
                liveBlockQueue.poll();
                queueHead = liveBlockQueue.peek();
            }
        }
    }

    /**
     * Remove the head of the queue if, and only if, it's a full block.
     * <p>
     * This does much the same thing skipCurrentBlockInQueue does, but
     * we don't remove the head until we check whether it's a block header.<br/>
     * This method is called when _checking_ before removing a block, while
     * the skip method is used _after_ polling the head to _finish_ a block
     * that should be skipped.<br/>
     * Note, this method calls the skip after removing the head in order to
     * keep the skip logic consistent.
     */
    private void removeHeadBlockFromQueue() {
        BlockItems queueHead = liveBlockQueue.peek();
        // Remove the "head" entry if it's a block header.
        // Otherwise skip this, because we don't want to remove a partial block.
        if (queueHead != null && queueHead.isStartOfNewBlock()) {
            queueHead = liveBlockQueue.poll();
            skipCurrentBlockInQueue(queueHead);
        }
    }

    /**
     * Get the exception that caused this session to fail.
     *
     * This is package scope so that the plugin can read it.
     *
     * @return The exception that caused this session to fail
     */
    Exception getSessionFailedCause() {
        return sessionFailedCause;
    }

    /**
     * Validate the client subscribe request.
     * This will check the following:
     * 1. The start block number is greater than or equal to the "unknown" block number sentinel.
     * 2. The end block number is greater than or equal to the "unknown" block number sentinel.
     * 3. The end block number is _either_ the "unknown" sentinal, _or_  greater than or equal to
     *    the start block number.
     * 4. The start block number is _either_ the "unknown" sentinal, _or_ greater than or equal to
     *    the oldest block number.
     */
    private boolean validateRequest(
            final long oldestBlock,
            final long latestBlock,
            final long startBlock,
            final long endBlock,
            final long clientId,
            final Logger logger) {
        boolean isValid = false;
        if (startBlock < UNKNOWN_BLOCK_NUMBER) {
            logger.log(Level.DEBUG, "Client {0} requested negative block {1}", clientId, startBlock);
            close(SubscribeStreamResponse.Code.READ_STREAM_INVALID_START_BLOCK_NUMBER);
        } else if (endBlock < UNKNOWN_BLOCK_NUMBER) {
            logger.log(Level.DEBUG, "Client {0} requested negative end block {1}", clientId, endBlock);
            // send invalid end block number response
            close(SubscribeStreamResponse.Code.READ_STREAM_INVALID_END_BLOCK_NUMBER);
        } else if (endBlock >= 0 && startBlock > endBlock) {
            final String message = "Client {0} requested end block {1} before start {2}";
            logger.log(Level.DEBUG, message, clientId, endBlock, startBlock);
            // send invalid end block number response
            close(SubscribeStreamResponse.Code.READ_STREAM_INVALID_END_BLOCK_NUMBER);
        } else {
            final long lastPermittedStart = latestBlock + pluginConfiguration.maximumFutureRequest();
            if (startBlock != UNKNOWN_BLOCK_NUMBER && (startBlock < oldestBlock || startBlock > lastPermittedStart)) {
                // client has requested a block that is neither live nor available in history
                // _and_ will not be available within a reasonable number of future blocks.
                final String message =
                        "Client {0} requested start block {1} that is neither live nor historical. Newest historical block is {2}";
                logger.log(Level.DEBUG, message, clientId, startBlock, latestBlock);
                // send invalid start block number response
                close(SubscribeStreamResponse.Code.READ_STREAM_INVALID_START_BLOCK_NUMBER);
            } else {
                if (startBlock == UNKNOWN_BLOCK_NUMBER || startBlock >= latestBlock) {
                    // Start at next live block or a future block within the "max live" range.
                    logger.log(Level.TRACE, "Client {0} has started streaming live blocks", clientId);
                } else {
                    // we are starting at a block that is in the historical stream
                    final String message = "Client {0} has started streaming historical blocks from {1}";
                    logger.log(Level.TRACE, message, clientId, startBlock);
                }
                isValid = true;
            }
        }
        return isValid;
    }

    /**
     * Get the client id for this session
     *
     * @return The client id for this session
     */
    public long clientId() {
        return clientId;
    }

    @Override
    public String toString() {
        return handlerName;
    }

    // Visible for testing.
    long getNextBlockToSend() {
        return nextBlockToSend;
    }

    // Visible for testing.
    LiveBlockHandler getLiveBlockHandler() {
        return liveBlockHandler;
    }

    /**
     * Close this session. This will unregister us from the block messaging system and cancel the subscription.
     */
    synchronized void close(final SubscribeStreamResponse.Code endStreamResponseCode) {
        LOGGER.log(Level.TRACE, "Closing BlockStreamSubscriberSession for client {0}", clientId);
        // Might get here before the session is ready, so check the countdown latch
        if (sessionReadyLatch.getCount() > 0) {
            sessionReadyLatch.countDown();
            LOGGER.log(Level.DEBUG, "Session ready latch was not counted down on close, releasing now");
        }
        // unregister us from the block messaging system, if we are not registered then this is noop
        context.blockMessaging().unregisterBlockItemHandler(liveBlockHandler);
        final Builder response = SubscribeStreamResponseUnparsed.newBuilder().status(endStreamResponseCode);
        responsePipeline.onNext(response.build());
        responsePipeline.onComplete();
        if (subscription != null) {
            subscription.cancel();
            subscription = null;
        }
        // Break out of the loop that sends blocks to the client, so the thread completes.
        interruptedStream.set(true);
    }

    private void sendOneBlockItemSet(final BlockUnparsed nextBlock) throws ParseException {
        LOGGER.log(Level.TRACE, "Sending full block {0} to {1}", nextBlockToSend, handlerName);
        final BlockHeader header =
                BlockHeader.PROTOBUF.parse(nextBlock.blockItems().getFirst().blockHeader());
        if (header.number() == nextBlockToSend) {
            sendOneBlockItemSet(nextBlock.blockItems());
        } else {
            LOGGER.log(
                    Level.ERROR,
                    "Block {0} should be sent, but we are trying to send block {1}.",
                    nextBlockToSend,
                    header.number());
        }
    }

    private void sendOneBlockItemSet(final BlockItems nextBatch) {
        LOGGER.log(
                Level.TRACE,
                "Sending next batch of {0} items for block {1} to client {2}",
                nextBatch.blockItems().size(),
                nextBatch.newBlockNumber(),
                handlerName);
        sendOneBlockItemSet(nextBatch.blockItems());
    }

    private void sendOneBlockItemSet(final List<BlockItemUnparsed> blockItems) {
        final BlockItemSetUnparsed dataToSend = new BlockItemSetUnparsed(blockItems);
        final OneOf<ResponseOneOfType> responseOneOf = new OneOf<>(ResponseOneOfType.BLOCK_ITEMS, dataToSend);
        responsePipeline.onNext(new SubscribeStreamResponseUnparsed(responseOneOf));
        LOGGER.log(Level.TRACE, "Sent block items for block {0} to {1}.", nextBlockToSend, handlerName);
    }

    // ==== Block Item Handler Class ===========================================

    // NOTE: The methods of this class are called from the messaging threads.
    //       This means we must not modify any state in the session object directly.
    //       Instead we must use a transfer queue to pass the block items to the session
    //       and possibly set an atomic flag if we are "too far behind".
    static class LiveBlockHandler implements NoBackPressureBlockItemHandler {
        /** The logger for this class. */
        private final Logger LOGGER = System.getLogger(getClass().getName());

        private final BlockingQueue<BlockItems> liveBlockQueue;
        private final AtomicLong latestLiveStreamBlock;
        private final String clientId;

        private LiveBlockHandler(
                @NonNull final BlockingQueue<BlockItems> liveBlockQueue,
                @NonNull final AtomicLong latestLiveStreamBlock,
                final String clientId) {
            this.liveBlockQueue = requireNonNull(liveBlockQueue);
            this.latestLiveStreamBlock = requireNonNull(latestLiveStreamBlock);
            this.clientId = clientId;
        }

        @Override
        public void onTooFarBehindError() {
            // Insert a signal to the session that it is "too far behind" live.
            // Should not ever happen with this design.
            LOGGER.log(Level.INFO, "Handler for {0} has fallen behind.", clientId);
        }

        @Override
        public void handleBlockItemsReceived(@NonNull final BlockItems blockItems) {
            if (blockItems.newBlockNumber() > latestLiveStreamBlock.get()) {
                latestLiveStreamBlock.set(blockItems.newBlockNumber());
                LOGGER.log(Level.TRACE, "Updated latest block to {0}.", latestLiveStreamBlock);
            }
            // Blocking so that the client thread has a chance to pull items
            // off the head when it's full.
            try {
                liveBlockQueue.put(blockItems);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
