// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.stream.subscriber;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.TRACE;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.locks.LockSupport.parkNanos;
import static org.hiero.block.node.spi.BlockNodePlugin.UNKNOWN_BLOCK_NUMBER;

import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.pbj.runtime.OneOf;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.grpc.Pipeline;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.UncheckedIOException;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.hiero.block.api.BlockEnd;
import org.hiero.block.api.SubscribeStreamRequest;
import org.hiero.block.api.SubscribeStreamResponse;
import org.hiero.block.api.SubscribeStreamResponse.Code;
import org.hiero.block.internal.BlockItemSetUnparsed;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.internal.SubscribeStreamResponseUnparsed;
import org.hiero.block.internal.SubscribeStreamResponseUnparsed.Builder;
import org.hiero.block.internal.SubscribeStreamResponseUnparsed.ResponseOneOfType;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.NoBackPressureBlockItemHandler;
import org.hiero.block.node.spi.historicalblocks.BlockAccessor;
import org.hiero.block.node.spi.historicalblocks.BlockRangeSet;

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
 * synchronized.<p>
 * #### Request Validity<br/>
 * The request is validated when the session starts, if it is invalid, the session is closed immediately with an
 * appropriate error code.<br/>
 * A valid request is one where:
 * <ul>
 *     <li>The request start and end are both equal to -1L: request is for livestream, wherever it may be.</li>
 *     <li>The request start is -1L, and the end is greater or equal to 0L: request is for all blocks from the first available up to and including the end block.</li>
 *     <li>The request start is greater or equal to 0L, and the end is -1L: request is for all blocks from the start block onwards indefinitely.</li>
 *     <li>The request start is greater or equal to 0L, and the end is greater or equal to start: request is for all blocks from the start block up to and including the end block.</li>
 * </ul>
 * #### Fulfilling the Request<br/>
 * After validating the request, the session determines if it can fulfill the request. If it cannot, it closes
 * immediately with an appropriate error code.<br/>
 * In most cases, we need to know which is the first available block (this means a block that the node can provide
 * historically) and also the latest known block (the latest block the node can provide, be that from history, or
 * available in the live stream).<br/>
 * The first available block is needed mostly because we need to have a reference point as to what we can supply,
 * because if a request starts with block 10 for instance, and we have no blocks historically available, we cannot
 * determine if the first block available will be less than or equal to 10, thus we cannot determine if we can
 * fulfill the request. Such cases should be very rare.<br/>
 * The latest known block is mostly needed because a request might be with a start of a future block. This means that
 * the block node does not yet have the block available, be that in history or live stream. A request that is too far
 * into the future cannot be fulfilled.<br/>
 * A request can be fulfilled if:
 * <ul>
 *     <li>The request is for live blocks (start and end equal to -1L). This can always be fulfilled.</li>
 *     <li>The request is for start of -1L (first available), and end greater or equal to 0L, and the first available block is less than or equal to end.</li>
 *     <li>The request is for start greater or equal to 0L, and end equal to -1L, and the first available block is less than or equal to start, and start is within the permitted future range.</li>
 *     <li>The request is for start greater or equal to 0L, and end greater or equal to start, and the first available block is less than or equal to start, and start is within the permitted future range.</li>
 * </ul>
 */
public class BlockStreamSubscriberSession implements Callable<BlockStreamSubscriberSession> {
    /** The logger for this class. */
    private final Logger LOGGER = System.getLogger(getClass().getName());

    /** The sentinel value for the initial state of the next block to send and latest live stream */
    private static final long INITIAL_STATE_SENTINEL = UNKNOWN_BLOCK_NUMBER - 1L;
    /** The maximum time units to wait for a live block to be available */
    private static final long MAX_LIVE_POLL_DELAY = 500;
    /** The time unit for the maximum live poll delay */
    private static final TimeUnit LIVE_POLL_UNITS = TimeUnit.MILLISECONDS;
    /** The "resolution", in ns, to use for a polling delays. */
    private static final long PARK_DELAY_TIME = 100_000;

    /** The pipeline to send responses to the client */
    private final Pipeline<? super SubscribeStreamResponseUnparsed> responsePipeline;
    /** A blocking queue to send blocks from the live handler to the session. */
    private final BlockingQueue<BlockItems> liveBlockQueue;
    /** A lock to hold the pipeline thread until this session is ready */
    private final CountDownLatch sessionReadyLatch;
    /** A flag indicating if the session should be interrupted */
    private final AtomicBoolean interruptedStream = new AtomicBoolean(false);
    /** The latest block number seen in the live stream */
    private final AtomicLong latestLiveStreamBlock;
    /** The next block number to send to the client */
    private final AtomicLong nextBlockToSend;
    /** The context for this session */
    private final SessionContext sessionContext;
    /** The context of the block node */
    private final BlockNodeContext blockNodeContext;
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
     * @param sessionContext The context for this session
     * @param responsePipeline The pipeline to send responses to the client
     * @param context The context for the block node
     */
    public BlockStreamSubscriberSession(
            @NonNull final SessionContext sessionContext,
            @NonNull final Pipeline<? super SubscribeStreamResponseUnparsed> responsePipeline,
            @NonNull final BlockNodeContext context,
            @NonNull final CountDownLatch sessionReadyLatch) {
        this.responsePipeline = requireNonNull(responsePipeline);
        this.sessionReadyLatch = requireNonNull(sessionReadyLatch);
        this.blockNodeContext = requireNonNull(context);
        this.sessionContext = requireNonNull(sessionContext);
        this.latestLiveStreamBlock = new AtomicLong(INITIAL_STATE_SENTINEL);
        this.nextBlockToSend = new AtomicLong(INITIAL_STATE_SENTINEL);
        this.liveBlockQueue = new ArrayBlockingQueue<>(sessionContext.subscriberConfig.liveQueueSize());
        this.liveBlockHandler = new LiveBlockHandler(liveBlockQueue, latestLiveStreamBlock, sessionContext.handlerName);
    }

    /**
     * A simple record that consolidates the context for this session, including
     * request values and state values that are updated as the session
     * progresses.
     */
    record SessionContext(
            @NonNull SubscriberConfig subscriberConfig,
            @NonNull String handlerName,
            long clientId,
            long firstRequestedBlock,
            long lastRequestedBlock) {
        SessionContext(
                @NonNull final SubscriberConfig subscriberConfig,
                @NonNull final String handlerName,
                final long clientId,
                final long firstRequestedBlock,
                final long lastRequestedBlock) {
            this.subscriberConfig = requireNonNull(subscriberConfig);
            this.handlerName = requireNonNull(handlerName);
            this.clientId = clientId;
            this.firstRequestedBlock = firstRequestedBlock;
            this.lastRequestedBlock = lastRequestedBlock;
        }

        /**
         * This method creates a valid SessionContext from the provided
         * arguments.
         */
        static SessionContext create(
                final long clientId,
                @NonNull final SubscribeStreamRequest request,
                @NonNull final BlockNodeContext context) {
            final String handlerName = "Live stream client " + clientId;
            final SubscriberConfig subscriberConfig = context.configuration().getConfigData(SubscriberConfig.class);
            return new SessionContext(
                    subscriberConfig, handlerName, clientId, request.startBlockNumber(), request.endBlockNumber());
        }
    }

    @Override
    public BlockStreamSubscriberSession call() {
        try {
            // we have just started with a new subscribe request, now we need to validate the request
            if (validateRequest()) {
                // the request is valid, now we can subscribe to the live block stream
                // so we can determine if we can fulfill the request
                // register us to listen to block items from the block messaging system
                blockNodeContext
                        .blockMessaging()
                        .registerNoBackpressureBlockItemHandler(liveBlockHandler, false, sessionContext.handlerName);
                if (canFulfillRequest()) {
                    // the request can be fulfilled, so we can start sending blocks
                    sessionReadyLatch.countDown();
                    // Send blocks forever if requested, otherwise send until we reach the requested end block
                    // or the stream is interrupted.
                    while (!(interruptedStream.get() || allRequestedBlocksSent())) {
                        if (nextBlockToSend.get() < UNKNOWN_BLOCK_NUMBER) {
                            // This should never happen, if it does, this means that we have failed to set
                            // a value for the next block to send, this is most likely a failure in handling.
                            // Throwing will result in a catch and the session will be closed with the cause.
                            final String message = "Unexpected nextBlockToSend sentinel value: %d for session: %s"
                                    .formatted(nextBlockToSend.get(), sessionContext);
                            LOGGER.log(INFO, message);
                            close(Code.ERROR);
                        } else if (nextBlockToSend.get() == UNKNOWN_BLOCK_NUMBER) {
                            // In this case, we want to start live, the request
                            // was start and end = -1 and -1
                            // We expect that the next block to send will be set
                            // to the first complete live block we receive.
                            resolveLiveNextBlockToSend();
                            sendLiveBlocksIfAvailable();
                        } else {
                            // Note, nextBlockToSend is always the next block we need to send.
                            // start by sending from history if we can.
                            sendHistoricalBlocks();
                            // then process live blocks, if available.
                            sendLiveBlocksIfAvailable();
                        }
                    }
                }
                if (allRequestedBlocksSent()) {
                    // We've sent all request blocks. Therefore, we close, according to the protocol.
                    close(SubscribeStreamResponse.Code.SUCCESS);
                } else {
                    // We've failed to send all requested blocks.
                    close(SubscribeStreamResponse.Code.ERROR);
                }
            }
        } catch (final InterruptedException e) {
            sessionFailedCause = e;
            close(SubscribeStreamResponse.Code.SUCCESS);
            Thread.currentThread().interrupt();
        } catch (final RuntimeException | ParseException e) {
            sessionFailedCause = e;
            close(SubscribeStreamResponse.Code.ERROR);
        }
        // todo Need to record a metric here with client ID tag, so we can record
        //    requested vs sent metrics.
        return this;
    }

    /**
     * This method is used to resolve the next block to send when the request
     * is for live blocks
     * (i.e. both {@link SessionContext#firstRequestedBlock} and {@link SessionContext#lastRequestedBlock})
     * are {@link org.hiero.block.node.spi.BlockNodePlugin#UNKNOWN_BLOCK_NUMBER}.
     * <p>
     * In order to resolve the next block to send, we need to look at the
     * live block queue and find the next block, that is the start of a new
     * block. If we have subscribed somewhere in the middle of a block, (a
     * block could be streamed in multiple batches), we need to skip to the
     * start of the next block.
     * <br>
     * NOTE: This method is to be called only once, immediately after
     * determining that the request is for live blocks only!
     */
    private void resolveLiveNextBlockToSend() {
        while (nextBlockToSend.get() == UNKNOWN_BLOCK_NUMBER) {
            final BlockItems head = liveBlockQueue.peek();
            if (head != null) {
                if (!head.isStartOfNewBlock()) {
                    // We are in the middle of a block, so we need to
                    // skip to the start of the next block.
                    // We must keep repeating this until we
                    // find a start of block.
                    skipCurrentBlockInQueue(head);
                } else {
                    // We have found the start of a new block,
                    // so we can set the next block to send
                    // to this block number.
                    final long nextBlock = head.blockNumber();
                    nextBlockToSend.set(nextBlock);
                    final String message =
                            "Resolved next block to send for client %d which requested the live stream to %d";
                    LOGGER.log(TRACE, message.formatted(sessionContext.clientId, nextBlock));
                }
            } else {
                awaitNewLiveEntries();
            }
        }
    }

    private long getLatestKnownBlock() {
        return Math.max(getLatestHistoricalBlock(), latestLiveStreamBlock.get());
    }

    private long getLatestHistoricalBlock() {
        return blockNodeContext.historicalBlockProvider().availableBlocks().max();
    }

    /**
     * Validate the client subscribe request.
     * This will check the following:
     * 1. The start block number is greater than or equal to the "unknown" block number sentinel.
     * 2. The end block number is greater than or equal to the "unknown" block number sentinel.
     * 3. The end block number is _either_ the "unknown" sentinel, _or_  greater than or equal to
     *    the start block number.
     * NOTE: visible for testing
     */
    boolean validateRequest() {
        final long clientId = sessionContext.clientId;
        final long firstRequestedBlock = sessionContext.firstRequestedBlock;
        final long lastRequestedBlock = sessionContext.lastRequestedBlock;
        boolean isValid = false;
        if (firstRequestedBlock < UNKNOWN_BLOCK_NUMBER) {
            LOGGER.log(Level.DEBUG, "Client {0} requested negative block {1}", clientId, firstRequestedBlock);
            close(SubscribeStreamResponse.Code.INVALID_START_BLOCK_NUMBER);
        } else if (lastRequestedBlock < UNKNOWN_BLOCK_NUMBER) {
            LOGGER.log(Level.DEBUG, "Client {0} requested negative end block {1}", clientId, lastRequestedBlock);
            // send invalid end block number response
            close(SubscribeStreamResponse.Code.INVALID_END_BLOCK_NUMBER);
        } else if (lastRequestedBlock >= 0 && firstRequestedBlock > lastRequestedBlock) {
            final String message = "Client {0} requested end block {1} before start {2}";
            LOGGER.log(Level.DEBUG, message, clientId, lastRequestedBlock, firstRequestedBlock);
            // send invalid end block number response
            close(SubscribeStreamResponse.Code.INVALID_END_BLOCK_NUMBER);
        } else {
            isValid = true;
        }
        return isValid;
    }

    /**
     * A message to be logged when a request cannot be fulfilled.
     */
    private static final String CANNOT_FULFILL_MESSAGE =
            "Request of client %d: {start=%d, end=%d, firstAvailableBlock=%d, latestKnownBlock=%d, lastPermittedStart=%d} cannot be fulfilled";

    /**
     * Verify if we can fulfill the request.
     * To be called only after validating the request if the request is valid!
     * After validating the request, we need to determine if we can fulfill it.
     * A request can be fulfilled if:
     * 1. The request is for live blocks only (start and end = -1)
     * 2. The start is the "unknown" sentinel, the end is greater than the
     *    "unknown" sentinel and the first available block is less than or
     *    equal to the end.
     * 3. The start is greater than the "unknown" sentinel, and the last
     *    permitted start is greater than or equal to the start, AND one of:
     *    3.1. The end is the "unknown" sentinel, and the first available block
     *         is less than or equal to the start
     *    3.2. The end is greater than the "unknown" sentinel, and the first
     *         available block is less than or equal to the start
     * NOTE: visible for testing
     */
    boolean canFulfillRequest() {
        // We have already validated the request, we know that first and last
        // are valid as per the specification. Now we need to determine if we
        // are currently in a state where we can fulfill the request.
        final long firstRequestedBlock = sessionContext.firstRequestedBlock;
        final long lastRequestedBlock = sessionContext.lastRequestedBlock;
        final long clientId = sessionContext.clientId;
        final BlockRangeSet availableBlocks =
                blockNodeContext.historicalBlockProvider().availableBlocks();
        final long firstAvailableBlock = availableBlocks.min();
        final long latestKnownBlock = getLatestKnownBlock();
        final long lastPermittedStart = latestKnownBlock + sessionContext.subscriberConfig.maximumFutureRequest();
        Logger.Level logLevel = TRACE;
        String logMessage = CANNOT_FULFILL_MESSAGE.formatted(
                clientId,
                firstRequestedBlock,
                lastRequestedBlock,
                firstAvailableBlock,
                latestKnownBlock,
                lastPermittedStart);
        boolean canFulfillRequest = false;
        if (firstRequestedBlock == UNKNOWN_BLOCK_NUMBER && firstRequestedBlock == lastRequestedBlock) {
            // stream everything live, as soon as it arrives
            // set the next block to send to the UNKNOWN_BLOCK_NUMBER
            nextBlockToSend.set(UNKNOWN_BLOCK_NUMBER);
            canFulfillRequest = true;
            logMessage = "Client %d has started streaming live blocks".formatted(clientId);
        } else if (firstRequestedBlock == UNKNOWN_BLOCK_NUMBER && lastRequestedBlock > UNKNOWN_BLOCK_NUMBER) {
            // stream everything up to lastRequestedBlock as soon as it arrives
            // if the block node has no blocks, or the first available block is
            // greater than lastRequestedBlock, then we cannot fulfill this request
            if (firstAvailableBlock > UNKNOWN_BLOCK_NUMBER && firstAvailableBlock <= lastRequestedBlock) {
                nextBlockToSend.set(firstAvailableBlock);
                canFulfillRequest = true;
                logMessage = "Client %d has started streaming blocks from firstAvailable=%d up to %d"
                        .formatted(clientId, firstAvailableBlock, lastRequestedBlock);
            }
        } else if (firstRequestedBlock > UNKNOWN_BLOCK_NUMBER && firstRequestedBlock > lastPermittedStart) {
            // the request is valid, but start is too far in the future, we cannot fulfill it
            logLevel = DEBUG;
            logMessage = "Client %d requested start=%d that is too far in the future, lastPermittedStart=%d"
                    .formatted(clientId, firstRequestedBlock, lastPermittedStart);
        } else if (firstRequestedBlock > UNKNOWN_BLOCK_NUMBER && lastRequestedBlock == UNKNOWN_BLOCK_NUMBER) {
            // stream everything from firstRequestedBlock onwards forever
            // if the block node has no blocks, or the first available block is
            // greater than firstRequestedBlock, then we cannot fulfill this request
            if (firstAvailableBlock > UNKNOWN_BLOCK_NUMBER && firstAvailableBlock <= firstRequestedBlock) {
                nextBlockToSend.set(firstRequestedBlock);
                canFulfillRequest = true;
                logMessage = "Client %d has started streaming blocks from firstRequested=%d indefinitely"
                        .formatted(clientId, firstRequestedBlock);
            }
        } else if (firstRequestedBlock > UNKNOWN_BLOCK_NUMBER && lastRequestedBlock > UNKNOWN_BLOCK_NUMBER) {
            // stream everything from firstRequestedBlock to lastRequestedBlock
            // we only need to determine if we have the first requested block available
            // if the last requested block or blocks in the requested range are missing,
            // we will supply them live as they arrive
            if (firstRequestedBlock > lastRequestedBlock) {
                // this should never happen as the request should have been rejected in validation
                logLevel = INFO;
                logMessage =
                        "Client %d requested start=%d and end=%d, but start cannot be less than end when both are specified"
                                .formatted(clientId, firstRequestedBlock, lastRequestedBlock);
            } else if (firstAvailableBlock > UNKNOWN_BLOCK_NUMBER && firstAvailableBlock <= firstRequestedBlock) {
                nextBlockToSend.set(firstRequestedBlock);
                canFulfillRequest = true;
                logMessage =
                        "Client %d has started streaming blocks in range from firstRequested=%d to lastRequested=%d"
                                .formatted(clientId, firstRequestedBlock, lastRequestedBlock);
            }
        } else {
            // this should never happen as the request should have been rejected in validation
            logLevel = INFO;
            logMessage = "Unexpected case in canFulfillRequest: %s, firstAvailableBlock=%d"
                    .formatted(sessionContext, firstAvailableBlock);
            // @todo(1673) consider to set the code to close with to ERROR here instead of NOT_AVAILABLE
        }
        if (!canFulfillRequest) {
            close(SubscribeStreamResponse.Code.NOT_AVAILABLE);
        }
        LOGGER.log(logLevel, logMessage);
        return canFulfillRequest;
    }

    private void sendHistoricalBlocks() throws ParseException {
        // Don't send anything from history if this stream is interrupted, has sent
        // every requested block, or we can send the next block from "live".
        while (isHistoryPermitted()) {
            // We need to send historical blocks.
            // We will only send one block at a time to keep things "smooth".
            // Start by getting a block accessor for the next block to send from the historical provider.
            final BlockAccessor nextBlockAccessor =
                    blockNodeContext.historicalBlockProvider().block(nextBlockToSend.get());
            if (nextBlockAccessor != null) {
                final BlockUnparsed block = nextBlockAccessor.blockUnparsed();
                if (block == null) {
                    // the retrieval of the block failed.
                    final String message = "Unable to retrieve historical block {0} for client {1}.";
                    // throwing here will result in the session being closed exceptionally.
                    throw new IllegalStateException(message);
                } else {
                    // We have retrieved the block to send, so send it.
                    sendOneFullBlock(block);
                    // Trim the queue if necessary, also increment the next block to send.
                    trimBlockItemQueue(nextBlockToSend.incrementAndGet());
                }
            } else {
                // Only give up if this is an historical block, otherwise just
                // go back up and see if live has the block.
                if (!(nextBlockToSend.get() < 0 || nextBlockToSend.get() >= getLatestHistoricalBlock())) {
                    // We cannot get the block needed, something has failed.
                    // close the stream with an "unavailable" response.
                    final String message = "Unable to read historical block, nextBlockToSend={0}.";
                    LOGGER.log(Level.INFO, message, nextBlockToSend);
                    close(SubscribeStreamResponse.Code.NOT_AVAILABLE);
                } else {
                    awaitNewLiveEntries();
                }
                break;
            }
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
        return !(interruptedStream.get() || hasRunPastLatestLive() || allRequestedBlocksSent() || nextBatchIsLive());
    }

    /**
     * This method determines if we have run past the latest live block.
     * <p>
     * This is true if the latest live block is less than the next block to send
     * and the latest live block higher than the UNKNOWN_BLOCK_NUMBER sentinel.
     */
    private boolean hasRunPastLatestLive() {
        return latestLiveStreamBlock.get() < nextBlockToSend.get()
                && latestLiveStreamBlock.get() > UNKNOWN_BLOCK_NUMBER;
    }

    private boolean nextBatchIsLive() {
        final BlockItems queueHead = liveBlockQueue.peek();
        if (queueHead != null && latestLiveStreamBlock.get() >= nextBlockToSend.get()) {
            // @todo(1673) is the below expression correct?
            return !queueHead.isStartOfNewBlock() || queueHead.blockNumber() == nextBlockToSend.get();
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
        return sessionContext.lastRequestedBlock != UNKNOWN_BLOCK_NUMBER
                && nextBlockToSend.get() > sessionContext.lastRequestedBlock;
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
            // Peek at the block item from the queue and _possibly_ process it
            BlockItems blockItems = liveBlockQueue.peek();
            // Live _might_ be ahead or behind the next expected block (particularly if
            // the requested start block is in the future), don't send this block, in that case.
            // If behind, remove that item from the queue; if ahead leave it in the queue.
            final long clientId = sessionContext.clientId;
            if (blockItems.isStartOfNewBlock() && blockItems.blockNumber() < nextBlockToSend.get()) {
                LOGGER.log(Level.TRACE, "Skipping block {0} for client {1}", blockItems.blockNumber(), clientId);
                skipCurrentBlockInQueue(blockItems);
            } else if (blockItems.blockNumber()
                    == nextBlockToSend.get()) { // todo if block number from block items cannot be -1, we are good here
                blockItems = liveBlockQueue.poll();
                if (blockItems != null) {
                    sendOneBlockItemSet(blockItems, blockItems.isEndOfBlock());
                    if (blockItems.isEndOfBlock()) {
                        trimBlockItemQueue(nextBlockToSend.getAndIncrement());
                    }
                }
            } else if (blockItems.isStartOfNewBlock() && blockItems.blockNumber() > nextBlockToSend.get()) {
                // This block is _future_, so we need to wait, and try to get the next block from history
                // first, then come back to this block.
                LOGGER.log(
                        Level.TRACE, "Retaining future block {0} for client {1}", blockItems.blockNumber(), clientId);
            } else {
                // This is a past or future _partial_ block, so we need to trim the queue.
                liveBlockQueue.poll(); // discard _this batch only_.
                trimBlockItemQueue(nextBlockToSend.get());
            }
            // Note: We depend here on the rule that there are no gaps between blocks.
            //       The header for block N immediately follows the proof for block N-1.
            final BlockItems nextBatch = liveBlockQueue.peek();
            if (nextBatch != null && nextBatch.isStartOfNewBlock()) {
                if (nextBatch.blockNumber() != nextBlockToSend.get()) {
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
                while (queueHead != null && queueHead.blockNumber() > nextBlockNumber && queueFull()) {
                    removeHeadBlockFromQueue();
                    queueHead = liveBlockQueue.peek();
                }
            }
        }
    }

    private boolean queueFull() {
        return liveBlockQueue.remainingCapacity() <= sessionContext.subscriberConfig.minimumLiveQueueCapacity();
    }

    /**
     * Remove the remainder of a block from the queue.
     * <p>
     * This method assumes that a possible header batch has already been viewed
     * but not removed from the queue (and is provided). The provided item is
     * checked, and if it is a header block, it is removed and then the
     * remainder of that block, up to the next header batch (which might be the
     * next item) is removed from the queue.<br/>
     * Note, the item provided _may be_ removed from the queue (even if it's not
     * a block header), so it is important to only call this method after
     * peeking at the item without removing it, and when this method returns,
     * the next item in the queue will be the start of a new block (or else
     * the queue will be empty).
     */
    private void skipCurrentBlockInQueue(BlockItems queueHead) {
        // The "head" entry is _not_ already removed, remove it, and the rest of
        // its block. This also handles a partial block at the head of the queue
        // when we cannot process that block (e.g. it's in the future or past from
        // the block we currently need to send).
        if (queueHead != null) {
            liveBlockQueue.poll(); // remove the "head" entry
            // Now remove "head" entries until the _next_ item is a block header
            queueHead = liveBlockQueue.peek(); // peek at the next item.
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
     * This is package scope so that the plugin can read it.
     *
     * @return The exception that caused this session to fail
     */
    Exception getSessionFailedCause() {
        return sessionFailedCause;
    }

    /**
     * Get the client id for this session
     *
     * @return The client id for this session
     */
    public long clientId() {
        return sessionContext.clientId;
    }

    @Override
    public String toString() {
        return sessionContext.handlerName;
    }

    // Visible for testing.
    long getNextBlockToSend() {
        return nextBlockToSend.longValue();
    }

    /**
     * Close this session. This will unregister us from the block messaging system and cancel the subscription.
     */
    synchronized void close(final SubscribeStreamResponse.Code endStreamResponseCode) {
        LOGGER.log(Level.TRACE, "Closing BlockStreamSubscriberSession for client {0}", sessionContext.clientId);
        // Might get here before the session is ready, so check the countdown latch
        if (sessionReadyLatch.getCount() > 0) {
            sessionReadyLatch.countDown();
            LOGGER.log(Level.DEBUG, "Session ready latch was not counted down on close, releasing now");
        }
        // unregister us from the block messaging system, if we are not registered then this is noop
        blockNodeContext.blockMessaging().unregisterBlockItemHandler(liveBlockHandler);
        // send an end stream response, if we have a code to set and are not interrupted.
        if (!interruptedStream.get() && endStreamResponseCode != null) {
            try {
                // attempt to send the end stream response
                final Builder response =
                        SubscribeStreamResponseUnparsed.newBuilder().status(endStreamResponseCode);
                responsePipeline.onNext(response.build());
            } catch (final UncheckedIOException e) {
                // Unfortunately this is the "standard" way to end a stream, so log
                // at debug rather than emitting noise in the logs.
                // Also, this confuses everyone, they all see this debug log and
                // assume the node crashed, so we must not print a stack trace.
                final String messageFormat = "Client connection is already closed %d: %s";
                final String message = messageFormat.formatted(sessionContext.clientId, e.getMessage());
                LOGGER.log(Level.DEBUG, message, e);
            } catch (final RuntimeException e) {
                // If the response cannot be sent, log and suppress this exception.
                final String message = "Suppressed client error when sending end stream response for client %d%n%s";
                LOGGER.log(Level.DEBUG, message.formatted(sessionContext.clientId, e.getMessage()), e);
            }
        }
        try {
            responsePipeline.onComplete();
        } catch (final RuntimeException e) {
            // If the pipeline cannot be completed, log and suppress this exception.
            final String message = "Suppressed client error when \"completing\" stream for client %d%n%s";
            LOGGER.log(Level.DEBUG, message.formatted(sessionContext.clientId, e.getMessage()), e);
        }
        // Break out of the loop that sends blocks to the client, so the thread completes.
        interruptedStream.set(true);
    }

    private void sendOneFullBlock(final BlockUnparsed nextBlock) throws ParseException {
        if (nextBlock.blockItems().getFirst().hasBlockHeader()) {
            final BlockHeader header =
                    BlockHeader.PROTOBUF.parse(nextBlock.blockItems().getFirst().blockHeader());
            if (header.number() == nextBlockToSend.get()) {
                // We're sending a whole block, so this block is complete
                sendOneBlockItemSet(nextBlock.blockItems(), true);
            } else {
                final String message = "Block {0} should be sent, but we are trying to send block {1}.";
                LOGGER.log(Level.WARNING, message, nextBlockToSend.get(), header.number());
            }
        } else {
            final String message = "Block {0} should be sent, but the block does not start with a block header.";
            LOGGER.log(Level.WARNING, message, nextBlockToSend.get());
        }
    }

    private void sendOneBlockItemSet(final BlockItems nextBatch, final boolean blockComplete) {
        sendOneBlockItemSet(nextBatch.blockItems(), blockComplete);
    }

    private void sendOneBlockItemSet(final List<BlockItemUnparsed> blockItems, final boolean blockComplete) {
        final BlockItemSetUnparsed dataToSend = new BlockItemSetUnparsed(blockItems);
        final OneOf<ResponseOneOfType> responseOneOf = new OneOf<>(ResponseOneOfType.BLOCK_ITEMS, dataToSend);
        try {
            responsePipeline.onNext(new SubscribeStreamResponseUnparsed(responseOneOf));
            if (blockComplete) {
                sendEndOfBlock(nextBlockToSend.get());
            }
        } catch (UncheckedIOException e) {
            // Unfortunately this is the "standard" way to end a stream, so log
            // at debug rather than emitting noise in the logs.
            // Also, this confuses everyone, they all see this debug log and
            // assume the node crashed, so we must not print a stack trace.
            final String messageFormat = "Client closed the connection when sending block items for client %d: %s";
            final String message = messageFormat.formatted(sessionContext.clientId, e.getMessage());
            LOGGER.log(Level.DEBUG, message, e);
            close(null); // cannot send the end stream response, just close the stream.
        } catch (RuntimeException e) {
            // If the pipeline is in an error state; close this session.
            final String message = "Client error sending block items for client %d: %s"
                    .formatted(sessionContext.clientId, e.getMessage());
            LOGGER.log(Level.DEBUG, message, e);
            close(null); // cannot send the end stream response, just close the stream.
        }
    }

    private void sendEndOfBlock(final long blockNumber) {
        final SubscribeStreamResponseUnparsed.Builder builder = SubscribeStreamResponseUnparsed.newBuilder();
        responsePipeline.onNext(builder.endOfBlock(new BlockEnd(blockNumber)).build());
    }

    // ==== Block Item Handler Class ===========================================

    // NOTE: The methods of this class are called from the messaging threads.
    //       This means we must not modify any state in the session object directly.
    //       Instead we must use a transfer queue to pass the block items to the session
    //       and possibly set an atomic flag if we are "too far behind".
    private static class LiveBlockHandler implements NoBackPressureBlockItemHandler {
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
            // @todo(1673) consider to also add a check for:
            //    blockItems.blockNumber() > latestLiveStreamBlock.get() && blockItems.isStartOfNewBlock()
            //    because if this is not the start of a new block, the queue will be trimmed
            //    this should improve accuracy when determining if we can fulfill the request, but
            //    we should also be careful and think about how this change would affect the session
            //    as a whole and we need to ensure that such a check is correct and make amends
            //    elsewhere if needed.
            if (blockItems.blockNumber() > latestLiveStreamBlock.get()) {
                latestLiveStreamBlock.set(blockItems.blockNumber());
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
