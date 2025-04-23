// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.stream.subscriber;

import static org.hiero.block.node.spi.BlockNodePlugin.METRICS_CATEGORY;
import static org.hiero.block.node.spi.BlockNodePlugin.UNKNOWN_BLOCK_NUMBER;

import com.hedera.pbj.runtime.OneOf;
import com.hedera.pbj.runtime.grpc.Pipeline;
import com.swirlds.metrics.api.Counter;
import com.swirlds.metrics.api.Counter.Config;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.NoBackPressureBlockItemHandler;
import org.hiero.block.node.spi.historicalblocks.BlockAccessor;
import org.hiero.hapi.block.node.BlockItemSetUnparsed;
import org.hiero.hapi.block.node.BlockItemUnparsed;
import org.hiero.hapi.block.node.BlockUnparsed;
import org.hiero.hapi.block.node.SubscribeStreamRequest;
import org.hiero.hapi.block.node.SubscribeStreamResponseCode;
import org.hiero.hapi.block.node.SubscribeStreamResponseUnparsed;
import org.hiero.hapi.block.node.SubscribeStreamResponseUnparsed.Builder;
import org.hiero.hapi.block.node.SubscribeStreamResponseUnparsed.ResponseOneOfType;

/**
 * This class is used to represent a session for a single BlockStream subscriber that has connected to the block node.
 * At least to start with we will only support receiving a single SubscribeStreamRequest per session.
 * <p>
 * This session supports two primary modes of operation: live-streaming and historical streaming. Also switching between
 * on demand as needed.
 * <h2>Threading</h2>
 * This class is called from many threads from web server, the block messaging system and its own background historical
 * fetching thread. To make its state thread safe it uses synchronized methods around all entry points from other
 * threads.
 */
public class BlockStreamSubscriberSession implements Callable<BlockStreamSubscriberSession> {
    /** The logger for this class. */
    private final Logger LOGGER = System.getLogger(getClass().getName());

    /** The maximum number of live blocks to queue up in this session */
    private static final int MAX_LIVE_BLOCKS = 20;
    /** The maximum time units to wait for a live block to be available */
    private static final long MAX_LIVE_POLL_DELAY = 500;
    /** The time unit for the maximum live poll delay */
    private static final TimeUnit LIVE_POLL_UNITS = TimeUnit.MILLISECONDS;

    /** The client id for this session */
    private final long clientId;
    /** The first block number to stream */
    private final long startBlockNumber;
    /** The last block number to stream, can be {@value BlockNodePlugin#UNKNOWN_BLOCK_NUMBER } to mean infinite */
    private final long endBlockNumber;
    /** A flag indicating the client request allows unverified blocks */
    private final boolean allowUnverified;
    /** The pipeline to send responses to the client */
    private final Pipeline<? super SubscribeStreamResponseUnparsed> responsePipeline;
    /** The context for the block node */
    private final BlockNodeContext context;
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
    private final AtomicLong latestLiveStreamBlock = new AtomicLong(UNKNOWN_BLOCK_NUMBER - 1L);
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
    public BlockStreamSubscriberSession(long clientId, SubscribeStreamRequest request, Pipeline<? super SubscribeStreamResponseUnparsed> responsePipeline, BlockNodeContext context, final CountDownLatch sessionReadyLatch) {
        LOGGER.log(Level.TRACE, request.toString());
        this.clientId = clientId;
        this.startBlockNumber = request.startBlockNumber();
        this.endBlockNumber = request.endBlockNumber();
        this.responsePipeline = Objects.requireNonNull(responsePipeline);
        this.context = Objects.requireNonNull(context);
        this.allowUnverified = request.allowUnverified();
        this.sessionReadyLatch = Objects.requireNonNull(sessionReadyLatch);
        // Next
        nextBlockToSend = startBlockNumber < 0 ? getLatestKnownBlock() : startBlockNumber;
        // start _before_ any possible end block, and also before the start block.
        handlerName = "liveStream client " + clientId;
        // create metrics
        historicToLiveStreamTransitions = context.metrics().getOrCreate(new Config(METRICS_CATEGORY, "historicToLiveStreamTransitions").withDescription("Historic to Live Stream Transitions"));
        liveToHistoricStreamTransitions = context.metrics().getOrCreate(new Config(METRICS_CATEGORY, "liveToHistoricStreamTransitions").withDescription("Live to Historic Stream Transitions"));
        liveBlockQueue = new ArrayBlockingQueue<>(MAX_LIVE_BLOCKS);
        liveBlockHandler = new LiveBlockHandler(liveBlockQueue, latestLiveStreamBlock);
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
            if (validateRequest(oldestBlockNumber, latestBlockNumber, startBlockNumber, endBlockNumber, allowUnverified,
                    clientId, LOGGER)) {
                // register us to listen to block items from the block messaging system
                LOGGER.log(Level.TRACE, "Registering a block subscriber handler for " + handlerName);
                context.blockMessaging().registerNoBackpressureBlockItemHandler(liveBlockHandler, false, handlerName);
                sessionReadyLatch.countDown();
                final boolean isInfinteStream = endBlockNumber == UNKNOWN_BLOCK_NUMBER;
                // Send blocks forever if requested, otherwise send until we reach the requested end block.
                while (!interruptedStream.get() && (isInfinteStream || nextBlockToSend <= endBlockNumber)) {
                    processBlockItemsForClient(nextBlockToSend);
                }
            }
        }
        catch (RuntimeException | InterruptedException e) {
            sessionFailedCause = e;
            interruptedStream.set(true);
        }
        // Need to record a metric here with client ID tag, so we can record
        // requested vs sent metrics.
        return this;
    }

    /**
     * Process blocks to send to the client.
     * <p>
     * Note, this method will "catch up" from a historical block provider if
     *     the "live" block items are "ahead" of the current block to be sent.
     *
     * @param nextBlockToSend The next block number to send to the client
     *
     * @return the next block to send _after_ this method has completed.
     *
     * @throws InterruptedException if the thread is interrupted while waiting for a "live" batch.
     */
    private long processBlockItemsForClient(long nextBlockToSend) throws InterruptedException {
        if (haveLiveBlock(nextBlockToSend)) {
            while (!liveBlockQueue.isEmpty()) {
                // take the block item from the queue and process it
                final BlockItems blockItems = liveBlockQueue.poll(MAX_LIVE_POLL_DELAY, LIVE_POLL_UNITS);
                if (blockItems != null) {
                }
            }
        }
        // check if blockItems is start of new block, and we have an end block number to check against
        final long itemsBlockNumber = blockItems.newBlockNumber();
        if (itemsBlockNumber != UNKNOWN_BLOCK_NUMBER && blockItems.isStartOfNewBlock()) {
            if (itemsBlockNumber > nextBlockToSend) {
                removeHeadBlockFromQueue();
                // @todo a couple issues here.
                //       1. if the block isn't available from the provider, it might be available "live".
                //       2. The "live" _batch_ might have been removed or following batches _in the same block_
                //          might have been removed.
                while(nextBlockToSend < itemsBlockNumber) {
                    // We are behind, so we need to request the missing block(s) from history.
                    BlockAccessor nextBlockAccessor = context.historicalBlockProvider().block(nextBlockToSend);
                    if (nextBlockAccessor != null) {
                        // We have a block to send, so send it.
                        sendOneBlockItemSet(nextBlockAccessor.blockUnparsed());
                    } else {
                        // We cannot get the block needed, something has failed.
                        // close the stream with an "unavailable" response.
                        final String message = "Unable to read historical block {0}.";
                        LOGGER.log(Level.INFO, message, nextBlockToSend);
                        close(SubscribeStreamResponseCode.READ_STREAM_NOT_AVAILABLE);
                        return UNKNOWN_BLOCK_NUMBER;
                    }
                }
            }
            // no else here; after we catch up, send the new block as well.
            if(itemsBlockNumber == nextBlockToSend) {
                // Increment the next block to send at the end of each block.
                if (blockItems.isEndOfBlock()) {
                    nextBlockToSend++;
                }
                // check if we have got past the end block number and should stop streaming
                if (endBlockNumber != UNKNOWN_BLOCK_NUMBER && blockItems.newBlockNumber() > endBlockNumber) {
                    LOGGER.log(Level.TRACE, "Client {0} has reached end block number {1}", clientId, endBlockNumber);
                    // send end of stream response
                    close(SubscribeStreamResponseCode.READ_STREAM_SUCCESS);
                    return;
                }
                // send the block items to the client
                sendOneBlockItemSet(blockItems.blockItems());
            } else {
                // The requested start is ahead of live, just wait for live to catch up.
                final String message = "Subscriber session %s tried to send block %,(d but next block to send is %,(d.";
                LOGGER.log(Level.TRACE, message.formatted(clientId, itemsBlockNumber, nextBlockToSend));
            }
        } else {
            if (itemsBlockNumber != UNKNOWN_BLOCK_NUMBER && itemsBlockNumber != nextBlockToSend) {
                // This block item set is not the same block as we're sending, but also not
                // the start of a new block. Something went very wrong here.
                close(SubscribeStreamResponseCode.READ_STREAM_NOT_AVAILABLE);
                return;
            }
            // We have a block item that is not the start of a new block, but _is_ for the
            // current block, so send it.
            sendOneBlockItemSet(blockItems.blockItems());
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
     * Check that there is block data available on the queue.
     * This returns true IFF the following are true.
     *     1. The queue is not empty
     *     2. The head of the queue is not null
     *     3. The latest block number is greater than or equal to the next block to send.
     */
    private boolean haveLiveBlock(final long nextBlockToSend) {
        return liveBlockQueue.peek() != null && latestLiveStreamBlock.get() >= nextBlockToSend;
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
    private boolean validateRequest(final long oldestBlock, final long latestBlock, final long startBlock, final long endBlock, final boolean unverified, final long clientId, final Logger logger) {
        boolean isValid = false;
        if (!unverified) {
            logger.log(Level.DEBUG, "Client {0} requested a validated stream but this is not supported", clientId);
            // send end stream response to client
            close(SubscribeStreamResponseCode.READ_STREAM_NOT_AVAILABLE);
        } else if (startBlock < UNKNOWN_BLOCK_NUMBER) {
            logger.log(Level.DEBUG, "Client {0} requested negative block {1}", clientId, startBlock);
            close(SubscribeStreamResponseCode.READ_STREAM_INVALID_START_BLOCK_NUMBER);
        } else if (endBlock < UNKNOWN_BLOCK_NUMBER) {
            logger.log(Level.DEBUG, "Client {0} requested negative end block {1}", clientId, endBlock);
            // send invalid end block number response
            close(SubscribeStreamResponseCode.READ_STREAM_INVALID_END_BLOCK_NUMBER);
        } else if (endBlock >= 0 && startBlock > endBlock) {
            final String message = "Client {0} requested end block {1} before start {2}";
            logger.log(Level.DEBUG, message, clientId, endBlock, startBlock);
            // send invalid end block number response
            close(SubscribeStreamResponseCode.READ_STREAM_INVALID_END_BLOCK_NUMBER);
        } else if (startBlock != UNKNOWN_BLOCK_NUMBER && (startBlock < oldestBlock || startBlock > (latestBlock + MAX_LIVE_BLOCKS))) {
            // client has requested a block that is neither live nor available in history
            final String message = "Client {0} requested start block {1} that is neither live nor historical. Newest historical block is {2}";
            logger.log(Level.DEBUG, message, clientId, startBlock, latestBlock);
            // send invalid start block number response
            close(SubscribeStreamResponseCode.READ_STREAM_INVALID_START_BLOCK_NUMBER);
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
    synchronized void close(final SubscribeStreamResponseCode endStreamResponseCode) {
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

    private void removeHeadBlockFromQueue() {
        // Remove the "head" entry if it's a block header.
        if (liveBlockQueue.peek() != null && liveBlockQueue.peek().isStartOfNewBlock()) {
            liveBlockQueue.poll();
        } else {
            // only remove whole blocks, never partial.
            return;
        }
        // Now remove "head" entries until the _next_ item is a block header
        while (liveBlockQueue.peek() != null && !(liveBlockQueue.peek().isStartOfNewBlock())) {
            liveBlockQueue.poll();
        }
    }

    private void sendOneBlockItemSet(final BlockUnparsed nextBlock) {
        sendOneBlockItemSet(nextBlock.blockItems());
    }

    private void sendOneBlockItemSet(final List<BlockItemUnparsed> blockItems) {
        final BlockItemSetUnparsed dataToSend = new BlockItemSetUnparsed(blockItems);
        final OneOf<ResponseOneOfType> responseOneOf = new OneOf<>(ResponseOneOfType.BLOCK_ITEMS, dataToSend);
        responsePipeline.onNext(new SubscribeStreamResponseUnparsed(responseOneOf));
    }

    // ==== Block Item Handler Class ===========================================

    // NOTE: The methods of this class are called from the messaging threads.
    //       This means we must not modify any state in the session object directly.
    //       Instead we must use a transfer queue to pass the block items to the session
    //       and possibly set an atomic flag if we are "too far behind".
    private static class LiveBlockHandler implements NoBackPressureBlockItemHandler {
        private final BlockingQueue<BlockItems> liveBlockQueue;
        private final AtomicLong latestLiveStreamBlock;

        private LiveBlockHandler(final BlockingQueue<BlockItems> liveBlockQueue, final AtomicLong latestLiveStreamBlock) {
            this.liveBlockQueue = liveBlockQueue;
            this.latestLiveStreamBlock = latestLiveStreamBlock;
        }

        @Override
        public void onTooFarBehindError() {
            // Insert a signal to the session that it is "too far behind" live.
            // Should not ever happen with this design.
        }

        @Override
        public void handleBlockItemsReceived(final BlockItems blockItems) {
            if (blockItems.newBlockNumber() > latestLiveStreamBlock.get()) {
                latestLiveStreamBlock.set(blockItems.newBlockNumber());
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
