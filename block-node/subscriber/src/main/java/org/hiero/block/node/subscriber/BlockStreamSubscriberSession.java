// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.subscriber;

import static org.hiero.block.node.spi.BlockNodePlugin.METRICS_CATEGORY;
import static org.hiero.block.node.spi.BlockNodePlugin.UNKNOWN_BLOCK_NUMBER;

import com.hedera.pbj.runtime.OneOf;
import com.hedera.pbj.runtime.grpc.Pipeline;
import com.swirlds.metrics.api.Counter;
import java.lang.System.Logger.Level;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import org.hiero.block.api.SubscribeStreamRequest;
import org.hiero.block.api.SubscribeStreamResponse.Code;
import org.hiero.block.internal.BlockItemSetUnparsed;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.internal.SubscribeStreamResponseUnparsed;
import org.hiero.block.internal.SubscribeStreamResponseUnparsed.ResponseOneOfType;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.NoBackPressureBlockItemHandler;
import org.hiero.block.node.spi.historicalblocks.BlockAccessor;

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
public class BlockStreamSubscriberSession implements NoBackPressureBlockItemHandler {
    /** The logger for this class. */
    private final System.Logger LOGGER = System.getLogger(getClass().getName());

    /** Enum for the state of a block source */
    public enum SubscriberState {
        NEW,
        SENDING_LIVE_STREAM_WAITING_FOR_ANY_BLOCK_START,
        SENDING_LIVE_STREAM_WAITING_FOR_EXPLICIT_BLOCK_START,
        SENDING_LIVE_STREAM,
        SENDING_HISTORICAL_STREAM,
        DISCONNECTED
    }

    /** The client id for this session */
    private final long clientId;
    /** The pipeline to send responses to the client */
    private final Pipeline<? super SubscribeStreamResponseUnparsed> responsePipeline;
    /** The context for the block node */
    private final BlockNodeContext context;
    /** The callback to call when this session is closed */
    private final Consumer<BlockStreamSubscriberSession> closeCallback;
    /** The number of historic to live stream transitions metric */
    private final Counter historicToLiveStreamTransitions;
    /** The number of live to historic stream transitions metric */
    private final Counter liveToHistoricStreamTransitions;
    /** The subscription for the GRPC connection with client */
    private Flow.Subscription subscription;
    /** The current state of the subscriber, i.e. state machine state */
    private SubscriberState currentState = SubscriberState.NEW;
    /** The thread for the historical stream */
    private Thread historicalStreamThread;
    /** The current block being sent to the client */
    private final AtomicLong currentBlockBeingSent = new AtomicLong(UNKNOWN_BLOCK_NUMBER);
    /** The latest block received from the live stream, start at crazy negative so more than 10 away from block 0 */
    private final AtomicLong latestLiveStreamBlock = new AtomicLong(Long.MIN_VALUE);
    /** The barrier to hold the live stream until we are ready to switch over from historic */
    private final AtomicReference<CountDownLatch> holdLiveStreamBarrier = new AtomicReference<>(null);
    /** The block number of the block that the live stream is waiting on */
    private long blockLiveStreamIsWaitingReadyToSend = UNKNOWN_BLOCK_NUMBER;
    /** The first block number to stream */
    private final long startBlockNumber;
    /** The last block number to stream, can be Long.MAX_VALUE to mean infinite */
    private final long endBlockNumber;

    private final String handlerName;

    /**
     * Constructor for the BlockStreamSubscriberSession class.
     *
     * @param clientId The client id for this session
     * @param responsePipeline The pipeline to send responses to the client
     * @param context The context for the block node
     */
    public BlockStreamSubscriberSession(
            long clientId,
            SubscribeStreamRequest request,
            Pipeline<? super SubscribeStreamResponseUnparsed> responsePipeline,
            BlockNodeContext context,
            Consumer<BlockStreamSubscriberSession> closeCallback) {
        LOGGER.log(Level.ERROR, request.toString());
        this.clientId = clientId;
        this.startBlockNumber = request.startBlockNumber();
        this.endBlockNumber =
                request.endBlockNumber() == UNKNOWN_BLOCK_NUMBER ? Long.MAX_VALUE : request.endBlockNumber();
        this.responsePipeline = responsePipeline;
        this.context = context;
        this.closeCallback = closeCallback;
        handlerName = "liveStream client " + clientId;
        // create metrics
        historicToLiveStreamTransitions = context.metrics()
                .getOrCreate(new Counter.Config(METRICS_CATEGORY, "historicToLiveStreamTransitions")
                        .withDescription("Historic to Live Stream Transitions"));
        liveToHistoricStreamTransitions = context.metrics()
                .getOrCreate(new Counter.Config(METRICS_CATEGORY, "liveToHistoricStreamTransitions")
                        .withDescription("Live to Historic Stream Transitions"));
        // get latest available blocks
        final long oldestBlockNumber =
                context.historicalBlockProvider().availableBlocks().min();
        final long latestBlockNumber =
                context.historicalBlockProvider().availableBlocks().max();
        // we have just received a new subscribe request, now we need to work out if we can service it
        if (!request.allowUnverified()) {
            // ----------> VALIDATED STREAM <----------
            LOGGER.log(Level.DEBUG, "Client {0} requested a validated stream but this is not supported", clientId);
            // send response to client
            responsePipeline.onNext(SubscribeStreamResponseUnparsed.newBuilder()
                    .status(Code.READ_STREAM_NOT_AVAILABLE)
                    .build());
            close();
        } else if (request.startBlockNumber() < UNKNOWN_BLOCK_NUMBER) {
            // ----------> NEGATIVE START AND NOT UNKNOWN_BLOCK_NUMBER <----------
            LOGGER.log(Level.DEBUG, "Client {0} requested negative block {1}", clientId, request.startBlockNumber());
            // send invalid start block number response
            responsePipeline.onNext(SubscribeStreamResponseUnparsed.newBuilder()
                    .status(Code.READ_STREAM_INVALID_START_BLOCK_NUMBER)
                    .build());
            close();
        } else if (request.endBlockNumber() < UNKNOWN_BLOCK_NUMBER) {
            // ----------> NEGATIVE END AND NOT UNKNOWN_BLOCK_NUMBER <----------
            LOGGER.log(Level.DEBUG, "Client {0} requested negative end block {1}", clientId, request.endBlockNumber());
            // send invalid end block number response
            responsePipeline.onNext(SubscribeStreamResponseUnparsed.newBuilder()
                    .status(Code.READ_STREAM_INVALID_END_BLOCK_NUMBER)
                    .build());
            close();
        } else if (request.endBlockNumber() >= 0 && startBlockNumber > endBlockNumber) {
            // ----------> END BEFORE START <----------
            LOGGER.log(
                    Level.DEBUG,
                    "Client {0} requested end block {1} before start {2}",
                    clientId,
                    request.endBlockNumber(),
                    request.startBlockNumber());
            // send invalid end block number response
            responsePipeline.onNext(SubscribeStreamResponseUnparsed.newBuilder()
                    .status(Code.READ_STREAM_INVALID_END_BLOCK_NUMBER)
                    .build());
            close();
        } else if (startBlockNumber != UNKNOWN_BLOCK_NUMBER
                && (startBlockNumber < oldestBlockNumber || startBlockNumber > (latestBlockNumber + 30))) {
            // client has requested a block that is neither live nor available historical
            LOGGER.log(
                    Level.DEBUG,
                    "Client {0} requested start block {1} that is neither live nor "
                            + "historical. Newest historical block is {2}",
                    clientId,
                    startBlockNumber,
                    context.historicalBlockProvider().availableBlocks().max());
            // send invalid start block number response
            responsePipeline.onNext(SubscribeStreamResponseUnparsed.newBuilder()
                    .status(Code.READ_STREAM_INVALID_START_BLOCK_NUMBER)
                    .build());
            close();
        } else {
            // ----------> VALID REQUEST EITHER LATEST LIVE OR HISTORIC <----------
            if (startBlockNumber == UNKNOWN_BLOCK_NUMBER || startBlockNumber == latestBlockNumber) {
                // ----------> START AT ANY BLOCK <----------
                currentState = SubscriberState.SENDING_LIVE_STREAM_WAITING_FOR_ANY_BLOCK_START;
                LOGGER.log(Level.TRACE, "Client {0} has started streaming live blocks", clientId);
            } else if (startBlockNumber > latestBlockNumber) {
                // ----------> SHORTLY AHEAD OF CURRENT BLOCK <----------
                // they requested a block that is not yet available, but should be shortly, so we need to hold the
                // live stream till we get to that block
                currentState = SubscriberState.SENDING_LIVE_STREAM_WAITING_FOR_EXPLICIT_BLOCK_START;
                LOGGER.log(
                        Level.TRACE,
                        "Client {0} has started streaming, waiting for block {1}",
                        clientId,
                        startBlockNumber);
            } else {
                // ----------> HISTORICAL STREAM <----------
                // we are starting at a block that is in the historical stream, so we need to start the historical
                // stream
                currentState = SubscriberState.SENDING_HISTORICAL_STREAM;
                LOGGER.log(
                        Level.TRACE,
                        "Client {0} has started streaming historical blocks from {1}",
                        clientId,
                        startBlockNumber);
                // start the historical stream
                sendHistoricalStream(startBlockNumber, endBlockNumber);
            }
            // Important, register this _after_ determining initial state and setting the currentState value.
            // register us to listen to block items from the block messaging system
            LOGGER.log(Level.TRACE, "Registering a block subscriber handler for " + handlerName);
            context.blockMessaging().registerNoBackpressureBlockItemHandler(this, false, handlerName);
        }
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
    synchronized boolean isHistoryStreamActive() {
        return historicalStreamThread != null && holdLiveStreamBarrier.get() != null;
    }

    // Visible for testing.
    long getCurrentBlockBeingSent() {
        return currentBlockBeingSent.get();
    }

    /**
     * Close this session. This will unregister us from the block messaging system and cancel the subscription.
     */
    public synchronized void close() {
        LOGGER.log(Level.TRACE, "Closing BlockStreamSubscriberSession for client {0}", clientId);
        closeCallback.accept(this);
        currentState = SubscriberState.DISCONNECTED;
        // unregister us from the block messaging system, if we are not registered then this is noop
        context.blockMessaging().unregisterBlockItemHandler(this);
        if (subscription != null) {
            subscription.cancel();
            subscription = null;
        }
        if (historicalStreamThread != null) {
            historicalStreamThread.interrupt();
            historicalStreamThread = null;
        }
    }

    /**
     * Process the block items to send to client, unless we are starting a block after the last block the items will be
     * sent to the client. If block is after the end block number then we will send an end of stream response and close
     * the session.
     *
     * @param blockItems The block items to send to the client
     */
    private synchronized void processBlockItemsForClient(final BlockItems blockItems) {
        // check if blockItems is start of new block, and we have an end block number to check against
        if (blockItems.newBlockNumber() != UNKNOWN_BLOCK_NUMBER) {
            currentBlockBeingSent.set(blockItems.newBlockNumber());
            // check if we have got past the end block number and should stop streaming
            if (endBlockNumber != UNKNOWN_BLOCK_NUMBER && blockItems.newBlockNumber() > endBlockNumber) {
                LOGGER.log(Level.TRACE, "Client {0} has reached end block number {1}", clientId, endBlockNumber);
                // send end of stream response
                responsePipeline.onNext(SubscribeStreamResponseUnparsed.newBuilder()
                        .status(Code.READ_STREAM_SUCCESS)
                        .build());
                close();
                return;
            }
        }
        // send the block items to the client
        responsePipeline.onNext(new SubscribeStreamResponseUnparsed(
                new OneOf<>(ResponseOneOfType.BLOCK_ITEMS, new BlockItemSetUnparsed(blockItems.blockItems()))));
    }

    // ==== Historical Streaming Thread Methods ========================================================================

    /**
     * Create a background thread to read historical blocks from the historical block provider and send them to the
     * client. Switching over to live-streaming when we reach the live stream block.
     *
     * @param startBlockNumber The block number to start from, never UNKNOWN_BLOCK_NUMBER
     * @param endBlockNumberRaw The block number to end, UNKNOWN_BLOCK_NUMBER if unbounded
     */
    private synchronized void sendHistoricalStream(long startBlockNumber, long endBlockNumberRaw) {
        final long endBlockNumber = endBlockNumberRaw == UNKNOWN_BLOCK_NUMBER ? Long.MAX_VALUE : endBlockNumberRaw;
        if (historicalStreamThread == null) {
            BlockStreamSubscriberSession owningSession = this;
            historicalStreamThread = new HistoricalStreamThread(this, startBlockNumber, endBlockNumber);
            historicalStreamThread.start();
        }
    }

    // ==== Block Item Handler Methods =================================================================================

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void onTooFarBehindError() {
        switch (currentState) {
            case SENDING_LIVE_STREAM -> {
                // we have fallen too far behind the live stream, switch to historical stream
                LOGGER.log(Level.DEBUG, "Client {0} has fallen too far behind the live stream", clientId);
                currentState = SubscriberState.SENDING_HISTORICAL_STREAM;
                liveToHistoricStreamTransitions.increment();
                sendHistoricalStream(currentBlockBeingSent.get() + 1, endBlockNumber);
            }
            case SENDING_HISTORICAL_STREAM -> {
                // we have caught up to the live stream, so reset barriers and register the handler.
                LOGGER.log(
                        Level.DEBUG,
                        "Client {0} has fallen too far behind, while awaiting joining from the historical stream",
                        clientId);
                if (holdLiveStreamBarrier.get() != null) {
                    // release the barrier
                    holdLiveStreamBarrier.get().countDown();
                    holdLiveStreamBarrier.set(null);
                    blockLiveStreamIsWaitingReadyToSend = UNKNOWN_BLOCK_NUMBER;
                }
                // reconnect to the live stream
                context.blockMessaging().registerBlockItemHandler(this, false, handlerName);
            }
        }
    }

    /**
     * {@inheritDoc}
     *
     * Called when we receive block items from the block messaging system.
     */
    @Override
    public void handleBlockItemsReceived(BlockItems blockItems) {
        synchronized (this) {
            // if we get the start of a block then update the latest live stream block
            if (blockItems.isStartOfNewBlock()) {
                latestLiveStreamBlock.set(blockItems.newBlockNumber());
            }
            switch (currentState) {
                case SENDING_LIVE_STREAM_WAITING_FOR_ANY_BLOCK_START -> {
                    if (blockItems.isStartOfNewBlock()) {
                        // we have received a block, so we can start streaming
                        LOGGER.log(
                                Level.TRACE,
                                "Client {0} has started streaming live blocks from {1}",
                                clientId,
                                blockItems.newBlockNumber());
                        currentState = SubscriberState.SENDING_LIVE_STREAM;
                        currentBlockBeingSent.set(blockItems.newBlockNumber());
                        // send the block items to the client
                        processBlockItemsForClient(blockItems);
                    } else {
                        // we have not received a block yet, so ignore the block items
                        LOGGER.log(Level.TRACE, "Client {0} is waiting for start of new block", clientId);
                    }
                }
                case SENDING_LIVE_STREAM_WAITING_FOR_EXPLICIT_BLOCK_START -> {
                    // handle cases where the client is waiting for a future block
                    if (latestLiveStreamBlock.get() > startBlockNumber) {
                        // we have started a block newer and have failed to start streaming so return error to client
                        LOGGER.log(
                                Level.TRACE,
                                "Client {0} has started streaming live blocks from {1} but we are waiting for {2} "
                                        + "which is now in the past",
                                clientId,
                                latestLiveStreamBlock.get(),
                                startBlockNumber);
                        responsePipeline.onNext(SubscribeStreamResponseUnparsed.newBuilder()
                                .status(Code.READ_STREAM_INVALID_START_BLOCK_NUMBER)
                                .build());
                        close();
                    } else if (latestLiveStreamBlock.get() == startBlockNumber) {
                        // we have received the start block, so we can start streaming
                        LOGGER.log(
                                Level.TRACE,
                                "Client {0} has started streaming live blocks from {1}",
                                clientId,
                                blockItems.newBlockNumber());
                        currentState = SubscriberState.SENDING_LIVE_STREAM;
                        currentBlockBeingSent.set(blockItems.newBlockNumber());
                        // send the block items to the client
                        processBlockItemsForClient(blockItems);
                    } else {
                        // we have not received a block yet, so ignore the block items
                        LOGGER.log(
                                Level.TRACE,
                                "Client {0} is waiting for start of block {1}",
                                clientId,
                                startBlockNumber);
                    }
                }
                case SENDING_LIVE_STREAM -> processBlockItemsForClient(blockItems);
                case SENDING_HISTORICAL_STREAM -> {
                    // check if the historical sending has caught up enough that it is within 10 blocks of the live
                    // stream. if so we will hold the live stream at the barrier and wait for the historical stream to
                    // switch over
                    final long differenceFromLive =
                            latestLiveStreamBlock.get() - Math.max(0, currentBlockBeingSent.get());
                    if (differenceFromLive > 0 && differenceFromLive < 10 && blockItems.isStartOfNewBlock()) {
                        // we are waiting for the historical stream to catch up, so get ready to
                        // block until we are ready to send the live stream. Don't just block here
                        // as we are in a synchronized block, and we will cause a deadlock.
                        holdLiveStreamBarrier.set(new CountDownLatch(1));
                        blockLiveStreamIsWaitingReadyToSend = blockItems.newBlockNumber();
                    }
                }
            }
        }
        // block if needed, done outside the synchronized block to avoid deadlock
        if (holdLiveStreamBarrier.get() != null) {
            try {
                LOGGER.log(
                        Level.DEBUG,
                        "Client {0} block item listener is waiting for historical stream to "
                                + "catch up to live stream. Current block {1} and live stream is at {2}",
                        clientId,
                        currentBlockBeingSent.get(),
                        latestLiveStreamBlock.get());
                holdLiveStreamBarrier.get().await();
                // we are now past the barrier, so we can send new block items to client and switch to live-streaming
                synchronized (this) {
                    currentState = SubscriberState.SENDING_LIVE_STREAM;
                    currentBlockBeingSent.set(blockItems.newBlockNumber());
                    processBlockItemsForClient(blockItems);
                }
            } catch (InterruptedException e) {
                LOGGER.log(
                        Level.INFO,
                        "Interrupted waiting on barrier in BlockStreamSubscriberSession for client {1}",
                        clientId,
                        e);
                Thread.currentThread().interrupt();
            }
        }
    }

    private static class HistoricalStreamThread extends Thread {
        private final BlockStreamSubscriberSession owningSession;
        private final long startBlockNumber;
        private final long endBlockNumber;

        public HistoricalStreamThread(
                BlockStreamSubscriberSession owningSession, long startBlockNumber, long endBlockNumber) {
            this.owningSession = owningSession;
            this.startBlockNumber = startBlockNumber;
            this.endBlockNumber = endBlockNumber;
        }

        @Override
        public void run() {
            try {
                for (long i = startBlockNumber;
                        i <= endBlockNumber && !Thread.currentThread().isInterrupted();
                        i++) {
                    // check if live stream is waiting ready for us at the barrier
                    if (owningSession.holdLiveStreamBarrier.get() != null) {
                        synchronized (owningSession) {
                            if (i == owningSession.blockLiveStreamIsWaitingReadyToSend) {
                                // release the barrier
                                owningSession.holdLiveStreamBarrier.get().countDown();
                                owningSession.holdLiveStreamBarrier.set(null);
                                owningSession.historicToLiveStreamTransitions.increment();
                                // stop historical streaming as live should now take over
                                return;
                            }
                        }
                    }
                    // request historical block
                    owningSession.LOGGER.log(
                            Level.TRACE,
                            "Client {0} requesting historical block {1} from HistoricalBlockProvider, latest block seen is {2}",
                            owningSession.clientId,
                            i,
                            owningSession.latestLiveStreamBlock.get());
                    final BlockAccessor blockAccessor =
                            owningSession.context.historicalBlockProvider().block(i);
                    if (blockAccessor != null) {
                        final BlockUnparsed block = blockAccessor.blockUnparsed();
                        // handle as if it was received from the block messaging system
                        owningSession.processBlockItemsForClient(new BlockItems(block.blockItems(), i));
                    } else if (owningSession.latestLiveStreamBlock.get() == Long.MIN_VALUE) {
                        // we have streamed all the historical blocks so fast we have not even got one block item
                        // message yet so wait a half second and try again
                        owningSession.LOGGER.log(
                                Level.TRACE,
                                "Client {0} has sent all historic and is waiting for first live",
                                owningSession.clientId);
                        LockSupport.parkNanos(500_000_000L);
                        owningSession.LOGGER.log(
                                Level.TRACE,
                                "Client {0} has sent all historic and has waited for first live, latest block seen is {1}",
                                owningSession.clientId,
                                owningSession.latestLiveStreamBlock.get());
                    } else {
                        // we have not received a block yet, so ignore the block items
                        owningSession.LOGGER.log(
                                Level.TRACE,
                                "Client {0} failed to get block {1} from HistoricalBlockProvider {2}",
                                owningSession.clientId,
                                i,
                                owningSession.context.historicalBlockProvider());
                        owningSession.close();
                    }
                }
            } catch (RuntimeException e) {
                owningSession.LOGGER.log(
                        Level.WARNING,
                        "Unexpected exception in HistoricalStreamThread for client " + owningSession.clientId,
                        e);
                throw e;
            }
        }
    }
}
