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
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.NoBackPressureBlockItemHandler;
import org.hiero.block.node.spi.historicalblocks.BlockAccessor;
import org.hiero.hapi.block.node.BlockItemSetUnparsed;
import org.hiero.hapi.block.node.SubscribeStreamRequest;
import org.hiero.hapi.block.node.SubscribeStreamResponseCode;
import org.hiero.hapi.block.node.SubscribeStreamResponseUnparsed;
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
    private long currentBlockBeingSent = UNKNOWN_BLOCK_NUMBER;
    /** The latest block received from the live stream, start at crazy negative so more than 10 away from block 0 */
    private long latestLiveStreamBlock = Long.MIN_VALUE;
    /** The barrier to hold the live stream until we are ready to switch over from historic */
    private final AtomicReference<CountDownLatch> holdLiveStreamBarrier = new AtomicReference<>();
    /** The block number of the block that the live stream is waiting on */
    private long blockLiveStreamIsWaitingReadyToSend = UNKNOWN_BLOCK_NUMBER;
    /** The first block number to stream */
    private final long startBlockNumber;
    /** The last block number to stream, can be Long.MAX_VALUE to mean infinite */
    private final long endBlockNumber;

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
        this.endBlockNumber = request.endBlockNumber() == 0 ? Long.MAX_VALUE : request.endBlockNumber();
        this.responsePipeline = responsePipeline;
        this.context = context;
        this.closeCallback = closeCallback;
        // create metrics
        historicToLiveStreamTransitions = context.metrics()
                .getOrCreate(new Counter.Config(METRICS_CATEGORY, "historicToLiveStreamTransitions")
                        .withDescription("Historic to Live Stream Transitions"));
        liveToHistoricStreamTransitions = context.metrics()
                .getOrCreate(new Counter.Config(METRICS_CATEGORY, "liveToHistoricStreamTransitions")
                        .withDescription("Live to Historic Stream Transitions"));
        // get latest available blocks
        final long oldestBlockNumber = context.historicalBlockProvider().oldestBlockNumber();
        final long latestBlockNumber = context.historicalBlockProvider().latestBlockNumber();
        // we have just received a new subscribe request, now we need to work out if we can service it
        if (!request.allowUnverified()) {
            // ----------> VALIDATED STREAM <----------
            LOGGER.log(Level.WARNING, "Client {0} requested a validated stream but this is not supported", clientId);
            // send response to client
            responsePipeline.onNext(SubscribeStreamResponseUnparsed.newBuilder()
                    .status(SubscribeStreamResponseCode.READ_STREAM_NOT_AVAILABLE)
                    .build());
            close();
        } else if (request.startBlockNumber() < UNKNOWN_BLOCK_NUMBER) {
            // ----------> NEGATIVE START AND NOT UNKNOWN_BLOCK_NUMBER <----------
            LOGGER.log(Level.WARNING, "Client {0} requested negative block {1}", clientId, request.startBlockNumber());
            // send invalid start block number response
            responsePipeline.onNext(SubscribeStreamResponseUnparsed.newBuilder()
                    .status(SubscribeStreamResponseCode.READ_STREAM_INVALID_START_BLOCK_NUMBER)
                    .build());
            close();
        } else if (request.endBlockNumber() > 0 && startBlockNumber > endBlockNumber) {
            // ----------> END BEFORE START <----------
            LOGGER.log(
                    Level.WARNING,
                    "Client {0} requested end block {1} before start {2}",
                    clientId,
                    request.endBlockNumber(),
                    request.startBlockNumber());
            // send invalid end block number response
            responsePipeline.onNext(SubscribeStreamResponseUnparsed.newBuilder()
                    .status(SubscribeStreamResponseCode.READ_STREAM_INVALID_END_BLOCK_NUMBER)
                    .build());
            close();
        } else if (startBlockNumber != UNKNOWN_BLOCK_NUMBER
                && (startBlockNumber < oldestBlockNumber || startBlockNumber > (latestBlockNumber + 30))) {
            // client has requested a block that is neither live nor available historical
            LOGGER.log(
                    Level.WARNING,
                    "Client {0} requested start block {1} that is neither live or "
                            + "historical. Newest historical block is {2}",
                    clientId,
                    startBlockNumber,
                    context.historicalBlockProvider().latestBlockNumber());
            // send invalid start block number response
            responsePipeline.onNext(SubscribeStreamResponseUnparsed.newBuilder()
                    .status(SubscribeStreamResponseCode.READ_STREAM_INVALID_START_BLOCK_NUMBER)
                    .build());
            close();
        } else {
            // ----------> VALID REQUEST EITHER LATEST LIVE OR HISTORIC <----------
            // register us to listen to block items from the block messaging system
            context.blockMessaging()
                    .registerNoBackpressureBlockItemHandler(this, false, "liveStream client " + clientId);
            if (startBlockNumber == UNKNOWN_BLOCK_NUMBER) {
                // ----------> START AT ANY BLOCK <----------
                currentState = SubscriberState.SENDING_LIVE_STREAM_WAITING_FOR_ANY_BLOCK_START;
                LOGGER.log(Level.INFO, "Client {0} has started streaming live blocks", clientId);
            } else if (startBlockNumber > latestBlockNumber) {
                // ----------> SHORTLY AHEAD OF CURRENT BLOCK <----------
                // they requested a block that is not yet available, but should be shortly, so we need to hold the
                // live stream till we get to that block
                currentState = SubscriberState.SENDING_LIVE_STREAM_WAITING_FOR_EXPLICIT_BLOCK_START;
                LOGGER.log(
                        Level.INFO,
                        "Client {0} has started streaming, waiting for block {1}",
                        clientId,
                        startBlockNumber);
            } else {
                // ----------> HISTORICAL STREAM <----------
                // we are starting at a block that is in the historical stream, so we need to start the historical
                // stream
                currentState = SubscriberState.SENDING_HISTORICAL_STREAM;
                LOGGER.log(
                        Level.INFO,
                        "Client {0} has started streaming historical blocks from {1}",
                        clientId,
                        startBlockNumber);
                // start the historical stream
                sendHistoricalStream(startBlockNumber, endBlockNumber);
            }
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

    /**
     * Close this session. This will unregister us from the block messaging system and cancel the subscription.
     */
    public synchronized void close() {
        LOGGER.log(Level.DEBUG, "Closing BlockStreamSubscriberSession for client {0}", clientId);
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
            currentBlockBeingSent = blockItems.newBlockNumber();
            // check if we have got past the end block number and should stop streaming
            if (blockItems.newBlockNumber() > endBlockNumber) {
                LOGGER.log(Level.DEBUG, "Client {0} has reached end block number {1}", clientId, endBlockNumber);
                // send end of stream response
                responsePipeline.onNext(SubscribeStreamResponseUnparsed.newBuilder()
                        .status(SubscribeStreamResponseCode.READ_STREAM_SUCCESS)
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
            historicalStreamThread = new Thread(() -> {
                try {
                    for (long i = startBlockNumber;
                            i <= endBlockNumber && !Thread.currentThread().isInterrupted();
                            i++) {
                        // check if live stream is waiting ready for us at the barrier
                        final CountDownLatch barrier = holdLiveStreamBarrier.get();
                        if (barrier != null) {
                            synchronized (BlockStreamSubscriberSession.this) {
                                if (i == blockLiveStreamIsWaitingReadyToSend) {
                                    // release the barrier
                                    barrier.countDown();
                                    holdLiveStreamBarrier.set(null);
                                    historicToLiveStreamTransitions.increment();
                                    // stop historical streaming as live should now take over
                                    return;
                                }
                            }
                        }
                        // request historical block
                        LOGGER.log(
                                Level.TRACE,
                                "Client {0} requesting historical block {1} from HistoricalBlockProvider, live block is {2}",
                                clientId,
                                i,
                                latestLiveStreamBlock);
                        final BlockAccessor blockAccessor =
                                context.historicalBlockProvider().block(i);
                        if (blockAccessor != null) {
                            final var block = blockAccessor.blockUnparsed();
                            // handle as if it was received from the block messaging system
                            processBlockItemsForClient(new BlockItems(block.blockItems(), i));
                        } else if (latestLiveStreamBlock == Long.MIN_VALUE) {
                            // we have streamed all the historical blocks so fast we have not even got one block item
                            // message yet so wait a couple seconds and try again
                            LOGGER.log(
                                    Level.DEBUG,
                                    "Client {0} has sent all historic and is waiting for first live",
                                    clientId);
                            LockSupport.parkNanos(2_000_000_000L);
                            LOGGER.log(
                                    Level.DEBUG,
                                    "Client {0} has sent all historic and has waited for first live, live block is {1}",
                                    clientId,
                                    latestLiveStreamBlock);
                        } else {
                            // we have not received a block yet, so ignore the block items
                            LOGGER.log(
                                    Level.DEBUG,
                                    "Client {0} failed to get block {1} from HistoricalBlockProvider {2}",
                                    clientId,
                                    i,
                                    context.historicalBlockProvider());
                            close();
                        }
                    }
                } catch (Exception e) {
                    LOGGER.log(Level.ERROR, "Error in BlockStreamSubscriberSession for client " + clientId, e);
                }
            });
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
                LOGGER.log(Level.INFO, "Client {0} has fallen too far behind the live stream", clientId);
                currentState = SubscriberState.SENDING_HISTORICAL_STREAM;
                liveToHistoricStreamTransitions.increment();
                sendHistoricalStream(currentBlockBeingSent, endBlockNumber);
            }
            case SENDING_HISTORICAL_STREAM -> {
                // we have fallen too far behind the historical stream, so reset barriers and reconnect to the
                // live stream
                LOGGER.log(
                        Level.INFO,
                        "Client {0} has fallen too far behind, while awaiting joining from the historical stream",
                        clientId);
                final CountDownLatch barrier = holdLiveStreamBarrier.get();
                if (barrier != null) {
                    // release the barrier
                    barrier.countDown();
                    holdLiveStreamBarrier.set(null);
                    blockLiveStreamIsWaitingReadyToSend = UNKNOWN_BLOCK_NUMBER;
                }
                // reconnect to the live stream
                context.blockMessaging().registerBlockItemHandler(this, false, "blockStream client " + clientId);
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
        System.out.println("BlockStreamSubscriberSession.handleBlockItemsReceived " + blockItems);
        synchronized (this) {
            // if we get the start of a block then update the latest live stream block
            if (blockItems.isStartOfNewBlock()) {
                latestLiveStreamBlock = blockItems.newBlockNumber();
            }
            switch (currentState) {
                case SENDING_LIVE_STREAM_WAITING_FOR_ANY_BLOCK_START -> {
                    if (blockItems.isStartOfNewBlock()) {
                        // we have received a block, so we can start streaming
                        LOGGER.log(
                                Level.DEBUG,
                                "Client {0} has started streaming live blocks from {1}",
                                clientId,
                                blockItems.newBlockNumber());
                        currentState = SubscriberState.SENDING_LIVE_STREAM;
                        currentBlockBeingSent = blockItems.newBlockNumber();
                        // send the block items to the client
                        processBlockItemsForClient(blockItems);
                    } else {
                        // we have not received a block yet, so ignore the block items
                        LOGGER.log(Level.DEBUG, "Client {0} is waiting for start of new block", clientId);
                    }
                }
                case SENDING_LIVE_STREAM_WAITING_FOR_EXPLICIT_BLOCK_START -> {
                    // handle cases where the client is waiting for a future block
                    if (latestLiveStreamBlock > startBlockNumber) {
                        // we have started a block newer and have failed to start streaming so return error to client
                        LOGGER.log(
                                Level.DEBUG,
                                "Client {0} has started streaming live blocks from {1} but we are waiting for {2} "
                                        + "which is now in the past",
                                clientId,
                                latestLiveStreamBlock,
                                startBlockNumber);
                        responsePipeline.onNext(SubscribeStreamResponseUnparsed.newBuilder()
                                .status(SubscribeStreamResponseCode.READ_STREAM_INVALID_START_BLOCK_NUMBER)
                                .build());
                        close();
                    } else if (latestLiveStreamBlock == startBlockNumber) {
                        // we have received the start block, so we can start streaming
                        LOGGER.log(
                                Level.DEBUG,
                                "Client {0} has started streaming live blocks from {1}",
                                clientId,
                                blockItems.newBlockNumber());
                        currentState = SubscriberState.SENDING_LIVE_STREAM;
                        currentBlockBeingSent = blockItems.newBlockNumber();
                        // send the block items to the client
                        processBlockItemsForClient(blockItems);
                    } else {
                        // we have not received a block yet, so ignore the block items
                        LOGGER.log(
                                Level.DEBUG,
                                "Client {0} is waiting for start of block {1}",
                                clientId,
                                startBlockNumber);
                    }
                }
                case SENDING_LIVE_STREAM -> processBlockItemsForClient(blockItems);
                case SENDING_HISTORICAL_STREAM -> {
                    // check if the historical sending has caught up enough that it is within 10 blocks of the live
                    // stream
                    // if so we will hold the live stream at the barrier and wait for the historical stream to switch
                    // over
                    if (blockItems.isStartOfNewBlock()
                            && (latestLiveStreamBlock - Math.max(0, currentBlockBeingSent)) < 10) {
                        // we are waiting for the live stream to catch up, so get ready to block until we are ready to
                        // send the live stream. Don't just block here as we are in a synchronized block, and we will
                        // cause a deadlock.
                        holdLiveStreamBarrier.set(new CountDownLatch(1));
                        blockLiveStreamIsWaitingReadyToSend = blockItems.newBlockNumber();
                    }
                }
            }
        }
        // block if needed, done outside the synchronized block to avoid deadlock
        final CountDownLatch barrier = holdLiveStreamBarrier.get();
        if (barrier != null) {
            try {
                LOGGER.log(
                        Level.INFO,
                        "Client {0} block item listener is waiting for historical stream to "
                                + "catch up to live stream. Current block {1} and live stream is at {2}",
                        clientId,
                        currentBlockBeingSent,
                        latestLiveStreamBlock);
                barrier.await();
                // we are now past the barrier, so we can send new block items to client and switch to live-streaming
                synchronized (this) {
                    currentState = SubscriberState.SENDING_LIVE_STREAM;
                    currentBlockBeingSent = blockItems.newBlockNumber();
                    processBlockItemsForClient(blockItems);
                }
            } catch (InterruptedException e) {
                LOGGER.log(
                        Level.ERROR,
                        "Error waiting on barrier in BlockStreamSubscriberSession for client {1}",
                        clientId,
                        e);
            }
        }
    }
    //
    //    public synchronized void itemsToSend(BlockItems blockItems) {
    //        switch (currentState) {
    //            case SENDING_LIVE_STREAM_WAITING_FOR_ANY_BLOCK_START -> {
    //                if (newBlockNumber != UNKNOWN_BLOCK_NUMBER) {
    //                    // we have received a block, so we can start streaming
    //                    LOGGER.log(
    //                            Level.DEBUG,
    //                            "Client {0} has started streaming live blocks from {1}",
    //                            clientId,
    //                            newBlockNumber);
    //                    currentState = SubscriberState.SENDING_LIVE_STREAM;
    //                    currentBlockBeingSent = newBlockNumber;
    //                    // send the block items to the client
    //                    processBlockItemsForClient(blockItems);
    //                } else {
    //                    // we have not received a block yet, so ignore the block items
    //                    LOGGER.log(Level.DEBUG, "Client {0} is waiting for start of new block", clientId);
    //                }
    //            }
    //            case SENDING_LIVE_STREAM_WAITING_FOR_EXPLICIT_BLOCK_START -> {
    //                // handle cases where the client is waiting for a future block
    //                } else if (newBlockNumber == subscribeStreamRequest.startBlockNumber()) {
    //                    // we have received a block, so we can start streaming
    //                    LOGGER.log(
    //                            Level.DEBUG,
    //                            "Client {0} has started streaming live blocks from {1}",
    //                            clientId,
    //                            newBlockNumber);
    //                    currentState = SubscriberState.SENDING_LIVE_STREAM;
    //                    currentBlockBeingSent = newBlockNumber;
    //                    // send the block items to the client
    //                    processBlockItemsForClient(blockItems);
    //                } else {
    //                    // we have not received a block yet, so ignore the block items
    //                    LOGGER.log(
    //                            Level.DEBUG,
    //                            "Client {0} is waiting for start of block {1}",
    //                            clientId,
    //                            subscribeStreamRequest.startBlockNumber());
    //                }
    //            }
    //            case SENDING_LIVE_STREAM -> processBlockItemsForClient(blockItems);
    //            case SENDING_HISTORICAL_STREAM -> {
    //                // check if start of new block,
    //                if (newBlockNumber != UNKNOWN_BLOCK_NUMBER) {
    //                    // check if we are within 10 blocks of where we are sending from history, if we are then use
    // barrier
    //                    // to
    //                    // wait for the historical streaming to catch up
    //                    if ((newBlockNumber - Math.max(0,currentBlockBeingSent)) < 10) {
    //                        // wait for the live stream to catch up
    //                        LOGGER.log(
    //                                Level.DEBUG,
    //                                "Client {0} listener is waiting for historical stream to catch up to live stream.
    // " +
    //                                "Current block {1} and live stream is at {2}",
    //                                clientId, currentBlockBeingSent, newBlockNumber);
    //                        // wait for the barrier to be released
    //                        holdLiveStreamBarrier = new CountDownLatch(1);
    //                        try {
    //                            holdLiveStreamBarrier.await();
    //                            // we are now past the barrier, so we can send new block items to client and switch to
    //                            // live-streaming
    //                            currentState = SubscriberState.SENDING_LIVE_STREAM;
    //                            currentBlockBeingSent = newBlockNumber;
    //                            processBlockItemsForClient(blockItems);
    //                        } catch (InterruptedException e) {
    //                            LOGGER.log(
    //                                    Level.ERROR,
    //                                    "Error waiting on barrier in BlockStreamSubscriberSession for client {1}",
    //                                    clientId,
    //                                    e);
    //                        }
    //                    }
    //                }
    //            }
    //        }
    //        // in all other states we just ignore the block items
    //    }
}
