package org.hiero.block.node.subscriber;

import static org.hiero.block.node.spi.BlockNodePlugin.METRICS_CATEGORY;
import static org.hiero.block.node.spi.BlockNodePlugin.UNKNOWN_BLOCK_NUMBER;

import com.hedera.pbj.runtime.OneOf;
import com.hedera.pbj.runtime.grpc.Pipeline;
import com.swirlds.metrics.api.Counter;
import java.lang.System.Logger.Level;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Flow;
import java.util.concurrent.Flow.Subscription;
import java.util.function.Consumer;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.NoBackPressureBlockItemHandler;
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
 * <p>
 * <h3>Threading</h3>
 * This class is called from many threads from web server, the block messaging system and its own background historical
 * fetching thread. To make its state thread safe it uses synchronized methods around all entry points from other
 * threads.
 */
public class BlockStreamSubscriberSession  implements Pipeline<SubscribeStreamRequest>, NoBackPressureBlockItemHandler {
    /** The logger for this class. */
    private final System.Logger LOGGER = System.getLogger(getClass().getName());

    /** Enum for the state of a block source */
    public enum SubscriberState {
        NEW,
        SENDING_LIVE_STREAM_WAITING_FOR_BLOCK_START,
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
    /** The request from the client */
    private SubscribeStreamRequest subscribeStreamRequest;
    /** The subscription for the GRPC connection with client */
    private Flow.Subscription subscription;
    /** The current state of the subscriber, i.e. state machine state */
    private SubscriberState currentState = SubscriberState.NEW;
    /** The thread for the historical stream */
    private Thread historicalStreamThread;
    /** The current block being sent to the client */
    private long currentBlockBeingSent = UNKNOWN_BLOCK_NUMBER;
    /** The barrier to hold the live stream until we are ready to switch over from historic */
    private CountDownLatch holdLiveStreamBarrier;
    /** The block number of the block that the live stream is waiting on */
    private long blockLiveStreamIsWaitingReadyToSend = UNKNOWN_BLOCK_NUMBER;

    /**
     * Constructor for the BlockStreamSubscriberSession class.
     *
     * @param clientId The client id for this session
     * @param responsePipeline The pipeline to send responses to the client
     * @param context The context for the block node
     */
    public BlockStreamSubscriberSession(long clientId, Pipeline<? super SubscribeStreamResponseUnparsed> responsePipeline,
            BlockNodeContext context, Consumer<BlockStreamSubscriberSession> closeCallback) {
        this.clientId = clientId;
        this.responsePipeline = responsePipeline;
        this.context = context;
        this.closeCallback = closeCallback;
        // create metrics
        historicToLiveStreamTransitions = context.metrics()
                .getOrCreate(new Counter.Config(METRICS_CATEGORY, "historicToLiveStreamTransitions").withDescription(
                        "Historic to Live Stream Transitions"));
        liveToHistoricStreamTransitions = context.metrics()
                .getOrCreate(new Counter.Config(METRICS_CATEGORY, "liveToHistoricStreamTransitions").withDescription(
                        "Live to Historic Stream Transitions"));
        // register us with the block messaging system
        context.blockMessaging().registerBlockItemHandler(this, false,
                "blockStream client " + clientId);
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
        LOGGER.log(Level.INFO, "Closing BlockStreamSubscriberSession for client {1}", clientId);
        closeCallback.accept(this);
        currentState = SubscriberState.DISCONNECTED;
        subscribeStreamRequest  = null;
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
     * Process the block items to send to client
     *
     * @param blockItems The block items to send to the client
     */
    private synchronized void processBlockItemsForClient(final BlockItems blockItems) {
        // check if blockItems is start of new block, and we have an end block number to check against
        if (blockItems.newBlockNumber() != UNKNOWN_BLOCK_NUMBER) {
            currentBlockBeingSent = blockItems.newBlockNumber();
            // check if we have hit the end block number and should stop streaming
            if (subscribeStreamRequest.endBlockNumber() != UNKNOWN_BLOCK_NUMBER &&
                    blockItems.newBlockNumber() > subscribeStreamRequest.endBlockNumber()) {
                LOGGER.log(Level.INFO, "Client {0} has reached end block number {1}", clientId,
                        subscribeStreamRequest.endBlockNumber());
                // send end of stream response
                responsePipeline.onNext(SubscribeStreamResponseUnparsed.newBuilder()
                        .status(SubscribeStreamResponseCode.READ_STREAM_SUCCESS)
                        .build());
                close();
            }
        }
        // send the block items to the client
        responsePipeline.onNext(new SubscribeStreamResponseUnparsed(
                new OneOf<>(ResponseOneOfType.BLOCK_ITEMS,new BlockItemSetUnparsed(blockItems.blockItems()))));
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
        final long endBlockNumber = endBlockNumberRaw == UNKNOWN_BLOCK_NUMBER ?
                Long.MAX_VALUE : endBlockNumberRaw;
        if (historicalStreamThread == null) {
            historicalStreamThread = new Thread(() -> {
                try {
                    for (long i = startBlockNumber; i <= endBlockNumber &&
                            !Thread.currentThread().isInterrupted(); i++) {
                        // check if live stream is waiting ready for us at the barrier
                        synchronized (BlockStreamSubscriberSession.this) {
                            if (holdLiveStreamBarrier != null && i == blockLiveStreamIsWaitingReadyToSend) {
                                // release the barrier
                                holdLiveStreamBarrier.countDown();
                                holdLiveStreamBarrier = null;
                                historicToLiveStreamTransitions.increment();
                                // stop
                                return;
                            }
                        }
                        // request historical block
                        final var block = context.historicalBlockProvider().block(i).blockUnparsed();
                        // handle as if it was received from the block messaging system
                        // TODO does it matter that we send a whole block at a time here?
                        handleBlockItemsReceived(new BlockItems(block.blockItems(),i));
                    }
                } catch (Exception e) {
                    LOGGER.log(Level.ERROR, "Error in BlockStreamSubscriberSession for client {1}", clientId, e);
                }
            });
            historicalStreamThread.start();
        }
    }

    // ==== Pipeline Flow Methods ======================================================================================

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
                sendHistoricalStream(currentBlockBeingSent, subscribeStreamRequest.endBlockNumber());
            }
            case SENDING_HISTORICAL_STREAM -> {
                // we have fallen too far behind the historical stream, so reset barriers and reconnect to the
                // live stream
                LOGGER.log(Level.INFO, "Client {0} has fallen too far behind, while awaiting joining from "
                        + "the historical stream", clientId);
                if (holdLiveStreamBarrier != null) {
                    // release the barrier
                    holdLiveStreamBarrier.countDown();
                    holdLiveStreamBarrier = null;
                    blockLiveStreamIsWaitingReadyToSend = UNKNOWN_BLOCK_NUMBER;
                }
                // reconnect to the live stream
                context.blockMessaging().registerBlockItemHandler(this, false,
                        "blockStream client " + clientId);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void handleBlockItemsReceived(BlockItems blockItems) {
        final long newBlockNumber = blockItems.newBlockNumber();
        switch (currentState) {
            case SENDING_LIVE_STREAM_WAITING_FOR_BLOCK_START -> {
                if (newBlockNumber != UNKNOWN_BLOCK_NUMBER) {
                    // we have received a block, so we can start streaming
                    LOGGER.log(Level.INFO, "Client {0} has started streaming live blocks from {1}",
                            clientId, newBlockNumber);
                    currentState = SubscriberState.SENDING_LIVE_STREAM;
                    currentBlockBeingSent = newBlockNumber;
                    // send the block items to the client
                    processBlockItemsForClient(blockItems);
                } else {
                    // we have not received a block yet, so ignore the block items
                    LOGGER.log(Level.DEBUG, "Client {0} is waiting for start of new block", clientId);
                }
            }
            case SENDING_LIVE_STREAM ->
                processBlockItemsForClient(blockItems);
            case SENDING_HISTORICAL_STREAM -> {
                // check if start of new block,
                if (newBlockNumber != UNKNOWN_BLOCK_NUMBER) {
                    // check if we are within 10 blocks of where we are sending from history, if we are then use barrier to
                    // wait for the historical streaming to catch up
                    if ((newBlockNumber - currentBlockBeingSent) < 10) {
                        // wait for the live stream to catch up
                        LOGGER.log(Level.DEBUG, "Client {0} listener is waiting for historical stream to "
                                + "catch up to live stream", clientId);
                        // wait for the barrier to be released
                        holdLiveStreamBarrier = new CountDownLatch(1);
                        try {
                            holdLiveStreamBarrier.await();
                            // we are now past the barrier, so we can send new block items to client and switch to
                            // live-streaming
                            currentState = SubscriberState.SENDING_LIVE_STREAM;
                            currentBlockBeingSent = newBlockNumber;
                            processBlockItemsForClient(blockItems);
                        } catch (InterruptedException e) {
                            LOGGER.log(Level.ERROR, "Error waiting on barrier in BlockStreamSubscriberSession "
                                    + "for client {1}", clientId, e);
                        }
                    }
                }
            }
        }
        // in all other states we just ignore the block items
    }

    // ==== Pipeline Flow Methods ======================================================================================

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void onSubscribe(Subscription subscription) {
        this.subscription = subscription;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onError(Throwable throwable) {
        LOGGER.log(Level.ERROR, "Error in BlockStreamSubscriberSession for client {1}", clientId, throwable);
        close();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onComplete() {
        close();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void clientEndStreamReceived() {
        close();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void onNext(SubscribeStreamRequest item) throws RuntimeException {
        // check if client has already sent a request and reject subsequent requests
        if (subscribeStreamRequest != null) {
            LOGGER.log(Level.WARNING, "Received new subsequent subscribe request from client {0} when already subscribed to stream {1}",
                    clientId, subscribeStreamRequest);
            // close the stream as this is a bad request
            close();
        } else {
            LOGGER.log(Level.INFO, "Received subscribe request from new client {0} for stream {1}",
                    clientId, item);
            subscribeStreamRequest = item;
            // we have just received a new subscribe request, now we need to work out if we can service it
            if (!subscribeStreamRequest.allowUnverified()) {
                // TODO implement validated stream handling, will need some new states to hold listener back till it is
                //      on validated block
                LOGGER.log(Level.WARNING, "Client {0} requested a validated stream but this is not supported yet", clientId);
            } else if (subscribeStreamRequest.startBlockNumber() == UNKNOWN_BLOCK_NUMBER) {
                currentState = SubscriberState.SENDING_LIVE_STREAM_WAITING_FOR_BLOCK_START;
                LOGGER.log(Level.INFO, "Client {0} has started streaming live blocks", clientId);
                context.blockMessaging().registerNoBackpressureBlockItemHandler(this,
                        false, "liveStream client " + clientId);
            } else if (subscribeStreamRequest.startBlockNumber() < context.historicalBlockProvider().latestBlockNumber()) {
                // they requested an older block, so we need to send a historical stream
                currentState = SubscriberState.SENDING_HISTORICAL_STREAM;
                LOGGER.log(Level.INFO, "Client {0} has started streaming historical blocks from {1} to {2}",
                        clientId, subscribeStreamRequest.startBlockNumber(), subscribeStreamRequest.endBlockNumber());
                sendHistoricalStream(subscribeStreamRequest.startBlockNumber(), subscribeStreamRequest.endBlockNumber());
            } else {
                // client has requested a block that is neither live nor available historical
                LOGGER.log(Level.WARNING, "Client {0} requested start block {1} that is neither live or "
                                + "historical. Newest historical block is {2}",
                        clientId, subscribeStreamRequest.startBlockNumber(),
                        context.historicalBlockProvider().latestBlockNumber());
                // send invalid start block number response
                responsePipeline.onNext(SubscribeStreamResponseUnparsed.newBuilder()
                        .status(SubscribeStreamResponseCode.READ_STREAM_INVALID_START_BLOCK_NUMBER)
                        .build());
                close();
            }
        }
    }

}
