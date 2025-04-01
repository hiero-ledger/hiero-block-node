// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.publisher;

import static java.lang.System.Logger.Level.DEBUG;
import static org.hiero.block.node.spi.BlockNodePlugin.UNKNOWN_BLOCK_NUMBER;

import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.pbj.runtime.OneOf;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.grpc.Pipeline;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.metrics.api.Counter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Flow;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import org.hiero.block.node.publisher.UpdateCallback.UpdateType;
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.hapi.block.node.Acknowledgement;
import org.hiero.hapi.block.node.BlockAcknowledgement;
import org.hiero.hapi.block.node.BlockItemUnparsed;
import org.hiero.hapi.block.node.EndOfStream;
import org.hiero.hapi.block.node.PublishStreamResponse;
import org.hiero.hapi.block.node.PublishStreamResponse.ResponseOneOfType;
import org.hiero.hapi.block.node.PublishStreamResponseCode;
import org.hiero.hapi.block.node.ResendBlock;
import org.hiero.hapi.block.node.SkipBlock;

/**
 * BlockStreamProducerSession is a session for a block stream producer. It handles the incoming block stream and sends
 * the responses to the client. It uses a state machine to manage what role it is in and what actions to take based on
 * the current state and incoming data.
 */
public final class BlockStreamProducerSession implements Pipeline<List<BlockItemUnparsed>> {
    /** The logger for this class. */
    private final System.Logger LOGGER = System.getLogger(getClass().getName());
    /** Enum for the state of a block source */
    public enum BlockState {
        NEW,
        PRIMARY,
        BEHIND,
        WAITING_FOR_RESEND,
        DISCONNECTED
    }

    /** The session ID for this session. Used to identify sessions in logs */
    private final long sessionId;
    /** The pipeline for sending responses to the client */
    private final Pipeline<? super PublishStreamResponse> responsePipeline;
    /** The callback for updating the publisher service plugin */
    private final UpdateCallback onUpdate;
    /** The metric for the number of live block items received */
    private final Counter liveBlockItemsReceived;
    /** Single lock for gating access to state changes within whole plugin. */
    private final ReentrantLock stateLock;
    /** The callback for sending block items to the block messaging service */
    private final Consumer<BlockItems> sendToBlockMessaging;
    /** The subscription for the GRPC connection with client */
    private Flow.Subscription subscription;
    /** The current state of this session, i.e. state machine state */
    private BlockState currentBlockState = BlockState.NEW;
    /** The current block number we are receiving from client */
    private long currentBlockNumber = BlockNodePlugin.UNKNOWN_BLOCK_NUMBER;
    /** The start of receiving time of the current block */
    private long startTimeOfCurrentBlock = 0;
    /** list used to store items if we are new and ahead of the current message stream block */
    private final List<BlockItemUnparsed> newBlockItems = new ArrayList<>();

    /**
     * Constructor for BlockStreamProducerSession.
     *
     * @param sessionId the session ID
     * @param responsePipeline the pipeline for sending responses to the client
     * @param onUpdate the callback for updating the publisher service plugin
     * @param liveBlockItemsReceived the metric for the number of live block items received
     * @param stateLock the lock for accessing state
     * @param sendToBlockMessaging the callback for sending block items to the block messaging service
     */
    public BlockStreamProducerSession(
            long sessionId,
            Pipeline<? super PublishStreamResponse> responsePipeline,
            UpdateCallback onUpdate,
            Counter liveBlockItemsReceived,
            ReentrantLock stateLock,
            Consumer<BlockItems> sendToBlockMessaging) {
        this.sessionId = sessionId;
        this.onUpdate = onUpdate;
        this.responsePipeline = responsePipeline;
        this.liveBlockItemsReceived = liveBlockItemsReceived;
        this.stateLock = stateLock;
        this.sendToBlockMessaging = sendToBlockMessaging;
        // log the creation of the session
        LOGGER.log(DEBUG, "Created new BlockStreamProducerSession");
    }

    /**
     * toString for help logging
     */
    @Override
    public String toString() {
        return "BlockStreamProducerSession{id=" + sessionId + ", currentBlockNumber=" + currentBlockNumber
                + ", currentBlockState=" + currentBlockState + '}';
    }

    /**
     * Get the current block number for this session.
     *
     * @return the current block number
     */
    long currentBlockNumber() {
        return currentBlockNumber;
    }

    /**
     * Get the current block state for this session.
     *
     * @return the current block state
     */
    BlockState currentBlockState() {
        return currentBlockState;
    }

    /**
     * Get the start time of receiving the current block for this session.
     *
     * @return the start time of receiving the current block
     */
    long startTimeOfCurrentBlock() {
        return startTimeOfCurrentBlock;
    }

    /**
     * Get the ID of the current session.
     *
     * @return the current session ID.
     */
    long sessionId() {
        return sessionId;
    }

    /**
     * Make this session the primary session for the block stream. This means that we are now the primary session and
     * will send block items to the block messaging service. This is called when we are in the new state and have
     * received the first block item for the new block. It is trusted that this is always called with the stateLock
     * already acquired.
     */
    void switchToPrimary() {
        // switch to primary state
        currentBlockState = BlockState.PRIMARY;
        // send any items we have in the new items list to the block messaging service
        if (!newBlockItems.isEmpty()) {
            // this items will always be the first items in a block so we can use the block number
            // we have to copy the items as we clear the list after sending
            sendToBlockMessaging.accept(new BlockItems(new ArrayList<>(newBlockItems), currentBlockNumber));
            // clear the list
            newBlockItems.clear();
        }
    }

    /**
     * Make this session a behind session for this block. This means that we are not the primary session for this block.
     * We can tell the client that we are behind so they can skip the rest of the block. This is called when we are in
     * the new state and have received the first block item for the new block. It is trusted that this is always called
     * with the stateLock already acquired.
     */
    void switchToBehind() {
        // switch to behind state
        currentBlockState = BlockState.BEHIND;
        // throw away any items we have in the new items list
        newBlockItems.clear();
        // let client know we do not need more data for the current block
        if (responsePipeline != null) {
            final PublishStreamResponse behindResponse = new PublishStreamResponse(
                    new OneOf<>(ResponseOneOfType.ACKNOWLEDGEMENT, new SkipBlock(currentBlockNumber)));
            responsePipeline.onNext(behindResponse);
        }
    }

    /**
     * Request a resend of the block from the client. We will stop what we are doing and go back to looking for the
     * requested block to be resent It is trusted that this is always called with the stateLock already acquired.
     *
     * @param blockNumber the block number to request to be resent
     */
    void requestResend(long blockNumber) {
        // switch to waiting for resend state
        currentBlockState = BlockState.WAITING_FOR_RESEND;
        currentBlockNumber = blockNumber;
        // throw away any items we have in the new items list
        newBlockItems.clear();
        // resend the block request to the block messaging service
        if (responsePipeline != null) {
            final PublishStreamResponse resendBlockResponse = new PublishStreamResponse(
                    new OneOf<>(ResponseOneOfType.RESEND_BLOCK, new ResendBlock(blockNumber)));
            responsePipeline.onNext(resendBlockResponse);
        }
    }

    /**
     * Close the session and cancel the subscription.
     */
    void close() {
        if (currentBlockState != BlockState.DISCONNECTED) {
            currentBlockState = BlockState.DISCONNECTED;
            // try to send a close response to the client
            if (responsePipeline != null) {
                final PublishStreamResponse closeResponse = new PublishStreamResponse(new OneOf<>(
                        ResponseOneOfType.END_STREAM,
                        new EndOfStream(PublishStreamResponseCode.STREAM_ITEMS_SUCCESS, currentBlockNumber)));
                responsePipeline.onNext(closeResponse);
            }
            if (subscription != null) {
                subscription.cancel();
                subscription = null;
            }
        }
    }

    /**
     * Send a block persisted message to the client. This is totally asynchronous call and independent of the current
     * block and state of thi session.
     *
     * @param blockNumber the block number
     * @param blockHash   the block hash
     */
    void sendBlockPersisted(long blockNumber, Bytes blockHash) {
        if (responsePipeline != null) {
            final PublishStreamResponse goodBlockResponse = new PublishStreamResponse(new OneOf<>(
                    ResponseOneOfType.ACKNOWLEDGEMENT,
                    new Acknowledgement(new BlockAcknowledgement(blockNumber, blockHash, false))));
            // send the response to the client
            responsePipeline.onNext(goodBlockResponse);
        }
    }

    // ==== Pipeline Flow Methods ==================================================================================

    /**
     * {@inheritDoc}
     * Called by web server thread so we have to acquire the lock to access state.
     */
    @SuppressWarnings("RedundantLabeledSwitchRuleCodeBlock")
    @Override
    public void onNext(List<BlockItemUnparsed> items) throws RuntimeException {
        stateLock.lock();
        try {
            // update the live block items received metric
            liveBlockItemsReceived.add(items.size());
            // check items to see if we are entering a new block
            final boolean newBlock = items.getFirst().hasBlockHeader();
            if (newBlock) {
                try {
                    long newBlockNumber = BlockHeader.PROTOBUF
                            .parse(items.getFirst().blockHeaderOrThrow())
                            .number();
                    // move to new state if we are not in the waiting for resend state, or if we are in the waiting
                    // for resend state and the block number is the same as the current block number
                    if (currentBlockState != BlockState.WAITING_FOR_RESEND || newBlockNumber == currentBlockNumber) {
                        // set the start time of the current block
                        startTimeOfCurrentBlock = System.nanoTime();
                        // we are in a new block so switch to new state
                        currentBlockState = BlockState.NEW;
                        // update the current block number
                        currentBlockNumber = newBlockNumber;
                        // throw away any items we have in the ahead list
                        newBlockItems.clear();
                    }
                } catch (ParseException e) {
                    throw new RuntimeException(e);
                }
            }
            switch (currentBlockState) {
                case NEW -> newBlockItems.addAll(items);
                case PRIMARY -> {
                    // we are in the primary state, so we can send the items directly to the block messaging service
                    // this will never be the first items in a block so we can always send UNKNOWN_BLOCK_NUMBER for
                    // block number
                    sendToBlockMessaging.accept(new BlockItems(items, UNKNOWN_BLOCK_NUMBER));
                }
                case BEHIND -> {
                    // we can ignore as any items we receive in this state are not relevant
                    LOGGER.log(DEBUG, "Received {0} items while in BEHIND state", items);
                }
                case WAITING_FOR_RESEND -> {
                    // we are waiting for a resend, so we can ignore any items we receive in this state
                    LOGGER.log(DEBUG, "Received {0} items while in WAITING_FOR_RESEND state", items);
                }
                case DISCONNECTED -> {
                    // do nothing but log, as we are disconnected
                    LOGGER.log(DEBUG, "BlockStreamProducerSession is disconnected, but received items: {0}", items);
                }
            }
            // call the onUpdate method to notify the block messaging service that we have received data and updated our
            // state, check if we are starting or ending a block
            if (newBlock && items.getLast().hasBlockProof()) {
                onUpdate.update(this, UpdateType.WHOLE_BLOCK, currentBlockNumber);
            } else if (items.getLast().hasBlockProof()) {
                // send end block update
                onUpdate.update(this, UpdateType.END_BLOCK, currentBlockNumber);
                // change state back to NEW as we have finished the current block
                currentBlockState = BlockState.NEW;
            } else if (newBlock) {
                // send start block update
                onUpdate.update(this, UpdateType.START_BLOCK, currentBlockNumber);
            } else {
                // we are not starting or ending a block, so we can just update the state
                onUpdate.update(this, UpdateType.BLOCK_ITEMS_RECEIVED);
            }
        } finally {
            stateLock.unlock();
        }
    }

    /**
     * {@inheritDoc}
     * Called by web server thread so we have to acquire the lock to access state.
     */
    @Override
    public void onError(Throwable throwable) {
        stateLock.lock();
        try {
            LOGGER.log(DEBUG, "BlockStreamProducerSession error", throwable.getMessage());
            close();
            // call the onUpdate method to notify the block messaging service that we have received data and updated our
            // state
            onUpdate.update(this, UpdateType.SESSION_CLOSED);
        } finally {
            stateLock.unlock();
        }
    }

    /**
     * {@inheritDoc}
     * Called by web server thread so we have to acquire the lock to access state.
     */
    @Override
    public void clientEndStreamReceived() {
        stateLock.lock();
        try {
            LOGGER.log(DEBUG, "BlockStreamProducerSession clientEndStreamReceived");
            close();
            // call the onUpdate method to notify the block messaging service that we have received data and updated our
            // state
            onUpdate.update(this, UpdateType.SESSION_CLOSED);
        } finally {
            stateLock.unlock();
        }
    }

    /**
     * {@inheritDoc}
     * Called by web server thread so we have to acquire the lock to access state.
     */
    @Override
    public void onComplete() {
        stateLock.lock();
        try {
            LOGGER.log(DEBUG, "BlockStreamProducerSession onComplete");
            close();
            // call the onUpdate method to notify the block messaging service that we have received data and updated our
            // state
            onUpdate.update(this, UpdateType.SESSION_CLOSED);
        } finally {
            stateLock.unlock();
        }
    }

    /**
     * {@inheritDoc}
     * Called by web server thread so we have to acquire the lock to access state.
     */
    @Override
    public void onSubscribe(Flow.Subscription subscription) {
        stateLock.lock();
        try {
            LOGGER.log(DEBUG, "BlockStreamProducerSession onSubscribe called");
            this.subscription = subscription;
            // TODO seems like we should be using {subscription} for flow control, calling its request() method
        } finally {
            stateLock.unlock();
        }
    }
}
