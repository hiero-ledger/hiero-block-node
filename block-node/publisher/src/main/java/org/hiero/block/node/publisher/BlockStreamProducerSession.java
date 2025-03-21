package org.hiero.block.node.publisher;

import static java.lang.System.Logger.Level.DEBUG;

import com.hedera.hapi.block.PublishStreamResponse;
import com.hedera.hapi.block.PublishStreamResponse.Acknowledgement;
import com.hedera.hapi.block.PublishStreamResponse.BlockAcknowledgement;
import com.hedera.hapi.block.PublishStreamResponse.EndOfStream;
import com.hedera.hapi.block.PublishStreamResponseCode;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.pbj.runtime.OneOf;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.grpc.Pipeline;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicLong;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.hapi.block.node.BlockItemUnparsed;

/**
 * BlockStreamProducerSession is a session for a block stream producer. It handles the incoming block stream and sends
 * the responses to the client.
 *
 * TODO the thread safety of this class is not great. so need to think more on right way to handle that
 */
class BlockStreamProducerSession
        implements Pipeline<List<BlockItemUnparsed>>,
        Comparable<BlockStreamProducerSession> {
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

    private final long sessionCreationTime = System.nanoTime();
    private final Pipeline<? super PublishStreamResponse> responsePipeline;
    private final BlockNodeContext context;
    private final Runnable onUpdate;
    private Flow.Subscription subscription;
    private BlockState currentBlockState = BlockState.NEW;
    private AtomicLong currentBlockNumber = new AtomicLong(BlockNodePlugin.UNKNOWN_BLOCK_NUMBER);
    private AtomicLong startTimeOfCurrentBlock = new AtomicLong(0);
    /** list used to store items if we are new and ahead of the current message stream block */
    private List<BlockItemUnparsed> newBlockItems = new ArrayList<>();

    public BlockStreamProducerSession(Pipeline<? super PublishStreamResponse> responsePipeline,
            BlockNodeContext context, Runnable onUpdate) {
        this.context = context;
        this.onUpdate = onUpdate;
        this.responsePipeline = responsePipeline;
        // log the creation of the session
        LOGGER.log(DEBUG, "Created new BlockStreamProducerSession[{0}]", sessionCreationTime);
    }

    /**
     * Get the current block number for this session.
     *
     * @return the current block number
     */
    synchronized long currentBlockNumber() {
        return currentBlockNumber.get();
    }

    synchronized BlockState currentBlockState() {
        return currentBlockState;
    }

    synchronized long startTimeOfCurrentBlock() {
        return startTimeOfCurrentBlock.get();
    }

    synchronized void switchToPrimary() {
        // switch to primary state
        currentBlockState = BlockState.PRIMARY;
        // send any items we have in the new items list to the block messaging service
        if (!newBlockItems.isEmpty()) {
            context.blockMessaging().sendBlockItems(newBlockItems);
            // clear the list
            newBlockItems.clear();
        }
    }

    synchronized void switchToBehind() {
        // switch to behind state
        currentBlockState = BlockState.BEHIND;
        // throw away any items we have in the new items list
        newBlockItems.clear();
        // let client know we do not need more data for the current block
        if (responsePipeline != null) {
            final PublishStreamResponse behindResponse =
                    new PublishStreamResponse(new OneOf<>(
                            ResponseOneOfType.ACKNOWLEDGEMENT,
                            new SkipBlock(currentBlockNumber.get())));
            responsePipeline.onNext(behindResponse);
        }
    }

    synchronized void requestResend(long blockNumber) {
        // switch to waiting for resend state
        currentBlockState = BlockState.WAITING_FOR_RESEND;
        currentBlockNumber.set(blockNumber);
        // throw away any items we have in the new items list
        newBlockItems.clear();
        // resend the block request to the block messaging service
        if (responsePipeline != null) {
            final PublishStreamResponse resendBlockResponse =
                    new PublishStreamResponse(new OneOf<>(
                            ResponseOneOfType.RESEND_BLOCK,
                            new ResendBlock(blockNumber)));
            responsePipeline.onNext(resendBlockResponse);
        }
    }

    /**
     * Close the session and cancel the subscription.
     */
    synchronized void close() {
        if (currentBlockState != BlockState.DISCONNECTED) {
            currentBlockState = BlockState.DISCONNECTED;
            // try to send a close response to the client
            if (responsePipeline != null) {
                final PublishStreamResponse closeResponse =
                        new PublishStreamResponse(new OneOf<>(
                                ResponseOneOfType.END_STREAM,
                                new EndOfStream(PublishStreamResponseCode.STREAM_ITEMS_SUCCESS,
                                        currentBlockNumber.get())));
                responsePipeline.onNext(closeResponse);
            }
            if (subscription != null) {
                subscription.cancel();
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
    public void sendBlockPersisted(long blockNumber, Bytes blockHash) {
        if (responsePipeline != null) {
            final PublishStreamResponse goodBlockResponse =
                    new PublishStreamResponse(new OneOf<>(
                            ResponseOneOfType.ACKNOWLEDGEMENT,
                            new Acknowledgement(
                                    new BlockAcknowledgement(
                                            blockNumber,
                                            blockHash,
                                            false
                                    ))));
            // send the response to the client
            responsePipeline.onNext(goodBlockResponse);
        }
    }

    // ==== Pipeline Flow Methods ==================================================================================

    @Override
    public synchronized void onNext(List<BlockItemUnparsed> items) throws RuntimeException {
        // check items to see if we are entering a new block
        if (items.size() == 1 && items.getFirst().hasBlockHeader()) {
            try {
                long newBlockNumber = BlockHeader.PROTOBUF.parse(items.getFirst().blockHeaderOrThrow()).number();
                // move to new state if we are not in the waiting for resend state, or if we are in the waiting
                // for resend state and the block number is the same as the current block number
                if (currentBlockState != BlockState.WAITING_FOR_RESEND || newBlockNumber == currentBlockNumber.get()) {
                    // set the start time of the current block
                    startTimeOfCurrentBlock.set(System.nanoTime());
                    // we are in a new block so switch to new state
                    currentBlockState = BlockState.NEW;
                    // update the current block number
                    currentBlockNumber.set(newBlockNumber);
                    // throw away any items we have in the ahead list
                    newBlockItems.clear();
                }
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }
        }
        switch(currentBlockState) {
            case NEW -> {
                newBlockItems.addAll(items);
            }
            case PRIMARY -> {
                // we are in the primary state, so we can send the items directly to the block messaging service
                context.blockMessaging().sendBlockItems(items);
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
        // state
        onUpdate.run();
    }

    @Override
    public synchronized void onError(Throwable throwable) {
        LOGGER.log(DEBUG, "BlockStreamProducerSession error", throwable.getMessage());
        close();
        // call the onUpdate method to notify the block messaging service that we have received data and updated our
        // state
        onUpdate.run();
    }

    @Override
    public synchronized void clientEndStreamReceived() {
        LOGGER.log(DEBUG, "BlockStreamProducerSession clientEndStreamReceived");
        close();
        // call the onUpdate method to notify the block messaging service that we have received data and updated our
        // state
        onUpdate.run();
    }

    @Override
    public synchronized void onComplete() {
        LOGGER.log(DEBUG, "BlockStreamProducerSession onComplete");
        close();
        // call the onUpdate method to notify the block messaging service that we have received data and updated our
        // state
        onUpdate.run();
    }

    @Override
    public synchronized void onSubscribe(Flow.Subscription subscription) {
        LOGGER.log(DEBUG, "BlockStreamProducerSession onSubscribe called");
        this.subscription = subscription;
        // TODO seems like we should be using {subscription} for flow control, calling its request() method
    }

    // ==== Comparable Methods ===================================================================================

    /**
     * Compare this session to another session based on the creation time. We need to be comparable so we can sort the
     * sessions in the set. We depend on the fact that sessionCreationTime being in nanoseconds is unique for each
     * session. So equals and hashcode based on object identity is the same as comparing sessionCreationTime.
     *
     * @param o the object to be compared.
     * @return a negative integer, zero, or a positive integer as this session is less than, equal to, or greater than
     * the specified object.
     */
    @Override
    public int compareTo(BlockStreamProducerSession o) {
        return Long.compare(sessionCreationTime, o.sessionCreationTime);
    }
}
