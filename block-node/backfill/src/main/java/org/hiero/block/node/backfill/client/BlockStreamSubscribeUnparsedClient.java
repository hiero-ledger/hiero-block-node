// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.backfill.client;

import static java.lang.System.Logger.Level.TRACE;
import static java.util.Objects.requireNonNull;
import static org.hiero.block.api.BlockStreamSubscribeServiceInterface.FULL_NAME;

import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.pbj.runtime.Codec;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.grpc.GrpcCall;
import com.hedera.pbj.runtime.grpc.GrpcClient;
import com.hedera.pbj.runtime.grpc.Pipeline;
import com.hedera.pbj.runtime.grpc.ServiceInterface;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.hiero.block.api.SubscribeStreamRequest;
import org.hiero.block.api.SubscribeStreamResponse;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.internal.SubscribeStreamResponseUnparsed;

/**
 * Client for subscribing to block streams using unparsed responses.
 * This client handles the subscription and processes incoming block items,
 * accumulating them into blocks until a complete block is received.
 * <p>
 * Might be thought as a similar to BlockNodeServiceInterface.BlockStreamSubscribeServiceClient
 * but specifically for unparsed responses and with convenience abstractions for handling closed range
 * requests and responses without dealing directly with streams and pipelines.
 */
public class BlockStreamSubscribeUnparsedClient implements Pipeline<SubscribeStreamResponseUnparsed> {
    // Logger
    private static final System.Logger LOGGER = System.getLogger(BlockStreamSubscribeUnparsedClient.class.getName());

    // from constructor
    private final GrpcClient grpcClient;
    private final ServiceInterface.RequestOptions requestOptions;

    // Per Request State
    private List<BlockItemUnparsed> currentBlockItems;
    private AtomicLong currentBlockNumber;
    private AtomicReference<List<BlockUnparsed>> replyRef;
    private AtomicReference<Throwable> errorRef;
    private CountDownLatch latch;

    public BlockStreamSubscribeUnparsedClient(
            @NonNull final GrpcClient grpcClient, @NonNull final ServiceInterface.RequestOptions requestOptions) {
        this.grpcClient = requireNonNull(grpcClient);
        this.requestOptions = requireNonNull(requestOptions);
    }

    /**
     * Subscribes to a batch of blocks from the block stream.
     * <p>
     * This method sends a request to subscribe to a range of blocks specified by the start and end block numbers.
     * It waits for the whole response, accumulating block items into blocks, until the end block is reached or an error occurs.
     * Then it returns a list of blocks that were received during the subscription.
     * <p>
     * meant to be used for closed ranges of blocks where start and end are both specified and used within a specific batch size
     * that avoids overwhelming the client with too many blocks at once. and long wait times.
     *
     * @param startBlockNumber the starting block number (inclusive)
     * @param endBlockNumber the ending block number (inclusive)
     * @return a list of blocks received during the subscription
     * @throws IllegalArgumentException if the start or end block number is invalid or endBlock is less than startBlock
     */
    public List<BlockUnparsed> getBatchOfBlocks(long startBlockNumber, long endBlockNumber) {
        // Validate input parameters
        if (startBlockNumber < 0 || endBlockNumber < 0 || startBlockNumber > endBlockNumber) {
            throw new IllegalArgumentException("Invalid block range: " + startBlockNumber + " to " + endBlockNumber);
        }
        // only start a new request if previous one is not in progress
        if (latch != null && latch.getCount() > 0) {
            // wait for response or error
            try {
                latch.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while waiting for blocks", e);
            }
        }

        // reset state for the request
        currentBlockItems = new ArrayList<>();
        currentBlockNumber = new AtomicLong(startBlockNumber);
        replyRef = new AtomicReference<>();
        errorRef = new AtomicReference<>();
        latch = new CountDownLatch(1);
        // Create request
        SubscribeStreamRequest request = SubscribeStreamRequest.newBuilder()
                .startBlockNumber(startBlockNumber)
                .endBlockNumber(endBlockNumber)
                .build();
        // Call
        final GrpcCall<SubscribeStreamRequest, SubscribeStreamResponseUnparsed> call = grpcClient.createCall(
                FULL_NAME + "/subscribeBlockStream",
                getSubscribeStreamRequestCodec(requestOptions),
                getSubscribeStreamResponseUnparsedCodec(requestOptions),
                this);
        call.sendRequest(request, true);

        // wait for response or error
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while waiting for blocks", e);
        }
        if (errorRef.get() != null) {
            throw new RuntimeException("Error fetching blocks", errorRef.get());
        }
        return replyRef.get();
    }

    /**
     * Does nothing on subscription.
     * @param subscription a new subscription
     */
    @Override
    public void onSubscribe(Flow.Subscription subscription) {
        LOGGER.log(TRACE, "received onSubscribe confirmation");
        // No action needed on subscription
    }

    /**
     * Handles incoming SubscribeStreamResponseUnparsed messages.
     * Accumulates block items into blocks and manages state transitions.
     * <p>
     * Processes the response by checking if it contains block items or status codes.
     * If it contains block items, it checks if the first item is a block header,
     * indicating the start of a new block. It verifies the block number and accumulates items
     * into the current block.
     * If the last item is a block proof, it finalizes the current block,
     * adds it to the reply list,
     * and resets the current block items and number for the next block.
     * If the response contains a status code, different from SUCCESS,
     * it sets the error reference accordingly.
     * @param subscribeStreamResponse the response to process
     */
    @Override
    public void onNext(SubscribeStreamResponseUnparsed subscribeStreamResponse) {
        if (subscribeStreamResponse.hasBlockItems()) {
            List<BlockItemUnparsed> blockItems =
                    subscribeStreamResponse.blockItems().blockItems();
            // Check if is new Block
            if (blockItems.getFirst().hasBlockHeader()) {
                // verify is the expected block number
                long expectedBlockNumber = currentBlockNumber.get();
                long actualBlockNumber = 0;
                try {
                    actualBlockNumber = BlockHeader.PROTOBUF
                            .parse(blockItems.getFirst().blockHeaderOrThrow())
                            .number();
                } catch (ParseException e) {
                    throw new RuntimeException(e);
                }
                if (actualBlockNumber != expectedBlockNumber) {
                    throw new IllegalStateException(
                            "Expected block number " + expectedBlockNumber + " but received " + actualBlockNumber);
                }
                // Create new Block and add to current block items
                currentBlockItems = new ArrayList<>(blockItems);
            } else {
                // Add items to current block
                currentBlockItems.addAll(blockItems);
            }

            // Check if response contains block proof (end of block)
            if (blockItems.getLast().hasBlockProof()) {
                // Create Block from current items
                BlockUnparsed block =
                        BlockUnparsed.newBuilder().blockItems(currentBlockItems).build();
                // Add to reply
                List<BlockUnparsed> blocks = replyRef.get();
                if (blocks == null) {
                    blocks = new ArrayList<>();
                    replyRef.set(blocks);
                }
                blocks.add(block);
                // Reset current block items and number for next block
                currentBlockItems = new ArrayList<>();
                currentBlockNumber.incrementAndGet();
            }

        } else if (subscribeStreamResponse.hasStatus()) {
            // If response has code, set the status
            SubscribeStreamResponse.Code codeStatus = subscribeStreamResponse.status();
            if (codeStatus != SubscribeStreamResponse.Code.SUCCESS) {
                errorRef.set(new RuntimeException("Received error code: " + codeStatus));
            }
        } else {
            // If no block items and no code, this is unexpected
            errorRef.set(new RuntimeException("Received unexpected response without block items or code"));
        }
    }

    /**
     * Handles errors during the subscription.
     * Sets the error reference and releases the latch to signal completion.
     * @param throwable the error encountered
     */
    @Override
    public void onError(Throwable throwable) {
        LOGGER.log(TRACE, "received onError", throwable);
        errorRef.set(throwable);
        replyRef.set(null);
        latch.countDown();
    }

    @Override
    public void onComplete() {
        LOGGER.log(TRACE, "received onComplete");
        latch.countDown();
    }

    private static Codec<SubscribeStreamRequest> getSubscribeStreamRequestCodec(
            @NonNull final ServiceInterface.RequestOptions options) {
        requireNonNull(options);

        // Default to protobuf, and don't error out if both are set:
        if (options.isJson() && !options.isProtobuf()) {
            return SubscribeStreamRequest.JSON;
        } else {
            return SubscribeStreamRequest.PROTOBUF;
        }
    }

    @NonNull
    private static Codec<SubscribeStreamResponseUnparsed> getSubscribeStreamResponseUnparsedCodec(
            @NonNull final ServiceInterface.RequestOptions options) {
        requireNonNull(options);

        // Default to protobuf, and don't error out if both are set:
        if (options.isJson() && !options.isProtobuf()) {
            return SubscribeStreamResponseUnparsed.JSON;
        } else {
            return SubscribeStreamResponseUnparsed.PROTOBUF;
        }
    }
}
