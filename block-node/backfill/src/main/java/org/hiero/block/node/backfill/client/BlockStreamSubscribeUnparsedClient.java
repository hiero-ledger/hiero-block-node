// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.backfill.client;

import static java.lang.System.Logger.Level.TRACE;
import static java.util.Objects.requireNonNull;
import static org.hiero.block.api.BlockStreamSubscribeServiceInterface.FULL_NAME;

import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.pbj.runtime.Codec;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.UncheckedParseException;
import com.hedera.pbj.runtime.grpc.GrpcCall;
import com.hedera.pbj.runtime.grpc.GrpcClient;
import com.hedera.pbj.runtime.grpc.Pipeline;
import com.hedera.pbj.runtime.grpc.ServiceInterface;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Flow;
import org.hiero.block.api.SubscribeStreamRequest;
import org.hiero.block.api.SubscribeStreamResponse;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.internal.SubscribeStreamResponseUnparsed;

/**
 * Client for subscribing to block streams using unparsed responses.
 * <p>
 * This implementation is <b>request-isolated</b>:
 * it does not keep per-request mutable state on {@code this}. Each call to
 * {@link #getBatchOfBlocks(long, long)} creates a per-request {@code RequestContext}
 * and a per-request {@link Pipeline} that closes over that context.
 * This design prevents late callbacks from one request from mutating the state of another
 * and avoids the need for per-field atomic types.
 *
 * <p><b>Thread-safety:</b> The instance is stateless across requests. You may invoke
 * {@code getBatchOfBlocks} concurrently from multiple threads; each invocation uses
 * its own context and pipeline.
 */
public class BlockStreamSubscribeUnparsedClient {

    private static final System.Logger LOGGER = System.getLogger(BlockStreamSubscribeUnparsedClient.class.getName());

    // From constructor
    private final GrpcClient grpcClient;
    private final ServiceInterface.RequestOptions requestOptions;

    public BlockStreamSubscribeUnparsedClient(
            @NonNull final GrpcClient grpcClient, @NonNull final ServiceInterface.RequestOptions requestOptions) {
        this.grpcClient = requireNonNull(grpcClient);
        this.requestOptions = requireNonNull(requestOptions);
    }

    /**
     * Subscribes to a closed range of blocks and returns them as a list once the stream completes.
     *
     * @param startBlockNumber inclusive start
     * @param endBlockNumber   inclusive end
     * @return list of received blocks (never {@code null})
     * @throws IllegalArgumentException on invalid range
     * @throws RuntimeException on stream error or interruption
     */
    public List<BlockUnparsed> getBatchOfBlocks(long startBlockNumber, long endBlockNumber) {
        // Validate input parameters
        if (startBlockNumber < 0 || endBlockNumber < 0 || startBlockNumber > endBlockNumber) {
            throw new IllegalArgumentException("Invalid block range: " + startBlockNumber + " to " + endBlockNumber);
        }

        // Build per-request context
        final RequestContext ctx = new RequestContext(startBlockNumber);

        // Create request
        final SubscribeStreamRequest request = SubscribeStreamRequest.newBuilder()
                .startBlockNumber(startBlockNumber)
                .endBlockNumber(endBlockNumber)
                .build();

        // Create a per-request pipeline that closes over `ctx`
        final Pipeline<SubscribeStreamResponseUnparsed> pipeline = new Pipeline<>() {
            @Override
            public void onSubscribe(Flow.Subscription subscription) {
                LOGGER.log(TRACE, "received onSubscribe confirmation");
                // No backpressure negotiation needed for this pattern.
            }

            @Override
            public void onNext(SubscribeStreamResponseUnparsed subscribeStreamResponse) {
                if (subscribeStreamResponse.hasBlockItems()) {
                    final List<BlockItemUnparsed> blockItems =
                            subscribeStreamResponse.blockItems().blockItems();

                    // Start of a new block
                    if (blockItems.getFirst().hasBlockHeader()) {
                        final long expected = ctx.expectedBlockNumber;
                        final long actual = extractBlockNumberFromBlockHeader(blockItems.getFirst());
                        if (actual != expected) {
                            ctx.fail(new IllegalStateException(
                                    "Expected block number " + expected + " but received " + actual));
                            return;
                        }
                        // Begin a new block with the header and following items in this frame
                        ctx.currentBlockItems = new ArrayList<>(blockItems);
                    } else {
                        // Continuation of the current block
                        ctx.currentBlockItems.addAll(blockItems);
                    }

                    // End of block
                    if (blockItems.getLast().hasBlockProof()) {
                        ctx.blocks.add(BlockUnparsed.newBuilder()
                                .blockItems(ctx.currentBlockItems)
                                .build());
                        ctx.currentBlockItems = new ArrayList<>();
                        ctx.expectedBlockNumber++;
                    }

                } else if (subscribeStreamResponse.hasStatus()) {
                    final SubscribeStreamResponse.Code code = subscribeStreamResponse.status();
                    if (code != SubscribeStreamResponse.Code.SUCCESS) {
                        ctx.fail(new RuntimeException("Received error code: " + code));
                    }
                } else {
                    ctx.fail(new RuntimeException("Received unexpected response without block items or code"));
                }
            }

            @Override
            public void onError(Throwable throwable) {
                LOGGER.log(TRACE, "received onError", throwable);
                ctx.fail(throwable);
            }

            @Override
            public void onComplete() {
                LOGGER.log(TRACE, "received onComplete");
                ctx.complete();
            }
        };

        // Issue the call using the per-request pipeline
        final GrpcCall<SubscribeStreamRequest, SubscribeStreamResponseUnparsed> call = grpcClient.createCall(
                FULL_NAME + "/subscribeBlockStream",
                getSubscribeStreamRequestCodec(requestOptions),
                getSubscribeStreamResponseUnparsedCodec(requestOptions),
                pipeline);

        call.sendRequest(request, true);

        // Wait for completion or error and return the blocks
        return ctx.await();
    }

    /**
     * Extracts the block number from a block header item.
     */
    private static long extractBlockNumberFromBlockHeader(BlockItemUnparsed itemUnparsed) {
        try {
            return BlockHeader.PROTOBUF.parse(itemUnparsed.blockHeaderOrThrow()).number();
        } catch (ParseException e) {
            throw new UncheckedParseException(e);
        }
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

    /**
     * Per-request state holder. All fields are confined to a single request.
     * The {@link CountDownLatch} establishes happens-before from callback threads to the waiter.
     */
    private static final class RequestContext {
        final CountDownLatch done = new CountDownLatch(1);
        final List<BlockUnparsed> blocks = new ArrayList<>();
        long expectedBlockNumber;
        List<BlockItemUnparsed> currentBlockItems = new ArrayList<>();
        Throwable error;

        RequestContext(long startBlock) {
            this.expectedBlockNumber = startBlock;
        }

        void fail(Throwable t) {
            this.error = t;
            done.countDown();
        }

        void complete() {
            done.countDown();
        }

        List<BlockUnparsed> await() {
            try {
                done.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while waiting for blocks", e);
            }
            if (error != null) {
                throw new RuntimeException("Error fetching blocks", error);
            }
            return blocks;
        }
    }
}
