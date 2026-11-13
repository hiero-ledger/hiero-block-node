// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.base.client;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.WARNING;
import static java.util.Objects.requireNonNull;
import static org.hiero.block.api.BlockStreamSubscribeServiceInterface.FULL_NAME;
import static org.hiero.block.node.base.ParseHelper.standardParse;

import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.grpc.GrpcCall;
import com.hedera.pbj.runtime.grpc.GrpcClient;
import com.hedera.pbj.runtime.grpc.Pipeline;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
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
    private final long awaitTimeoutMs;

    /**
     * Constructs a new client for subscribing to block streams.
     *
     * @param grpcClient the gRPC client to use for communication
     */
    public BlockStreamSubscribeUnparsedClient(@NonNull final GrpcClient grpcClient, long awaitTimeoutMs) {
        this.grpcClient = requireNonNull(grpcClient);
        this.awaitTimeoutMs = awaitTimeoutMs;
    }

    /**
     * Outcome of a batch request.
     * <p>
     * A request can end normally with fewer blocks than requested, so a caller must compare the size of
     * {@code blocks} with the requested range rather than rely on {@code error} alone.
     *
     * @param blocks the complete blocks received, in order from the requested start block with no holes
     * @param error  the reason the request ended early, or {@code null} if the stream ended normally
     */
    public record BatchResult(
            @NonNull List<BlockUnparsed> blocks, @Nullable Throwable error) {}

    /**
     * Subscribes to a closed range of blocks and returns the blocks received once the request ends.
     * A failure of the stream does not discard the blocks that arrived before it.
     *
     * @param startBlockNumber inclusive start
     * @param endBlockNumber   inclusive end
     * @return the blocks received and the error that ended the request early, if any
     * @throws IllegalArgumentException on invalid range
     */
    public BatchResult getBatchOfBlocks(long startBlockNumber, long endBlockNumber) {
        // Validate input parameters
        if (startBlockNumber < 0 || endBlockNumber < 0 || startBlockNumber > endBlockNumber) {
            throw new IllegalArgumentException("Invalid block range: " + startBlockNumber + " to " + endBlockNumber);
        }

        // Build per-request context
        final RequestContext ctx = new RequestContext(startBlockNumber, endBlockNumber);

        // Create request
        final SubscribeStreamRequest request = SubscribeStreamRequest.newBuilder()
                .startBlockNumber(startBlockNumber)
                .endBlockNumber(endBlockNumber)
                .build();

        // Create a per-request pipeline that closes over `ctx`
        final Pipeline<SubscribeStreamResponseUnparsed> pipeline = new SubscribePipeline(ctx);

        // Issue the call using the per-request pipeline. A failure to open the stream ends the request
        // like any other terminal event, so the caller gets a result with no blocks instead of an exception.
        try {
            final GrpcCall<SubscribeStreamRequest, SubscribeStreamResponseUnparsed> call = grpcClient.createCall(
                    FULL_NAME + "/subscribeBlockStream",
                    SubscribeStreamRequest.PROTOBUF,
                    SubscribeStreamResponseUnparsed.PROTOBUF,
                    pipeline,
                    Map.of());
            call.sendRequest(request, true);
        } catch (RuntimeException e) {
            LOGGER.log(DEBUG, "Failed to open the block stream", e);
            ctx.fail(e);
        }

        // Wait for completion or error and return the blocks
        return ctx.await(awaitTimeoutMs);
    }

    /**
     * Extracts the block number from a block header item.
     */
    private static long extractBlockNumberFromBlockHeader(BlockItemUnparsed itemUnparsed) throws ParseException {
        return standardParse(BlockHeader.PROTOBUF, itemUnparsed.blockHeaderOrThrow())
                .number();
    }

    /**
     * Per-request state holder. All fields are confined to a single request.
     * The {@link CountDownLatch} establishes happens-before from callback threads to the waiter when a
     * callback ends the request; the {@link ConcurrentLinkedQueue} does so for each block when the waiter
     * ends it.
     */
    private static final class RequestContext {
        private static final Object COMPLETED = new Object();

        final CountDownLatch done = new CountDownLatch(1);
        final long startBlock;
        final long endBlock;
        long expectedBlockNumber;
        List<BlockItemUnparsed> currentBlockItems = new ArrayList<>();
        boolean blockStarted;
        /// The complete blocks, in the order received. Only the callback thread adds to it, so it always holds
        /// a contiguous run from the start block, and the waiter can copy it at any time.
        private final ConcurrentLinkedQueue<BlockUnparsed> blocks = new ConcurrentLinkedQueue<>();
        /// How the request ended: [#COMPLETED] or the failure. Set once, by whichever of [#complete()]
        /// and [#fail(Throwable)] gets there first.
        private final AtomicReference<Object> outcome = new AtomicReference<>();

        RequestContext(long startBlock, long endBlock) {
            this.startBlock = startBlock;
            this.endBlock = endBlock;
            this.expectedBlockNumber = startBlock;
        }

        boolean isFinished() {
            return outcome.get() != null;
        }

        /// Adds a complete block.
        void addBlock(BlockUnparsed block) {
            blocks.add(block);
        }

        void fail(Throwable t) {
            finish(t);
        }

        void complete() {
            finish(COMPLETED);
        }

        private void finish(Object result) {
            if (outcome.compareAndSet(null, result)) {
                done.countDown();
            }
        }

        BatchResult await(long timeoutMs) {
            try {
                if (timeoutMs > 0) {
                    if (!done.await(timeoutMs, TimeUnit.MILLISECONDS)) {
                        final String msg =
                                "BACKFILL_GRPC_OVERALL_TIMEOUT=%d, exceeded for Block range starting with block %d"
                                        .formatted(timeoutMs, expectedBlockNumber);
                        // Warn that the BACKFILL_GRPC_OVERALL_TIMEOUT was exceeded
                        LOGGER.log(WARNING, msg);
                        fail(new RuntimeException("Timed out waiting for block stream response"));
                    }
                } else {
                    done.await();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                fail(e);
            }
            // The request has ended here, so the outcome is final. A block the callback thread was still adding
            // when the request ended is returned if it lands before this copy; later blocks are not returned.
            final Throwable error = outcome.get() instanceof Throwable failure ? failure : null;
            return new BatchResult(List.copyOf(blocks), error);
        }
    }

    /**
     * Pipeline implementation for handling responses from the block stream subscription.
     * It processes incoming {@link SubscribeStreamResponseUnparsed} messages and manages
     * the state of the request context.
     */
    private static final class SubscribePipeline implements Pipeline<SubscribeStreamResponseUnparsed> {
        private final RequestContext ctx;

        /**
         * Constructs a new pipeline for processing block stream subscription responses.
         *
         * @param ctx the request context to manage state across callbacks
         */
        SubscribePipeline(RequestContext ctx) {
            this.ctx = ctx;
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            LOGGER.log(DEBUG, "received onSubscribe confirmation");
            // No backpressure negotiation needed for this pattern.
        }

        @Override
        public void onNext(SubscribeStreamResponseUnparsed resp) {
            // Once the request has completed or failed, nothing more is buffered and a block that was
            // only partly received is dropped.
            if (ctx.isFinished()) {
                ctx.currentBlockItems.clear();
                return;
            }
            try {
                if (resp.hasBlockItems()) {
                    final List<BlockItemUnparsed> frame = resp.blockItems().blockItems();
                    if (frame.getFirst().hasBlockHeader()) {
                        final long expected = ctx.expectedBlockNumber;
                        final long actual = extractBlockNumberFromBlockHeader(frame.getFirst());
                        if (actual < ctx.startBlock || actual > ctx.endBlock) {
                            ctx.fail(new IllegalStateException("Received block number " + actual
                                    + " outside the requested range " + ctx.startBlock + " to " + ctx.endBlock));
                            return;
                        }
                        if (actual != expected) {
                            ctx.fail(new IllegalStateException(
                                    "Expected block number " + expected + " but received " + actual));
                            return;
                        }
                        // Start a new block: reuse the buffer and populate it.
                        ctx.currentBlockItems.clear();
                        ctx.blockStarted = true;
                    } else if (!ctx.blockStarted) {
                        ctx.fail(new IllegalStateException("Received block items before a block header"));
                        return;
                    }
                    // Continuation: append everything to the buffer.
                    ctx.currentBlockItems.addAll(frame);
                } else if (resp.hasStatus()) {
                    final SubscribeStreamResponse.Code code = resp.status();
                    if (code != SubscribeStreamResponse.Code.SUCCESS) {
                        ctx.fail(new RuntimeException("Received error code: " + code));
                    }
                } else if (resp.hasEndOfBlock()) {
                    if (!ctx.blockStarted) {
                        ctx.fail(new IllegalStateException("Received end of block without a block header"));
                        return;
                    }
                    // Snapshot the current items to avoid retaining the large buffer in the finished block.
                    final List<BlockItemUnparsed> snapshot = List.copyOf(ctx.currentBlockItems);
                    ctx.addBlock(BlockUnparsed.newBuilder().blockItems(snapshot).build());
                    ctx.currentBlockItems.clear();
                    ctx.blockStarted = false;
                    ctx.expectedBlockNumber++;
                    if (ctx.expectedBlockNumber > ctx.endBlock) {
                        ctx.complete();
                    }
                } else {
                    ctx.fail(new RuntimeException("Received unexpected response without block items or code"));
                }
            } catch (ParseException e) {
                LOGGER.log(DEBUG, "Parse error in block item", e);
                ctx.fail(e);
            } catch (RuntimeException e) {
                LOGGER.log(DEBUG, "Runtime error processing SubscribeStreamResponseUnparsed", e);
                ctx.fail(e);
            }
        }

        @Override
        public void onError(Throwable throwable) {
            // After the request ended this is expected, typically the connection of an abandoned stream closing.
            LOGGER.log(ctx.isFinished() ? DEBUG : WARNING, "received onError", throwable);
            ctx.fail(throwable);
        }

        @Override
        public void onComplete() {
            LOGGER.log(DEBUG, "received onComplete");
            ctx.complete();
        }
    }
}
