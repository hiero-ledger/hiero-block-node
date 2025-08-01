// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.stream.publisher;

import static java.lang.System.Logger.Level.INFO;
import static org.hiero.block.node.spi.BlockNodePlugin.METRICS_CATEGORY;
import static org.hiero.block.node.spi.BlockNodePlugin.UNKNOWN_BLOCK_NUMBER;

import com.hedera.hapi.block.stream.BlockProof;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.grpc.Pipeline;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.metrics.api.Counter;
import com.swirlds.metrics.api.Metrics;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.lang.System.Logger;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.atomic.AtomicLong;
import org.hiero.block.api.PublishStreamResponse;
import org.hiero.block.api.PublishStreamResponse.BlockAcknowledgement;
import org.hiero.block.api.PublishStreamResponse.EndOfStream;
import org.hiero.block.api.PublishStreamResponse.EndOfStream.Code;
import org.hiero.block.api.PublishStreamResponse.ResendBlock;
import org.hiero.block.api.PublishStreamResponse.SkipBlock;
import org.hiero.block.internal.BlockItemSetUnparsed;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.PublishStreamRequestUnparsed;
import org.hiero.block.node.stream.publisher.StreamPublisherManager.BlockAction;

/**
 * A handler for processing publish stream requests.
 * Each distinct publisher will have its own instance of this handler. Each
 * handler will be on it`s own separate thread. The handler is responsible for
 * processing incoming publish stream requests to the extent that it will
 * propagate incoming data to the {@link StreamPublisherManager} based on what
 * {@link BlockAction} the publisher manager returns for the current streaming
 * block. This handler will also return responses to the publisher it handles.
 * Calls made to the publisher manager are to be considered thread-safe and also
 * whatever action the manager returns for the current streaming block is to be
 * considered valid. Logic that needs to execute based on the action is safe to
 * execute in the handler thread. An important note is that an action needs to
 * be queried every time a new request is received
 * (the {@link #onNext(PublishStreamRequestUnparsed)} method is called) because
 * subsequent requests might be related to the same block and the status may
 * have changed since the last request, but also if we start streaming a new
 * block, the action for it might be different that what is usually expected,
 * especially true in a multi-publisher environment.
 */
public final class PublisherHandler implements Pipeline<PublishStreamRequestUnparsed> {
    private final Logger LOGGER = System.getLogger(getClass().getName());
    /** The replies pipeline, used for sending responses to the publisher. */
    private final Pipeline<? super PublishStreamResponse> replies;
    /** Metrics for the handlers. This instance is shared between all handlers. */
    private final MetricsHolder metrics;
    /** The publisher manager. We use it to make {@link BlockAction} queries. */
    private final StreamPublisherManager publisherManager;
    /** The transfer queue for propagating incoming items. Each handler has it`s own queue. */
    private final BlockingQueue<BlockItemSetUnparsed> blockItemsQueue;
    /** The ID of this handler. This is used to identify the handler within the publisher manager. */
    private final long handlerId;
    /** The current streaming block number. This is used to track the current block being streamed. */
    private final AtomicLong currentStreamingBlockNumber;
    /** The current block action. This is tracked to help the manager determine
     * what to do with the current block. */
    private BlockAction blockAction;

    /**
     * Initialize a new publisher handler.
     *
     * @param nextId the next handler ID to use
     * @param replyPipeline the pipeline to send replies to
     * @param handlerMetrics the metrics for this handler
     * @param manager the publisher manager that manages this handler
     * @param transferQueue the queue for transferring block items to the manager
     */
    public PublisherHandler(
            final long nextId,
            @NonNull final Pipeline<? super PublishStreamResponse> replyPipeline,
            @NonNull final MetricsHolder handlerMetrics,
            @NonNull final StreamPublisherManager manager,
            @NonNull final BlockingQueue<BlockItemSetUnparsed> transferQueue) {
        handlerId = nextId;
        replies = Objects.requireNonNull(replyPipeline);
        metrics = Objects.requireNonNull(handlerMetrics);
        publisherManager = Objects.requireNonNull(manager);
        blockItemsQueue = Objects.requireNonNull(transferQueue);
        currentStreamingBlockNumber = new AtomicLong(UNKNOWN_BLOCK_NUMBER);
    }

    // ==== Flow Methods =======================================================

    @Override
    public void onError(@NonNull final Throwable throwable) {
        // This is a "terminal" method, called when an _unrecoverable_ error
        // occurs. No other methods will be called by the Helidon layer after this.
        try {
            sendEndOfStream(Code.ERROR); // this might not succeed...
            // @todo(1416) update metrics
        } finally {
            // Shut down this handler, even if sending the message failed
            // or metrics failed.
            shutdown();
        }
    }

    @Override
    public void onComplete() {
        // This is mostly a cleanup method, called when the stream is complete
        // and `onNext` will not be called again.
        shutdown(); // @todo(1416) is this all the handling we need to do here?
    }

    @Override
    public void clientEndStreamReceived() {
        // called when the _gRPC layer_ receives an HTTP end stream from the client.
        // THIS IS NOT the same as the `EndStream` message in the API.
        shutdown(); // @todo(1416) is this all the handling we need to do here?
    }

    @Override
    public void onSubscribe(@NonNull final Subscription subscription) {
        // Check javadoc, this "starts" the subscription for this handler.
        // mostly we need to call subscription.request to start the flow of items.
        // @todo(1416) should we do something here?
    }

    @Override
    public void onNext(@NonNull final PublishStreamRequestUnparsed request) {
        if (request.hasBlockItems()) {
            final BlockItemSetUnparsed itemSetUnparsed = Objects.requireNonNull(request.blockItems());
            final List<BlockItemUnparsed> blockItems = itemSetUnparsed.blockItems();
            if (blockItems.isEmpty()) {
                try {
                    sendEndOfStream(Code.INVALID_REQUEST);
                    // @todo(1416) add metrics
                    resetState();
                } finally {
                    shutdown();
                }
            } else {
                handleBlockItemsRequest(itemSetUnparsed, blockItems);
            }
        } else if (request.hasEndStream()) {
            // @todo(1236) handle an endStream potentially received from publisher, for now we simply shutdown
            shutdown();
        } else {
            // this should never happen
            try {
                sendEndOfStream(Code.ERROR);
                // @todo(1416) add metrics
                resetState();
            } finally {
                shutdown();
            }
        }
    }

    private void handleBlockItemsRequest(
            final BlockItemSetUnparsed itemSetUnparsed, final List<BlockItemUnparsed> blockItems) {
        long blockNumber = currentStreamingBlockNumber.get();
        final BlockItemUnparsed first = blockItems.getFirst();
        // every time we receive an item set, we need to check if we have
        // a block header, if we do, we need to take the number and store it
        // in memory, this is now the current streaming block number.
        if (first.hasBlockHeader()) {
            // if we have a block header, this means that we are at the
            // start of a new block, so we can update the current streaming
            if (UNKNOWN_BLOCK_NUMBER == blockNumber) {
                final BlockHeader header;
                final Bytes headerBytes = first.blockHeader();
                if (headerBytes != null) {
                    try {
                        header = BlockHeader.PROTOBUF.parse(headerBytes);
                    } catch (final ParseException e) {
                        // if we have reached this block, this means that the
                        // request is invalid
                        try {
                            sendEndOfStream(Code.INVALID_REQUEST);
                            // @todo(1416) add metrics
                            resetState();
                        } finally {
                            shutdown();
                        }
                        return;
                    }
                } else {
                    // this should never happen
                    try {
                        sendEndOfStream(Code.ERROR);
                        // @todo(1416) add metrics
                        resetState();
                    } finally {
                        shutdown();
                    }
                    return;
                }
                blockNumber = header.number();
                // this means that we are starting a new block, so we can
                // update the current streaming block number
                currentStreamingBlockNumber.set(blockNumber);
            } else {
                // If we have entered here, we have an invalid request, the
                // block number is not reset which means that the block
                // from the request prior to this one has not been streamed in
                // full. Having a block header indicates that this request
                // starts streaming a new block, which should not be the case
                // if the previous block has not been streamed in full.
                try {
                    sendEndOfStream(Code.INVALID_REQUEST);
                    // @todo(1416) add metrics
                    resetState();
                } finally {
                    shutdown();
                }
                return;
            }
        } else if (UNKNOWN_BLOCK_NUMBER == blockNumber) {
            // here we should drop the batch, _this is normal_, and can happen
            // in many cases, including if the publisher sent several batches
            // for a block that should be skipped in the time it took for the
            // header batch to arrive and the "skip" response to be sent back,
            // due to network latency and processing time.
            // @todo(1416) add metrics
            // @todo(1413) add logs
            return;
        }
        // now we need to query the manager with the block number currently
        // being streamed, we will receive a response that will tell us
        // what to do with the items we have received, and we can trust that
        // no matter what the response is, we can safely take the appropriate
        // action. IMPORTANT: we need to do this check every time, even if
        // the current received set is in the middle of the batch, because
        // the response that is received from the manager might have changed.
        // query here
        blockAction = publisherManager.getActionForBlock(blockNumber, blockAction, handlerId);
        final BatchHandleResult handleResult =
                switch (blockAction) {
                    case ACCEPT -> handleAccept(itemSetUnparsed, blockItems);
                    case SKIP -> handleSkip(blockNumber);
                    case RESEND -> handleResend();
                    case END_BEHIND -> handleEndBehind();
                    case END_DUPLICATE -> handleEndDuplicate();
                    case END_ERROR -> handleEndError();
                };
        if (handleResult.shouldReset()) {
            resetState();
        }
        if (handleResult.shouldShutdown()) {
            shutdown();
        }
        // we must now check if the last item in the set is a proof.
        final BlockItemUnparsed last = blockItems.getLast();
        if (last.hasBlockProof()) {
            handleBlockProof(last.blockProof());
        }
    }

    /**
     * Handle a block proof received from the publisher.
     * <p>
     * This method is called when a block proof is received from the publisher.
     * It will parse the block proof and close the block in the publisher manager.
     * If parsing fails, it will log an error and close the block with a null proof.
     * <p>
     * It is important to note that a null proof sent to the close block method
     * does not prevent sending the proof into messaging, it just means the
     * publisher manager won't have the proof for metrics, logging, and
     * calculating whether other handlers are too slow and a resend must be
     * initiated.
     *
     * @param blockProofBytes the bytes of the block proof to handle
     */
    private void handleBlockProof(final Bytes blockProofBytes) {
        try {
            publisherManager.closeBlock(BlockProof.PROTOBUF.parse(blockProofBytes), handlerId);
        } catch (ParseException e) {
            publisherManager.closeBlock(null, handlerId);
            LOGGER.log(INFO, "Failed to parse block proof: {}", e.getMessage());
        }
        resetState();
    }

    // ==== Publisher Response Methods =========================================
    /**
     * Send an acknowledgement for the last block number that was persisted.
     * <p>
     * This method is called when the a block is persisted and verified
     * so we need to acknowledge it to the publisher. The acknowledgement
     * is sent as a response to the publisher, indicating that all blocks up to
     * and including the given block number are safely stored in this block node.
     *
     * @param newLastAcknowledgedBlockNumber the last block number that was
     *     verified and persisted.
     */
    public void sendAcknowledgement(final long newLastAcknowledgedBlockNumber) {
        // We only ever need to acknowledge once for a given block number, even
        // if there are several blocks "behind" that acknowledgement.
        // The publishers expect that acknowledgement for block N implicitly
        // acknowledges all blocks up to and including N.
        final BlockAcknowledgement ack = BlockAcknowledgement.newBuilder()
                .blockNumber(newLastAcknowledgedBlockNumber)
                .build();
        final PublishStreamResponse response =
                PublishStreamResponse.newBuilder().acknowledgement(ack).build();
        replies.onNext(response);
        metrics.blockAcknowledgementsSent.increment(); // @todo(1415) add label
    }

    private void sendEndOfStream(final EndOfStream.Code codeToSend) {
        final EndOfStream endOfStream = EndOfStream.newBuilder()
                .status(codeToSend)
                .blockNumber(publisherManager.getLatestBlockNumber())
                .build();
        final PublishStreamResponse response =
                PublishStreamResponse.newBuilder().endStream(endOfStream).build();
        replies.onNext(response);
        // @todo(1416) handle additional metrics outside of this method
        metrics.endOfStreamsSent.increment(); // @todo(1415) add label
    }

    // ==== Block Action Handling Methods ======================================

    /**
     * A batch handling result.
     * <p>
     * A simple record to return when handling a batch. This result informs
     * the caller whether the handler should shut down and/or reset its current
     * block action and current streaming block number.
     */
    private record BatchHandleResult(boolean shouldShutdown, boolean shouldReset) {}

    /**
     * Handle the ACCEPT action for a block.
     */
    private BatchHandleResult handleAccept(
            final BlockItemSetUnparsed itemSetUnparsed, final List<BlockItemUnparsed> blockItems) {
        // If the action is ACCEPT, we can safely propagate the items
        // to the manager.
        // @todo(1416) Ignoring this return value can result in lost data.
        //   An alternative is to use `put`, which might block, but won't
        //   just fail to add the item set.
        // @todo(1416) need to check if the item was added and handle "not added".
        blockItemsQueue.offer(itemSetUnparsed);
        final int itemsReceived = blockItems.size();
        metrics.liveBlockItemsReceived.add(itemsReceived); // @todo(1415) add label
        return new BatchHandleResult(false, false);
    }

    /**
     * Handle the SKIP action for a block.
     */
    private BatchHandleResult handleSkip(final long blockNumber) {
        // If the action is SKIP, we need to send a skip response
        // to the publisher and not propagate the items.
        final SkipBlock skipBlock =
                SkipBlock.newBuilder().blockNumber(blockNumber).build();
        final PublishStreamResponse response =
                PublishStreamResponse.newBuilder().skipBlock(skipBlock).build();
        replies.onNext(response);
        metrics.blockSkipsSent.increment(); // @todo(1415) add label
        return new BatchHandleResult(false, true);
    }

    /**
     * Handle the RESEND action for a block.
     */
    private BatchHandleResult handleResend() {
        // If the action is RESEND, we need to send a resend
        // response to the publisher and not propagate the items.
        final ResendBlock resendBlock = ResendBlock.newBuilder()
                .blockNumber(publisherManager.getLatestBlockNumber() + 1L)
                .build();
        final PublishStreamResponse response =
                PublishStreamResponse.newBuilder().resendBlock(resendBlock).build();
        replies.onNext(response);
        metrics.blockResendsSent.increment(); // @todo(1415) add label
        return new BatchHandleResult(false, true);
    }

    /**
     * Handle the END_BEHIND action for a block.
     */
    private BatchHandleResult handleEndBehind() {
        // If the action is END_BEHIND, we need to send an end of stream
        // response to the publisher and not propagate the items.
        sendEndOfStream(Code.BEHIND);
        return new BatchHandleResult(true, true);
    }

    /**
     * Handle the END_DUPLICATE action for a block.
     */
    private BatchHandleResult handleEndDuplicate() {
        // If the action is END_DUPLICATE, we need to send an end of stream
        // response to the publisher and not propagate the items.
        sendEndOfStream(Code.DUPLICATE_BLOCK);
        return new BatchHandleResult(true, true);
    }

    /**
     * Handle the END_ERROR action for a block.
     */
    private BatchHandleResult handleEndError() {
        // If the action is END_ERROR, we need to send an end of stream
        // response to the publisher and not propagate the items.
        sendEndOfStream(Code.ERROR);
        metrics.streamErrors.increment(); // @todo(1415) add label
        return new BatchHandleResult(true, true);
    }

    /**
     * This method will reset the state of the handler. Block action will be
     * set to null, and the current streaming block number will be set to
     * {@value org.hiero.block.node.spi.BlockNodePlugin#UNKNOWN_BLOCK_NUMBER}.
     */
    private void resetState() {
        // reset the block action
        blockAction = null;
        // reset the current streaming block number
        currentStreamingBlockNumber.set(UNKNOWN_BLOCK_NUMBER);
    }

    /**
     * This method is called when we want to orderly shut down the handler.
     * Any cleanup that is needed should be done here.
     */
    private void shutdown() {
        // reset the block action
        blockAction = null;
        // This method is called when the handler is removed from the manager.
        // We should clean up any resources that are no longer needed.
        publisherManager.removeHandler(handlerId);
    }

    // ==== Metrics ============================================================
    /**
     * Metrics for tracking publisher handler activity:
     * <pre>
     * {@link #liveBlockItemsReceived} - Count of live block items received from a producer
     * {@link #blockAcknowledgementsSent} - Count of acknowledgements sent
     * {@link #streamErrors} - Count of stream errors
     * {@link #blockSkipsSent} - Count of block skip responses
     * {@link #blockResendsSent} - Count of block resend responses
     * {@link #endOfStreamsSent} - Count of end of stream responses (should always be at most 1 per stream)
     * {@link #endStreamsReceived} - Count of end streams received (should always be at most 1 per stream)
     * </pre>
     */
    public record MetricsHolder(
            Counter liveBlockItemsReceived,
            Counter blockAcknowledgementsSent,
            Counter streamErrors,
            Counter blockSkipsSent,
            Counter blockResendsSent,
            Counter endOfStreamsSent,
            Counter endStreamsReceived) {
        /**
         * Factory method.
         * Creates a new instance of {@link MetricsHolder} using the provided
         * {@link Metrics} instance.
         * @return a new, valid, fully initialized {@link MetricsHolder} instance
         */
        static MetricsHolder createMetrics(@NonNull final Metrics metrics) {
            final Counter liveBlockItemsReceived =
                    metrics.getOrCreate(new Counter.Config(METRICS_CATEGORY, "publisher_block_items_received")
                            .withDescription("Live block items received"));
            final Counter blockAcknowledgementsSent =
                    metrics.getOrCreate(new Counter.Config(METRICS_CATEGORY, "publisher_blocks_ack_sent")
                            .withDescription("Block‑ack messages sent"));
            final Counter streamErrors =
                    metrics.getOrCreate(new Counter.Config(METRICS_CATEGORY, "publisher_stream_errors")
                            .withDescription("Publisher connection streams that end in an error"));
            final Counter blockSkipsSent =
                    metrics.getOrCreate(new Counter.Config(METRICS_CATEGORY, "publisher_blocks_skips_sent")
                            .withDescription("Block‑ack skips sent"));
            final Counter blockResendsSent =
                    metrics.getOrCreate(new Counter.Config(METRICS_CATEGORY, "publisher_blocks_resend_sent")
                            .withDescription("Block Resend messages sent"));
            final Counter endOfStreamsSent =
                    metrics.getOrCreate(new Counter.Config(METRICS_CATEGORY, "publisher_block_endofstream_sent")
                            .withDescription("Block End-of-Stream messages sent"));
            final Counter endStreamsReceived =
                    metrics.getOrCreate(new Counter.Config(METRICS_CATEGORY, "publisher_block_endstream_received")
                            .withDescription("Block End-Stream messages received"));
            return new MetricsHolder(
                    liveBlockItemsReceived,
                    blockAcknowledgementsSent,
                    streamErrors,
                    blockSkipsSent,
                    blockResendsSent,
                    endOfStreamsSent,
                    endStreamsReceived);
        }
    }
}
