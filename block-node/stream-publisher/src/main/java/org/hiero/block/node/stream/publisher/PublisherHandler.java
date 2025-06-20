// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.stream.publisher;

import static org.hiero.block.node.spi.BlockNodePlugin.METRICS_CATEGORY;
import static org.hiero.block.node.spi.BlockNodePlugin.UNKNOWN_BLOCK_NUMBER;

import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.grpc.Pipeline;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.metrics.api.Counter;
import com.swirlds.metrics.api.Metrics;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.atomic.AtomicLong;
import org.hiero.block.api.PublishStreamResponse;
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

    /**
     * {@inheritDoc}
     */
    @Override
    public void onError(@NonNull final Throwable throwable) {
        // This is a "terminal" method, called when an _unrecoverable_ error
        // occurs. No other methods will be called by the Helidon layer after this.
        // todo is it ok to call handleEndError here directly? The logic is
        //  reusable as of now
        handleEndError();
        shutdown();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onComplete() {
        // This is mostly a cleanup method, called when the stream is complete
        // and `onNext` will not be called again.
        shutdown(); // todo is this all the handling we need to do here?
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void clientEndStreamReceived() {
        // called when the _gRPC layer_ receives an end stream from the client.
        // THIS IS NOT the same as the `EndStream` message in the API.
        shutdown(); // todo is this all the handling we need to do here?
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onSubscribe(@NonNull final Subscription subscription) {
        // Check javadoc, this "starts" the subscription for this handler.
        // mostly we need to call subscription.request to start the flow of items.
        // todo should we do something here?
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onNext(@NonNull final PublishStreamRequestUnparsed item) {
        final BlockItemSetUnparsed itemSetUnparsed = Objects.requireNonNull(item.blockItems());
        long blockNumber = currentStreamingBlockNumber.get();
        final List<BlockItemUnparsed> blockItems = itemSetUnparsed.blockItems();
        final BlockItemUnparsed first = blockItems.getFirst();
        // every time we receive an item set, we need to check if we have
        // a block header, if we do, we need to take the number and store it
        // in memory, this is now the current streaming block number.
        if (first.hasBlockHeader()) {
            // if we have a block header, this means that we are at the
            // start of a new block, so we can update the current streaming
            if (UNKNOWN_BLOCK_NUMBER == blockNumber) {
                final BlockHeader header = extractBlockHeader(first);
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
                final EndOfStream endOfStream = EndOfStream.newBuilder()
                        .status(Code.INVALID_REQUEST)
                        .blockNumber(publisherManager.getLatestBlockNumber())
                        .build();
                final PublishStreamResponse response = PublishStreamResponse.newBuilder()
                        .endStream(endOfStream)
                        .build();
                replies.onNext(response);
                metrics.endOfStreamsSent.increment(); // todo add label
                metrics.streamErrors.increment(); // todo add label
                shutdown();
                return;
            }
        } else if (UNKNOWN_BLOCK_NUMBER == blockNumber) {
            // here we should drop the batch
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
        blockAction = publisherManager.getActionForBlock(blockNumber, blockAction);
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
            resetCurrentBlockNumber();
        }
        if (handleResult.shouldShutdown()) {
            shutdown();
        }
        // @todo we need to handle the block proof here, if this batch ends with a block proof,
        //     Specifically, we need to reset the block action to null, and also reset the block number.
    }

    /**
     * Handle the ACCEPT action for a block.
     */
    private BatchHandleResult handleAccept(
            final BlockItemSetUnparsed itemSetUnparsed, final List<BlockItemUnparsed> blockItems) {
        // If the action is ACCEPT, we can safely propagate the items
        // to the manager.

        // todo Ignoring this return value can result in lost data.
        //   An alternative is to use `put`, which might block, but won't
        //   just fail to add the item set.
        // @todo need to check if the item was added and handle "not added".
        blockItemsQueue.offer(itemSetUnparsed);
        final int itemsReceived = blockItems.size();
        metrics.liveBlockItemsReceived.add(itemsReceived); // todo add label
        // we must now check if the last item in the set is a proof.
        final BlockItemUnparsed last = blockItems.getLast();
        if (last.hasBlockProof()) {
            // we need to always check if the last item in the set has a
            // block proof. If we received a block proof, this means that
            // we have reached the end of a block, and we update the current
            // streaming block number to -1. Then when reading the block
            // header next time, if the value is not -1, we know that something
            // is wrong.
            return new BatchHandleResult(false, true);
        }
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
        metrics.blockSkipsSent.increment(); // todo add label
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
        metrics.blockResendsSent.increment(); // todo add label
        return new BatchHandleResult(false, true);
    }

    /**
     * Handle the END_BEHIND action for a block.
     */
    private BatchHandleResult handleEndBehind() {
        // If the action is END_BEHIND, we need to send an end of stream
        // response to the publisher and not propagate the items.
        final long latestPersistedBlockNumber = publisherManager.getLatestBlockNumber();
        final EndOfStream endOfStream = EndOfStream.newBuilder()
                .status(Code.BEHIND)
                .blockNumber(latestPersistedBlockNumber)
                .build();
        final PublishStreamResponse response =
                PublishStreamResponse.newBuilder().endStream(endOfStream).build();
        replies.onNext(response);
        metrics.endOfStreamsSent.increment(); // todo add label
        return new BatchHandleResult(true, true);
    }

    /**
     * Handle the END_DUPLICATE action for a block.
     */
    private BatchHandleResult handleEndDuplicate() {
        // If the action is END_DUPLICATE, we need to send an end of stream
        // response to the publisher and not propagate the items.
        final long latestPersistedBlockNumber = publisherManager.getLatestBlockNumber();
        final EndOfStream endOfStream = EndOfStream.newBuilder()
                .status(Code.DUPLICATE_BLOCK)
                .blockNumber(latestPersistedBlockNumber)
                .build();
        final PublishStreamResponse response =
                PublishStreamResponse.newBuilder().endStream(endOfStream).build();
        replies.onNext(response);
        metrics.endOfStreamsSent.increment(); // todo add label
        return new BatchHandleResult(true, true);
    }

    /**
     * Handle the END_ERROR action for a block.
     */
    private BatchHandleResult handleEndError() {
        // If the action is END_ERROR, we need to send an end of stream
        // response to the publisher and not propagate the items.
        final EndOfStream endOfStream = EndOfStream.newBuilder()
                .status(Code.ERROR)
                .blockNumber(publisherManager.getLatestBlockNumber())
                .build();
        final PublishStreamResponse response =
                PublishStreamResponse.newBuilder().endStream(endOfStream).build();
        replies.onNext(response);
        metrics.endOfStreamsSent.increment(); // todo add label
        metrics.streamErrors.increment(); // todo add label
        return new BatchHandleResult(true, true);
    }

    /**
     * Reset the current streaming block number to the unknown block number.
     * This is important because it flags that we are ready to receive a new
     * block.
     */
    private void resetCurrentBlockNumber() {
        currentStreamingBlockNumber.set(UNKNOWN_BLOCK_NUMBER);
    }

    /**
     * This method extracts the block header from a given
     * {@link BlockItemUnparsed}. It should be verified prior to calling this
     * method that the block item has a block header.
     *
     * @param blockItem the block item to extract the header from, must not be
     * null and must contain a block header
     * @return the extracted block header
     */
    private BlockHeader extractBlockHeader(@NonNull final BlockItemUnparsed blockItem) {
        // todo headerBytes could be null, how to handle this?
        final Bytes headerBytes = blockItem.blockHeader();
        if (headerBytes != null) {
            try {
                return BlockHeader.PROTOBUF.parse(headerBytes);
            } catch (final ParseException e) {
                // todo how to handle this? send a end of stream bad request!
                throw new RuntimeException(e);
            }
        } else {
            // this should never happen
            // todo how to handle this? send a end of stream bad request or internal error?!
            throw new UnsupportedOperationException("todo implement how to handle null block header bytes");
        }
    }

    /**
     * This method is called when we want to orderly shut down the handler.
     * Any cleanup that is needed should be done here.
     */
    private void shutdown() {
        // This method is called when the handler is removed from the manager.
        // We should clean up any resources that are no longer needed.
        publisherManager.removeHandler(handlerId);
    }

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

    private record BatchHandleResult(boolean shouldShutdown, boolean shouldReset) {}
}
