// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.stream.publisher;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.TRACE;
import static java.lang.System.Logger.Level.WARNING;
import static org.hiero.block.node.spi.BlockNodePlugin.UNKNOWN_BLOCK_NUMBER;
import static org.hiero.block.node.stream.publisher.StreamPublisherPlugin.METRIC_PUBLISHER_BLOCKS_ACK_SENT;
import static org.hiero.block.node.stream.publisher.StreamPublisherPlugin.METRIC_PUBLISHER_BLOCKS_RESEND_SENT;
import static org.hiero.block.node.stream.publisher.StreamPublisherPlugin.METRIC_PUBLISHER_BLOCKS_SKIPS_SENT;
import static org.hiero.block.node.stream.publisher.StreamPublisherPlugin.METRIC_PUBLISHER_BLOCK_ENDOFSTREAM_SENT;
import static org.hiero.block.node.stream.publisher.StreamPublisherPlugin.METRIC_PUBLISHER_BLOCK_ENDSTREAM_RECEIVED;
import static org.hiero.block.node.stream.publisher.StreamPublisherPlugin.METRIC_PUBLISHER_BLOCK_ITEMS_RECEIVED;
import static org.hiero.block.node.stream.publisher.StreamPublisherPlugin.METRIC_PUBLISHER_BLOCK_NODE_BEHIND_SENT;
import static org.hiero.block.node.stream.publisher.StreamPublisherPlugin.METRIC_PUBLISHER_BLOCK_SEND_RESPONSE_FAILED;
import static org.hiero.block.node.stream.publisher.StreamPublisherPlugin.METRIC_PUBLISHER_RECEIVE_LATENCY_NS;
import static org.hiero.block.node.stream.publisher.StreamPublisherPlugin.METRIC_PUBLISHER_STREAM_ERRORS;
import static org.hiero.block.node.stream.publisher.StreamPublisherPlugin.METRIC_PUBLISHER_STREAM_SETS_DROPPED;

import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.grpc.Pipeline;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.net.SocketException;
import java.util.Deque;
import java.util.List;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.hiero.block.api.BlockEnd;
import org.hiero.block.api.PublishStreamRequest.EndStream;
import org.hiero.block.api.PublishStreamResponse;
import org.hiero.block.api.PublishStreamResponse.BehindPublisher;
import org.hiero.block.api.PublishStreamResponse.BlockAcknowledgement;
import org.hiero.block.api.PublishStreamResponse.EndOfStream;
import org.hiero.block.api.PublishStreamResponse.EndOfStream.Code;
import org.hiero.block.api.PublishStreamResponse.ResendBlock;
import org.hiero.block.api.PublishStreamResponse.ResponseOneOfType;
import org.hiero.block.api.PublishStreamResponse.SkipBlock;
import org.hiero.block.internal.BlockItemSetUnparsed;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.PublishStreamRequestUnparsed;
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.block.node.stream.publisher.StreamPublisherManager.ActionForBlock;
import org.hiero.block.node.stream.publisher.StreamPublisherManager.BlockAction;
import org.hiero.metrics.LongCounter;
import org.hiero.metrics.core.MetricRegistry;

/// A handler for processing publish stream requests.
/// Each distinct publisher will have its own instance of this handler. Each
/// handler will be on it`s own separate thread. The handler is responsible for
/// processing incoming publish stream requests to the extent that it will
/// propagate incoming data to the [StreamPublisherManager] based on what
/// [BlockAction] the publisher manager returns for the current streaming
/// block. This handler will also return responses to the publisher it handles.
/// Calls made to the publisher manager are to be considered thread-safe and also
/// whatever action the manager returns for the current streaming block is to be
/// considered valid. Logic that needs to execute based on the action is safe to
/// execute in the handler thread. An important note is that an action needs to
/// be queried every time a new request is received
/// (the [#onNext(PublishStreamRequestUnparsed)] method is called) because
/// subsequent requests might be related to the same block and the status may
/// have changed since the last request, but also if we start streaming a new
/// block, the action for it might be different that what is usually expected,
/// especially true in a multi-publisher environment.
public final class PublisherHandler implements Pipeline<PublishStreamRequestUnparsed> {
    // Note: not surrounding correlationId={3} with [] intentionally as it will break Loki's parsing.
    private static final String LATENCY_END_METRIC_MESSAGE =
            "metric-end-to-end-latency-by-block-end block={0,number,#} nsTimestamp={1,number,#} handlerId={2} correlationId={3}";
    private static final String LATENCY_START_MESSAGE =
            "metric-end-to-end-latency-by-block-start block={0,number,#} nsTimestamp={1,number,#} handlerId={2} correlationId={3}";

    private final Logger LOGGER = System.getLogger(getClass().getName());
    /// The replies pipeline, used for sending responses to the publisher.
    private final Pipeline<? super PublishStreamResponse> replies;
    /// Metrics for the handlers. This instance is shared between all handlers.
    private final MetricsHolder metrics;
    /// The publisher manager. We use it to make [BlockAction] queries.
    private final StreamPublisherManager publisherManager;
    /// The ID of this handler. This is used to identify the handler within the publisher manager.
    private final long handlerId;
    /// The correlation ID from the gRPC `hiero-correlation-id` header, or `""` if absent.
    /// Stored as the raw ID value (without brackets). Used as the first parameter in log format strings,
    /// e.g. `"[{0}] message {1}", correlationIdPrefix, arg` to avoid per-call string allocation.
    private final String correlationIdPrefix;
    /// The current streaming block number. This is used to track the current block being streamed.
    private final AtomicLong currentStreamingBlockNumber;
    /// The current queue of the block currently streaming
    private final AtomicReference<Deque<BlockItemSetUnparsed>> currentBlockQueue;
    /// The unacknowledged blocks that were streamed to completion by this handler.
    private final NavigableSet<Long> unacknowledgedStreamedBlocks;
    /// The state of the publisher, true if it is still active.
    private final AtomicBoolean isActive;
    // @todo() remove this (and its usage) and use telemetry or metrics queries instead
    /// The start time in nanos of block being currently streamed
    private long currentStreamingBlockHeaderReceivedTime = System.nanoTime();

    /// The current block action.
    /// This is tracked to help the manager determine what to do with the current
    /// block.
    private final AtomicReference<BlockAction> blockAction;

    /// Initialize a new publisher handler.
    ///
    /// @param nextId the next handler ID to use
    /// @param replyPipeline the pipeline to send replies to
    /// @param handlerMetrics the metrics for this handler
    /// @param manager the publisher manager that manages this handler
    /// @param correlationId the correlation ID from the gRPC `hiero-correlation-id` header, or null/empty if absent
    public PublisherHandler(
            final long nextId,
            @NonNull final Pipeline<? super PublishStreamResponse> replyPipeline,
            @NonNull final MetricsHolder handlerMetrics,
            @NonNull final StreamPublisherManager manager,
            final String correlationId) {
        handlerId = nextId;
        replies = Objects.requireNonNull(replyPipeline);
        metrics = Objects.requireNonNull(handlerMetrics);
        publisherManager = Objects.requireNonNull(manager);
        correlationIdPrefix = (correlationId == null || correlationId.isEmpty()) ? "" : correlationId;
        currentStreamingBlockNumber = new AtomicLong(UNKNOWN_BLOCK_NUMBER);
        currentBlockQueue = new AtomicReference<>();
        blockAction = new AtomicReference<>();
        unacknowledgedStreamedBlocks = new ConcurrentSkipListSet<>();
        isActive = new AtomicBoolean(true);
    }

    // A package-private method for accessing correlation ID for tracing support.
    String getCorrelationId() {
        return correlationIdPrefix;
    }

    // ==== Flow Methods =======================================================

    @Override
    public void onError(@NonNull final Throwable throwable) {
        try {
            // This is a "terminal" method, called when an _unrecoverable_ error
            // occurs. No other methods will be called by the Helidon layer after this.
            final String message = "Handler %d received error: %s".formatted(handlerId, throwable);
            LOGGER.log(DEBUG, message, throwable);
            sendEndOfStream(Code.ERROR); // this might not succeed...
        } finally {
            // Shut down this handler, even if sending the message failed
            // or metrics failed.
            checkMidBlockAndShutdown(currentStreamingBlockNumber.get());
        }
    }

    @Override
    public void onComplete() {
        // This is mostly a cleanup method, called when the stream is complete
        // and `onNext` will not be called again.
        checkMidBlockAndShutdown(currentStreamingBlockNumber.get());
    }

    @Override
    public void clientEndStreamReceived() {
        // called when the _gRPC layer_ receives an HTTP end stream from the client.
        // THIS IS NOT the same as the `EndStream` message in the API.
        checkMidBlockAndShutdown(currentStreamingBlockNumber.get());
    }

    @Override
    public void onSubscribe(@NonNull final Subscription subscription) {
        // This "starts" the subscription for this handler.
        // not really anything to do here.
    }

    @Override
    public void onNext(@NonNull final PublishStreamRequestUnparsed request) {
        if (!isActive.get()) {
            checkMidBlockAndShutdown(currentStreamingBlockNumber.get());
        } else {
            try {
                LOGGER.log(TRACE, "[{0}] Handler {1} received request", correlationIdPrefix, handlerId);
                final PublisherRequestResult result = processNextRequestUnparsed(request);
                result.handle();
                LOGGER.log(TRACE, "[{0}] Handler {1} finished processing request", correlationIdPrefix, handlerId);
            } catch (final RuntimeException e) {
                // If we reach here, it means that the handler was interrupted or
                // an unexpected error occurred. We should log the error and shut down.
                try {
                    LOGGER.log(INFO, "[%2$s] Error processing request: %1$s".formatted(e, correlationIdPrefix), e);
                    sendEndOfStream(Code.ERROR);
                } finally {
                    checkMidBlockAndShutdown(currentStreamingBlockNumber.get());
                }
            }
            // @todo check the current backlog by calling a manager method to
            //    check backlog and pause for a few milliseconds if difference
            //    between latest streamed and latest persisted gets too large.
        }
    }

    /// This method returns the ID of this handler.
    ///
    /// @return the ID of this handler
    long getId() {
        return handlerId;
    }

    /// Send an acknowledgement for the last block number that was persisted.
    ///
    /// This method is called when the a block is persisted and verified
    /// so we need to acknowledge it to the publisher. The acknowledgement
    /// is sent as a response to the publisher, indicating that all blocks up to
    /// and including the given block number are safely stored in this block node.
    ///
    /// @param blockToAcknowledge the last block number that was
    ///     verified and persisted.
    public void sendAcknowledgement(final long blockToAcknowledge) {
        final String ackTraceStartMessage = "[{0}] Handler {1} sending acknowledgement for block {2}";
        LOGGER.log(TRACE, ackTraceStartMessage, correlationIdPrefix, handlerId, blockToAcknowledge);
        // We only ever need to acknowledge once for a given block number, even
        // if there are several blocks "behind" that acknowledgement.
        // The publishers expect that acknowledgement for block N implicitly
        // acknowledges all blocks up to and including N.
        final BlockAcknowledgement ack = BlockAcknowledgement.newBuilder()
                .blockNumber(blockToAcknowledge)
                .build();
        final PublishStreamResponse response =
                PublishStreamResponse.newBuilder().acknowledgement(ack).build();
        if (sendResponse(response)) {
            // if response was sent successfully, we can remove
            // all unacknowledged blocks that are less than or equal to the
            // new last acknowledged block number.
            unacknowledgedStreamedBlocks.headSet(blockToAcknowledge, true).clear();
            metrics.blockAcknowledgementsSent.increment(); // @todo(1415) add label
            final long ackTime = System.nanoTime();
            LOGGER.log(TRACE, LATENCY_END_METRIC_MESSAGE, blockToAcknowledge, ackTime, handlerId, correlationIdPrefix);
            final String ackTraceEndMessage = "[{2}] Sent acknowledgement for block {0,number,#} from handler {1}";
            LOGGER.log(TRACE, ackTraceEndMessage, blockToAcknowledge, handlerId, correlationIdPrefix);
        } else {
            // Here, we must end immediately, because if we failed to send, then
            // we will never get another `onNext`.
            checkMidBlockAndShutdown(currentStreamingBlockNumber.get());
        }
    }

    /// This method must be called when a verification fails for a given block.
    /// If this handler was the one that streamed the block, we will attempt to
    /// send an [EndOfStream] with a [Code#BAD_BLOCK_PROOF] and proceed to
    /// schedule a shutdown for the handler.
    ///
    /// @param blockNumber of the block that failed verification
    /// @return true if the handler has sent the [Code#BAD_BLOCK_PROOF] message
    boolean handleFailedVerification(final long blockNumber) {
        LOGGER.log(
                DEBUG,
                "[{0}] Handler {1} handling failed verification for block {2}",
                correlationIdPrefix,
                handlerId,
                blockNumber);
        if (unacknowledgedStreamedBlocks.remove(blockNumber)) {
            // If the block number that failed verification was sent by this
            // handler, we need to send an EndOfStream with BAD_BLOCK_PROOF code.
            endStreamWithCode(Code.BAD_BLOCK_PROOF, false);
            return true;
        } else {
            return false;
        }
    }

    /// This method must be called when the handler needs to end with a code.
    /// This includes ending successfully, ending for failed persistence,
    /// ending a stalled publisher, or various other situations.
    ///
    /// @param codeToSend the end of stream code to send.
    /// @param immediate whether to schedule the shutdown or shut down immediately.
    void endStreamWithCode(final Code codeToSend, boolean immediate) {
        try {
            LOGGER.log(DEBUG, "[{0}] Handler {1} ending with code {2}", correlationIdPrefix, handlerId, codeToSend);
            sendEndOfStream(codeToSend);
        } finally {
            if (immediate) {
                checkMidBlockAndShutdown(currentStreamingBlockNumber.get());
            } else {
                scheduleShutdown();
            }
        }
    }

    /// Set this handler to shut down at the next efficient opportunity.
    /// This is generally after the next end-of-block message is received.
    void scheduleShutdown() {
        isActive.set(false);
    }

    private void checkMidBlockAndShutdown(final long blockNumber) {
        try {
            isActive.compareAndSet(true, false); // shouldn't be needed, but set just in case.
            if (isCurrentlyMidBlock(blockNumber)) {
                publisherManager.blockIsEnding(currentStreamingBlockNumber.get(), handlerId);
            }
        } finally {
            shutdown();
            resetState();
        }
    }

    /// todo(1420) add documentation
    private PublisherRequestResult processNextRequestUnparsed(final PublishStreamRequestUnparsed request) {
        final PublisherRequestResult result;
        if (request.hasBlockItems()) {
            final BlockItemSetUnparsed itemSetUnparsed = request.blockItems();
            final List<BlockItemUnparsed> blockItems = itemSetUnparsed.blockItems();
            if (blockItems.isEmpty()) {
                result = new SendEndAndShutdownResult(this, Code.INVALID_REQUEST, currentStreamingBlockNumber.get());
            } else {
                result = handleBlockItemsRequest(itemSetUnparsed, blockItems);
            }
        } else if (request.hasEndStream()) {
            result = handleEndStreamRequest(request.endStream());
        } else if (request.hasEndOfBlock()) {
            result = handleEndOfBlock(request.endOfBlock());
        } else {
            // this should never happen
            result = new SendEndAndShutdownResult(this, Code.ERROR, currentStreamingBlockNumber.get());
        }
        return result;
    }

    /// This method handles a request for a block of items and returns a
    /// [PublisherRequestResult] that we must then [PublisherRequestResult#handle()].
    /// @param itemSetUnparsed the item set we have received
    /// @param blockItems the items contained in the set received
    /// @return a [PublisherRequestResult] that we must then [PublisherRequestResult#handle()].
    private PublisherRequestResult handleBlockItemsRequest(
            final BlockItemSetUnparsed itemSetUnparsed, final List<BlockItemUnparsed> blockItems) {
        long blockNumber = currentStreamingBlockNumber.get();
        final BlockItemUnparsed first = blockItems.getFirst();
        // every time we receive an item set, we need to check if we have
        // a block header, if we do, we need to take the number and store it
        // in memory, this is now the current streaming block number.
        final boolean requestContainsHeader = first.hasBlockHeader();
        if (requestContainsHeader) {
            // If we have a block header, this means that we are at the
            // start of a new block.
            final BlockHeader header;
            final Bytes headerBytes = first.blockHeader();
            if (headerBytes != null) {
                try {
                    header = BlockHeader.PROTOBUF.parse(headerBytes);
                } catch (final ParseException e) {
                    LOGGER.log(DEBUG, "[{0}] Failed to parse BlockHeader due to {1}", correlationIdPrefix, e);
                    // if we have reached this block, this means that the
                    // request is invalid
                    return new SendEndAndShutdownResult(this, Code.INVALID_REQUEST, blockNumber);
                }
            } else {
                final String message = "[{0}] Handler {1} received a BlockHeader with null bytes";
                LOGGER.log(DEBUG, message, correlationIdPrefix, handlerId);
                // this should never happen
                return new SendEndAndShutdownResult(this, Code.ERROR, blockNumber);
            }
            if (isCurrentlyMidBlock(blockNumber) && blockNumber != header.number()) {
                // If we are in the middle of streaming a block, and we have received a new header,
                // we need to end the current block that we are streaming and start streaming the new one.
                // We need to schedule the current streaming block to be resent only if it is different than
                // the new one we are just starting.
                publisherManager.blockIsEnding(blockNumber, handlerId);
                // We have to reset the state of the handler before proceeding
                resetState();
            }
            // Now we can update the block number and the current streaming number to match the new block
            blockNumber = header.number();
            currentStreamingBlockNumber.set(blockNumber);
            // this means that we are starting a new block, so we can
            // update the current streaming block number
            currentStreamingBlockHeaderReceivedTime = System.nanoTime();
            final long blockStartTime = currentStreamingBlockHeaderReceivedTime;
            LOGGER.log(TRACE, LATENCY_START_MESSAGE, blockNumber, blockStartTime, handlerId, correlationIdPrefix);
        } else if (UNKNOWN_BLOCK_NUMBER == blockNumber) {
            // here we should drop the batch, _this is normal_, and can happen
            // in many cases, including if the publisher sent several batches
            // for a block that should be skipped in the time it took for the
            // header batch to arrive and the "skip" response to be sent back,
            // due to network latency and processing time.
            metrics.blockItemSetsDropped.increment();
            final String message = "[{0}] Handler {1} dropping batch because first block item is not BlockHeader";
            LOGGER.log(DEBUG, message, correlationIdPrefix, handlerId);
            return new ContinueResult(this);
        }
        // now we need to query the manager with the block number currently
        // being streamed, we will receive a response that will tell us
        // what to do with the items we have received, and we can trust that
        // no matter what the response is, we can safely take the appropriate
        // action. IMPORTANT: we need to do this check every time, even if
        // the current received set is in the middle of the batch, because
        // the response that is received from the manager might have changed.
        // query here
        final BlockAction actionFromPublisher =
                publisherManager.getActionForBlock(blockNumber, blockAction.get(), handlerId);
        blockAction.set(actionFromPublisher);
        return switch (actionFromPublisher) {
            case ACCEPT -> handleAccept(blockNumber, requestContainsHeader, itemSetUnparsed);
            case SKIP -> handleSkip(blockNumber);
            case RESEND -> {
                final String errorMessage =
                        "[{0}] Handler {1} unexpectedly received the block action {2} as an action for new header/block in progress";
                yield handleEndError(WARNING, errorMessage, correlationIdPrefix, handlerId, actionFromPublisher);
            }
            case SEND_BEHIND -> handleSendBehind();
            case END_DUPLICATE -> handleEndDuplicate();
            case END_ERROR -> {
                final String errorMessage =
                        "[{0}] Handler {1} received the block action {2} as an action for new header/block in progress";
                yield handleEndError(DEBUG, errorMessage, correlationIdPrefix, handlerId, actionFromPublisher);
            }
        };
    }

    /// todo(1420) add documentation
    ///
    /// @return
    private PublisherRequestResult handleEndStreamRequest(final EndStream endStream) {
        final EndStream.Code code = endStream.endCode();
        final long endStreamEarliestBlockNumber = endStream.earliestBlockNumber();
        final long endStreamLatestBlockNumber = endStream.latestBlockNumber();
        final String earliestAndLatestBlockNumbers =
                "Earliest publisher block number: %d, Latest publisher block number: %d"
                        .formatted(endStreamEarliestBlockNumber, endStreamLatestBlockNumber);
        // We need to validate the request's values, ERROR is not obliged to
        // have earliest and latest block numbers. For ERROR, we do not use
        // the earliest and latest block numbers.
        final PublisherRequestResult result;
        if (isEndStreamRequestValid(code, endStreamEarliestBlockNumber, endStreamLatestBlockNumber)) {
            // We can ignore the returned result below, we need it mainly for
            // the switch expression so that we are forced at compile time to
            // handle all possible end stream codes.
            result = handleValidEndStreamRequest(code, earliestAndLatestBlockNumbers, endStreamLatestBlockNumber);
        } else {
            LOGGER.log(
                    INFO,
                    "[{3}] Handler {0} received an invalid EndStream request with code {1}. {2}",
                    handlerId,
                    code,
                    earliestAndLatestBlockNumbers,
                    correlationIdPrefix);
            // @todo(2536) re-evaluate the result returned when the request is invalid
            result = new ShutdownResult(this, currentStreamingBlockNumber.get());
        }
        return result;
    }

    private PublisherRequestResult handleEndOfBlock(final BlockEnd endOfBlock) {
        final long endOfBlockNumber = endOfBlock.blockNumber();
        final long currentStreamingNumber = currentStreamingBlockNumber.get();
        final PublisherRequestResult result;
        if (currentStreamingNumber <= UNKNOWN_BLOCK_NUMBER) {
            LOGGER.log(
                    INFO,
                    "[{0}] Handler {1} received EndOfBlock for block {2}, but is not currently streaming a block",
                    correlationIdPrefix,
                    handlerId,
                    endOfBlockNumber);
            result = new ContinueResult(this);
        } else {
            if (endOfBlockNumber != currentStreamingNumber) {
                LOGGER.log(
                        INFO,
                        "[{0}] Handler {1} is expected to end block {2}, but received end for block {3}.",
                        correlationIdPrefix,
                        handlerId,
                        currentStreamingNumber,
                        endOfBlockNumber);
            }
            metrics.receiveBlockTimeLatencyNs.increment(System.nanoTime() - currentStreamingBlockHeaderReceivedTime);
            unacknowledgedStreamedBlocks.add(currentStreamingNumber);
            final ActionForBlock actionForBlock = publisherManager.endOfBlock(currentStreamingNumber);
            publisherManager.closeBlock(handlerId);
            result = switch (actionForBlock.action()) {
                // If we get ACCEPT, we must simply reset the state and continue
                case ACCEPT -> {
                    if (actionForBlock.blockNumber() > UNKNOWN_BLOCK_NUMBER
                            && currentStreamingNumber == actionForBlock.blockNumber()) {
                        yield new ResetStateResult(this);
                    } else {
                        yield unexpectedActionForEndOfBlock(actionForBlock, currentStreamingNumber);
                    }
                }
                // If we get a resend, we must handle it
                case RESEND -> handleResend(actionForBlock.blockNumber());
                // These cases are not expected to be returned
                case SKIP, SEND_BEHIND, END_DUPLICATE, END_ERROR ->
                    unexpectedActionForEndOfBlock(actionForBlock, currentStreamingNumber);
            };
        }
        return result;
    }

    private PublisherRequestResult unexpectedActionForEndOfBlock(
            final ActionForBlock actionForBlock, final long blockToEnd) {
        final String errorMessage =
                "[{0}] Handler {1} received unexpected action for block: {2}, when ending block {3}";
        return handleEndError(WARNING, errorMessage, correlationIdPrefix, handlerId, actionForBlock, blockToEnd);
    }

    private boolean isEndStreamRequestValid(
            final EndStream.Code code, final long endStreamEarliestBlockNumber, final long endStreamLatestBlockNumber) {
        boolean isRequestValid = false;
        if (EndStream.Code.ERROR != code) {
            if (endStreamEarliestBlockNumber >= 0 && endStreamLatestBlockNumber >= endStreamEarliestBlockNumber) {
                // The request is valid because both earliest and latest are
                // greater than or equal to 0, and latest is greater than or
                // equal to earliest.
                isRequestValid = true;
            }
        } else {
            // If the code is ERROR, we do not need to validate the earliest and
            // latest block numbers, because in the ERROR case, they are not
            // expected based on the proto API.
            isRequestValid = true;
        }
        return isRequestValid;
    }

    private PublisherRequestResult handleValidEndStreamRequest(
            final EndStream.Code code,
            final String earliestAndLatestBlockNumbers,
            final long endStreamLatestBlockNumber) {
        return switch (code) {
            case UNRECOGNIZED, UNKNOWN -> {
                final String message = "Handler %d received EndStream with UNKNOWN. %s"
                        .formatted(handlerId, earliestAndLatestBlockNumbers);
                yield handleEndStream(WARNING, message);
            }
            case RESET -> {
                final String message = "Handler %d received EndStream with RESET. %s"
                        .formatted(handlerId, earliestAndLatestBlockNumbers);
                yield handleEndStream(DEBUG, message);
            }
            case TIMEOUT -> {
                final String message = "Handler %d received EndStream with TIMEOUT. %s"
                        .formatted(handlerId, earliestAndLatestBlockNumbers);
                yield handleEndStream(INFO, message);
            }
            case ERROR -> {
                final String message = "Handler %d received EndStream with ERROR.".formatted(handlerId);
                yield handleEndStream(INFO, message);
            }
            case TOO_FAR_BEHIND -> {
                final String message = "Handler %d received EndStream with TOO_FAR_BEHIND. %s"
                        .formatted(handlerId, earliestAndLatestBlockNumbers);
                yield handleEndStreamBehind(DEBUG, message, endStreamLatestBlockNumber);
            }
        };
    }

    // ==== Publisher Response Methods =========================================

    /// This method proceeds to send an [EndOfStream] with the given [Code] to
    /// the publisher. The publisher should then proceed to shutdow after this
    /// method is called.
    /// @param codeToSend an [EndOfStream.Code] to be sent to the publisher
    private void sendEndOfStream(final Code codeToSend) {
        final EndOfStream endOfStream = EndOfStream.newBuilder()
                .status(codeToSend)
                .blockNumber(publisherManager.getLatestBlockNumber())
                .build();
        final PublishStreamResponse response =
                PublishStreamResponse.newBuilder().endStream(endOfStream).build();
        if (sendResponse(response)) {
            metrics.endOfStreamsSent.increment(); // @todo(1415) add label
        } // no else here, we expect to be shut down regardless...
    }

    /// Everytime we interact with the response pipeline we need to make sure we
    /// catch all exceptions, as it is very possible that the pipeline will throw.
    /// @param response to be sent to the pipeline
    /// @return boolean value if the response was successfully sent
    private boolean sendResponse(final PublishStreamResponse response) {
        try {
            long start = System.nanoTime();
            replies.onNext(response);
            long duration = System.nanoTime() - start;
            final String entryMessage = "[{0}] Handler {1} replies.onNext took {2,number,#} ns to send {3}";
            final ResponseOneOfType responseKind = response.response().kind();
            LOGGER.log(DEBUG, entryMessage, correlationIdPrefix, handlerId, duration, responseKind);
            return true;
        } catch (final UncheckedIOException e) {
            // Unfortunately this is the "standard" way to end a stream, so log
            // at debug rather than emitting noise in the logs.
            // Also, this confuses everyone, they all see this debug log and
            // assume the node crashed, so we must not print a stack trace.
            final String messageFormat = "[%3$s] Publisher closed the connection unexpectedly for client %1$d: %2$s";
            final Throwable actualException = e.getCause() != null ? e.getCause() : e;
            final String message = messageFormat.formatted(handlerId, actualException, correlationIdPrefix);
            LOGGER.log(DEBUG, message, e);
            metrics.sendResponseFailed.increment(); // @todo(1415) add label
            return false;
        } catch (final RuntimeException e) {
            final String message = "[%4$s] Failed to send response '%1$s' for handler %2$d: %3$s"
                    .formatted(response.response().kind(), handlerId, e, correlationIdPrefix);
            LOGGER.log(DEBUG, message, e);
            metrics.sendResponseFailed.increment(); // @todo(1415) add label
            return false;
        }
    }

    /// Handle the ACCEPT action for a block.
    private PublisherRequestResult handleAccept(
            final long blockNumber, final boolean requestContainsHeader, final BlockItemSetUnparsed itemSetUnparsed) {
        if (requestContainsHeader) {
            final Deque<BlockItemSetUnparsed> newBlockQueue = new ConcurrentLinkedDeque<>();
            currentBlockQueue.set(newBlockQueue);
            publisherManager.registerQueueForBlock(handlerId, newBlockQueue, blockNumber);
        }
        currentBlockQueue.get().offer(itemSetUnparsed);
        publisherManager.signalDataReady();
        metrics.liveBlockItemsReceived.increment(itemSetUnparsed.blockItems().size()); // @todo(1415) add label
        return new ContinueResult(this);
    }

    /// Handle the SKIP action for a block.
    private PublisherRequestResult handleSkip(final long blockNumber) {
        LOGGER.log(
                DEBUG, "[{0}] Handler {1} is sending SKIP for block {2}", correlationIdPrefix, handlerId, blockNumber);
        // If the action is SKIP, we need to send a skip response
        // to the publisher and not propagate the items.
        final SkipBlock skipBlock =
                SkipBlock.newBuilder().blockNumber(blockNumber).build();
        return new SkipBlockResult(this, skipBlock, currentStreamingBlockNumber.get());
    }

    /// Handle the RESEND action for a block.
    private PublisherRequestResult handleResend(final long blockToResend) {
        if (blockToResend > UNKNOWN_BLOCK_NUMBER) {
            LOGGER.log(
                    DEBUG, "[{0}] Handler {1} is sending RESEND({2})", correlationIdPrefix, handlerId, blockToResend);
            // If the action is RESEND, we need to send a resend
            // response to the publisher and not propagate the items.
            final ResendBlock resendBlock =
                    ResendBlock.newBuilder().blockNumber(blockToResend).build();
            return new ResendBlockResult(this, resendBlock, currentStreamingBlockNumber.get());
        } else {
            // This should not happen, the publisher should hot be handling a RESEND action with invalid block number
            // to resend
            final String message = "[{0}] Handler {1} received a RESEND action with invalid block number {2}";
            return handleEndError(WARNING, message, correlationIdPrefix, handlerId, blockToResend);
        }
    }

    /// Handle the END_BEHIND action for a block.
    private PublisherRequestResult handleSendBehind() {
        LOGGER.log(
                DEBUG,
                "[{0}] Handler {1} is sending Behind({2}).",
                correlationIdPrefix,
                handlerId,
                publisherManager.getLatestBlockNumber());
        // If the action is SEND_BEHIND, we need to send an end of stream
        // response to the publisher and not propagate the items.
        final BehindPublisher behindMessage = BehindPublisher.newBuilder()
                .blockNumber(publisherManager.getLatestBlockNumber())
                .build();
        return new BehindPublisherResult(this, behindMessage, currentStreamingBlockNumber.get());
    }

    /// Handle the END_DUPLICATE action for a block.
    private PublisherRequestResult handleEndDuplicate() {
        LOGGER.log(
                DEBUG,
                "[{0}] Handler {1} is sending DUPLICATE_BLOCK({2}).",
                correlationIdPrefix,
                handlerId,
                publisherManager.getLatestBlockNumber());
        // If the action is END_DUPLICATE, we need to send an end of stream
        // response to the publisher and not propagate the items.
        return new SendEndAndShutdownResult(this, Code.DUPLICATE_BLOCK, currentStreamingBlockNumber.get());
    }

    /// Handle the END_ERROR action for a block with an error message.
    private PublisherRequestResult handleEndError(
            final Level logLevel, final String errorMessage, final Object... errorMessageParams) {
        LOGGER.log(logLevel, errorMessage, errorMessageParams);
        // If the action is END_ERROR, we need to send an end of stream
        // response to the publisher and not propagate the items.
        // SendEndAndShutdownResult sends the end of stream when handled.
        metrics.streamErrors.increment(); // @todo(1415) add label
        return new SendEndAndShutdownResult(this, Code.ERROR, currentStreamingBlockNumber.get());
    }

    // ==== EndStream Handling Methods =========================================

    /// This method handles an [EndStream] requests with codes
    /// <pre>
    ///     [EndStream.Code#UNKNOWN]
    ///     [EndStream.Code#RESET]
    ///     [EndStream.Code#TIMEOUT]
    ///     [EndStream.Code#ERROR]
    /// </pre>
    private PublisherRequestResult handleEndStream(final Level logLevel, final String message) {
        LOGGER.log(logLevel, "[{0}] {1}", correlationIdPrefix, message);
        final long blockInProgress = currentStreamingBlockNumber.get();
        if (isCurrentlyMidBlock(blockInProgress)) {
            // This should generally not happen, we expect an end stream request
            // from a publisher after it has completely streamed a full block.
            publisherManager.blockIsEnding(blockInProgress, handlerId);
        }
        metrics.endStreamsReceived.increment();
        return new ShutdownResult(this, currentStreamingBlockNumber.get());
    }

    /// This method handles an [EndStream] request with
    /// [EndStream.Code#TOO_FAR_BEHIND].
    private PublisherRequestResult handleEndStreamBehind(
            final Level logLevel, final String message, final long endStreamLatestBlockNumber) {
        if (endStreamLatestBlockNumber > publisherManager.getLatestBlockNumber()) {
            publisherManager.notifyTooFarBehind(endStreamLatestBlockNumber);
        }
        return handleEndStream(logLevel, message);
    }

    // ==== Private Methods ====================================================

    /// This method will reset the state of the handler. Block action will be
    /// set to null, and the current streaming block number will be set to
    /// {@value BlockNodePlugin#UNKNOWN_BLOCK_NUMBER}.
    private void resetState() {
        blockAction.set(null);
        currentStreamingBlockNumber.set(UNKNOWN_BLOCK_NUMBER);
        currentBlockQueue.set(null);
    }

    /// This method is called when we want to orderly shut down the handler.
    /// Any cleanup that is needed should be done here.
    private void shutdown() {
        try {
            // This method is called when the handler is removed from the manager.
            // We should clean up any resources that are no longer needed.
            publisherManager.removeHandler(handlerId);
        } catch (final RuntimeException e) {
            // this should not happen
            final String message = "[%2$s] RuntimeException during removal of handler %1$d from manager"
                    .formatted(handlerId, correlationIdPrefix);
            LOGGER.log(WARNING, message, e);
        } finally {
            try {
                replies.onComplete();
                replies.closeConnection();
                // @todo() Add labeled metric when possible.
                //    Metric: "handler-closed" Labels: "clean" or "with-exception"
                //    with-exception should be set in all exception cases, even if not logged.
            } catch (final UncheckedIOException wrapper) {
                IOException wrapped = wrapper.getCause();
                if (!(wrapped instanceof SocketException)) {
                    final String message = "[%2$s] IO Exception during shutdown for handler %1$d"
                            .formatted(handlerId, correlationIdPrefix);
                    LOGGER.log(DEBUG, message, wrapper);
                }
            } catch (final RuntimeException e) {
                final String message = "[%2$s] RuntimeException during shutdown for handler %1$d"
                        .formatted(handlerId, correlationIdPrefix);
                LOGGER.log(DEBUG, message, e);
            }
            LOGGER.log(DEBUG, "[{0}] Handler {1} issued onComplete/closeConnection", correlationIdPrefix, handlerId);
        }
    }

    /// Check if we have a block in progress. The parameter should be used to supply the
    /// [#currentStreamingBlockNumber] value.
    private boolean isCurrentlyMidBlock(final long blockInProgress) {
        if (blockInProgress != currentStreamingBlockNumber.get()) {
            final String message = "[{0}] Streaming check for mid block, but block number does not match.";
            LOGGER.log(DEBUG, message, correlationIdPrefix);
        }
        return blockInProgress > UNKNOWN_BLOCK_NUMBER && currentBlockQueue.get() != null;
    }

    // ==== Publisher Request Results ==========================================

    /// A simple interface that represents the result of handling
    /// a [PublishStreamRequestUnparsed]. This result must be then handled
    /// before continuing with the next request.
    private interface PublisherRequestResult {
        /// This method handles the result of a processed request. This is to be
        /// used in a centralized place where we can have a single result of
        /// a request that was just processed.
        void handle();
    }

    /// Base class for [PublisherRequestResult].
    private abstract static class PublisherRequestResultBase implements PublisherRequestResult {
        /// Reference to the handler we want to do operations on.
        protected final PublisherHandler handler;

        protected PublisherRequestResultBase(@NonNull final PublisherHandler handler) {
            this.handler = Objects.requireNonNull(handler);
        }
    }

    /// This type of result aims to send an [EndOfStream] to the publisher with
    /// a specified code and then to shut down the handler.
    private static final class SendEndAndShutdownResult extends PublisherRequestResultBase {
        private final EndOfStream.Code codeToSend;
        private final long currentStreamingNumber;

        private SendEndAndShutdownResult(
                @NonNull final PublisherHandler handler,
                @NonNull final Code codeToSend,
                final long currentStreamingNumber) {
            super(handler);
            this.codeToSend = Objects.requireNonNull(codeToSend);
            this.currentStreamingNumber = currentStreamingNumber;
        }

        @Override
        public void handle() {
            try {
                handler.sendEndOfStream(codeToSend);
            } finally {
                handler.checkMidBlockAndShutdown(currentStreamingNumber);
            }
        }
    }

    /// This type of result aims to send a [SkipBlock] to the publisher and then
    /// reset the state. The handler will be closed if sending the response is
    /// not successful.
    private static final class SkipBlockResult extends PublisherRequestResultBase {
        private final SkipBlock skipBlockResponse;
        private final long currentStreamingNumber;

        private SkipBlockResult(
                @NonNull final PublisherHandler handler,
                @NonNull final SkipBlock skipBlockResponse,
                final long currentStreamingNumber) {
            super(handler);
            this.skipBlockResponse = Objects.requireNonNull(skipBlockResponse);
            this.currentStreamingNumber = currentStreamingNumber;
        }

        @Override
        public void handle() {
            final PublishStreamResponse response = PublishStreamResponse.newBuilder()
                    .skipBlock(skipBlockResponse)
                    .build();
            if (handler.sendResponse(response)) {
                handler.metrics.blockSkipsSent.increment(); // @todo(1415) add label
                handler.resetState();
            } else {
                handler.checkMidBlockAndShutdown(currentStreamingNumber);
            }
        }
    }

    /// This type of result aims to send a [ResendBlock] to the publisher and
    /// then reset the state. The handler will be closed if sending the response
    /// is not successful.
    private static final class ResendBlockResult extends PublisherRequestResultBase {
        private final ResendBlock resendBlockResponse;
        private final long currentStreamingNumber;

        private ResendBlockResult(
                @NonNull final PublisherHandler handler,
                @NonNull final ResendBlock resendBlockResponse,
                final long currentStreamingNumber) {
            super(handler);
            this.resendBlockResponse = Objects.requireNonNull(resendBlockResponse);
            this.currentStreamingNumber = currentStreamingNumber;
        }

        @Override
        public void handle() {
            final PublishStreamResponse response = PublishStreamResponse.newBuilder()
                    .resendBlock(resendBlockResponse)
                    .build();
            if (handler.sendResponse(response)) {
                handler.metrics.blockResendsSent.increment(); // @todo(1415) add label
                handler.resetState();
            } else {
                handler.checkMidBlockAndShutdown(currentStreamingNumber);
            }
        }
    }

    /// This type of result aims to send a [BehindPublisher] to the publisher
    /// and then reset the state. The handler will be closed if sending the
    /// response is not successful.
    private static final class BehindPublisherResult extends PublisherRequestResultBase {
        private final BehindPublisher behindPublisherResponse;
        private final long currentStreamingNumber;

        private BehindPublisherResult(
                @NonNull final PublisherHandler handler,
                @NonNull final BehindPublisher behindPublisherResponse,
                final long currentStreamingNumber) {
            super(handler);
            this.behindPublisherResponse = Objects.requireNonNull(behindPublisherResponse);
            this.currentStreamingNumber = currentStreamingNumber;
        }

        @Override
        public void handle() {
            final PublishStreamResponse response = PublishStreamResponse.newBuilder()
                    .nodeBehindPublisher(behindPublisherResponse)
                    .build();
            if (handler.sendResponse(response)) {
                handler.metrics.nodeBehindSent.increment(); // @todo(1415) add label
                handler.resetState();
            } else {
                handler.checkMidBlockAndShutdown(currentStreamingNumber);
            }
        }
    }

    /// This type of result aims to directly shutdown the handler without
    /// sending any responses to the publisher.
    private static final class ShutdownResult extends PublisherRequestResultBase {
        private final long currentStreamingNumber;

        private ShutdownResult(@NonNull final PublisherHandler handler, final long currentStreamingNumber) {
            super(handler);
            this.currentStreamingNumber = currentStreamingNumber;
        }

        @Override
        public void handle() {
            handler.checkMidBlockAndShutdown(currentStreamingNumber);
        }
    }

    /// This type of result aims to reset the state of the publisher.
    private static final class ResetStateResult extends PublisherRequestResultBase {
        private ResetStateResult(@NonNull final PublisherHandler handler) {
            super(handler);
        }

        @Override
        public void handle() {
            handler.resetState();
        }
    }

    /// This type of result aims to do nothing, effectively allowing the
    /// publisher to continue its work.
    private static final class ContinueResult extends PublisherRequestResultBase {
        private ContinueResult(@NonNull final PublisherHandler handler) {
            super(handler);
        }

        @Override
        public void handle() {
            // Do nothing
        }
    }

    // ==== Metrics ============================================================

    /// Metrics for tracking publisher handler activity:
    /// <pre>
    /// [#liveBlockItemsReceived] - Count of live block items received from a producer
    /// [#blockAcknowledgementsSent] - Count of acknowledgements sent
    /// [#streamErrors] - Count of stream errors
    /// [#blockSkipsSent] - Count of block skip responses
    /// [#blockResendsSent] - Count of block resend responses
    /// [#endOfStreamsSent] - Count of end of stream responses (should always be at most 1 per stream)
    /// [#endStreamsReceived] - Count of end streams received (should always be at most 1 per stream)
    /// [#receiveBlockTimeLatencyNs] - Time it takes for a block to be received from block header to block proof, in
    /// nanoseconds
    /// </pre>
    public record MetricsHolder(
            LongCounter.Measurement liveBlockItemsReceived,
            LongCounter.Measurement blockAcknowledgementsSent,
            LongCounter.Measurement blockItemSetsDropped,
            LongCounter.Measurement streamErrors,
            LongCounter.Measurement blockSkipsSent,
            LongCounter.Measurement blockResendsSent,
            LongCounter.Measurement endOfStreamsSent,
            LongCounter.Measurement nodeBehindSent,
            LongCounter.Measurement sendResponseFailed,
            LongCounter.Measurement endStreamsReceived,
            LongCounter.Measurement receiveBlockTimeLatencyNs) {
        /// Factory method.
        /// Creates a new instance of [MetricsHolder] using the provided
        /// [MetricRegistry] instance.
        /// @return a new, valid, fully initialized [MetricsHolder] instance
        static MetricsHolder createMetrics(@NonNull final MetricRegistry metricRegistry) {
            final LongCounter.Measurement liveBlockItemsReceived = metricRegistry
                    .register(LongCounter.builder(METRIC_PUBLISHER_BLOCK_ITEMS_RECEIVED)
                            .setDescription("Live block items received"))
                    .getOrCreateNotLabeled();
            final LongCounter.Measurement blockAcknowledgementsSent = metricRegistry
                    .register(LongCounter.builder(METRIC_PUBLISHER_BLOCKS_ACK_SENT)
                            .setDescription("Block‑ack messages sent"))
                    .getOrCreateNotLabeled();
            final LongCounter.Measurement blockItemSetsDropped = metricRegistry
                    .register(LongCounter.builder(METRIC_PUBLISHER_STREAM_SETS_DROPPED)
                            .setDescription("Publisher block item sets dropped because the block is missing a header."))
                    .getOrCreateNotLabeled();
            final LongCounter.Measurement streamErrors = metricRegistry
                    .register(LongCounter.builder(METRIC_PUBLISHER_STREAM_ERRORS)
                            .setDescription("Publisher connection streams that end in an error"))
                    .getOrCreateNotLabeled();
            final LongCounter.Measurement blockSkipsSent = metricRegistry
                    .register(LongCounter.builder(METRIC_PUBLISHER_BLOCKS_SKIPS_SENT)
                            .setDescription("Block‑ack skips sent"))
                    .getOrCreateNotLabeled();
            final LongCounter.Measurement blockResendsSent = metricRegistry
                    .register(LongCounter.builder(METRIC_PUBLISHER_BLOCKS_RESEND_SENT)
                            .setDescription("Block Resend messages sent"))
                    .getOrCreateNotLabeled();
            final LongCounter.Measurement nodeBehindSent = metricRegistry
                    .register(LongCounter.builder(METRIC_PUBLISHER_BLOCK_NODE_BEHIND_SENT)
                            .setDescription("Node Behind Publisher messages sent"))
                    .getOrCreateNotLabeled();
            final LongCounter.Measurement endOfStreamsSent = metricRegistry
                    .register(LongCounter.builder(METRIC_PUBLISHER_BLOCK_ENDOFSTREAM_SENT)
                            .setDescription("Block End-of-Stream messages sent"))
                    .getOrCreateNotLabeled();
            final LongCounter.Measurement sendResponseFailed = metricRegistry
                    .register(LongCounter.builder(METRIC_PUBLISHER_BLOCK_SEND_RESPONSE_FAILED)
                            .setDescription("Count of failures to send responses to a publisher"))
                    .getOrCreateNotLabeled();
            final LongCounter.Measurement endStreamsReceived = metricRegistry
                    .register(LongCounter.builder(METRIC_PUBLISHER_BLOCK_ENDSTREAM_RECEIVED)
                            .setDescription("Block End-Stream messages received"))
                    .getOrCreateNotLabeled();
            final LongCounter.Measurement receiveBlockTimeLatencyNs = metricRegistry
                    .register(
                            LongCounter.builder(METRIC_PUBLISHER_RECEIVE_LATENCY_NS)
                                    .setDescription(
                                            "Latency in nanoseconds between block being sent by publisher and being fully streamed from block header to block proof, also known as of network in-transit time latency"))
                    .getOrCreateNotLabeled();

            return new MetricsHolder(
                    liveBlockItemsReceived,
                    blockAcknowledgementsSent,
                    blockItemSetsDropped,
                    streamErrors,
                    blockSkipsSent,
                    blockResendsSent,
                    endOfStreamsSent,
                    nodeBehindSent,
                    sendResponseFailed,
                    endStreamsReceived,
                    receiveBlockTimeLatencyNs);
        }
    }
}
