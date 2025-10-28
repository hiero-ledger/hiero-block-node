// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.stream.publisher;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.TRACE;
import static java.lang.System.Logger.Level.WARNING;
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
import java.io.UncheckedIOException;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.List;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.atomic.AtomicLong;
import org.hiero.block.api.PublishStreamRequest.EndStream;
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
    /** The unacknowledged yet blocks that were streamed to completion by this handler. */
    private final NavigableSet<Long> unacknowledgedStreamedBlocks;
    /** The start time in nanos of block being currently streamed */
    private long currentStreamingBlockHeaderReceivedTime = System.nanoTime();

    /**
     * The current block action.
     * This is tracked to help the manager determine what to do with the current
     * block.
     */
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
        unacknowledgedStreamedBlocks = new ConcurrentSkipListSet<>();
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
        shutdown();
    }

    @Override
    public void clientEndStreamReceived() {
        // called when the _gRPC layer_ receives an HTTP end stream from the client.
        // THIS IS NOT the same as the `EndStream` message in the API.
        shutdown();
    }

    @Override
    public void onSubscribe(@NonNull final Subscription subscription) {
        // This "starts" the subscription for this handler.
        // not really anything to do here.
    }

    @Override
    public void onNext(@NonNull final PublishStreamRequestUnparsed request) {
        try {
            LOGGER.log(TRACE, "Handler {0} received request", handlerId);
            processNextRequestUnparsed(request);
            LOGGER.log(TRACE, "Handler {0} finished processing request", handlerId);
        } catch (final InterruptedException | RuntimeException e) {
            // If we reach here, it means that the handler was interrupted or
            // an unexpected error occurred. We should log the error and shut down.
            LOGGER.log(INFO, "Error processing request: %s", e);
            sendEndAndResetState(Code.ERROR);
        }
    }

    /**
     * todo(1420) add documentation
     */
    private void processNextRequestUnparsed(final PublishStreamRequestUnparsed request) throws InterruptedException {
        if (request.hasBlockItems()) {
            final BlockItemSetUnparsed itemSetUnparsed = Objects.requireNonNull(request.blockItems());
            final List<BlockItemUnparsed> blockItems = itemSetUnparsed.blockItems();
            if (blockItems.isEmpty()) {
                sendEndAndResetState(Code.INVALID_REQUEST);
            } else {
                handleBlockItemsRequest(itemSetUnparsed, blockItems);
            }
        } else if (request.hasEndStream()) {
            try {
                handleEndStreamRequest(Objects.requireNonNull(request.endStream()));
            } finally {
                shutdown();
            }
        } else if (request.hasEndOfBlock()) {
            // @todo(1626) Do nothing for now.
        } else {
            // this should never happen
            sendEndAndResetState(Code.ERROR);
        }
    }

    /**
     * todo(1420) add documentation
     */
    private void sendEndAndResetState(final Code endOfStreamCode) {
        try {
            sendEndOfStream(endOfStreamCode);
            // @todo(1416) add metrics
            resetState();
        } finally {
            shutdown();
        }
    }

    /**
     * todo(1420) add documentation
     */
    private void handleBlockItemsRequest(
            final BlockItemSetUnparsed itemSetUnparsed, final List<BlockItemUnparsed> blockItems)
            throws InterruptedException {
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
                        LOGGER.log(DEBUG, "Failed to parse BlockHeader due to {0}", e);
                        // if we have reached this block, this means that the
                        // request is invalid
                        sendEndAndResetState(Code.INVALID_REQUEST);
                        return;
                    }
                } else {
                    LOGGER.log(DEBUG, "Handler {0} received a BlockHeader with null bytes", handlerId);
                    // this should never happen
                    sendEndAndResetState(Code.ERROR);
                    return;
                }
                blockNumber = header.number();
                // this means that we are starting a new block, so we can
                // update the current streaming block number
                final String traceMessage = "metric-end-to-end-latency-by-block-start {0},{1}ns";
                currentStreamingBlockHeaderReceivedTime = System.nanoTime();
                LOGGER.log(TRACE, traceMessage, blockNumber, currentStreamingBlockHeaderReceivedTime);
                currentStreamingBlockNumber.set(blockNumber);
            } else {
                LOGGER.log(
                        DEBUG,
                        "Handler {0} received a BlockHeader while already streaming block {1}",
                        handlerId,
                        blockNumber);
                // If we have entered here, we have an invalid request, the
                // block number is not reset which means that the block
                // from the request prior to this one has not been streamed in
                // full. Having a block header indicates that this request
                // starts streaming a new block, which should not be the case
                // if the previous block has not been streamed in full.
                sendEndAndResetState(Code.INVALID_REQUEST);
                return;
            }
        } else if (UNKNOWN_BLOCK_NUMBER == blockNumber) {
            // here we should drop the batch, _this is normal_, and can happen
            // in many cases, including if the publisher sent several batches
            // for a block that should be skipped in the time it took for the
            // header batch to arrive and the "skip" response to be sent back,
            // due to network latency and processing time.
            // @todo(1416) add metrics
            LOGGER.log(DEBUG, "Handler {0} dropping batch because first block item is not BlockHeader", handlerId);
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
        handleBlockActionResult(handleResult);
        // we must now check if the last item in the set is a proof.
        final BlockItemUnparsed last = blockItems.getLast();
        if (last.hasBlockProof()) {
            handleBlockProof(last.blockProof(), blockNumber);
        }
    }

    /**
     * todo(1420) add documentation
     */
    private void handleEndStreamRequest(final EndStream endStream) {
        final EndStream.Code code = endStream.endCode();
        final long endStreamEarliestBlockNumber = endStream.earliestBlockNumber();
        final long endStreamLatestBlockNumber = endStream.latestBlockNumber();
        final String earliestAndLatestBlockNumbers =
                "Earliest publisher block number: %d, Latest publisher block number: %d"
                        .formatted(endStreamEarliestBlockNumber, endStreamLatestBlockNumber);
        // We need to validate the request's values, ERROR is not obliged to
        // have earliest and latest block numbers. For ERROR, we do not use
        // the earliest and latest block numbers.
        if (isEndStreamRequestValid(code, endStreamEarliestBlockNumber, endStreamLatestBlockNumber)) {
            // We can ignore the returned result below, we need it mainly for
            // the switch expression so that we are forced at compile time to
            // handle all possible end stream codes.
            handleValidEndStreamRequest(code, earliestAndLatestBlockNumbers, endStreamLatestBlockNumber);
        } else {
            // @todo(1416) is this the correct log or action we need to take if the request is invalid?
            LOGGER.log(
                    INFO,
                    "Handler %d received an invalid EndStream request with code %s. %s"
                            .formatted(handlerId, code, earliestAndLatestBlockNumbers));
        }
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

    @SuppressWarnings("UnusedReturnValue")
    private EndStreamResult handleValidEndStreamRequest(
            final EndStream.Code code,
            final String earliestAndLatestBlockNumbers,
            final long endStreamLatestBlockNumber) {
        return switch (code) {
            case EndStream.Code.UNKNOWN -> {
                final String message = "Handler %d received EndStream with UNKNOWN. %s"
                        .formatted(handlerId, earliestAndLatestBlockNumbers);
                yield handleEndStream(WARNING, message);
            }
            case EndStream.Code.RESET -> {
                final String message = "Handler %d received EndStream with RESET. %s"
                        .formatted(handlerId, earliestAndLatestBlockNumbers);
                yield handleEndStream(DEBUG, message);
            }
            case EndStream.Code.TIMEOUT -> {
                final String message = "Handler %d received EndStream with TIMEOUT. %s"
                        .formatted(handlerId, earliestAndLatestBlockNumbers);
                yield handleEndStream(DEBUG, message);
            }
            case EndStream.Code.ERROR -> {
                final String message = "Handler %d received EndStream with ERROR.".formatted(handlerId);
                yield handleEndStream(DEBUG, message);
            }
            case EndStream.Code.TOO_FAR_BEHIND -> {
                final String message = "Handler %d received EndStream with TOO_FAR_BEHIND. %s"
                        .formatted(handlerId, earliestAndLatestBlockNumbers);
                yield handleEndStreamBehind(DEBUG, message, endStreamLatestBlockNumber);
            }
        };
    }

    /**
     * This method is called when the manager is shutting down and needs
     * to force all handlers to close their publisher communication channels.
     */
    public void closeCommunication() {
        sendEndOfStream(Code.SUCCESS);
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
        LOGGER.log(
                TRACE, "Handler {0} sending acknowledgement for block {1}", handlerId, newLastAcknowledgedBlockNumber);
        // We only ever need to acknowledge once for a given block number, even
        // if there are several blocks "behind" that acknowledgement.
        // The publishers expect that acknowledgement for block N implicitly
        // acknowledges all blocks up to and including N.
        final BlockAcknowledgement ack = BlockAcknowledgement.newBuilder()
                .blockNumber(newLastAcknowledgedBlockNumber)
                .build();
        final PublishStreamResponse response =
                PublishStreamResponse.newBuilder().acknowledgement(ack).build();
        if (sendResponse(response)) {
            // if response was sent successfully, we can remove
            // all unacknowledged blocks that are less than or equal to the
            // new last acknowledged block number.
            unacknowledgedStreamedBlocks
                    .headSet(newLastAcknowledgedBlockNumber, true)
                    .clear();
            metrics.blockAcknowledgementsSent.increment(); // @todo(1415) add label

            final String ackMessage = "Sent acknowledgement for block {0} from handler {1}";
            final String traceMessage = "metric-end-to-end-latency-by-block-end {0},{1}ns";
            LOGGER.log(TRACE, traceMessage, newLastAcknowledgedBlockNumber, System.nanoTime());
            LOGGER.log(TRACE, ackMessage, newLastAcknowledgedBlockNumber, handlerId);
        }
    }

    /**
     * todo(1420) add documentation
     */
    private void sendEndOfStream(final EndOfStream.Code codeToSend) {
        final EndOfStream endOfStream = EndOfStream.newBuilder()
                .status(codeToSend)
                .blockNumber(publisherManager.getLatestBlockNumber())
                .build();
        final PublishStreamResponse response =
                PublishStreamResponse.newBuilder().endStream(endOfStream).build();
        if (sendResponse(response)) {
            metrics.endOfStreamsSent.increment(); // @todo(1415) add label
        }
    }

    /**
     * Everytime we interact with the response pipeline we need to make sure we
     * catch all exceptions, as it is very possible that the pipeline will throw.
     * In such cases, the method will call {@link #shutdown()} before returning.
     *
     * @param response to be sent to the pipeline
     * @return boolean value if the response was successfully sent
     */
    private boolean sendResponse(final PublishStreamResponse response) {
        try {
            long start = System.nanoTime();
            replies.onNext(response);
            long duration = System.nanoTime() - start;
            LOGGER.log(DEBUG, "Handler {0} replies.onNext took {1} ns", handlerId, duration);
            return true;
        } catch (UncheckedIOException e) {
            shutdown(); // this method is idempotent and can be called multiple times
            // Unfortunately this is the "standard" way to end a stream, so log
            // at debug rather than emitting noise in the logs.
            // Also, this confuses everyone, they all see this debug log and
            // assume the node crashed, so we must not print a stack trace.
            final String messageFormat = "Publisher closed the connection unexpectedly for client %d: %s";
            final String exceptionMessage = e.getCause() != null ? e.getCause().getMessage() : e.getMessage();
            final String message = messageFormat.formatted(handlerId, exceptionMessage);
            LOGGER.log(Level.DEBUG, message);
            metrics.sendResponseFailed.increment(); // @todo(1415) add label
            return false;
        } catch (final RuntimeException e) {
            shutdown(); // this method is idempotent and can be called multiple times
            final String message = "Failed to send response for handler %d: %s".formatted(handlerId, e.getMessage());
            LOGGER.log(DEBUG, message, e);
            metrics.sendResponseFailed.increment(); // @todo(1415) add label
            return false;
        }
    }

    /**
     * This method must be called when a verification fails for a given block.
     * If this handler was the one that streamed the block, we will attempt to
     * send an {@link EndOfStream} with a {@link Code#BAD_BLOCK_PROOF} and
     * proceed to shut down the handler.
     * Otherwise, we will send a {@link ResendBlock} response to the publisher.
     * If the response fails to send, we will shut down the handler.
     *
     * @param blockNumber of the block that failed verification
     * @return the id of this handler
     */
    public long handleFailedVerification(final long blockNumber) {
        LOGGER.log(DEBUG, "Handler {0} handling failed verification for block {1}", handlerId, blockNumber);
        if (unacknowledgedStreamedBlocks.remove(blockNumber)) {
            // If the block number that failed verification was sent by this
            // handler, we need to send an EndOfStream with BAD_BLOCK_PROOF code.
            try {
                sendEndOfStream(Code.BAD_BLOCK_PROOF);
            } finally {
                shutdown();
            }
        } else {
            // Else we need to send a RESEND response, if sending the response
            // fails, we will shut down the handler.
            handleBlockActionResult(handleResend());
        }
        return handlerId;
    }

    /**
     * This method must be called when persistence fails for a given block.
     * We will attempt to send an {@link EndOfStream} with a {@link Code#PERSISTENCE_FAILED} and
     * proceed to shut down the handler.
     *
     * @return the id of this handler
     */
    public long handleFailedPersistence() {
        LOGGER.log(DEBUG, "Handler {0} handling failed persistence", handlerId);
        try {
            sendEndOfStream(Code.PERSISTENCE_FAILED);
        } finally {
            shutdown();
        }

        return handlerId;
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
     * This method handles the result of a block action handle.
     *
     * @param handleResult the result to handle
     */
    private void handleBlockActionResult(final BatchHandleResult handleResult) {
        if (handleResult.shouldReset()) {
            resetState();
        }
        if (handleResult.shouldShutdown()) {
            shutdown();
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
     * @param blockNumber the block number that this proof verifies
     */
    private void handleBlockProof(final Bytes blockProofBytes, final long blockNumber) {
        try {
            publisherManager.closeBlock(BlockProof.PROTOBUF.parse(blockProofBytes), handlerId);
        } catch (final ParseException e) {
            publisherManager.closeBlock(null, handlerId);
            LOGGER.log(INFO, "Failed to parse block proof: {}", e.getMessage());
        }
        long proofReceivedTime = System.nanoTime() - currentStreamingBlockHeaderReceivedTime;
        metrics.receiveBlockTimeLatencyNs.add(proofReceivedTime);
        LOGGER.log(
                TRACE,
                "Publisher Handler {0} Received block proof for block: {1}, and it took {2}ns",
                handlerId,
                blockNumber,
                proofReceivedTime);
        unacknowledgedStreamedBlocks.add(blockNumber);
        resetState();
    }

    /**
     * Handle the ACCEPT action for a block.
     */
    private BatchHandleResult handleAccept(
            final BlockItemSetUnparsed itemSetUnparsed, final List<BlockItemUnparsed> blockItems)
            throws InterruptedException {
        // If the action is ACCEPT, we can safely propagate the items
        // to the manager.
        blockItemsQueue.put(itemSetUnparsed);
        final int itemsReceived = blockItems.size();
        metrics.liveBlockItemsReceived.add(itemsReceived); // @todo(1415) add label
        return new BatchHandleResult(false, false);
    }

    /**
     * Handle the SKIP action for a block.
     */
    private BatchHandleResult handleSkip(final long blockNumber) {
        LOGGER.log(DEBUG, "Handler {0} is sending SKIP for block {1}", handlerId, blockNumber);
        // If the action is SKIP, we need to send a skip response
        // to the publisher and not propagate the items.
        final SkipBlock skipBlock =
                SkipBlock.newBuilder().blockNumber(blockNumber).build();
        final PublishStreamResponse response =
                PublishStreamResponse.newBuilder().skipBlock(skipBlock).build();
        if (sendResponse(response)) {
            metrics.blockSkipsSent.increment(); // @todo(1415) add label
            return new BatchHandleResult(false, true);
        } else {
            return new BatchHandleResult(true, true);
        }
    }

    /**
     * Handle the RESEND action for a block.
     */
    private BatchHandleResult handleResend() {
        final long blockToResend = publisherManager.getLatestBlockNumber() + 1L;
        LOGGER.log(DEBUG, "Handler {0} is sending RESEND({1})", handlerId, blockToResend);
        // If the action is RESEND, we need to send a resend
        // response to the publisher and not propagate the items.
        final ResendBlock resendBlock = ResendBlock.newBuilder()
                .blockNumber(publisherManager.getLatestBlockNumber() + 1L)
                .build();
        final PublishStreamResponse response =
                PublishStreamResponse.newBuilder().resendBlock(resendBlock).build();
        if (sendResponse(response)) {
            metrics.blockResendsSent.increment(); // @todo(1415) add label
            return new BatchHandleResult(false, true);
        } else {
            return new BatchHandleResult(true, true);
        }
    }

    /**
     * Handle the END_BEHIND action for a block.
     */
    private BatchHandleResult handleEndBehind() {
        LOGGER.log(DEBUG, "Handler {0} is sending BEHIND({1}).", handlerId, publisherManager.getLatestBlockNumber());
        // If the action is END_BEHIND, we need to send an end of stream
        // response to the publisher and not propagate the items.
        sendEndOfStream(Code.BEHIND);
        return new BatchHandleResult(true, true);
    }

    /**
     * Handle the END_DUPLICATE action for a block.
     */
    private BatchHandleResult handleEndDuplicate() {
        LOGGER.log(
                DEBUG,
                "Handler {0} is sending DUPLICATE_BLOCK({1}).",
                handlerId,
                publisherManager.getLatestBlockNumber());
        // If the action is END_DUPLICATE, we need to send an end of stream
        // response to the publisher and not propagate the items.
        sendEndOfStream(Code.DUPLICATE_BLOCK);
        return new BatchHandleResult(true, true);
    }

    /**
     * Handle the END_ERROR action for a block.
     */
    private BatchHandleResult handleEndError() {
        LOGGER.log(DEBUG, "Handler {0} is sending ERROR", handlerId);
        // If the action is END_ERROR, we need to send an end of stream
        // response to the publisher and not propagate the items.
        sendEndOfStream(Code.ERROR);
        metrics.streamErrors.increment(); // @todo(1415) add label
        return new BatchHandleResult(true, true);
    }

    // ==== EndStream Handling Methods =========================================

    /**
     * Simple record to hold the result of an end stream request handling.
     *
     * @param shouldShutdown boolean value indicating whether the handler should
     * shut down after handling the end stream request.
     */
    private record EndStreamResult(boolean shouldShutdown) {}

    /**
     * This method handles an {@link EndStream} requests with codes
     * <pre>
     *     {@link EndStream.Code#UNKNOWN}
     *     {@link EndStream.Code#RESET}
     *     {@link EndStream.Code#TIMEOUT}
     *     {@link EndStream.Code#ERROR}
     * </pre>
     */
    private EndStreamResult handleEndStream(final Level logLevel, final String message) {
        LOGGER.log(logLevel, message);
        final long blockNumber = currentStreamingBlockNumber.get();
        if ((blockAction != null) && (UNKNOWN_BLOCK_NUMBER != blockNumber)) {
            // This should generally not happen, we expect an end stream request
            // from a publisher after it has completely streamed a full block.
            LOGGER.log(INFO, "Handler %d is ending mid-block %d".formatted(handlerId, blockNumber));
            publisherManager.handlerIsEnding(blockNumber, handlerId);
        }
        metrics.endStreamsReceived.increment();
        return new EndStreamResult(true);
    }

    /**
     * This method handles an {@link EndStream} request with
     * {@link EndStream.Code#TOO_FAR_BEHIND}.
     */
    private EndStreamResult handleEndStreamBehind(
            final Level logLevel, final String message, final long endStreamLatestBlockNumber) {
        if (endStreamLatestBlockNumber > publisherManager.getLatestBlockNumber()) {
            publisherManager.notifyTooFarBehind(endStreamLatestBlockNumber);
        }
        return handleEndStream(logLevel, message);
    }

    // ==== Private Methods ====================================================

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
        final long blockInProgress = currentStreamingBlockNumber.get();
        if (blockInProgress != UNKNOWN_BLOCK_NUMBER) {
            try {
                publisherManager.handlerIsEnding(blockInProgress, handlerId);
                publisherManager.closeBlock(null, handlerId);
            } catch (final RuntimeException e) {
                LOGGER.log(WARNING, "Exception during handler ending for handler %d".formatted(handlerId), e);
            }
        }
        // reset state
        resetState();
        try {
            // This method is called when the handler is removed from the manager.
            // We should clean up any resources that are no longer needed.
            publisherManager.removeHandler(handlerId);
        } catch (final RuntimeException e) {
            // this should not happen
            LOGGER.log(WARNING, "Exception during removal of handler %d from manager".formatted(handlerId), e);
        } finally {
            try {
                // onComplete call in finally block to ensure it is called
                replies.onComplete();
            } catch (final RuntimeException e) {
                LOGGER.log(DEBUG, "Exception during calling onComplete for handler %d".formatted(handlerId), e);
            }
        }
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
     * {@link #receiveBlockTimeLatencyNs} - Time it takes for a block to be received from block header to block proof, in nanoseconds
     * </pre>
     */
    public record MetricsHolder(
            Counter liveBlockItemsReceived,
            Counter blockAcknowledgementsSent,
            Counter streamErrors,
            Counter blockSkipsSent,
            Counter blockResendsSent,
            Counter endOfStreamsSent,
            Counter sendResponseFailed,
            Counter endStreamsReceived,
            Counter receiveBlockTimeLatencyNs) {
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
            final Counter sendResponseFailed =
                    metrics.getOrCreate(new Counter.Config(METRICS_CATEGORY, "publisher_block_send_response_failed")
                            .withDescription("Count of failures to send responses to a publisher"));
            final Counter endStreamsReceived =
                    metrics.getOrCreate(new Counter.Config(METRICS_CATEGORY, "publisher_block_endstream_received")
                            .withDescription("Block End-Stream messages received"));
            final Counter receiveBlockTimeLatencyNs = metrics.getOrCreate(
                    new Counter.Config(METRICS_CATEGORY, "publisher_receive_latency_ns")
                            .withDescription(
                                    "Latency in nanoseconds between block being sent by publisher and being fully streamed from block header to block proof, also known as of network in-transit time latency"));

            return new MetricsHolder(
                    liveBlockItemsReceived,
                    blockAcknowledgementsSent,
                    streamErrors,
                    blockSkipsSent,
                    blockResendsSent,
                    endOfStreamsSent,
                    sendResponseFailed,
                    endStreamsReceived,
                    receiveBlockTimeLatencyNs);
        }
    }
}
