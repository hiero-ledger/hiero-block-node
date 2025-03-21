// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.publisher;

import static java.lang.System.Logger;
import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.ERROR;
import static java.lang.System.Logger.Level.WARNING;
import static org.hiero.block.server.metrics.BlockNodeMetricTypes.Counter.LiveBlockItemsReceived;
import static org.hiero.block.server.metrics.BlockNodeMetricTypes.Counter.SuccessfulPubStreamRespSent;
import static org.hiero.block.server.metrics.BlockNodeMetricTypes.Gauge.CurrentBlockNumberInbound;

import com.hedera.hapi.block.Acknowledgement;
import com.hedera.hapi.block.BlockAcknowledgement;
import com.hedera.hapi.block.EndOfStream;
import com.hedera.hapi.block.PublishStreamResponse;
import com.hedera.hapi.block.PublishStreamResponseCode;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.grpc.Pipeline;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.time.InstantSource;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;
import org.hiero.block.server.block.BlockInfo;
import org.hiero.block.server.events.BlockNodeEventHandler;
import org.hiero.block.server.events.LivenessCalculator;
import org.hiero.block.server.events.ObjectEvent;
import org.hiero.block.server.mediator.Publisher;
import org.hiero.block.server.mediator.SubscriptionHandler;
import org.hiero.block.server.metrics.MetricsService;
import org.hiero.block.server.service.ServiceStatus;
import org.hiero.hapi.block.node.BlockItemUnparsed;

/**
 * The BlockStreamProducerSession class plugs into Helidon's server-initiated bidirectional gRPC
 * service implementation. Helidon calls methods on this class as networking events occur with the
 * connection to the upstream producer (e.g. block items streamed from the Consensus Node to the
 * server). There is one of these created per connection to the upstream producer(consensus node).
 */
public class BlockStreamProducerSessionOld
        implements Pipeline<List<BlockItemUnparsed>, PublishStreamResponse>,
        Comparable<BlockStreamProducerSessionOld> {

    private final Logger LOGGER = System.getLogger(getClass().getName());

    private final long sessionCreationTime = System.nanoTime();
    private final SubscriptionHandler<PublishStreamResponse> subscriptionHandler;
    private final Publisher<List<BlockItemUnparsed>> publisher;
    private final ServiceStatus serviceStatus;
    private final MetricsService metricsService;
    private final Flow.Subscriber<? super PublishStreamResponse> publishStreamResponseObserver;

    private final AtomicBoolean isResponsePermitted = new AtomicBoolean(true);

    private final LivenessCalculator livenessCalculator;

    private boolean allowCurrentBlockStream = false;

    /**
     * Constructor for the ProducerBlockStreamObserver class. It is responsible for calling the
     * mediator with blocks as they arrive from the upstream producer. It also sends responses back
     * to the upstream producer via the responseStreamObserver.
     *
     * @param producerLivenessClock the clock used to calculate the producer liveness.
     * @param publisher the block item list publisher to used to pass block item lists to consumers
     *     as they arrive from the upstream producer.
     * @param subscriptionHandler the subscription handler used to
     * @param publishStreamResponseObserver the response stream observer to send responses back to
     *     the upstream producer for each block item processed.
     * @param serviceStatus the service status used to stop the server in the event of an
     *     unrecoverable error.
     * @param metricsService - the service responsible for handling metrics
     * @param publisherConfig - the configuration settings
     */
    public BlockStreamProducerSessionOld(
            @NonNull final InstantSource producerLivenessClock,
            @NonNull final Publisher<List<BlockItemUnparsed>> publisher,
            @NonNull final SubscriptionHandler<PublishStreamResponse> subscriptionHandler,
            @NonNull final Pipeline<? super PublishStreamResponse> publishStreamResponseObserver,
            @NonNull final ServiceStatus serviceStatus,
            @NonNull final PublisherConfig publisherConfig,
            @NonNull final MetricsService metricsService) {

        this.livenessCalculator =
                new LivenessCalculator(producerLivenessClock, publisherConfig.timeoutThresholdMillis());

        this.publisher = publisher;
        this.publishStreamResponseObserver = publishStreamResponseObserver;
        this.subscriptionHandler = subscriptionHandler;
        this.metricsService = Objects.requireNonNull(metricsService);
        this.serviceStatus = serviceStatus;
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
        LOGGER.log(DEBUG, "onSubscribe called");
    }

    /**
     * Helidon triggers this method when it receives a new PublishStreamRequest from the upstream
     * producer. The method publish the block item data to all subscribers via the Publisher and
     * sends a response back to the upstream producer.
     *
     */
    @Override
    public void onNext(@NonNull final List<BlockItemUnparsed> blockItems) {

        try {
            LOGGER.log(DEBUG, "Received PublishStreamRequest from producer with " + blockItems.size() + " BlockItems.");
            if (blockItems.isEmpty()) {
                return;
            }

            metricsService.get(LiveBlockItemsReceived).add(blockItems.size());

            // Publish the block to all the subscribers unless
            // there's an issue with the StreamMediator.
            if (serviceStatus.isRunning()) {
                // Refresh the producer liveness
                livenessCalculator.refresh();

                // pre-check for valid block
                if (preCheck(blockItems)) {

                    final BlockItemUnparsed blockItemUnparsed = blockItems.getFirst();
                    if (blockItemUnparsed.hasBlockHeader()) {

                        try {
                            long blockNumber = BlockHeader.PROTOBUF
                                    .parse(Objects.requireNonNull(blockItemUnparsed.blockHeader()))
                                    .number();
                            serviceStatus.setLatestReceivedBlockNumber(blockNumber);
                            metricsService.get(CurrentBlockNumberInbound).set(blockNumber);
                        } catch (ParseException e) {
                            throw new RuntimeException(e);
                        }
                    }

                    // Publish the block to the mediator
                    publisher.publish(blockItems);
                }
            } else {
                LOGGER.log(ERROR, getClass().getName() + " is not accepting BlockItems");
                stopProcessing();

                // Close the upstream connection to the producer(s)
                publishStreamResponseObserver.onNext(buildErrorStreamResponse());
                LOGGER.log(ERROR, "Error PublishStreamResponse sent to upstream producer");
            }
        } catch (Exception e) {
            LOGGER.log(ERROR, "Error processing block items", e);
            publishStreamResponseObserver.onNext(buildErrorStreamResponse());
            // should we halt processing?
            stopProcessing();
        }
    }

    @Override
    public void onEvent(ObjectEvent<PublishStreamResponse> event, long sequence, boolean endOfBatch) {

        if (isResponsePermitted.get()) {
            if (isTimeoutExpired()) {
                stopProcessing();
                LOGGER.log(DEBUG, "Producer liveness timeout. Unsubscribed ProducerBlockItemObserver.");
            } else {
                LOGGER.log(DEBUG, "Publishing response to upstream producer: " + publishStreamResponseObserver);
                publishStreamResponseObserver.onNext(event.get());
                metricsService.get(SuccessfulPubStreamRespSent).increment();
            }
        }
    }

    @NonNull
    private PublishStreamResponse buildErrorStreamResponse() {
        long blockNumber = serviceStatus.getLatestAckedBlock() != null
                ? serviceStatus.getLatestAckedBlock().getBlockNumber()
                : serviceStatus.getLatestReceivedBlockNumber();
        final EndOfStream endOfStream = EndOfStream.newBuilder()
                .blockNumber(blockNumber)
                .status(PublishStreamResponseCode.STREAM_ITEMS_INTERNAL_ERROR)
                .build();
        return PublishStreamResponse.newBuilder().status(endOfStream).build();
    }

    /**
     * Helidon triggers this method when an error occurs on the bidirectional stream to the upstream
     * producer.
     *
     * @param t the error occurred on the stream
     */
    @Override
    public void onError(@NonNull final Throwable t) {
        stopProcessing();
        LOGGER.log(ERROR, "onError method invoked with an exception: ", t);
        LOGGER.log(ERROR, "Producer cancelled the stream. Observer unsubscribed.");
    }

    /**
     * Helidon triggers this method when the bidirectional stream to the upstream producer is
     * completed. Unsubscribe all the observers from the mediator.
     */
    @Override
    public void onComplete() {
        stopProcessing();
        LOGGER.log(DEBUG, "Producer completed the stream. Observer unsubscribed.");
    }

    @Override
    public boolean isTimeoutExpired() {
        return livenessCalculator.isTimeoutExpired();
    }

    @Override
    public void clientEndStreamReceived() {
        stopProcessing();
        LOGGER.log(DEBUG, "Producer cancelled the stream. Observer unsubscribed.");
    }

    private void stopProcessing() {
        isResponsePermitted.set(false);
        unsubscribe();
    }

    @Override
    public void unsubscribe() {
        subscriptionHandler.unsubscribe(this);
    }

    /**
     * Pre-check for valid block, if the block is a duplicate or future block, we don't stream to the Ring Buffer.
     * @param blockItems the list of block items
     * @return true if the block should stream forward to RB otherwise false
     */
    private boolean preCheck(@NonNull final List<BlockItemUnparsed> blockItems) {

        // we only check if is the start of a new block.
        BlockItemUnparsed firstItem = blockItems.getFirst();
        if (!firstItem.hasBlockHeader()) {
            return allowCurrentBlockStream;
        }

        final long nextBlockNumber = attemptParseBlockHeaderNumber(firstItem);
        final long nextExpectedBlockNumber = serviceStatus.getLatestReceivedBlockNumber() + 1;

        // temporary workaround so it always allows the first block at startup
        if (nextExpectedBlockNumber == 1) {
            allowCurrentBlockStream = true;
            return true;
        }

        // duplicate block
        if (nextBlockNumber < nextExpectedBlockNumber) {
            // we don't stream to the RB until we check a new blockHeader for the expected block
            allowCurrentBlockStream = false;
            LOGGER.log(
                    WARNING,
                    "Received a duplicate block, received_block_number: {0}, expected_block_number: {1}",
                    nextBlockNumber,
                    nextExpectedBlockNumber);

            notifyOfDuplicateBlock(nextBlockNumber);
            return false;
        }

        // future non-immediate block
        if (nextBlockNumber > nextExpectedBlockNumber) {
            // we don't stream to the RB until we check a new blockHeader for the expected block
            allowCurrentBlockStream = false;
            LOGGER.log(
                    WARNING,
                    "Received a future block, received_block_number: {0}, expected_block_number: {1}",
                    nextBlockNumber,
                    nextExpectedBlockNumber);

            notifyOfFutureBlock(serviceStatus.getLatestAckedBlock().getBlockNumber());
            return false;
        }

        // if block number is neither duplicate nor future (should be the same as expected)
        // we allow the stream of subsequent batches
        allowCurrentBlockStream = true;
        // we also allow the batch that contains the block_header
        return true;
    }

    /**
     * Parse the block header number from the given block item.
     * If the block header is not parsable, log the error and throw a runtime exception.
     * This is necessary to wrap the checked exception that is thrown by the parse method.
     * */
    private long attemptParseBlockHeaderNumber(@NonNull final BlockItemUnparsed blockItem) {
        try {
            return BlockHeader.PROTOBUF.parse(blockItem.blockHeader()).number();
        } catch (ParseException e) {
            LOGGER.log(ERROR, "Error parsing block header", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Get the block hash for the given block number, only if is the latest acked block.
     * otherwise Empty
     *
     * @param blockNumber the block number
     * @return a promise of block hash if it exists
     */
    private Optional<Bytes> getBlockHash(final long blockNumber) {
        final BlockInfo latestAckedBlockNumber = serviceStatus.getLatestAckedBlock();
        if (latestAckedBlockNumber != null && latestAckedBlockNumber.getBlockNumber() == blockNumber) {
            return Optional.ofNullable(latestAckedBlockNumber.getBlockHash());
        }
        // if the block is older than the latest acked block, we don't have the hash on hand
        return Optional.empty();
    }

    /**
     * Notify the producer of a future block that was not expected was received.
     *
     * @param currentBlock the current block number that is persisted and verified.
     */
    private void notifyOfFutureBlock(final long currentBlock) {
        final EndOfStream endOfStream = EndOfStream.newBuilder()
                .status(PublishStreamResponseCode.STREAM_ITEMS_BEHIND)
                .blockNumber(currentBlock)
                .build();

        final PublishStreamResponse publishStreamResponse =
                PublishStreamResponse.newBuilder().status(endOfStream).build();

        publishStreamResponseObserver.onNext(publishStreamResponse);
    }

    /**
     * Notify the producer of a duplicate block that was received.
     *
     * @param duplicateBlockNumber the block number that was received and is a duplicate
     */
    private void notifyOfDuplicateBlock(final long duplicateBlockNumber) {
        final BlockAcknowledgement blockAcknowledgement = BlockAcknowledgement.newBuilder()
                .blockAlreadyExists(true)
                .blockNumber(duplicateBlockNumber)
                .blockRootHash(getBlockHash(duplicateBlockNumber).orElse(Bytes.EMPTY))
                .build();

        final PublishStreamResponse publishStreamResponse = PublishStreamResponse.newBuilder()
                .acknowledgement(Acknowledgement.newBuilder()
                        .blockAck(blockAcknowledgement)
                        .build())
                .build();

        publishStreamResponseObserver.onNext(publishStreamResponse);
    }

    /**
     * Compare this session to another session based on the creation time.
     *
     * @param o the object to be compared.
     * @return a negative integer, zero, or a positive integer as this session is less than,
     *          equal to, or greater than the specified object.
     */
    @Override
    public int compareTo(BlockStreamProducerSessionOld o) {
        return Long.compare(sessionCreationTime, o.sessionCreationTime);
    }
}
