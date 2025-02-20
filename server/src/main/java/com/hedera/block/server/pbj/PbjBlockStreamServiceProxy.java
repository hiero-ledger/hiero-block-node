// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.server.pbj;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.ERROR;

import com.hedera.block.server.block.BlockInfo;
import com.hedera.block.server.config.BlockNodeContext;
import com.hedera.block.server.consumer.ClosedRangeHistoricStreamEventHandlerBuilder;
import com.hedera.block.server.consumer.LiveStreamEventHandlerBuilder;
import com.hedera.block.server.events.BlockNodeEventHandler;
import com.hedera.block.server.events.ObjectEvent;
import com.hedera.block.server.mediator.LiveStreamMediator;
import com.hedera.block.server.notifier.Notifier;
import com.hedera.block.server.persistence.storage.read.BlockReader;
import com.hedera.block.server.producer.NoOpProducerObserver;
import com.hedera.block.server.producer.ProducerBlockItemObserver;
import com.hedera.block.server.producer.ProducerConfig;
import com.hedera.block.server.service.ServiceStatus;
import com.hedera.block.server.verification.StreamVerificationHandlerImpl;
import com.hedera.hapi.block.BlockItemUnparsed;
import com.hedera.hapi.block.BlockUnparsed;
import com.hedera.hapi.block.PublishStreamRequestUnparsed;
import com.hedera.hapi.block.PublishStreamResponse;
import com.hedera.hapi.block.SubscribeStreamRequest;
import com.hedera.hapi.block.SubscribeStreamResponseCode;
import com.hedera.hapi.block.SubscribeStreamResponseUnparsed;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.grpc.Pipeline;
import com.hedera.pbj.runtime.grpc.Pipelines;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.time.Clock;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.inject.Inject;

/**
 * PbjBlockStreamServiceProxy is the runtime binding between the PBJ Helidon Plugin and the
 * Block Node. The Helidon Plugin routes inbound requests to this class based on the methods
 * and service names in PbjBlockStreamService. Service implementations are instantiated via
 * the open method thereby bridging the client requests into the Block Node application.
 */
public class PbjBlockStreamServiceProxy implements PbjBlockStreamService {

    private static final System.Logger LOGGER = System.getLogger(PbjBlockStreamServiceProxy.class.getName());

    private final LiveStreamMediator streamMediator;
    private final ServiceStatus serviceStatus;
    private final BlockNodeContext blockNodeContext;
    private final BlockReader<BlockUnparsed> blockReader;
    private final Notifier notifier;
    private final ExecutorService closedRangeHistoricStreamingExecutorService;
    private final ExecutorService liveStreamExecutorService;

    public static SubscribeStreamResponseUnparsed READ_STREAM_INVALID_START_BLOCK_NUMBER_RESPONSE;
    public static SubscribeStreamResponseUnparsed READ_STREAM_INVALID_END_BLOCK_NUMBER_RESPONSE;
    public static SubscribeStreamResponseUnparsed READ_STREAM_SUCCESS_RESPONSE;
    public static SubscribeStreamResponseUnparsed READ_STREAM_NOT_AVAILABLE;

    static {
        // Initialize these responses as flyweights
        READ_STREAM_INVALID_START_BLOCK_NUMBER_RESPONSE = SubscribeStreamResponseUnparsed.newBuilder()
                .status(SubscribeStreamResponseCode.READ_STREAM_INVALID_START_BLOCK_NUMBER)
                .build();

        READ_STREAM_INVALID_END_BLOCK_NUMBER_RESPONSE = SubscribeStreamResponseUnparsed.newBuilder()
                .status(SubscribeStreamResponseCode.READ_STREAM_INVALID_END_BLOCK_NUMBER)
                .build();

        READ_STREAM_SUCCESS_RESPONSE = SubscribeStreamResponseUnparsed.newBuilder()
                .status(SubscribeStreamResponseCode.READ_STREAM_SUCCESS)
                .build();

        READ_STREAM_NOT_AVAILABLE = SubscribeStreamResponseUnparsed.newBuilder()
                .status(SubscribeStreamResponseCode.READ_STREAM_NOT_AVAILABLE)
                .build();
    }
    /**
     * Creates a new PbjBlockStreamServiceProxy instance.
     *
     * @param streamMediator the live stream mediator
     * @param serviceStatus the service status
     * @param streamPersistenceHandler the stream persistence handler
     * @param streamVerificationHandler the stream verification handler
     * @param notifier the notifier
     * @param blockNodeContext the block node context
     */
    @Inject
    public PbjBlockStreamServiceProxy(
            @NonNull final LiveStreamMediator streamMediator,
            @NonNull final ServiceStatus serviceStatus,
            @NonNull final BlockNodeEventHandler<ObjectEvent<List<BlockItemUnparsed>>> streamPersistenceHandler,
            @NonNull final StreamVerificationHandlerImpl streamVerificationHandler,
            @NonNull final BlockReader<BlockUnparsed> blockReader,
            @NonNull final Notifier notifier,
            @NonNull final BlockNodeContext blockNodeContext) {

        this.serviceStatus = Objects.requireNonNull(serviceStatus);
        this.notifier = Objects.requireNonNull(notifier);
        this.blockNodeContext = Objects.requireNonNull(blockNodeContext);
        streamMediator.subscribe(Objects.requireNonNull(streamPersistenceHandler));
        streamMediator.subscribe(Objects.requireNonNull(streamVerificationHandler));
        this.streamMediator = Objects.requireNonNull(streamMediator);

        // Leverage virtual threads given that these are IO-bound tasks
        this.closedRangeHistoricStreamingExecutorService = Executors.newVirtualThreadPerTaskExecutor();
        this.liveStreamExecutorService = Executors.newVirtualThreadPerTaskExecutor();
        this.blockReader = Objects.requireNonNull(blockReader);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @NonNull
    public Pipeline<? super Bytes> open(
            final @NonNull Method method,
            final @NonNull RequestOptions options,
            final @NonNull Pipeline<? super Bytes> replies) {

        final var m = (BlockStreamMethod) method;
        try {
            return switch (m) {
                case publishBlockStream -> {
                    notifier.unsubscribeAllExpired();
                    yield Pipelines.<List<BlockItemUnparsed>, PublishStreamResponse>bidiStreaming()
                            .mapRequest(bytes -> parsePublishStreamRequest(bytes, options))
                            .method(this::publishBlockStream)
                            .mapResponse(bytes -> createPublishStreamResponse(bytes, options))
                            .respondTo(replies)
                            .build();
                }
                case subscribeBlockStream -> Pipelines
                        .<SubscribeStreamRequest, SubscribeStreamResponseUnparsed>serverStreaming()
                        .mapRequest(bytes -> parseSubscribeStreamRequest(bytes, options))
                        .method(this::subscribeBlockStream)
                        .mapResponse(reply -> createSubscribeStreamResponse(reply, options))
                        .respondTo(replies)
                        .build();
            };
        } catch (Exception e) {
            replies.onError(e);
            return Pipelines.noop();
        }
    }

    /**
     * Publishes the block stream.
     *
     * @param helidonProducerObserver the helidon producer observer
     * @return the pipeline
     */
    Pipeline<List<BlockItemUnparsed>> publishBlockStream(
            Pipeline<? super PublishStreamResponse> helidonProducerObserver) {
        LOGGER.log(DEBUG, "Executing bidirectional publishBlockStream gRPC method");

        // Unsubscribe any expired notifiers
        notifier.unsubscribeAllExpired();

        final ProducerConfig.ProducerType producerType = blockNodeContext
                .configuration()
                .getConfigData(ProducerConfig.class)
                .type();

        if (producerType == ProducerConfig.ProducerType.NO_OP) {
            return new NoOpProducerObserver(helidonProducerObserver, blockNodeContext);
        }

        final var producerBlockItemObserver = new ProducerBlockItemObserver(
                Clock.systemDefaultZone(),
                streamMediator,
                notifier,
                helidonProducerObserver,
                blockNodeContext,
                serviceStatus);

        if (serviceStatus.isRunning()) {
            // Register the producer observer with the notifier to publish responses back to the
            // producer
            notifier.subscribe(producerBlockItemObserver);
        }

        return producerBlockItemObserver;
    }

    /**
     * Subscribes to the block stream.
     *
     * @param subscribeStreamRequest the subscribe stream request
     * @param helidonConsumerObserver the stream response observer provided by Helidon
     */
    void subscribeBlockStream(
            @NonNull final SubscribeStreamRequest subscribeStreamRequest,
            @NonNull final Pipeline<? super SubscribeStreamResponseUnparsed> helidonConsumerObserver) {

        LOGGER.log(DEBUG, "Executing Server Streaming subscribeBlockStream gRPC method");

        Objects.requireNonNull(subscribeStreamRequest);
        Objects.requireNonNull(helidonConsumerObserver);

        if (serviceStatus.isRunning()) {
            // Unsubscribe any expired notifiers
            streamMediator.unsubscribeAllExpired();

            // Validate inbound request parameters
            if (!isValidRequestedRange(subscribeStreamRequest, helidonConsumerObserver)) {
                return;
            }

            // Validate inbound request parameters with current block number
            final BlockInfo latestAckedBlockInfo = serviceStatus.getLatestAckedBlock();
            if (latestAckedBlockInfo != null) {
                long currentBlockNumber = latestAckedBlockInfo.getBlockNumber();
                if (!isRequestedRangeValidForCurrentBlock(
                        subscribeStreamRequest, currentBlockNumber, helidonConsumerObserver)) {
                    return;
                }
            }

            // Check to see if the client is requesting a live
            // stream (endBlockNumber is 0)
            if (subscribeStreamRequest.endBlockNumber() == 0) {
                // Build the live stream event handler
                //                final Runnable liveStreamEventHandler = LiveStreamEventHandlerBuilder.buildPoller(
                //                        streamMediator,
                //                        helidonConsumerObserver,
                //                        blockNodeContext.metricsService(),
                //                        blockNodeContext.configuration());

                //                liveStreamExecutorService.submit(liveStreamEventHandler);

                final var liveStreamEventHandler = LiveStreamEventHandlerBuilder.build(
                        new ExecutorCompletionService<>(Executors.newSingleThreadExecutor()),
                        Clock.systemDefaultZone(),
                        streamMediator,
                        helidonConsumerObserver,
                        blockNodeContext.metricsService(),
                        blockNodeContext.configuration());

                streamMediator.subscribe(liveStreamEventHandler);
            } else {
                final Runnable closedRangeHistoricStreamingRunnable =
                        ClosedRangeHistoricStreamEventHandlerBuilder.build(
                                subscribeStreamRequest.startBlockNumber(),
                                subscribeStreamRequest.endBlockNumber(),
                                blockReader,
                                helidonConsumerObserver,
                                blockNodeContext.metricsService(),
                                blockNodeContext.configuration());

                // Submit the runnable to the executor service
                closedRangeHistoricStreamingExecutorService.submit(closedRangeHistoricStreamingRunnable);
            }

        } else {
            LOGGER.log(ERROR, "Server Streaming subscribeBlockStream gRPC Service is not currently running");

            // Send a response to the consumer indicating the stream is not available
            helidonConsumerObserver.onNext(SubscribeStreamResponseUnparsed.newBuilder()
                    .status(SubscribeStreamResponseCode.READ_STREAM_NOT_AVAILABLE)
                    .build());
            helidonConsumerObserver.onComplete();
        }
    }

    static boolean isValidRequestedRange(
            final SubscribeStreamRequest subscribeStreamRequest,
            final Pipeline<? super SubscribeStreamResponseUnparsed> helidonConsumerObserver) {

        final long startBlockNumber = subscribeStreamRequest.startBlockNumber();
        final long endBlockNumber = subscribeStreamRequest.endBlockNumber();
        if (startBlockNumber < 0) {
            LOGGER.log(DEBUG, "Requested start block number {0} is less than 0", startBlockNumber);
            helidonConsumerObserver.onNext(READ_STREAM_INVALID_START_BLOCK_NUMBER_RESPONSE);
            helidonConsumerObserver.onComplete();
            return false;
        }

        // endBlockNumber can be 0 but not negative
        if (endBlockNumber < 0) {
            LOGGER.log(DEBUG, "Requested end block number {0} is less than 0", endBlockNumber);
            helidonConsumerObserver.onNext(READ_STREAM_INVALID_END_BLOCK_NUMBER_RESPONSE);
            helidonConsumerObserver.onComplete();
            return false;
        }

        // endBlockNumber can be 0 (signals infinite stream) but not less than startBlockNumber
        if (endBlockNumber != 0 && endBlockNumber < startBlockNumber) {
            LOGGER.log(
                    DEBUG,
                    "Requested end block number {0} is less than the requested start block number {1}",
                    endBlockNumber,
                    startBlockNumber);
            helidonConsumerObserver.onNext(READ_STREAM_INVALID_END_BLOCK_NUMBER_RESPONSE);
            helidonConsumerObserver.onComplete();
            return false;
        }

        return true;
    }

    static boolean isRequestedRangeValidForCurrentBlock(
            final SubscribeStreamRequest subscribeStreamRequest,
            final long currentBlockNumber,
            final Pipeline<? super SubscribeStreamResponseUnparsed> helidonConsumerObserver) {

        final long startBlockNumber = subscribeStreamRequest.startBlockNumber();
        final long endBlockNumber = subscribeStreamRequest.endBlockNumber();

        // Make sure the requested range exists on the block node
        if (currentBlockNumber < startBlockNumber) {
            LOGGER.log(
                    DEBUG,
                    "Requested start block number {0} is greater than the current block number {1}",
                    startBlockNumber,
                    currentBlockNumber);
            helidonConsumerObserver.onNext(READ_STREAM_INVALID_START_BLOCK_NUMBER_RESPONSE);
            helidonConsumerObserver.onComplete();
            return false;
        }

        // Make sure the requested range exists on the block node
        if (endBlockNumber != 0 && currentBlockNumber < endBlockNumber) {
            LOGGER.log(
                    DEBUG,
                    "Requested end block number {0} is greater than the current block number {1}",
                    endBlockNumber,
                    currentBlockNumber);
            helidonConsumerObserver.onNext(READ_STREAM_INVALID_END_BLOCK_NUMBER_RESPONSE);
            helidonConsumerObserver.onComplete();
            return false;
        }

        return true;
    }

    @NonNull
    private SubscribeStreamRequest parseSubscribeStreamRequest(
            @NonNull final Bytes message, @NonNull final RequestOptions options) throws ParseException {
        return SubscribeStreamRequest.PROTOBUF.parse(message);
    }

    @NonNull
    private Bytes createSubscribeStreamResponse(
            @NonNull final SubscribeStreamResponseUnparsed subscribeStreamResponse,
            @NonNull final RequestOptions options) {
        return SubscribeStreamResponseUnparsed.PROTOBUF.toBytes(subscribeStreamResponse);
    }

    @NonNull
    private List<BlockItemUnparsed> parsePublishStreamRequest(
            @NonNull final Bytes message, @NonNull final RequestOptions options) throws ParseException {
        final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.PROTOBUF.parse(message);
        return request.blockItems().blockItems();
    }

    @NonNull
    private Bytes createPublishStreamResponse(
            @NonNull final PublishStreamResponse publishStreamResponse, @NonNull final RequestOptions options) {
        return PublishStreamResponse.PROTOBUF.toBytes(publishStreamResponse);
    }
}
