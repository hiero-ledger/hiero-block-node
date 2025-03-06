// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.server.notifier;

import static com.hedera.block.server.metrics.BlockNodeMetricTypes.Counter.SuccessfulPubStreamResp;
import static com.hedera.block.server.metrics.BlockNodeMetricTypes.Gauge.NotifierRingBufferRemainingCapacity;
import static java.lang.System.Logger.Level.ERROR;

import com.hedera.block.server.mediator.MediatorConfig;
import com.hedera.block.server.mediator.SubscriptionHandlerBase;
import com.hedera.block.server.metrics.MetricsService;
import com.hedera.block.server.service.ServiceStatus;
import com.hedera.hapi.block.Acknowledgement;
import com.hedera.hapi.block.BlockAcknowledgement;
import com.hedera.hapi.block.EndOfStream;
import com.hedera.hapi.block.PublishStreamResponse;
import com.hedera.hapi.block.PublishStreamResponseCode;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.concurrent.ConcurrentHashMap;
import javax.inject.Inject;
import javax.inject.Singleton;

/**
 * Use NotifierImpl to mediate the stream of responses from the persistence layer back to multiple
 * producers.
 *
 * <p>As an implementation of the StreamMediator interface, it proxies block item persistence
 * responses back to the producers as they arrive via a RingBuffer maintained in the base class and
 * persists the block items to a store. It also notifies the mediator of critical system events and
 * will stop the server in the event of an unrecoverable error.
 */
@Singleton
public class NotifierImpl extends SubscriptionHandlerBase<PublishStreamResponse> implements Notifier {

    private final System.Logger LOGGER = System.getLogger(getClass().getName());

    /** The initial capacity of producers in the subscriber map. */
    private static final int SUBSCRIBER_INIT_CAPACITY = 5;

    private final Notifiable mediator;
    private final MetricsService metricsService;
    private final ServiceStatus serviceStatus;

    /**
     * Constructs a new NotifierImpl instance with the given mediator, block node context, and
     * service status.
     *
     * @param mediator the mediator to notify of critical system events
     * @param metricsService the metrics service
     * @param notifierConfig the notifier configuration
     * @param serviceStatus the service status to stop the service and web server if an exception
     *     occurs
     */
    @Inject
    public NotifierImpl(
            @NonNull final Notifiable mediator,
            @NonNull final MetricsService metricsService,
            @NonNull final NotifierConfig notifierConfig,
            @NonNull final MediatorConfig mediatorConfig,
            @NonNull final ServiceStatus serviceStatus) {

        super(
                new ConcurrentHashMap<>(SUBSCRIBER_INIT_CAPACITY),
                metricsService,
                mediatorConfig,
                notifierConfig.ringBufferSize());

        this.mediator = mediator;
        this.metricsService = metricsService;
        this.serviceStatus = serviceStatus;
    }

    @Override
    public void notifyUnrecoverableError() {

        mediator.notifyUnrecoverableError();

        // Publish an end of stream response to the producers.
        final PublishStreamResponse errorStreamResponse = buildErrorStreamResponse();
        ringBuffer.publishEvent((event, sequence) -> event.set(errorStreamResponse));

        // Stop the server
        serviceStatus.stopWebServer(getClass().getName());
    }

    /**
     * Publishes the given PublishStreamResponse
     *
     * @param response the PublishStreamResponse to publish
     */
    @Override
    public void publish(@NonNull PublishStreamResponse response) {
        if (serviceStatus.isRunning()) {
            // Publish the block item to the subscribers
            ringBuffer.publishEvent((event, sequence) -> event.set(response));

            metricsService.get(NotifierRingBufferRemainingCapacity).set(ringBuffer.remainingCapacity());
            metricsService.get(SuccessfulPubStreamResp).increment();
        } else {
            LOGGER.log(ERROR, "Service is not running. Notifier skipping sendAck");
        }
    }

    /**
     * Builds an error stream response.
     *
     * @return the error stream response
     */
    @NonNull
    private PublishStreamResponse buildErrorStreamResponse() {
        long blockNumber = serviceStatus.getLatestAckedBlock() != null
                ? serviceStatus.getLatestAckedBlock().getBlockNumber()
                : serviceStatus.getLatestReceivedBlockNumber();
        final EndOfStream endOfStream = EndOfStream.newBuilder()
                .status(PublishStreamResponseCode.STREAM_ITEMS_INTERNAL_ERROR)
                .blockNumber(blockNumber)
                .build();
        return PublishStreamResponse.newBuilder().status(endOfStream).build();
    }

    /**
     * Builds an acknowledgement for the given block number and block hash.
     *
     * @param blockHash root hash of the block to ack
     * @param blockNumber number of the block to ack
     * @param alreadyExists true if the block already exists, false otherwise
     * @return the acknowledgement
     */
    @NonNull
    Acknowledgement buildAck(final @NonNull Bytes blockHash, final long blockNumber, boolean alreadyExists) {
        final BlockAcknowledgement blockAcknowledgement = BlockAcknowledgement.newBuilder()
                .blockRootHash(blockHash)
                .blockNumber(blockNumber)
                .blockAlreadyExists(alreadyExists)
                .build();

        return Acknowledgement.newBuilder().blockAck(blockAcknowledgement).build();
    }

    /**
     * Sends an end of stream message for the given block number.
     *
     * @param block_number number of the block to ack
     * @param responseCode the response code to send
     */
    @Override
    public void sendEndOfStream(long block_number, @NonNull PublishStreamResponseCode responseCode) {
        final var publishStreamResponse = PublishStreamResponse.newBuilder()
                .status(EndOfStream.newBuilder()
                        .blockNumber(block_number)
                        .status(responseCode)
                        .build())
                .build();

        publish(publishStreamResponse);
    }

    /**
     * Sends an acknowledgement for the given block number and block hash.
     *
     * @param blockNumber number of the block to ack
     * @param blockHash root hash of the block to ack
     * @param duplicated true if the block is a duplicate, false otherwise
     */
    @Override
    public void sendAck(long blockNumber, @NonNull Bytes blockHash, boolean duplicated) {

        final var publishStreamResponse = PublishStreamResponse.newBuilder()
                .acknowledgement(buildAck(blockHash, blockNumber, duplicated))
                .build();

        publish(publishStreamResponse);
    }
}
