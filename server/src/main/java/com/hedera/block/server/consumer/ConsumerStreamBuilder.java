// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.server.consumer;

import com.hedera.block.server.mediator.SubscriptionHandler;
import com.hedera.block.server.metrics.MetricsService;
import com.hedera.block.server.persistence.storage.read.BlockReader;
import com.hedera.block.server.service.ServiceStatus;
import com.hedera.hapi.block.BlockItemUnparsed;
import com.hedera.hapi.block.BlockUnparsed;
import com.hedera.hapi.block.SubscribeStreamRequest;
import com.hedera.hapi.block.SubscribeStreamResponseUnparsed;
import com.hedera.pbj.runtime.grpc.Pipeline;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.time.InstantSource;
import java.util.List;

/**
 * LiveStreamEventHandlerBuilder is a factory class for building the event handler chain for
 * streaming block items.
 */
public final class ConsumerStreamBuilder {

    /**
     * Builder method to create a runnable that will handle the streaming of block items.
     *
     * @param producerLivenessClock the clock to use to determine the producer liveness
     * @param subscribeStreamRequest the request to subscribe to the stream
     * @param subscriptionHandler the handler for the subscription
     * @param helidonConsumerObserver the observer to use to send responses to the client
     * @param blockReader the reader to use to read blocks
     * @param serviceStatus the status of the service
     * @param metricsService the service responsible for handling metrics
     * @param consumerConfig the consumer configuration
     * @return the runnable that will handle the streaming of block items
     */
    @NonNull
    public static Runnable build(
            @NonNull final InstantSource producerLivenessClock,
            @NonNull final SubscribeStreamRequest subscribeStreamRequest,
            @NonNull final SubscriptionHandler<List<BlockItemUnparsed>> subscriptionHandler,
            @NonNull final Pipeline<? super SubscribeStreamResponseUnparsed> helidonConsumerObserver,
            @NonNull final BlockReader<BlockUnparsed> blockReader,
            @NonNull final ServiceStatus serviceStatus,
            @NonNull final MetricsService metricsService,
            @NonNull final ConsumerConfig consumerConfig) {

        return new ConsumerStreamRunnable(buildStreamManager(
                producerLivenessClock,
                subscribeStreamRequest,
                subscriptionHandler,
                helidonConsumerObserver,
                blockReader,
                serviceStatus,
                metricsService,
                consumerConfig));
    }

    public static OpenRangeStreamManager buildStreamManager(
            @NonNull final InstantSource producerLivenessClock,
            @NonNull final SubscribeStreamRequest subscribeStreamRequest,
            @NonNull final SubscriptionHandler<List<BlockItemUnparsed>> subscriptionHandler,
            @NonNull final Pipeline<? super SubscribeStreamResponseUnparsed> helidonConsumerObserver,
            @NonNull final BlockReader<BlockUnparsed> blockReader,
            @NonNull final ServiceStatus serviceStatus,
            @NonNull final MetricsService metricsService,
            @NonNull final ConsumerConfig consumerConfig) {

        final HistoricDataPoller<List<BlockItemUnparsed>> historicDataPoller =
                new HistoricDataPollerImpl(blockReader, metricsService, consumerConfig);

        final ConsumerStreamResponseObserver consumerStreamResponseObserver =
                new ConsumerStreamResponseObserver(helidonConsumerObserver, metricsService);

        return new OpenRangeStreamManager(
                producerLivenessClock,
                subscribeStreamRequest,
                subscriptionHandler,
                historicDataPoller,
                consumerStreamResponseObserver,
                serviceStatus,
                metricsService,
                consumerConfig);
    }
}
