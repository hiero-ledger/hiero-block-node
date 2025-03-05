// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.server.mediator;

import static com.hedera.block.server.metrics.BlockNodeMetricTypes.Counter.LiveBlockItems;
import static com.hedera.block.server.metrics.BlockNodeMetricTypes.Counter.LiveBlockStreamMediatorError;
import static com.hedera.block.server.metrics.BlockNodeMetricTypes.Gauge.MediatorRingBufferRemainingCapacity;
import static java.lang.System.Logger;
import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.ERROR;

import com.hedera.block.server.events.BlockNodeEventHandler;
import com.hedera.block.server.events.ObjectEvent;
import com.hedera.block.server.metrics.MetricsService;
import com.hedera.block.server.service.ServiceStatus;
import com.hedera.hapi.block.BlockItemUnparsed;
import com.lmax.disruptor.BatchEventProcessor;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.List;
import java.util.Map;

/**
 * Use LiveStreamMediatorImpl to mediate the live stream of blocks from a producer to multiple
 * consumers.
 *
 * <p>As an implementation of the StreamMediator interface, it proxies block items to the
 * subscribers as they arrive via a RingBuffer maintained in the base class and persists the block
 * items to a store.
 */
class LiveStreamMediatorImpl extends SubscriptionHandlerBase<List<BlockItemUnparsed>> implements LiveStreamMediator {

    private final Logger LOGGER = System.getLogger(getClass().getName());

    private final ServiceStatus serviceStatus;
    private final MetricsService metricsService;

    /**
     * Constructs a new LiveStreamMediatorImpl instance with the given subscribers, and service
     * status. This constructor is primarily used for testing purposes. Users of this constructor
     * should take care to supply a thread-safe map implementation for the subscribers to handle the
     * dynamic addition and removal of subscribers at runtime.
     *
     * @param subscribers the map of subscribers to batch event processors. It's recommended the map
     *     implementation is thread-safe
     * @param serviceStatus the service status to stop the service and web server if an exception
     *     occurs while persisting a block item, stop the web server for maintenance, etc
     * @param metricsService - the service responsible for handling metrics
     * @param mediatorConfig - the configuration settings for the mediator
     */
    LiveStreamMediatorImpl(
            @NonNull
                    final Map<
                                    BlockNodeEventHandler<ObjectEvent<List<BlockItemUnparsed>>>,
                                    BatchEventProcessor<ObjectEvent<List<BlockItemUnparsed>>>>
                            subscribers,
            @NonNull final ServiceStatus serviceStatus,
            @NonNull final MetricsService metricsService,
            @NonNull final MediatorConfig mediatorConfig) {

        super(subscribers, metricsService, mediatorConfig, mediatorConfig.ringBufferSize());

        this.serviceStatus = serviceStatus;
        this.metricsService = metricsService;
    }

    /**
     * Publishes the given block item to all subscribers. If an exception occurs while persisting
     * the block items, the service status is set to not running, and all downstream consumers are
     * unsubscribed.
     *
     * @param blockItems the block item from the upstream producer to publish to downstream
     *     consumers
     */
    @Override
    public void publish(@NonNull final List<BlockItemUnparsed> blockItems) {

        if (serviceStatus.isRunning()) {
            LOGGER.log(DEBUG, "Publishing BlockItems: " + blockItems.size());
            ringBuffer.publishEvent((event, sequence) -> event.set(blockItems));

            long remainingCapacity = ringBuffer.remainingCapacity();
            metricsService.get(MediatorRingBufferRemainingCapacity).set(remainingCapacity);

            // Increment the block item counter by all block items published
            metricsService.get(LiveBlockItems).add(blockItems.size());
        } else {
            LOGGER.log(ERROR, "StreamMediator is not accepting BlockItems");
        }
    }

    @Override
    public void notifyUnrecoverableError() {

        // Disable BlockItem publication for upstream producers
        serviceStatus.stopRunning(this.getClass().getName());
        LOGGER.log(ERROR, "An exception occurred. Stopping the service.");

        // Increment the error counter
        metricsService.get(LiveBlockStreamMediatorError).increment();

        LOGGER.log(ERROR, "Sending an error response to end the stream for all consumers.");

        // @todo(662): Change how we broadcast an end of stream response in the event of an unrecoverable error.
        // Publish an end of stream response to all downstream consumers
    }
}
