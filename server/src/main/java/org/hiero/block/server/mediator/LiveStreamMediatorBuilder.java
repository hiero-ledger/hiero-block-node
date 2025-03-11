// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.mediator;

import com.hedera.hapi.block.BlockItemUnparsed;
import com.lmax.disruptor.BatchEventProcessor;
import com.lmax.disruptor.EventPoller;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.hiero.block.server.consumer.StreamManager;
import org.hiero.block.server.events.BlockNodeEventHandler;
import org.hiero.block.server.events.ObjectEvent;
import org.hiero.block.server.metrics.MetricsService;
import org.hiero.block.server.persistence.storage.write.AsyncBlockWriter;
import org.hiero.block.server.service.ServiceStatus;

/**
 * Use builder methods to create a {@link StreamMediator} to handle live stream events from a
 * producer to N consumers.
 *
 * <p>When a stream mediator is created, it will accept new block items from a producer, publish
 * them to all consumers subscribed to the stream, and persist the block items to storage
 * represented by a {@link AsyncBlockWriter}.
 */
public class LiveStreamMediatorBuilder {

    private final MetricsService metricsService;
    private final MediatorConfig mediatorConfig;
    private final ServiceStatus serviceStatus;

    private Map<
                    BlockNodeEventHandler<ObjectEvent<List<BlockItemUnparsed>>>,
                    BatchEventProcessor<ObjectEvent<List<BlockItemUnparsed>>>>
            subscribers;

    private Map<StreamManager, EventPoller<ObjectEvent<List<BlockItemUnparsed>>>> pollSubscribers;

    /** The initial capacity of the subscriber map. */
    private static final int SUBSCRIBER_INIT_CAPACITY = 32;

    private LiveStreamMediatorBuilder(
            @NonNull final MetricsService metricsService,
            @NonNull final MediatorConfig mediatorConfig,
            @NonNull final ServiceStatus serviceStatus) {
        this.subscribers = new ConcurrentHashMap<>(SUBSCRIBER_INIT_CAPACITY);
        this.pollSubscribers = new ConcurrentHashMap<>(SUBSCRIBER_INIT_CAPACITY);
        this.metricsService = metricsService;
        this.mediatorConfig = mediatorConfig;
        this.serviceStatus = serviceStatus;
    }

    /**
     * Create a new instance of the builder using the minimum required parameters.
     *
     * @param metricsService - the service responsible for handling metrics
     * @param mediatorConfig - the configuration settings for mediator
     * @param serviceStatus is required to provide the stream mediator with access to check the
     *     status of the server and to stop the web server if necessary.
     * @return a new stream mediator builder configured with required parameters.
     */
    @NonNull
    public static LiveStreamMediatorBuilder newBuilder(
            @NonNull final MetricsService metricsService,
            @NonNull final MediatorConfig mediatorConfig,
            @NonNull final ServiceStatus serviceStatus) {
        return new LiveStreamMediatorBuilder(metricsService, mediatorConfig, serviceStatus);
    }

    /**
     * Optionally, provide a map implementation of subscribers the stream mediator. This method
     * should only be used for testing purposely. Provided map implementations should be thread-safe
     * to handle subscribers being added and removed dynamically from the stream mediator at
     * runtime.
     *
     * @param subscribers is the map of subscribers to set
     * @return the builder
     */
    // spotless:off
    @NonNull
    public LiveStreamMediatorBuilder subscribers(
            @NonNull final Map<BlockNodeEventHandler<ObjectEvent<List<BlockItemUnparsed>>>,
                               BatchEventProcessor<ObjectEvent<List<BlockItemUnparsed>>>> subscribers) {
        this.subscribers = subscribers;
        return this;
    }
    // spotless:on

    // spotless:off
    @NonNull
    public LiveStreamMediatorBuilder pollSubscribers(
            @NonNull final Map<StreamManager,
                               EventPoller<ObjectEvent<List<BlockItemUnparsed>>>> pollSubscribers) {
        this.pollSubscribers = pollSubscribers;
        return this;
    }
    // spotless:on

    /**
     * Use the build method to construct a stream mediator to handle live stream events from a
     * producer to N consumers.
     *
     * @return the stream mediator to handle live stream events between a producer and N consumers.
     */
    @NonNull
    public LiveStreamMediator build() {
        return new LiveStreamMediatorImpl(subscribers, pollSubscribers, serviceStatus, metricsService, mediatorConfig);
    }
}
