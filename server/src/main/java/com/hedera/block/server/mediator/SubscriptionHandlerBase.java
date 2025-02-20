// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.server.mediator;

import static java.lang.System.Logger.Level.DEBUG;

import com.hedera.block.server.events.BlockNodeEventHandler;
import com.hedera.block.server.events.ObjectEvent;
import com.lmax.disruptor.BatchEventProcessor;
import com.lmax.disruptor.BatchEventProcessorBuilder;
import com.lmax.disruptor.EventPoller;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.util.DaemonThreadFactory;
import com.swirlds.metrics.api.LongGauge;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Inherit from this class to leverage RingBuffer subscription handling.
 *
 * <p>Subclasses may use the ringBuffer to publish events to the subscribers. This base class
 * contains the logic to manage subscriptions to the ring buffer.
 *
 * @param <V> the type of the subscription events
 */
public abstract class SubscriptionHandlerBase<V> implements SubscriptionHandler<V> {

    private final System.Logger LOGGER = System.getLogger(getClass().getName());

    private final Map<BlockNodeEventHandler<ObjectEvent<V>>, BatchEventProcessor<ObjectEvent<V>>> subscribers;
    private final Map<BlockNodeEventHandler<ObjectEvent<V>>, EventPoller<ObjectEvent<V>>> pollSubscribers;

    /** The ring buffer to publish events to the subscribers. */
    protected final RingBuffer<ObjectEvent<V>> ringBuffer;

    private final LongGauge subscriptionGauge;
    private final ExecutorService executor;

    /**
     * Constructs an abstract SubscriptionHandler instance with the given subscribers, block writer,
     * and service status. Users of this constructor should take care to supply a thread-safe map
     * implementation for the subscribers to handle the dynamic addition and removal of subscribers
     * at runtime.
     *
     * @param subscribers the map of subscribers to batch event processors. It's recommended the map
     *     implementation is thread-safe
     * @param subscriptionGauge the gauge to keep track of the number of subscribers
     * @param ringBufferSize the size of the ring buffer
     */
    protected SubscriptionHandlerBase(
            @NonNull final Map<BlockNodeEventHandler<ObjectEvent<V>>, BatchEventProcessor<ObjectEvent<V>>> subscribers,
            @NonNull final LongGauge subscriptionGauge,
            final int ringBufferSize) {

        this.subscribers = subscribers;
        this.pollSubscribers = new ConcurrentHashMap<>();
        this.subscriptionGauge = subscriptionGauge;

        // Initialize and start the disruptor
        final Disruptor<ObjectEvent<V>> disruptor =
                new Disruptor<>(ObjectEvent::new, ringBufferSize, DaemonThreadFactory.INSTANCE);
        this.ringBuffer = disruptor.start();
        this.executor = Executors.newCachedThreadPool(DaemonThreadFactory.INSTANCE);
    }

    /**
     * Subscribes the given handler to the stream of events.
     *
     * @param handler the handler to subscribe
     */
    @Override
    public void subscribe(@NonNull final BlockNodeEventHandler<ObjectEvent<V>> handler) {

        if (!subscribers.containsKey(handler)) {
            // Initialize the batch event processor and set it on the ring buffer
            final BatchEventProcessor<ObjectEvent<V>> batchEventProcessor =
                    new BatchEventProcessorBuilder().build(ringBuffer, ringBuffer.newBarrier(), handler);

            ringBuffer.addGatingSequences(batchEventProcessor.getSequence());
            executor.execute(batchEventProcessor);

            // Keep track of the subscriber
            subscribers.put(handler, batchEventProcessor);

            // Update the subscriber metrics.
            subscriptionGauge.set(subscribers.size());
        }
    }

    @Override
    public Poller<ObjectEvent<V>> subscribePoller(@NonNull final BlockNodeEventHandler<ObjectEvent<V>> handler) {

        if (!subscribers.containsKey(handler)) {

            final EventPoller<ObjectEvent<V>> eventPoller = ringBuffer.newPoller();
            ringBuffer.addGatingSequences(eventPoller.getSequence());
            pollSubscribers.put(handler, eventPoller);

            // Update the subscriber metrics.
            subscriptionGauge.set(subscribers.size() + pollSubscribers.size());
            return new PollerImpl<>(eventPoller, ringBuffer.getBufferSize());
        }

        return null;
    }

    @Override
    public void unsubscribePoller(@NonNull final BlockNodeEventHandler<ObjectEvent<V>> handler) {
        final EventPoller<ObjectEvent<V>> eventPoller = pollSubscribers.remove(handler);
        if (eventPoller != null) {
            ringBuffer.removeGatingSequence(eventPoller.getSequence());
        }
        LOGGER.log(DEBUG, "Unsubscribed poller handler");

        // Update the subscriber metrics.
        subscriptionGauge.set(subscribers.size() + pollSubscribers.size());
    }

    /**
     * Unsubscribes the given handler from the stream of events.
     *
     * @param handler the handler to unsubscribe
     */
    @Override
    public void unsubscribe(@NonNull final BlockNodeEventHandler<ObjectEvent<V>> handler) {

        // Remove the subscriber
        final var batchEventProcessor = subscribers.remove(handler);
        if (batchEventProcessor != null) {
            // Stop the processor
            batchEventProcessor.halt();

            // Remove the gating sequence from the ring buffer
            ringBuffer.removeGatingSequence(batchEventProcessor.getSequence());
        }

        // Update the subscriber metrics.
        subscriptionGauge.set(subscribers.size() + pollSubscribers.size());
    }

    /**
     * Checks if the given handler is subscribed to the stream of events.
     *
     * @param handler the handler to check
     * @return true if the handler is subscribed, false otherwise
     */
    @Override
    public boolean isSubscribed(@NonNull BlockNodeEventHandler<ObjectEvent<V>> handler) {
        return subscribers.containsKey(handler);
    }

    /** Unsubscribes all the expired handlers from the stream of events. */
    @Override
    public void unsubscribeAllExpired() {
        subscribers.keySet().stream()
                .filter(BlockNodeEventHandler::isTimeoutExpired)
                .forEach(this::unsubscribe);
    }
}
