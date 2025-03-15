// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.messaging.impl;

import com.hedera.hapi.block.BlockItemUnparsed;
import com.lmax.disruptor.BatchEventProcessor;
import com.lmax.disruptor.BatchEventProcessorBuilder;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.ExceptionHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.SleepingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.swirlds.config.api.spi.ConfigurationBuilderFactory;
import java.lang.System.Logger.Level;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ThreadFactory;
import org.hiero.block.server.messaging.BlockItemHandler;
import org.hiero.block.server.messaging.BlockNotification;
import org.hiero.block.server.messaging.BlockNotificationHandler;
import org.hiero.block.server.messaging.MessagingService;
import org.hiero.block.server.messaging.NoBackPressureBlockItemHandler;

/**
 * Implementation of the MessagingService interface. It uses the LMAX Disruptor to handle block item batches and block
 * notifications.
 */
public class MessagingServiceImpl implements MessagingService {

    /** Logger for the messaging service. */
    private static final System.Logger LOGGER = System.getLogger(MessagingServiceImpl.class.getName());

    /**
     * The thread factory used to create the virtual threads for the disruptor. Virtual threads are daemon threads by
     * default.
     */
    public static final ThreadFactory VIRTUAL_THREAD_FACTORY = Thread.ofVirtual()
            .name("messaging-service-handler", 0)
            .uncaughtExceptionHandler((thread, throwable) -> LOGGER.log(
                    Level.ERROR,
                    "Uncaught exception in thread " + thread.getName() + ": " + throwable.getMessage(),
                    throwable))
            .factory();

    /**
     * The exception handler for the block item batch disruptor. It handles exceptions in the block item batch event
     * handlers.
     */
    private static final ExceptionHandler<BlockItemBatchRingEvent> BLOCK_ITEM_EXCEPTION_HANDLER =
            new ExceptionHandler<>() {
                @Override
                public void handleEventException(
                        final Throwable ex, final long sequence, final BlockItemBatchRingEvent event) {
                    LOGGER.log(Level.ERROR, "Exception in block item batch event: " + ex.getMessage(), ex);
                }

                @Override
                public void handleOnStartException(Throwable ex) {
                    LOGGER.log(Level.ERROR, "Exception in block item disruptor startup: " + ex.getMessage(), ex);
                }

                @Override
                public void handleOnShutdownException(Throwable ex) {
                    LOGGER.log(Level.ERROR, "Exception in block item disruptor shutdown: " + ex.getMessage(), ex);
                }
            };

    /**
     * The exception handler for the block notification disruptor. It handles exceptions in the block notification
     * event handlers.
     */
    private static final ExceptionHandler<BlockNotificationRingEvent> BLOCK_NOTIFICATION_EXCEPTION_HANDLER =
            new ExceptionHandler<>() {
                @Override
                public void handleEventException(
                        final Throwable ex, final long sequence, final BlockNotificationRingEvent event) {
                    LOGGER.log(Level.ERROR, "Exception in block notification ring: " + ex.getMessage(), ex);
                }

                @Override
                public void handleOnStartException(Throwable ex) {
                    LOGGER.log(
                            Level.ERROR, "Exception in block notification disruptor startup: " + ex.getMessage(), ex);
                }

                @Override
                public void handleOnShutdownException(Throwable ex) {
                    LOGGER.log(
                            Level.ERROR, "Exception in block notification disruptor shutdown: " + ex.getMessage(), ex);
                }
            };

    /**
     * The disruptor that handles the block item batches. It is used to send block items to the different handlers.
     * It is a single producer, multiple consumer disruptor.
     */
    private final Disruptor<BlockItemBatchRingEvent> blockItemDisruptor;

    /**
     * The disruptor that handles the block notifications. It is used to send block notifications to the different
     * handlers. It is a single producer, multiple consumer disruptor.
     */
    private final Disruptor<BlockNotificationRingEvent> blockNotificationDisruptor;

    /** Collect all block item handlers till start then register them with the disruptor. */
    private final ArrayList<EventHandler<BlockItemBatchRingEvent>> blockItemHandlers = new ArrayList<>();

    /** Collect all block notification handlers till start then register them with the disruptor. */
    private final ArrayList<EventHandler<BlockNotificationRingEvent>> blockNotificationHandlers = new ArrayList<>();

    /** Map of dynamic no back pressure block item handlers to their threads. So that we can stop them */
    private final Map<NoBackPressureBlockItemHandler, Thread> dynamicNoBackPressureBlockItemHandlers = new HashMap<>();

    /** Map of dynamic no back pressure block item handlers to their event processors. So that we can stop them */
    private final Map<NoBackPressureBlockItemHandler, BatchEventProcessor<BlockItemBatchRingEvent>>
            dynamicNoBackPressureBlockItemHandlerToEventProcessor = new HashMap<>();

    /**
     * Constructs a new MessagingServiceImpl instance with the default configuration. It uses the
     * ConfigurationBuilderFactory to load the configuration from the classpath.
     */
    public MessagingServiceImpl() {
        this(getConfig());
    }

    /**
     * Constructs a new MessagingServiceImpl instance with the given configuration.
     *
     * @param config the configuration for the messaging service
     */
    public MessagingServiceImpl(final MessagingConfig config) {
        blockItemDisruptor = new Disruptor<>(
                BlockItemBatchRingEvent::new,
                config.queueSize(),
                VIRTUAL_THREAD_FACTORY,
                ProducerType.SINGLE,
                new SleepingWaitStrategy());
        blockNotificationDisruptor = new Disruptor<>(
                BlockNotificationRingEvent::new,
                config.queueSize(),
                VIRTUAL_THREAD_FACTORY,
                ProducerType.SINGLE,
                new SleepingWaitStrategy());
    }

    /**
     * Get the configuration for the messaging service. It uses the ConfigurationBuilderFactory to load the
     * configuration from the classpath. Public as it is used in tests, but is not exported from the module.
     *
     * @return the configuration for the messaging service
     * @throws IllegalStateException if no ConfigurationBuilderFactory implementation is found
     */
    public static MessagingConfig getConfig() {
        ConfigurationBuilderFactory configurationBuilderFactory = null;
        final ServiceLoader<ConfigurationBuilderFactory> serviceLoader =
                ServiceLoader.load(ConfigurationBuilderFactory.class, MessagingServiceImpl.class.getClassLoader());
        final Iterator<ConfigurationBuilderFactory> iterator = serviceLoader.iterator();
        if (iterator.hasNext()) {
            configurationBuilderFactory = iterator.next();
        }
        if (configurationBuilderFactory == null) {
            throw new IllegalStateException("No ConfigurationBuilderFactory implementation found!");
        }
        // Load the configuration from the classpath
        return configurationBuilderFactory
                .create()
                .autoDiscoverExtensions()
                .withConfigDataType(MessagingConfig.class)
                .build()
                .getConfigData(MessagingConfig.class);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void sendBlockItems(final List<BlockItemUnparsed> items) {
        blockItemDisruptor.getRingBuffer().publishEvent((event, sequence) -> event.set(items));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void registerBlockItemHandler(BlockItemHandler handler) throws IllegalStateException {
        if (blockItemDisruptor.hasStarted()) {
            throw new IllegalStateException("Cannot register block item handler after the service is started");
        }
        blockItemHandlers.add((event, sequence, endOfBatch) -> handler.handleBlockItemsReceived(event.get()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void registerDynamicNoBackpressureBlockItemHandler(NoBackPressureBlockItemHandler handler) {
        final RingBuffer<BlockItemBatchRingEvent> ringBuffer = blockItemDisruptor.getRingBuffer();
        final SequenceBarrier barrier = ringBuffer.newBarrier();
        final EventHandler<BlockItemBatchRingEvent> eventHandler = (event, sequence, endOfBatch) -> {
            // send on the event block items
            handler.handleBlockItemsReceived(event.get());
            // check if the event processor is too far behind
            final double percentageBehindHead =
                    (100d * ((double) (barrier.getCursor() - sequence) / (double) ringBuffer.getBufferSize()));
            if (percentageBehindHead > 80) {
                // If the event processor is more than 80% behind, we need to stop it.
                // This is a sign that the event processor is not able to keep up with the
                // rate of events being published.
                unregisterDynamicNoBackpressureBlockItemHandler(handler);
                // the handler it got too far behind
                handler.onTooFarBehindError();
            }
        };
        // Create the event processor for the block item batch ring
        final BatchEventProcessor<BlockItemBatchRingEvent> batchEventProcessor =
                new BatchEventProcessorBuilder().build(ringBuffer, barrier, eventHandler);
        // Dynamically add sequences to the ring buffer
        ringBuffer.addGatingSequences(batchEventProcessor.getSequence());
        // Create the new virtual thread to power the batch processor
        final Thread handlerThread = VIRTUAL_THREAD_FACTORY.newThread(batchEventProcessor);
        // keep track of the event processor & thread so we can stop them later
        dynamicNoBackPressureBlockItemHandlerToEventProcessor.put(handler, batchEventProcessor);
        dynamicNoBackPressureBlockItemHandlers.put(handler, handlerThread);
        // start the event processor thread
        handlerThread.start();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void unregisterDynamicNoBackpressureBlockItemHandler(NoBackPressureBlockItemHandler handler) {
        final Thread handlerThread = dynamicNoBackPressureBlockItemHandlers.remove(handler);
        final BatchEventProcessor<BlockItemBatchRingEvent> eventProcessor =
                dynamicNoBackPressureBlockItemHandlerToEventProcessor.remove(handler);
        if (eventProcessor != null) {
            blockItemDisruptor.getRingBuffer().removeGatingSequence(eventProcessor.getSequence());
            // stop the event processor
            eventProcessor.halt();
        }
        // interrupt the thread so it stops quickly
        if (handlerThread != null) {
            handlerThread.interrupt();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void sendBlockNotification(BlockNotification notification) {
        blockNotificationDisruptor.getRingBuffer().publishEvent((event, sequence) -> event.set(notification));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void registerBlockNotificationHandler(BlockNotificationHandler handler) throws IllegalStateException {
        blockNotificationHandlers.add((event, sequence, endOfBatch) -> handler.handleBlockNotification(event.get()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void start() {
        // Register the block item handlers with the disruptor
        //noinspection unchecked
        blockItemDisruptor.handleEventsWith(blockItemHandlers.toArray(new EventHandler[0]));
        // Register the block notification handlers with the disruptor
        //noinspection unchecked
        blockNotificationDisruptor.handleEventsWith(blockNotificationHandlers.toArray(new EventHandler[0]));
        // Set the exception handler for the disruptors
        blockItemDisruptor.setDefaultExceptionHandler(BLOCK_ITEM_EXCEPTION_HANDLER);
        blockNotificationDisruptor.setDefaultExceptionHandler(BLOCK_NOTIFICATION_EXCEPTION_HANDLER);
        // start the disruptors
        blockItemDisruptor.start();
        blockNotificationDisruptor.start();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void shutdown() {
        // Shuts down all the threads handling events.
        blockItemDisruptor.shutdown();
        blockNotificationDisruptor.shutdown();
        // Stop all the dynamic no back pressure block item handlers
        for (Thread thread : dynamicNoBackPressureBlockItemHandlers.values()) {
            thread.interrupt();
        }
    }
}
