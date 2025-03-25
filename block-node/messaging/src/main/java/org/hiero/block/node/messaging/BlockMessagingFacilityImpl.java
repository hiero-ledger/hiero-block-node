// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.messaging;

import static org.hiero.block.node.spi.BlockNodePlugin.UNKNOWN_BLOCK_NUMBER;

import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.pbj.runtime.ParseException;
import com.lmax.disruptor.BatchEventProcessor;
import com.lmax.disruptor.BatchEventProcessorBuilder;
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
import org.hiero.block.node.spi.blockmessaging.BlockItemHandler;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.BlockMessagingFacility;
import org.hiero.block.node.spi.blockmessaging.BlockNotification;
import org.hiero.block.node.spi.blockmessaging.BlockNotificationHandler;
import org.hiero.block.node.spi.blockmessaging.NoBackPressureBlockItemHandler;
import org.hiero.hapi.block.node.BlockItemUnparsed;

/**
 * Implementation of the MessagingService interface. It uses the LMAX Disruptor to handle block item batches and block
 * notifications. It is designed to be thread safe and can be used by multiple threads.
 */
public class BlockMessagingFacilityImpl implements BlockMessagingFacility {

    /** Logger for the messaging service. */
    private static final System.Logger LOGGER = System.getLogger(BlockMessagingFacilityImpl.class.getName());

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
     * The thread factory used to create the normal platform threads for the disruptor.
     */
    public static final ThreadFactory PLATFORM_THREAD_FACTORY = Thread.ofPlatform()
            .name("messaging-service-handler", 0)
            .daemon(true)
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

    /** Map of block item handlers to their threads. So that we can stop them */
    private final Map<BlockItemHandler, Thread> blockItemHandlerToThread = new HashMap<>();

    /** Map of block item handlers to their event processors. So that we can stop them */
    private final Map<BlockItemHandler, BatchEventProcessor<BlockItemBatchRingEvent>> blockItemHandlerToEventProcessor =
            new HashMap<>();

    /** Map of block notification handlers to their threads. So that we can stop them */
    private final Map<BlockNotificationHandler, Thread> blockNotificationHandlerToThread = new HashMap<>();

    /** Map of block notification handlers to their event processors. So that we can stop them */
    private final Map<BlockNotificationHandler, BatchEventProcessor<BlockNotificationRingEvent>>
            blockNotificationHandlerToEventProcessor = new HashMap<>();

    /**
     * List of pre-registered block item handlers, that were registered before the service started. These will be added
     * when the service is started and the list cleared
     */
    private final List<PreRegisteredBlockItemHandler> preRegisteredBlockItemHandlers = new ArrayList<>();

    /**
     * List of pre-registered block notification handlers, that were registered before the service started. These will
     * be added when the service is started and the list cleared
     */
    private final List<PreRegisteredBlockNotificationHandler> preRegisteredBlockNotificationHandlers =
            new ArrayList<>();

    /**
     * Constructs a new MessagingServiceImpl instance with the default configuration. It uses the
     * ConfigurationBuilderFactory to load the configuration from the classpath.
     */
    public BlockMessagingFacilityImpl() {
        this(getConfig());
    }

    /**
     * Constructs a new MessagingServiceImpl instance with the given configuration.
     *
     * @param config the configuration for the messaging service
     */
    public BlockMessagingFacilityImpl(final MessagingConfig config) {
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
        // Set the exception handler for the disruptors
        blockItemDisruptor.setDefaultExceptionHandler(BLOCK_ITEM_EXCEPTION_HANDLER);
        blockNotificationDisruptor.setDefaultExceptionHandler(BLOCK_NOTIFICATION_EXCEPTION_HANDLER);
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
        final ServiceLoader<ConfigurationBuilderFactory> serviceLoader = ServiceLoader.load(
                ConfigurationBuilderFactory.class, BlockMessagingFacilityImpl.class.getClassLoader());
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
    public void sendBlockItems(final BlockItems blockItems) {
        blockItemDisruptor.getRingBuffer().publishEvent((event, sequence) -> event.set(blockItems));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void registerBlockItemHandler(
            final BlockItemHandler handler, final boolean cpuIntensiveHandler, final String handlerName) {
        final InformedEventHandler<BlockItemBatchRingEvent> informedEventHandler =
                (event, sequence, endOfBatch, percentageBehindRingHead) ->
                        handler.handleBlockItemsReceived(event.get());
        if (blockItemDisruptor.hasStarted()) {
            // if the disruptor is already running, we need to register the handler with the disruptor
            registerHandler(
                    handler,
                    cpuIntensiveHandler,
                    handlerName,
                    blockItemDisruptor.getRingBuffer(),
                    informedEventHandler,
                    blockItemHandlerToEventProcessor,
                    blockItemHandlerToThread);
        } else {
            // if the disruptor is not running, we need to add the handler to the list of pre-registered handlers
            preRegisteredBlockItemHandlers.add(
                    new PreRegisteredBlockItemHandler(handler, informedEventHandler, cpuIntensiveHandler, handlerName));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void registerNoBackpressureBlockItemHandler(
            final NoBackPressureBlockItemHandler handler, final boolean cpuIntensiveHandler, final String handlerName) {
        final InformedEventHandler<BlockItemBatchRingEvent> informedEventHandler =
                (event, sequence, endOfBatch, percentageBehindRingHead) -> {
                    // send on the event block items
                    handler.handleBlockItemsReceived(event.get());
                    if (percentageBehindRingHead > 80) {
                        // If the event processor is more than 80% behind, we need to stop it.
                        // This is a sign that the event processor is not able to keep up with the
                        // rate of events being published.
                        unregisterBlockItemHandler(handler);
                        // the handler it got too far behind
                        handler.onTooFarBehindError();
                    }
                };
        if (blockItemDisruptor.hasStarted()) {
            registerHandler(
                    handler,
                    cpuIntensiveHandler,
                    handlerName,
                    blockItemDisruptor.getRingBuffer(),
                    informedEventHandler,
                    blockItemHandlerToEventProcessor,
                    blockItemHandlerToThread);
        } else {
            // if the disruptor is not running, we need to add the handler to the list of pre-registered handlers
            preRegisteredBlockItemHandlers.add(
                    new PreRegisteredBlockItemHandler(handler, informedEventHandler, cpuIntensiveHandler, handlerName));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void unregisterBlockItemHandler(final BlockItemHandler handler) {
        unregisterHandler(
                handler,
                blockItemDisruptor.getRingBuffer(),
                blockItemHandlerToEventProcessor,
                blockItemHandlerToThread);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void sendBlockNotification(final BlockNotification notification) {
        blockNotificationDisruptor.getRingBuffer().publishEvent((event, sequence) -> event.set(notification));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void registerBlockNotificationHandler(
            final BlockNotificationHandler handler, final boolean cpuIntensiveHandler, final String handlerName) {
        final InformedEventHandler<BlockNotificationRingEvent> informedEventHandler =
                (event, sequence, endOfBatch, percentageBehindRingHead) -> {
                    // send on the event
                    handler.handleBlockNotification(event.get());
                };
        if (blockNotificationDisruptor.hasStarted()) {
            // if the disruptor is already running, we need to register the handler with the disruptor
            registerHandler(
                    handler,
                    cpuIntensiveHandler,
                    handlerName,
                    blockNotificationDisruptor.getRingBuffer(),
                    informedEventHandler,
                    blockNotificationHandlerToEventProcessor,
                    blockNotificationHandlerToThread);
        } else {
            // if the disruptor is not running, we need to add the handler to the list of pre-registered handlers
            preRegisteredBlockNotificationHandlers.add(new PreRegisteredBlockNotificationHandler(
                    handler, informedEventHandler, cpuIntensiveHandler, handlerName));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void unregisterBlockNotificationHandler(final BlockNotificationHandler handler) {
        unregisterHandler(
                handler,
                blockNotificationDisruptor.getRingBuffer(),
                blockNotificationHandlerToEventProcessor,
                blockNotificationHandlerToThread);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void start() {
        // start the disruptors
        blockItemDisruptor.start();
        blockNotificationDisruptor.start();
        // register all the pre-registered block item handlers
        for (var preRegisteredHandler : preRegisteredBlockItemHandlers) {
            registerHandler(
                    preRegisteredHandler.handler(),
                    preRegisteredHandler.cpuIntensiveHandler(),
                    preRegisteredHandler.handlerName(),
                    blockItemDisruptor.getRingBuffer(),
                    preRegisteredHandler.informedHandler(),
                    blockItemHandlerToEventProcessor,
                    blockItemHandlerToThread);
        }
        // register all the pre-registered block notification handlers
        for (var preRegisteredHandler : preRegisteredBlockNotificationHandlers) {
            registerHandler(
                    preRegisteredHandler.handler(),
                    preRegisteredHandler.cpuIntensiveHandler(),
                    preRegisteredHandler.handlerName(),
                    blockNotificationDisruptor.getRingBuffer(),
                    preRegisteredHandler.informedHandler(),
                    blockNotificationHandlerToEventProcessor,
                    blockNotificationHandlerToThread);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void shutdown() {
        // Stop all the block item event handlers
        for (var eventHandler : blockItemHandlerToEventProcessor.values()) {
            blockItemDisruptor.getRingBuffer().removeGatingSequence(eventHandler.getSequence());
            eventHandler.halt();
        }
        // Stop all the block item handler threads
        for (Thread thread : blockItemHandlerToThread.values()) {
            thread.interrupt();
        }
        // Stop all the block notification event handlers
        for (var eventHandler : blockNotificationHandlerToEventProcessor.values()) {
            blockNotificationDisruptor.getRingBuffer().removeGatingSequence(eventHandler.getSequence());
            eventHandler.halt();
        }
        // Shuts down all the threads handling events.
        blockItemDisruptor.shutdown();
        blockNotificationDisruptor.shutdown();
        // Stop all the block notification handlers
        for (Thread thread : blockNotificationHandlerToThread.values()) {
            thread.interrupt();
        }
    }

    /**
     * Registers a handler with the ring buffer. This generic method allows all the logic to be common and hence any bug
     * hopefully only need fixing once. Any improvements can be made in one place.
     *
     * @param <H> the type of the handler
     * @param <E> the type of the event
     * @param handler the handler to register
     * @param cpuIntensiveHandler hint to the service that this handler is CPU intensive vs IO intensive
     * @param handlerName the name of the handler, used for thread name and logging
     * @param ringBuffer the ring buffer to register with
     * @param informedEventHandler the event handler to call when an event is published
     * @param handlerToEventProcessor the map of handlers to event processors
     * @param handlerToThread the map of handlers to threads
     */
    private static <H, E> void registerHandler(
            final H handler,
            final boolean cpuIntensiveHandler,
            final String handlerName,
            final RingBuffer<E> ringBuffer,
            final InformedEventHandler<E> informedEventHandler,
            final Map<H, BatchEventProcessor<E>> handlerToEventProcessor,
            final Map<H, Thread> handlerToThread) {
        final SequenceBarrier barrier = ringBuffer.newBarrier();
        // Create the event processor for the block item batch ring
        final BatchEventProcessor<E> batchEventProcessor = new BatchEventProcessorBuilder()
                .build(ringBuffer, barrier, (event, sequence, endOfBatch) -> {
                    // calculate position in the ring buffer
                    final double percentageBehindHead =
                            (100d * ((double) (barrier.getCursor() - sequence) / (double) ringBuffer.getBufferSize()));
                    // send on the event
                    informedEventHandler.onEvent(event, sequence, endOfBatch, percentageBehindHead);
                });
        // Dynamically add sequences to the ring buffer
        ringBuffer.addGatingSequences(batchEventProcessor.getSequence());
        // Create the new virtual thread to power the batch processor
        final Thread handlerThread = cpuIntensiveHandler
                ? PLATFORM_THREAD_FACTORY.newThread(batchEventProcessor)
                : VIRTUAL_THREAD_FACTORY.newThread(batchEventProcessor);
        handlerThread.setName("MessageHandler:" + (handlerName == null ? "Unknown" : handlerName));
        // keep track of the event processor & thread so we can stop them later
        handlerToEventProcessor.put(handler, batchEventProcessor);
        handlerToThread.put(handler, handlerThread);
        // start the event processor thread
        handlerThread.start();
    }

    /**
     * Unregisters the handler from the ring buffer and stops the event processor. This generic method allows all the
     * logic to be common and hence any bug hopefully only need fixing once. Any improvements can be made in one place.
     *
     * @param <H> the type of the handler
     * @param <E> the type of the event
     * @param handler the handler to unregister
     * @param ringBuffer the ring buffer to unregister from
     * @param handlerToEventProcessor the map of handlers to event processors
     * @param handlerToThread the map of handlers to threads
     */
    private static <H, E> void unregisterHandler(
            final H handler,
            final RingBuffer<E> ringBuffer,
            final Map<H, BatchEventProcessor<E>> handlerToEventProcessor,
            final Map<H, Thread> handlerToThread) {
        final Thread handlerThread = handlerToThread.remove(handler);
        final BatchEventProcessor<E> eventProcessor = handlerToEventProcessor.remove(handler);
        if (eventProcessor != null) {
            ringBuffer.removeGatingSequence(eventProcessor.getSequence());
            // stop the event processor
            eventProcessor.halt();
        }
        // interrupt the thread so it stops quickly
        if (handlerThread != null) {
            handlerThread.interrupt();
        }
    }

    /**
     * Utility method to get the block number from the block items. If the block items are staring from start of new
     * block with a block header.
     *
     * @param blockItems The block items to get the block number from
     * @return The block number if the block items are start of new block, otherwise UNKNOWN_BLOCK_NUMBER
     */
    private static long getBlockNumberFromBlockItems(List<BlockItemUnparsed> blockItems) {
        if (!blockItems.isEmpty() && blockItems.getFirst().hasBlockHeader()) {
            try {
                //noinspection DataFlowIssue
                return BlockHeader.PROTOBUF
                        .parse(blockItems.getFirst().blockHeader())
                        .number();
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }
        }
        return UNKNOWN_BLOCK_NUMBER;
    }

    /**
     * Extended EventHandler interface that provides the percentage behind the ring head to the event handler.
     *
     * @param <T> the type of the event
     */
    private interface InformedEventHandler<T> {
        /**
         * Called when a publisher has published an event to the {@link RingBuffer}.  The {@link BatchEventProcessor} will
         * read messages from the {@link RingBuffer} in batches, where a batch is all the events available to be
         * processed without having to wait for any new event to arrive.  This can be useful for event handlers that need
         * to do slower operations like I/O as they can group together the data from multiple events into a single
         * operation.  Implementations should ensure that the operation is always performed when endOfBatch is true as
         * the time between that message and the next one is indeterminate.
         *
         * @param event      published to the {@link RingBuffer}
         * @param sequence   of the event being processed
         * @param endOfBatch flag to indicate if this is the last event in a batch from the {@link RingBuffer}
         * @param percentageBehindRingHead percentage 0.0 to 100.0 behind the ring head this handler is
         * @throws Exception if the EventHandler would like the exception handled further up the chain.
         */
        void onEvent(T event, long sequence, boolean endOfBatch, double percentageBehindRingHead) throws Exception;
    }

    /**
     * Record for pre-registered block item handlers.
     *
     * @param handler the block item handler
     * @param informedHandler the event handler to call when an event is published
     * @param cpuIntensiveHandler hint to the service that this handler is CPU intensive vs IO intensive
     * @param handlerName the name of the handler, used for thread name and logging
     */
    private record PreRegisteredBlockItemHandler(
            BlockItemHandler handler,
            InformedEventHandler<BlockItemBatchRingEvent> informedHandler,
            boolean cpuIntensiveHandler,
            String handlerName) {}

    /**
     * Record for pre-registered block notification handlers.
     *
     * @param handler the block notification handler
     * @param informedHandler the informed event handler
     * @param cpuIntensiveHandler hint to the service that this handler is CPU intensive vs IO intensive
     * @param handlerName the name of the handler, used for thread name and logging
     */
    private record PreRegisteredBlockNotificationHandler(
            BlockNotificationHandler handler,
            InformedEventHandler<BlockNotificationRingEvent> informedHandler,
            boolean cpuIntensiveHandler,
            String handlerName) {}
}
