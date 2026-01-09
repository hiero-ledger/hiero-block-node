// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.messaging;

import static java.lang.System.Logger.Level.TRACE;

import com.lmax.disruptor.BatchEventProcessor;
import com.lmax.disruptor.BatchEventProcessorBuilder;
import com.lmax.disruptor.ExceptionHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.SleepingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.swirlds.metrics.api.Counter;
import com.swirlds.metrics.api.DoubleGauge;
import com.swirlds.metrics.api.LongGauge;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.lang.System.Logger.Level;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.ServiceBuilder;
import org.hiero.block.node.spi.blockmessaging.BackfilledBlockNotification;
import org.hiero.block.node.spi.blockmessaging.BlockItemHandler;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.BlockMessagingFacility;
import org.hiero.block.node.spi.blockmessaging.BlockNotificationHandler;
import org.hiero.block.node.spi.blockmessaging.NewestBlockKnownToNetworkNotification;
import org.hiero.block.node.spi.blockmessaging.NoBackPressureBlockItemHandler;
import org.hiero.block.node.spi.blockmessaging.PersistedNotification;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;

/**
 * Implementation of the MessagingService interface. It uses the LMAX Disruptor to handle block item batches and block
 * notifications. It is designed to be thread safe and can be used by multiple threads.
 */
public class BlockMessagingFacilityImpl implements BlockMessagingFacility {

    /** Logger for the messaging service. */
    private static final System.Logger LOGGER = System.getLogger(BlockMessagingFacilityImpl.class.getName());

    // Metrics
    /** Counter for incoming block items seen by the mediator */
    private Counter blockItemsReceivedCounter;
    /** Counter for notifications issued after verification */
    private Counter blockVerificationNotificationsCounter;
    /** Counter for notifications issued after persistence */
    private Counter blockPersistedNotificationsCounter;
    /** Counter for notifications issued after backfilling */
    private Counter blockBackfilledNotificationsCounter;
    /** Counter for notifications issued after the newest block known to network */
    private Counter newestBlockKnownToNetworkNotificationsCounter;

    /** Gauge for active item listeners */
    private LongGauge itemListenersGauge;
    /** Gauge for active notification listeners */
    private LongGauge notificationListenersGauge;
    /** Gauge for percent of item queue utilised */
    private DoubleGauge itemQueuePercentUsedGauge;
    /** Gauge for percent of notification queue utilised */
    private DoubleGauge notificationQueuePercentUsedGauge;

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
    private Disruptor<BlockItemBatchRingEvent> blockItemDisruptor;

    /**
     * The disruptor that handles the block notifications. It is used to send block notifications to the different
     * handlers. It is a single producer, multiple consumer disruptor.
     */
    private Disruptor<BlockNotificationRingEvent> blockNotificationDisruptor;

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

    private ExecutorService messageForwarder;

    /**
     * {@inheritDoc}
     */
    @NonNull
    @Override
    public List<Class<? extends Record>> configDataTypes() {
        return List.of(MessagingConfig.class);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void init(BlockNodeContext context, ServiceBuilder serviceBuilder) {
        final MessagingConfig messagingConfig = context.configuration().getConfigData(MessagingConfig.class);
        blockItemDisruptor = new Disruptor<>(
                BlockItemBatchRingEvent::new,
                messagingConfig.blockItemQueueSize(),
                VIRTUAL_THREAD_FACTORY,
                ProducerType.SINGLE,
                new SleepingWaitStrategy());
        blockNotificationDisruptor = new Disruptor<>(
                BlockNotificationRingEvent::new,
                messagingConfig.blockNotificationQueueSize(),
                VIRTUAL_THREAD_FACTORY,
                ProducerType.SINGLE,
                new SleepingWaitStrategy());
        // Set the exception handler for the disruptors
        blockItemDisruptor.setDefaultExceptionHandler(BLOCK_ITEM_EXCEPTION_HANDLER);
        blockNotificationDisruptor.setDefaultExceptionHandler(BLOCK_NOTIFICATION_EXCEPTION_HANDLER);

        // Initialize metrics
        initMetrics(context);

        // log successful initialization
        LOGGER.log(
                TRACE,
                "BlockMessagingFacility initialized with block item queue size: {0} and block notification queue size: {1}",
                messagingConfig.blockItemQueueSize(),
                messagingConfig.blockNotificationQueueSize());
        messageForwarder = context.threadPoolManager().createSingleThreadExecutor("messageForwarder");
    }

    /**
     * Initialize metrics for the messaging facility.
     *
     * @param context the block node context
     */
    private void initMetrics(BlockNodeContext context) {
        // Initialize counters
        blockItemsReceivedCounter = context.metrics()
                .getOrCreate(new Counter.Config(METRICS_CATEGORY, "messaging_block_items_received")
                        .withDescription("Incoming block items seen by the mediator"));

        blockVerificationNotificationsCounter = context.metrics()
                .getOrCreate(new Counter.Config(METRICS_CATEGORY, "messaging_block_verification_notifications")
                        .withDescription("Notifications issued after verification"));

        blockPersistedNotificationsCounter = context.metrics()
                .getOrCreate(new Counter.Config(METRICS_CATEGORY, "messaging_block_persisted_notifications")
                        .withDescription("Notifications issued after persistence"));

        blockBackfilledNotificationsCounter = context.metrics()
                .getOrCreate(new Counter.Config(METRICS_CATEGORY, "messaging_block_backfilled_notifications")
                        .withDescription("Notifications issued after backfilling"));

        newestBlockKnownToNetworkNotificationsCounter = context.metrics()
                .getOrCreate(
                        new Counter.Config(METRICS_CATEGORY, "messaging_newest_block_known_to_network_notifications")
                                .withDescription("Notifications issued after the newest block known to network"));

        // Initialize gauges
        itemListenersGauge = context.metrics()
                .getOrCreate(new LongGauge.Config(METRICS_CATEGORY, "messaging_no_of_item_listeners")
                        .withDescription("Active item listeners"));

        notificationListenersGauge = context.metrics()
                .getOrCreate(new LongGauge.Config(METRICS_CATEGORY, "messaging_no_of_notification_listeners")
                        .withDescription("Active notification listeners"));

        itemQueuePercentUsedGauge = context.metrics()
                .getOrCreate(new DoubleGauge.Config(METRICS_CATEGORY, "messaging_item_queue_percent_used")
                        .withDescription("Percent of item queue utilised"));

        notificationQueuePercentUsedGauge = context.metrics()
                .getOrCreate(new DoubleGauge.Config(METRICS_CATEGORY, "messaging_notification_queue_percent_used")
                        .withDescription("Percent of notification queue utilised"));

        // Register metrics updater for gauges
        context.metrics().addUpdater(this::updateMetrics);
    }

    /**
     * Update gauge metrics with current values.
     */
    private void updateMetrics() {
        // Update listener count gauges
        itemListenersGauge.set(blockItemHandlerToThread.size());
        notificationListenersGauge.set(blockNotificationHandlerToThread.size());

        // Calculate and update item queue usage
        if (blockItemDisruptor != null && blockItemDisruptor.hasStarted()) {
            RingBuffer<BlockItemBatchRingEvent> itemRing = blockItemDisruptor.getRingBuffer();
            long itemCursor = itemRing.getCursor();
            long itemMinSequence = itemRing.getMinimumGatingSequence();
            double percentUsed = ((double) (itemCursor - itemMinSequence) / (double) itemRing.getBufferSize()) * 100.0;
            itemQueuePercentUsedGauge.set(Math.min(100.0, Math.max(0.0, percentUsed)));
        }

        // Calculate and update notification queue usage
        if (blockNotificationDisruptor != null && blockNotificationDisruptor.hasStarted()) {
            RingBuffer<BlockNotificationRingEvent> notificationRing = blockNotificationDisruptor.getRingBuffer();
            long notifCursor = notificationRing.getCursor();
            long notifMinSequence = notificationRing.getMinimumGatingSequence();
            double percentUsed =
                    ((double) (notifCursor - notifMinSequence) / (double) notificationRing.getBufferSize()) * 100.0;
            notificationQueuePercentUsedGauge.set(Math.min(100.0, Math.max(0.0, percentUsed)));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void sendBlockItems(final BlockItems blockItems) {
        blockItemDisruptor.getRingBuffer().publishEvent((event, sequence) -> event.set(blockItems));
        // metrics
        long blockItemsSize = blockItems.blockItems().size();
        blockItemsReceivedCounter.add(blockItemsSize);
        // log sending of block items with details
        LOGGER.log(
                TRACE,
                "Sending block items: size={0} isStartOfNewBlock={1} isEndOfBlock={2}, block={3}",
                blockItemsSize,
                blockItems.isStartOfNewBlock(),
                blockItems.isEndOfBlock(),
                blockItems.blockNumber());
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

        // log the registration of the handler
        LOGGER.log(
                TRACE,
                "Registering block item handler: {0}, cpuIntensive: {1}, handlerName: {2}",
                handler.getClass().getSimpleName(),
                cpuIntensiveHandler,
                handlerName);
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

        // log the registration of the handler
        LOGGER.log(
                TRACE,
                "Registering no backpressure block item handler: {0}, cpuIntensive: {1}, handlerName: {2}",
                handler.getClass().getSimpleName(),
                cpuIntensiveHandler,
                handlerName);
    }

    /**
     * {@inheritDoc}F
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
    public void sendBlockVerification(VerificationNotification notification) {
        messageForwarder.submit(() -> {
            blockNotificationDisruptor.getRingBuffer().publishEvent((event, sequence) -> event.set(notification));
            // metrics
            blockVerificationNotificationsCounter.increment();
            // logs
            LOGGER.log(
                    TRACE,
                    "Sending block verification notification for block={0} blockSource={1} and success={2} ",
                    notification.blockNumber(),
                    notification.source(),
                    notification.success());
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void sendBlockPersisted(PersistedNotification notification) {
        messageForwarder.submit(() -> {
            LOGGER.log(
                    TRACE,
                    "Sending block persisted notification: block={0} succeeded={1} source={2}",
                    notification.blockNumber(),
                    notification.succeeded(),
                    notification.blockSource());
            blockNotificationDisruptor.getRingBuffer().publishEvent((event, sequence) -> event.set(notification));
            blockPersistedNotificationsCounter.increment();
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void sendBackfilledBlockNotification(BackfilledBlockNotification notification) {
        messageForwarder.submit(() -> {
            LOGGER.log(TRACE, "Sending backfilled block notification: block={0}", notification.blockNumber());
            blockNotificationDisruptor.getRingBuffer().publishEvent((event, sequence) -> event.set(notification));
            blockBackfilledNotificationsCounter.increment();
        });
    }

    @Override
    public void sendNewestBlockKnownToNetwork(NewestBlockKnownToNetworkNotification notification) {
        messageForwarder.submit(() -> {
            LOGGER.log(TRACE, "Sending NewestBlockKnownToNetwork notification: block={0}", notification.blockNumber());
            blockNotificationDisruptor.getRingBuffer().publishEvent((event, sequence) -> event.set(notification));
            newestBlockKnownToNetworkNotificationsCounter.increment();
        });
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
                    if (event.getVerificationNotification() != null) {
                        handler.handleVerification(event.getVerificationNotification());
                    } else if (event.getPersistedNotification() != null) {
                        handler.handlePersisted(event.getPersistedNotification());
                    } else if (event.getBackfilledBlockNotification() != null) {
                        handler.handleBackfilled(event.getBackfilledBlockNotification());
                    } else if (event.getNewestBlockKnownToNetworkNotification() != null) {
                        handler.handleNewestBlockKnownToNetwork(event.getNewestBlockKnownToNetworkNotification());
                    } else {
                        LOGGER.log(Level.INFO, "Received an event with no notification set");
                    }
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

        // log the registration of the handler
        LOGGER.log(
                TRACE,
                "Registering block notification handler: {0}, cpuIntensive: {1}, handlerName: {2}",
                handler.getClass().getSimpleName(),
                cpuIntensiveHandler,
                handlerName);
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

        // log successful start
        if (LOGGER.isLoggable(TRACE)) {
            LOGGER.log(
                    TRACE,
                    "BlockMessagingFacility successfully started with block item queue size: {0} and block notification queue size: {1}",
                    blockItemDisruptor.getRingBuffer().getBufferSize(),
                    blockNotificationDisruptor.getRingBuffer().getBufferSize());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void stop() {
        // Stop all the block item event handlers
        for (var eventHandler : blockItemHandlerToEventProcessor.values()) {
            blockItemDisruptor.getRingBuffer().removeGatingSequence(eventHandler.getSequence());
            eventHandler.halt();
        }
        // Stop all the block item handler threads
        for (Thread thread : blockItemHandlerToThread.values()) {
            thread.interrupt();
            // log the stopping of the thread
            LOGGER.log(TRACE, "Stopped block item handler thread: {0}", thread.getName());
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
            // log the stopping of the thread
            LOGGER.log(TRACE, "Stopped block notification handler thread: {0}", thread.getName());
        }
        messageForwarder.shutdown();
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
            // log the unregistration of the handler
            LOGGER.log(
                    TRACE,
                    "Unregistered block item handler: {0}, thread: {1}",
                    handler.getClass().getSimpleName(),
                    handlerThread.getName());
        }
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
         */
        void onEvent(T event, long sequence, boolean endOfBatch, double percentageBehindRingHead);
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
