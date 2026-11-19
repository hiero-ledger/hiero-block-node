// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.messaging;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.ERROR;
import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.TRACE;
import static java.lang.System.Logger.Level.WARNING;

import com.lmax.disruptor.BatchEventProcessor;
import com.lmax.disruptor.BatchEventProcessorBuilder;
import com.lmax.disruptor.ExceptionHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import java.io.InterruptedIOException;
import java.lang.System.Logger.Level;
import java.nio.channels.ClosedByInterruptException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.ServiceBuilder;
import org.hiero.block.node.spi.blockmessaging.AddressBookHistoryNotification;
import org.hiero.block.node.spi.blockmessaging.ApplicationStateNotificationHandler;
import org.hiero.block.node.spi.blockmessaging.AvailableBlocksNotification;
import org.hiero.block.node.spi.blockmessaging.BackfilledBlockNotification;
import org.hiero.block.node.spi.blockmessaging.BlockItemHandler;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.BlockMessagingFacility;
import org.hiero.block.node.spi.blockmessaging.BlockNotificationHandler;
import org.hiero.block.node.spi.blockmessaging.GatingHandler;
import org.hiero.block.node.spi.blockmessaging.NewestBlockKnownToNetworkNotification;
import org.hiero.block.node.spi.blockmessaging.NextExpectedPublisherBlockNotification;
import org.hiero.block.node.spi.blockmessaging.NoBackPressureBlockItemHandler;
import org.hiero.block.node.spi.blockmessaging.PersistedNotification;
import org.hiero.block.node.spi.blockmessaging.PublisherStatusUpdateNotification;
import org.hiero.block.node.spi.blockmessaging.StoredBlocksNotification;
import org.hiero.block.node.spi.blockmessaging.TssDataNotification;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import org.hiero.metrics.LongCounter;
import org.hiero.metrics.ObservableGauge;
import org.hiero.metrics.core.MetricKey;
import org.hiero.metrics.core.MetricRegistry;

/**
 * Implementation of the MessagingService interface. It uses the LMAX Disruptor to handle block item batches, block
 * notifications, unvalidated blocks and verified blocks. It is designed to be thread safe and can be used by multiple
 * threads.
 * <p>
 * Unvalidated blocks are whole blocks that are not verified yet and do not come from the live publisher, for now the
 * blocks fetched by backfill. They have a ring of their own, so a burst of them never makes the live block item
 * producer or the block notification senders wait. Handlers registered with {@link #registerUnvalidatedBlockHandler}
 * receive them through their {@link BlockNotificationHandler#handleBackfilled} method, on a thread of their own.
 * <p>
 * Verified blocks are the results of block verification, which carry the whole block when verification succeeded.
 * Only handlers registered with {@link #registerVerifiedBlockHandler} receive them, through their
 * {@link BlockNotificationHandler#handleVerification} method, and a handler registered only that way receives no other
 * block notifications. Where they travel depends on {@link MessagingConfig#verifiedBlockRingEnabled()}:
 * <ul>
 *     <li>false, the default: on the block notification ring, through the message forwarder, as before the verified
 *     block ring existed. A verified block handler then has a registration on that ring, a single one shared with its
 *     block notification registration if it has one, so it receives its verification results and its other block
 *     notifications on one thread, in the order they were sent.</li>
 *     <li>true: on the verified block ring, a ring of their own, on a thread of their own per handler. THE VERIFIED
 *     BLOCK RING IS NOT READY FOR PRODUCTION: it is for testing only and must not be enabled in production.</li>
 * </ul>
 */
public class BlockMessagingFacilityImpl implements BlockMessagingFacility {

    /** Logger for the messaging service. */
    private static final System.Logger LOGGER = System.getLogger(BlockMessagingFacilityImpl.class.getName());

    /** Metric key for incoming block items seen by the mediator */
    public static final MetricKey<LongCounter> METRIC_MESSAGING_BLOCK_ITEMS_RECEIVED =
            MetricKey.of("messaging_block_items_received", LongCounter.class).addCategory(METRICS_CATEGORY);
    /** Metric key for notifications issued after verification */
    public static final MetricKey<LongCounter> METRIC_MESSAGING_BLOCK_VERIFICATION_NOTIFICATIONS = MetricKey.of(
                    "messaging_block_verification_notifications", LongCounter.class)
            .addCategory(METRICS_CATEGORY);
    /** Metric key for notifications issued after persistence */
    public static final MetricKey<LongCounter> METRIC_MESSAGING_BLOCK_PERSISTED_NOTIFICATIONS = MetricKey.of(
                    "messaging_block_persisted_notifications", LongCounter.class)
            .addCategory(METRICS_CATEGORY);
    /** Metric key for notifications issued after backfilling */
    public static final MetricKey<LongCounter> METRIC_MESSAGING_BLOCK_BACKFILLED_NOTIFICATIONS = MetricKey.of(
                    "messaging_block_backfilled_notifications", LongCounter.class)
            .addCategory(METRICS_CATEGORY);
    /** Metric key for notifications issued after the newest block known to network */
    public static final MetricKey<LongCounter> METRIC_MESSAGING_NEWEST_BLOCK_KNOWN_TO_NETWORK_NOTIFICATIONS =
            MetricKey.of("messaging_newest_block_known_to_network_notifications", LongCounter.class)
                    .addCategory(METRICS_CATEGORY);
    /** Metric key for notifications issued after a publisher was told this node is behind it */
    public static final MetricKey<LongCounter> METRIC_MESSAGING_NEXT_EXPECTED_PUBLISHER_BLOCK_NOTIFICATIONS =
            MetricKey.of("messaging_next_expected_publisher_block_notifications", LongCounter.class)
                    .addCategory(METRICS_CATEGORY);
    /** Metric key for publisher status update notifications sent */
    public static final MetricKey<LongCounter> METRIC_MESSAGING_PUBLISHER_STATUS_UPDATE_NOTIFICATIONS = MetricKey.of(
                    "messaging_publisher_status_update_notifications", LongCounter.class)
            .addCategory(METRICS_CATEGORY);
    /** Metric key for TSS data update notifications sent */
    public static final MetricKey<LongCounter> METRIC_MESSAGING_TSS_DATA_UPDATE_NOTIFICATIONS = MetricKey.of(
                    "messaging_tss_data_update_notifications", LongCounter.class)
            .addCategory(METRICS_CATEGORY);
    /** Metric key for address book history update notifications sent */
    public static final MetricKey<LongCounter> METRIC_MESSAGING_ADDRESS_BOOK_HISTORY_UPDATE_NOTIFICATIONS =
            MetricKey.of("messaging_address_book_history_update_notifications", LongCounter.class)
                    .addCategory(METRICS_CATEGORY);
    /** Metric key for stored blocks update notifications sent */
    public static final MetricKey<LongCounter> METRIC_MESSAGING_STORED_BLOCKS_UPDATE_NOTIFICATIONS = MetricKey.of(
                    "messaging_stored_blocks_update_notifications", LongCounter.class)
            .addCategory(METRICS_CATEGORY);
    /** Metric key for available blocks update notifications sent */
    public static final MetricKey<LongCounter> METRIC_MESSAGING_AVAILABLE_BLOCKS_UPDATE_NOTIFICATIONS = MetricKey.of(
                    "messaging_available_blocks_update_notifications", LongCounter.class)
            .addCategory(METRICS_CATEGORY);
    /** Metric key for messages skipped because a message handler failed on them */
    public static final MetricKey<LongCounter> METRIC_MESSAGING_HANDLER_FAILURES =
            MetricKey.of("messaging_handler_failures", LongCounter.class).addCategory(METRICS_CATEGORY);
    /** Metric key for the number of active item listeners */
    public static final MetricKey<ObservableGauge> METRIC_MESSAGING_NO_OF_ITEM_LISTENERS = MetricKey.of(
                    "messaging_no_of_item_listeners", ObservableGauge.class)
            .addCategory(METRICS_CATEGORY);
    /** Metric key for the number of active notification listeners */
    public static final MetricKey<ObservableGauge> METRIC_MESSAGING_NO_OF_NOTIFICATION_LISTENERS = MetricKey.of(
                    "messaging_no_of_notification_listeners", ObservableGauge.class)
            .addCategory(METRICS_CATEGORY);
    /** Metric key for the percent of item queue utilised */
    public static final MetricKey<ObservableGauge> METRIC_MESSAGING_ITEM_QUEUE_PERCENT_USED = MetricKey.of(
                    "messaging_item_queue_percent_used", ObservableGauge.class)
            .addCategory(METRICS_CATEGORY);
    /** Metric key for the percent of notification queue utilised */
    public static final MetricKey<ObservableGauge> METRIC_MESSAGING_NOTIFICATION_QUEUE_PERCENT_USED = MetricKey.of(
                    "messaging_notification_queue_percent_used", ObservableGauge.class)
            .addCategory(METRICS_CATEGORY);
    /** Metric key for the percent of unvalidated block queue utilised */
    public static final MetricKey<ObservableGauge> METRIC_MESSAGING_UNVALIDATED_BLOCK_QUEUE_PERCENT_USED = MetricKey.of(
                    "messaging_unvalidated_block_queue_percent_used", ObservableGauge.class)
            .addCategory(METRICS_CATEGORY);
    /** Metric key for the percent of verified block queue utilised */
    public static final MetricKey<ObservableGauge> METRIC_MESSAGING_VERIFIED_BLOCK_QUEUE_PERCENT_USED = MetricKey.of(
                    "messaging_verified_block_queue_percent_used", ObservableGauge.class)
            .addCategory(METRICS_CATEGORY);

    // Metrics
    /** Counter for incoming block items seen by the mediator */
    private LongCounter.Measurement blockItemsReceivedCounter;
    /** Counter for notifications issued after verification */
    private LongCounter.Measurement blockVerificationNotificationsCounter;
    /** Counter for notifications issued after persistence */
    private LongCounter.Measurement blockPersistedNotificationsCounter;
    /** Counter for notifications issued after backfilling */
    private LongCounter.Measurement blockBackfilledNotificationsCounter;
    /** Counter for notifications issued after the newest block known to network */
    private LongCounter.Measurement newestBlockKnownToNetworkNotificationsCounter;
    /** Counter for notifications issued after a publisher was told this node is behind it */
    private LongCounter.Measurement nextExpectedPublisherBlockNotificationsCounter;
    /** Counter for publisher status update notifications sent */
    private LongCounter.Measurement publisherStatusUpdateNotificationsCounter;
    /** Counter for TSS data update notifications sent */
    private LongCounter.Measurement tssDataUpdateNotificationsCounter;
    /** Counter for address book history update notifications sent */
    private LongCounter.Measurement addressBookHistoryUpdateNotificationsCounter;
    /** Counter for stored blocks update notifications sent */
    private LongCounter.Measurement storedBlocksUpdateNotificationsCounter;
    /** Counter for available blocks update notifications sent */
    private LongCounter.Measurement availableBlocksUpdateNotificationsCounter;
    /** Counter for messages skipped because a message handler failed on them */
    private LongCounter.Measurement handlerFailuresCounter;

    /**
     * The thread factory for the ring handler threads. Each handler is a dedicated, long-lived loop that mostly waits for
     * the next message, so it gets its own platform thread rather than a virtual thread.
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
     * The disruptor that handles the block item batches. It is used to send block items to the different handlers.
     * It is a single producer, multiple consumer disruptor.
     */
    private Disruptor<BlockItemBatchRingEvent> blockItemDisruptor;

    /**
     * The disruptor that handles the block notifications. It is used to send block notifications to the different
     * handlers. It is a single producer, multiple consumer disruptor.
     */
    private Disruptor<BlockNotificationRingEvent> blockNotificationDisruptor;

    /**
     * The disruptor that handles the unvalidated blocks. It is used to send backfilled blocks to the unvalidated block
     * handlers. It is a multiple producer, multiple consumer disruptor: blocks are published directly from the sending
     * threads, without the message forwarder.
     */
    private Disruptor<UnvalidatedBlockRingEvent> unvalidatedBlockDisruptor;

    /**
     * The disruptor that handles the verified blocks. It is used to send block verification notifications to the
     * verified block handlers, but only when the verified block ring is enabled, see {@link #verifiedBlockRingEnabled}.
     * It is a multiple producer, multiple consumer disruptor: notifications are published directly from the sending
     * threads, without the message forwarder.
     */
    private Disruptor<VerifiedBlockRingEvent> verifiedBlockDisruptor;

    /**
     * Whether block verification notifications travel on the verified block ring, from
     * {@link MessagingConfig#verifiedBlockRingEnabled()}. False by default: they travel on the block notification ring,
     * as before the verified block ring existed. THE VERIFIED BLOCK RING IS NOT READY FOR PRODUCTION.
     */
    private boolean verifiedBlockRingEnabled;

    /**
     * The ways each handler on the block notification ring was registered there. A handler gets one registration, and
     * so one thread, on that ring however many ways it was registered, and keeps it until it is unregistered every way.
     * While the verified block ring is disabled, a verified block handler is registered on the block notification ring
     * too, so a handler registered both ways receives its verification results and its other block notifications on
     * one thread, in the order they were sent.
     * <p>
     * The registrations also decide what each handler receives, whatever the setting of the verified block ring: block
     * verification notifications only reach handlers registered with {@link #registerVerifiedBlockHandler}, and the
     * other block notifications only reach handlers registered with {@link #registerBlockNotificationHandler}. The
     * handler threads read this map for every notification, so its values are immutable sets, replaced on every change.
     */
    private final Map<BlockNotificationHandler, Set<NotificationRingRegistration>> notificationRingRegistrations =
            new ConcurrentHashMap<>();

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

    /** Map of application state notification handlers to their threads. So that we can stop them */
    private final Map<ApplicationStateNotificationHandler, Thread> applicationStateNotificationHandlerToThread =
            new HashMap<>();

    /** Map of application state notification handlers to their event processors. So that we can stop them */
    private final Map<ApplicationStateNotificationHandler, BatchEventProcessor<BlockNotificationRingEvent>>
            applicationStateNotificationHandlerToEventProcessor = new HashMap<>();

    /** Map of unvalidated block handlers to their threads. So that we can stop them */
    private final Map<BlockNotificationHandler, Thread> unvalidatedBlockHandlerToThread = new HashMap<>();

    /** Map of unvalidated block handlers to their event processors. So that we can stop them */
    private final Map<BlockNotificationHandler, BatchEventProcessor<UnvalidatedBlockRingEvent>>
            unvalidatedBlockHandlerToEventProcessor = new HashMap<>();

    /** Map of verified block handlers to their threads. So that we can stop them */
    private final Map<BlockNotificationHandler, Thread> verifiedBlockHandlerToThread = new HashMap<>();

    /** Map of verified block handlers to their event processors. So that we can stop them */
    private final Map<BlockNotificationHandler, BatchEventProcessor<VerifiedBlockRingEvent>>
            verifiedBlockHandlerToEventProcessor = new HashMap<>();

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
     * List of pre-registered application state notification handlers, that were registered before the service started. These
     * will be added when the service is started and the list cleared
     */
    private final List<PreRegisteredApplicationStateNotificationHandler>
            preRegisteredApplicationStateNotificationHandlers = new ArrayList<>();

    /**
     * List of pre-registered unvalidated block handlers, that were registered before the service started. These will be
     * added when the service is started and the list cleared
     */
    private final List<PreRegisteredUnvalidatedBlockHandler> preRegisteredUnvalidatedBlockHandlers = new ArrayList<>();

    /**
     * List of pre-registered verified block handlers, that were registered before the service started. These will be
     * added when the service is started and the list cleared
     */
    private final List<PreRegisteredVerifiedBlockHandler> preRegisteredVerifiedBlockHandlers = new ArrayList<>();

    private ExecutorService messageForwarder;

    /**
     * {@inheritDoc}
     */
    @Override
    public void init(BlockNodeContext context, ServiceBuilder serviceBuilder) {
        final MessagingConfig messagingConfig = context.configuration().getConfigData(MessagingConfig.class);
        blockItemDisruptor = new Disruptor<>(
                BlockItemBatchRingEvent::new,
                messagingConfig.blockItemQueueSize(),
                PLATFORM_THREAD_FACTORY,
                ProducerType.SINGLE,
                createWaitStrategy(messagingConfig));
        blockNotificationDisruptor = new Disruptor<>(
                BlockNotificationRingEvent::new,
                messagingConfig.blockNotificationQueueSize(),
                PLATFORM_THREAD_FACTORY,
                ProducerType.SINGLE,
                createWaitStrategy(messagingConfig));
        // Unvalidated blocks are published directly from the sending threads, and several threads may send at once
        unvalidatedBlockDisruptor = new Disruptor<>(
                UnvalidatedBlockRingEvent::new,
                messagingConfig.unvalidatedBlockQueueSize(),
                PLATFORM_THREAD_FACTORY,
                ProducerType.MULTI,
                createWaitStrategy(messagingConfig));
        // Verified blocks are published directly from the sending threads, and several threads may send at once. The
        // ring only carries messages when it is enabled; otherwise verified block handlers are on the notification ring
        verifiedBlockDisruptor = new Disruptor<>(
                VerifiedBlockRingEvent::new,
                messagingConfig.verifiedBlockQueueSize(),
                PLATFORM_THREAD_FACTORY,
                ProducerType.MULTI,
                createWaitStrategy(messagingConfig));
        verifiedBlockRingEnabled = messagingConfig.verifiedBlockRingEnabled();
        if (verifiedBlockRingEnabled) {
            LOGGER.log(
                    WARNING,
                    "The verified block ring is enabled (messaging.verifiedBlockRingEnabled is true). It is NOT READY"
                            + " FOR PRODUCTION: it is for testing only and must not be enabled in production");
        }

        // Initialize metrics
        initMetrics(context);

        // log successful initialization
        LOGGER.log(
                DEBUG,
                "BlockMessagingFacility initialized with block item queue size: {0}, block notification queue size: {1}"
                        + ", unvalidated block queue size: {2}, verified block queue size: {3} and verified block ring"
                        + " enabled: {4}",
                messagingConfig.blockItemQueueSize(),
                messagingConfig.blockNotificationQueueSize(),
                messagingConfig.unvalidatedBlockQueueSize(),
                messagingConfig.verifiedBlockQueueSize(),
                verifiedBlockRingEnabled);
        LOGGER.log(
                INFO,
                "Messaging rings use wait strategy {0} with yield timeout {1,number,#} us",
                messagingConfig.waitStrategy(),
                messagingConfig.waitStrategyYieldTimeoutMicros());
        messageForwarder = context.threadPoolManager().createSingleThreadExecutor("messageForwarder");
    }

    /**
     * Create the wait strategy for one ring. Every ring needs its own instance.
     *
     * @param messagingConfig the messaging configuration
     * @return a new wait strategy for a ring
     */
    private static WaitStrategy createWaitStrategy(final MessagingConfig messagingConfig) {
        return messagingConfig.waitStrategy().create(messagingConfig.waitStrategyYieldTimeoutMicros());
    }

    /**
     * Initialize metrics for the messaging facility.
     *
     * @param context the block node context
     */
    private void initMetrics(BlockNodeContext context) {
        // Initialize counters
        final MetricRegistry metricRegistry = context.metricRegistry();
        blockItemsReceivedCounter = metricRegistry
                .register(LongCounter.builder(METRIC_MESSAGING_BLOCK_ITEMS_RECEIVED)
                        .setDescription("Incoming block items seen by the mediator"))
                .getOrCreateNotLabeled();

        blockVerificationNotificationsCounter = metricRegistry
                .register(LongCounter.builder(METRIC_MESSAGING_BLOCK_VERIFICATION_NOTIFICATIONS)
                        .setDescription("Notifications issued after verification"))
                .getOrCreateNotLabeled();

        blockPersistedNotificationsCounter = metricRegistry
                .register(LongCounter.builder(METRIC_MESSAGING_BLOCK_PERSISTED_NOTIFICATIONS)
                        .setDescription("Notifications issued after persistence"))
                .getOrCreateNotLabeled();

        blockBackfilledNotificationsCounter = metricRegistry
                .register(LongCounter.builder(METRIC_MESSAGING_BLOCK_BACKFILLED_NOTIFICATIONS)
                        .setDescription("Notifications issued after backfilling"))
                .getOrCreateNotLabeled();

        newestBlockKnownToNetworkNotificationsCounter = metricRegistry
                .register(LongCounter.builder(METRIC_MESSAGING_NEWEST_BLOCK_KNOWN_TO_NETWORK_NOTIFICATIONS)
                        .setDescription("Notifications issued after the newest block known to network"))
                .getOrCreateNotLabeled();

        nextExpectedPublisherBlockNotificationsCounter = metricRegistry
                .register(LongCounter.builder(METRIC_MESSAGING_NEXT_EXPECTED_PUBLISHER_BLOCK_NOTIFICATIONS)
                        .setDescription("Notifications issued after a publisher was told this node is behind it"))
                .getOrCreateNotLabeled();

        publisherStatusUpdateNotificationsCounter = metricRegistry
                .register(LongCounter.builder(METRIC_MESSAGING_PUBLISHER_STATUS_UPDATE_NOTIFICATIONS)
                        .setDescription("Notifications issued for publisher status updates"))
                .getOrCreateNotLabeled();

        tssDataUpdateNotificationsCounter = metricRegistry
                .register(LongCounter.builder(METRIC_MESSAGING_TSS_DATA_UPDATE_NOTIFICATIONS)
                        .setDescription("TSS data update notifications sent"))
                .getOrCreateNotLabeled();

        addressBookHistoryUpdateNotificationsCounter = metricRegistry
                .register(LongCounter.builder(METRIC_MESSAGING_ADDRESS_BOOK_HISTORY_UPDATE_NOTIFICATIONS)
                        .setDescription("Address book history update notifications sent"))
                .getOrCreateNotLabeled();

        storedBlocksUpdateNotificationsCounter = metricRegistry
                .register(LongCounter.builder(METRIC_MESSAGING_STORED_BLOCKS_UPDATE_NOTIFICATIONS)
                        .setDescription("Stored blocks update notifications sent"))
                .getOrCreateNotLabeled();

        availableBlocksUpdateNotificationsCounter = metricRegistry
                .register(LongCounter.builder(METRIC_MESSAGING_AVAILABLE_BLOCKS_UPDATE_NOTIFICATIONS)
                        .setDescription("Available blocks update notifications sent"))
                .getOrCreateNotLabeled();

        handlerFailuresCounter = metricRegistry
                .register(LongCounter.builder(METRIC_MESSAGING_HANDLER_FAILURES)
                        .setDescription("Messages skipped after a message handler failed"))
                .getOrCreateNotLabeled();

        // Initialize gauges
        metricRegistry
                .register(ObservableGauge.builder(METRIC_MESSAGING_NO_OF_ITEM_LISTENERS)
                        .setDescription("Active item listeners"))
                .observe(blockItemHandlerToThread::size);

        metricRegistry
                .register(ObservableGauge.builder(METRIC_MESSAGING_NO_OF_NOTIFICATION_LISTENERS)
                        .setDescription("Active notification listeners"))
                .observe(() ->
                        blockNotificationHandlerToThread.size() + applicationStateNotificationHandlerToThread.size());

        metricRegistry
                .register(ObservableGauge.builder(METRIC_MESSAGING_ITEM_QUEUE_PERCENT_USED)
                        .setDescription("Percent of item queue utilised"))
                .observe(() -> getUsagePercentage(blockItemDisruptor));

        metricRegistry
                .register(ObservableGauge.builder(METRIC_MESSAGING_NOTIFICATION_QUEUE_PERCENT_USED)
                        .setDescription("Percent of notification queue utilised"))
                .observe(() -> getUsagePercentage(blockNotificationDisruptor));

        metricRegistry
                .register(ObservableGauge.builder(METRIC_MESSAGING_UNVALIDATED_BLOCK_QUEUE_PERCENT_USED)
                        .setDescription("Percent of unvalidated block queue utilised"))
                .observe(() -> getUsagePercentage(unvalidatedBlockDisruptor));

        metricRegistry
                .register(ObservableGauge.builder(METRIC_MESSAGING_VERIFIED_BLOCK_QUEUE_PERCENT_USED)
                        .setDescription("Percent of verified block queue utilised"))
                .observe(() -> getUsagePercentage(verifiedBlockDisruptor));
    }

    private <T> Double getUsagePercentage(Disruptor<T> disruptor) {
        if (disruptor == null || !disruptor.hasStarted()) {
            return 0.0;
        }
        RingBuffer<T> itemRing = disruptor.getRingBuffer();
        long itemCursor = itemRing.getCursor();
        long itemMinSequence = itemRing.getMinimumGatingSequence();
        double percentUsed = ((double) (itemCursor - itemMinSequence) / (double) itemRing.getBufferSize()) * 100.0;
        return Math.clamp(percentUsed, 0.0, 100.0);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void sendBlockItems(final BlockItems blockItems) {
        blockItemDisruptor.getRingBuffer().publishEvent((event, sequence) -> event.set(blockItems));
        // metrics
        long blockItemsSize = blockItems.blockItems().size();
        blockItemsReceivedCounter.increment(blockItemsSize);
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
                    handlerName,
                    blockItemDisruptor.getRingBuffer(),
                    informedEventHandler,
                    blockItemHandlerToEventProcessor,
                    blockItemHandlerToThread);
        } else {
            // if the disruptor is not running, we need to add the handler to the list of pre-registered handlers
            preRegisteredBlockItemHandlers.add(
                    new PreRegisteredBlockItemHandler(handler, informedEventHandler, handlerName));
        }

        // log the registration of the handler
        LOGGER.log(
                DEBUG,
                "Registering block item handler: {0}, handlerName: {1}",
                handler.getClass().getSimpleName(),
                handlerName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void registerNoBackpressureBlockItemHandler(
            final NoBackPressureBlockItemHandler handler, final boolean cpuIntensiveHandler, final String handlerName) {
        // One-shot eviction guard: the event processor may deliver a few more buffered events after halt()
        // is called, so we use an AtomicBoolean to ensure unregister and onTooFarBehindError fire exactly once.
        final AtomicBoolean evicted = new AtomicBoolean(false);
        // The handler starts at the ring cursor, which is usually mid-block. Drop batches until the first block start
        // so the handler only ever sees whole blocks. Each registration (including re-registration after eviction)
        // gets a fresh flag. Only the single event processor thread touches it.
        final AtomicBoolean blockStartSeen = new AtomicBoolean(false);
        final InformedEventHandler<BlockItemBatchRingEvent> informedEventHandler =
                (event, sequence, endOfBatch, percentageBehindRingHead) -> {
                    if (evicted.get()) {
                        // Already evicted; skip remaining buffered events.
                        return;
                    }
                    if (percentageBehindRingHead > 80 && evicted.compareAndSet(false, true)) {
                        // Check lag BEFORE dispatching: if already too far behind, evict now rather
                        // than calling handleBlockItemsReceived (which might block indefinitely).
                        unregisterBlockItemHandler(handler);
                        handler.onTooFarBehindError();
                        return;
                    }
                    final BlockItems batch = event.get();
                    if (!blockStartSeen.get()) {
                        if (!batch.isStartOfNewBlock()) {
                            return;
                        }
                        blockStartSeen.set(true);
                    }
                    handler.handleBlockItemsReceived(batch);
                };
        if (blockItemDisruptor.hasStarted()) {
            registerHandler(
                    handler,
                    handlerName,
                    blockItemDisruptor.getRingBuffer(),
                    informedEventHandler,
                    blockItemHandlerToEventProcessor,
                    blockItemHandlerToThread);
        } else {
            // if the disruptor is not running, we need to add the handler to the list of pre-registered handlers
            preRegisteredBlockItemHandlers.add(
                    new PreRegisteredBlockItemHandler(handler, informedEventHandler, handlerName));
        }

        // log the registration of the handler
        LOGGER.log(
                DEBUG,
                "Registering no backpressure block item handler: {0}, handlerName: {1}",
                handler.getClass().getSimpleName(),
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
     * Hands a notification send to the message forwarder. Once {@link #stop()} has shut the forwarder down, the
     * notification is dropped rather than throwing {@link RejectedExecutionException} into the caller: plugins that are
     * stopped after this facility may still report state, e.g. a block provider finishing a write.
     *
     * @param send the send to run on the message forwarder
     */
    private void forward(final Runnable send) {
        try {
            messageForwarder.submit(send);
        } catch (final RejectedExecutionException e) {
            if (!messageForwarder.isShutdown()) {
                throw e;
            }
            LOGGER.log(DEBUG, "Messaging facility is stopped, dropping notification");
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * By default the notification goes through the message forwarder onto the block notification ring, as before the
     * verified block ring existed, and this call never waits. When the verified block ring is enabled (NOT READY FOR
     * PRODUCTION, see {@link MessagingConfig#verifiedBlockRingEnabled()}) it is published on that ring directly from the
     * calling thread, without the message forwarder; while that ring is full, this call waits. Several threads may call
     * this method at once.
     */
    @Override
    public void sendBlockVerification(final VerificationNotification notification) {
        if (verifiedBlockRingEnabled) {
            verifiedBlockDisruptor.getRingBuffer().publishEvent((event, sequence) -> event.set(notification));
            countAndLogVerificationSent(notification);
        } else {
            forward(() -> {
                blockNotificationDisruptor.getRingBuffer().publishEvent((event, sequence) -> event.set(notification));
                countAndLogVerificationSent(notification);
            });
        }
    }

    /**
     * Counts a block verification notification that was just published and logs it.
     *
     * @param notification the published notification
     */
    private void countAndLogVerificationSent(final VerificationNotification notification) {
        // metrics
        blockVerificationNotificationsCounter.increment();
        // logs
        LOGGER.log(
                DEBUG,
                "Sending block verification notification for block={0} blockSource={1} and success={2} ",
                notification.blockNumber(),
                notification.source(),
                notification.success());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void sendBlockPersisted(PersistedNotification notification) {
        forward(() -> {
            LOGGER.log(
                    DEBUG,
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
     * <p>
     * The notification is published on the unvalidated block ring directly from the calling thread, without the
     * message forwarder. While that ring is full, this call waits. Several threads may call this method at once.
     */
    @Override
    public void sendBackfilledBlockNotification(final BackfilledBlockNotification notification) {
        LOGGER.log(TRACE, "Sending backfilled block notification: block={0}", notification.blockNumber());
        unvalidatedBlockDisruptor.getRingBuffer().publishEvent((event, sequence) -> event.set(notification));
        blockBackfilledNotificationsCounter.increment();
    }

    @Override
    public void sendNewestBlockKnownToNetwork(NewestBlockKnownToNetworkNotification notification) {
        forward(() -> {
            LOGGER.log(DEBUG, "Sending NewestBlockKnownToNetwork notification: block={0}", notification.blockNumber());
            blockNotificationDisruptor.getRingBuffer().publishEvent((event, sequence) -> event.set(notification));
            newestBlockKnownToNetworkNotificationsCounter.increment();
        });
    }

    @Override
    public void sendNextExpectedPublisherBlock(final NextExpectedPublisherBlockNotification notification) {
        forward(() -> {
            LOGGER.log(DEBUG, "Sending next expected publisher block notification: {0}", notification);
            blockNotificationDisruptor.getRingBuffer().publishEvent((event, sequence) -> event.set(notification));
            nextExpectedPublisherBlockNotificationsCounter.increment();
        });
    }

    @Override
    public void sendPublisherStatusUpdate(final PublisherStatusUpdateNotification notification) {
        forward(() -> {
            LOGGER.log(DEBUG, "Sending publisher status update notification: {0}", notification);
            blockNotificationDisruptor.getRingBuffer().publishEvent((event, sequence) -> event.set(notification));
            publisherStatusUpdateNotificationsCounter.increment();
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void sendTssDataUpdate(final TssDataNotification notification) {
        forward(() -> {
            LOGGER.log(DEBUG, "Sending TSS data update notification");
            blockNotificationDisruptor.getRingBuffer().publishEvent((event, sequence) -> event.set(notification));
            tssDataUpdateNotificationsCounter.increment();
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void sendAddressBookHistoryUpdate(final AddressBookHistoryNotification notification) {
        forward(() -> {
            LOGGER.log(DEBUG, "Sending address book history update notification");
            blockNotificationDisruptor.getRingBuffer().publishEvent((event, sequence) -> event.set(notification));
            addressBookHistoryUpdateNotificationsCounter.increment();
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void sendStoredBlocksUpdate(final StoredBlocksNotification notification) {
        forward(() -> {
            LOGGER.log(DEBUG, "Sending stored blocks update notification");
            blockNotificationDisruptor.getRingBuffer().publishEvent((event, sequence) -> event.set(notification));
            storedBlocksUpdateNotificationsCounter.increment();
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void sendAvailableBlocksUpdate(final AvailableBlocksNotification notification) {
        forward(() -> {
            LOGGER.log(DEBUG, "Sending available blocks update notification");
            blockNotificationDisruptor.getRingBuffer().publishEvent((event, sequence) -> event.set(notification));
            availableBlocksUpdateNotificationsCounter.increment();
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void registerApplicationStateNotificationHandler(
            final ApplicationStateNotificationHandler handler,
            final boolean cpuIntensiveHandler,
            final String handlerName) {
        final InformedEventHandler<BlockNotificationRingEvent> informedEventHandler =
                (event, sequence, endOfBatch, percentageBehindRingHead) -> {
                    if (event.getTssDataNotification() != null) {
                        handler.handleTssDataUpdate(event.getTssDataNotification());
                    } else if (event.getAddressBookHistoryNotification() != null) {
                        handler.handleAddressBookHistoryUpdate(event.getAddressBookHistoryNotification());
                    } else if (event.getStoredBlocksNotification() != null) {
                        handler.handleStoredBlocksUpdate(event.getStoredBlocksNotification());
                    } else if (event.getAvailableBlocksNotification() != null) {
                        handler.handleAvailableBlocksUpdate(event.getAvailableBlocksNotification());
                    }
                    // Block-stream notification types on the same ring buffer are silently ignored.
                };
        if (blockNotificationDisruptor.hasStarted()) {
            registerHandler(
                    handler,
                    handlerName,
                    blockNotificationDisruptor.getRingBuffer(),
                    informedEventHandler,
                    applicationStateNotificationHandlerToEventProcessor,
                    applicationStateNotificationHandlerToThread);
        } else {
            preRegisteredApplicationStateNotificationHandlers.add(
                    new PreRegisteredApplicationStateNotificationHandler(handler, informedEventHandler, handlerName));
        }
        LOGGER.log(
                DEBUG,
                "Registering application state notification handler: {0}, handlerName: {1}",
                handler.getClass().getSimpleName(),
                handlerName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void unregisterApplicationStateNotificationHandler(
            final ApplicationStateNotificationHandler handler) {
        unregisterHandler(
                handler,
                blockNotificationDisruptor.getRingBuffer(),
                applicationStateNotificationHandlerToEventProcessor,
                applicationStateNotificationHandlerToThread);
    }

    /**
     * {@inheritDoc}
     * <p>
     * The handler receives no block verification notifications unless it is registered with
     * {@link #registerVerifiedBlockHandler} too. A handler that is also a verified block handler while the verified
     * block ring is disabled keeps the single registration it already has on the block notification ring.
     */
    @Override
    public synchronized void registerBlockNotificationHandler(
            final BlockNotificationHandler handler, final boolean cpuIntensiveHandler, final String handlerName) {
        addNotificationRingRegistration(handler, NotificationRingRegistration.BLOCK_NOTIFICATIONS, handlerName);
    }

    /**
     * {@inheritDoc}
     * <p>
     * A handler that is also a verified block handler while the verified block ring is disabled stops receiving block
     * notifications but stays on the block notification ring, for its verification results, until it is unregistered
     * as a verified block handler too.
     */
    @Override
    public synchronized void unregisterBlockNotificationHandler(final BlockNotificationHandler handler) {
        removeNotificationRingRegistration(handler, NotificationRingRegistration.BLOCK_NOTIFICATIONS);
    }

    /**
     * Records one more way a handler is registered on the block notification ring, and attaches the handler to the ring
     * if this is its first registration there. A handler registered several ways keeps a single registration, and so
     * a single thread, on the ring.
     *
     * @param handler the handler to register
     * @param registration the way the handler is registered
     * @param handlerName the name of the handler, used for thread name and logging
     */
    private void addNotificationRingRegistration(
            final BlockNotificationHandler handler,
            final NotificationRingRegistration registration,
            final String handlerName) {
        final Set<NotificationRingRegistration> current = notificationRingRegistrations.getOrDefault(handler, Set.of());
        final EnumSet<NotificationRingRegistration> updated = EnumSet.of(registration);
        updated.addAll(current);
        notificationRingRegistrations.put(handler, Set.copyOf(updated));
        if (current.isEmpty()) {
            attachToNotificationRing(handler, handlerName);
        } else {
            LOGGER.log(
                    DEBUG,
                    "Block notification handler {0} is already on the block notification ring, now registered as {1}",
                    handler.getClass().getSimpleName(),
                    updated);
        }
    }

    /**
     * Removes one way a handler is registered on the block notification ring, and detaches the handler from the ring
     * once it is no longer registered any way.
     *
     * @param handler the handler to unregister
     * @param registration the way the handler is no longer registered
     */
    private void removeNotificationRingRegistration(
            final BlockNotificationHandler handler, final NotificationRingRegistration registration) {
        final Set<NotificationRingRegistration> current = notificationRingRegistrations.get(handler);
        if (current == null) {
            LOGGER.log(
                    DEBUG,
                    "Block notification handler {0} is not on the block notification ring",
                    handler.getClass().getSimpleName());
        } else {
            final EnumSet<NotificationRingRegistration> remaining = EnumSet.noneOf(NotificationRingRegistration.class);
            remaining.addAll(current);
            remaining.remove(registration);
            if (remaining.isEmpty()) {
                notificationRingRegistrations.remove(handler);
                preRegisteredBlockNotificationHandlers.removeIf(preRegistered -> preRegistered.handler() == handler);
                unregisterHandler(
                        handler,
                        blockNotificationDisruptor.getRingBuffer(),
                        blockNotificationHandlerToEventProcessor,
                        blockNotificationHandlerToThread);
            } else {
                notificationRingRegistrations.put(handler, Set.copyOf(remaining));
                LOGGER.log(
                        DEBUG,
                        "Block notification handler {0} stays on the block notification ring, still registered as {1}",
                        handler.getClass().getSimpleName(),
                        remaining);
            }
        }
    }

    /**
     * Whether a handler on the block notification ring is registered for the notification an event carries. A block
     * verification notification only reaches handlers registered with {@link #registerVerifiedBlockHandler}; every
     * other block notification only reaches handlers registered with {@link #registerBlockNotificationHandler}.
     *
     * @param handler the handler on the block notification ring
     * @param event the event about to be delivered to the handler
     * @return true if the handler is registered for the notification of the event
     */
    private boolean isRegisteredFor(final BlockNotificationHandler handler, final BlockNotificationRingEvent event) {
        final NotificationRingRegistration requiredRegistration;
        if (event.getVerificationNotification() != null) {
            requiredRegistration = NotificationRingRegistration.VERIFIED_BLOCKS;
        } else {
            requiredRegistration = NotificationRingRegistration.BLOCK_NOTIFICATIONS;
        }
        return notificationRingRegistrations.getOrDefault(handler, Set.of()).contains(requiredRegistration);
    }

    /**
     * Attaches a handler to the block notification ring. The handler receives, on one thread and in the order the
     * notifications were sent, the notifications it is registered for: the block verification notifications if it is a
     * verified block handler while the verified block ring is disabled, and the other block notifications if it is a
     * block notification handler.
     *
     * @param handler the handler to attach
     * @param handlerName the name of the handler, used for thread name and logging
     */
    private void attachToNotificationRing(final BlockNotificationHandler handler, final String handlerName) {
        final InformedEventHandler<BlockNotificationRingEvent> informedEventHandler =
                (event, sequence, endOfBatch, percentageBehindRingHead) -> {
                    // send on the event, only if the handler is registered for it
                    if (isRegisteredFor(handler, event)) {
                        deliverBlockNotification(handler, event);
                    }
                };
        if (blockNotificationDisruptor.hasStarted()) {
            // if the disruptor is already running, we need to register the handler with the disruptor
            registerHandler(
                    handler,
                    handlerName,
                    blockNotificationDisruptor.getRingBuffer(),
                    informedEventHandler,
                    blockNotificationHandlerToEventProcessor,
                    blockNotificationHandlerToThread);
        } else {
            // if the disruptor is not running, we need to add the handler to the list of pre-registered handlers
            preRegisteredBlockNotificationHandlers.add(
                    new PreRegisteredBlockNotificationHandler(handler, informedEventHandler, handlerName));
        }

        // log the registration of the handler
        LOGGER.log(
                DEBUG,
                "Registering block notification handler: {0}, handlerName: {1}",
                handler.getClass().getSimpleName(),
                handlerName);
    }

    /**
     * Delivers the notification an event of the block notification ring carries to a handler registered for it.
     *
     * @param handler the handler to deliver to
     * @param event the event carrying the notification
     */
    private static void deliverBlockNotification(
            final BlockNotificationHandler handler, final BlockNotificationRingEvent event) {
        if (event.getVerificationNotification() != null) {
            handler.handleVerification(event.getVerificationNotification());
        } else if (event.getPersistedNotification() != null) {
            handler.handlePersisted(event.getPersistedNotification());
        } else if (event.getNewestBlockKnownToNetworkNotification() != null) {
            handler.handleNewestBlockKnownToNetwork(event.getNewestBlockKnownToNetworkNotification());
        } else if (event.getNextExpectedPublisherBlockNotification() != null) {
            handler.handleNextExpectedPublisherBlock(event.getNextExpectedPublisherBlockNotification());
        } else if (event.getPublisherStatusUpdateNotification() != null) {
            handler.handlePublisherStatusUpdate(event.getPublisherStatusUpdateNotification());
        } else if (event.getTssDataNotification() == null
                && event.getAddressBookHistoryNotification() == null
                && event.getStoredBlocksNotification() == null
                && event.getAvailableBlocksNotification() == null) {
            // Context notification types are silently ignored; only log when truly empty.
            LOGGER.log(Level.INFO, "Received an event with no notification set");
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * The handler gets a thread of its own for the unvalidated block ring, separate from the thread that delivers its
     * block notifications if it is also registered as a block notification handler.
     */
    @Override
    public synchronized void registerUnvalidatedBlockHandler(
            final BlockNotificationHandler handler, final boolean cpuIntensiveHandler, final String handlerName) {
        final InformedEventHandler<UnvalidatedBlockRingEvent> informedEventHandler =
                (event, sequence, endOfBatch, percentageBehindRingHead) -> handler.handleBackfilled(event.get());
        final String unvalidatedBlockHandlerName = "UnvalidatedBlocks:" + handlerName;
        if (unvalidatedBlockDisruptor.hasStarted()) {
            registerHandler(
                    handler,
                    unvalidatedBlockHandlerName,
                    unvalidatedBlockDisruptor.getRingBuffer(),
                    informedEventHandler,
                    unvalidatedBlockHandlerToEventProcessor,
                    unvalidatedBlockHandlerToThread);
        } else {
            preRegisteredUnvalidatedBlockHandlers.add(new PreRegisteredUnvalidatedBlockHandler(
                    handler, informedEventHandler, unvalidatedBlockHandlerName));
        }

        // log the registration of the handler
        LOGGER.log(
                DEBUG,
                "Registering unvalidated block handler: {0}, handlerName: {1}",
                handler.getClass().getSimpleName(),
                handlerName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void unregisterUnvalidatedBlockHandler(final BlockNotificationHandler handler) {
        unregisterHandler(
                handler,
                unvalidatedBlockDisruptor.getRingBuffer(),
                unvalidatedBlockHandlerToEventProcessor,
                unvalidatedBlockHandlerToThread);
    }

    /**
     * {@inheritDoc}
     * <p>
     * By default, with the verified block ring disabled, the handler is registered on the block notification ring and
     * receives the verification results there, on the same thread as its other block notifications if it is also a
     * block notification handler; a handler that is already registered there as a block notification handler keeps
     * that single registration. When the verified block ring is enabled (NOT READY FOR PRODUCTION, see
     * {@link MessagingConfig#verifiedBlockRingEnabled()}) the handler gets a thread of its own for the verified block
     * ring, separate from the thread that delivers its block notifications if it is also registered as a block
     * notification handler.
     */
    @Override
    public synchronized void registerVerifiedBlockHandler(
            final BlockNotificationHandler handler, final boolean cpuIntensiveHandler, final String handlerName) {
        if (verifiedBlockRingEnabled) {
            attachToVerifiedBlockRing(handler, handlerName);
        } else {
            addNotificationRingRegistration(handler, NotificationRingRegistration.VERIFIED_BLOCKS, handlerName);
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * By default, with the verified block ring disabled, a handler that is also registered as a block notification
     * handler stops receiving verification results but stays on the block notification ring, for its other block
     * notifications, until it is unregistered that way too.
     */
    @Override
    public synchronized void unregisterVerifiedBlockHandler(final BlockNotificationHandler handler) {
        if (verifiedBlockRingEnabled) {
            unregisterHandler(
                    handler,
                    verifiedBlockDisruptor.getRingBuffer(),
                    verifiedBlockHandlerToEventProcessor,
                    verifiedBlockHandlerToThread);
        } else {
            removeNotificationRingRegistration(handler, NotificationRingRegistration.VERIFIED_BLOCKS);
        }
    }

    /**
     * Attaches a handler to the verified block ring, used only when that ring is enabled. The handler gets a thread of
     * its own for the ring.
     *
     * @param handler the handler to attach
     * @param handlerName the name of the handler, used for thread name and logging
     */
    private void attachToVerifiedBlockRing(final BlockNotificationHandler handler, final String handlerName) {
        final InformedEventHandler<VerifiedBlockRingEvent> informedEventHandler =
                (event, sequence, endOfBatch, percentageBehindRingHead) -> handler.handleVerification(event.get());
        final String verifiedBlockHandlerName = "VerifiedBlocks:" + handlerName;
        if (verifiedBlockDisruptor.hasStarted()) {
            registerHandler(
                    handler,
                    verifiedBlockHandlerName,
                    verifiedBlockDisruptor.getRingBuffer(),
                    informedEventHandler,
                    verifiedBlockHandlerToEventProcessor,
                    verifiedBlockHandlerToThread);
        } else {
            preRegisteredVerifiedBlockHandlers.add(
                    new PreRegisteredVerifiedBlockHandler(handler, informedEventHandler, verifiedBlockHandlerName));
        }

        // log the registration of the handler
        LOGGER.log(
                DEBUG,
                "Registering verified block handler: {0}, handlerName: {1}",
                handler.getClass().getSimpleName(),
                handlerName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void start() {
        // start the disruptors
        blockItemDisruptor.start();
        blockNotificationDisruptor.start();
        unvalidatedBlockDisruptor.start();
        verifiedBlockDisruptor.start();
        // register all the pre-registered block item handlers
        for (var preRegisteredHandler : preRegisteredBlockItemHandlers) {
            registerHandler(
                    preRegisteredHandler.handler(),
                    preRegisteredHandler.handlerName(),
                    blockItemDisruptor.getRingBuffer(),
                    preRegisteredHandler.informedHandler(),
                    blockItemHandlerToEventProcessor,
                    blockItemHandlerToThread);
        }
        preRegisteredBlockItemHandlers.clear();

        // register all the pre-registered block notification handlers
        for (var preRegisteredHandler : preRegisteredBlockNotificationHandlers) {
            registerHandler(
                    preRegisteredHandler.handler(),
                    preRegisteredHandler.handlerName(),
                    blockNotificationDisruptor.getRingBuffer(),
                    preRegisteredHandler.informedHandler(),
                    blockNotificationHandlerToEventProcessor,
                    blockNotificationHandlerToThread);
        }
        preRegisteredBlockNotificationHandlers.clear();

        // register all the pre-registered application state notification handlers
        for (var preRegisteredHandler : preRegisteredApplicationStateNotificationHandlers) {
            registerHandler(
                    preRegisteredHandler.handler(),
                    preRegisteredHandler.handlerName(),
                    blockNotificationDisruptor.getRingBuffer(),
                    preRegisteredHandler.informedHandler(),
                    applicationStateNotificationHandlerToEventProcessor,
                    applicationStateNotificationHandlerToThread);
        }
        preRegisteredApplicationStateNotificationHandlers.clear();

        // register all the pre-registered handlers with the unvalidated block ring
        for (var preRegisteredHandler : preRegisteredUnvalidatedBlockHandlers) {
            registerHandler(
                    preRegisteredHandler.handler(),
                    preRegisteredHandler.handlerName(),
                    unvalidatedBlockDisruptor.getRingBuffer(),
                    preRegisteredHandler.informedHandler(),
                    unvalidatedBlockHandlerToEventProcessor,
                    unvalidatedBlockHandlerToThread);
        }
        preRegisteredUnvalidatedBlockHandlers.clear();

        // register all the pre-registered handlers with the verified block ring
        for (var preRegisteredHandler : preRegisteredVerifiedBlockHandlers) {
            registerHandler(
                    preRegisteredHandler.handler(),
                    preRegisteredHandler.handlerName(),
                    verifiedBlockDisruptor.getRingBuffer(),
                    preRegisteredHandler.informedHandler(),
                    verifiedBlockHandlerToEventProcessor,
                    verifiedBlockHandlerToThread);
        }
        preRegisteredVerifiedBlockHandlers.clear();

        // log successful start
        if (LOGGER.isLoggable(DEBUG)) {
            LOGGER.log(
                    DEBUG,
                    "BlockMessagingFacility successfully started with block item queue size: {0}, block notification"
                            + " queue size: {1}, unvalidated block queue size: {2} and verified block queue size: {3}",
                    blockItemDisruptor.getRingBuffer().getBufferSize(),
                    blockNotificationDisruptor.getRingBuffer().getBufferSize(),
                    unvalidatedBlockDisruptor.getRingBuffer().getBufferSize(),
                    verifiedBlockDisruptor.getRingBuffer().getBufferSize());
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
        blockItemHandlerToEventProcessor.clear();
        // Stop all the block item handler threads
        for (Thread thread : blockItemHandlerToThread.values()) {
            thread.interrupt();
            // log the stopping of the thread
            LOGGER.log(DEBUG, "Stopped block item handler thread: {0}", thread.getName());
        }
        blockItemHandlerToThread.clear();
        // Stop all the block notification event handlers
        for (var eventHandler : blockNotificationHandlerToEventProcessor.values()) {
            blockNotificationDisruptor.getRingBuffer().removeGatingSequence(eventHandler.getSequence());
            eventHandler.halt();
        }
        blockNotificationHandlerToEventProcessor.clear();
        // Stop all the context notification event handlers
        for (var eventHandler : applicationStateNotificationHandlerToEventProcessor.values()) {
            blockNotificationDisruptor.getRingBuffer().removeGatingSequence(eventHandler.getSequence());
            eventHandler.halt();
        }
        applicationStateNotificationHandlerToEventProcessor.clear();
        // Stop all the unvalidated block event handlers, so a sender waiting on a full ring is released
        for (var eventHandler : unvalidatedBlockHandlerToEventProcessor.values()) {
            unvalidatedBlockDisruptor.getRingBuffer().removeGatingSequence(eventHandler.getSequence());
            eventHandler.halt();
        }
        unvalidatedBlockHandlerToEventProcessor.clear();
        // Stop all the verified block event handlers, so a sender waiting on a full ring is released
        for (var eventHandler : verifiedBlockHandlerToEventProcessor.values()) {
            verifiedBlockDisruptor.getRingBuffer().removeGatingSequence(eventHandler.getSequence());
            eventHandler.halt();
        }
        verifiedBlockHandlerToEventProcessor.clear();
        // Shuts down all the threads handling events.
        blockItemDisruptor.shutdown();
        blockNotificationDisruptor.shutdown();
        unvalidatedBlockDisruptor.shutdown();
        verifiedBlockDisruptor.shutdown();
        // Stop all the block notification handlers
        for (Thread thread : blockNotificationHandlerToThread.values()) {
            thread.interrupt();
            // log the stopping of the thread
            LOGGER.log(DEBUG, "Stopped block notification handler thread: {0}", thread.getName());
        }
        blockNotificationHandlerToThread.clear();
        // Stop all the application state notification handler threads
        for (Thread thread : applicationStateNotificationHandlerToThread.values()) {
            thread.interrupt();
            LOGGER.log(DEBUG, "Stopped application state notification handler thread: {0}", thread.getName());
        }
        applicationStateNotificationHandlerToThread.clear();
        // Stop all the unvalidated block handler threads
        for (Thread thread : unvalidatedBlockHandlerToThread.values()) {
            thread.interrupt();
            LOGGER.log(DEBUG, "Stopped unvalidated block handler thread: {0}", thread.getName());
        }
        unvalidatedBlockHandlerToThread.clear();
        // Stop all the verified block handler threads
        for (Thread thread : verifiedBlockHandlerToThread.values()) {
            thread.interrupt();
            LOGGER.log(DEBUG, "Stopped verified block handler thread: {0}", thread.getName());
        }
        verifiedBlockHandlerToThread.clear();
        messageForwarder.shutdown();
    }

    /**
     * Registers a handler with the ring buffer. This generic method allows all the logic to be common and hence any bug
     * hopefully only need fixing once. Any improvements can be made in one place.
     *
     * @param <H> the type of the GatingHandler
     * @param <E> the type of the event
     * @param handler the handler to register
     * @param handlerName the name of the handler, used for thread name and logging
     * @param ringBuffer the ring buffer to register with
     * @param informedEventHandler the event handler to call when an event is published
     * @param handlerToEventProcessor the map of handlers to event processors
     * @param handlerToThread the map of handlers to threads
     */
    private <H extends GatingHandler, E> void registerHandler(
            final H handler,
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
        final String name = handlerName == null ? "Unknown" : handlerName;
        // Without this the processor rethrows, its thread exits and a gating sequence stops the ring
        batchEventProcessor.setExceptionHandler(new LoggingExceptionHandler<>(name, barrier, handlerFailuresCounter));
        // Dynamically add sequences to the ring buffer
        if (handler.isGating()) {
            ringBuffer.addGatingSequences(batchEventProcessor.getSequence());
        } else {
            // Non-gating (no-backpressure) handlers must start from the current cursor so they
            // only receive future events. The default starting sequence is -1, which causes the
            // processor to replay every past event still in the ring buffer — for a backfill
            // subscriber this fills liveBlockQueue and bypasses the historical read path.
            batchEventProcessor.getSequence().set(ringBuffer.getCursor());
        }
        final Thread handlerThread = PLATFORM_THREAD_FACTORY.newThread(batchEventProcessor);
        handlerThread.setName("MessageHandler:" + name);
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
     * @param <H> the type of the GatingHandler
     * @param <E> the type of the event
     * @param handler the handler to unregister
     * @param ringBuffer the ring buffer to unregister from
     * @param handlerToEventProcessor the map of handlers to event processors
     * @param handlerToThread the map of handlers to threads
     */
    private static <H extends GatingHandler, E> void unregisterHandler(
            final H handler,
            final RingBuffer<E> ringBuffer,
            final Map<H, BatchEventProcessor<E>> handlerToEventProcessor,
            final Map<H, Thread> handlerToThread) {
        final Thread handlerThread = handlerToThread.remove(handler);
        final BatchEventProcessor<E> eventProcessor = handlerToEventProcessor.remove(handler);
        if (eventProcessor != null) {
            if (handler.isGating()) {
                ringBuffer.removeGatingSequence(eventProcessor.getSequence());
            }
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
     * Exception handler for the event processor of a single message handler. It logs the failure and lets the
     * processor move on to the next message, so one bad message does not stop the handler or stall the ring.
     * Unregistering a handler or stopping the facility halts the processor and then interrupts the handler thread, so
     * a failure caused by that interrupt after the halt is expected and logged at DEBUG. Every other failure is logged
     * at ERROR and counted.
     *
     * @param <E> the type of the event
     */
    private static final class LoggingExceptionHandler<E> implements ExceptionHandler<E> {
        /** The name of the message handler, used for logging */
        private final String handlerName;
        /** The barrier of the event processor, alerted when the processor is halted */
        private final SequenceBarrier barrier;
        /** Counter for messages skipped because a message handler failed on them */
        private final LongCounter.Measurement handlerFailuresCounter;

        /**
         * Create a new exception handler for the named message handler.
         *
         * @param handlerName the name of the message handler
         * @param barrier the barrier of the event processor, alerted when the processor is halted
         * @param handlerFailuresCounter counter for messages skipped because the handler failed on them
         */
        private LoggingExceptionHandler(
                final String handlerName,
                final SequenceBarrier barrier,
                final LongCounter.Measurement handlerFailuresCounter) {
            this.handlerName = handlerName;
            this.barrier = barrier;
            this.handlerFailuresCounter = handlerFailuresCounter;
        }

        @Override
        public void handleEventException(final Throwable throwable, final long sequence, final E event) {
            if (isStopInterrupt(throwable)) {
                LOGGER.log(
                        DEBUG,
                        () -> "Message handler %s interrupted while stopping on sequence %d, skipping the message"
                                .formatted(handlerName, sequence),
                        throwable);
            } else {
                handlerFailuresCounter.increment();
                LOGGER.log(
                        ERROR,
                        () -> "Message handler %s failed on sequence %d, skipping the message"
                                .formatted(handlerName, sequence),
                        throwable);
            }
        }

        /**
         * Whether the failure comes from the interrupt sent by unregistering the handler or stopping the facility. Both
         * halt the processor, which alerts its barrier, before interrupting the thread, so an interrupt without the
         * alert has some other source and is a real failure.
         *
         * @param throwable the failure thrown by the message handler
         * @return true if the processor was halted and the failure was caused by an interrupt
         */
        private boolean isStopInterrupt(final Throwable throwable) {
            return barrier.isAlerted() && (Thread.currentThread().isInterrupted() || isCausedByInterrupt(throwable));
        }

        /**
         * Whether the throwable, or any of its causes, comes from an interrupt of the handler thread. A handler that
         * catches the interrupt and rethrows clears the thread's interrupt flag, so only the cause chain shows it.
         *
         * @param throwable the failure thrown by the message handler
         * @return true if the failure was caused by an interrupt
         */
        private static boolean isCausedByInterrupt(final Throwable throwable) {
            for (Throwable cause = throwable; cause != null; cause = cause.getCause()) {
                if (cause instanceof InterruptedException
                        || cause instanceof InterruptedIOException
                        || cause instanceof ClosedByInterruptException) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public void handleOnStartException(final Throwable throwable) {
            LOGGER.log(ERROR, () -> "Message handler %s failed on start".formatted(handlerName), throwable);
        }

        @Override
        public void handleOnShutdownException(final Throwable throwable) {
            LOGGER.log(ERROR, () -> "Message handler %s failed on shutdown".formatted(handlerName), throwable);
        }
    }

    /**
     * Record for pre-registered block item handlers.
     *
     * @param handler the block item handler
     * @param informedHandler the event handler to call when an event is published
     * @param handlerName the name of the handler, used for thread name and logging
     */
    private record PreRegisteredBlockItemHandler(
            BlockItemHandler handler,
            InformedEventHandler<BlockItemBatchRingEvent> informedHandler,
            String handlerName) {}

    /**
     * Record for pre-registered block notification handlers.
     *
     * @param handler the block notification handler
     * @param informedHandler the informed event handler
     * @param handlerName the name of the handler, used for thread name and logging
     */
    private record PreRegisteredBlockNotificationHandler(
            BlockNotificationHandler handler,
            InformedEventHandler<BlockNotificationRingEvent> informedHandler,
            String handlerName) {}

    /**
     * Record for pre-registered application state notification handlers.
     *
     * @param handler the application state notification handler
     * @param informedHandler the informed event handler
     * @param handlerName the name of the handler, used for thread name and logging
     */
    private record PreRegisteredApplicationStateNotificationHandler(
            ApplicationStateNotificationHandler handler,
            InformedEventHandler<BlockNotificationRingEvent> informedHandler,
            String handlerName) {}

    /**
     * Record for pre-registered unvalidated block handlers.
     *
     * @param handler the unvalidated block handler
     * @param informedHandler the informed event handler
     * @param handlerName the name of the handler, used for thread name and logging
     */
    private record PreRegisteredUnvalidatedBlockHandler(
            BlockNotificationHandler handler,
            InformedEventHandler<UnvalidatedBlockRingEvent> informedHandler,
            String handlerName) {}

    /**
     * Record for pre-registered verified block handlers.
     *
     * @param handler the verified block handler
     * @param informedHandler the informed event handler
     * @param handlerName the name of the handler, used for thread name and logging
     */
    private record PreRegisteredVerifiedBlockHandler(
            BlockNotificationHandler handler,
            InformedEventHandler<VerifiedBlockRingEvent> informedHandler,
            String handlerName) {}

    /** The ways a handler can be registered on the block notification ring, each with what it receives there. */
    private enum NotificationRingRegistration {
        /**
         * Registered with {@link BlockMessagingFacilityImpl#registerBlockNotificationHandler}: receives every block
         * notification except the block verification notifications.
         */
        BLOCK_NOTIFICATIONS,
        /**
         * Registered with {@link BlockMessagingFacilityImpl#registerVerifiedBlockHandler} while the verified block ring
         * is disabled: receives the block verification notifications.
         */
        VERIFIED_BLOCKS
    }
}
