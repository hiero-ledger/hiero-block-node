// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.publisher;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.WARNING;

import com.hedera.pbj.runtime.grpc.GrpcException;
import com.hedera.pbj.runtime.grpc.Pipeline;
import com.hedera.pbj.runtime.grpc.Pipelines;
import com.hedera.pbj.runtime.grpc.ServiceInterface;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.metrics.api.Counter;
import com.swirlds.metrics.api.LongGauge;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;
import org.hiero.block.node.publisher.PublisherConfig.PublisherType;
import org.hiero.block.node.publisher.UpdateCallback.UpdateType;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.block.node.spi.ServiceBuilder;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.BlockNotification;
import org.hiero.block.node.spi.blockmessaging.BlockNotificationHandler;
import org.hiero.hapi.block.node.BlockItemUnparsed;
import org.hiero.hapi.block.node.PublishStreamRequestUnparsed;
import org.hiero.hapi.block.node.PublishStreamResponse;

/**
 * Provides implementation for the block stream publisher endpoints of the server. These handle incoming push block
 * streams from the consensus node.
 * <p>
 * In concept what this plugin is trying to do is not complicated. It aims to accept incoming push GRPC streams of block
 * item batches from one or consensus nodes. Consolidate them into a single consistent stream of blocks with increasing
 * block numbers with none missing, duplicated or partial. Optimizing for the lowest latency on a block by block basis
 * by always selecting the consensus node that is first to start sending that block. It then sends that stream out to
 * the rest of the system via the block messaging service. Also, it needs to handle sending confirmation messages back
 * to the consensus nodes when a block have been verified and persisted to disk. If verification fails, it needs to
 * revert to before the bad block and send a message back to the consensus nodes to request a resend of the block.
 * <h2>Threading</h2>
 * Threading is a challenge here as we have calls in from a number of threads. GRPC data arrives to sessions on web
 * server handler threads. The block notification handler is called from the block verification thread. Because we
 * have lots of coordinated state to manage, we need to be careful about thread safety. To solve that we create a single
 * lock all interactions with the state of this plugin. So everytime we are called from a different thread, we acquire
 * that lock before doing anything that interacts with state. We also considered an event thread style model and decided
 * an extra thread would be overkill, so save that thread for somewhere we need it more. The same lock is used for the
 * whole plugin, both PublisherServicePlugin and BlockStreamProducerSession.
 * <p>
 * TODO Still lots to work out on tracking the various stages of blocks, latest in flight, etc.
 */
public final class PublisherServicePlugin implements BlockNodePlugin, ServiceInterface, BlockNotificationHandler {
    /** The logger for this class. */
    private final System.Logger LOGGER = System.getLogger(getClass().getName());

    /** Single lock for gating access to state changes. */
    private final ReentrantLock stateLock = new ReentrantLock();

    // all these fields are as if they are final, but we need to set them in the init method

    /** The block node context, for access to core facilities. */
    private BlockNodeContext context;
    /** The configuration for the publisher */
    private PublisherConfig publisherConfig;
    /** The timeout from config for receiving a block in nanos */
    private long timeOutNanos;
    /** The number of live block items received from a producer. */
    private Counter liveBlockItemsReceived;
    /** The number of live block items messaged to the messaging service. */
    private Counter liveBlockItemsMessaged;
    /** The lowest incoming block number. */
    private LongGauge lowestBlockNumberInbound;
    /** The latest incoming block number. */
    private LongGauge currentBlockNumberInbound;
    /** The highest incoming block number. */
    private LongGauge highestIncomingBlockNumber;
    /** The number of producers publishing block items. */
    private LongGauge numberOfProducers;

    // state fields always updated under the state lock

    /** Set of all open sessions. */
    private final Set<BlockStreamProducerSession> openSessions = new HashSet<>();
    /** The current block number being processed. */
    private long currentBlockNumber = UNKNOWN_BLOCK_NUMBER;
    /** The current ACKED block number */
    private long latestAckedBlockNumber = UNKNOWN_BLOCK_NUMBER;
    /** The current chosen primary consensus node session, or null if there is no primary */
    private BlockStreamProducerSession currentPrimarySession = null;
    /** The next session id to use when a new session is created */
    private long nextSessionId = 0;

    /**
     * Handles session state updates and manages the selection of primary sessions for block processing.
     * This method is called whenever a session's state changes and is responsible for coordinating
     * block processing across multiple sessions.
     * <p>
     * The method performs the following key operations:
     * <ul>
     * <li>Updates metrics for block numbers across all sessions
     * <li>Handles block completion by incrementing current block number when primary session ends a block
     * <li>Manages primary session selection:
     * <ul>
     * <li>Validates current primary session for timeouts and correct block numbers
     * <li>Selects new primary session based on earliest start time and valid block numbers
     * <li>Switches other sessions to BEHIND state
     * </ul>
     * <li>Handles error cases:
     * <ul>
     * <li>Requests block resend when primary session is invalid
     * <li>Updates block number if all sessions are ahead
     * <li>Logs warnings for timeout and incorrect block number scenarios
     * </ul>
     * </ul>
     *
     * @param session the session that triggered the update
     * @param updateType the type of update (START_BLOCK, END_BLOCK, WHOLE_BLOCK, etc.)
     * @param blockNumber the block number associated with the update
     */
    private void onSessionUpdate(BlockStreamProducerSession session, UpdateType updateType, long blockNumber) {
        LOGGER.log(DEBUG, "onSessionUpdate: type={0} blockNumber={1} session={2}", updateType, blockNumber, session);
        stateLock.lock();
        try {
            // ==== Update Metrics =====================================================================
            final LongSummaryStatistics blockNumbersStats = openSessions.stream()
                    .mapToLong(BlockStreamProducerSession::currentBlockNumber)
                    .summaryStatistics();
            lowestBlockNumberInbound.set(blockNumbersStats.getMin());
            highestIncomingBlockNumber.set(blockNumbersStats.getMax());

            // ==== Clean Up ===========================================================================

            openSessions.removeIf(
                    openSession -> openSession.currentBlockState() == BlockStreamProducerSession.BlockState.DISCONNECTED
                            && openSession.sessionId() != session.sessionId());
            numberOfProducers.set(openSessions.size());

            // ==== Pre-checks handle ===================================================================

            if (session == null) {
                return;
            }

            if (currentBlockNumber != UNKNOWN_BLOCK_NUMBER && latestAckedBlockNumber != UNKNOWN_BLOCK_NUMBER) {
                // Duplicate Pre-check, even before acquiring the lock
                if (blockNumber <= latestAckedBlockNumber) {
                    session.sendDuplicateAck(latestAckedBlockNumber);
                    return;
                }

                // Ahead Pre-Check, similar to above,
                // but there is how many blocks ahead we can keep in buffer?
                // TODO, this offset should be calculated using a config value, or hard-code on a specific number but
                // keep
                // as
                // constant.
                long offset = 3; // this should be 1 + BufferCapacity=2.
                if (blockNumber > currentBlockNumber + offset) {
                    session.sendStreamItemsBehind(latestAckedBlockNumber);
                    return;
                }
            }

            // ==== Active Primary Session Handle =======================================================
            if (currentPrimarySession != null) {

                // if update type is END_BLOCK and from primary session, we need to update the current block number, so
                // that we start looking for next block
                final boolean isCurrentPrimarySessionEnded = updateType == UpdateType.END_BLOCK
                        && session.currentBlockState() == BlockStreamProducerSession.BlockState.PRIMARY;

                if (isCurrentPrimarySessionEnded) {
                    currentBlockNumber = currentBlockNumber + 1;
                    currentPrimarySession = null;
                    return;
                }

                // check if current primary session has timed out
                final boolean currentPrimaryHasTimedOut =
                        (System.nanoTime() - currentPrimarySession.startTimeOfCurrentBlock()) > timeOutNanos;

                // we do not have a good primary session, aka not providing correct block number
                if (currentPrimaryHasTimedOut) {
                    LOGGER.log(
                            WARNING,
                            "onSessionUpdate: currentPrimaryHasTimedOut, primarySession={1}",
                            currentPrimarySession);
                } else {
                    // this is odd, we have a primary session, but it is not the current block number
                    LOGGER.log(
                            WARNING,
                            "onSessionUpdate: currentPrimarySession is not providing correct block number, "
                                    + "currentBlockNumber={0} primarySession={1}",
                            currentBlockNumber,
                            currentPrimarySession);
                }
                currentPrimarySession = null;
                // Seems like all we can do here is request a resend of the block
                openSessions.forEach(openSession -> openSession.requestResend(currentBlockNumber));

                return;
            }

            // ==== Inactive Primary Session Handle =====================================================

            // try and pick a new primary session if there is one
            // first lets see if there are not any sessions that have a valid block they are working on
            if (openSessions.stream().noneMatch(openSession -> openSession.currentBlockNumber() >= 0)) {
                return;
            }

            // then lets see if there are any sessions that can provide the current block number
            boolean haveSessionForCurrentBlockNumber = openSessions.stream()
                    .anyMatch(
                            openSession -> openSession.currentBlockState() == BlockStreamProducerSession.BlockState.NEW
                                    && openSession.currentBlockNumber() == currentBlockNumber);
            // if we don't then let's pick the lowest block number among all sessions that is greater than our
            // current block as our new current block
            if (!haveSessionForCurrentBlockNumber) {
                final OptionalLong newCurrentBlockNumber = openSessions.stream()
                        .mapToLong(BlockStreamProducerSession::currentBlockNumber)
                        .filter(blockNumber1 -> blockNumber1 > currentBlockNumber)
                        .min();
                if (newCurrentBlockNumber.isPresent()) {
                    LOGGER.log(
                            INFO,
                            "onSessionUpdate: currentBlockNumber updated from {0} to {1} from sessions, "
                                    + "availableBlocks={2}",
                            currentBlockNumber,
                            newCurrentBlockNumber.getAsLong(),
                            openSessions.stream()
                                    .mapToLong(BlockStreamProducerSession::currentBlockNumber)
                                    .summaryStatistics());
                    currentBlockNumber = newCurrentBlockNumber.getAsLong();
                    currentBlockNumberInbound.set(currentBlockNumber);
                } else {
                    // this is not ideal, we have no sessions that are ahead of the current block number
                    LOGGER.log(
                            WARNING,
                            "onSessionUpdate: currentBlockNumber or newer is not being provided by any "
                                    + "session, currentBlockNumber={0} availableBlocks={1}",
                            currentBlockNumber,
                            openSessions.stream()
                                    .mapToLong(BlockStreamProducerSession::currentBlockNumber)
                                    .summaryStatistics());
                }
            }
            Optional<BlockStreamProducerSession> newPrimarySession = openSessions.stream()
                    .filter(openSession -> openSession.currentBlockState() == BlockStreamProducerSession.BlockState.NEW
                            && openSession.currentBlockNumber() >= currentBlockNumber)
                    .min(Comparator.comparingLong(BlockStreamProducerSession::startTimeOfCurrentBlock));
            LOGGER.log(DEBUG, "onSessionUpdate: newPrimarySession was found {0}", newPrimarySession);
            if (newPrimarySession.isPresent()) {
                final BlockStreamProducerSession openSession = newPrimarySession.get();
                // skip setting currentPrimarySession, because this is a whole block and setting here as primary
                // is not necessary, as we are going to search for new primary for next block
                if (updateType != UpdateType.WHOLE_BLOCK) {
                    // set the current primary session
                    currentPrimarySession = openSession;
                }
                openSession.switchToPrimary();
                // tell all other sessions to switch to behind
                openSessions.stream()
                        .filter(otherSession -> otherSession != currentPrimarySession
                                && otherSession.sessionId() != openSession.sessionId())
                        .forEach(BlockStreamProducerSession::switchToBehind);
            } else {
                // no primary session, set to null
                currentPrimarySession = null;
                // this can happen if all sessions are behind or ahead, so lets check if they
                // are ahead as
                // that will mean we will never get any blocks
                if (openSessions.isEmpty()) {
                    // think this should never happen or at least be very rare
                    LOGGER.log(WARNING, "No sessions found, yet we got a onSessionUpdate() call");
                } else {
                    final long currentMinSessionBlockNumber = openSessions.stream()
                            .mapToLong(BlockStreamProducerSession::currentBlockNumber)
                            .min()
                            .orElse(UNKNOWN_BLOCK_NUMBER);
                    if (currentMinSessionBlockNumber > currentBlockNumber) {
                        LOGGER.log(
                                WARNING,
                                "All sessions are ahead [{1}] of the current block number [{2}], "
                                        + "this means we wil never get another block",
                                currentMinSessionBlockNumber,
                                currentBlockNumber);
                    }
                }
            }
        } finally {
            stateLock.unlock();
        }
    }

    /**
     * Called when we have a new block item batch to send to the messaging service. This is called from a
     * BlockStreamProducerSession. It allows a single forwarding point so we can update metrics and handle production vs
     * no-op modes.
     *
     * @param blockItems the block items to send to the messaging service
     */
    private void sendBlockItemsToMessagingService(@NonNull final BlockItems blockItems) {
        if (publisherConfig.type() == PublisherType.PRODUCTION) {
            // send the block items to the messaging service
            context.blockMessaging().sendBlockItems(blockItems);
            // update the metrics
            liveBlockItemsMessaged.add(blockItems.blockItems().size());
        } else {
            // in test mode, we just log the block items
            LOGGER.log(INFO, "NO_OP MODE -> Not sending block items to messaging service: {0}", blockItems);
        }
    }

    // ==== BlockNodePlugin Methods ===================================================================================

    /**
     * {@inheritDoc}
     */
    @NonNull
    @Override
    public List<Class<? extends Record>> configDataTypes() {
        return List.of(PublisherConfig.class);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void init(@NonNull final BlockNodeContext context, @NonNull final ServiceBuilder serviceBuilder) {
        Objects.requireNonNull(context);
        Objects.requireNonNull(serviceBuilder);

        this.context = context;
        // load the publisher config
        publisherConfig = context.configuration().getConfigData(PublisherConfig.class);
        // get the timeout in nanos
        timeOutNanos = publisherConfig.timeoutThresholdMillis() * 1_000_000L;
        // get type of publisher to use and log it
        LOGGER.log(INFO, "Using publisher type: {0}", publisherConfig.type());

        // create metrics
        liveBlockItemsReceived = context.metrics()
                .getOrCreate(new Counter.Config(METRICS_CATEGORY, "live_block_items_received")
                        .withDescription("Live Block Items Received"));
        liveBlockItemsMessaged = context.metrics()
                .getOrCreate(new Counter.Config(METRICS_CATEGORY, "live_block_items_messaged")
                        .withDescription("Live Block Items Messaged"));
        lowestBlockNumberInbound = context.metrics()
                .getOrCreate(new LongGauge.Config(METRICS_CATEGORY, "lowest_block_number_inbound")
                        .withDescription("Lowest Block Number Inbound"));
        currentBlockNumberInbound = context.metrics()
                .getOrCreate(new LongGauge.Config(METRICS_CATEGORY, "current_block_number_inbound")
                        .withDescription("Current Block Number Inbound"));
        highestIncomingBlockNumber = context.metrics()
                .getOrCreate(new LongGauge.Config(METRICS_CATEGORY, "highest_block_number_inbound")
                        .withDescription("Highest Live Incoming Block Number"));
        numberOfProducers = context.metrics()
                .getOrCreate(new LongGauge.Config(METRICS_CATEGORY, "producers")
                        .withDescription("No of Connected Producers"));

        // register us as a service
        serviceBuilder.registerGrpcService(this);
        // register us as a block notification handler
        context.blockMessaging()
                .registerBlockNotificationHandler(this, false, PublisherServicePlugin.class.getSimpleName());
    }

    /**
     * Called when block node is starting up after all plugins have been initialized and after web server is started.
     */
    @Override
    public void start() {
        // get the latest block number known to the system and add one for the current block
        final long latestBlockNumber =
                context.historicalBlockProvider().availableBlocks().max();
        // check if we know of any blocks
        if (latestBlockNumber != UNKNOWN_BLOCK_NUMBER) {
            // set the current block number to the latest block number known + 1
            currentBlockNumber = latestBlockNumber + 1;
            currentBlockNumberInbound.set(currentBlockNumber);
        }
    }

    /**
     * Called when block node ish shutting down
     */
    @Override
    public void stop() {
        LOGGER.log(INFO, "Stopping Publisher Service Plugin, closing {1} open sessions", openSessions.size());
        // close all open sessions
        openSessions.forEach(BlockStreamProducerSession::close);
        // clear open sessions
        openSessions.clear();
        // reset the number of producers metric
        if (numberOfProducers != null) numberOfProducers.set(0);
    }

    // ==== BlockNotificationHandler Methods ===========================================================================

    /**
     * BlockStreamPublisherService types define the gRPC methods available on the BlockStreamPublisherService.
     */
    enum BlockStreamPublisherServiceMethod implements Method {
        /**
         * The publishBlockStream method represents the bidirectional gRPC streaming method
         * Consensus Nodes should use to publish the BlockStream to the Block Node.
         */
        publishBlockStream
    }

    /**
     * Receive notifications from verification and persistence services and update our handling of listeners
     * accordingly. This is called on thread dedicated to this registered handler.
     *
     * @param notification the block notification to handle
     */
    @Override
    public void handleBlockNotification(@NonNull final BlockNotification notification) {
        Objects.requireNonNull(notification);
        stateLock.lock();
        try {
            // We have nothing to do for BlockNotification.BLOCK_VERIFIED so can ignore it
            switch (notification.type()) {
                case BLOCK_PERSISTED -> {
                    LOGGER.log(
                            DEBUG,
                            "Received notification that block {0} have been persisted.",
                            notification.blockNumber());
                    // let all subscribers know we have a good copy of the block saved to disk
                    latestAckedBlockNumber = notification.blockNumber();
                    openSessions.forEach(session ->
                            session.sendBlockPersisted(notification.blockNumber(), notification.blockHash()));
                }
                case BLOCK_FAILED_VERIFICATION -> {
                    LOGGER.log(
                            DEBUG,
                            "Received notification that block {0} have failed verification.",
                            notification.blockNumber());
                    // set the chosen source for the current block to null as we do not have one yet
                    // do this first to try and avoid more bad items sent into the system
                    currentPrimarySession = null;
                    // We need to go and request all sessions to resend the block
                    openSessions.forEach(session -> session.requestResend(notification.blockNumber()));
                    // reset out block number to last good block
                    currentBlockNumber = notification.blockNumber() - 1;
                    currentBlockNumberInbound.set(currentBlockNumber);
                }
            }
        } finally {
            stateLock.unlock();
        }
    }

    // ==== ServiceInterface Methods ===================================================================================

    /**
     * {@inheritDoc}
     */
    @NonNull
    public String serviceName() {
        return "BlockStreamService";
    }

    /**
     * {@inheritDoc}
     */
    @NonNull
    public String fullName() {
        return "com.hedera.hapi.block." + serviceName();
    }

    /**
     * {@inheritDoc}
     */
    @NonNull
    public List<Method> methods() {
        return Arrays.asList(BlockStreamPublisherServiceMethod.values());
    }

    /**
     * {@inheritDoc}
     *
     * This is called each time a new consensus node client connects to the service. It is called on a web server thread
     * so we need to acquire the state lock before doing anything that interacts with state.
     */
    @NonNull
    @Override
    public Pipeline<? super Bytes> open(
            @NonNull Method method, @NonNull RequestOptions opts, @NonNull Pipeline<? super Bytes> responses)
            throws GrpcException {
        stateLock.lock();
        try {
            final BlockStreamPublisherServiceMethod blockStreamPublisherServiceMethod =
                    (BlockStreamPublisherServiceMethod) method;
            return switch (blockStreamPublisherServiceMethod) {
                case publishBlockStream:
                    final var pipe = Pipelines.<List<BlockItemUnparsed>, PublishStreamResponse>bidiStreaming()
                            .mapRequest(bytes -> PublishStreamRequestUnparsed.PROTOBUF
                                    .parse(bytes)
                                    .blockItemsOrThrow()
                                    .blockItems())
                            .method(responsePipeline -> {
                                BlockStreamProducerSession producerBlockItemObserver = new BlockStreamProducerSession(
                                        nextSessionId++,
                                        responsePipeline,
                                        this::onSessionUpdate,
                                        liveBlockItemsReceived,
                                        stateLock,
                                        this::sendBlockItemsToMessagingService);
                                // add the session to the set of open sessions
                                openSessions.add(producerBlockItemObserver);
                                numberOfProducers.set(openSessions.size());
                                return producerBlockItemObserver;
                            })
                            .mapResponse(PublishStreamResponse.PROTOBUF::toBytes)
                            .respondTo(responses)
                            .build();
                    // the set of sessions have been updated, so we need to call the onSessionUpdate method
                    onSessionUpdate(null, UpdateType.SESSION_ADDED, UNKNOWN_BLOCK_NUMBER);
                    // return the pipeline
                    yield pipe;

                    /*case subscribeBlockStream:
                    // we do not support this method
                    throw new RuntimeException();*/
            };
        } finally {
            stateLock.unlock();
        }
    }
}
