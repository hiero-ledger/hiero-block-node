// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.consumer;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.ERROR;
import static java.lang.System.Logger.Level.TRACE;
import static org.hiero.block.server.metrics.BlockNodeMetricTypes.Counter.HistoricToLiveStreamTransitions;
import static org.hiero.block.server.metrics.BlockNodeMetricTypes.Counter.OutboundStreamingError;

import com.hedera.hapi.block.SubscribeStreamRequest;
import com.hedera.hapi.block.SubscribeStreamResponseCode;
import com.hedera.hapi.block.stream.BlockProof;
import com.hedera.pbj.runtime.ParseException;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.UncheckedIOException;
import java.time.InstantSource;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import org.hiero.block.server.events.LivenessCalculator;
import org.hiero.block.server.events.ObjectEvent;
import org.hiero.block.server.mediator.Poller;
import org.hiero.block.server.mediator.SubscriptionHandler;
import org.hiero.block.server.metrics.MetricsService;
import org.hiero.block.server.service.ServiceStatus;
import org.hiero.hapi.block.node.BlockItemUnparsed;

/**
 * The OpenRangeStreamManager class is responsible for managing the state transitions between
 * live-streaming and historic-streaming for open-range requests. The class is designed
 * to be used in a busy-wait loop to continuously produce block item batches for a downstream
 * client.
 */
public class OpenRangeStreamManager implements StreamManager {

    private final System.Logger LOGGER = System.getLogger(getClass().getName());

    private final State initialState;
    private State currentState;

    private final AtomicLong currentLiveBlockNumber;
    private final AtomicLong currentHistoricBlockNumber;
    private final String managerId;

    private final AtomicBoolean isLiveStreamInitialized = new AtomicBoolean(false);

    // @todo(#750) - Commented out first pass. Need to revisit.
    //    private final int cueHistoricStreamingPaddingBlocks;

    private final LivenessCalculator livenessCalculator;
    private final HistoricDataPoller<List<BlockItemUnparsed>> historicDataPoller;
    private final SubscriptionHandler<List<BlockItemUnparsed>> subscriptionHandler;
    private Poller<ObjectEvent<List<BlockItemUnparsed>>> liveStreamingDataPoller;
    private final ConsumerStreamResponseObserver consumerStreamResponseObserver;

    private final ServiceStatus serviceStatus;
    private final MetricsService metricsService;

    // @todo(#750) - Commented out first pass. Need to revisit.
    //    private final Configuration configuration;

    public OpenRangeStreamManager(
            @NonNull final InstantSource producerLivenessClock,
            @NonNull final SubscribeStreamRequest subscribeStreamRequest,
            @NonNull final SubscriptionHandler<List<BlockItemUnparsed>> subscriptionHandler,
            @NonNull final HistoricDataPoller<List<BlockItemUnparsed>> historicDataPoller,
            @NonNull final ConsumerStreamResponseObserver consumerStreamResponseObserver,
            @NonNull final ServiceStatus serviceStatus,
            @NonNull final MetricsService metricsService,
            @NonNull final ConsumerConfig consumerConfig) {

        this.livenessCalculator =
                new LivenessCalculator(producerLivenessClock, consumerConfig.timeoutThresholdMillis());

        this.currentLiveBlockNumber = new AtomicLong(0);
        this.currentHistoricBlockNumber = new AtomicLong(subscribeStreamRequest.startBlockNumber());

        this.subscriptionHandler = Objects.requireNonNull(subscriptionHandler);
        this.historicDataPoller = Objects.requireNonNull(historicDataPoller);
        this.consumerStreamResponseObserver = Objects.requireNonNull(consumerStreamResponseObserver);
        this.serviceStatus = Objects.requireNonNull(serviceStatus);
        this.metricsService = Objects.requireNonNull(metricsService);

        // Assign a unique instance id to the manager for troubleshooting
        this.managerId = UUID.randomUUID().toString();

        // @todo(#750) - Commented out first pass. Need to revisit.
        //        this.configuration = Objects.requireNonNull(configuration);
        //        this.cueHistoricStreamingPaddingBlocks =
        //                configuration.getConfigData(ConsumerConfig.class).cueHistoricStreamingPaddingBlocks();

        // Pick the state machine entrypoint based on the start block number
        currentState = (subscribeStreamRequest.startBlockNumber() == 0) ? State.INIT_LIVE : State.INIT_HISTORIC;
        initialState = currentState;
    }

    /**
     * The execute method is the main entrypoint for the state machine. It is designed to be used in a
     * busy-wait loop to continuously produce block item batches for a downstream client.
     *
     * @return true if the stream should continue producing block item batches, false otherwise
     */
    @Override
    public boolean execute() {
        try {
            // Check the liveness when the caller first
            // invokes this method. This allows the manager
            // to proactively release resources if the producer
            // stops streaming.
            if (livenessCalculator.isTimeoutExpired()) {
                // Unsubscribe from the live stream
                cleanUpLiveStream();

                // Notify the downstream client we've stopped processing the stream
                consumerStreamResponseObserver.send(SubscribeStreamResponseCode.READ_STREAM_NOT_AVAILABLE);
                LOGGER.log(DEBUG, "Producer liveness timeout. Unsubscribed from the live stream.");
                return false;
            } else {
                this.currentState = currentState.execute(this);
            }

            // Inside the busy-wait loop, we
            // need to wait to avoid consuming
            // the whole CPU
            LockSupport.parkNanos(500_000L);

            // For open-range, always return true
            // to keep the stream producing block item
            // batches indefinitely.
            return true;
        } catch (Exception e) {
            cleanUpLiveStream();

            Throwable cause = e.getCause();
            if (cause instanceof UncheckedIOException) {
                // UncheckedIOException at this layer will almost
                // always be wrapped SocketExceptions from individual
                // clients disconnecting from the server streaming
                // service. This should be happening all the time.
                LOGGER.log(
                        DEBUG,
                        "UncheckedIOException caught from Pipeline instance. Unsubscribed consumer observer instance",
                        e);

            } else {
                // Report the error
                metricsService.get(OutboundStreamingError).increment();
                LOGGER.log(ERROR, "Exception thrown while streaming data", e);

                // Send an error response to the client
                LOGGER.log(ERROR, "Sending error response to client");
                consumerStreamResponseObserver.send(SubscribeStreamResponseCode.READ_STREAM_NOT_AVAILABLE);
            }

            return false;
        }
    }

    private void cleanUpLiveStream() {
        // Unsubscribe from the live stream
        isLiveStreamInitialized.set(false);
        if (subscriptionHandler != null) {
            LOGGER.log(DEBUG, "Unsubscribed from the live stream");
            subscriptionHandler.unsubscribePoller(this);
        }

        liveStreamingDataPoller = null;
    }

    // The state machine for managing the transitions for closed-range and open-range historic
    // streaming and the transitions between data sources.
    // For more information on the design, see the repository document:
    //
    // server/docs/design/historic-streaming/open-range-historic-streaming.md
    enum State {
        /**
         * INIT_HISTORIC is the starting state for closed-range historic requests and
         * open-range historic streaming requests. It's responsible for initializing the historic
         * data poller. It will transition to the HISTORIC_STREAMING state.
         */
        INIT_HISTORIC {
            @NonNull
            @Override
            public State execute(@NonNull final OpenRangeStreamManager m) {

                long currentAckedBlockNumber = getLatestAckedBlock(m);
                long historicStreamBlockNumber = m.currentHistoricBlockNumber.get();
                m.historicDataPoller.init(historicStreamBlockNumber);

                // @todo(#750) - Commented out first pass. Need to revisit.
                //   Validation in the proxy prevents a client requesting an historic start block number
                //   greater than the current acked block number. However, we can arrive at this state
                //   when downgrading from live-streaming to historic streaming. During the downgrade process,
                //   the historic stream block number will be greater than the current acked block number.
                //   Here, we're padding the number so that, when we transition, the current acked block number
                //   is at least the padding number ahead. The hope is to reduce "flapping" where we're constantly
                //   downgrading to historic then upgrading to live then downgrading to historic, etc.
                //                long paddedHistoricStreamBlockNumber = historicStreamBlockNumber +
                //   m.cueHistoricStreamingPaddingBlocks;
                //                if (paddedHistoricStreamBlockNumber > currentAckedBlockNumber) {
                //                    LOGGER.log(
                //                            TRACE,
                //                            "Padded Historic stream block number: {0} > Current acked block number:
                //   {1}",
                //                            paddedHistoricStreamBlockNumber,
                //                            currentAckedBlockNumber);
                //                    logTransition(State.CUE_HISTORIC_STREAMING);
                //                    return State.CUE_HISTORIC_STREAMING;
                //                }

                LOGGER.log(
                        DEBUG,
                        "{0} - Initialized historic data poller with the block number: {1}",
                        m.managerId,
                        historicStreamBlockNumber);
                logTransition(State.HISTORIC_STREAMING, m);
                return State.HISTORIC_STREAMING;
            }
        },

        // @todo(#750) - Commented out first pass. Need to revisit.
        //        CUE_HISTORIC_STREAMING {
        //            @NonNull
        //            @Override
        //            public State execute(@NonNull final OpenRangeStreamManager m) {
        //                long currentAckedBlockNumber = getLatestAckedBlock(m);
        //                long historicStreamBlockNumber = m.currentHistoricBlockNumber.get();
        //                // If we just transitioned back to HISTORY_STREAMING from DRAIN_LIVE_STREAMING,
        //                // the current acked block number will still be behind the historic stream block number
        //                // we proactively set in DRAIN_LIVE_STREAMING before transitioning.
        //                //
        //                // Loop until the current acked block number exceeds the historic stream block number.
        //                // It's ok to loop here. We transitioned back to HISTORIC_STREAMING because the client
        //                // was not keeping up with the live stream. So the client will continue to
        //                // receive data buffered in Helidon.
        //                long paddedHistoricStreamBlockNumber = historicStreamBlockNumber +
        //   m.cueHistoricStreamingPaddingBlocks;
        //                if (paddedHistoricStreamBlockNumber > currentAckedBlockNumber) {
        //                    LOGGER.log(
        //                            TRACE,
        //                            "Padded Historic stream block number: {0} > Current acked block number: {1}",
        //                            paddedHistoricStreamBlockNumber,
        //                            currentAckedBlockNumber);
        //                    logTransition(State.CUE_HISTORIC_STREAMING);
        //                    return State.CUE_HISTORIC_STREAMING;
        //                }
        //    LOGGER.log(
        //                        TRACE,
        //                        "Historic stream block number: {0} > Current acked block number: {1}",
        //                        historicStreamBlockNumber,
        //                        currentAckedBlockNumber);
        //                logTransition(State.HISTORIC_STREAMING);
        //                // Increment the dashboard metric
        //                m.metricsService.get(LiveToHistoricStreamTransitions).increment();
        //                return State.HISTORIC_STREAMING;
        //            }
        //        },

        /**
         * HISTORIC_STREAMING is the state for streaming historic data to the client. It's responsible
         * for polling the next batch of historic block items and sending it to the client. If a block
         * item batch is not returned, it will transition back to the HISTORIC_STREAMING state to try again.
         * It will transition to INIT_LIVE if the current historic block has caught up to the current acked block.
         * It will transition to the LIVE_STREAMING state when the historic stream has caught up to the
         * live stream.
         */
        HISTORIC_STREAMING {
            @NonNull
            @Override
            public State execute(@NonNull final OpenRangeStreamManager m) throws Exception {
                long historicStreamBlockNumber = m.currentHistoricBlockNumber.get();
                if (m.isLiveStreamInitialized.get()) {
                    long liveStreamBlockNumber = m.currentLiveBlockNumber.get();
                    if (historicStreamBlockNumber == liveStreamBlockNumber) {

                        // The historic stream has caught up to the cued up live stream.
                        // Now transition over to the live stream.
                        LOGGER.log(
                                DEBUG,
                                "{0} - Historic stream block number: {1} == Live stream block number: {2}",
                                m.managerId,
                                historicStreamBlockNumber,
                                liveStreamBlockNumber);
                        LOGGER.log(
                                DEBUG,
                                "{0} - Transitioning from historic stream to live stream on block number: {1}",
                                m.managerId,
                                liveStreamBlockNumber);
                        logTransition(State.LIVE_STREAMING, m);

                        // Increment the dashboard metric
                        m.metricsService.get(HistoricToLiveStreamTransitions).increment();

                        return State.LIVE_STREAMING;
                    }
                } else {
                    long currentAckedBlockNumber = getLatestAckedBlock(m);
                    if (historicStreamBlockNumber == currentAckedBlockNumber) {
                        // If the historic stream has caught up to the latest acked block.
                        // Initialize the live stream.
                        LOGGER.log(
                                TRACE,
                                "Historic stream block number: {0} == Current acked block number: {1}",
                                historicStreamBlockNumber,
                                currentAckedBlockNumber);
                        logTransition(State.INIT_LIVE, m);
                        return State.INIT_LIVE;
                    }
                }

                // Fetch the next batch of historic data
                final Optional<List<BlockItemUnparsed>> historicDataOpt = m.historicDataPoller.poll();
                if (historicDataOpt.isPresent()) {
                    LOGGER.log(
                            TRACE,
                            "{0} - Fetched an historic batch with data. Loop to get the next batch.",
                            m.managerId);
                    final List<BlockItemUnparsed> blockItems = historicDataOpt.get();
                    if (blockItems.getLast().hasBlockProof()) {
                        final long currentHistoricBlockNumber = getBlockNumber(blockItems);
                        LOGGER.log(
                                TRACE,
                                "{0} - Found the block proof of historic block number: {1}",
                                m.managerId,
                                currentHistoricBlockNumber);

                        // Only increment the historic block number when we've received the
                        // block proof from the batch.
                        m.currentHistoricBlockNumber.set(currentHistoricBlockNumber + 1);
                    }

                    // send data to the client
                    sendData(m, blockItems);

                } else {
                    LOGGER.log(TRACE, "{0} - No data returned from historic poll. Loop to check again.", m.managerId);
                }

                logTransition(State.HISTORIC_STREAMING, m);
                return State.HISTORIC_STREAMING;
            }
        },
        /**
         * INIT_LIVE is the starting state for open-range live-streaming requests. It's responsible for
         * initializing the live stream poller. It will transition to the LIVE_STREAMING state.
         */
        INIT_LIVE {
            @NonNull
            @Override
            public State execute(@NonNull final OpenRangeStreamManager m) {
                if (m.liveStreamingDataPoller == null) {
                    // Subscribe to the live stream
                    m.liveStreamingDataPoller = m.subscriptionHandler.subscribePoller(m);
                    LOGGER.log(DEBUG, "{0} - Initialized live stream poller", m.managerId);
                }

                // For Live-streaming requests (request [start_block_number = 0, end_block_number = 0]) we simply need
                // to start polling.
                // For historic-streaming requests (request [start_block_number > 0, end_block_number = 0]) we
                // need to cue up the live stream to the start of the next block so we can transition at a block
                // boundary.
                final State next =
                        (m.initialState == State.INIT_LIVE) ? State.LIVE_STREAMING : State.CUE_LIVE_STREAMING;
                logTransition(next, m);

                return next;
            }
        },
        /**
         * CUE_LIVE_STREAMING is the state for cueing up the live stream to the next block to transition
         * on block boundary. It will transition to the LIVE_STREAMING state.
         */
        CUE_LIVE_STREAMING {
            @NonNull
            @Override
            public State execute(@NonNull final OpenRangeStreamManager m) throws Exception {
                final Optional<ObjectEvent<List<BlockItemUnparsed>>> liveDataOpt = m.liveStreamingDataPoller.poll();
                if (liveDataOpt.isPresent()) {
                    final List<BlockItemUnparsed> blockItems = liveDataOpt.get().get();
                    if (blockItems.getLast().hasBlockProof()) {
                        final long currentLiveBlockNumber = getBlockNumber(blockItems);

                        LOGGER.log(
                                TRACE,
                                "{0} - Found the block proof of live block number: {1}",
                                m.managerId,
                                currentLiveBlockNumber);

                        // The poller consumed all the block items for the current block,
                        // so we know the poller is now cued up for the start of the next block.
                        // Set the value to currentBlockNumber + 1, so the live stream will start
                        // on the next block. All transitions must be at block boundaries.
                        m.currentLiveBlockNumber.set(currentLiveBlockNumber + 1);

                        // We're initialized
                        m.isLiveStreamInitialized.set(true);

                        // Now that we're cued up to the correct live block, pick the next state
                        // based on the initial state. If the request was strictly for live-streaming,
                        // then we can start immediately. However, if the request was to start further back in history,
                        // we need to catch up to the cued block number before transitioning to live.
                        //
                        // LIVE_STREAMING: will start immediately polling the live stream.
                        // HISTORIC_STREAMING: first needs to catch up to the cued block number
                        // before transitioning to LIVE_STREAMING.
                        LOGGER.log(
                                DEBUG,
                                "{0} - Initialized live stream to block number: {1}",
                                m.managerId,
                                m.currentLiveBlockNumber.get());
                        State next =
                                (m.initialState == State.INIT_LIVE) ? State.LIVE_STREAMING : State.HISTORIC_STREAMING;
                        logTransition(next, m);

                        return next;
                    } else {
                        LOGGER.log(TRACE, "{0} - Block Proof not found. Loop to check again.", m.managerId);
                    }
                } else {
                    LOGGER.log(TRACE, "{0} - No data returned from live poll. Loop to check again.", m.managerId);
                }

                // Try again to initialize the live stream to the next block
                logTransition(State.CUE_LIVE_STREAMING, m);
                return State.CUE_LIVE_STREAMING;
            }
        },
        /**
         * LIVE_STREAMING is the state for streaming live data from the live stream poller to the client.
         * It will continue to query the live stream poller regardless of whether it received data or not.
         */
        LIVE_STREAMING {
            @NonNull
            @Override
            public State execute(@NonNull final OpenRangeStreamManager m) throws Exception {
                // @todo(#750) - Commented out first pass. Need to revisit.
                //                if (m.liveStreamingDataPoller.exceedsThreshold()) {
                //                    LOGGER.log(
                //                            DEBUG,
                //                            "Client is not keeping up with the live stream. Transitioning to historic
                // streaming.");
                //                    logTransition(State.DRAIN_LIVE_STREAMING);
                //                    return State.DRAIN_LIVE_STREAMING;
                //                }

                // Get the next batch of live data
                final Optional<ObjectEvent<List<BlockItemUnparsed>>> liveDataOpt = m.liveStreamingDataPoller.poll();
                if (liveDataOpt.isPresent()) {
                    // send data to the client
                    sendData(m, liveDataOpt.get().get());
                    LOGGER.log(TRACE, "{0} - Fetched a live batch with data. Loop to get the next batch.", m.managerId);
                } else {
                    LOGGER.log(TRACE, "{0} - No data returned from live poll. Loop to check again.", m.managerId);
                }

                logTransition(State.LIVE_STREAMING, m);
                return State.LIVE_STREAMING;
            }
        };
        // @todo(#750) - Commented out first pass. Need to revisit.
        //        DRAIN_LIVE_STREAMING {
        //            @NonNull
        //            @Override
        //            public State execute(@NonNull final OpenRangeStreamManager m) throws Exception {
        //                // Drain the live stream
        //                final Optional<ObjectEvent<List<BlockItemUnparsed>>> liveDataOpt =
        // m.liveStreamingDataPoller.poll();
        //                if (liveDataOpt.isPresent()) {
        //                    final List<BlockItemUnparsed> blockItems = liveDataOpt.get().get();
        //                    // send data to the client
        //                    m.consumerStreamResponseObserver.send(blockItems);
        //                    // The poller found block item data. If the block items
        //                    // contain a block proof, then we can transition to historic streaming.
        //                    // Otherwise, fall through and loop again.
        //                    if (blockItems.getLast().hasBlockProof()) {
        //                        long liveBlockNumber = getBlockNumber(blockItems);
        //                        LOGGER.log(TRACE, "Found the block proof for live block number: {0}",
        // liveBlockNumber);
        //                        // Prep the historic stream to start from the next block
        //                        m.currentHistoricBlockNumber.set(liveBlockNumber + 1);
        //                        logTransition(State.INIT_HISTORIC);
        //                        // Unsubscribe from the live stream to free up the sequence in the ring buffer
        //                        m.cleanUpLiveStream();
        //                        return State.INIT_HISTORIC;
        //                    }
        //                } else {
        //                    LOGGER.log(TRACE, "No data returned from live poll. Loop to check again.");
        //                }
        //                logTransition(State.DRAIN_LIVE_STREAMING);
        //                return State.DRAIN_LIVE_STREAMING;
        //            }
        //        };

        private static final System.Logger LOGGER = System.getLogger(State.class.getName());

        @NonNull
        public abstract State execute(@NonNull final OpenRangeStreamManager m) throws Exception;

        private static void sendData(@NonNull final OpenRangeStreamManager m, final List<BlockItemUnparsed> blockItems)
                throws ParseException {

            // Refresh the liveness when sending data to
            // the client
            m.livenessCalculator.refresh();
            m.consumerStreamResponseObserver.send(blockItems);
        }

        private static long getLatestAckedBlock(OpenRangeStreamManager m) {
            return (m.serviceStatus.getLatestAckedBlock() != null)
                    ? m.serviceStatus.getLatestAckedBlock().getBlockNumber()
                    : 0;
        }

        private static long getBlockNumber(final List<BlockItemUnparsed> blockItems) throws ParseException {
            return BlockProof.PROTOBUF.parse(blockItems.getLast().blockProof()).block();
        }

        public void logTransition(State next, OpenRangeStreamManager m) {
            LOGGER.log(TRACE, "{0} - Transition: {1} ==> {2}", m.managerId, this, next);
        }
    }

    State getState() {
        return currentState;
    }
}
