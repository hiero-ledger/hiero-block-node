// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.stream.subscriber;

import static java.util.Objects.requireNonNull;

import com.hedera.pbj.runtime.grpc.GrpcException;
import com.hedera.pbj.runtime.grpc.Pipeline;
import com.hedera.pbj.runtime.grpc.Pipelines;
import com.hedera.pbj.runtime.grpc.Pipelines.ServerStreamingMethod;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import org.hiero.block.api.BlockStreamSubscribeServiceInterface;
import org.hiero.block.api.SubscribeStreamRequest;
import org.hiero.block.api.SubscribeStreamResponse;
import org.hiero.block.api.SubscribeStreamResponse.Code;
import org.hiero.block.internal.SubscribeStreamResponseUnparsed;
import org.hiero.block.internal.SubscribeStreamResponseUnparsed.Builder;
import org.hiero.block.node.spi.ApplicationStateFacility;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.block.node.spi.ServiceBuilder;
import org.hiero.block.node.stream.subscriber.BlockStreamSubscriberSession.SessionContext;
import org.hiero.metrics.LongCounter;
import org.hiero.metrics.LongGauge;
import org.hiero.metrics.core.MetricKey;
import org.hiero.metrics.core.MetricRegistry;

/**
 * Provides implementation for the block stream subscriber endpoints of the server. These handle incoming requests for block
 * stream from consumers.
 *
 * <p>The plugin registers itself with the service builder during initialization and manages
 * the lifecycle of subscriber connections.
 */
public class SubscriberServicePlugin implements BlockNodePlugin, BlockStreamSubscribeServiceInterface {
    /** Metric key for the number of open subscriber connections */
    public static final MetricKey<LongGauge> METRIC_SUBSCRIBER_OPEN_CONNECTIONS =
            MetricKey.of("subscriber_open_connections", LongGauge.class).addCategory(METRICS_CATEGORY);
    /** Metric key for the number of subscriber errors */
    public static final MetricKey<LongCounter> METRIC_SUBSCRIBER_ERRORS =
            MetricKey.of("subscriber_errors", LongCounter.class).addCategory(METRICS_CATEGORY);

    /** The logger for this class. */
    private final Logger LOGGER = System.getLogger(getClass().getName());
    /** The block node context, used to provide access to facilities */
    private BlockNodeContext context;
    /** The application state facility, for updating application state. */
    private ApplicationStateFacility applicationStateFacility;
    /** A handler for client requests */
    private SubscribeBlockStreamHandler clientHandler;

    /*==================== BlockNodePlugin Methods ====================*/

    /**
     * {@inheritDoc}
     */
    @Override
    public void init(
            @NonNull final BlockNodeContext context,
            @NonNull final ServiceBuilder serviceBuilder,
            @NonNull final ApplicationStateFacility applicationStateFacility) {
        this.context = requireNonNull(context);
        this.applicationStateFacility = Objects.requireNonNull(applicationStateFacility);
        // register us as a service
        serviceBuilder.registerGrpcService(this);
    }

    @Override
    public void start() {
        // Create the client handler and wait for it to start and reach a ready state.
        clientHandler = new SubscribeBlockStreamHandler(context);
    }

    @Override
    public void stop() {
        clientHandler.stop();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @NonNull
    public List<Class<? extends Record>> configDataTypes() {
        return List.of(SubscriberConfig.class);
    }

    /*==================== BlockStreamSubscribeServiceInterface Methods ====================*/

    /**
     * {@inheritDoc}
     *
     * This is called each time a new stream consumer connects to the server.
     */
    @Override
    @NonNull
    public Pipeline<? super Bytes> open(
            @NonNull Method method, @NonNull RequestOptions opts, @NonNull Pipeline<? super Bytes> responses)
            throws GrpcException {
        LOGGER.log(Level.DEBUG, "Real Plugin Open called");
        final BlockStreamSubscribeServiceMethod subscriberServiceMethod = (BlockStreamSubscribeServiceMethod) method;
        return switch (subscriberServiceMethod) {
            case subscribeBlockStream ->
                // subscribeBlockStream is server streaming end point, so the client sends a single request, and the
                // server sends many responses
                Pipelines.<SubscribeStreamRequest, SubscribeStreamResponseUnparsed>serverStreaming()
                        .mapRequest(SubscribeStreamRequest.PROTOBUF::parse)
                        .method(clientHandler)
                        .mapResponse(SubscribeStreamResponseUnparsed.PROTOBUF::toBytes)
                        .respondTo(responses)
                        .build();
        };
    }

    /**
     * Does nothing but is required by the interface. We override the open method directly to handle requests.
     */
    @Override
    public void subscribeBlockStream(
            SubscribeStreamRequest request, Pipeline<? super SubscribeStreamResponse> replies) {
        // This method is not used as wr override the open method directly, but is required by the interface.
    }

    // Visible for Testing
    Map<Long, BlockStreamSubscriberSession> getOpenSessions() {
        return clientHandler.getOpenSessions();
    }

    /**
     * Handler for block stream subscription requests from clients. Handles creation of session, assigning a clientId and managing futures.
     */
    static class SubscribeBlockStreamHandler
            implements ServerStreamingMethod<SubscribeStreamRequest, SubscribeStreamResponseUnparsed> {
        private final Logger LOGGER = System.getLogger(getClass().getName());
        /** Count of active sessions, because LongGauge doesn't support increment/decrement */
        private final AtomicLong sessionCount = new AtomicLong(0L);
        /** The next client id to use when a new client session is created */
        private final AtomicLong nextClientId = new AtomicLong(0);
        /** A context that applies to the pipeline this handler supports. */
        private final BlockNodeContext context;
        /** Set of open client sessions */
        private volatile Map<Long, BlockStreamSubscriberSession> openSessions;
        // Metrics
        /** Counter for errors while streaming to subscribers */
        private final LongCounter.Measurement subscriberErrorsCounter;
        /** Gauge for number of subscribers */
        private final LongGauge.Measurement numberOfSubscribers;

        private final ExecutorService virtualThreadExecutor;
        private volatile CompletionService<BlockStreamSubscriberSession> streamSessions;

        private SubscribeBlockStreamHandler(@NonNull final BlockNodeContext context) {
            this.context = requireNonNull(context);
            openSessions = new ConcurrentSkipListMap<>();
            virtualThreadExecutor = context.threadPoolManager().getVirtualThreadExecutor();
            streamSessions = new ExecutorCompletionService<>(virtualThreadExecutor);
            // create the metrics
            final MetricRegistry metricRegistry = context.metricRegistry();
            numberOfSubscribers = metricRegistry
                    .register(LongGauge.builder(METRIC_SUBSCRIBER_OPEN_CONNECTIONS)
                            .setDescription("Connected subscribers"))
                    .getOrCreateNotLabeled();
            subscriberErrorsCounter = metricRegistry
                    .register(LongCounter.builder(METRIC_SUBSCRIBER_ERRORS)
                            .setDescription("Errors while streaming to subscribers"))
                    .getOrCreateNotLabeled();
        }

        private void stop() {
            // Stop allowing new connection threads.
            CompletionService<BlockStreamSubscriberSession> sessionsToClose = streamSessions;
            streamSessions = null;
            Map<Long, BlockStreamSubscriberSession> closeableSessions = openSessions;
            openSessions = null;
            // Close all connections and notify the clients.
            // Handle a nigh impossible situation where stop is called twice.
            if (closeableSessions != null) {
                for (final BlockStreamSubscriberSession session : closeableSessions.values()) {
                    session.close(SubscribeStreamResponse.Code.SUCCESS);
                }
                // Make sure all the threads complete.
                while (!closeableSessions.isEmpty() && sessionsToClose != null) {
                    try {
                        // This blocks until the session thread ends, but the close
                        // calls above _should have_ ended all the threads already.
                        closeableSessions.remove(sessionsToClose.take().get().clientId());
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } catch (ExecutionException e) {
                        // This should never happen, but if it does, log the error.
                        final String message = "Error ending subscriber session: {0}.";
                        LOGGER.log(Level.ERROR, message, e);
                    }
                }
            }
        }

        @SuppressWarnings("NestedAssignment")
        @Override
        public void apply(
                @NonNull final SubscribeStreamRequest request,
                @NonNull final Pipeline<? super SubscribeStreamResponseUnparsed> responsePipeline)
                throws InterruptedException {
            final long clientId = nextClientId.getAndIncrement();
            final CountDownLatch sessionReadyLatch = new CountDownLatch(1);
            // IMPORTANT! Assign these to local variables to avoid potential
            // concurrent modification issues.
            final CompletionService<BlockStreamSubscriberSession> streams = streamSessions;
            final Map<Long, BlockStreamSubscriberSession> sessions = openSessions;
            if (streams != null && sessions != null) {
                final SessionContext sessionContext = SessionContext.create(clientId, request, context);
                final BlockStreamSubscriberSession blockStreamSession =
                        new BlockStreamSubscriberSession(sessionContext, responsePipeline, context, sessionReadyLatch);
                streams.submit(blockStreamSession);
                // Wait for the session to start
                sessionReadyLatch.await();
                // add the session to the set of open sessions
                sessions.put(clientId, blockStreamSession);
                numberOfSubscribers.set(sessionCount.incrementAndGet());
                Future<BlockStreamSubscriberSession> completedSessionFuture;
                // Get any available completed sessions and log success/failure.
                while ((completedSessionFuture = streams.poll()) != null) {
                    handleCompletedStream(completedSessionFuture);
                }
            } else {
                failStreamRequest(responsePipeline);
            }
        }

        /**
         * Sends an error response to the client if a new request cannot be fulfilled.
         * <p>
         * This is typically called when a request comes in after the handler is
         * processing a stop or shut down.
         */
        private void failStreamRequest(
                @NonNull final Pipeline<? super SubscribeStreamResponseUnparsed> responsePipeline) {
            final Builder response =
                    SubscribeStreamResponseUnparsed.newBuilder().status(Code.NOT_AVAILABLE);
            responsePipeline.onNext(response.build());
            try {
                responsePipeline.onComplete();
            } catch (RuntimeException e) {
                // If the pipeline cannot be completed, log and suppress this exception.
                final String message = "Suppressed client error when \"failing\" stream for new client %n%s";
                LOGGER.log(Level.DEBUG, message.formatted(e.getMessage()), e);
            }
        }

        private void handleCompletedStream(final Future<BlockStreamSubscriberSession> completedSessionFuture)
                throws InterruptedException {
            try {
                BlockStreamSubscriberSession completedSession = completedSessionFuture.get();
                long clientId = completedSession.clientId();
                // Remove the completed session from open sessions.
                final Map<Long, BlockStreamSubscriberSession> sessions = openSessions;
                if (sessions != null) sessions.remove(clientId);
                final Exception failureCause = completedSession.getSessionFailedCause();
                if (failureCause != null) {
                    // If the session failed, log the failure.
                    // Subscribers can reconnect or retry, so this is only an informational log.
                    final String message = "Subscriber session %(,d failed due to {0}.".formatted(clientId);
                    LOGGER.log(Level.INFO, message, failureCause);
                    subscriberErrorsCounter.increment();
                } else {
                    // Otherwise, log that the session completed successfully.
                    LOGGER.log(Level.TRACE, "Subscriber session %(,d completed successfully.".formatted(clientId));
                }
            } catch (final CancellationException | ExecutionException e) {
                // Note, this only happens if something truly unexpected (i.e. an Error) caused
                // the session to fail, so the error is significant.
                final String message = "Subscriber session failed due to unhandled %s:%n{0}.".formatted(e.getCause());
                LOGGER.log(Level.ERROR, message, e);
                subscriberErrorsCounter.increment();
            }
            // Decrement the session count and update the metric.
            numberOfSubscribers.set(sessionCount.decrementAndGet());
        }

        /*==================== Testing Access Methods ====================*/
        /**
         * Testing method to provide visibility into the open sessions (which are message handlers)
         * so we can trigger messaging behaviors and see results.
         */
        Map<Long, BlockStreamSubscriberSession> getOpenSessions() {
            return Collections.unmodifiableMap(openSessions);
        }
    }
}
