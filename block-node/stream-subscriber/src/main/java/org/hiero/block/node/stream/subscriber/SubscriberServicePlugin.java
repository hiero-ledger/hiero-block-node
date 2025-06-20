// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.stream.subscriber;

import static java.util.Objects.requireNonNull;

import com.hedera.pbj.runtime.grpc.GrpcException;
import com.hedera.pbj.runtime.grpc.Pipeline;
import com.hedera.pbj.runtime.grpc.Pipelines;
import com.hedera.pbj.runtime.grpc.Pipelines.ServerStreamingMethod;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.metrics.api.Counter;
import com.swirlds.metrics.api.LongGauge;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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
import org.hiero.block.internal.SubscribeStreamResponseUnparsed;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.block.node.spi.ServiceBuilder;

/**
 * Provides implementation for the block stream subscriber endpoints of the server. These handle incoming requests for block
 * stream from consumers.
 *
 * <p>The plugin registers itself with the service builder during initialization and manages
 * the lifecycle of subscriber connections.
 */
public class SubscriberServicePlugin implements BlockNodePlugin, BlockStreamSubscribeServiceInterface {
    /** The logger for this class. */
    private final Logger LOGGER = System.getLogger(getClass().getName());
    /** The block node context, used to provide access to facilities */
    private BlockNodeContext context;
    /** A handler for client requests */
    private SubscribeBlockStreamHandler clientHandler;

    /*==================== BlockNodePlugin Methods ====================*/

    /**
     * {@inheritDoc}
     */
    @Override
    public void init(@NonNull final BlockNodeContext context, @NonNull final ServiceBuilder serviceBuilder) {
        this.context = requireNonNull(context);
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
        private final Map<Long, BlockStreamSubscriberSession> openSessions;
        // Metrics
        /** Counter for errors while streaming to subscribers */
        private final Counter subscriberErrorsCounter;
        /** Gauge for number of subscribers */
        private final LongGauge numberOfSubscribers;

        private final ExecutorService virtualThreadExecutor;
        private final ExecutorCompletionService<BlockStreamSubscriberSession> streamSessions;

        private SubscribeBlockStreamHandler(@NonNull final BlockNodeContext context) {
            this.context = requireNonNull(context);
            this.openSessions = new ConcurrentSkipListMap<>();
            virtualThreadExecutor = context.threadPoolManager().getVirtualThreadExecutor();
            streamSessions = new ExecutorCompletionService<>(virtualThreadExecutor);
            // create the metrics
            numberOfSubscribers = context.metrics()
                    .getOrCreate(new LongGauge.Config(METRICS_CATEGORY, "subscriber_open_connections")
                            .withDescription("Connected subscribers"));
            subscriberErrorsCounter = context.metrics()
                    .getOrCreate(new Counter.Config(METRICS_CATEGORY, "subscriber_errors")
                            .withDescription("Errors while streaming to subscribers"));
        }

        private void stop() {
            // Stop allowing new connection threads.
            virtualThreadExecutor.shutdown();
            // Close all connections and notify the clients.
            for (final BlockStreamSubscriberSession session : openSessions.values()) {
                session.close(SubscribeStreamResponse.Code.SUCCESS);
            }
            // Make sure all the threads complete.
            while (!openSessions.isEmpty()) {
                try {
                    // This blocks until the session thread ends, but the close
                    // calls above _should have_ ended all the threads already.
                    openSessions.remove(streamSessions.take().get().clientId());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (ExecutionException e) {
                    // This should never happen, but if it does, log the error.
                    final String message = "Error ending subscriber session: {0}.";
                    LOGGER.log(Level.ERROR, message, e);
                }
            }
        }

        @Override
        public void apply(
                @NonNull final SubscribeStreamRequest request,
                @NonNull final Pipeline<? super SubscribeStreamResponseUnparsed> responsePipeline)
                throws InterruptedException {
            final long clientId = nextClientId.getAndIncrement();
            final CountDownLatch sessionReadyLatch = new CountDownLatch(1);
            final BlockStreamSubscriberSession blockStreamSession =
                    new BlockStreamSubscriberSession(clientId, request, responsePipeline, context, sessionReadyLatch);
            streamSessions.submit(blockStreamSession);
            // Wait for the session to start
            sessionReadyLatch.await();
            // add the session to the set of open sessions
            openSessions.put(clientId, blockStreamSession);
            numberOfSubscribers.set(sessionCount.incrementAndGet());
            Future<BlockStreamSubscriberSession> completedSessionFuture;
            // Get any available completed sessions and log success/failure.
            // noinspection NestedAssignment
            while ((completedSessionFuture = streamSessions.poll()) != null) {
                handleCompletedStream(completedSessionFuture);
            }
        }

        private void handleCompletedStream(final Future<BlockStreamSubscriberSession> completedSessionFuture)
                throws InterruptedException {
            try {
                BlockStreamSubscriberSession completedSession = completedSessionFuture.get();
                long clientId = completedSession.clientId();
                // Remove the completed session from open sessions.
                openSessions.remove(clientId);
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
            } catch (ExecutionException e) {
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
