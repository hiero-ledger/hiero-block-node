// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.stream.subscriber;

import static java.util.Objects.requireNonNull;

import com.hedera.pbj.runtime.grpc.GrpcException;
import com.hedera.pbj.runtime.grpc.Pipeline;
import com.hedera.pbj.runtime.grpc.Pipelines;
import com.hedera.pbj.runtime.grpc.Pipelines.ServerStreamingMethod;
import com.hedera.pbj.runtime.grpc.ServiceInterface;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.metrics.api.LongGauge;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import org.hiero.block.api.SubscribeStreamRequest;
import org.hiero.block.api.SubscribeStreamResponse;
import org.hiero.block.api.protoc.BlockStreamSubscribeServiceGrpc;
import org.hiero.block.internal.SubscribeStreamResponseUnparsed;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.block.node.spi.ServiceBuilder;

/**
 * Provides implementation for the block stream subscriber endpoints of the server. These handle incoming requests for block
 * stream from consumers.
 *
 * <p>This plugin is responsible for:
 * <ul>
 *   <li>Managing subscriber sessions for clients requesting block streams</li>
 *   <li>Handling both live-streaming and historical block data requests</li>
 *   <li>Implementing the gRPC service interface for block stream subscriptions</li>
 *   <li>Coordinating between multiple publishers when they connect to the block node</li>
 *   <li>Maintaining metrics about active subscribers and stream health</li>
 * </ul>
 *
 * <p>The plugin registers itself with the service builder during initialization and manages
 * the lifecycle of subscriber connections.
 */
public class SubscriberServicePlugin implements BlockNodePlugin, ServiceInterface {
    /** The service name for this service, which must match the gRPC service name */
    private static final String SERVICE_NAME = parseGrpcName();
    /** The logger for this class. */
    private final Logger LOGGER = System.getLogger(getClass().getName());
    /** Metric for the number of subscribers receiving block items. */
    private final List<SubscribeBlockStreamHandler> clientHandlers = new LinkedList<>();
    /** The block node context */
    private BlockNodeContext context;
    /** The subscriber plugin configuration */
    private SubscriberConfig pluginConfiguration;
    /** The client handler for client subscriptions*/
    private SubscribeBlockStreamHandler clientHandler;

    /*==================== BlockNodePlugin Methods ====================*/

    /**
     * {@inheritDoc}
     */
    @Override
    public void init(@NonNull final BlockNodeContext context, @NonNull final ServiceBuilder serviceBuilder) {
        requireNonNull(serviceBuilder);
        this.context = requireNonNull(context);
        pluginConfiguration = context.configuration().getConfigData(SubscriberConfig.class);
        // register us as a service
        serviceBuilder.registerGrpcService(this);
    }

    /*==================== ServiceInterface Methods ====================*/

    /**
     * BlockStreamSubscriberService types define the gRPC methods available on the BlockStreamSubscriberService.
     */
    enum SubscriberServiceMethod implements Method {
        /**
         * The subscribeBlockStream method represents the server-streaming gRPC method
         * consumers should use to subscribe to the BlockStream from the Block Node.
         */
        subscribeBlockStream
    }

    @Override
    public void start() {
        // Create the client handler and wait for it to start and reach a ready state.
        clientHandler = new SubscribeBlockStreamHandler(context, this);
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
    public String serviceName() {
        return SERVICE_NAME;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @NonNull
    public String fullName() {
        return BlockStreamSubscribeServiceGrpc.SERVICE_NAME;
    }

    /**
     * Minimal method to parse the gRPC service name from the full name.
     * <br/>This is called once and stored statically.
     */
    private static String parseGrpcName() {
        final String[] parts = BlockStreamSubscribeServiceGrpc.SERVICE_NAME.split("\\.");
        return parts[parts.length - 1];
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @NonNull
    public List<Method> methods() {
        return Arrays.asList(SubscriberServiceMethod.values());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Class<? extends Record>> configDataTypes() {
        return List.of(SubscriberConfig.class);
    }

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
        final SubscriberServiceMethod subscriberServiceMethod = (SubscriberServiceMethod) method;
        return switch (subscriberServiceMethod) {
            case subscribeBlockStream:
                clientHandlers.add(clientHandler);
                // subscribeBlockStream is server streaming end point so the client sends a single request and the
                // server sends many responses
                yield Pipelines.<SubscribeStreamRequest, SubscribeStreamResponseUnparsed>serverStreaming()
                        .mapRequest(SubscribeStreamRequest.PROTOBUF::parse)
                        .method(clientHandler)
                        .mapResponse(SubscribeStreamResponseUnparsed.PROTOBUF::toBytes)
                        .respondTo(responses)
                        .build();
        };
    }

    // Visible for Testing
    Map<Long, BlockStreamSubscriberSession> getOpenSessions() {
        return clientHandler.getOpenSessions();
    }

    /**
     * Handler for block stream subscription requests from clients.
     * Responsible for:
     * <ul>
     *   <li>Managing client sessions for block stream subscribers</li>
     *   <li>Processing subscription requests and streaming block data to clients</li>
     *   <li>Tracking active subscriber sessions and their lifecycle</li>
     *   <li>Handling graceful termination of subscriber connections</li>
     *   <li>Reporting metrics about active subscribers</li>
     * </ul>
     */
    static class SubscribeBlockStreamHandler
            implements ServerStreamingMethod<SubscribeStreamRequest, SubscribeStreamResponseUnparsed> {
        private final Logger LOGGER = System.getLogger(getClass().getName());
        /** Count of active sessions, because LongGauge doesn't support increment/decrement */
        private final AtomicLong sessionCount = new AtomicLong(0L);
        /** The next client id to use when a new client session is created */
        private final AtomicLong nextClientId = new AtomicLong(0);
        /** The block node context */
        private final BlockNodeContext context;
        /** The subscriber plugin responsible for handling incoming requests*/
        private final SubscriberServicePlugin plugin;
        /** Set of open client sessions */
        private final Map<Long, BlockStreamSubscriberSession> openSessions;

        private final LongGauge numberOfSubscribers;
        private final ExecutorCompletionService<BlockStreamSubscriberSession> streamSessions;

        private SubscribeBlockStreamHandler(
                @NonNull final BlockNodeContext context, @NonNull final SubscriberServicePlugin plugin) {
            this.context = requireNonNull(context);
            this.plugin = requireNonNull(plugin);
            this.openSessions = new ConcurrentSkipListMap<>();
            streamSessions = new ExecutorCompletionService<>(Executors.newVirtualThreadPerTaskExecutor());
            // create the metrics
            numberOfSubscribers = context.metrics()
                    .getOrCreate(new LongGauge.Config(METRICS_CATEGORY, "subscribers")
                            .withDescription("Number of Connected Subscribers"));
        }

        private void stop() {
            // Close all connections and notify the clients.
            for (final BlockStreamSubscriberSession session : openSessions.values()) {
                session.close(SubscribeStreamResponse.Code.READ_STREAM_SUCCESS);
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
            // add the session to the set of open sessions
            sessionReadyLatch.await();
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
                } else {
                    // Otherwise, log that the session completed successfully.
                    LOGGER.log(Level.TRACE, "Subscriber session %(,d completed successfully.".formatted(clientId));
                }
            } catch (ExecutionException e) {
                // Note, this only happens if something truly unexpected (i.e. an Error) caused
                // the session to fail, so the error is significant.
                final String message = "Subscriber session failed due to unhandled %s:%n{0}.".formatted(e.getCause());
                LOGGER.log(Level.ERROR, message, e);
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
