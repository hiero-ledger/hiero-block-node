// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.subscriber;

import com.hedera.pbj.runtime.grpc.GrpcException;
import com.hedera.pbj.runtime.grpc.Pipeline;
import com.hedera.pbj.runtime.grpc.Pipelines;
import com.hedera.pbj.runtime.grpc.ServiceInterface;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.metrics.api.LongGauge;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.lang.System.Logger.Level;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicLong;
import org.hiero.block.api.SubscribeStreamRequest;
import org.hiero.block.api.protoc.BlockStreamSubscribeServiceGrpc;
import org.hiero.block.internal.SubscribeStreamResponseUnparsed;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.block.node.spi.ServiceBuilder;

/** Provides implementation for the health endpoints of the server. */
public class SubscriberServicePlugin implements BlockNodePlugin, ServiceInterface {
    /** The logger for this class. */
    private final System.Logger LOGGER = System.getLogger(getClass().getName());
    /** The next client id to use when a new client session is created */
    private final AtomicLong nextClientId = new AtomicLong(0);
    /** Set of open client sessions */
    private final ConcurrentSkipListSet<BlockStreamSubscriberSession> openSessions =
            new ConcurrentSkipListSet<>(Comparator.comparingLong(BlockStreamSubscriberSession::clientId));
    /** The block node context */
    private BlockNodeContext context;
    /** The number of subscribers publishing block items. */
    private LongGauge numberOfSubscribers;

    /**
     * Callback method to be called when a subscriber session is closed.
     *
     * @param blockStreamProducerSession the session that was closed
     */
    private void closedSubscriberSessionCallback(
            @NonNull final BlockStreamSubscriberSession blockStreamProducerSession) {
        long sessionIndex = blockStreamProducerSession.clientId();
        int sessionCount = openSessions.size();
        // remove the session from the set of open sessions
        LOGGER.log(Level.DEBUG, "Removing session ID %d of %d.".formatted(sessionIndex, sessionCount));
        openSessions.remove(blockStreamProducerSession);
        numberOfSubscribers.set(openSessions.size());
    }

    private static final BlockStreamSubscriberSession[] EMPTY_SESSION_ARRAY = {};
    /**
     * Testing method to provide visibility into the open sessions (which are message handlers)
     * so we can trigger messaging behaviors and see results.
     */
    BlockStreamSubscriberSession[] getOpenSessions() {
        return openSessions.toArray(EMPTY_SESSION_ARRAY);
    }

    // ==== BlockNodePlugin Methods ====================================================================================

    /**
     * {@inheritDoc}
     */
    @Override
    public void init(BlockNodeContext context, ServiceBuilder serviceBuilder) {
        this.context = context;
        // create the metrics
        numberOfSubscribers = context.metrics()
                .getOrCreate(new LongGauge.Config(METRICS_CATEGORY, "subscribers")
                        .withDescription("No of Connected Subscribers"));
        // register us as a service
        serviceBuilder.registerGrpcService(this);
    }

    // ==== ServiceInterface Methods ===================================================================================

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

    /**
     * {@inheritDoc}
     */
    @NonNull
    public String serviceName() {
        String[] parts = fullName().split("\\.");
        return parts[parts.length - 1];
    }

    /**
     * {@inheritDoc}
     */
    @NonNull
    public String fullName() {
        return BlockStreamSubscribeServiceGrpc.SERVICE_NAME;
    }

    /**
     * {@inheritDoc}
     */
    @NonNull
    public List<Method> methods() {
        return Arrays.asList(SubscriberServiceMethod.values());
    }

    /**
     * {@inheritDoc}
     *
     * This is called each time a new stream consumer connects to the server.
     */
    @NonNull
    @Override
    public Pipeline<? super Bytes> open(
            @NonNull Method method, @NonNull RequestOptions opts, @NonNull Pipeline<? super Bytes> responses)
            throws GrpcException {
        LOGGER.log(Level.INFO, "Real Plugin Open called");
        final SubscriberServiceMethod subscriberServiceMethod = (SubscriberServiceMethod) method;
        return switch (subscriberServiceMethod) {
            case subscribeBlockStream:
                // subscribeBlockStream is server streaming end point so the client sends a single request and the
                // server sends many responses
                yield Pipelines.<SubscribeStreamRequest, SubscribeStreamResponseUnparsed>serverStreaming()
                        .mapRequest(SubscribeStreamRequest.PROTOBUF::parse)
                        .method((request, responsePipeline) -> {
                            final BlockStreamSubscriberSession blockStreamSession = new BlockStreamSubscriberSession(
                                    nextClientId.getAndIncrement(),
                                    request,
                                    responsePipeline,
                                    context,
                                    this::closedSubscriberSessionCallback);
                            // add the session to the set of open sessions
                            openSessions.add(blockStreamSession);
                            numberOfSubscribers.set(openSessions.size());
                        })
                        .mapResponse(SubscribeStreamResponseUnparsed.PROTOBUF::toBytes)
                        .respondTo(responses)
                        .build();
        };
    }
}
