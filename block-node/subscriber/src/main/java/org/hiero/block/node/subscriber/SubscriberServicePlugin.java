// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.subscriber;

import com.hedera.pbj.grpc.helidon.PbjRouting;
import com.hedera.pbj.runtime.grpc.GrpcException;
import com.hedera.pbj.runtime.grpc.Pipeline;
import com.hedera.pbj.runtime.grpc.Pipelines;
import com.hedera.pbj.runtime.grpc.ServiceInterface;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.metrics.api.LongGauge;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.helidon.common.Builder;
import io.helidon.webserver.Routing;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicLong;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.hapi.block.node.SubscribeStreamRequest;
import org.hiero.hapi.block.node.SubscribeStreamResponseUnparsed;

/** Provides implementation for the health endpoints of the server. */
public class SubscriberServicePlugin implements BlockNodePlugin, ServiceInterface {

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
        // remove the session from the set of open sessions
        openSessions.remove(blockStreamProducerSession);
        numberOfSubscribers.set(openSessions.size());
    }

    // ==== BlockNodePlugin Methods ====================================================================================

    /**
     * {@inheritDoc}
     */
    @Override
    public Builder<?, ? extends Routing> init(BlockNodeContext context) {
        this.context = context;
        // create the metrics
        numberOfSubscribers = context.metrics()
                .getOrCreate(new LongGauge.Config(METRICS_CATEGORY, "subscribers")
                        .withDescription("No of Connected Subscribers"));
        // register us as a service
        //return PbjRouting.builder().service(this);
        context.pbjRoutingBuilder().service(this);
        return null;
    }

    // ==== ServiceInterface Methods ===================================================================================

    /**
     * BlockStreamSubscriberService types define the gRPC methods available on the BlockStreamSubscriberService.
     */
    enum BlockStreamSubscriberServiceMethod implements Method {
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
        return "BlockStreamSubscriberService";
    }

    /**
     * {@inheritDoc}
     */
    @NonNull
    public String fullName() {
        return "com.hedera.hapi.block.node." + serviceName();
    }

    /**
     * {@inheritDoc}
     */
    @NonNull
    public List<Method> methods() {
        return Arrays.asList(BlockStreamSubscriberServiceMethod.values());
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
        final BlockStreamSubscriberServiceMethod blockStreamSubscriberServiceMethod =
                (BlockStreamSubscriberServiceMethod) method;
        return switch (blockStreamSubscriberServiceMethod) {
            case subscribeBlockStream:
                yield Pipelines.<SubscribeStreamRequest, SubscribeStreamResponseUnparsed>bidiStreaming()
                        .mapRequest(SubscribeStreamRequest.PROTOBUF::parse)
                        .method(responsePipeline -> {
                            final BlockStreamSubscriberSession blockStreamProducerSession =
                                    new BlockStreamSubscriberSession(
                                            nextClientId.getAndIncrement(),
                                            responsePipeline,
                                            context,
                                            this::closedSubscriberSessionCallback);
                            // add the session to the set of open sessions
                            openSessions.add(blockStreamProducerSession);
                            numberOfSubscribers.set(openSessions.size());
                            return blockStreamProducerSession;
                        })
                        .mapResponse(SubscribeStreamResponseUnparsed.PROTOBUF::toBytes)
                        .respondTo(responses)
                        .build();
        };
    }
}
