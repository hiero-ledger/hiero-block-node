// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.stream.publisher;

import com.hedera.pbj.runtime.grpc.Pipeline;
import com.hedera.pbj.runtime.grpc.Pipelines;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.metrics.api.Counter;
import com.swirlds.metrics.api.LongGauge;
import com.swirlds.metrics.api.Metrics;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.List;
import java.util.Objects;
import org.hiero.block.api.BlockStreamPublishServiceInterface;
import org.hiero.block.api.PublishStreamRequest;
import org.hiero.block.api.PublishStreamResponse;
import org.hiero.block.internal.PublishStreamRequestUnparsed;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.block.node.spi.ServiceBuilder;

public final class StreamPublisherPlugin implements BlockNodePlugin, BlockStreamPublishServiceInterface {
    /** The logger for this class. */
    private final System.Logger LOGGER = System.getLogger(getClass().getName());

    /** The block node context, for access to core facilities. */
    private BlockNodeContext context;
    /** The configuration for the publisher */
    private PublisherConfig publisherConfig;
    /** */
    private ServiceBuilder serviceBuilder;
    /** The publisher block manager, which connects handlers to the messaging facility. */
    private StreamPublisherManager publisherManager;

    // Metrics fields
    /** The metrics used by the publisher Handlers. */
    private PublisherHandler.MetricsHolder handlerMetrics;
    /** The metrics used by the publisher Manager. */
    private LiveStreamPublisherManager.MetricsHolder managerMetrics;
    /** The number of live block items messaged to the messaging service. */
    private Counter liveBlockItemsMessaged;
    /** The number of producers publishing block items. */
    private LongGauge numberOfProducers;

    @Override
    @NonNull
    public Pipeline<? super Bytes> open(
            @NonNull final Method method,
            @NonNull final RequestOptions options,
            @NonNull final Pipeline<? super Bytes> replies) {
        final BlockStreamPublishServiceMethod blockStreamPublisherServiceMethod =
                (BlockStreamPublishServiceMethod) method;
        return switch (blockStreamPublisherServiceMethod) {
            case publishBlockStream ->
                Pipelines.<PublishStreamRequestUnparsed, PublishStreamResponse>bidiStreaming()
                        .mapRequest(PublishStreamRequestUnparsed.PROTOBUF::parse)
                        .method(this::initiatePublisherHandler)
                        .respondTo(replies)
                        .mapResponse(PublishStreamResponse.PROTOBUF::toBytes)
                        .build();
        };
    }

    public Pipeline<? super PublishStreamRequestUnparsed> initiatePublisherHandler(
            @NonNull final Pipeline<? super PublishStreamResponse> replies) {
        return publisherManager.addHandler(replies, handlerMetrics);
    }

    @Override
    public void init(@NonNull final BlockNodeContext context, @NonNull final ServiceBuilder serviceBuilder) {
        this.context = Objects.requireNonNull(context);
        this.serviceBuilder = Objects.requireNonNull(serviceBuilder);
        // load the publisher config
        publisherConfig = context.configuration().getConfigData(PublisherConfig.class);
    }

    @Override
    public void start() {
        // Initialize plugin metrics
        initMetrics(context.metrics());
        // register us as a service
        serviceBuilder.registerGrpcService(this);
        // register us as a block notification handler
        publisherManager = new LiveStreamPublisherManager(context, managerMetrics);
        context.blockMessaging()
                .registerBlockNotificationHandler(
                        publisherManager, false, LiveStreamPublisherManager.class.getSimpleName());
    }

    @Override
    public void stop() {
        // @todo clean up the publisher manager and handlers
    }

    /**
     * Initialize all metrics for the publisher service plugin.
     *
     * @param metrics the metrics provider
     */
    private void initMetrics(@NonNull final Metrics metrics) {
        // Initialize Handler and Manager metrics.
        // We create these here to keep the cardinality under control.
        // The Handler metrics require labels to make the metrics useful in most
        // cases, for now they're just not very useful.
        handlerMetrics = PublisherHandler.MetricsHolder.createMetrics(metrics);
        // There's only one manager, so we don't generally need labels for these.
        managerMetrics = LiveStreamPublisherManager.MetricsHolder.createMetrics(metrics);
    }

    /**
     * {@inheritDoc}
     */
    @NonNull
    @Override
    public List<Class<? extends Record>> configDataTypes() {
        return List.of(PublisherConfig.class);
    }

    /* "dead" methods required by the interface */
    @Override
    public Pipeline<? super PublishStreamRequest> publishBlockStream(
            final Pipeline<? super PublishStreamResponse> replies) {
        // do nothing; in order to use unparsed alternatives we must override
        // open instead of this method.
        return null;
    }
}
