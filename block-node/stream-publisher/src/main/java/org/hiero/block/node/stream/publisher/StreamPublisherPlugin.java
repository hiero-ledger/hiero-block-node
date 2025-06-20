package org.hiero.block.node.stream.publisher;

import com.hedera.pbj.runtime.grpc.Pipeline;
import com.hedera.pbj.runtime.grpc.Pipelines;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.metrics.api.Counter;
import com.swirlds.metrics.api.LongGauge;
import com.swirlds.metrics.api.Metrics;
import java.util.List;
import java.util.Objects;
import org.hiero.block.api.BlockStreamPublishServiceInterface;
import org.hiero.block.api.PublishStreamRequest;
import org.hiero.block.api.PublishStreamResponse;
import org.hiero.block.internal.PublishStreamRequestUnparsed;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.block.node.spi.ServiceBuilder;
import org.hiero.block.node.spi.blockmessaging.BlockNotificationHandler;
import org.hiero.block.node.spi.blockmessaging.PersistedNotification;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import edu.umd.cs.findbugs.annotations.NonNull;

public class StreamPublisherPlugin implements BlockNodePlugin, BlockStreamPublishServiceInterface,
        BlockNotificationHandler {
    /** The logger for this class. */
    private final System.Logger LOGGER = System.getLogger(getClass().getName());

    /** The block node context, for access to core facilities. */
    private BlockNodeContext context;
    /** The configuration for the publisher */
    private PublisherConfig publisherConfig;
    /** The publisher block manager, which connects handlers to the messaging facility. */
    private StreamPublisherManager publisherManager;

    // Metrics fields
    /** The metrics used by the publisher Handlers. */
    private PublisherHandler.MetricsHolder handlerMetrics;
    /** The metrics used by the publisher Manager. */
    private StreamPublisherManager.MetricsHolder managerMetrics;
    /** The number of live block items messaged to the messaging service. */
    private Counter liveBlockItemsMessaged;
    /** The number of producers publishing block items. */
    private LongGauge numberOfProducers;

    @Override
    @NonNull
    public Pipeline<? super Bytes> open(@NonNull final Method method, @NonNull final RequestOptions options,
            @NonNull final Pipeline<? super Bytes> replies) {
        final BlockStreamPublishServiceMethod blockStreamPublisherServiceMethod =
                (BlockStreamPublishServiceMethod) method;
        return switch (blockStreamPublisherServiceMethod) {
            case publishBlockStream ->
                Pipelines.<PublishStreamRequestUnparsed, PublishStreamResponse>bidiStreaming()
                        .mapRequest(PublishStreamRequestUnparsed.PROTOBUF::parse)
                        .method(this::initatePublisherHandler)
                        .respondTo(replies)
                        .mapResponse(PublishStreamResponse.PROTOBUF::toBytes)
                        .build();
        };
    }

    public Pipeline<? super PublishStreamRequestUnparsed> initatePublisherHandler(
            final Pipeline<? super PublishStreamResponse> replies) {
        return publisherManager.addHandler(replies, handlerMetrics);
    }

    @Override
    public void init(final BlockNodeContext context, final ServiceBuilder serviceBuilder) {
        Objects.requireNonNull(context);
        Objects.requireNonNull(serviceBuilder);
        this.context = context;
        // load the publisher config
        publisherConfig = context.configuration().getConfigData(PublisherConfig.class);
        // Initialize plugin metrics
        initMetrics(context.metrics());
        // register us as a service
        serviceBuilder.registerGrpcService(this);
        // register us as a block notification handler
        context.blockMessaging()
                .registerBlockNotificationHandler(this, false, StreamPublisherPlugin.class.getSimpleName());
        publisherManager = new StreamPublisherManager(context, managerMetrics);
    }

    @Override
    public void start() {
        // Start the helidon service instance
    }

    @Override
    public void stop() {
        // End the helidon service instance
    }

    @Override
    public void handleVerification(final VerificationNotification notification) {
        // Need to check, but should only handle the "failed" case.
        // on success we should probably do nothing.
    }

    @Override
    public void handlePersisted(final PersistedNotification notification) {
        // update the latest known verified and persisted block number
        // in the publisher block manager, and signal all handlers to send acknowledgements
    }

    /**
     * Initialize all metrics for the publisher service plugin.
     *
     * @param metrics the metrics provider
     */
    private void initMetrics(Metrics metrics) {
        // Initialize Handler and Manager metrics.
        // We create these here to keep the cardinality under control.
        // The Handler metrics require labels to make the metrics useful in most
        // cases, for now they're just not very useful.
        handlerMetrics = PublisherHandler.MetricsHolder.createMetrics(context.metrics());
        // There's only one manager, so we don't generally need labels for these.
        managerMetrics = StreamPublisherManager.MetricsHolder.createMetrics(context.metrics());
    }

    /**
     * {@inheritDoc}
     */
    @NonNull
    @Override
    public List<Class<? extends Record>> configDataTypes() {
        return List.of(PublisherConfig.class);
    }

    /*-------------------- "dead" methods required by the interface --------------------*/
    @Override
    public Pipeline<? super PublishStreamRequest> publishBlockStream(
            final Pipeline<? super PublishStreamResponse> replies) {
        // do nothing; in order to use unparsed alternatives we must override
        // open instead of this method.
        return null;
    }

}
