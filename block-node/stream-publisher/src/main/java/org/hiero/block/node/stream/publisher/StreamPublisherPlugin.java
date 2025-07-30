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

/**
 * A plugin for the block node.
 * <p>
 * This plugin implements the publishBlockStream API that allows publishers to
 * send block streams to the block node.
 * <p>
 * This plugin separates the responsibility for handling the publish protocol
 * for each publisher into a separate handler, all of which are managed by a
 * `PublisherManager`. The handler is responsible for implementing the single-
 * publisher protocol, accepting batches of block items, sending
 * acknowledgements, interpreting out-of-order block headers, and generally
 * ensuring that one specific publisher connection implements the defined
 * protocol correctly.
 * <p>
 * The publisher manager is responsible for keeping track of which block is
 * currently streaming, which (if any) subsequent blocks are being streamed
 * in advance by other publishers, and also manages notification handling so
 * that messaging sends one notification and, if needed, all handlers can send
 * appropriate responses to their publishers.
 *
 */
public final class StreamPublisherPlugin implements BlockNodePlugin, BlockStreamPublishServiceInterface {
    // @todo(1413) add proper logging usage to this class.
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
    private LiveStreamPublisherManager.MetricsHolder managerMetrics;
    /** The number of live block items messaged to the messaging service. */
    private Counter liveBlockItemsMessaged;
    /** The number of producers publishing block items. */
    private LongGauge numberOfProducers;

    /**
     * {@inheritDoc}
     * <p>
     * We must override this method to provide a custom implementation that
     * uses the unparsed request type, which allows us to handle the request
     * without needing to fully parse the individual {@code BlockItem}s.<br/>
     * This performance optimization reduces publish-to-subscribe latency by
     * roughly 90%, and reduces GC overhead substantially.
     */
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

    @Override
    public void init(@NonNull final BlockNodeContext context, @NonNull final ServiceBuilder serviceBuilder) {
        this.context = Objects.requireNonNull(context);
        // load the publisher config
        publisherConfig = context.configuration().getConfigData(PublisherConfig.class);
        // register us as a service, we need to register the gRPC service in
        // the init method, otherwise the server will be started and we will not
        // have registered at all
        serviceBuilder.registerGrpcService(this);
    }

    @Override
    public void start() {
        // Initialize plugin metrics
        initMetrics(context.metrics());
        // Initialize the publisher manager
        publisherManager = new LiveStreamPublisherManager(context, managerMetrics);
        // register the manager as a notification handler
        context.blockMessaging()
                .registerBlockNotificationHandler(
                        publisherManager, false, LiveStreamPublisherManager.class.getSimpleName());
    }

    @Override
    public void stop() {
        // @todo(1416) clean up the publisher manager and handlers
    }

    @Override
    @NonNull
    public List<Class<? extends Record>> configDataTypes() {
        return List.of(PublisherConfig.class);
    }

    /**
     * This method is called when a new publisher handler is created.
     * <p>
     * A new handler is created when a new publisher connects to the block node.
     * @param replies the pipeline to which the replies will be sent
     * @return a new, valid, fully initialized publisher handler
     */
    private Pipeline<? super PublishStreamRequestUnparsed> initiatePublisherHandler(
            @NonNull final Pipeline<? super PublishStreamResponse> replies) {
        return publisherManager.addHandler(replies, handlerMetrics);
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

    // ==== "dead" methods required by the interface ===========================
    @Override
    public Pipeline<? super PublishStreamRequest> publishBlockStream(
            final Pipeline<? super PublishStreamResponse> replies) {
        // do nothing; in order to use unparsed alternatives we must override
        // open instead of this method.
        return null;
    }
}
