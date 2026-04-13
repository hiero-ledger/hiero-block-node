// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.stream.publisher;

import static org.hiero.block.node.spi.BlockNodePlugin.METRICS_CATEGORY;

import com.hedera.pbj.runtime.grpc.Pipeline;
import com.hedera.pbj.runtime.grpc.Pipelines;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.List;
import java.util.Objects;
import org.hiero.block.api.BlockStreamPublishServiceInterface;
import org.hiero.block.api.PublishStreamRequest;
import org.hiero.block.api.PublishStreamResponse;
import org.hiero.block.internal.PublishStreamRequestUnparsed;
import org.hiero.block.node.app.config.ServerConfig;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.block.node.spi.ServiceBuilder;
import org.hiero.metrics.LongCounter;
import org.hiero.metrics.LongGauge;
import org.hiero.metrics.core.MetricKey;
import org.hiero.metrics.core.MetricRegistry;

/// A plugin for the block node.
///
/// This plugin implements the publishBlockStream API that allows publishers to
/// send block streams to the block node.
///
/// This plugin separates the responsibility for handling the publish protocol
/// for each publisher into a separate handler, all of which are managed by a
/// `PublisherManager`. The handler is responsible for implementing the single-
/// publisher protocol, accepting batches of block items, sending
/// acknowledgements, interpreting out-of-order block headers, and generally
/// ensuring that one specific publisher connection implements the defined
/// protocol correctly.
///
/// The publisher manager is responsible for keeping track of which block is
/// currently streaming, which (if any) subsequent blocks are being streamed
/// in advance by other publishers, and also manages notification handling so
/// that messaging sends one notification and, if needed, all handlers can send
/// appropriate responses to their publishers.
public final class StreamPublisherPlugin implements BlockNodePlugin, BlockStreamPublishServiceInterface {

    /// Maximum length for the correlation ID header value.
    /// Chosen to accommodate current formats (e.g. `N#-STR#`, `N#-STR#-BLK#-REQ#`) and
    /// future formats such as prefixed UUIDs, while bounding log line growth.
    static final int MAX_CORRELATION_ID_LENGTH = 64;

    // Metric key constants
    public static final MetricKey<LongCounter> METRIC_PUBLISHER_BLOCK_ITEMS_RECEIVED =
            MetricKey.of("publisher_block_items_received", LongCounter.class).addCategory(METRICS_CATEGORY);
    public static final MetricKey<LongCounter> METRIC_PUBLISHER_BLOCKS_ACK_SENT =
            MetricKey.of("publisher_blocks_ack_sent", LongCounter.class).addCategory(METRICS_CATEGORY);
    public static final MetricKey<LongCounter> METRIC_PUBLISHER_STREAM_SETS_DROPPED =
            MetricKey.of("publisher_stream_sets_dropped", LongCounter.class).addCategory(METRICS_CATEGORY);
    public static final MetricKey<LongCounter> METRIC_PUBLISHER_STREAM_ERRORS =
            MetricKey.of("publisher_stream_errors", LongCounter.class).addCategory(METRICS_CATEGORY);
    public static final MetricKey<LongCounter> METRIC_PUBLISHER_BLOCKS_SKIPS_SENT =
            MetricKey.of("publisher_blocks_skips_sent", LongCounter.class).addCategory(METRICS_CATEGORY);
    public static final MetricKey<LongCounter> METRIC_PUBLISHER_BLOCKS_RESEND_SENT =
            MetricKey.of("publisher_blocks_resend_sent", LongCounter.class).addCategory(METRICS_CATEGORY);
    public static final MetricKey<LongCounter> METRIC_PUBLISHER_BLOCK_NODE_BEHIND_SENT =
            MetricKey.of("publisher_block_node_behind_sent", LongCounter.class).addCategory(METRICS_CATEGORY);
    public static final MetricKey<LongCounter> METRIC_PUBLISHER_BLOCK_ENDOFSTREAM_SENT =
            MetricKey.of("publisher_block_endofstream_sent", LongCounter.class).addCategory(METRICS_CATEGORY);
    public static final MetricKey<LongCounter> METRIC_PUBLISHER_BLOCK_SEND_RESPONSE_FAILED = MetricKey.of(
                    "publisher_block_send_response_failed", LongCounter.class)
            .addCategory(METRICS_CATEGORY);
    public static final MetricKey<LongCounter> METRIC_PUBLISHER_BLOCK_ENDSTREAM_RECEIVED = MetricKey.of(
                    "publisher_block_endstream_received", LongCounter.class)
            .addCategory(METRICS_CATEGORY);
    public static final MetricKey<LongCounter> METRIC_PUBLISHER_RECEIVE_LATENCY_NS =
            MetricKey.of("publisher_receive_latency_ns", LongCounter.class).addCategory(METRICS_CATEGORY);
    public static final MetricKey<LongCounter> METRIC_PUBLISHER_BLOCK_ITEMS_MESSAGED =
            MetricKey.of("publisher_block_items_messaged", LongCounter.class).addCategory(METRICS_CATEGORY);
    public static final MetricKey<LongCounter> METRIC_PUBLISHER_BLOCK_BATCHES_MESSAGED =
            MetricKey.of("publisher_block_batches_messaged", LongCounter.class).addCategory(METRICS_CATEGORY);
    public static final MetricKey<LongCounter> METRIC_PUBLISHER_BLOCKS_CLOSED_COMPLETE =
            MetricKey.of("publisher_blocks_closed_complete", LongCounter.class).addCategory(METRICS_CATEGORY);
    public static final MetricKey<LongGauge> METRIC_PUBLISHER_OPEN_CONNECTIONS =
            MetricKey.of("publisher_open_connections", LongGauge.class).addCategory(METRICS_CATEGORY);
    public static final MetricKey<LongGauge> METRIC_PUBLISHER_LOWEST_BLOCK_NUMBER_INBOUND = MetricKey.of(
                    "publisher_lowest_block_number_inbound", LongGauge.class)
            .addCategory(METRICS_CATEGORY);
    public static final MetricKey<LongGauge> METRIC_PUBLISHER_HIGHEST_BLOCK_NUMBER_INBOUND = MetricKey.of(
                    "publisher_highest_block_number_inbound", LongGauge.class)
            .addCategory(METRICS_CATEGORY);
    public static final MetricKey<LongGauge> METRIC_PUBLISHER_LATEST_BLOCK_NUMBER_ACKNOWLEDGED = MetricKey.of(
                    "publisher_latest_block_number_acknowledged", LongGauge.class)
            .addCategory(METRICS_CATEGORY);
    public static final MetricKey<LongCounter> METRIC_PUBLISHER_STALL_TIMEOUTS_SENT =
            MetricKey.of("publisher_stall_timeouts_sent", LongCounter.class).addCategory(METRICS_CATEGORY);

    /// The logger for this class.
    private static final System.Logger LOGGER = System.getLogger(StreamPublisherPlugin.class.getName());

    /// The block node context, for access to core facilities.
    private BlockNodeContext context;
    /// The publisher block manager, which connects handlers to the messaging facility.
    private StreamPublisherManager publisherManager;

    // Metrics fields
    /// The metrics used by the publisher Handlers.
    private PublisherHandler.MetricsHolder handlerMetrics;
    /// The metrics used by the publisher Manager.
    private LiveStreamPublisherManager.MetricsHolder managerMetrics;
    /// The number of live block items messaged to the messaging service.
    private LongCounter liveBlockItemsMessaged;
    /// The number of producers publishing block items.
    private LongGauge numberOfProducers;

    /// {@inheritDoc}
    ///
    /// We must override this method to provide a custom implementation that
    /// uses the unparsed request type, which allows us to handle the request
    /// without needing to fully parse the individual `BlockItem`s.
    /// This performance optimization reduces publish-to-subscribe latency by
    /// roughly 90%, and reduces GC overhead substantially.
    @Override
    @NonNull
    public Pipeline<? super Bytes> open(
            @NonNull final Method method,
            @NonNull final RequestOptions options,
            @NonNull final Pipeline<? super Bytes> replies) {
        final BlockStreamPublishServiceMethod blockStreamPublisherServiceMethod =
                (BlockStreamPublishServiceMethod) method;

        final int maxMessageSize =
                context.configuration().getConfigData(ServerConfig.class).maxMessageSizeBytes();

        final String rawCorrelationId = options.metadata().getOrDefault("hiero-correlation-id", "");
        final String correlationId = truncateCorrelationId(rawCorrelationId);

        return switch (blockStreamPublisherServiceMethod) {
            case publishBlockStream ->
                Pipelines.<PublishStreamRequestUnparsed, PublishStreamResponse>bidiStreaming()
                        .mapRequest(
                                bytes -> PublishStreamRequestUnparsed.PROTOBUF.parse(
                                        bytes.toReadableSequentialData(), // input data
                                        false, // strictMode
                                        true, // parseUnknownFields
                                        maxMessageSize / 8,
                                        maxMessageSize) // maxDepth
                                )
                        .method(r -> initiatePublisherHandler(r, correlationId))
                        .respondTo(replies)
                        .mapResponse(PublishStreamResponse.PROTOBUF::toBytes)
                        .build();
        };
    }

    @Override
    public void init(@NonNull final BlockNodeContext context, @NonNull final ServiceBuilder serviceBuilder) {
        this.context = Objects.requireNonNull(context);
        // register us as a service, we need to register the gRPC service in
        // the init method, otherwise the server will be started and we will not
        // have registered at all
        serviceBuilder.registerGrpcService(this);
    }

    @Override
    public void start() {
        // Initialize plugin metrics
        initMetrics(context.metricRegistry());
        // Initialize the publisher manager
        publisherManager = new LiveStreamPublisherManager(context, managerMetrics);
        // register the manager as a notification handler
        context.blockMessaging()
                .registerBlockNotificationHandler(
                        publisherManager, false, LiveStreamPublisherManager.class.getSimpleName());
    }

    @Override
    public void stop() {
        context.blockMessaging().unregisterBlockNotificationHandler(publisherManager);
        publisherManager.shutdown();
    }

    @Override
    @NonNull
    public List<Class<? extends Record>> configDataTypes() {
        return List.of(PublisherConfig.class);
    }

    /// This method is called when a new publisher handler is created.
    ///
    /// A new handler is created when a new publisher connects to the block node.
    /// @param replies the pipeline to which the replies will be sent
    /// @param correlationId the correlation ID from the gRPC `hiero-correlation-id` header, or empty if absent
    /// @return a new, valid, fully initialized publisher handler
    private Pipeline<? super PublishStreamRequestUnparsed> initiatePublisherHandler(
            @NonNull final Pipeline<? super PublishStreamResponse> replies, final String correlationId) {
        return publisherManager.addHandler(replies, handlerMetrics, correlationId);
    }

    /// Truncates the given correlation ID to [#MAX_CORRELATION_ID_LENGTH] characters.
    ///
    /// Protects against oversized header values that could inflate every log line.
    /// If the value exceeds the limit, it is truncated and a warning is logged.
    ///
    /// @param correlationId the raw value from the gRPC header
    /// @return the value unchanged if within the limit, or truncated to [#MAX_CORRELATION_ID_LENGTH]
    static String truncateCorrelationId(final String correlationId) {
        if (correlationId == null) {
            return "";
        }
        if (correlationId.length() > MAX_CORRELATION_ID_LENGTH) {
            LOGGER.log(
                    System.Logger.Level.WARNING,
                    "Received hiero-correlation-id header of {0} characters exceeds limit of {1} and will be truncated: {2}",
                    correlationId.length(),
                    MAX_CORRELATION_ID_LENGTH,
                    correlationId.substring(0, MAX_CORRELATION_ID_LENGTH));
            return correlationId.substring(0, MAX_CORRELATION_ID_LENGTH);
        }
        return correlationId;
    }

    /// Initialize all metrics for the publisher service plugin.
    ///
    /// @param metrics the metrics provider
    private void initMetrics(@NonNull final MetricRegistry metrics) {
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
