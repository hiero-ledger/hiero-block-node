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

    // Metrics fields
    /** The number of live block items received from a producer. */
    private Counter liveBlockItemsReceived;
    /** The number of live block items messaged to the messaging service. */
    private Counter liveBlockItemsMessaged;
    /** The lowest incoming block number. */
    private LongGauge lowestBlockNumberInbound;
    /** The latest incoming block number. */
    private LongGauge currentBlockNumberInbound;
    /** The highest incoming block number. */
    private LongGauge highestBlockNumberInbound;
    /** The number of producers publishing block items. */
    private LongGauge numberOfProducers;
    /** The number of block-ack messages sent. */
    private Counter blockAcksSent;
    /** The latest block number for which an ack was sent. */
    private LongGauge latestBlockNumberAckSent;
    /** The number of stream errors. */
    private Counter streamErrors;
    /** The number of block-skip messages sent. */
    private Counter blockSkipsSent;
    /** The number of block-resend messages sent. */
    private Counter blockResendsSent;
    /** The number of block end-of-stream messages sent. */
    private Counter blockEndOfStreamsSent;
    /** The number of block end-of-stream messages received. */
    private Counter blockEndStreamsReceived;

    private PublisherHandler createHandler() {
        // Create a new handler for a publisher, just set the metrics (because we have
        // so blasted many) set the block manager reference, and return it.
        // The caller will set the replies pipeline and connect the handler to
        // the pipeline of requests.
        return null; // Temporary placeholder
    }

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
                        .method(this::initatePublisher)
                        .respondTo(replies)
                        .mapResponse(PublishStreamResponse.PROTOBUF::toBytes)
                        .build();
        };
    }

    public Pipeline<? super PublishStreamRequestUnparsed> initatePublisher(
            final Pipeline<? super PublishStreamResponse> replies) {
        // Create a handler for this publisher, and connect the replies pipeline
        // for it to send a stream of responses to the publisher.
        return null; // Temporary placeholder
    }

    @Override
    public void init(final BlockNodeContext context, final ServiceBuilder serviceBuilder) {
        Objects.requireNonNull(context);
        Objects.requireNonNull(serviceBuilder);

        this.context = context;
        // load the publisher config
        publisherConfig = context.configuration().getConfigData(PublisherConfig.class);
        // Initialize metrics
        initMetrics(context.metrics());
        // register us as a service
        serviceBuilder.registerGrpcService(this);
        // register us as a block notification handler
        context.blockMessaging()
                .registerBlockNotificationHandler(this, false, StreamPublisherPlugin.class.getSimpleName());
        // Create a publisher block manager
    }

    @Override
    public void start() {
        // Open the helidon service instance
    }

    @Override
    public void stop() {
        // end the helidon service instance
    }

    @Override
    public void handleVerification(final VerificationNotification notification) {
        // Need to check, but should only handle the "failed" case.
        // on success we should generally do nothing.
    }

    @Override
    public void handlePersisted(final PersistedNotification notification) {
        // update the latest known verified and persisted block number
        // in the publisher block manager
    }

    /**
     * Initialize all metrics for the publisher service plugin.
     *
     * @param metrics the metrics provider
     */
    // Note, some of these metrics are not sensible without labels to indicate _which_
    // publisher is setting a value, which requires labels.
    private void initMetrics(Metrics metrics) {
        // Initialize counters
        liveBlockItemsReceived =
                metrics.getOrCreate(new Counter.Config(METRICS_CATEGORY, "publisher_block_items_received")
                        .withDescription("Live block items received (sum over all publishers)"));
        liveBlockItemsMessaged =
                metrics.getOrCreate(new Counter.Config(METRICS_CATEGORY, "publisher_block_items_messaged")
                        .withDescription("Live block items messaged to the messaging service"));
        blockAcksSent = metrics.getOrCreate(new Counter.Config(METRICS_CATEGORY, "publisher_blocks_ack_sent")
                .withDescription("Block‑ack messages sent"));
        streamErrors = metrics.getOrCreate(new Counter.Config(METRICS_CATEGORY, "publisher_stream_errors")
                .withDescription("Publisher connection streams that end in an error"));
        blockSkipsSent = metrics.getOrCreate(new Counter.Config(METRICS_CATEGORY, "publisher_blocks_skips_sent")
                .withDescription("Block‑ack skips sent"));
        blockResendsSent = metrics.getOrCreate(new Counter.Config(METRICS_CATEGORY, "publisher_blocks_resend_sent")
                .withDescription("Block Resend messages sent"));
        blockEndOfStreamsSent =
                metrics.getOrCreate(new Counter.Config(METRICS_CATEGORY, "publisher_block_endofstream_sent")
                        .withDescription("Block End-of-Stream messages sent"));
        blockEndStreamsReceived =
                metrics.getOrCreate(new Counter.Config(METRICS_CATEGORY, "publisher_block_endstream_received")
                        .withDescription("Block End-Stream messages received"));
        // Initialize gauges
        lowestBlockNumberInbound =
                metrics.getOrCreate(new LongGauge.Config(METRICS_CATEGORY, "publisher_lowest_block_number_inbound")
                        .withDescription("Oldest inbound block number"));
        currentBlockNumberInbound =
                metrics.getOrCreate(new LongGauge.Config(METRICS_CATEGORY, "publisher_current_block_number_inbound")
                        .withDescription("Current block number from primary publisher"));
        highestBlockNumberInbound =
                metrics.getOrCreate(new LongGauge.Config(METRICS_CATEGORY, "publisher_highest_block_number_inbound")
                        .withDescription("Newest inbound block number"));
        numberOfProducers = metrics.getOrCreate(new LongGauge.Config(METRICS_CATEGORY, "publisher_open_connections")
                .withDescription("Connected publishers"));
        latestBlockNumberAckSent =
                metrics.getOrCreate(new LongGauge.Config(METRICS_CATEGORY, "publisher_latest_block_number_ack_sent")
                        .withDescription("Latest Block Number Ack Sent from Publisher"));
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
