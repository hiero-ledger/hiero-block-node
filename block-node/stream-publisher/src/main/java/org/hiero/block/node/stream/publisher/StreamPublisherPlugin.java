package org.hiero.block.node.stream.publisher;

import static java.lang.System.Logger.Level.INFO;

import com.hedera.pbj.runtime.grpc.Pipeline;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.metrics.api.Counter;
import com.swirlds.metrics.api.LongGauge;
import com.swirlds.metrics.api.Metrics;
import java.util.List;
import java.util.Objects;
import org.hiero.block.api.BlockStreamPublishServiceInterface;
import org.hiero.block.api.PublishStreamRequest;
import org.hiero.block.api.PublishStreamResponse;
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
    private LongGauge highestIncomingBlockNumber;
    /** The number of producers publishing block items. */
    private LongGauge numberOfProducers;
    /** The number of block-ack messages sent. */
    private Counter blocksAckSent;
    /** The latest block number for which an ack was sent. */
    private LongGauge latestBlockNumberAckSent;
    /** The number of stream errors. */
    private Counter streamErrors;
    /** The number of block-skip messages sent. */
    private Counter blocksSkipsSent;
    /** The number of block-resend messages sent. */
    private Counter blocksResendSent;
    /** The number of block end-of-stream messages sent. */
    private Counter blocksEndOfStreamSent;
    /** The number of block end-of-stream messages received. */
    private Counter blocksEndOfStreamReceived;

    @Override
    public Pipeline<? super PublishStreamRequest> publishBlockStream(
            final Pipeline<? super PublishStreamResponse> replies) {
        return null;
    }

    @Override
    @NonNull
    public Pipeline<? super Bytes> open(@NonNull final Method method, @NonNull final RequestOptions options,
            @NonNull final Pipeline<? super Bytes> replies) {
        return BlockStreamPublishServiceInterface.super.open(method, options, replies);
    }

    @Override
    public void init(final BlockNodeContext context, final ServiceBuilder serviceBuilder) {
        Objects.requireNonNull(context);
        Objects.requireNonNull(serviceBuilder);

        this.context = context;
        // load the publisher config
        publisherConfig = context.configuration().getConfigData(PublisherConfig.class);
        // get type of publisher to use and log it
        LOGGER.log(INFO, "Using publisher type: {0}", publisherConfig.type());

        // Initialize metrics
        initMetrics(context.metrics());
        // register us as a service
        serviceBuilder.registerGrpcService(this);
        // register us as a block notification handler
        context.blockMessaging()
                .registerBlockNotificationHandler(this, false, StreamPublisherPlugin.class.getSimpleName());
    }

    @Override
    public void start() {
    }

    @Override
    public void stop() {
    }

    @Override
    public void handleVerification(final VerificationNotification notification) {
    }

    @Override
    public void handlePersisted(final PersistedNotification notification) {
    }

    /**
     * Initialize all metrics for the publisher service plugin.
     *
     * @param metrics the metrics provider
     */
    private void initMetrics(Metrics metrics) {
        // Initialize counters
        liveBlockItemsReceived =
                metrics.getOrCreate(new Counter.Config(METRICS_CATEGORY, "publisher_block_items_received")
                        .withDescription("Live block items received (sum over all publishers)"));

        liveBlockItemsMessaged =
                metrics.getOrCreate(new Counter.Config(METRICS_CATEGORY, "publisher_block_items_messaged")
                        .withDescription("Live block items messaged to the messaging service"));

        blocksAckSent = metrics.getOrCreate(new Counter.Config(METRICS_CATEGORY, "publisher_blocks_ack_sent")
                .withDescription("Block‑ack messages sent"));

        streamErrors = metrics.getOrCreate(new Counter.Config(METRICS_CATEGORY, "publisher_stream_errors")
                .withDescription("Publisher connection streams that end in an error"));

        blocksSkipsSent = metrics.getOrCreate(new Counter.Config(METRICS_CATEGORY, "publisher_blocks_skips_sent")
                .withDescription("Block‑ack skips sent"));

        blocksResendSent = metrics.getOrCreate(new Counter.Config(METRICS_CATEGORY, "publisher_blocks_resend_sent")
                .withDescription("Block Resend messages sent"));

        blocksEndOfStreamSent =
                metrics.getOrCreate(new Counter.Config(METRICS_CATEGORY, "publisher_block_endofstream_sent")
                        .withDescription("Block End-of-Stream messages sent"));

        blocksEndOfStreamReceived =
                metrics.getOrCreate(new Counter.Config(METRICS_CATEGORY, "publisher_block_endstream_received")
                        .withDescription("Block End-Stream messages received"));

        // Initialize gauges
        lowestBlockNumberInbound =
                metrics.getOrCreate(new LongGauge.Config(METRICS_CATEGORY, "publisher_lowest_block_number_inbound")
                        .withDescription("Oldest inbound block number"));

        currentBlockNumberInbound =
                metrics.getOrCreate(new LongGauge.Config(METRICS_CATEGORY, "publisher_current_block_number_inbound")
                        .withDescription("Current block number from primary publisher"));

        highestIncomingBlockNumber =
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

}
