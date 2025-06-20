package org.hiero.block.node.stream.publisher;

import static org.hiero.block.node.spi.BlockNodePlugin.METRICS_CATEGORY;

import com.hedera.pbj.runtime.grpc.Pipeline;
import com.swirlds.metrics.api.Counter;
import com.swirlds.metrics.api.Metrics;
import java.util.Objects;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.TransferQueue;
import org.hiero.block.api.PublishStreamResponse;
import org.hiero.block.internal.BlockItemSetUnparsed;
import org.hiero.block.internal.PublishStreamRequestUnparsed;

public class PublisherHandler implements Pipeline<PublishStreamRequestUnparsed> {
    private final Pipeline<? super PublishStreamResponse> replies;
    private final MetricsHolder metrics;
    private final StreamPublisherManager publisherManager;
    private final TransferQueue<BlockItemSetUnparsed> blockItemsQueue;
    private final long handlerId;

    /**
     * Initialize a new publisher handler.
     *
     * @param nextId the next handler ID to use
     * @param replyPipeline the pipeline to send replies to
     * @param handlerMetrics the metrics for this handler
     * @param manager the publisher manager that manages this handler
     * @param transferQueue the queue for transferring block items to the manager
     */
    public PublisherHandler(final long nextId, final Pipeline<? super PublishStreamResponse> replyPipeline,
            final MetricsHolder handlerMetrics,
            final StreamPublisherManager manager,
            final TransferQueue<BlockItemSetUnparsed> transferQueue) {
        handlerId = nextId;
        replies = Objects.requireNonNull(replyPipeline);
        metrics = Objects.requireNonNull(handlerMetrics);
        publisherManager = Objects.requireNonNull(manager);
        blockItemsQueue = Objects.requireNonNull(transferQueue);
    }

    @Override
    public void onError(final Throwable throwable) {
        // This is a "terminal" method, called when an _unrecoverable_ error
        // occurs. No other methods will be called by the Helidon layer after this.
        if(publisherManager != null) {
            publisherManager.removeHandler(handlerId);
        }
    }

    @Override
    public void onComplete() {
        // This is mostly a cleanup method, called when the stream is complete
        // and `onNext` will not be called again.
        if(publisherManager != null) {
            publisherManager.removeHandler(handlerId);
        }
    }

    @Override
    public void clientEndStreamReceived() {
        // called when the _gRPC layer_ receives an end stream from the client.
        // THIS IS NOT the same as the `EndStream` message in the API.
        if(publisherManager != null) {
            publisherManager.removeHandler(handlerId);
        }
    }

    @Override
    public void onSubscribe(final Subscription subscription) {
        // Check javadoc, this "starts" the subscription for this handler.
        // mostly we need to call subscription.request to start the flow of items.
    }

    @Override
    public void onNext(final PublishStreamRequestUnparsed item) {
    }

    /**
     * Metrics for tracking publisher handler activity:
     * liveBlockItemsReceived - Count of live block items received from a producer
     * blockAcknowledgementsSent - Count of acknowledgements sent
     * streamErrors - Count of stream errors
     * blockSkipsSent - Count of block skip responses
     * blockResendsSent - Count of block resend responses
     * endOfStreamsSent- Count of end of stream responses (should always be at most 1 per stream)
     * endStreamsReceived- Count of end streams received (should always be at most 1 per stream)
     */
    public record MetricsHolder(
            Counter liveBlockItemsReceived,
            Counter blockAcknowledgementsSent,
            Counter streamErrors,
            Counter blockSkipsSent,
            Counter blockResendsSent,
            Counter endOfStreamsSent,
            Counter endStreamsReceived
    ) {
        static MetricsHolder createMetrics(Metrics metrics) {
            Counter liveBlockItemsReceived = metrics.getOrCreate(
                    new Counter.Config(METRICS_CATEGORY, "publisher_block_items_received")
                            .withDescription("Live block items received"));
            Counter blockAcknowledgementsSent = metrics.getOrCreate(
                    new Counter.Config(METRICS_CATEGORY, "publisher_blocks_ack_sent")
                            .withDescription("Block‑ack messages sent"));
            Counter streamErrors = metrics.getOrCreate(
                    new Counter.Config(METRICS_CATEGORY, "publisher_stream_errors")
                            .withDescription("Publisher connection streams that end in an error"));
            Counter blockSkipsSent = metrics.getOrCreate(
                    new Counter.Config(METRICS_CATEGORY, "publisher_blocks_skips_sent")
                            .withDescription("Block‑ack skips sent"));
            Counter blockResendsSent = metrics.getOrCreate(
                    new Counter.Config(METRICS_CATEGORY, "publisher_blocks_resend_sent")
                            .withDescription("Block Resend messages sent"));
            Counter endOfStreamsSent = metrics.getOrCreate(
                    new Counter.Config(METRICS_CATEGORY, "publisher_block_endofstream_sent")
                            .withDescription("Block End-of-Stream messages sent"));
            Counter endStreamsReceived = metrics.getOrCreate(
                    new Counter.Config(METRICS_CATEGORY, "publisher_block_endstream_received")
                            .withDescription("Block End-Stream messages received"));
            return new MetricsHolder(
                    liveBlockItemsReceived,
                    blockAcknowledgementsSent,
                    streamErrors,
                    blockSkipsSent,
                    blockResendsSent,
                    endOfStreamsSent,
                    endStreamsReceived
            );
        }
    }
}
