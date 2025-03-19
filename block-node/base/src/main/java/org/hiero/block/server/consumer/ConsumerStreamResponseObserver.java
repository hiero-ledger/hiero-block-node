// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.consumer;

import static java.lang.System.Logger.Level.TRACE;
import static org.hiero.block.server.metrics.BlockNodeMetricTypes.Counter.LiveBlockItemsConsumed;
import static org.hiero.block.server.metrics.BlockNodeMetricTypes.Gauge.CurrentBlockNumberOutbound;

import com.hedera.hapi.block.BlockItemSetUnparsed;
import com.hedera.hapi.block.BlockItemUnparsed;
import com.hedera.hapi.block.SubscribeStreamResponseCode;
import com.hedera.hapi.block.SubscribeStreamResponseUnparsed;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.grpc.Pipeline;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import org.hiero.block.server.metrics.MetricsService;

/**
 * ConsumerStreamResponseObserver is responsible for sending responses to the downstream client.
 */
public class ConsumerStreamResponseObserver {

    private final System.Logger LOGGER = System.getLogger(getClass().getName());

    private final MetricsService metricsService;
    private final Pipeline<? super SubscribeStreamResponseUnparsed> helidonConsumerObserver;

    private final AtomicBoolean streamStarted = new AtomicBoolean(false);

    /**
     * Constructs a ConsumerStreamResponseObserver.
     *
     * @param helidonConsumerObserver the observer to use to send responses to the consumer
     * @param metricsService - the service responsible for handling metrics
     */
    public ConsumerStreamResponseObserver(
            @NonNull final Pipeline<? super SubscribeStreamResponseUnparsed> helidonConsumerObserver,
            @NonNull final MetricsService metricsService) {

        this.metricsService = Objects.requireNonNull(metricsService);
        this.helidonConsumerObserver = helidonConsumerObserver;
    }

    /**
     * Use this method to send a response code to the downstream client.
     *
     * @param responseCode the response code to send
     */
    public void send(@NonNull final SubscribeStreamResponseCode responseCode) {
        final SubscribeStreamResponseUnparsed subscribeStreamResponse = SubscribeStreamResponseUnparsed.newBuilder()
                .status(responseCode)
                .build();

        helidonConsumerObserver.onNext(subscribeStreamResponse);
        helidonConsumerObserver.onComplete();
    }

    /**
     * Use this method to send a list of BlockItems to the downstream client.
     *
     * @param blockItems the list of BlockItems to send
     * @throws ParseException if there is an error parsing the BlockItems
     */
    public void send(@NonNull final List<BlockItemUnparsed> blockItems) throws ParseException {

        // Only start sending BlockItems after we've reached
        // the beginning of a block.
        final BlockItemUnparsed firstBlockItem = blockItems.getFirst();
        if (!streamStarted.get() && firstBlockItem.hasBlockHeader()) {
            streamStarted.set(true);
        }

        if (streamStarted.get()) {
            if (firstBlockItem.hasBlockHeader()) {
                reportFirstBlock(firstBlockItem);
            }

            metricsService.get(LiveBlockItemsConsumed).add(blockItems.size());

            // Build the response
            final SubscribeStreamResponseUnparsed subscribeStreamResponse = SubscribeStreamResponseUnparsed.newBuilder()
                    .blockItems(BlockItemSetUnparsed.newBuilder()
                            .blockItems(blockItems)
                            .build())
                    .build();

            // Send the response to the client via Helidon
            helidonConsumerObserver.onNext(subscribeStreamResponse);
        }
    }

    private void reportFirstBlock(final BlockItemUnparsed firstBlockItem) throws ParseException {
        long blockNumber =
                BlockHeader.PROTOBUF.parse(firstBlockItem.blockHeader()).number();
        if (LOGGER.isLoggable(TRACE)) {
            LOGGER.log(TRACE, "{0} sending block: {1}", Thread.currentThread(), blockNumber);
        }
        metricsService.get(CurrentBlockNumberOutbound).set(blockNumber);
    }
}
