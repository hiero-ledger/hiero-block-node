// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.server.consumer;

import static com.hedera.block.server.metrics.BlockNodeMetricTypes.Gauge.CurrentBlockNumberOutbound;
import static java.lang.System.Logger.Level.DEBUG;

import com.hedera.block.server.events.BlockNodeEventHandler;
import com.hedera.block.server.events.LivenessCalculator;
import com.hedera.block.server.events.ObjectEvent;
import com.hedera.block.server.mediator.Poller;
import com.hedera.block.server.mediator.SubscriptionHandler;
import com.hedera.block.server.metrics.BlockNodeMetricTypes;
import com.hedera.block.server.metrics.MetricsService;
import com.hedera.hapi.block.BlockItemSetUnparsed;
import com.hedera.hapi.block.BlockItemUnparsed;
import com.hedera.hapi.block.SubscribeStreamResponseUnparsed;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.grpc.Pipeline;
import com.swirlds.config.api.Configuration;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

public class PollConsumerStreamResponseObserver
        implements Runnable, BlockNodeEventHandler<ObjectEvent<List<BlockItemUnparsed>>> {

    private final System.Logger LOGGER = System.getLogger(getClass().getName());

    private final MetricsService metricsService;
    private final Pipeline<? super SubscribeStreamResponseUnparsed> helidonConsumerObserver;
    private final SubscriptionHandler<List<BlockItemUnparsed>> subscriptionHandler;

    private final AtomicBoolean isResponsePermitted = new AtomicBoolean(true);

    private final AtomicBoolean streamStarted = new AtomicBoolean(false);

    private LivenessCalculator livenessCalculator;
    private final Poller<ObjectEvent<List<BlockItemUnparsed>>> poller;

    public PollConsumerStreamResponseObserver(
            @NonNull final SubscriptionHandler<List<BlockItemUnparsed>> subscriptionHandler,
            @NonNull final Pipeline<? super SubscribeStreamResponseUnparsed> helidonConsumerObserver,
            @NonNull final MetricsService metricsService,
            @NonNull final Configuration configuration) {

        this.metricsService = Objects.requireNonNull(metricsService);
        this.helidonConsumerObserver = Objects.requireNonNull(helidonConsumerObserver);
        this.subscriptionHandler = Objects.requireNonNull(subscriptionHandler);
        this.poller = subscriptionHandler.subscribePoller(this);
    }

    @Override
    public void run() {
        while (true) {
            try {
                ObjectEvent<List<BlockItemUnparsed>> event = poller.poll();
                if (event != null) {
                    if (isResponsePermitted.get()) {
                        final List<BlockItemUnparsed> blockItems = event.get();
                        send(blockItems);
                    }
                    refreshLiveness();
                }
            } catch (Exception e) {
                subscriptionHandler.unsubscribePoller(this);
                throw new RuntimeException(e);
            }
            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
                subscriptionHandler.unsubscribePoller(this);
                throw new RuntimeException(e);
            }
        }
    }

    private void refreshLiveness() {
        if (livenessCalculator != null) {
            livenessCalculator.refresh();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isTimeoutExpired() {
        if (livenessCalculator != null) {
            subscriptionHandler.unsubscribePoller(this);
            return livenessCalculator.isTimeoutExpired();
        }

        return false;
    }

    @Override
    public void unsubscribe() {
        subscriptionHandler.unsubscribePoller(this);
    }

    @Override
    public void onEvent(ObjectEvent<List<BlockItemUnparsed>> event, long sequence, boolean endOfBatch)
            throws Exception {
        throw new UnsupportedOperationException("Not implemented");
    }

    private void send(@NonNull final List<BlockItemUnparsed> blockItems) throws ParseException {

        // Only start sending BlockItems after we've reached
        // the beginning of a block.
        final BlockItemUnparsed firstBlockItem = blockItems.getFirst();
        if (!streamStarted.get() && firstBlockItem.hasBlockHeader()) {
            streamStarted.set(true);
        }

        if (streamStarted.get()) {
            if (firstBlockItem.hasBlockHeader()) {
                long blockNumber =
                        BlockHeader.PROTOBUF.parse(firstBlockItem.blockHeader()).number();
                if (LOGGER.isLoggable(DEBUG)) {
                    LOGGER.log(DEBUG, "{0} sending block: {1}", Thread.currentThread(), blockNumber);
                }
                metricsService.get(CurrentBlockNumberOutbound).set(blockNumber);
            }

            metricsService
                    .get(BlockNodeMetricTypes.Counter.LiveBlockItemsConsumed)
                    .add(blockItems.size());

            final SubscribeStreamResponseUnparsed subscribeStreamResponse = SubscribeStreamResponseUnparsed.newBuilder()
                    .blockItems(BlockItemSetUnparsed.newBuilder()
                            .blockItems(blockItems)
                            .build())
                    .build();

            // Send the response down through Helidon
            helidonConsumerObserver.onNext(subscribeStreamResponse);
        }
    }
}
