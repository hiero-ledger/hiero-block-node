// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.server.consumer;

import static com.hedera.block.server.metrics.BlockNodeMetricTypes.Gauge.CurrentBlockNumberOutbound;
import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.ERROR;

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
import com.hedera.pbj.runtime.OneOf;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.grpc.Pipeline;
import com.swirlds.config.api.Configuration;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

public class PullConsumerStreamResponseObserver
        implements Runnable, BlockNodeEventHandler<ObjectEvent<List<BlockItemUnparsed>>> {

    private final System.Logger LOGGER = System.getLogger(getClass().getName());

    private final MetricsService metricsService;
    private final Pipeline<? super SubscribeStreamResponseUnparsed> helidonConsumerObserver;
    private final SubscriptionHandler<List<BlockItemUnparsed>> subscriptionHandler;

    private final AtomicBoolean isResponsePermitted = new AtomicBoolean(true);
    private final PullConsumerStreamResponseObserver.ResponseSender statusResponseSender = new StatusResponseSender();
    private final PullConsumerStreamResponseObserver.ResponseSender blockItemsResponseSender =
            new BlockItemsResponseSender();

    private static final String PROTOCOL_VIOLATION_MESSAGE =
            "Protocol Violation. %s is OneOf type %s but %s is null.\n%s";

    private LivenessCalculator livenessCalculator;
    private final Poller<ObjectEvent<List<BlockItemUnparsed>>> poller;

    public PullConsumerStreamResponseObserver(
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
                        final SubscribeStreamResponseUnparsed subscribeStreamResponse =
                                SubscribeStreamResponseUnparsed.newBuilder()
                                        .blockItems(BlockItemSetUnparsed.newBuilder()
                                                .blockItems(event.get())
                                                .build())
                                        .build();
                        final ResponseSender responseSender = getResponseSender(subscribeStreamResponse);
                        responseSender.send(subscribeStreamResponse);
                    }
                    refreshLiveness();
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void refreshLiveness() {
        if (livenessCalculator != null) {
            livenessCalculator.refresh();
        }
    }

    @NonNull
    private PullConsumerStreamResponseObserver.ResponseSender getResponseSender(
            @NonNull final SubscribeStreamResponseUnparsed subscribeStreamResponse) {

        final OneOf<SubscribeStreamResponseUnparsed.ResponseOneOfType> responseType =
                subscribeStreamResponse.response();
        return switch (responseType.kind()) {
            case STATUS -> {
                // Per the spec, status messages signal
                // the end of processing. Unsubscribe this
                // observer and send a message back to the
                // client
                //                unsubscribe();
                yield statusResponseSender;
            }
            case BLOCK_ITEMS -> blockItemsResponseSender;
                // An unknown response type here is a protocol violation
                // and should shut down the server.
            default -> throw new IllegalArgumentException("Unknown response type: " + responseType.kind());
        };
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isTimeoutExpired() {
        if (livenessCalculator != null) {
            return livenessCalculator.isTimeoutExpired();
        }

        return false;
    }

    @Override
    public void unsubscribe() {
        subscriptionHandler.unsubscribe(this);
    }

    @Override
    public void onEvent(ObjectEvent<List<BlockItemUnparsed>> event, long sequence, boolean endOfBatch)
            throws Exception {
        throw new UnsupportedOperationException("Not implemented");
    }

    private interface ResponseSender {
        void send(@NonNull final SubscribeStreamResponseUnparsed subscribeStreamResponse) throws ParseException;
    }

    private final class BlockItemsResponseSender implements PullConsumerStreamResponseObserver.ResponseSender {
        private boolean streamStarted = false;

        public void send(@NonNull final SubscribeStreamResponseUnparsed subscribeStreamResponse) throws ParseException {

            if (subscribeStreamResponse.blockItems() == null) {
                final String message = PROTOCOL_VIOLATION_MESSAGE.formatted(
                        "SubscribeStreamResponse", "BLOCK_ITEMS", "block_items", subscribeStreamResponse);
                LOGGER.log(ERROR, message);
                throw new IllegalArgumentException(message);
            }

            final List<BlockItemUnparsed> blockItems =
                    Objects.requireNonNull(subscribeStreamResponse.blockItems()).blockItems();

            // Only start sending BlockItems after we've reached
            // the beginning of a block.
            final BlockItemUnparsed firstBlockItem = blockItems.getFirst();
            if (!streamStarted && firstBlockItem.hasBlockHeader()) {
                streamStarted = true;
            }

            if (streamStarted) {
                if (firstBlockItem.hasBlockHeader()) {
                    long blockNumber = BlockHeader.PROTOBUF
                            .parse(Objects.requireNonNull(firstBlockItem.blockHeader()))
                            .number();
                    if (LOGGER.isLoggable(DEBUG)) {
                        LOGGER.log(DEBUG, "{0} sending block: {1}", Thread.currentThread(), blockNumber);
                    }
                    metricsService.get(CurrentBlockNumberOutbound).set(blockNumber);
                }

                metricsService
                        .get(BlockNodeMetricTypes.Counter.LiveBlockItemsConsumed)
                        .add(blockItems.size());

                // Send the response down through Helidon
                helidonConsumerObserver.onNext(subscribeStreamResponse);
            }
        }
    }

    // TODO: Implement another StatusResponseSender that will unsubscribe the observer once the
    // status code is fixed.
    private final class StatusResponseSender implements PullConsumerStreamResponseObserver.ResponseSender {
        public void send(@NonNull final SubscribeStreamResponseUnparsed subscribeStreamResponse) {
            LOGGER.log(DEBUG, "Sending SubscribeStreamResponse downstream: " + subscribeStreamResponse);
            helidonConsumerObserver.onNext(subscribeStreamResponse);
            helidonConsumerObserver.onComplete();
        }
    }
}
