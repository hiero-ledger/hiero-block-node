// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.producer;

import static java.lang.System.Logger.Level.ERROR;
import static java.lang.System.Logger.Level.INFO;
import static org.hiero.block.server.metrics.BlockNodeMetricTypes.Counter.LiveBlockItemsReceived;

import com.hedera.hapi.block.BlockItemUnparsed;
import com.hedera.hapi.block.PublishStreamResponse;
import com.hedera.pbj.runtime.grpc.Pipeline;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.List;
import java.util.concurrent.Flow;
import org.hiero.block.server.events.BlockNodeEventHandler;
import org.hiero.block.server.events.ObjectEvent;
import org.hiero.block.server.metrics.MetricsService;

/**
 * The NoOpProducerObserver class is a stub implementation of the producer observer intended for testing
 * purposes only. It is designed to isolate the Block Node from the Helidon layers during testing while
 * still providing metrics and logging for troubleshooting.
 */
public class NoOpProducerObserver
        implements Pipeline<List<BlockItemUnparsed>>, BlockNodeEventHandler<ObjectEvent<PublishStreamResponse>> {

    private final System.Logger LOGGER = System.getLogger(getClass().getName());
    private final MetricsService metricsService;

    /**
     * Creates a new NoOpProducerObserver instance for testing and troubleshooting only.
     *
     * @param publishStreamResponseObserver the stream response observer provided by Helidon
     * @param metricsService the metrics service
     */
    public NoOpProducerObserver(
            @NonNull final Pipeline<? super PublishStreamResponse> publishStreamResponseObserver,
            @NonNull final MetricsService metricsService) {
        LOGGER.log(INFO, "Using " + getClass().getName());
        this.metricsService = metricsService;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onNext(List<BlockItemUnparsed> blockItems) {

        metricsService.get(LiveBlockItemsReceived).add(blockItems.size());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onEvent(ObjectEvent<PublishStreamResponse> publishStreamResponseObjectEvent, long l, boolean b)
            throws Exception {}

    /**
     * {@inheritDoc}
     */
    @Override
    public void onSubscribe(Flow.Subscription subscription) {}

    /**
     * {@inheritDoc}
     */
    @Override
    public void onError(Throwable throwable) {
        LOGGER.log(ERROR, "onError method invoked with an exception: ", throwable);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onComplete() {}
}
