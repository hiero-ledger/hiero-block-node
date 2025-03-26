// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.mediator;

import static java.lang.System.Logger.Level.INFO;
import static org.hiero.block.server.metrics.BlockNodeMetricTypes.Counter.LiveBlockItems;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.List;
import org.hiero.block.server.consumer.StreamManager;
import org.hiero.block.server.events.BlockNodeEventHandler;
import org.hiero.block.server.events.ObjectEvent;
import org.hiero.block.server.metrics.MetricsService;
import org.hiero.hapi.block.node.BlockItemUnparsed;

/**
 * The NoOpLiveStreamMediator class is a stub implementation of the live stream mediator intended for testing
 * purposes only. It is designed to isolate the Producer component from downstream components subscribed to
 * the ring buffer during testing while still providing metrics and logging for troubleshooting.
 */
public class NoOpLiveStreamMediator implements LiveStreamMediator {

    private final MetricsService metricsService;

    /**
     * Creates a new NoOpLiveStreamMediator instance for testing and troubleshooting only.
     *
     * @param metricsService the metrics service
     */
    public NoOpLiveStreamMediator(@NonNull final MetricsService metricsService) {
        System.getLogger(getClass().getName()).log(INFO, "Using " + getClass().getSimpleName());
        this.metricsService = metricsService;
    }

    @Override
    public void publish(@NonNull List<BlockItemUnparsed> blockItems) {
        metricsService.get(LiveBlockItems).add(blockItems.size());
    }

    @Override
    public void subscribe(@NonNull BlockNodeEventHandler<ObjectEvent<List<BlockItemUnparsed>>> handler) {}

    @Override
    public Poller<ObjectEvent<List<BlockItemUnparsed>>> subscribePoller(@NonNull final StreamManager streamManager) {
        return null;
    }

    @Override
    public void unsubscribePoller(@NonNull final StreamManager streamManager) {}

    @Override
    public boolean isSubscribed(@NonNull final StreamManager streamManager) {
        return false;
    }

    @Override
    public void unsubscribe(@NonNull BlockNodeEventHandler<ObjectEvent<List<BlockItemUnparsed>>> handler) {}

    @Override
    public boolean isSubscribed(@NonNull BlockNodeEventHandler<ObjectEvent<List<BlockItemUnparsed>>> handler) {
        return false;
    }

    @Override
    public void unsubscribeAllExpired() {}

    @Override
    public void notifyUnrecoverableError() {}
}
