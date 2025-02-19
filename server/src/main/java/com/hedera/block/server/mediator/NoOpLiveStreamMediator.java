// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.server.mediator;

import static com.hedera.block.server.metrics.BlockNodeMetricTypes.Counter.LiveBlockItems;
import static java.lang.System.Logger.Level.INFO;

import com.hedera.block.server.config.BlockNodeContext;
import com.hedera.block.server.events.BlockNodeEventHandler;
import com.hedera.block.server.events.ObjectEvent;
import com.hedera.block.server.metrics.MetricsService;
import com.hedera.hapi.block.BlockItemUnparsed;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.List;

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
     * @param blockNodeContext the block node context
     */
    public NoOpLiveStreamMediator(@NonNull final BlockNodeContext blockNodeContext) {
        System.getLogger(getClass().getName()).log(INFO, "Using " + getClass().getSimpleName());
        this.metricsService = blockNodeContext.metricsService();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void publish(@NonNull List<BlockItemUnparsed> blockItems) {
        metricsService.get(LiveBlockItems).add(blockItems.size());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void subscribe(@NonNull BlockNodeEventHandler<ObjectEvent<List<BlockItemUnparsed>>> handler) {}

    @Override
    public Poller<ObjectEvent<List<BlockItemUnparsed>>> subscribePoller(
            @NonNull BlockNodeEventHandler<ObjectEvent<List<BlockItemUnparsed>>> handler) {

        throw new UnsupportedOperationException("Not implemented");
    }

    public List<BlockItemUnparsed> getBlockItems() {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void unsubscribe(@NonNull BlockNodeEventHandler<ObjectEvent<List<BlockItemUnparsed>>> handler) {}

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isSubscribed(@NonNull BlockNodeEventHandler<ObjectEvent<List<BlockItemUnparsed>>> handler) {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void unsubscribeAllExpired() {}

    /**
     * {@inheritDoc}
     */
    @Override
    public void notifyUnrecoverableError() {}
}
