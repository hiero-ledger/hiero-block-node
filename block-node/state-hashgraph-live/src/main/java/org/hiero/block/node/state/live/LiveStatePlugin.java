// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.state.live;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.List;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.block.node.spi.ServiceBuilder;
import org.hiero.block.node.spi.blockmessaging.BlockNotificationHandler;

/**
 * Beta plugin that maintains a live, queryable copy of Hashgraph network state inside the
 * Block Node by replaying verified block-stream state changes.
 *
 * <p>This skeleton class wires the plugin into the BlockNodePlugin SPI and the
 * BlockNotificationHandler virtual-thread dispatcher. Subsequent stories layer on the
 * lifecycle manager, snapshot management, state-change application, and gRPC query
 * service. See {@code docs/design/state/live-state.md} for the full design.
 */
public final class LiveStatePlugin implements BlockNodePlugin, BlockNotificationHandler {

    private BlockNodeContext context;

    @NonNull
    @Override
    public List<Class<? extends Record>> configDataTypes() {
        return List.of(LiveStateConfig.class);
    }

    @Override
    public void init(@NonNull final BlockNodeContext context, @NonNull final ServiceBuilder serviceBuilder) {
        this.context = context;
    }

    @Override
    public void start() {
        if (context != null) {
            context.blockMessaging().registerBlockNotificationHandler(this, true, name());
        }
    }

    @Override
    public void stop() {
        if (context != null) {
            context.blockMessaging().unregisterBlockNotificationHandler(this);
        }
    }
}
