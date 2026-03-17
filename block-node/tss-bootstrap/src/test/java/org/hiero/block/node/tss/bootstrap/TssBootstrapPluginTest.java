// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.tss.bootstrap;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import org.hiero.block.node.app.fixtures.async.BlockingExecutor;
import org.hiero.block.node.app.fixtures.async.ScheduledBlockingExecutor;
import org.hiero.block.node.app.fixtures.plugintest.GrpcPluginTestBase;
import org.hiero.block.node.app.fixtures.plugintest.NoBlocksHistoricalBlockFacility;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class TssBootstrapPluginTest
        extends GrpcPluginTestBase<TssBootstrapPlugin, BlockingExecutor, ScheduledExecutorService> {
    public TssBootstrapPluginTest() {
        super(
                new BlockingExecutor(new LinkedBlockingQueue<>()),
                new ScheduledBlockingExecutor(new LinkedBlockingQueue<>()));
        start(new TssBootstrapPlugin(), new NoBlocksHistoricalBlockFacility());
    }

    @Test
    @DisplayName("should always succeed")
    void shouldAlwaysSucceed() {
        assertTrue(true);
    }
}
