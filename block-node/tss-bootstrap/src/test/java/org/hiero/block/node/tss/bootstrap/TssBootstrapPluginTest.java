// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.tss.bootstrap;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import org.hiero.block.node.app.fixtures.async.BlockingExecutor;
import org.hiero.block.node.app.fixtures.async.ScheduledBlockingExecutor;
import org.hiero.block.node.app.fixtures.plugintest.GrpcPluginTestBase;
import org.hiero.block.node.app.fixtures.plugintest.NoBlocksHistoricalBlockFacility;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TssBootstrapPluginTest
        extends GrpcPluginTestBase<TssBootstrapPlugin, BlockingExecutor, ScheduledExecutorService> {

    Map<String, String> defaultConfig;
    Path testTempDir;

    public TssBootstrapPluginTest(@TempDir final Path tempDir) {
        super(
                new BlockingExecutor(new LinkedBlockingQueue<>()),
                new ScheduledBlockingExecutor(new LinkedBlockingQueue<>()));
        this.testTempDir = Objects.requireNonNull(tempDir);
        defaultConfig = new HashMap<>();
        defaultConfig.put("node.earliestManagedBlock", "0");
        defaultConfig.put(
                "node.tssDataFilePath", tempDir.resolve("tss-data.bin").toString());
    }

    @Test
    @DisplayName("start without persisted data")
    void startWithoutPersistedData() {
        final TssBootstrapPlugin tssBootstrapPlugin = new TssBootstrapPlugin();
        start(tssBootstrapPlugin, new NoBlocksHistoricalBlockFacility(), defaultConfig);
        assertNotNull(tssBootstrapPlugin);
    }
}
