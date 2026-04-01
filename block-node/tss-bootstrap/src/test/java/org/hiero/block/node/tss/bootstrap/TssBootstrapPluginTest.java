// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.tss.bootstrap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import org.hiero.block.node.app.fixtures.async.BlockingExecutor;
import org.hiero.block.node.app.fixtures.async.ScheduledBlockingExecutor;
import org.hiero.block.node.app.fixtures.plugintest.NoBlocksHistoricalBlockFacility;
import org.hiero.block.node.app.fixtures.plugintest.PluginTestBase;
import org.hiero.block.node.spi.BlockNodeContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class TssBootstrapPluginTest
        extends PluginTestBase<TssBootstrapPlugin, BlockingExecutor, ScheduledExecutorService> {

    Map<String, String> defaultConfig = new HashMap<>();

    public TssBootstrapPluginTest() {
        super(
                new BlockingExecutor(new LinkedBlockingQueue<>()),
                new ScheduledBlockingExecutor(new LinkedBlockingQueue<>()));
        defaultConfig.put("tss.bootstrap.tssParametersFilePath", "tss-parameters.bin");
        defaultConfig.put(
                "tss.bootstrap.ledgerId",
                "ZmU0MTA2ZWUwNGMzMzgyNTljZDcyMWEwN2Y0ZWFhZDMxMjEyZjEyZWFjOGE1MWFjYjM4YTc3OGE0Y2QyMDg3Mw==");
        defaultConfig.put(
                "tss.bootstrap.wrapsVerificationKey",
                "NTUyZTAxNDNjMGE4MTBmNmYwN2VkOGUyZWI0ZGM1MTkwNWEzMmFkMzljZDQ5Yzk0MWE0MGU1YmM4YWEyZDg4NA==");
    }

    @Test
    @DisplayName("start without persisted data")
    void startWithoutPersistedData() {
        final TssBootstrapPlugin tssBootstrapPlugin = new TssBootstrapPlugin();
        start(tssBootstrapPlugin, new NoBlocksHistoricalBlockFacility(), defaultConfig);
        assertNotNull(tssBootstrapPlugin);
    }

    @Test
    @DisplayName("test ApplicationStateFacility with tss data in config")
    void testApplicationStateFacilityWithConfig() {
        /// track the updateTssDataCount
        final int[] updateTssDataCount = {0};

        /// TssBootstrap plugin should notify the ApplicationStateFacility and receive the same message back
        /// via the onContextUpdate
        final TssBootstrapPlugin tssBootstrapPlugin = new TssBootstrapPlugin() {
            @Override
            public void onContextUpdate(BlockNodeContext context) {
                updateTssDataCount[0]++;
                assertEquals(
                        Bytes.fromBase64(defaultConfig.get("tss.bootstrap.ledgerId")),
                        context.tssData().ledgerId());
                assertEquals(
                        Bytes.fromBase64(defaultConfig.get("tss.bootstrap.wrapsVerificationKey")),
                        context.tssData().wrapsVerificationKey());
            }
        };
        start(tssBootstrapPlugin, new NoBlocksHistoricalBlockFacility(), defaultConfig);
        assertEquals(1, updateTssDataCount[0]);
    }

    @Test
    @DisplayName("test ApplicationStateFacility with no tss data on startup")
    void testApplicationStateFacilityWithoutConfig() {
        /// track the updateTssDataCount
        final int[] updateTssDataCount = {0};

        /// TssBootstrap plugin should notify the ApplicationStateFacility and receive the same message back
        /// via the onContextUpdate
        final TssBootstrapPlugin tssBootstrapPlugin = new TssBootstrapPlugin() {
            @Override
            public void onContextUpdate(BlockNodeContext context) {
                updateTssDataCount[0]++;
            }
        };
        start(tssBootstrapPlugin, new NoBlocksHistoricalBlockFacility());
        assertEquals(0, updateTssDataCount[0]);
    }
}
