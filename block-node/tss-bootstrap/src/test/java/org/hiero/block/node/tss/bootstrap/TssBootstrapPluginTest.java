// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.tss.bootstrap;

import static java.lang.System.Logger.Level.INFO;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import org.hiero.block.node.app.fixtures.async.BlockingExecutor;
import org.hiero.block.node.app.fixtures.async.ScheduledBlockingExecutor;
import org.hiero.block.node.app.fixtures.plugintest.NoBlocksHistoricalBlockFacility;
import org.hiero.block.node.app.fixtures.plugintest.PluginTestBase;
import org.hiero.block.node.spi.BlockNodeContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TssBootstrapPluginTest
        extends PluginTestBase<TssBootstrapPlugin, BlockingExecutor, ScheduledExecutorService> {

    Path testTempDir;

    Map<String, String> defaultConfig = new HashMap<>();

    public TssBootstrapPluginTest(@TempDir final Path tempDir) {
        super(
                new BlockingExecutor(new LinkedBlockingQueue<>()),
                new ScheduledBlockingExecutor(new LinkedBlockingQueue<>()));
        this.testTempDir = Objects.requireNonNull(tempDir);

        defaultConfig.put(
                "tss.bootstrap.ledgerId",
                "ZmU0MTA2ZWUwNGMzMzgyNTljZDcyMWEwN2Y0ZWFhZDMxMjEyZjEyZWFjOGE1MWFjYjM4YTc3OGE0Y2QyMDg3Mw==");
        defaultConfig.put(
                "tss.bootstrap.wrapsVerificationKey",
                "NTUyZTAxNDNjMGE4MTBmNmYwN2VkOGUyZWI0ZGM1MTkwNWEzMmFkMzljZDQ5Yzk0MWE0MGU1YmM4YWEyZDg4NA==");
        defaultConfig.put("tss.bootstrap.nodeId", "1");
        defaultConfig.put("tss.bootstrap.weight", "3");
        defaultConfig.put(
                "tss.bootstrap.schnorrPublicKey",
                "NWFkYWI2ZjBmYjQzMjk1OGU3OTdiNTI2NjRmNjY4OWQ1Yjg2MTRiYzE5NDE4MTQzODRlZDI4NmQyOTM4MDQxNg==");
        defaultConfig.put(
                "tss.bootstrap.tssParametersFilePath",
                testTempDir.resolve("tss-parameters.bin").toString());
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
                assertEquals(
                        Bytes.fromBase64(defaultConfig.get("tss.bootstrap.schnorrPublicKey")),
                        context.tssData().currentRoster().rosterEntries().get(0).schnorrPublicKey());
                assertEquals(
                        Integer.valueOf(defaultConfig.get("tss.bootstrap.nodeId"))
                                .longValue(),
                        context.tssData().currentRoster().rosterEntries().get(0).nodeId());
                assertEquals(
                        Integer.valueOf(defaultConfig.get("tss.bootstrap.weight"))
                                .longValue(),
                        context.tssData().currentRoster().rosterEntries().get(0).weight());
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
                LOGGER.log(INFO, "Processed TssBootstrapConfig: {0}", context);
            }
        };
        start(tssBootstrapPlugin, new NoBlocksHistoricalBlockFacility(), new HashMap<>());
        assertEquals(0, updateTssDataCount[0]);
    }
}
