// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.roster.bootstrap.tss;

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

public class RosterBootstrapTssPluginTest
        extends PluginTestBase<RosterBootstrapTssPlugin, BlockingExecutor, ScheduledExecutorService> {

    Path testTempDir;

    Map<String, String> defaultConfig = new HashMap<>();

    public RosterBootstrapTssPluginTest(@TempDir final Path tempDir) {
        super(
                new BlockingExecutor(new LinkedBlockingQueue<>()),
                new ScheduledBlockingExecutor(new LinkedBlockingQueue<>()));
        this.testTempDir = Objects.requireNonNull(tempDir);

        defaultConfig.put(
                "roster.bootstrap.tss.ledgerId",
                "ZmU0MTA2ZWUwNGMzMzgyNTljZDcyMWEwN2Y0ZWFhZDMxMjEyZjEyZWFjOGE1MWFjYjM4YTc3OGE0Y2QyMDg3Mw==");
        defaultConfig.put(
                "roster.bootstrap.tss.wrapsVerificationKey",
                "NTUyZTAxNDNjMGE4MTBmNmYwN2VkOGUyZWI0ZGM1MTkwNWEzMmFkMzljZDQ5Yzk0MWE0MGU1YmM4YWEyZDg4NA==");
        defaultConfig.put("roster.bootstrap.tss.nodeId", "1");
        defaultConfig.put("roster.bootstrap.tss.weight", "3");
        defaultConfig.put("roster.bootstrap.tss.validFromBlock", "100");
        defaultConfig.put("roster.bootstrap.tss.rosterValidFromBlock", "50");
        defaultConfig.put(
                "roster.bootstrap.tss.schnorrPublicKey",
                "NWFkYWI2ZjBmYjQzMjk1OGU3OTdiNTI2NjRmNjY4OWQ1Yjg2MTRiYzE5NDE4MTQzODRlZDI4NmQyOTM4MDQxNg==");
        defaultConfig.put(
                "roster.bootstrap.tss.tssParametersFilePath",
                testTempDir.resolve("tss-parameters.bin").toString());
    }

    @Test
    @DisplayName("start without persisted data")
    void startWithoutPersistedData() {
        final RosterBootstrapTssPlugin rosterBootstrapTssPlugin = new RosterBootstrapTssPlugin();
        start(rosterBootstrapTssPlugin, new NoBlocksHistoricalBlockFacility(), defaultConfig);
        assertNotNull(rosterBootstrapTssPlugin);
    }

    @Test
    @DisplayName("test ApplicationStateFacility with tss data in config")
    void testApplicationStateFacilityWithConfig() {
        /// track the updateTssDataCount
        final int[] updateTssDataCount = {0};

        /// RosterBootstrapTssPlugin should notify the ApplicationStateFacility and receive the same message back
        /// via the onContextUpdate
        final RosterBootstrapTssPlugin rosterBootstrapTssPlugin = new RosterBootstrapTssPlugin() {
            @Override
            public void onContextUpdate(BlockNodeContext context) {
                updateTssDataCount[0]++;
                assertEquals(
                        Bytes.fromBase64(defaultConfig.get("roster.bootstrap.tss.ledgerId")),
                        context.tssData().ledgerId());
                assertEquals(
                        Bytes.fromBase64(defaultConfig.get("roster.bootstrap.tss.wrapsVerificationKey")),
                        context.tssData().wrapsVerificationKey());
                assertEquals(
                        Bytes.fromBase64(defaultConfig.get("roster.bootstrap.tss.schnorrPublicKey")),
                        context.tssData().currentRoster().rosterEntries().get(0).schnorrPublicKey());
                assertEquals(
                        Integer.valueOf(defaultConfig.get("roster.bootstrap.tss.nodeId"))
                                .longValue(),
                        context.tssData().currentRoster().rosterEntries().get(0).nodeId());
                assertEquals(
                        Integer.valueOf(defaultConfig.get("roster.bootstrap.tss.weight"))
                                .longValue(),
                        context.tssData().currentRoster().rosterEntries().get(0).weight());
                assertEquals(
                        Integer.valueOf(defaultConfig.get("roster.bootstrap.tss.validFromBlock"))
                                .longValue(),
                        context.tssData().validFromBlock());
                assertEquals(
                        Integer.valueOf(defaultConfig.get("roster.bootstrap.tss.rosterValidFromBlock"))
                                .longValue(),
                        context.tssData().currentRoster().validFromBlock());
            }
        };
        start(rosterBootstrapTssPlugin, new NoBlocksHistoricalBlockFacility(), defaultConfig);
        assertEquals(1, updateTssDataCount[0]);
    }

    @Test
    @DisplayName("test ApplicationStateFacility with no tss data on startup")
    void testApplicationStateFacilityWithoutConfig() {
        /// track the updateTssDataCount
        final int[] updateTssDataCount = {0};

        /// RosterBootstrapTssPlugin should notify the ApplicationStateFacility and receive the same message back
        /// via the onContextUpdate
        final RosterBootstrapTssPlugin rosterBootstrapTssPlugin = new RosterBootstrapTssPlugin() {
            @Override
            public void onContextUpdate(BlockNodeContext context) {
                updateTssDataCount[0]++;
                LOGGER.log(INFO, "Processed RosterBootstrapTssConfig: {0}", context);
            }
        };
        start(rosterBootstrapTssPlugin, new NoBlocksHistoricalBlockFacility(), new HashMap<>());
        assertEquals(0, updateTssDataCount[0]);
    }
}
