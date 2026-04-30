// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.roster.bootstrap.tss;

import static java.lang.System.Logger.Level.INFO;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import org.hiero.block.api.RosterEntry;
import org.hiero.block.api.TssData;
import org.hiero.block.api.TssRoster;
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

    final TssData tssData;

    Path testTempDir;

    Map<String, String> defaultConfig = new HashMap<>();

    public RosterBootstrapTssPluginTest(@TempDir final Path tempDir) throws IOException {
        super(
                new BlockingExecutor(new LinkedBlockingQueue<>()),
                new ScheduledBlockingExecutor(new LinkedBlockingQueue<>()));
        this.testTempDir = Objects.requireNonNull(tempDir);

        final String tssDataJsonPath = testTempDir + "/tss-data.json";
        tssData = TssData.newBuilder()
                .ledgerId(Bytes.fromBase64(
                        "ZmU0MTA2ZWUwNGMzMzgyNTljZDcyMWEwN2Y0ZWFhZDMxMjEyZjEyZWFjOGE1MWFjYjM4YTc3OGE0Y2QyMDg3Mw=="))
                .wrapsVerificationKey(Bytes.fromBase64(
                        "NTUyZTAxNDNjMGE4MTBmNmYwN2VkOGUyZWI0ZGM1MTkwNWEzMmFkMzljZDQ5Yzk0MWE0MGU1YmM4YWEyZDg4NA=="))
                .currentRoster(TssRoster.newBuilder()
                        .rosterEntries(RosterEntry.newBuilder()
                                .nodeId(1)
                                .weight(3)
                                .schnorrPublicKey(
                                        Bytes.fromBase64(
                                                "NWFkYWI2ZjBmYjQzMjk1OGU3OTdiNTI2NjRmNjY4OWQ1Yjg2MTRiYzE5NDE4MTQzODRlZDI4NmQyOTM4MDQxNg=="))
                                .build())
                        .build())
                .build();

        createTestBlockNodeSourcesFile(tssData, tssDataJsonPath);

        defaultConfig.put("roster.bootstrap.tss.tssDataJsonPath", tssDataJsonPath);
    }

    private void createTestBlockNodeSourcesFile(TssData tssData, String configPath) throws IOException {
        String jsonString = TssData.JSON.toJSON(tssData);
        // Write the JSON string to the specified file path
        java.nio.file.Files.write(java.nio.file.Paths.get(configPath), jsonString.getBytes());
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
                TssData contextTssData = context.tssData();
                updateTssDataCount[0]++;
                assertEquals(tssData.ledgerId(), contextTssData.ledgerId());
                assertEquals(tssData.wrapsVerificationKey(), contextTssData.wrapsVerificationKey());
                assertEquals(
                        tssData.currentRoster().rosterEntries().get(0).schnorrPublicKey(),
                        contextTssData.currentRoster().rosterEntries().get(0).schnorrPublicKey());
                assertEquals(
                        tssData.currentRoster().rosterEntries().get(0).nodeId(),
                        contextTssData.currentRoster().rosterEntries().get(0).nodeId());
                assertEquals(
                        tssData.currentRoster().rosterEntries().get(0).weight(),
                        contextTssData.currentRoster().rosterEntries().get(0).weight());
                assertEquals(tssData.validFromBlock(), contextTssData.validFromBlock());
                assertEquals(
                        tssData.currentRoster().validFromBlock(),
                        contextTssData.currentRoster().validFromBlock());
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
