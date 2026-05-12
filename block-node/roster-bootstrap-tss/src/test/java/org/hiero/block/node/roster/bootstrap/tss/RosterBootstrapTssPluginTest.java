// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.roster.bootstrap.tss;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import org.hiero.block.api.TssData;
import org.hiero.block.node.app.fixtures.async.BlockingExecutor;
import org.hiero.block.node.app.fixtures.async.ScheduledBlockingExecutor;
import org.hiero.block.node.app.fixtures.plugintest.PluginTestBase;
import org.hiero.block.node.app.fixtures.plugintest.SimpleInMemoryHistoricalBlockFacility;
import org.hiero.block.node.app.fixtures.server.TestBlockNodeServer;
import org.hiero.block.node.spi.BlockNodeContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class RosterBootstrapTssPluginTest
        extends PluginTestBase<RosterBootstrapTssPlugin, BlockingExecutor, ScheduledBlockingExecutor> {
    Map<String, String> defaultConfig = new HashMap<>();

    private List<TestBlockNodeServer> testBlockNodeServers;

    /// TempDir for the current test
    private final Path testTempDir;

    @BeforeEach
    void setup() {
        testBlockNodeServers = new ArrayList<>();
    }

    @AfterEach
    void cleanup() {
        // stop any started test block node servers
        if (testBlockNodeServers != null) {
            for (TestBlockNodeServer server : testBlockNodeServers) {
                if (server != null) {
                    server.stop();
                }
            }
        }

        plugin.stop();
    }

    public RosterBootstrapTssPluginTest(@TempDir final Path tempDir) {
        super(
                new BlockingExecutor(new LinkedBlockingQueue<>()),
                new ScheduledBlockingExecutor(new LinkedBlockingQueue<>()));

        this.testTempDir = tempDir;

        // todo: setup the config for the BN peers test
        defaultConfig.put("key", "value");
    }

    @Test
    @DisplayName("request TssData from a peer bn ")
    void requestTssDataFromPeerBN() throws IOException, InterruptedException {
        final TestBlockNodeServer server1 = new TestBlockNodeServer(0, new SimpleInMemoryHistoricalBlockFacility());
        testBlockNodeServers.add(server1);
        String blockNodeSourcesPath = testTempDir + "/blocknode-sources.json";

        createTestBlockNodeSourcesFile(
                BlockNodeSource.newBuilder()
                        .nodes(BlockNodeSourceConfig.newBuilder()
                                .address("localhost")
                                .port(server1.port())
                                .priority(1)
                                .build())
                        .build(),
                blockNodeSourcesPath);

        // Config Override
        Map<String, String> configOverride = RosterBootstrapTssConfigBuilder.newBuilder()
                .blockNodeSourcesPath(blockNodeSourcesPath)
                .queryPeerInterval(500)
                .queryPeerInitialDelay(500)
                .maxIncomingBufferSize(104_857_600)
                .enableTLS(false) // start quickly
                .build();

        final int[] contextUpdated = {0};
        final TssData[] tssData = {null};
        CountDownLatch latch = new CountDownLatch(1);

        RosterBootstrapTssPlugin plugin = new RosterBootstrapTssPlugin() {

            @Override
            public void onContextUpdate(BlockNodeContext context) {
                contextUpdated[0]++;
                tssData[0] = context.tssData();
                latch.countDown();
            }
        };

        start(plugin, new SimpleInMemoryHistoricalBlockFacility(), configOverride);

        latch.await();

        assertTrue(contextUpdated[0] > 0);
        assertNotNull(tssData[0]);

        // These are magic numbers, yes. The {@link TestBlockNodeServer} does not yet have a way to pass in TssData to
        // hand back to testers. Using the values that are passed back to make sure the statusDetails api is
        // being called. Todo: add TssData flexibility to {@link TestBlockNodeServer}
        assertEquals(Bytes.fromHex("01010101"), tssData[0].ledgerId());
        assertEquals(Bytes.fromHex("02020202"), tssData[0].wrapsVerificationKey());
    }

    private void createTestBlockNodeSourcesFile(BlockNodeSource blockNodeSource, String configPath) throws IOException {
        String jsonString = BlockNodeSource.JSON.toJSON(blockNodeSource);
        // Write the JSON string to the specified file path
        java.nio.file.Files.write(java.nio.file.Paths.get(configPath), jsonString.getBytes());
    }

    /// Builder for creating backfill configuration maps for testing.
    public static class RosterBootstrapTssConfigBuilder {

        private String blockNodeSourcesPath;
        private int queryPeerInterval;
        private int queryPeerInitialDelay;
        private int maxIncomingBufferSize;
        private boolean enableTLS;

        private RosterBootstrapTssConfigBuilder() {
            // private to force use of NewBuilder()
        }

        public static RosterBootstrapTssConfigBuilder newBuilder() {
            return new RosterBootstrapTssConfigBuilder();
        }

        public RosterBootstrapTssConfigBuilder blockNodeSourcesPath(String path) {
            this.blockNodeSourcesPath = path;
            return this;
        }

        public RosterBootstrapTssConfigBuilder queryPeerInterval(int value) {
            this.queryPeerInterval = value;
            return this;
        }

        public RosterBootstrapTssConfigBuilder queryPeerInitialDelay(int value) {
            this.queryPeerInitialDelay = value;
            return this;
        }

        public RosterBootstrapTssConfigBuilder maxIncomingBufferSize(int value) {
            this.maxIncomingBufferSize = value;
            return this;
        }

        public RosterBootstrapTssConfigBuilder enableTLS(boolean value) {
            this.enableTLS = value;
            return this;
        }

        public Map<String, String> build() {
            if (blockNodeSourcesPath == null || blockNodeSourcesPath.isBlank()) {
                throw new IllegalStateException("backfillSourcePath is required");
            }

            return new HashMap<>(Map.of(
                    "roster.bootstrap.tss.blockNodeSourcesPath", blockNodeSourcesPath,
                    "roster.bootstrap.tss.queryPeerInterval", String.valueOf(queryPeerInterval),
                    "roster.bootstrap.tss.queryPeerInitialDelay", String.valueOf(queryPeerInitialDelay),
                    "roster.bootstrap.tss.maxIncomingBufferSize", String.valueOf(maxIncomingBufferSize),
                    "roster.bootstrap.tss.enableTLS", String.valueOf(enableTLS)));
        }
    }
}
