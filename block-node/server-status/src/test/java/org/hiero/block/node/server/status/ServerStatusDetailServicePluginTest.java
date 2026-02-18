// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.server.status;

import static org.hiero.block.node.app.fixtures.TestUtils.enableDebugLogging;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hedera.hapi.node.base.SemanticVersion;
import com.hedera.pbj.runtime.ParseException;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import org.hiero.block.api.BlockNodeVersions;
import org.hiero.block.api.BlockNodeVersions.PluginVersion;
import org.hiero.block.api.ServerStatusDetailResponse;
import org.hiero.block.api.ServerStatusRequest;
import org.hiero.block.node.app.fixtures.async.BlockingExecutor;
import org.hiero.block.node.app.fixtures.async.ScheduledBlockingExecutor;
import org.hiero.block.node.app.fixtures.blocks.TestBlock;
import org.hiero.block.node.app.fixtures.blocks.TestBlockBuilder;
import org.hiero.block.node.app.fixtures.plugintest.GrpcPluginTestBase;
import org.hiero.block.node.app.fixtures.plugintest.SimpleInMemoryHistoricalBlockFacility;
import org.hiero.block.node.spi.module.SemanticVersionUtilities;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Tests for the ServerStatusServicePlugin class.
 * Validates the functionality of the server status service and its responses
 * under different conditions.
 */
public class ServerStatusDetailServicePluginTest
        extends GrpcPluginTestBase<ServerStatusServicePlugin, BlockingExecutor, ScheduledExecutorService> {
    private final ServerStatusServicePlugin plugin = new ServerStatusServicePlugin();

    public ServerStatusDetailServicePluginTest() {
        super(
                new BlockingExecutor(new LinkedBlockingQueue<>()),
                new ScheduledBlockingExecutor(new LinkedBlockingQueue<>()));
        start(plugin, plugin.methods().getLast(), new SimpleInMemoryHistoricalBlockFacility());
    }

    /**
     * Enable debug logging for each test.
     */
    @BeforeEach
    void setup() {
        enableDebugLogging();
    }

    /**
     * Tests that the server status detail response is valid when requested.
     * Verifies the block node version and the plugin versions.
     *
     * @throws ParseException if there is an error parsing the response
     */
    @Test
    @DisplayName("Should return valid Server Detail Status when requested")
    void shouldReturnValidServerStatus() throws ParseException {
        final ServerStatusRequest request = ServerStatusRequest.newBuilder().build();
        toPluginPipe.onNext(ServerStatusRequest.PROTOBUF.toBytes(request));
        assertEquals(1, fromPluginBytes.size());

        final ServerStatusDetailResponse response =
                ServerStatusDetailResponse.PROTOBUF.parse(fromPluginBytes.getFirst());

        assertNotNull(response);
        assertTrue(response.hasVersionInformation());
        final BlockNodeVersions blockNodeVersions = response.versionInformation();
        assertNotNull(blockNodeVersions);
        final SemanticVersion semanticVersion = SemanticVersionUtilities.from(this.getClass());
        assertEquals(semanticVersion, blockNodeVersions.blockNodeVersion());
        final List<PluginVersion> pluginVersions = blockNodeVersions.installedPluginVersions();
        assertEquals(1, pluginVersions.size());
        final PluginVersion pluginVersion = pluginVersions.getFirst();
        assertEquals(plugin.getClass().getName(), pluginVersion.pluginId());
        assertEquals(semanticVersion, pluginVersion.pluginSoftwareVersion());
        // Features default to empty list
        assertEquals(0, pluginVersion.pluginFeatureNames().size());
    }

    /**
     * Helper method to send a specified number of test blocks to the block messaging system.
     *
     * @param numberOfBlocks the number of test blocks to create and send
     */
    private void sendBlocks(int numberOfBlocks) {
        // Send some blocks
        for (long bn = 0; bn < numberOfBlocks; bn++) {
            final TestBlock block = TestBlockBuilder.generateBlockWithNumber(bn);
            blockMessaging.sendBlockItems(block.asBlockItems());
        }
    }
}
