// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.server.status;

import static org.hiero.block.node.app.fixtures.TestUtils.enableDebugLogging;
import static org.hiero.block.node.spi.BlockNodePlugin.UNKNOWN_BLOCK_NUMBER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.hedera.pbj.runtime.ParseException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import org.hiero.block.api.ServerStatusRequest;
import org.hiero.block.api.ServerStatusResponse;
import org.hiero.block.node.app.fixtures.async.BlockingExecutor;
import org.hiero.block.node.app.fixtures.async.ScheduledBlockingExecutor;
import org.hiero.block.node.app.fixtures.blocks.TestBlock;
import org.hiero.block.node.app.fixtures.blocks.TestBlockBuilder;
import org.hiero.block.node.app.fixtures.plugintest.GrpcPluginTestBase;
import org.hiero.block.node.app.fixtures.plugintest.SimpleInMemoryHistoricalBlockFacility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Tests for the ServerStatusServicePlugin class.
 * Validates the functionality of the server status service and its responses
 * under different conditions.
 */
public class ServerStatusServicePluginTest
        extends GrpcPluginTestBase<ServerStatusServicePlugin, BlockingExecutor, ScheduledExecutorService> {
    private final ServerStatusServicePlugin plugin = new ServerStatusServicePlugin();

    public ServerStatusServicePluginTest() {
        super(
                new BlockingExecutor(new LinkedBlockingQueue<>()),
                new ScheduledBlockingExecutor(new LinkedBlockingQueue<>()));
        start(plugin, plugin.methods().getFirst(), new SimpleInMemoryHistoricalBlockFacility());
    }

    /**
     * Enable debug logging for each test.
     */
    @BeforeEach
    void setup() {
        enableDebugLogging();
    }

    /**
     * Tests that the server status response is valid when no blocks are available.
     * Verifies the first and last available block numbers and other response properties.
     *
     * @throws ParseException if there is an error parsing the response
     */
    @Test
    @DisplayName("Should return valid Server Status when no blocks available")
    void shouldReturnValidServerStatus() throws ParseException {
        final ServerStatusRequest request = ServerStatusRequest.newBuilder().build();
        toPluginPipe.onNext(ServerStatusRequest.PROTOBUF.toBytes(request));
        assertEquals(1, fromPluginBytes.size());

        final ServerStatusResponse response = ServerStatusResponse.PROTOBUF.parse(fromPluginBytes.getFirst());

        assertNotNull(response);
        assertEquals(UNKNOWN_BLOCK_NUMBER, response.firstAvailableBlock());
        assertEquals(UNKNOWN_BLOCK_NUMBER, response.lastAvailableBlock());
        assertFalse(response.onlyLatestState());
    }

    /**
     * Tests the server status response after adding a new batch of blocks.
     * Verifies that the first and last available block numbers are correctly updated.
     *
     * @throws ParseException if there is an error parsing the response
     */
    @Test
    @DisplayName("Should return valid Server Status, after new batch of blocks")
    void shouldReturnValidServerStatusForNewBlockBatch() throws ParseException {
        final int blocks = 5;
        sendBlocks(blocks);
        final ServerStatusRequest request = ServerStatusRequest.newBuilder().build();
        toPluginPipe.onNext(ServerStatusRequest.PROTOBUF.toBytes(request));
        assertEquals(1, fromPluginBytes.size());

        final ServerStatusResponse response = ServerStatusResponse.PROTOBUF.parse(fromPluginBytes.getLast());

        assertNotNull(response);
        assertEquals(0, response.firstAvailableBlock());
        assertEquals(blocks - 1, response.lastAvailableBlock());
        assertFalse(response.onlyLatestState());
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
