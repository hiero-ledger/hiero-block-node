// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.server.status;

import static org.hiero.block.node.app.fixtures.TestUtils.enableDebugLogging;
import static org.hiero.block.node.app.fixtures.blocks.BlockItemUtils.toBlockItemsUnparsed;
import static org.hiero.block.node.app.fixtures.blocks.SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocks;
import static org.hiero.block.node.spi.BlockNodePlugin.UNKNOWN_BLOCK_NUMBER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.grpc.ServiceInterface;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import org.hiero.block.api.ServerStatusRequest;
import org.hiero.block.api.ServerStatusResponse;
import org.hiero.block.node.app.fixtures.async.BlockingSerialExecutor;
import org.hiero.block.node.app.fixtures.plugintest.GrpcPluginTestBase;
import org.hiero.block.node.app.fixtures.plugintest.SimpleInMemoryHistoricalBlockFacility;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Tests for the ServerStatusServicePlugin class.
 * Validates the functionality of the server status service and its responses
 * under different conditions.
 */
public class ServerStatusServicePluginTest
        extends GrpcPluginTestBase<ServerStatusServicePlugin, BlockingSerialExecutor> {
    private final ServerStatusServicePlugin plugin = new ServerStatusServicePlugin();

    public ServerStatusServicePluginTest() {
        super(new BlockingSerialExecutor(new LinkedBlockingQueue<>()));
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
     * Verifies that the service interface correctly registers and exposes
     * the server status method.
     */
    @Test
    @DisplayName("Should return correct method for ServerStatusServicePlugin")
    void shouldReturnCorrectMethod() {
        assertNotNull(serviceInterface);
        List<ServiceInterface.Method> methods = serviceInterface.methods();
        assertNotNull(methods);
        assertEquals(1, methods.size());
        assertEquals(plugin.methods().getFirst(), methods.getFirst());
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
        assertEquals(0, response.firstAvailableBlock());
        assertEquals(0, response.lastAvailableBlock());
        assertFalse(response.onlyLatestState());

        // TODO(#579) Remove when block node version information is implemented.
        assertFalse(response.hasVersionInformation());
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

        // TODO() Remove when block node version information is implemented.
        assertFalse(response.hasVersionInformation());
    }

    /**
     * Helper method to send a specified number of test blocks to the block messaging system.
     *
     * @param numberOfBlocks the number of test blocks to create and send
     */
    private void sendBlocks(int numberOfBlocks) {
        BlockItem[] blockItems = createNumberOfVerySimpleBlocks(numberOfBlocks);
        // Send some blocks
        for (BlockItem blockItem : blockItems) {
            long blockNumber =
                    blockItem.hasBlockHeader() ? blockItem.blockHeader().number() : UNKNOWN_BLOCK_NUMBER;
            blockMessaging.sendBlockItems(new BlockItems(toBlockItemsUnparsed(blockItem), blockNumber));
        }
    }
}
