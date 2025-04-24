// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.access.service;

import static org.hiero.block.node.access.service.BlockAccessServicePlugin.BlockAccessServiceMethod.getBlock;
import static org.hiero.block.node.app.fixtures.TestUtils.enableDebugLogging;
import static org.hiero.block.node.app.fixtures.blocks.BlockItemUtils.toBlockItemsUnparsed;
import static org.hiero.block.node.app.fixtures.blocks.SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocks;
import static org.hiero.block.node.spi.BlockNodePlugin.UNKNOWN_BLOCK_NUMBER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.grpc.ServiceInterface;
import java.util.List;
import org.hiero.block.api.BlockRequest;
import org.hiero.block.api.BlockResponse;
import org.hiero.block.api.BlockResponse.Code;
import org.hiero.block.node.app.fixtures.plugintest.GrpcPluginTestBase;
import org.hiero.block.node.app.fixtures.plugintest.SimpleInMemoryHistoricalBlockFacility;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class BlockAccessServicePluginTest extends GrpcPluginTestBase<BlockAccessServicePlugin> {

    public BlockAccessServicePluginTest() {
        super();
        start(new BlockAccessServicePlugin(), getBlock, new SimpleInMemoryHistoricalBlockFacility());
    }

    /**
     * Enable debug logging for each test.
     */
    @BeforeEach
    void setup() {
        // enable debug System.logger logging
        enableDebugLogging();
        // Send some blocks
        sendBlocks(25);
    }

    /**
     * Test the service methods are correctly defined.
     */
    @Test
    @DisplayName("Test the service interface for BlockAccessServicePlugin")
    void testServiceInterfaceBasics() {
        // check we have a service interface
        assertNotNull(serviceInterface);
        // check the methods from service interface
        List<ServiceInterface.Method> methods = serviceInterface.methods();
        assertNotNull(methods);
        assertEquals(1, methods.size());
        assertEquals(getBlock, methods.getFirst());
    }

    @Test
    @DisplayName("Happy Path Test, BlockAccessServicePlugin for an existing Block Number")
    void happyTestGetBlock() throws ParseException {
        final long blockNumber = 1;
        final BlockRequest request = BlockRequest.newBuilder()
                .blockNumber(blockNumber)
                .allowUnverified(true)
                .retrieveLatest(false)
                .build();
        toPluginPipe.onNext(BlockRequest.PROTOBUF.toBytes(request));
        // Check we get a response
        assertEquals(1, fromPluginBytes.size());
        // parse the response
        BlockResponse response = BlockResponse.PROTOBUF.parse(fromPluginBytes.get(0));
        // check that the status is success
        assertEquals(Code.READ_BLOCK_SUCCESS, response.status());
        // check that the block number is correct
        assertEquals(1, response.block().items().getFirst().blockHeader().number());
    }

    @Test
    @DisplayName("Negative Test, GetBlock for a non-existing Block Number")
    void negativeTestNonExistingBlock() throws ParseException {
        final long blockNumber = 1000;
        final BlockRequest request = BlockRequest.newBuilder()
                .blockNumber(blockNumber)
                .allowUnverified(true)
                .retrieveLatest(false)
                .build();
        toPluginPipe.onNext(BlockRequest.PROTOBUF.toBytes(request));
        // Check we get a response
        assertEquals(1, fromPluginBytes.size());
        // parse the response
        BlockResponse response = BlockResponse.PROTOBUF.parse(fromPluginBytes.get(0));
        // check that the status is NOT FOUND
        assertEquals(Code.READ_BLOCK_NOT_AVAILABLE, response.status());
        // check block is null
        assertNull(response.block());
    }

    @Test
    @DisplayName("Request Latest Block")
    void testRequestLatestBlock() throws ParseException {
        final BlockRequest request = BlockRequest.newBuilder()
                .blockNumber(-1)
                .allowUnverified(true)
                .retrieveLatest(true)
                .build();
        toPluginPipe.onNext(BlockRequest.PROTOBUF.toBytes(request));
        // Check we get a response
        assertEquals(1, fromPluginBytes.size());
        // parse the response
        BlockResponse response = BlockResponse.PROTOBUF.parse(fromPluginBytes.get(0));
        // check that the status is success
        assertEquals(Code.READ_BLOCK_SUCCESS, response.status());
        // check that the block number is correct
        assertEquals(24, response.block().items().getFirst().blockHeader().number());
    }

    @Test
    @DisplayName("Request Latest and a specific Block different from latest, should fail with READ_BLOCK_NOT_FOUND")
    void testRequestLatestBlockDifferent() throws ParseException {
        final long blockNumber = 1;
        final BlockRequest request = BlockRequest.newBuilder()
                .blockNumber(blockNumber)
                .allowUnverified(true)
                .retrieveLatest(true)
                .build();
        toPluginPipe.onNext(BlockRequest.PROTOBUF.toBytes(request));
        // Check we get a response
        assertEquals(1, fromPluginBytes.size());
        // parse the response
        BlockResponse response = BlockResponse.PROTOBUF.parse(fromPluginBytes.get(0));
        // check that the status is success
        assertEquals(Code.READ_BLOCK_NOT_FOUND, response.status());
    }

    @Test
    @DisplayName("block_number is -1 and retrieve_latest is false - should return READ_BLOCK_NOT_FOUND")
    void testBlockNumberIsMinusOneAndRetrieveLatestIsFalse() throws ParseException {
        final BlockRequest request = BlockRequest.newBuilder()
                .blockNumber(-1)
                .allowUnverified(true)
                .retrieveLatest(false)
                .build();
        toPluginPipe.onNext(BlockRequest.PROTOBUF.toBytes(request));
        // Check we get a response
        assertEquals(1, fromPluginBytes.size());
        // parse the response
        BlockResponse response = BlockResponse.PROTOBUF.parse(fromPluginBytes.get(0));
        // check that the status is READ_BLOCK_NOT_FOUND
        assertEquals(Code.READ_BLOCK_NOT_FOUND, response.status());
    }

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
