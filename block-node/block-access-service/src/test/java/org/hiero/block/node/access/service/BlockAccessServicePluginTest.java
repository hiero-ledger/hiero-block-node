// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.access.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.hedera.pbj.runtime.Codec;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.grpc.ServiceInterface;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import org.hiero.block.api.BlockRequest;
import org.hiero.block.api.BlockResponse;
import org.hiero.block.api.BlockResponse.Code;
import org.hiero.block.node.app.fixtures.async.BlockingExecutor;
import org.hiero.block.node.app.fixtures.async.ScheduledBlockingExecutor;
import org.hiero.block.node.app.fixtures.blocks.BlockUtils;
import org.hiero.block.node.app.fixtures.blocks.TestBlock;
import org.hiero.block.node.app.fixtures.blocks.TestBlockBuilder;
import org.hiero.block.node.app.fixtures.plugintest.GrpcPluginTestBase;
import org.hiero.block.node.app.fixtures.plugintest.SimpleInMemoryHistoricalBlockFacility;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.historicalblocks.BlockAccessor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class BlockAccessServicePluginTest
        extends GrpcPluginTestBase<BlockAccessServicePlugin, BlockingExecutor, ScheduledExecutorService> {
    private final BlockAccessServicePlugin plugin = new BlockAccessServicePlugin();

    public BlockAccessServicePluginTest() {
        super(
                new BlockingExecutor(new LinkedBlockingQueue<>()),
                new ScheduledBlockingExecutor(new LinkedBlockingQueue<>()));
        start(plugin, plugin.methods().getFirst(), new SimpleInMemoryHistoricalBlockFacility());
    }

    @BeforeEach
    void setup() {
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
        assertEquals(plugin.methods().getFirst(), methods.getFirst());
    }

    @Test
    @DisplayName("Happy Path Test, BlockAccessServicePlugin for an existing Block Number")
    void happyTestGetBlock() throws ParseException {
        final long blockNumber = 1;
        final BlockRequest request =
                BlockRequest.newBuilder().blockNumber(blockNumber).build();
        toPluginPipe.onNext(BlockRequest.PROTOBUF.toBytes(request));
        // Check we get a response
        assertEquals(1, fromPluginBytes.size());
        // parse the response
        BlockResponse response = BlockResponse.PROTOBUF.parse(fromPluginBytes.get(0));
        // check that the status is success
        assertEquals(Code.SUCCESS, response.status());
        // check that the block number is correct
        assertEquals(1, response.block().items().getFirst().blockHeader().number());
    }

    @Test
    @DisplayName("Negative Test, GetBlock for a non-existing Block Number")
    void negativeTestNonExistingBlock() throws ParseException {
        final long blockNumber = 1000;
        final BlockRequest request =
                BlockRequest.newBuilder().blockNumber(blockNumber).build();
        toPluginPipe.onNext(BlockRequest.PROTOBUF.toBytes(request));
        // Check we get a response
        assertEquals(1, fromPluginBytes.size());
        // parse the response
        BlockResponse response = BlockResponse.PROTOBUF.parse(fromPluginBytes.get(0));
        // check that the status is NOT AVAILABLE
        assertEquals(Code.NOT_AVAILABLE, response.status());
        // check block is null
        assertNull(response.block());
    }

    @Test
    @DisplayName("Request Latest Block")
    void testRequestLatestBlock() throws ParseException {
        final BlockRequest request =
                BlockRequest.newBuilder().retrieveLatest(true).build();
        toPluginPipe.onNext(BlockRequest.PROTOBUF.toBytes(request));
        // Check we get a response
        assertEquals(1, fromPluginBytes.size());
        // parse the response
        BlockResponse response = BlockResponse.PROTOBUF.parse(fromPluginBytes.get(0));
        // check that the status is success
        assertEquals(Code.SUCCESS, response.status());
        // check that the block number is correct
        assertEquals(24, response.block().items().getFirst().blockHeader().number());
    }

    @Test
    @DisplayName("Request Latest Block on empty store")
    void testRequestLatestBlockOnEmptyStore() throws ParseException {
        // restart with a fresh plugin and empty historical block facility
        start(plugin, plugin.methods().getFirst(), new SimpleInMemoryHistoricalBlockFacility());

        final BlockRequest request =
                BlockRequest.newBuilder().retrieveLatest(true).build();
        toPluginPipe.onNext(BlockRequest.PROTOBUF.toBytes(request));
        // Check we get a response
        assertEquals(1, fromPluginBytes.size());
        // parse the response
        BlockResponse response = BlockResponse.PROTOBUF.parse(fromPluginBytes.get(0));
        // check that the status is NOT_AVAILABLE
        assertEquals(Code.NOT_AVAILABLE, response.status());
    }

    @Test
    @DisplayName("Invalid Request Latest Block")
    void testInvalidRequestLatestBlock() throws ParseException {
        final BlockRequest request =
                BlockRequest.newBuilder().retrieveLatest(false).build();
        toPluginPipe.onNext(BlockRequest.PROTOBUF.toBytes(request));
        // Check we get a response
        assertEquals(1, fromPluginBytes.size());
        // parse the response
        BlockResponse response = BlockResponse.PROTOBUF.parse(fromPluginBytes.get(0));
        // check that the status is NOT_AVAILABLE
        assertEquals(Code.INVALID_REQUEST, response.status());
    }

    @Test
    @DisplayName("block_number is -1 - should return latest block")
    void testBlockNumberIsMinusOne() throws ParseException {
        final BlockRequest request = BlockRequest.newBuilder().blockNumber(-1).build();
        toPluginPipe.onNext(BlockRequest.PROTOBUF.toBytes(request));
        // Check we get a response
        assertEquals(1, fromPluginBytes.size());
        // parse the response
        BlockResponse response = BlockResponse.PROTOBUF.parse(fromPluginBytes.get(0));
        // check that the status is success
        assertEquals(Code.SUCCESS, response.status());
        // check that the block number is correct
        assertEquals(24, response.block().items().getFirst().blockHeader().number());
    }

    @Test
    @DisplayName("Invalid Request")
    void testInvalidRequest() throws ParseException {
        final BlockRequest request = BlockRequest.newBuilder().build();
        toPluginPipe.onNext(BlockRequest.PROTOBUF.toBytes(request));
        // Check we get a response
        assertEquals(1, fromPluginBytes.size());
        // parse the response
        BlockResponse response = BlockResponse.PROTOBUF.parse(fromPluginBytes.get(0));
        // check that the status is NOT_AVAILABLE
        assertEquals(Code.INVALID_REQUEST, response.status());
    }

    @Test
    @DisplayName("TSS Wraps Transition Block (466) can be retrieved from BlockAccessService")
    void testGetTssWrapsLargeBlock() throws ParseException, IOException {
        final BlockUtils.SampleBlockInfo info = BlockUtils.getSampleBlockInfo(BlockUtils.SAMPLE_BLOCKS.BLOCK_466);
        blockMessaging.sendBlockItems(
                new BlockItems(info.blockUnparsed().blockItems(), info.blockNumber(), true, true));

        final BlockRequest request =
                BlockRequest.newBuilder().blockNumber(info.blockNumber()).build();
        toPluginPipe.onNext(BlockRequest.PROTOBUF.toBytes(request));

        assertEquals(1, fromPluginBytes.size());
        final BlockResponse response = BlockResponse.PROTOBUF.parse(
                fromPluginBytes.get(0).toReadableSequentialData(),
                false,
                false,
                Codec.DEFAULT_MAX_DEPTH,
                BlockAccessor.MAX_BLOCK_SIZE_BYTES);
        assertEquals(Code.SUCCESS, response.status());
        assertEquals(466, response.block().items().getFirst().blockHeader().number());
    }

    private void sendBlocks(int numberOfBlocks) {
        // Send some blocks
        for (long bn = 0; bn < numberOfBlocks; bn++) {
            final TestBlock block = TestBlockBuilder.generateBlockWithNumber(bn);
            blockMessaging.sendBlockItems(block.asBlockItems());
        }
    }
}
