// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.access.service;

import static org.hiero.block.node.base.ParseHelper.standardParse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.grpc.GrpcException;
import com.hedera.pbj.runtime.grpc.GrpcStatus;
import com.hedera.pbj.runtime.grpc.Pipeline;
import com.hedera.pbj.runtime.grpc.ServiceInterface;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Flow;
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
import org.hiero.block.node.app.fixtures.plugintest.RecordingServiceBuilder;
import org.hiero.block.node.app.fixtures.plugintest.SimpleBlockRangeSet;
import org.hiero.block.node.app.fixtures.plugintest.SimpleInMemoryHistoricalBlockFacility;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.bulkhead.BlockReadBulkhead;
import org.hiero.block.node.spi.historicalblocks.BlockAccessor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class BlockAccessServicePluginTest
        extends GrpcPluginTestBase<BlockAccessServicePlugin, BlockingExecutor, ScheduledExecutorService> {

    private final BlockAccessServicePlugin plugin = new BlockAccessServicePlugin();
    private final FailableHistoricalBlockFacility historicalFacility = new FailableHistoricalBlockFacility();

    public BlockAccessServicePluginTest() {
        super(
                new BlockingExecutor(new LinkedBlockingQueue<>()),
                new ScheduledBlockingExecutor(new LinkedBlockingQueue<>()));
        start(plugin, plugin.methods().getFirst(), historicalFacility);
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
        BlockResponse response = standardParse(BlockResponse.PROTOBUF, fromPluginBytes.get(0));
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
        BlockResponse response = standardParse(BlockResponse.PROTOBUF, fromPluginBytes.get(0));
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
        BlockResponse response = standardParse(BlockResponse.PROTOBUF, fromPluginBytes.get(0));
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
        BlockResponse response = standardParse(BlockResponse.PROTOBUF, fromPluginBytes.get(0));
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
        BlockResponse response = standardParse(BlockResponse.PROTOBUF, fromPluginBytes.get(0));
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
        BlockResponse response = standardParse(BlockResponse.PROTOBUF, fromPluginBytes.get(0));
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
        BlockResponse response = standardParse(BlockResponse.PROTOBUF, fromPluginBytes.get(0));
        // check that the status is NOT_AVAILABLE
        assertEquals(Code.INVALID_REQUEST, response.status());
    }

    @Test
    @DisplayName("TSS WRAPS Transition Block can be retrieved from BlockAccessService")
    void testGetTssWrapsLargeBlock() throws ParseException, IOException {
        final BlockUtils.SampleBlockInfo info = BlockUtils.getSampleBlockInfo(BlockUtils.SAMPLE_BLOCKS.BLOCK_322);
        blockMessaging.sendBlockItems(
                new BlockItems(info.blockUnparsed().blockItems(), info.blockNumber(), true, true));

        final BlockRequest request =
                BlockRequest.newBuilder().blockNumber(info.blockNumber()).build();
        toPluginPipe.onNext(BlockRequest.PROTOBUF.toBytes(request));

        assertEquals(1, fromPluginBytes.size());
        final BlockResponse response = standardParse(BlockResponse.PROTOBUF, fromPluginBytes.get(0), Integer.MAX_VALUE);
        assertEquals(Code.SUCCESS, response.status());
        assertEquals(
                info.blockNumber(),
                response.block().items().getFirst().blockHeader().number());
    }

    @Test
    @DisplayName("getBlock is rejected with RESOURCE_EXHAUSTED once the shared block-read bulkhead is exhausted")
    void getBlockRejectedWhenBulkheadExhausted() {
        final BlockReadBulkhead bulkhead = webserviceBuilder.blockReadBulkhead();
        for (int i = 0; i < bulkhead.totalPermits(); i++) {
            assertTrue(
                    bulkhead.tryAcquire(BlockReadBulkhead.Method.SUBSCRIBER_CATCH_UP),
                    "expected to drain every bulkhead permit");
        }
        try {
            final List<Throwable> errors = new ArrayList<>();
            final Pipeline<Bytes> replies = new Pipeline<>() {
                @Override
                public void onSubscribe(final Flow.Subscription subscription) {}

                @Override
                public void onNext(final Bytes item) {}

                @Override
                public void onError(final Throwable throwable) {
                    errors.add(throwable);
                }

                @Override
                public void onComplete() {}
            };
            final Pipeline<? super Bytes> inbound = serviceInterface.open(method, requestOptions(), replies);
            final Bytes requestBytes = BlockRequest.PROTOBUF.toBytes(
                    BlockRequest.newBuilder().blockNumber(1L).build());
            // Pipelines.unary() both forwards the exception to onError() and rethrows it to the
            // onNext() caller, so we assert the rethrow and still check onError() below.
            assertThrows(GrpcException.class, () -> inbound.onNext(requestBytes));

            assertEquals(1, errors.size());
            assertInstanceOf(GrpcException.class, errors.getFirst());
            assertEquals(GrpcStatus.RESOURCE_EXHAUSTED, ((GrpcException) errors.getFirst()).status());
            assertEquals(1, bulkheadRequests(BlockReadBulkhead.Method.GET_BLOCK, "rejected"));
            assertEquals(0, bulkheadRequests(BlockReadBulkhead.Method.GET_BLOCK, "acquired"));
        } finally {
            for (int i = 0; i < bulkhead.totalPermits(); i++) {
                bulkhead.release();
            }
        }
    }

    @Test
    @DisplayName("getBlock releases its bulkhead permit on every outcome: success, not found, provider failure")
    void getBlockReleasesPermitOnEveryOutcome() throws ParseException {
        final BlockReadBulkhead bulkhead = webserviceBuilder.blockReadBulkhead();
        final int permitsBefore = bulkhead.totalPermits();

        // SUCCESS
        assertEquals(Code.SUCCESS, getBlockStatus(1));
        // NOT_FOUND: advertised as available but missing from storage
        final SimpleBlockRangeSet advertised = new SimpleBlockRangeSet();
        advertised.add(0, 100);
        historicalFacility.setTemporaryAvailableBlocks(advertised);
        try {
            assertEquals(Code.NOT_FOUND, getBlockStatus(50));
            // NOT_FOUND: the provider throws while reading
            historicalFacility.failReads(true);
            assertEquals(Code.NOT_FOUND, getBlockStatus(1));
        } finally {
            historicalFacility.failReads(false);
            historicalFacility.clearTemporaryAvailableBlocks();
        }
        // NOT_AVAILABLE returns before the permit is ever taken
        assertEquals(Code.NOT_AVAILABLE, getBlockStatus(1000));

        // Every permit must still be acquirable (catches a leak), and no more than the pool size
        // (catches an over-release).
        for (int i = 0; i < permitsBefore; i++) {
            assertTrue(bulkhead.tryAcquire(BlockReadBulkhead.Method.SUBSCRIBER_CATCH_UP), "permit " + i + " leaked");
        }
        assertFalse(bulkhead.tryAcquire(BlockReadBulkhead.Method.SUBSCRIBER_CATCH_UP), "permit was over-released");
        for (int i = 0; i < permitsBefore; i++) {
            bulkhead.release();
        }
        // SUCCESS, both NOT_FOUND cases took a permit; NOT_AVAILABLE did not.
        assertEquals(3, bulkheadRequests(BlockReadBulkhead.Method.GET_BLOCK, "acquired"));
        assertEquals(0, bulkheadRequests(BlockReadBulkhead.Method.GET_BLOCK, "rejected"));
    }

    /// A unary pipeline accepts one request, so each call opens its own.
    private Code getBlockStatus(final long blockNumber) throws ParseException {
        final List<Bytes> responses = new ArrayList<>();
        final Pipeline<Bytes> replies = new Pipeline<>() {
            @Override
            public void onSubscribe(final Flow.Subscription subscription) {}

            @Override
            public void onNext(final Bytes item) {
                responses.add(item);
            }

            @Override
            public void onError(final Throwable throwable) {
                throw new AssertionError("unexpected error", throwable);
            }

            @Override
            public void onComplete() {}
        };
        final Pipeline<? super Bytes> inbound = serviceInterface.open(method, requestOptions(), replies);
        inbound.onNext(BlockRequest.PROTOBUF.toBytes(
                BlockRequest.newBuilder().blockNumber(blockNumber).build()));
        assertEquals(1, responses.size());
        return standardParse(BlockResponse.PROTOBUF, responses.getFirst()).status();
    }

    private long bulkheadRequests(final BlockReadBulkhead.Method method, final String status) {
        return ((RecordingServiceBuilder) webserviceBuilder).blockReadBulkheadRequests(method, status);
    }

    /// An in-memory facility whose reads can be made to fail, to exercise the provider-failure path.
    private static final class FailableHistoricalBlockFacility extends SimpleInMemoryHistoricalBlockFacility {
        private volatile boolean failReads;

        void failReads(final boolean fail) {
            this.failReads = fail;
        }

        @Override
        public BlockAccessor block(final long blockNumber) {
            if (failReads) {
                throw new IllegalStateException("simulated storage failure");
            }
            return super.block(blockNumber);
        }
    }

    private static ServiceInterface.RequestOptions requestOptions() {
        return new ServiceInterface.RequestOptions() {
            @Override
            public Optional<String> authority() {
                return Optional.empty();
            }

            @Override
            public String contentType() {
                return ServiceInterface.RequestOptions.APPLICATION_GRPC_PROTO;
            }
        };
    }

    private void sendBlocks(int numberOfBlocks) {
        // Send some blocks
        for (long bn = 0; bn < numberOfBlocks; bn++) {
            final TestBlock block = TestBlockBuilder.generateBlockWithNumber(bn);
            blockMessaging.sendBlockItems(block.asBlockItems());
        }
    }
}
