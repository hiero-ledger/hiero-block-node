// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.stream.subscriber;

import static java.util.concurrent.locks.LockSupport.parkNanos;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.hiero.block.node.app.fixtures.TestUtils.enableDebugLogging;
import static org.hiero.block.node.app.fixtures.blocks.SimpleTestBlockItemBuilder.toBlockItems;

import com.hedera.hapi.block.stream.Block;
import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Function;
import java.util.stream.Stream;
import org.hiero.block.api.SubscribeStreamRequest;
import org.hiero.block.api.SubscribeStreamResponse;
import org.hiero.block.api.SubscribeStreamResponse.Code;
import org.hiero.block.node.app.fixtures.async.ScheduledBlockingExecutor;
import org.hiero.block.node.app.fixtures.blocks.SimpleTestBlockItemBuilder;
import org.hiero.block.node.app.fixtures.plugintest.GrpcPluginTestBase;
import org.hiero.block.node.app.fixtures.plugintest.SimpleInMemoryHistoricalBlockFacility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests for the {@link SubscriberServicePlugin}.
 */
@SuppressWarnings("DataFlowIssue")
@DisplayName("SubscriberServicePlugin Tests")
@Timeout(10) // fail tests that take longer than 10 seconds
class SubscriberServicePluginTest {
    // CONST
    private static final int responseWaitLimit = 50_000;

    // EXTRACTORS
    private static final Function<Bytes, SubscribeStreamResponse> responseExtractor = bytes -> {
        try {
            return SubscribeStreamResponse.PROTOBUF.parse(bytes);
        } catch (final ParseException e) {
            throw new RuntimeException(e);
        }
    };
    private static final Function<SubscribeStreamResponse, SubscribeStreamResponse.Code> responseStatusExtractor =
            SubscribeStreamResponse::status;

    /**
     * Enable debug logging for each test.
     */
    @BeforeEach
    protected void setup() {
        enableDebugLogging();
    }

    /**
     * Plugin tests for the {@link SubscriberServicePlugin}.
     */
    @Nested
    @DisplayName("Plugin Tests")
    class PluginTests extends GrpcPluginTestBase<SubscriberServicePlugin, ExecutorService, ScheduledBlockingExecutor> {
        // SETUP
        private final SimpleInMemoryHistoricalBlockFacility historicalBlockFacility;

        /**
         * Constructor.
         * Sets up environment for testing.
         */
        PluginTests() {
            super(Executors.newSingleThreadExecutor(), new ScheduledBlockingExecutor(new LinkedBlockingQueue<>()));
            final SubscriberServicePlugin toTest = new SubscriberServicePlugin();
            historicalBlockFacility = new SimpleInMemoryHistoricalBlockFacility();
            start(toTest, toTest.methods().getFirst(), historicalBlockFacility);
        }

        /**
         * Functionality tests for the subscriber plugin.
         */
        @Nested
        @DisplayName("Functionality Tests")
        class FunctionalityTests {
            /**
             * Positive tests for the subscriber plugin.
             */
            @Nested
            @DisplayName("Positive Subscriber Tests")
            class PositiveSubscriberTests {
                /**
                 * Positive tests for single block requests.
                 */
                @Nested
                @DisplayName("Single Block Request Tests")
                class SingleBlockRequestTests {
                    /**
                     * This test aims to assert that when a valid request for a
                     * single block is sent to the plugin, a response with the
                     * block items is returned, followed by a success status
                     * response. Here, the block requested already exists before
                     * the request is made.
                     */
                    @Test
                    @DisplayName("Test Subscriber: Valid Request Single Block Already Existing")
                    void testSuccessfulRequestSingleBlock() {
                        // First we create the block
                        final List<Block> blockZero =
                                SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksBatched(0, 0);
                        // Supply the needed items
                        final List<BlockItem> expected = blockZero.getFirst().items();
                        blockMessaging.sendBlockItems(toBlockItems(expected));
                        // Then, we create the request for block 0
                        final SubscribeStreamRequest request = SubscribeStreamRequest.newBuilder()
                                .startBlockNumber(0L)
                                .endBlockNumber(0L)
                                .build();
                        // Send the request
                        toPluginPipe.onNext(SubscribeStreamRequest.PROTOBUF.toBytes(request));
                        // Wait for responses
                        final int expectedResponses = 3; // one with items, end of block, one with success status
                        awaitResponse(fromPluginBytes, expectedResponses);
                        // Assert responses count and status success
                        assertThat(fromPluginBytes)
                                .hasSize(expectedResponses)
                                .last()
                                .extracting(responseExtractor)
                                .isNotNull()
                                .returns(Code.SUCCESS, responseStatusExtractor);
                        // Extract and assert block items response
                        final List<BlockItem> actual = responseExtractor
                                .apply(fromPluginBytes.getFirst())
                                .blockItems()
                                .blockItems();
                        assertBlockReceived(expected, actual);
                    }

                    /**
                     * This test aims to assert that when a valid request for a
                     * single block is sent to the plugin, a response with the
                     * block items is returned, followed by a success status
                     * response. Here, the block requested does not exist at the
                     * time of the request, but is supplied later from history.
                     * This is a request for future block, but the request can
                     * be fulfilled because we have some historical data, thus
                     * we can determine how far in the future the requested
                     * block is.
                     */
                    @Test
                    @DisplayName("Test Subscriber: Valid Request Single Block Future From History")
                    void testSuccessfulRequestSingleBlockFutureFromHistory() {
                        // First we create the block
                        final List<Block> blockZero =
                                SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksBatched(0, 0);
                        // Supply a block, otherwise if there is no data at all, we
                        // cannot fulfill the request, because we cannot determine
                        // how much in the future the requested block is
                        final List<BlockItem> blockZeroItems =
                                blockZero.getFirst().items();
                        historicalBlockFacility.handleBlockItemsReceived(toBlockItems(blockZeroItems));
                        // Then, we create the request for block 0
                        final SubscribeStreamRequest request = SubscribeStreamRequest.newBuilder()
                                .startBlockNumber(1L)
                                .endBlockNumber(1L)
                                .build();
                        // Send the request
                        toPluginPipe.onNext(SubscribeStreamRequest.PROTOBUF.toBytes(request));
                        // Now supply the requested block to history
                        final List<Block> blockOne =
                                SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksBatched(1, 1);
                        final List<BlockItem> expected = blockOne.getFirst().items();
                        historicalBlockFacility.handleBlockItemsReceived(toBlockItems(expected));
                        // Wait for responses
                        final int expectedResponses = 3; // one with items, end of block, one with success status
                        awaitResponse(fromPluginBytes, expectedResponses);
                        // Assert responses count and status success
                        assertThat(fromPluginBytes)
                                .hasSize(expectedResponses)
                                .last()
                                .extracting(responseExtractor)
                                .isNotNull()
                                .returns(Code.SUCCESS, responseStatusExtractor);
                        // Extract and assert block items response
                        final List<BlockItem> actual = responseExtractor
                                .apply(fromPluginBytes.getFirst())
                                .blockItems()
                                .blockItems();
                        assertBlockReceived(expected, actual);
                    }

                    /**
                     * This test aims to assert that when a valid request for a
                     * single block is sent to the plugin, a response with the
                     * block items is returned, followed by a success status
                     * response. Here, the block requested does not exist at the
                     * time of the request, but is supplied later from live
                     * data. This is a request for future block, but the request
                     * can be fulfilled because we have some historical data,
                     * thus we can determine how far in the future the requested
                     * block is.
                     */
                    @Test
                    @DisplayName("Test Subscriber: Valid Request Single Block Future From Live")
                    void testSuccessfulRequestSingleBlockFutureFromLive() {
                        // First we create the block
                        final List<Block> blockZero =
                                SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksBatched(0, 0);
                        // Supply a block, otherwise if there is no data at all, we
                        // cannot fulfill the request, because we cannot determine
                        // how much in the future the requested block is
                        final List<BlockItem> blockZeroItems =
                                blockZero.getFirst().items();
                        historicalBlockFacility.handleBlockItemsReceived(toBlockItems(blockZeroItems));
                        // Then, we create the request for block 0
                        final SubscribeStreamRequest request = SubscribeStreamRequest.newBuilder()
                                .startBlockNumber(1L)
                                .endBlockNumber(1L)
                                .build();
                        // Send the request
                        toPluginPipe.onNext(SubscribeStreamRequest.PROTOBUF.toBytes(request));
                        // Disable history to listen to live data so we ensure
                        // block will be supplied from live
                        historicalBlockFacility.setDisablePlugin();
                        // Now supply the requested block to history
                        final List<Block> blockOne =
                                SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksBatched(1, 1);
                        final List<BlockItem> expected = blockOne.getFirst().items();
                        blockMessaging.sendBlockItems(toBlockItems(expected));
                        // Wait for responses
                        final int expectedResponses = 3; // one with items, end of block, one with success status
                        awaitResponse(fromPluginBytes, expectedResponses);
                        // Assert responses count and status success
                        assertThat(fromPluginBytes)
                                .hasSize(expectedResponses)
                                .last()
                                .extracting(responseExtractor)
                                .isNotNull()
                                .returns(Code.SUCCESS, responseStatusExtractor);
                        // Extract and assert block items response
                        final List<BlockItem> actual = responseExtractor
                                .apply(fromPluginBytes.getFirst())
                                .blockItems()
                                .blockItems();
                        assertBlockReceived(expected, actual);
                    }

                    /**
                     * This test aims to assert that when a valid request for a
                     * single block is sent to the plugin, a response with the
                     * block items is returned, followed by a success status
                     * response. Here, the block requested does not exist at the
                     * time of the request, but is supplied later from live
                     * data. This is a request for future block, but the request
                     * can be fulfilled because we have some historical data,
                     * thus we can determine how far in the future the requested
                     * block is. The Block is supplied to live in multiple
                     * batches. The live data must nevertheless be streamed to
                     * completion. A single block sent in multiple batches is
                     * sufficient to assert this.
                     */
                    @Test
                    @DisplayName("Test Subscriber: Valid Request Single Block Future From Live In Batches")
                    void testSuccessfulRequestSingleBlockFutureFromLiveInBatches() {
                        // First we create the block
                        final List<Block> blockZero =
                                SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksBatched(0, 0);
                        // Supply a block, otherwise if there is no data at all, we
                        // cannot fulfill the request, because we cannot determine
                        // how much in the future the requested block is
                        final List<BlockItem> blockZeroItems =
                                blockZero.getFirst().items();
                        historicalBlockFacility.handleBlockItemsReceived(toBlockItems(blockZeroItems));
                        // Then, we create the request for block 0
                        final SubscribeStreamRequest request = SubscribeStreamRequest.newBuilder()
                                .startBlockNumber(1L)
                                .endBlockNumber(1L)
                                .build();
                        // Send the request
                        toPluginPipe.onNext(SubscribeStreamRequest.PROTOBUF.toBytes(request));
                        // Disable history to listen to live data so we ensure
                        // block will be supplied from live
                        historicalBlockFacility.setDisablePlugin();
                        // Now supply the requested block to history in multiple batches
                        final List<Block> blockOne =
                                SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksBatched(1, 1);
                        final List<BlockItem> expected = blockOne.getFirst().items();
                        // Now, send to live only the header as a first batch
                        final List<BlockItem> headerOnly = expected.subList(0, 1);
                        blockMessaging.sendBlockItems(toBlockItems(headerOnly));
                        // we expect that a response will be sent
                        awaitResponse(fromPluginBytes, 1);
                        // Now send the rest of the block in one batch
                        final List<BlockItem> restOfBlock = expected.subList(1, expected.size());
                        blockMessaging.sendBlockItems(toBlockItems(
                                restOfBlock,
                                false,
                                headerOnly.getFirst().blockHeader().number()));
                        // Wait for all responses, now we expect two responses,
                        // one is for the rest of the block items, the other is
                        // the success status
                        awaitResponse(fromPluginBytes, 2);
                        // Assert total expected responses count, end of block, and status success
                        final int expectedResponses = 4;
                        awaitResponse(fromPluginBytes, expectedResponses);
                        // Assert responses count and status success
                        assertThat(fromPluginBytes)
                                .hasSize(expectedResponses)
                                .last()
                                .extracting(responseExtractor)
                                .isNotNull()
                                .returns(Code.SUCCESS, responseStatusExtractor);
                        // Extract and assert block items response for the header
                        final List<BlockItem> actualHeaderOnly = responseExtractor
                                .apply(fromPluginBytes.getFirst())
                                .blockItems()
                                .blockItems();
                        assertBlockReceived(headerOnly, actualHeaderOnly);
                        // Extract and assert block items response for the rest of the block
                        final List<BlockItem> actualRestOfBlock = responseExtractor
                                .apply(fromPluginBytes.get(1))
                                .blockItems()
                                .blockItems();
                        assertBlockReceived(restOfBlock, actualRestOfBlock);
                    }
                }

                /**
                 * Multiple block requests with a closed range (start and end
                 * defined).
                 */
                @Nested
                @DisplayName("Multiple Block Request Closed Range Tests")
                class MultipleBlockRequestClosedRangeTests {
                    /**
                     * This test aims to assert that when a valid request for
                     * multiple blocks is sent to the plugin, a response(s) with
                     * the block items is returned, followed by a success status
                     * response. Here, the blocks requested already exist before
                     * the request is made.
                     */
                    @Test
                    @DisplayName("Test Subscriber: Valid Request Multiple Blocks Already Existing")
                    void testSuccessfulRequestMultipleBlocksClosedRange() {
                        // First we create the blocks
                        final List<Block> blocksZeroToTwo =
                                SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksBatched(0, 2);
                        // Supply the needed items
                        for (final Block block : blocksZeroToTwo) {
                            blockMessaging.sendBlockItems(toBlockItems(block.items()));
                        }
                        // Then, we create the request for blocks 0 to 2
                        final SubscribeStreamRequest request = SubscribeStreamRequest.newBuilder()
                                .startBlockNumber(0L)
                                .endBlockNumber(2L)
                                .build();
                        // Send the request
                        toPluginPipe.onNext(SubscribeStreamRequest.PROTOBUF.toBytes(request));
                        // Wait for responses
                        final int expectedResponses = (2 * blocksZeroToTwo.size())
                                + 1; // three with items and end of block, one with success status
                        awaitResponse(fromPluginBytes, expectedResponses);
                        // Assert responses count and status success
                        assertThat(fromPluginBytes)
                                .hasSize(expectedResponses)
                                .last()
                                .extracting(responseExtractor)
                                .isNotNull()
                                .returns(Code.SUCCESS, responseStatusExtractor);
                        // Extract and assert block items response
                        final List<Bytes> blockItemResponses = fromPluginBytes.subList(0, fromPluginBytes.size());
                        assertBlockItemsMatch(blocksZeroToTwo, blockItemResponses);
                    }

                    /**
                     * This test aims to assert that when a valid request for
                     * multiple blocks is sent to the plugin, a response(s) with
                     * the block items is returned, followed by a success status
                     * response. Here, the blocks requested do not exist at the
                     * time of the request, but are supplied later from history.
                     * This is a request for future blocks, but the request can
                     * be fulfilled because we have some historical data, thus
                     * we can determine how far in the future the requested
                     * blocks are.
                     */
                    @Test
                    @DisplayName("Test Subscriber: Valid Request Multiple Blocks Future From History")
                    void testSuccessfulRequestMultipleBlocksClosedRangeFutureFromHistory() {
                        // First we create the blocks
                        final List<Block> blocksOneToTwo =
                                SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksBatched(1, 2);
                        // Supply a block, otherwise if there is no data at all, we
                        // cannot fulfill the request, because we cannot determine
                        // how much in the future the requested block is
                        final List<Block> blockZero =
                                SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksBatched(0, 0);
                        final List<BlockItem> blockZeroItems =
                                blockZero.getFirst().items();
                        historicalBlockFacility.handleBlockItemsReceived(toBlockItems(blockZeroItems));
                        // Then, we create the request for blocks 1 to 2
                        final SubscribeStreamRequest request = SubscribeStreamRequest.newBuilder()
                                .startBlockNumber(1L)
                                .endBlockNumber(2L)
                                .build();
                        // Send the request
                        toPluginPipe.onNext(SubscribeStreamRequest.PROTOBUF.toBytes(request));
                        // Now supply the requested blocks to history
                        for (final Block block : blocksOneToTwo) {
                            historicalBlockFacility.handleBlockItemsReceived(toBlockItems(block.items()));
                        }
                        // Wait for responses
                        final int expectedResponses = (2 * blocksOneToTwo.size())
                                + 1; // two with items and end of block, one with success status
                        awaitResponse(fromPluginBytes, expectedResponses);
                        // Assert responses count and status success
                        assertThat(fromPluginBytes)
                                .hasSize(expectedResponses)
                                .last()
                                .extracting(responseExtractor)
                                .isNotNull()
                                .returns(Code.SUCCESS, responseStatusExtractor);
                        // Extract and assert block items response
                        final List<Bytes> blockItemResponses = fromPluginBytes.subList(0, fromPluginBytes.size());
                        assertBlockItemsMatch(blocksOneToTwo, blockItemResponses);
                    }

                    /**
                     * This test aims to assert that when a valid request for
                     * multiple blocks is sent to the plugin, a response(s) with
                     * the block items is returned, followed by a success status
                     * response. Here, the blocks requested do not exist at the
                     * time of the request, but are supplied later from live
                     * data. This is a request for future blocks, but the
                     * request can be fulfilled because we have some historical
                     * data, thus we can determine how far in the future the
                     * requested blocks are.
                     */
                    @Test
                    @DisplayName("Test Subscriber: Valid Request Multiple Blocks Future From Live")
                    void testSuccessfulRequestMultipleBlocksClosedRangeFutureFromLive() {
                        // First we create the blocks
                        final List<Block> blocksOneToTwo =
                                SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksBatched(1, 2);
                        // Supply a block, otherwise if there is no data at all, we
                        // cannot fulfill the request, because we cannot determine
                        // how much in the future the requested block is
                        final List<Block> blockZero =
                                SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksBatched(0, 0);
                        final List<BlockItem> blockZeroItems =
                                blockZero.getFirst().items();
                        historicalBlockFacility.handleBlockItemsReceived(toBlockItems(blockZeroItems));
                        // Then, we create the request for blocks 1 to 2
                        final SubscribeStreamRequest request = SubscribeStreamRequest.newBuilder()
                                .startBlockNumber(1L)
                                .endBlockNumber(2L)
                                .build();
                        // Send the request
                        toPluginPipe.onNext(SubscribeStreamRequest.PROTOBUF.toBytes(request));
                        // Disable history to listen to live data so we ensure
                        // blocks will be supplied from live
                        historicalBlockFacility.setDisablePlugin();
                        // Now supply the requested blocks to history
                        for (final Block block : blocksOneToTwo) {
                            blockMessaging.sendBlockItems(toBlockItems(block.items()));
                        }
                        // Wait for responses
                        final int expectedResponses = (2 * blocksOneToTwo.size())
                                + 1; // two with items and end of block, one with success status
                        awaitResponse(fromPluginBytes, expectedResponses);
                        // Assert responses count and status success
                        assertThat(fromPluginBytes)
                                .hasSize(expectedResponses)
                                .last()
                                .extracting(responseExtractor)
                                .isNotNull()
                                .returns(Code.SUCCESS, responseStatusExtractor);
                        // Extract and assert block items response
                        final List<Bytes> blockItemResponses = fromPluginBytes.subList(0, fromPluginBytes.size());
                        assertBlockItemsMatch(blocksOneToTwo, blockItemResponses);
                    }
                }

                /**
                 * Multiple block requests with an open range (start defined,
                 * end open).
                 */
                @Nested
                @DisplayName("Multiple Block Request Start Defined - End Open Range Tests")
                class MultipleBlockRequestStartDefinedEndOpenRangeTests {
                    /**
                     * This test aims to assert that when a valid request for
                     * multiple blocks is sent to the plugin, a response(s) with
                     * the block items is returned, followed by a success status
                     * response. This request starts with a defined block and
                     * continues indefinitely. Here, the first block requested
                     * already exists before the request is made.
                     */
                    @Test
                    @DisplayName(
                            "Test Subscriber: Valid Request Multiple Blocks Start Defined - End Open Ranged, Start Already Existing")
                    void testSuccessfulRequestMultipleBlocksStartDefinedEndOpenRange() {
                        // First we create the blocks
                        final List<Block> blocksZeroToTwo =
                                SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksBatched(0, 2);
                        // Supply the needed items
                        for (final Block block : blocksZeroToTwo) {
                            historicalBlockFacility.handleBlockItemsReceived(toBlockItems(block.items()));
                        }
                        // Then, we create the request for blocks 0 to open
                        final SubscribeStreamRequest request = SubscribeStreamRequest.newBuilder()
                                .startBlockNumber(0L)
                                .endBlockNumber(-1L)
                                .build();
                        // Send the request
                        toPluginPipe.onNext(SubscribeStreamRequest.PROTOBUF.toBytes(request));
                        // Wait for responses
                        final int expectedBlockItemResponses =
                                2 * blocksZeroToTwo.size(); // three with items and end block
                        awaitResponse(fromPluginBytes, expectedBlockItemResponses);
                        // now we need to stop the plugin to end the open range request and
                        // receive the success status response
                        plugin.stop();
                        final int expectedResponses =
                                expectedBlockItemResponses + 1; // three with items, one with success status
                        // Assert responses count and status success
                        assertThat(fromPluginBytes)
                                .hasSize(expectedResponses)
                                .last()
                                .extracting(responseExtractor)
                                .isNotNull()
                                .returns(Code.SUCCESS, responseStatusExtractor);
                        // Extract and assert block items response
                        final List<Bytes> blockItemResponses = fromPluginBytes.subList(0, fromPluginBytes.size());
                        assertBlockItemsMatch(blocksZeroToTwo, blockItemResponses);
                    }

                    /**
                     * This test aims to assert that when a valid request for
                     * multiple blocks is sent to the plugin, a response(s) with
                     * the block items is returned, followed by a success status
                     * response. This request starts with a defined block and
                     * continues indefinitely. Here, the first block requested
                     * does not exist at the time of the request, but is
                     * supplied later from history. This is a request for future
                     * blocks, but the request can be fulfilled because we have
                     * some historical data, thus we can determine how far in
                     * the future the requested blocks are.
                     */
                    @Test
                    @DisplayName(
                            "Test Subscriber: Valid Request Multiple Blocks Start Defined - End Open Range, Future From History")
                    void testSuccessfulRequestMultipleBlocksStartDefinedEndOpenRangeFutureFromHistory() {
                        // First we create the blocks
                        final List<Block> blocksOneToTwo =
                                SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksBatched(1, 2);
                        // Supply a block, otherwise if there is no data at all, we
                        // cannot fulfill the request, because we cannot determine
                        // how much in the future the requested block is
                        final List<Block> blockZero =
                                SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksBatched(0, 0);
                        final List<BlockItem> blockZeroItems =
                                blockZero.getFirst().items();
                        historicalBlockFacility.handleBlockItemsReceived(toBlockItems(blockZeroItems));
                        // Then, we create the request for blocks 1 to open
                        final SubscribeStreamRequest request = SubscribeStreamRequest.newBuilder()
                                .startBlockNumber(1L)
                                .endBlockNumber(-1L)
                                .build();
                        // Send the request
                        toPluginPipe.onNext(SubscribeStreamRequest.PROTOBUF.toBytes(request));
                        // Now supply the requested blocks to history
                        for (final Block block : blocksOneToTwo) {
                            historicalBlockFacility.handleBlockItemsReceived(toBlockItems(block.items()));
                        }
                        // Wait for responses
                        final int expectedBlockItemResponses =
                                2 * blocksOneToTwo.size(); // two with items and end block
                        awaitResponse(fromPluginBytes, expectedBlockItemResponses);
                        // now we need to stop the plugin to end the open range request and
                        // receive the success status response
                        plugin.stop();
                        final int expectedResponses =
                                expectedBlockItemResponses + 1; // two with items, one with success status
                        // Assert responses count and status success
                        assertThat(fromPluginBytes)
                                .hasSize(expectedResponses)
                                .last()
                                .extracting(responseExtractor)
                                .isNotNull()
                                .returns(Code.SUCCESS, responseStatusExtractor);
                        // Extract and assert block items response
                        final List<Bytes> blockItemResponses = fromPluginBytes.subList(0, fromPluginBytes.size());
                        assertBlockItemsMatch(blocksOneToTwo, blockItemResponses);
                    }

                    /**
                     * This test aims to assert that when a valid request for
                     * multiple blocks is sent to the plugin, a response(s) with
                     * the block items is returned, followed by a success status
                     * response. This request starts with a defined block and
                     * continues indefinitely. Here, the first block requested
                     * does not exist at the time of the request, but is
                     * supplied later from live data. This is a request for
                     * future blocks, but the request can be fulfilled because
                     * we have some historical data, thus we can determine how
                     * far in the future the requested blocks are.
                     */
                    @Test
                    @DisplayName(
                            "Test Subscriber: Valid Request Multiple Blocks Start Defined - End Open Range, Future From Live")
                    void testSuccessfulRequestMultipleBlocksStartDefinedEndOpenRangeFutureFromLive() {
                        // First we create the blocks
                        final List<Block> blocksOneToTwo =
                                SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksBatched(1, 2);
                        // Supply a block, otherwise if there is no data at all, we
                        // cannot fulfill the request, because we cannot determine
                        // how much in the future the requested block is
                        final List<Block> blockZero =
                                SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksBatched(0, 0);
                        final List<BlockItem> blockZeroItems =
                                blockZero.getFirst().items();
                        historicalBlockFacility.handleBlockItemsReceived(toBlockItems(blockZeroItems));
                        // Then, we create the request for blocks 1 to open
                        final SubscribeStreamRequest request = SubscribeStreamRequest.newBuilder()
                                .startBlockNumber(1L)
                                .endBlockNumber(-1L)
                                .build();
                        // Send the request
                        toPluginPipe.onNext(SubscribeStreamRequest.PROTOBUF.toBytes(request));
                        // Disable history to listen to live data so we ensure
                        // blocks will be supplied from live
                        historicalBlockFacility.setDisablePlugin();
                        // Now supply the requested blocks to history
                        for (final Block block : blocksOneToTwo) {
                            blockMessaging.sendBlockItems(toBlockItems(block.items()));
                        }
                        // Wait for responses
                        final int expectedBlockItemResponses =
                                2 * blocksOneToTwo.size(); // two with items and end block
                        awaitResponse(fromPluginBytes, expectedBlockItemResponses);
                        // now we need to stop the plugin to end the open range request and
                        // receive the success status response
                        plugin.stop();
                        final int expectedResponses =
                                expectedBlockItemResponses + 1; // two with items, one with success status
                        // Assert responses count and status success
                        assertThat(fromPluginBytes)
                                .hasSize(expectedResponses)
                                .last()
                                .extracting(responseExtractor)
                                .isNotNull()
                                .returns(Code.SUCCESS, responseStatusExtractor);
                        // Extract and assert block items response
                        final List<Bytes> blockItemResponses = fromPluginBytes.subList(0, fromPluginBytes.size());
                        assertBlockItemsMatch(blocksOneToTwo, blockItemResponses);
                    }
                }

                /**
                 * Multiple block requests with a range starting from the
                 * first available block in history (start = -1), and a defined
                 * end block.
                 */
                @Nested
                @DisplayName("Multiple Block Request Start From First Available - End Defined Range Tests")
                class MultipleBlockRequestStartFromFirstAvailableEndDefinedRangeTests {
                    /**
                     * This test aims to assert that when a valid request for
                     * multiple blocks is sent to the plugin, a response(s) with
                     * the block items is returned, followed by a success status
                     * response. This request starts with the first available
                     * block and continues up to a defined block (including).
                     * Here, the blocks requested already exist before the
                     * request is made.
                     */
                    @Test
                    @DisplayName(
                            "Test Subscriber: Valid Request Multiple Blocks Start From First Available - End Defined Range, Blocks Already Existing")
                    void testSuccessfulRequestMultipleBlocksStartFromFirstAvailableEndDefinedRange() {
                        // First we create the blocks
                        final List<Block> blocksZeroToTwo =
                                SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksBatched(0, 2);
                        // Supply the needed items
                        for (final Block block : blocksZeroToTwo) {
                            historicalBlockFacility.handleBlockItemsReceived(toBlockItems(block.items()));
                        }
                        // Then, we create the request for blocks first available (0) to 2
                        final SubscribeStreamRequest request = SubscribeStreamRequest.newBuilder()
                                .startBlockNumber(-1L)
                                .endBlockNumber(2L)
                                .build();
                        // Send the request
                        toPluginPipe.onNext(SubscribeStreamRequest.PROTOBUF.toBytes(request));
                        // Wait for responses
                        final int expectedResponses = (2 * blocksZeroToTwo.size())
                                + 1; // three with items and end block, one with success status
                        awaitResponse(fromPluginBytes, expectedResponses);
                        // Assert responses count and status success
                        assertThat(fromPluginBytes)
                                .hasSize(expectedResponses)
                                .last()
                                .extracting(responseExtractor)
                                .isNotNull()
                                .returns(Code.SUCCESS, responseStatusExtractor);
                        // Extract and assert block items response
                        final List<Bytes> blockItemResponses = fromPluginBytes.subList(0, fromPluginBytes.size());
                        assertBlockItemsMatch(blocksZeroToTwo, blockItemResponses);
                    }

                    /**
                     * This test aims to assert that when a valid request for
                     * multiple blocks is sent to the plugin, a response(s) with
                     * the block items is returned, followed by a success status
                     * response. Here, the blocks requested do not exist at the
                     * time of the request, but are supplied later from history.
                     * The request starts from the first available block in
                     * history and is up to a defined end block. This is a
                     * request for future blocks, but the request can be
                     * fulfilled because we have some historical data, thus we
                     * can determine how far in the future the requested blocks
                     * are.
                     */
                    @Test
                    @DisplayName(
                            "Test Subscriber: Valid Request Multiple Blocks Start From First Available - End Defined Range Future From History")
                    void testSuccessfulRequestMultipleBlocksStartFromFirstAvailableEndDefinedRangeFutureFromHistory() {
                        // First we create the blocks
                        final List<Block> blocksZeroToTwo =
                                SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksBatched(0, 2);
                        // Supply a block, otherwise if there is no data at all, we
                        // cannot fulfill the request, because we cannot determine
                        // how much in the future the requested block is
                        final List<BlockItem> blockZeroItems =
                                blocksZeroToTwo.getFirst().items();
                        historicalBlockFacility.handleBlockItemsReceived(toBlockItems(blockZeroItems));
                        // Then, we create the request for blocks first available (0) to 2
                        final SubscribeStreamRequest request = SubscribeStreamRequest.newBuilder()
                                .startBlockNumber(-1L)
                                .endBlockNumber(2L)
                                .build();
                        // Send the request
                        toPluginPipe.onNext(SubscribeStreamRequest.PROTOBUF.toBytes(request));
                        // Now supply the requested blocks to history
                        for (final Block block : blocksZeroToTwo.subList(1, blocksZeroToTwo.size())) {
                            historicalBlockFacility.handleBlockItemsReceived(toBlockItems(block.items()));
                        }
                        // Wait for responses
                        final int expectedResponses =
                                (2 * blocksZeroToTwo.size()) + 1; // three with items, one with success status
                        awaitResponse(fromPluginBytes, expectedResponses);
                        // Assert responses count and status success
                        assertThat(fromPluginBytes)
                                .hasSize(expectedResponses)
                                .last()
                                .extracting(responseExtractor)
                                .isNotNull()
                                .returns(Code.SUCCESS, responseStatusExtractor);
                        // Extract and assert block items response
                        final List<Bytes> blockItemResponses = fromPluginBytes.subList(0, fromPluginBytes.size());
                        assertBlockItemsMatch(blocksZeroToTwo, blockItemResponses);
                    }

                    /**
                     * This test aims to assert that when a valid request for
                     * multiple blocks is sent to the plugin, a response(s) with
                     * the block items is returned, followed by a success status
                     * response. Here, the blocks requested do not exist at the
                     * time of the request, but are supplied later from live
                     * data. The request starts from the first available block
                     * in history and is up to a defined end block. This is a
                     * request for future blocks, but the request can be
                     * fulfilled because we have some historical data, thus we
                     * can determine how far in the future the requested blocks
                     * are.
                     */
                    @Test
                    @DisplayName(
                            "Test Subscriber: Valid Request Multiple Blocks Start From First Available - End Defined Range Future From Live")
                    void testSuccessfulRequestMultipleBlocksStartFromFirstAvailableEndDefinedRangeFutureFromLive() {
                        // First we create the blocks
                        final List<Block> blocksZeroToTwo =
                                SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksBatched(0, 2);
                        // Supply a block, otherwise if there is no data at all, we
                        // cannot fulfill the request, because we cannot determine
                        // how much in the future the requested block is
                        final List<BlockItem> blockZeroItems =
                                blocksZeroToTwo.getFirst().items();
                        historicalBlockFacility.handleBlockItemsReceived(toBlockItems(blockZeroItems));
                        // Then, we create the request for blocks first available (0) to 2
                        final SubscribeStreamRequest request = SubscribeStreamRequest.newBuilder()
                                .startBlockNumber(-1L)
                                .endBlockNumber(2L)
                                .build();
                        // Send the request
                        toPluginPipe.onNext(SubscribeStreamRequest.PROTOBUF.toBytes(request));
                        // Disable history to listen to live data so we ensure
                        // blocks will be supplied from live
                        historicalBlockFacility.setDisablePlugin();
                        // Now supply the requested blocks to live ring buffer
                        for (final Block block : blocksZeroToTwo.subList(1, blocksZeroToTwo.size())) {
                            blockMessaging.sendBlockItems(toBlockItems(block.items()));
                        }
                        // Wait for responses
                        final int expectedResponses = (2 * blocksZeroToTwo.size())
                                + 1; // three with items and end of block, one with success status
                        awaitResponse(fromPluginBytes, expectedResponses);
                        // Assert responses count and status success
                        assertThat(fromPluginBytes)
                                .hasSize(expectedResponses)
                                .last()
                                .extracting(responseExtractor)
                                .isNotNull()
                                .returns(Code.SUCCESS, responseStatusExtractor);
                        // Extract and assert block items response
                        final List<Bytes> blockItemResponses = fromPluginBytes.subList(0, fromPluginBytes.size());
                        assertBlockItemsMatch(blocksZeroToTwo, blockItemResponses);
                    }
                }

                /**
                 * Live stream requests (start = -1, end = -1).
                 */
                @Nested
                @DisplayName("Live Stream Request Tests")
                class LiveStreamRequestTest {
                    /**
                     * This test aims to assert that when a valid request for
                     * live stream is sent to the plugin, a response(s) with the
                     * block items is returned, and items keep streaming
                     * indefinitely. This is a request for live blocks, which
                     * can always be fulfilled.
                     */
                    @Test
                    @DisplayName("Test Subscriber: Valid Request Live Stream")
                    void testSuccessfulRequestLiveStream() {
                        // First we create the blocks
                        final List<Block> blocksZeroToTwo =
                                SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksBatched(0, 2);
                        // Then, we create the request for live stream
                        final SubscribeStreamRequest request = SubscribeStreamRequest.newBuilder()
                                .startBlockNumber(-1L)
                                .endBlockNumber(-1L)
                                .build();
                        // Send the request
                        toPluginPipe.onNext(SubscribeStreamRequest.PROTOBUF.toBytes(request));
                        // Disable history to listen to live data so we ensure
                        // blocks will be supplied from live
                        historicalBlockFacility.setDisablePlugin();
                        // Now supply the requested blocks to live ring buffer
                        for (final Block block : blocksZeroToTwo) {
                            blockMessaging.sendBlockItems(toBlockItems(block.items()));
                        }
                        // Wait for responses for block items
                        final int expectedBlockItemResponses =
                                2 * blocksZeroToTwo.size(); // three with items and end block
                        awaitResponse(fromPluginBytes, expectedBlockItemResponses);
                        // now we need to stop the plugin to end the live stream request and
                        // receive the success status response
                        plugin.stop();
                        final int expectedResponses = expectedBlockItemResponses + 1;
                        // Assert responses count and status success
                        assertThat(fromPluginBytes).hasSize(expectedResponses);
                        // Extract and assert block items response
                        final List<Bytes> blockItemResponses = fromPluginBytes.subList(0, fromPluginBytes.size());
                        assertBlockItemsMatch(blocksZeroToTwo, blockItemResponses);
                    }

                    /**
                     * This test aims to assert that when a valid request for
                     * live stream is sent to the plugin, a response(s) with the
                     * block items is returned, and items keep streaming
                     * indefinitely. This is a request for live blocks, which
                     * can always be fulfilled. Here, we supply the first block
                     * from live stream, and then we supply following from
                     * history. This is to ensure that the plugin can handle
                     * blocks coming from both live and history when in live
                     * stream mode.
                     */
                    @Test
                    @DisplayName("Test Subscriber: Valid Request Live Stream From Live Then History")
                    @Disabled(
                            "@todo(1673) should not be failing, we fail because history not permitted, we think we jump ahead")
                    void testSuccessfulRequestLiveStreamFromLiveThenHistory() {
                        // First we create the blocks
                        final List<Block> blocksZeroToTwo =
                                SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksBatched(0, 2);
                        // Then, we create the request for live stream
                        final SubscribeStreamRequest request = SubscribeStreamRequest.newBuilder()
                                .startBlockNumber(-1L)
                                .endBlockNumber(-1L)
                                .build();
                        // Send the request
                        toPluginPipe.onNext(SubscribeStreamRequest.PROTOBUF.toBytes(request));
                        // Send the first block from live
                        final List<BlockItem> blockZeroItems =
                                blocksZeroToTwo.getFirst().items();
                        blockMessaging.sendBlockItems(toBlockItems(blockZeroItems));
                        // Await the first response
                        awaitResponse(fromPluginBytes, 1);
                        // Now supply the following blocks from history
                        for (final Block block : blocksZeroToTwo.subList(1, blocksZeroToTwo.size())) {
                            historicalBlockFacility.handleBlockItemsReceived(toBlockItems(block.items()));
                        }
                        // Wait for responses for block items
                        awaitResponse(fromPluginBytes, 2);
                        // now we need to stop the plugin to end the live stream request and
                        // receive the success status response
                        plugin.stop();
                        final int expectedResponses = (2 * blocksZeroToTwo.size())
                                + 1; // three with items and end of block, one with success status
                        // Assert responses count and status success
                        assertThat(fromPluginBytes)
                                .hasSize(expectedResponses)
                                .last()
                                .extracting(responseExtractor)
                                .isNotNull()
                                .returns(Code.SUCCESS, responseStatusExtractor);
                        // Extract and assert block items response
                        final List<Bytes> blockItemResponses = fromPluginBytes.subList(0, fromPluginBytes.size());
                        assertBlockItemsMatch(blocksZeroToTwo, blockItemResponses);
                    }

                    /**
                     * This test aims to assert that when a valid request for
                     * live stream is sent to the plugin, a response with the
                     * block items is returned, and items keep streaming
                     * indefinitely. This is a request for live blocks, which
                     * can always be fulfilled. Here, we simulate that the
                     * subscriber has subscribed mid-block, meaning the live
                     * ring buffer holds partially the current block. If that is
                     * so, the plugin should discard that partial block and
                     * start sending from the next block.
                     */
                    @Test
                    @DisplayName("Test Subscriber: Valid Request Live Stream Mid-Block Subscription")
                    void testSuccessfulRequestLiveStreamMidBlockSubscription() {
                        // First we create the blocks
                        final List<Block> blocksZeroToTwo =
                                SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksBatched(0, 2);
                        // Then, we create the request for live stream
                        final SubscribeStreamRequest request = SubscribeStreamRequest.newBuilder()
                                .startBlockNumber(-1L)
                                .endBlockNumber(-1L)
                                .build();
                        // Send the request
                        toPluginPipe.onNext(SubscribeStreamRequest.PROTOBUF.toBytes(request));
                        // Send a partial block from live, simulating a mid-block subscription
                        final List<BlockItem> firstBlock =
                                blocksZeroToTwo.getFirst().items();
                        blockMessaging.sendBlockItems(toBlockItems(
                                firstBlock,
                                false,
                                firstBlock.getFirst().blockHeader().number()));
                        // Now supply the following blocks from history
                        final List<Block> blocksOneToTwo = blocksZeroToTwo.subList(1, blocksZeroToTwo.size());
                        for (final Block block : blocksOneToTwo) {
                            blockMessaging.sendBlockItems(toBlockItems(block.items()));
                        }
                        // Wait for responses for block items
                        final int expectedBlockItemResponses =
                                2 * blocksOneToTwo.size(); // two with items and end block
                        awaitResponse(fromPluginBytes, expectedBlockItemResponses);
                        // now we need to stop the plugin to end the live stream request and
                        // receive the success status response
                        plugin.stop();
                        final int expectedResponses =
                                expectedBlockItemResponses + 1; // two with items, one with success status
                        // Assert responses count and status success
                        assertThat(fromPluginBytes)
                                .hasSize(expectedResponses)
                                .last()
                                .extracting(responseExtractor)
                                .isNotNull()
                                .returns(Code.SUCCESS, responseStatusExtractor);
                        // Extract and assert block items response
                        final List<Bytes> blockItemResponses = fromPluginBytes.subList(0, fromPluginBytes.size());
                        assertBlockItemsMatch(blocksOneToTwo, blockItemResponses);
                    }
                }

                /**
                 * This test aims to assert that when we have blocks without
                 * headers we don't send those partial blocks to the subscriber.
                 *
                 * Note: Subscribers are supposed to forward the stream as received from live and there are (rare) conditions
                 * where that might result in sending partial blocks. this test might fail even when the subscriber is behaving correctly.
                 */
                @Test
                @DisplayName("Test Subscriber: Valid Request Live Stream Subscription With Some Blocks Without Headers")
                void testSuccessfulRequestLiveStreamWithBlockAndSomeBlocksWithoutHeadersSubscription() {
                    // First we create the blocks
                    final List<Block> blocksZeroToTwo =
                            SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksBatched(0, 2);
                    // Then, we create the request for live stream
                    final SubscribeStreamRequest request = SubscribeStreamRequest.newBuilder()
                            .startBlockNumber(-1L)
                            .endBlockNumber(-1L)
                            .build();
                    // Send the request
                    toPluginPipe.onNext(SubscribeStreamRequest.PROTOBUF.toBytes(request));
                    // Send a block 0
                    final List<BlockItem> firstBlock =
                            blocksZeroToTwo.getFirst().items();
                    blockMessaging.sendBlockItems(toBlockItems(
                            firstBlock,
                            true,
                            firstBlock.getFirst().blockHeader().number()));
                    // Now supply the following blocks without headers
                    final List<Block> blocksOneToTwo = blocksZeroToTwo.subList(1, blocksZeroToTwo.size());
                    for (final Block block : blocksOneToTwo) {
                        blockMessaging.sendBlockItems(toBlockItems(
                                block.items(),
                                false,
                                firstBlock.getFirst().blockHeader().number()));
                    }
                    // Wait for responses for block items
                    final int expectedBlockItemResponses = 2; // only one with items and end block
                    awaitResponse(fromPluginBytes, expectedBlockItemResponses);
                    // now we need to stop the plugin to end the live stream request and
                    // receive the success status response
                    plugin.stop();
                    final int expectedResponses = expectedBlockItemResponses + 1; // items and one with success status
                    // Assert responses count and status success
                    assertThat(fromPluginBytes)
                            .hasSize(expectedResponses)
                            .last()
                            .extracting(responseExtractor)
                            .isNotNull()
                            .returns(Code.SUCCESS, responseStatusExtractor);
                    // Extract and assert block items response
                    final List<Bytes> blockItemResponses = fromPluginBytes.subList(0, fromPluginBytes.size());
                    assertBlockItemsMatch(List.of(blocksZeroToTwo.getFirst()), blockItemResponses);
                }
            }

            /**
             * Negative tests for the subscriber plugin.
             */
            @Nested
            @DisplayName("Negative Subscriber Tests")
            class NegativeSubscriberTests {
                /**
                 * This test aims to assert that when a valid request is sent to
                 * the plugin, but the request is starts with block too far in
                 * the future, an error response with the expected error code is
                 * returned.
                 */
                @Test
                @DisplayName("Test Subscriber: Valid Request Too Far In Future")
                void testSubscriberValidRequestTooFarInFuture() {
                    // Supply a block, otherwise if there is no data at all, we
                    // cannot fulfill the request, because we cannot determine
                    // how much in the future the requested block is
                    final List<Block> blockZero =
                            SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksBatched(0, 0);
                    final List<BlockItem> blockZeroItems = blockZero.getFirst().items();
                    historicalBlockFacility.handleBlockItemsReceived(toBlockItems(blockZeroItems));
                    // Then, we create the request for block 10_000, which is too far in the future
                    final SubscribeStreamRequest request = SubscribeStreamRequest.newBuilder()
                            .startBlockNumber(10_000L)
                            .endBlockNumber(10_000L)
                            .build();
                    // Send the request
                    toPluginPipe.onNext(SubscribeStreamRequest.PROTOBUF.toBytes(request));
                    // Wait for responses
                    final int expectedResponses = 1; // one with error status
                    awaitResponse(fromPluginBytes, expectedResponses);
                    // Assert responses count and status success
                    assertThat(fromPluginBytes)
                            .hasSize(expectedResponses)
                            .first()
                            .extracting(responseExtractor)
                            .isNotNull()
                            .returns(Code.NOT_AVAILABLE, responseStatusExtractor);
                }

                /**
                 * This test aims to asser that an invalid request sent to the
                 * plugin will result in an error response with the expected
                 * error code.
                 */
                @ParameterizedTest
                @MethodSource("invalidRequests")
                @DisplayName("Test Subscriber: Invalid Request")
                void testSubscriberInvalidRequest(final SubscribeStreamRequest request, final Code expectedCode) {
                    toPluginPipe.onNext(SubscribeStreamRequest.PROTOBUF.toBytes(request));
                    final int expectedResponses = 1; // one error status
                    awaitResponse(fromPluginBytes, expectedResponses);
                    assertThat(fromPluginBytes)
                            .hasSize(expectedResponses)
                            .first()
                            .extracting(responseExtractor)
                            .isNotNull()
                            .returns(expectedCode, responseStatusExtractor);
                }

                /**
                 * This test aims to asser that when valid requests are sent to
                 * the plugin, but the request cannot be fulfilled, the
                 * appropriate error code is returned.
                 */
                @ParameterizedTest
                @MethodSource("validRequests")
                @DisplayName("Test Subscriber: Valid Request, Cannot Fulfill")
                void testSubscriberValidRequestCannotFulfill(
                        final SubscribeStreamRequest request, final Code expectedCode) {
                    toPluginPipe.onNext(SubscribeStreamRequest.PROTOBUF.toBytes(request));
                    final int expectedResponses = 1; // one failure status
                    awaitResponse(fromPluginBytes, expectedResponses);
                    assertThat(fromPluginBytes)
                            .hasSize(1)
                            .first()
                            .extracting(responseExtractor)
                            .isNotNull()
                            .returns(expectedCode, responseStatusExtractor);
                }

                /**
                 * All types of invalid requests. Used in parameterized tests.
                 */
                private static Stream<Arguments> invalidRequests() {
                    return Stream.of(
                            Arguments.of( // start < -1L
                                    SubscribeStreamRequest.newBuilder()
                                            .startBlockNumber(-2L)
                                            .endBlockNumber(-1L)
                                            .build(),
                                    Code.INVALID_START_BLOCK_NUMBER),
                            Arguments.of( // end < -1L
                                    SubscribeStreamRequest.newBuilder()
                                            .startBlockNumber(-1L)
                                            .endBlockNumber(-2L)
                                            .build(),
                                    Code.INVALID_END_BLOCK_NUMBER),
                            Arguments.of( // end >= 0 && end < start, both whole numbers
                                    SubscribeStreamRequest.newBuilder()
                                            .startBlockNumber(10L)
                                            .endBlockNumber(5L)
                                            .build(),
                                    Code.INVALID_END_BLOCK_NUMBER));
                }

                /**
                 * All types of valid requests except request for live which
                 * can always be fulfilled.
                 */
                private static Stream<Arguments> validRequests() {
                    return Stream.of(
                            Arguments.of(
                                    SubscribeStreamRequest.newBuilder()
                                            .startBlockNumber(0L)
                                            .endBlockNumber(0L)
                                            .build(),
                                    Code.NOT_AVAILABLE),
                            Arguments.of(
                                    SubscribeStreamRequest.newBuilder()
                                            .startBlockNumber(0L)
                                            .endBlockNumber(10L)
                                            .build(),
                                    Code.NOT_AVAILABLE),
                            Arguments.of(
                                    SubscribeStreamRequest.newBuilder()
                                            .startBlockNumber(-1L)
                                            .endBlockNumber(10L)
                                            .build(),
                                    Code.NOT_AVAILABLE),
                            Arguments.of(
                                    SubscribeStreamRequest.newBuilder()
                                            .startBlockNumber(10L)
                                            .endBlockNumber(-1)
                                            .build(),
                                    Code.NOT_AVAILABLE));
                }
            }
        }
    }

    private static void assertBlockItemsMatch(final List<Block> expectedBlocks, final List<Bytes> blocksFromPipeline) {
        final List<List<BlockItem>> expectedBlockItems =
                expectedBlocks.stream().map(Block::items).toList();
        // Block items and end of block for each block
        long currentBlockNumber = -1;
        for (int i = 0, j = 0; i < expectedBlockItems.size(); i++) {
            final SubscribeStreamResponse subscribeResponse = responseExtractor.apply(blocksFromPipeline.get(i));
            if (subscribeResponse.hasBlockItems()) {
                final List<BlockItem> actual = subscribeResponse.blockItems().blockItems();
                final List<BlockItem> expected = expectedBlockItems.get(j++);
                assertBlockReceived(expected, actual);
                final BlockItem first = actual.getFirst();
                if (first.hasBlockHeader()) {
                    BlockHeader header = first.blockHeader();
                    currentBlockNumber = header.number();
                }
                final BlockItem last = actual.getLast();
            } else if (subscribeResponse.hasEndOfBlock()) {
                assertEndOfBlock(subscribeResponse, currentBlockNumber);
                currentBlockNumber = -1;
            } else {
                fail("Unexpected response type %s."
                        .formatted(subscribeResponse.response().kind()));
            }
        }
    }

    private static void assertEndOfBlock(final SubscribeStreamResponse response, final long blockNumber) {
        assertThat(response.hasEndOfBlock());
        assertThat(response.endOfBlock().blockNumber()).isEqualTo(blockNumber);
    }

    private static void assertBlockReceived(final List<BlockItem> expected, final List<BlockItem> actual) {
        assertThat(actual).hasSameSizeAs(expected);
        for (int i = 0; i < expected.size(); i++) {
            final BlockItem expectedItem = expected.get(i);
            final BlockItem actualItem = actual.get(i);
            final boolean areObjectsEqual = expectedItem.equals(actualItem);
            final String expectedHex = BlockItem.PROTOBUF.toBytes(expectedItem).toHex();
            final String actualHex = BlockItem.PROTOBUF.toBytes(actualItem).toHex();
            final boolean areHexEqual = expectedHex.equals(actualHex);
            assertThat(areObjectsEqual).isTrue();
            assertThat(areHexEqual).isTrue();
        }
    }

    private void awaitResponse(final List<Bytes> fromPluginBytes, final int requiredReplies) {
        int retries = 0;
        parkNanos(100_000L); // Always wait at least once.
        while (fromPluginBytes.size() < requiredReplies && retries < responseWaitLimit) {
            // wait for a response
            parkNanos(100_000L);
            retries++;
        }
        if (retries >= responseWaitLimit) {
            System.out.printf("Timed out waiting for %d responses%n", requiredReplies);
        }
    }
}
