// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.stream.subscriber;

import static java.util.concurrent.locks.LockSupport.parkNanos;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.hiero.block.node.app.fixtures.TestUtils.enableDebugLogging;

import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.grpc.ServiceInterface;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.stream.Stream;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.hiero.block.api.SubscribeStreamRequest;
import org.hiero.block.api.SubscribeStreamResponse;
import org.hiero.block.api.SubscribeStreamResponse.Code;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.node.app.fixtures.blocks.SimpleTestBlockItemBuilder;
import org.hiero.block.node.app.fixtures.plugintest.GrpcPluginTestBase;
import org.hiero.block.node.app.fixtures.plugintest.SimpleInMemoryHistoricalBlockFacility;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
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
        } catch (final Exception e) {
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
    class PluginTests extends GrpcPluginTestBase<SubscriberServicePlugin, ExecutorService> {
        // SETUP
        private final SimpleInMemoryHistoricalBlockFacility historicalBlockFacility;

        /**
         * Constructor.
         * Sets up environment for testing.
         */
        PluginTests() {
            super(Executors.newSingleThreadExecutor());
            final SubscriberServicePlugin toTest = new SubscriberServicePlugin();
            historicalBlockFacility = new SimpleInMemoryHistoricalBlockFacility();
            start(toTest, toTest.methods().getFirst(), historicalBlockFacility);
        }

        /**
         * Verifies that the service interface correctly registers and exposes
         * the server status method.
         */
        @Test
        @DisplayName("Test verify correct method/s registered for SubscriberServicePlugin in test base")
        void testVerifyCorrectMethodRegistered() {
            assertThat(serviceInterface)
                    .isNotNull()
                    .extracting(ServiceInterface::methods)
                    .asInstanceOf(InstanceOfAssertFactories.LIST)
                    .hasSize(1)
                    .containsExactly(plugin.methods().getFirst())
                    .actual()
                    .forEach(m -> System.out.println("Methods registered for plugin tests: " + m));
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
                     * This test aims to assert that when a valid request for a single block is sent to the plugin,
                     * a response with the block items is returned, followed by a success status response. Here, the
                     * block requested already exists before the request is made.
                     */
                    @Test
                    @DisplayName("Test Subscriber: Valid Request Single Block Already Existing")
                    void testSuccessfulRequestSingleBlock() {
                        // First we create the block
                        final List<List<BlockItem>> blockZero =
                                SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksBatched(0, 0);
                        // Supply the needed items
                        final List<BlockItem> expected = blockZero.getFirst();
                        blockMessaging.sendBlockItems(toBlockItems(expected));
                        // Then, we create the request for block 0
                        final SubscribeStreamRequest request = SubscribeStreamRequest.newBuilder()
                                .startBlockNumber(0L)
                                .endBlockNumber(0L)
                                .build();
                        // Send the request
                        toPluginPipe.onNext(SubscribeStreamRequest.PROTOBUF.toBytes(request));
                        // Wait for responses
                        final int expectedResponses = 2; // one with items, one with success status
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
                     * This test aims to assert that when a valid request for a single block is sent to the plugin,
                     * a response with the block items is returned, followed by a success status response. Here, the
                     * block requested does not exist at the time of the request, but is supplied later from
                     * history. This is a request for future block, but the request can be fulfilled because we have
                     * some historical data, thus we can determine how far in the future the requested block is.
                     */
                    @Test
                    @DisplayName("Test Subscriber: Valid Request Single Block Future From History")
                    void testSuccessfulRequestSingleBlockFutureFromHistory() {
                        // First we create the block
                        final List<List<BlockItem>> blockZero =
                                SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksBatched(0, 0);
                        // Supply a block, otherwise if there is no data at all, we
                        // cannot fulfill the request, because we cannot determine
                        // how much in the future the requested block is
                        final List<BlockItem> blockZeroItems = blockZero.getFirst();
                        historicalBlockFacility.handleBlockItemsReceived(toBlockItems(blockZeroItems));
                        // Then, we create the request for block 0
                        final SubscribeStreamRequest request = SubscribeStreamRequest.newBuilder()
                                .startBlockNumber(1L)
                                .endBlockNumber(1L)
                                .build();
                        // Send the request
                        toPluginPipe.onNext(SubscribeStreamRequest.PROTOBUF.toBytes(request));
                        // Now supply the requested block to history
                        final List<List<BlockItem>> blockOne =
                                SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksBatched(1, 1);
                        final List<BlockItem> expected = blockOne.getFirst();
                        historicalBlockFacility.handleBlockItemsReceived(toBlockItems(expected));
                        // Wait for responses
                        final int expectedResponses = 2; // one with items, one with success status
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
                     * This test aims to assert that when a valid request for a single block is sent to the plugin,
                     * a response with the block items is returned, followed by a success status response. Here, the
                     * block requested does not exist at the time of the request, but is supplied later from live
                     * data. This is a request for future block, but the request can be fulfilled because we have
                     * some historical data, thus we can determine how far in the future the requested block is.
                     */
                    @Test
                    @DisplayName("Test Subscriber: Valid Request Single Block Future From Live")
                    void testSuccessfulRequestSingleBlockFutureFromLive() {
                        // First we create the block
                        final List<List<BlockItem>> blockZero =
                                SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksBatched(0, 0);
                        // Supply a block, otherwise if there is no data at all, we
                        // cannot fulfill the request, because we cannot determine
                        // how much in the future the requested block is
                        final List<BlockItem> blockZeroItems = blockZero.getFirst();
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
                        final List<List<BlockItem>> blockOne =
                                SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksBatched(1, 1);
                        final List<BlockItem> expected = blockOne.getFirst();
                        blockMessaging.sendBlockItems(toBlockItems(expected));
                        // Wait for responses
                        final int expectedResponses = 2; // one with items, one with success status
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
                }

                /**
                 * Multiple block requests with a closed range (start and end defined).
                 */
                @Nested
                @DisplayName("Multiple Block Request Closed Range Tests")
                class MultipleBlockRequestClosedRangeTests {
                    /**
                     * This test aims to assert that when a valid request for multiple blocks is sent to the plugin,
                     * a response with the block items is returned, followed by a success status response. Here, the
                     * blocks requested already exist before the request is made.
                     */
                    @Test
                    @DisplayName("Test Subscriber: Valid Request Multiple Blocks Already Existing")
                    void testSuccessfulRequestMultipleBlocksClosedRange() {
                        // First we create the blocks
                        final List<List<BlockItem>> blocksZeroToTwo =
                                SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksBatched(0, 2);
                        // Supply the needed items
                        for (final List<BlockItem> block : blocksZeroToTwo) {
                            blockMessaging.sendBlockItems(toBlockItems(block));
                        }
                        // Then, we create the request for blocks 0 to 2
                        final SubscribeStreamRequest request = SubscribeStreamRequest.newBuilder()
                                .startBlockNumber(0L)
                                .endBlockNumber(2L)
                                .build();
                        // Send the request
                        toPluginPipe.onNext(SubscribeStreamRequest.PROTOBUF.toBytes(request));
                        // Wait for responses
                        final int expectedResponses =
                                blocksZeroToTwo.size() + 1; // three with items, one with success status
                        awaitResponse(fromPluginBytes, expectedResponses);
                        // Assert responses count and status success
                        assertThat(fromPluginBytes)
                                .hasSize(expectedResponses)
                                .last()
                                .extracting(responseExtractor)
                                .isNotNull()
                                .returns(Code.SUCCESS, responseStatusExtractor);
                        // Extract and assert block items response
                        final List<Bytes> blockItemRequests = fromPluginBytes.subList(0, fromPluginBytes.size());
                        for (int i = 0; i < blocksZeroToTwo.size(); i++) {
                            final List<BlockItem> expected = blocksZeroToTwo.get(i);
                            final List<BlockItem> actual = responseExtractor
                                    .apply(blockItemRequests.get(i))
                                    .blockItems()
                                    .blockItems();
                            assertBlockReceived(expected, actual);
                        }
                    }

                    /**
                     * This test aims to assert that when a valid request for multiple blocks is sent to the plugin,
                     * a response with the block items is returned, followed by a success status response. Here, the
                     * blocks requested do not exist at the time of the request, but are supplied later from history.
                     * This is a request for future blocks, but the request can be fulfilled because we have some
                     * historical data, thus we can determine how far in the future the requested blocks are.
                     */
                    @Test
                    @DisplayName("Test Subscriber: Valid Request Multiple Blocks Future From History")
                    void testSuccessfulRequestMultipleBlocksClosedRangeFutureFromHistory() {
                        // First we create the blocks
                        final List<List<BlockItem>> blocksOneToTwo =
                                SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksBatched(1, 2);
                        // Supply a block, otherwise if there is no data at all, we
                        // cannot fulfill the request, because we cannot determine
                        // how much in the future the requested block is
                        final List<List<BlockItem>> blockZero =
                                SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksBatched(0, 0);
                        final List<BlockItem> blockZeroItems = blockZero.getFirst();
                        historicalBlockFacility.handleBlockItemsReceived(toBlockItems(blockZeroItems));
                        // Then, we create the request for blocks 1 to 2
                        final SubscribeStreamRequest request = SubscribeStreamRequest.newBuilder()
                                .startBlockNumber(1L)
                                .endBlockNumber(2L)
                                .build();
                        // Send the request
                        toPluginPipe.onNext(SubscribeStreamRequest.PROTOBUF.toBytes(request));
                        // Now supply the requested blocks to history
                        for (final List<BlockItem> block : blocksOneToTwo) {
                            historicalBlockFacility.handleBlockItemsReceived(toBlockItems(block));
                        }
                        // Wait for responses
                        final int expectedResponses =
                                blocksOneToTwo.size() + 1; // two with items, one with success status
                        awaitResponse(fromPluginBytes, expectedResponses);
                        // Assert responses count and status success
                        assertThat(fromPluginBytes)
                                .hasSize(expectedResponses)
                                .last()
                                .extracting(responseExtractor)
                                .isNotNull()
                                .returns(Code.SUCCESS, responseStatusExtractor);
                        // Extract and assert block items response
                        final List<Bytes> blockItemRequests = fromPluginBytes.subList(0, fromPluginBytes.size());
                        for (int i = 0; i < blocksOneToTwo.size(); i++) {
                            final List<BlockItem> expected = blocksOneToTwo.get(i);
                            final List<BlockItem> actual = responseExtractor
                                    .apply(blockItemRequests.get(i))
                                    .blockItems()
                                    .blockItems();
                            assertBlockReceived(expected, actual);
                        }
                    }

                    /**
                     * This test aims to assert that when a valid request for multiple blocks is sent to the plugin,
                     * a response with the block items is returned, followed by a success status response. Here, the
                     * blocks requested do not exist at the time of the request, but are supplied later from live
                     * data. This is a request for future blocks, but the request can be fulfilled because we have
                     * some historical data, thus we can determine how far in the future the requested blocks are.
                     */
                    @Test
                    @DisplayName("Test Subscriber: Valid Request Multiple Blocks Future From Live")
                    void testSuccessfulRequestMultipleBlocksClosedRangeFutureFromLive() {
                        // First we create the blocks
                        final List<List<BlockItem>> blocksOneToTwo =
                                SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksBatched(1, 2);
                        // Supply a block, otherwise if there is no data at all, we
                        // cannot fulfill the request, because we cannot determine
                        // how much in the future the requested block is
                        final List<List<BlockItem>> blockZero =
                                SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksBatched(0, 0);
                        final List<BlockItem> blockZeroItems = blockZero.getFirst();
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
                        for (final List<BlockItem> block : blocksOneToTwo) {
                            blockMessaging.sendBlockItems(toBlockItems(block));
                        }
                        // Wait for responses
                        final int expectedResponses =
                                blocksOneToTwo.size() + 1; // two with items, one with success status
                        awaitResponse(fromPluginBytes, expectedResponses);
                        // Assert responses count and status success
                        assertThat(fromPluginBytes)
                                .hasSize(expectedResponses)
                                .last()
                                .extracting(responseExtractor)
                                .isNotNull()
                                .returns(Code.SUCCESS, responseStatusExtractor);
                        // Extract and assert block items response
                        final List<Bytes> blockItemRequests = fromPluginBytes.subList(0, fromPluginBytes.size());
                        for (int i = 0; i < blocksOneToTwo.size(); i++) {
                            final List<BlockItem> expected = blocksOneToTwo.get(i);
                            final List<BlockItem> actual = responseExtractor
                                    .apply(blockItemRequests.get(i))
                                    .blockItems()
                                    .blockItems();
                            assertBlockReceived(expected, actual);
                        }
                    }
                }

                /**
                 * Multiple block requests with an open range (start defined, end open).
                 */
                @Nested
                @DisplayName("Multiple Block Request Start Defined - End Open Range Tests")
                class MultipleBlockRequestStartDefinedEndOpenRangeTests {
                    /**
                     * This test aims to assert that when a valid request for multiple blocks is sent to the plugin,
                     * a response with the block items is returned, followed by a success status response. Here, the
                     * blocks requested already exist before the request is made.
                     */
                    @Test
                    @DisplayName(
                            "Test Subscriber: Valid Request Multiple Blocks Start Defined - End Open Ranged Already Existing")
                    void testSuccessfulRequestMultipleBlocksStartDefinedEndOpenRange() {
                        // First we create the blocks
                        final List<List<BlockItem>> blocksZeroToTwo =
                                SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksBatched(0, 2);
                        // Supply the needed items
                        for (final List<BlockItem> block : blocksZeroToTwo) {
                            historicalBlockFacility.handleBlockItemsReceived(toBlockItems(block));
                        }
                        // Then, we create the request for blocks 0 to open
                        final SubscribeStreamRequest request = SubscribeStreamRequest.newBuilder()
                                .startBlockNumber(0L)
                                .endBlockNumber(-1L)
                                .build();
                        // Send the request
                        toPluginPipe.onNext(SubscribeStreamRequest.PROTOBUF.toBytes(request));
                        // Wait for responses
                        final int blockItemResponses = blocksZeroToTwo.size(); // three with items
                        awaitResponse(fromPluginBytes, blockItemResponses);
                        // now we need to stop the plugin to end the open range request and
                        // receive the success status response
                        plugin.stop();
                        final int expectedResponses =
                                blockItemResponses + 1; // three with items, one with success status
                        // Assert responses count and status success
                        assertThat(fromPluginBytes)
                                .hasSize(expectedResponses)
                                .last()
                                .extracting(responseExtractor)
                                .isNotNull()
                                .returns(Code.SUCCESS, responseStatusExtractor);
                        // Extract and assert block items response
                        final List<Bytes> blockItemRequests = fromPluginBytes.subList(0, fromPluginBytes.size());
                        for (int i = 0; i < blocksZeroToTwo.size(); i++) {
                            final List<BlockItem> expected = blocksZeroToTwo.get(i);
                            final List<BlockItem> actual = responseExtractor
                                    .apply(blockItemRequests.get(i))
                                    .blockItems()
                                    .blockItems();
                            assertBlockReceived(expected, actual);
                        }
                    }

                    /**
                     * This test aims to assert that when a valid request for multiple blocks is sent to the plugin,
                     * a response with the block items is returned, followed by a success status response. Here, the
                     * blocks requested do not exist at the time of the request, but are supplied later from history.
                     * This is a request for future blocks, but the request can be fulfilled because we have some
                     * historical data, thus we can determine how far in the future the requested blocks are.
                     */
                    @Test
                    @DisplayName(
                            "Test Subscriber: Valid Request Multiple Blocks Start Defined - End Open Range Future From History")
                    void testSuccessfulRequestMultipleBlocksStartDefinedEndOpenRangeFutureFromHistory() {
                        // First we create the blocks
                        final List<List<BlockItem>> blocksOneToTwo =
                                SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksBatched(1, 2);
                        // Supply a block, otherwise if there is no data at all, we
                        // cannot fulfill the request, because we cannot determine
                        // how much in the future the requested block is
                        final List<List<BlockItem>> blockZero =
                                SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksBatched(0, 0);
                        final List<BlockItem> blockZeroItems = blockZero.getFirst();
                        historicalBlockFacility.handleBlockItemsReceived(toBlockItems(blockZeroItems));
                        // Then, we create the request for blocks 1 to open
                        final SubscribeStreamRequest request = SubscribeStreamRequest.newBuilder()
                                .startBlockNumber(1L)
                                .endBlockNumber(-1L)
                                .build();
                        // Send the request
                        toPluginPipe.onNext(SubscribeStreamRequest.PROTOBUF.toBytes(request));
                        // Now supply the requested blocks to history
                        for (final List<BlockItem> block : blocksOneToTwo) {
                            historicalBlockFacility.handleBlockItemsReceived(toBlockItems(block));
                        }
                        // Wait for responses
                        final int blockItemResponses = blocksOneToTwo.size(); // two with items
                        awaitResponse(fromPluginBytes, blockItemResponses);
                        // now we need to stop the plugin to end the open range request and
                        // receive the success status response
                        plugin.stop();
                        final int expectedResponses = blockItemResponses + 1; // two with items, one with success status
                        // Assert responses count and status success
                        assertThat(fromPluginBytes)
                                .hasSize(expectedResponses)
                                .last()
                                .extracting(responseExtractor)
                                .isNotNull()
                                .returns(Code.SUCCESS, responseStatusExtractor);
                        // Extract and assert block items response
                        final List<Bytes> blockItemRequests = fromPluginBytes.subList(0, fromPluginBytes.size());
                        for (int i = 0; i < blocksOneToTwo.size(); i++) {
                            final List<BlockItem> expected = blocksOneToTwo.get(i);
                            final List<BlockItem> actual = responseExtractor
                                    .apply(blockItemRequests.get(i))
                                    .blockItems()
                                    .blockItems();
                            assertBlockReceived(expected, actual);
                        }
                    }

                    /**
                     * This test aims to assert that when a valid request for multiple blocks is sent to the plugin,
                     * a response with the block items is returned, followed by a success status response. Here, the
                     * blocks requested do not exist at the time of the request, but are supplied later from live
                     * data. This is a request for future blocks, but the request can be fulfilled because we have
                     * some historical data, thus we can determine how far in the future the requested blocks are.
                     */
                    @Test
                    @DisplayName(
                            "Test Subscriber: Valid Request Multiple Blocks Start Defined - End Open Range Future From Live")
                    void testSuccessfulRequestMultipleBlocksStartDefinedEndOpenRangeFutureFromLive() {
                        // First we create the blocks
                        final List<List<BlockItem>> blocksOneToTwo =
                                SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksBatched(1, 2);
                        // Supply a block, otherwise if there is no data at all, we
                        // cannot fulfill the request, because we cannot determine
                        // how much in the future the requested block is
                        final List<List<BlockItem>> blockZero =
                                SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksBatched(0, 0);
                        final List<BlockItem> blockZeroItems = blockZero.getFirst();
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
                        for (final List<BlockItem> block : blocksOneToTwo) {
                            blockMessaging.sendBlockItems(toBlockItems(block));
                        }
                        // Wait for responses
                        final int blockItemResponses = blocksOneToTwo.size(); // two with items
                        awaitResponse(fromPluginBytes, blockItemResponses);
                        // now we need to stop the plugin to end the open range request and
                        // receive the success status response
                        plugin.stop();
                        final int expectedResponses = blockItemResponses + 1; // two with items, one with success status
                        // Assert responses count and status success
                        assertThat(fromPluginBytes)
                                .hasSize(expectedResponses)
                                .last()
                                .extracting(responseExtractor)
                                .isNotNull()
                                .returns(Code.SUCCESS, responseStatusExtractor);
                        // Extract and assert block items response
                        final List<Bytes> blockItemRequests = fromPluginBytes.subList(0, fromPluginBytes.size());
                        for (int i = 0; i < blocksOneToTwo.size(); i++) {
                            final List<BlockItem> expected = blocksOneToTwo.get(i);
                            final List<BlockItem> actual = responseExtractor
                                    .apply(blockItemRequests.get(i))
                                    .blockItems()
                                    .blockItems();
                            assertBlockReceived(expected, actual);
                        }
                    }
                }

                /**
                 * Multiple block requests with a range starting from the first available block in history
                 * (start = -1), and a defined end block.
                 */
                @Nested
                @DisplayName("Multiple Block Request Start From First Available - End Defined Range Tests")
                class MultipleBlockRequestStartFromFirstAvailableEndDefinedRangeTests {
                    /**
                     * This test aims to assert that when a valid request for multiple blocks is sent to the plugin,
                     * a response with the block items is returned, followed by a success status response. Here, the
                     * blocks requested already exist before the request is made. The request starts from the first
                     * available block in history and is up to a defined end block.
                     */
                    @Test
                    @DisplayName(
                            "Test Subscriber: Valid Request Multiple Blocks Start From First Available - End Defined Range Already Existing")
                    void testSuccessfulRequestMultipleBlocksStartFromFirstAvailableEndDefinedRange() {
                        // First we create the blocks
                        final List<List<BlockItem>> blocksZeroToTwo =
                                SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksBatched(0, 2);
                        // Supply the needed items
                        for (final List<BlockItem> block : blocksZeroToTwo) {
                            historicalBlockFacility.handleBlockItemsReceived(toBlockItems(block));
                        }
                        // Then, we create the request for blocks first available (0) to 2
                        final SubscribeStreamRequest request = SubscribeStreamRequest.newBuilder()
                                .startBlockNumber(-1L)
                                .endBlockNumber(2L)
                                .build();
                        // Send the request
                        toPluginPipe.onNext(SubscribeStreamRequest.PROTOBUF.toBytes(request));
                        // Wait for responses
                        final int expectedResponses =
                                blocksZeroToTwo.size() + 1; // three with items, one with success status
                        awaitResponse(fromPluginBytes, expectedResponses);
                        // Assert responses count and status success
                        assertThat(fromPluginBytes)
                                .hasSize(expectedResponses)
                                .last()
                                .extracting(responseExtractor)
                                .isNotNull()
                                .returns(Code.SUCCESS, responseStatusExtractor);
                        // Extract and assert block items response
                        final List<Bytes> blockItemRequests = fromPluginBytes.subList(0, fromPluginBytes.size());
                        for (int i = 0; i < blocksZeroToTwo.size(); i++) {
                            final List<BlockItem> expected = blocksZeroToTwo.get(i);
                            final List<BlockItem> actual = responseExtractor
                                    .apply(blockItemRequests.get(i))
                                    .blockItems()
                                    .blockItems();
                            assertBlockReceived(expected, actual);
                        }
                    }

                    /**
                     * This test aims to assert that when a valid request for multiple blocks is sent to the plugin,
                     * a response with the block items is returned, followed by a success status response. Here, the
                     * blocks requested do not exist at the time of the request, but are supplied later from history.
                     * The request starts from the first available block in history and is up to a defined end block.
                     * This is a request for future blocks, but the request can be fulfilled because we have some
                     * historical data, thus we can determine how far in the future the requested blocks are.
                     * The request starts from the first available block in history and is up to a defined end block.
                     */
                    @Test
                    @DisplayName(
                            "Test Subscriber: Valid Request Multiple Blocks Start From First Available - End Defined Range Future From History")
                    void testSuccessfulRequestMultipleBlocksStartFromFirstAvailableEndDefinedRangeFutureFromHistory() {
                        // First we create the blocks
                        final List<List<BlockItem>> blocksZeroToTwo =
                                SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksBatched(0, 2);
                        // Supply a block, otherwise if there is no data at all, we
                        // cannot fulfill the request, because we cannot determine
                        // how much in the future the requested block is
                        historicalBlockFacility.handleBlockItemsReceived(toBlockItems(blocksZeroToTwo.getFirst()));
                        // Then, we create the request for blocks first available (0) to 2
                        final SubscribeStreamRequest request = SubscribeStreamRequest.newBuilder()
                                .startBlockNumber(-1L)
                                .endBlockNumber(2L)
                                .build();
                        // Send the request
                        toPluginPipe.onNext(SubscribeStreamRequest.PROTOBUF.toBytes(request));
                        // Now supply the requested blocks to history
                        for (final List<BlockItem> block : blocksZeroToTwo.subList(1, blocksZeroToTwo.size())) {
                            historicalBlockFacility.handleBlockItemsReceived(toBlockItems(block));
                        }
                        // Wait for responses
                        final int expectedResponses =
                                blocksZeroToTwo.size() + 1; // three with items, one with success status
                        awaitResponse(fromPluginBytes, expectedResponses);
                        // Assert responses count and status success
                        assertThat(fromPluginBytes)
                                .hasSize(expectedResponses)
                                .last()
                                .extracting(responseExtractor)
                                .isNotNull()
                                .returns(Code.SUCCESS, responseStatusExtractor);
                        // Extract and assert block items response
                        final List<Bytes> blockItemRequests = fromPluginBytes.subList(0, fromPluginBytes.size());
                        for (int i = 0; i < blocksZeroToTwo.size(); i++) {
                            final List<BlockItem> expected = blocksZeroToTwo.get(i);
                            final List<BlockItem> actual = responseExtractor
                                    .apply(blockItemRequests.get(i))
                                    .blockItems()
                                    .blockItems();
                            assertBlockReceived(expected, actual);
                        }
                    }

                    /**
                     * This test aims to assert that when a valid request for multiple blocks is sent to the plugin,
                     * a response with the block items is returned, followed by a success status response. Here, the
                     * blocks requested do not exist at the time of the request, but are supplied later from live
                     * data. The request starts from the first available block in history and is up to a defined end
                     * block. This is a request for future blocks, but the request can be fulfilled because we have
                     * some historical data, thus we can determine how far in the future the requested blocks are.
                     * The request starts from the first available block in history and is up to a defined end block.
                     */
                    @Test
                    @DisplayName(
                            "Test Subscriber: Valid Request Multiple Blocks Start From First Available - End Defined Range Future From Live")
                    void testSuccessfulRequestMultipleBlocksStartFromFirstAvailableEndDefinedRangeFutureFromLive() {
                        // First we create the blocks
                        final List<List<BlockItem>> blocksZeroToTwo =
                                SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksBatched(0, 2);
                        // Supply a block, otherwise if there is no data at all, we
                        // cannot fulfill the request, because we cannot determine
                        // how much in the future the requested block is
                        historicalBlockFacility.handleBlockItemsReceived(toBlockItems(blocksZeroToTwo.getFirst()));
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
                        for (final List<BlockItem> block : blocksZeroToTwo.subList(1, blocksZeroToTwo.size())) {
                            blockMessaging.sendBlockItems(toBlockItems(block));
                        }
                        // Wait for responses
                        final int expectedResponses =
                                blocksZeroToTwo.size() + 1; // three with items, one with success status
                        awaitResponse(fromPluginBytes, expectedResponses);
                        // Assert responses count and status success
                        assertThat(fromPluginBytes)
                                .hasSize(expectedResponses)
                                .last()
                                .extracting(responseExtractor)
                                .isNotNull()
                                .returns(Code.SUCCESS, responseStatusExtractor);
                        // Extract and assert block items response
                        final List<Bytes> blockItemRequests = fromPluginBytes.subList(0, fromPluginBytes.size());
                        for (int i = 0; i < blocksZeroToTwo.size(); i++) {
                            final List<BlockItem> expected = blocksZeroToTwo.get(i);
                            final List<BlockItem> actual = responseExtractor
                                    .apply(blockItemRequests.get(i))
                                    .blockItems()
                                    .blockItems();
                            assertBlockReceived(expected, actual);
                        }
                    }
                }

                /**
                 * Live stream requests (start = -1, end = -1).
                 */
                @Nested
                @DisplayName("Live Stream Request Tests")
                class LiveStreamRequestTest {
                    /**
                     * This test aims to assert that when a valid request for live stream is sent to the plugin,
                     * a response with the block items is returned, and items keep streaming indefinitely.
                     * This is a request for live blocks, which can always be fulfilled.
                     */
                    @Test
                    @DisplayName("Test Subscriber: Valid Request Live Stream")
                    void testSuccessfulRequestLiveStream() {
                        // First we create the blocks
                        final List<List<BlockItem>> blocksZeroToTwo =
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
                        for (final List<BlockItem> block : blocksZeroToTwo) {
                            blockMessaging.sendBlockItems(toBlockItems(block));
                        }
                        // Wait for responses for block items
                        final int blockItemResponses = blocksZeroToTwo.size(); // three with items
                        awaitResponse(fromPluginBytes, blockItemResponses);
                        // now we need to stop the plugin to end the live stream request and
                        // receive the success status response
                        plugin.stop();
                        final int expectedResponses = blockItemResponses + 1; // three with items, one
                        // Assert responses count and status success
                        assertThat(fromPluginBytes).hasSize(expectedResponses);
                        // Extract and assert block items response
                        final List<Bytes> blockItemRequests = fromPluginBytes.subList(0, fromPluginBytes.size());
                        for (int i = 0; i < blocksZeroToTwo.size(); i++) {
                            final List<BlockItem> expected = blocksZeroToTwo.get(i);
                            final List<BlockItem> actual = responseExtractor
                                    .apply(blockItemRequests.get(i))
                                    .blockItems()
                                    .blockItems();
                            assertBlockReceived(expected, actual);
                        }
                    }

                    /**
                     * This test aims to assert that when a valid request for live stream is sent to the plugin,
                     * a response with the block items is returned, and items keep streaming indefinitely.
                     * This is a request for live blocks, which can always be fulfilled. Here, we supply the first
                     * block from live stream, and then we supply following from history. This is to ensure that
                     * the plugin can handle blocks coming from both live and history when in live stream mode.
                     */
                    @Test
                    @DisplayName("Test Subscriber: Valid Request Live Stream From Live Then History")
                    @Disabled(
                            "@todo(1374) should not be failing, we fail because history not permitted, we think we jump ahead")
                    void testSuccessfulRequestLiveStreamFromLiveThenHistory() {
                        // First we create the blocks
                        final List<List<BlockItem>> blocksZeroToTwo =
                                SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksBatched(0, 2);
                        // Then, we create the request for live stream
                        final SubscribeStreamRequest request = SubscribeStreamRequest.newBuilder()
                                .startBlockNumber(-1L)
                                .endBlockNumber(-1L)
                                .build();
                        // Send the request
                        toPluginPipe.onNext(SubscribeStreamRequest.PROTOBUF.toBytes(request));
                        // Send the first block from live
                        blockMessaging.sendBlockItems(toBlockItems(blocksZeroToTwo.getFirst()));
                        // Await the first response
                        awaitResponse(fromPluginBytes, 1);
                        // Now supply the following blocks from history
                        for (final List<BlockItem> block : blocksZeroToTwo.subList(1, blocksZeroToTwo.size())) {
                            historicalBlockFacility.handleBlockItemsReceived(toBlockItems(block));
                        }
                        // Wait for responses for block items
                        awaitResponse(fromPluginBytes, 2);
                        // now we need to stop the plugin to end the live stream request and
                        // receive the success status response
                        plugin.stop();
                        final int expectedResponses =
                                blocksZeroToTwo.size() + 1; // three with items, one with success status
                        // Assert responses count and status success
                        assertThat(fromPluginBytes)
                                .hasSize(expectedResponses)
                                .last()
                                .extracting(responseExtractor)
                                .isNotNull()
                                .returns(Code.SUCCESS, responseStatusExtractor);
                        // Extract and assert block items response
                        final List<Bytes> blockItemRequests = fromPluginBytes.subList(0, fromPluginBytes.size());
                        for (int i = 0; i < blocksZeroToTwo.size(); i++) {
                            final List<BlockItem> expected = blocksZeroToTwo.get(i);
                            final List<BlockItem> actual = responseExtractor
                                    .apply(blockItemRequests.get(i))
                                    .blockItems()
                                    .blockItems();
                            assertBlockReceived(expected, actual);
                        }
                    }

                    /**
                     * This test aims to assert that when a valid request for live stream is sent to the plugin,
                     * a response with the block items is returned, and items keep streaming indefinitely.
                     * This is a request for live blocks, which can always be fulfilled. Here, we simulate that the
                     * subscriber has subscribed mid-block, meaning the live ring buffer holds partially the current
                     * block. If that is so, the plugin should discard that partial block and start sending from the next
                     * block.
                     */
                    @Test
                    @DisplayName("Test Subscriber: Valid Request Live Stream Mid-Block Subscription")
                    void testSuccessfulRequestLiveStreamMidBlockSubscription() {
                        // First we create the blocks
                        final List<List<BlockItem>> blocksZeroToTwo =
                                SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksBatched(0, 2);
                        // Then, we create the request for live stream
                        final SubscribeStreamRequest request = SubscribeStreamRequest.newBuilder()
                                .startBlockNumber(-1L)
                                .endBlockNumber(-1L)
                                .build();
                        // Send the request
                        toPluginPipe.onNext(SubscribeStreamRequest.PROTOBUF.toBytes(request));
                        // Send a partial block from live, simulating a mid-block subscription
                        final List<BlockItem> firstBlock = blocksZeroToTwo.getFirst();
                        blockMessaging.sendBlockItems(toBlockItems(firstBlock, false));
                        // Now supply the following blocks from history
                        final List<List<BlockItem>> blocksOneToTwo = blocksZeroToTwo.subList(1, blocksZeroToTwo.size());
                        for (final List<BlockItem> block : blocksOneToTwo) {
                            blockMessaging.sendBlockItems(toBlockItems(block));
                        }
                        // Wait for responses for block items
                        final int expectedBlockItemResponses = blocksOneToTwo.size(); // two with items
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
                        final List<Bytes> blockItemRequests = fromPluginBytes.subList(0, fromPluginBytes.size());
                        for (int i = 0; i < expectedBlockItemResponses; i++) {
                            final List<BlockItem> expected = blocksOneToTwo.get(i);
                            final List<BlockItem> actual = responseExtractor
                                    .apply(blockItemRequests.get(i))
                                    .blockItems()
                                    .blockItems();
                            assertBlockReceived(expected, actual);
                        }
                    }
                }
            }

            /**
             * Negative tests for the subscriber plugin.
             */
            @Nested
            @DisplayName("Negative Subscriber Tests")
            class NegativeSubscriberTests {
                /**
                 * This test aims to asser that when a valid request is sent to
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
                    final List<List<BlockItem>> blockZero =
                            SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksBatched(0, 0);
                    final List<BlockItem> blockZeroItems = blockZero.getFirst();
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
                    final int expectedResponses = 1;
                    awaitResponse(fromPluginBytes, expectedResponses);
                    assertThat(fromPluginBytes)
                            .hasSize(expectedResponses)
                            .first()
                            .extracting(responseExtractor)
                            .isNotNull()
                            .returns(expectedCode, responseStatusExtractor);
                    System.out.println();
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
                    final int expectedResponses = 1;
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

    private static BlockItems toBlockItems(final List<BlockItem> expected) {
        return toBlockItems(expected, true);
    }

    private static BlockItems toBlockItems(final List<BlockItem> expected, final boolean sendHeader) {
        final List<BlockItemUnparsed> result = new ArrayList<>();
        final List<BlockItem> actualExpected = sendHeader ? expected : expected.subList(1, expected.size());
        for (final BlockItem item : actualExpected) {
            try {
                result.add(BlockItemUnparsed.PROTOBUF.parse(BlockItem.PROTOBUF.toBytes(item)));
            } catch (final ParseException e) {
                fail("Failed to parse BlockItem to BlockItemUnparsed", e);
            }
        }
        return new BlockItems(result, expected.getFirst().blockHeader().number());
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
