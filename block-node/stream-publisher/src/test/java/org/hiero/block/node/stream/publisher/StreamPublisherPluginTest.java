// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.stream.publisher;

import static java.util.concurrent.locks.LockSupport.parkNanos;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hiero.block.node.app.fixtures.TestUtils.enableDebugLogging;
import static org.hiero.block.node.app.fixtures.blocks.SimpleTestBlockItemBuilder.toBlockItems;

import com.hedera.hapi.block.stream.Block;
import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.UncheckedParseException;
import com.hedera.pbj.runtime.grpc.ServiceInterface;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.hiero.block.api.BlockItemSet;
import org.hiero.block.api.PublishStreamRequest;
import org.hiero.block.api.PublishStreamResponse;
import org.hiero.block.api.PublishStreamResponse.EndOfStream.Code;
import org.hiero.block.api.PublishStreamResponse.ResponseOneOfType;
import org.hiero.block.internal.BlockItemSetUnparsed;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.PublishStreamRequestUnparsed;
import org.hiero.block.node.app.config.node.NodeConfig;
import org.hiero.block.node.app.fixtures.blocks.SimpleTestBlockItemBuilder;
import org.hiero.block.node.app.fixtures.plugintest.GrpcPluginTestBase;
import org.hiero.block.node.app.fixtures.plugintest.SimpleInMemoryHistoricalBlockFacility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Tests for the {@link StreamPublisherPlugin}.
 */
@DisplayName("StreamPublisherPlugin Tests")
class StreamPublisherPluginTest {
    // ASSERTION MAPPERS
    private static final Function<Bytes, PublishStreamResponse> bytesToPublishStreamResponseMapper = bytes -> {
        try {
            return PublishStreamResponse.PROTOBUF.parse(bytes);
        } catch (final ParseException e) {
            throw new UncheckedParseException(e);
        }
    };
    // ASSERTION EXTRACTORS
    private static final Function<PublishStreamResponse, ResponseOneOfType> responseKindExtractor =
            response -> response.response().kind();
    private static final Function<PublishStreamResponse, Code> endStreamResponseCodeExtractor =
            response -> Objects.requireNonNull(response.endStream()).status();
    private static final Function<PublishStreamResponse, Long> acknowledgementBlockNumberExtractor =
            response -> Objects.requireNonNull(response.acknowledgement()).blockNumber();

    /**
     * Enable debug logging for each test.
     */
    @BeforeEach
    void setup() {
        enableDebugLogging();
    }

    /**
     * Test for the {@link StreamPublisherPlugin} plugin.
     */
    @Nested
    @DisplayName("Plugin Tests")
    class PluginTest extends GrpcPluginTestBase<StreamPublisherPlugin, ExecutorService> {
        /**
         * Constructor for the plugin tests.
         */
        PluginTest() {
            super(Executors.newSingleThreadExecutor());
            final StreamPublisherPlugin toTest = new StreamPublisherPlugin();
            final SimpleInMemoryHistoricalBlockFacility historicalBlockFacility =
                    new SimpleInMemoryHistoricalBlockFacility();
            start(toTest, toTest.methods().getFirst(), historicalBlockFacility);
        }

        /**
         * Verifies that the service interface correctly registers and exposes
         * the server status method.
         */
        @Test
        @DisplayName("Test verify correct method/s registered for StreamPublisherPlugin in test base")
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
         * This test aims to verify that when null block items are published to
         * the pipeline, an
         * {@link PublishStreamResponse.EndOfStream}
         * response is returned with code {@link Code#INVALID_REQUEST}.
         */
        @Test
        @DisplayName("Test publish null block items")
        void testPublishNullItems() {
            // Build a PublishStreamRequest with null block items
            final PublishStreamRequest request = PublishStreamRequest.newBuilder()
                    .blockItems(BlockItemSet.newBuilder()
                            .blockItems((List<BlockItem>) null)
                            .build())
                    .build();
            // Send the request to the pipeline
            toPluginPipe.onNext(PublishStreamRequest.PROTOBUF.toBytes(request));
            // Assert response
            assertThat(fromPluginBytes)
                    .hasSize(1)
                    .first()
                    .extracting(bytesToPublishStreamResponseMapper)
                    .isNotNull()
                    .returns(ResponseOneOfType.END_STREAM, responseKindExtractor)
                    .returns(Code.INVALID_REQUEST, endStreamResponseCodeExtractor);
        }

        /**
         * This test aims to verify that when empty block items are published to
         * the pipeline, an
         * {@link PublishStreamResponse.EndOfStream}
         * response is returned with code {@link Code#INVALID_REQUEST}.
         */
        @Test
        @DisplayName("Test publish empty block items")
        void testPublishEmptyItems() {
            // Build a PublishStreamRequest with empty block items
            final PublishStreamRequest request = PublishStreamRequest.newBuilder()
                    .blockItems(BlockItemSet.newBuilder()
                            .blockItems(Collections.emptyList())
                            .build())
                    .build();
            // Send the request to the pipeline
            toPluginPipe.onNext(PublishStreamRequest.PROTOBUF.toBytes(request));
            // Assert response
            assertThat(fromPluginBytes)
                    .hasSize(1)
                    .first()
                    .extracting(bytesToPublishStreamResponseMapper)
                    .isNotNull()
                    .returns(ResponseOneOfType.END_STREAM, responseKindExtractor)
                    .returns(Code.INVALID_REQUEST, endStreamResponseCodeExtractor);
        }

        /**
         * This test aims to verify that when a request with unset oneOf is
         * published to the pipeline, an
         * {@link PublishStreamResponse.EndOfStream}
         * response is returned with code {@link Code#ERROR}.
         */
        @Test
        @DisplayName("Test publish unset oneOf")
        void testPublishUnsetOneOf() {
            // Build a PublishStreamRequest with an unset oneOf
            final PublishStreamRequest request =
                    PublishStreamRequest.newBuilder().build();
            // Send the request to the pipeline
            toPluginPipe.onNext(PublishStreamRequest.PROTOBUF.toBytes(request));
            // Assert response
            assertThat(fromPluginBytes)
                    .hasSize(1)
                    .first()
                    .extracting(bytesToPublishStreamResponseMapper)
                    .isNotNull()
                    .returns(ResponseOneOfType.END_STREAM, responseKindExtractor)
                    .returns(Code.ERROR, endStreamResponseCodeExtractor);
        }

        /**
         * This test aims to verify that when a valid block is published to the
         * pipeline, a {@link PublishStreamResponse.BlockAcknowledgement}
         * response is returned.
         */
        @Test
        @DisplayName("Test publish a valid block as items")
        void testPublishValidBlock() {
            final BlockItemUnparsed[] block = SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsed(0, 1);
            // Build a PublishStreamRequest with a valid block as items
            final BlockItemSetUnparsed blockItems =
                    BlockItemSetUnparsed.newBuilder().blockItems(block).build();
            final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                    .blockItems(blockItems)
                    .build();
            // Send the request to the pipeline
            toPluginPipe.onNext(PublishStreamRequestUnparsed.PROTOBUF.toBytes(request));
            // Await to ensure async execution and assert response
            parkNanos(500_000_000L);
            assertThat(fromPluginBytes)
                    .hasSize(1)
                    .first()
                    .extracting(bytesToPublishStreamResponseMapper)
                    .isNotNull()
                    .returns(ResponseOneOfType.ACKNOWLEDGEMENT, responseKindExtractor)
                    .returns(0L, acknowledgementBlockNumberExtractor);
        }
    }

    /**
     * Test for the {@link StreamPublisherPlugin} plugin when publishing a block
     * prior to the earliest managed block.
     */
    @Nested
    @DisplayName("Plugin Tests Pre Earliest Managed Block")
    class PluginTestsPreEarliestManagedBlock extends GrpcPluginTestBase<StreamPublisherPlugin, ExecutorService> {
        /** The historical block facility to use when testing. */
        private final SimpleInMemoryHistoricalBlockFacility historicalBlockFacility;

        /**
         * Constructor for the plugin tests.
         */
        PluginTestsPreEarliestManagedBlock() {
            super(Executors.newSingleThreadExecutor());
            final StreamPublisherPlugin toTest = new StreamPublisherPlugin();
            historicalBlockFacility = new SimpleInMemoryHistoricalBlockFacility();
            final Map<String, String> configOverrides =
                    Map.ofEntries(Map.entry("block.node.earliestManagedBlock", "10"));
            start(toTest, toTest.methods().getFirst(), historicalBlockFacility, configOverrides);
            // Assert that the earliest managed block is set to 10
            final long earliestManagedBlock = blockNodeContext
                    .configuration()
                    .getConfigData(NodeConfig.class)
                    .earliestManagedBlock();
            assertThat(earliestManagedBlock).isEqualTo(10L);
        }

        /**
         * This test aims to assert that a valid block could be streamed to the
         * plugin even if it is prior to the earliestManagedBlock, granted that
         * this is the first block ever published after the plugin has started.
         * Here, we have no prior block history.
         */
        @Test
        @DisplayName("Test publish a valid block as items prior to earliestManagedBlock, no history")
        void testStreamPriorToEarliestManagedBlockNoHistory() {
            // Build a PublishStreamRequest with a valid block as items
            final BlockItemUnparsed[] block = SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsed(0, 1);
            final BlockItemSetUnparsed blockItems =
                    BlockItemSetUnparsed.newBuilder().blockItems(block).build();
            final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                    .blockItems(blockItems)
                    .build();
            // Send the request to the pipeline
            toPluginPipe.onNext(PublishStreamRequestUnparsed.PROTOBUF.toBytes(request));
            // Await to ensure async execution and assert response
            parkNanos(500_000_000L);
            // Assert that the block has been successfully streamed
            assertThat(fromPluginBytes)
                    .hasSize(1)
                    .first()
                    .extracting(bytesToPublishStreamResponseMapper)
                    .isNotNull()
                    .returns(ResponseOneOfType.ACKNOWLEDGEMENT, responseKindExtractor)
                    .returns(0L, acknowledgementBlockNumberExtractor);
        }

        /**
         * This test aims to assert that a valid block could be streamed to the
         * plugin even if it is prior to the earliestManagedBlock, granted that
         * there is prior block history and the start of the stream is after the
         * history.
         */
        @Test
        @DisplayName(
                "Test publish a valid block as items prior to earliestManagedBlock, with history, start after history")
        void testStreamPriorToEarliestManagedBlockWithHistoryStartAfterHistory() {
            // First, we need to ensure we have some history.
            final List<Block> blocks = SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksBatched(0, 5);
            // Disable the plugin's message handling, otherwise it will send notifications.
            historicalBlockFacility.setDisablePlugin();
            // Add all the blocks to the historical block facility.
            for (final Block block : blocks) {
                historicalBlockFacility.handleBlockItemsReceived(toBlockItems(block.items()));
            }
            // Assert that the historical block facility has blocks 0-5
            assertThat(
                    blockNodeContext.historicalBlockProvider().availableBlocks().contains(0, 5));
            // Build a PublishStreamRequest with a valid block as items prior to earliestManagedBlock && after history
            final BlockItemUnparsed[] block = SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsed(6, 6);
            final BlockItemSetUnparsed blockItems =
                    BlockItemSetUnparsed.newBuilder().blockItems(block).build();
            final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                    .blockItems(blockItems)
                    .build();
            // Before sending the request, re-enable the historical block facility's message handling in order
            // to pick up the live items so we can eventually receive a persisted notification and are able to
            // acknowledge the block.
            historicalBlockFacility.clearDisablePlugin();
            // Send the request to the pipeline
            toPluginPipe.onNext(PublishStreamRequestUnparsed.PROTOBUF.toBytes(request));
            // Await to ensure async execution and assert response
            parkNanos(500_000_000L);
            // Assert that the block has been successfully streamed
            assertThat(fromPluginBytes)
                    .hasSize(1)
                    .first()
                    .extracting(bytesToPublishStreamResponseMapper)
                    .isNotNull()
                    .returns(ResponseOneOfType.ACKNOWLEDGEMENT, responseKindExtractor)
                    .returns(6L, acknowledgementBlockNumberExtractor);
        }

        /**
         * This test aims to assert that a valid block could be streamed to the
         * plugin even if it is prior to the earliestManagedBlock, granted that
         * there is prior block history and the start of the stream is before
         * the history.
         */
        @Test
        @DisplayName(
                "Test publish a valid block as items prior to earliestManagedBlock, with history, start after history")
        void testStreamPriorToEarliestManagedBlockWithHistoryStartBeforeHistory() {
            // First, we need to ensure we have some history.
            final List<Block> blocks = SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksBatched(3, 5);
            // Disable the plugin's message handling, otherwise it will send notifications.
            historicalBlockFacility.setDisablePlugin();
            // Add all the blocks to the historical block facility.
            for (final Block block : blocks) {
                historicalBlockFacility.handleBlockItemsReceived(toBlockItems(block.items()));
            }
            // Assert that the historical block facility has blocks 3-5
            assertThat(
                    blockNodeContext.historicalBlockProvider().availableBlocks().contains(3, 5));
            // Build a PublishStreamRequest with a valid block as items prior to earliestManagedBlock && history
            final BlockItemUnparsed[] block = SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsed(2, 2);
            final BlockItemSetUnparsed blockItems =
                    BlockItemSetUnparsed.newBuilder().blockItems(block).build();
            final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                    .blockItems(blockItems)
                    .build();
            // Before sending the request, re-enable the historical block facility's message handling in order
            // to pick up the live items so we can eventually receive a persisted notification and are able to
            // acknowledge the block.
            historicalBlockFacility.clearDisablePlugin();
            // Send the request to the pipeline
            toPluginPipe.onNext(PublishStreamRequestUnparsed.PROTOBUF.toBytes(request));
            // Await to ensure async execution and assert response
            parkNanos(500_000_000L);
            // Assert that the block has been successfully streamed
            assertThat(fromPluginBytes)
                    .hasSize(1)
                    .first()
                    .extracting(bytesToPublishStreamResponseMapper)
                    .isNotNull()
                    .returns(ResponseOneOfType.ACKNOWLEDGEMENT, responseKindExtractor)
                    .returns(2L, acknowledgementBlockNumberExtractor);
        }

        /**
         * This test aims to assert that a valid block could be streamed to the
         * plugin even if it is prior to the earliestManagedBlock, granted that
         * there is prior block history and the start of the stream is in the
         * middle of the history.
         */
        @Test
        @DisplayName(
                "Test publish a valid block as items prior to earliestManagedBlock, with history, start mid history")
        void testStreamPriorToEarliestManagedBlockWithHistoryStartMidHistory() {
            // First, we need to ensure we have some history.
            final List<Block> blocks = SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksBatched(0, 5);
            // Disable the plugin's message handling, otherwise it will send notifications.
            historicalBlockFacility.setDisablePlugin();
            // Add all the blocks to the historical block facility.
            for (final Block block : blocks) {
                historicalBlockFacility.handleBlockItemsReceived(toBlockItems(block.items()));
            }
            // Assert that the historical block facility has blocks 0-5
            assertThat(
                    blockNodeContext.historicalBlockProvider().availableBlocks().contains(0, 5));
            // Build a PublishStreamRequest with a valid block as items prior to earliestManagedBlock && mid history
            final BlockItemUnparsed[] block = SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsed(3, 3);
            final BlockItemSetUnparsed blockItems =
                    BlockItemSetUnparsed.newBuilder().blockItems(block).build();
            final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                    .blockItems(blockItems)
                    .build();
            // Before sending the request, re-enable the historical block facility's message handling in order
            // to pick up the live items so we can eventually receive a persisted notification and are able to
            // acknowledge the block.
            historicalBlockFacility.clearDisablePlugin();
            // Send the request to the pipeline
            toPluginPipe.onNext(PublishStreamRequestUnparsed.PROTOBUF.toBytes(request));
            // Await to ensure async execution and assert response
            parkNanos(500_000_000L);
            // Assert that the block has been successfully streamed
            assertThat(fromPluginBytes)
                    .hasSize(1)
                    .first()
                    .extracting(bytesToPublishStreamResponseMapper)
                    .isNotNull()
                    .returns(ResponseOneOfType.ACKNOWLEDGEMENT, responseKindExtractor)
                    .returns(3L, acknowledgementBlockNumberExtractor);
        }

        // @todo(1693) add tests:
        //    - block must continue chain after first streamed
        //    - first block cannot be prior to earliest managed when history is higher than or equal to earliest managed
        //    - add e2e tests
    }
}
