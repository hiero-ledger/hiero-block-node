// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.stream.publisher;

import static java.util.concurrent.locks.LockSupport.parkNanos;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hiero.block.node.app.fixtures.TestUtils.enableDebugLogging;
import static org.hiero.block.node.app.fixtures.blocks.SimpleTestBlockItemBuilder.toBlockItems;
import static org.hiero.block.node.stream.publisher.fixtures.PublishApiUtility.endThisBlock;

import com.hedera.hapi.block.stream.Block;
import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.UncheckedParseException;
import com.hedera.pbj.runtime.grpc.Pipeline;
import com.hedera.pbj.runtime.grpc.ServiceInterface;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow.Subscription;
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
    private static final Function<PublishStreamResponse, Long> endStreamResponseBlockNumberExtractor =
            response -> Objects.requireNonNull(response.endStream()).blockNumber();
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
        private final SimpleInMemoryHistoricalBlockFacility historicalBlockFacility;

        /**
         * Constructor for the plugin tests.
         */
        PluginTest() {
            super(Executors.newSingleThreadExecutor());
            historicalBlockFacility = new SimpleInMemoryHistoricalBlockFacility();
            final StreamPublisherPlugin toTest = new StreamPublisherPlugin();
            start(toTest, toTest.methods().getFirst(), historicalBlockFacility);
        }

        private void reopenPublishStream() {
            fromPluginBytes = new ArrayList<>();
            fromPluginPipe = new Pipeline<>() {
                @Override
                public void clientEndStreamReceived() {
                    LOGGER.log(System.Logger.Level.TRACE, "clientEndStreamReceived");
                }

                @Override
                public void onNext(Bytes item) {
                    fromPluginBytes.add(item);
                }

                @Override
                public void onSubscribe(Subscription subscription) {
                    LOGGER.log(System.Logger.Level.TRACE, "onSubscribe");
                }

                @Override
                public void onError(Throwable throwable) {
                    throw new AssertionError("Unexpected error from plugin", throwable);
                }

                @Override
                public void onComplete() {
                    LOGGER.log(System.Logger.Level.TRACE, "onComplete");
                }
            };
            final ServiceInterface.RequestOptions options = new ServiceInterface.RequestOptions() {
                @Override
                public Optional<String> authority() {
                    return Optional.empty();
                }

                @Override
                public boolean isProtobuf() {
                    return true;
                }

                @Override
                public boolean isJson() {
                    return false;
                }

                @Override
                public String contentType() {
                    return "application/grpc";
                }
            };
            toPluginPipe = serviceInterface.open(plugin.methods().getFirst(), options, fromPluginPipe);
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
                    .returns(Code.INVALID_REQUEST, endStreamResponseCodeExtractor)
                    .returns(-1L, endStreamResponseBlockNumberExtractor);
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
                    .returns(Code.INVALID_REQUEST, endStreamResponseCodeExtractor)
                    .returns(-1L, endStreamResponseBlockNumberExtractor);
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
                    .returns(Code.ERROR, endStreamResponseCodeExtractor)
                    .returns(-1L, endStreamResponseBlockNumberExtractor);
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
            endThisBlock(toPluginPipe, 0L);
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

        @Test
        @DisplayName("Test resend block after incomplete stream and reconnect")
        void testResendBlockAfterIncompleteStreamReconnect() {
            // Stream block 0 to completion and verify the acknowledgement. This establishes
            // normal behaviour before we simulate a mid-stream disconnect.
            final BlockItemUnparsed[] firstBlock = SimpleTestBlockItemBuilder.createSimpleBlockUnparsedWithNumber(0);
            final PublishStreamRequestUnparsed firstRequest = PublishStreamRequestUnparsed.newBuilder()
                    .blockItems(BlockItemSetUnparsed.newBuilder()
                            .blockItems(firstBlock)
                            .build())
                    .build();
            toPluginPipe.onNext(PublishStreamRequestUnparsed.PROTOBUF.toBytes(firstRequest));
            parkNanos(500_000_000L);
            assertThat(fromPluginBytes)
                    .hasSize(1)
                    .first()
                    .extracting(bytesToPublishStreamResponseMapper)
                    .isNotNull()
                    .returns(ResponseOneOfType.ACKNOWLEDGEMENT, responseKindExtractor)
                    .returns(0L, acknowledgementBlockNumberExtractor);
            fromPluginBytes.clear();

            // Begin streaming block 1 but stop before the proof to mimic the publisher
            // dropping the connection mid-block. The in-memory historical facility is
            // temporarily disabled so it will ignore the partial block.
            historicalBlockFacility.setDisablePlugin();

            final BlockItemUnparsed[] secondBlock = SimpleTestBlockItemBuilder.createSimpleBlockUnparsedWithNumber(1);
            final PublishStreamRequestUnparsed secondBlockHeaderRequest = PublishStreamRequestUnparsed.newBuilder()
                    .blockItems(BlockItemSetUnparsed.newBuilder()
                            .blockItems(secondBlock[0])
                            .build())
                    .build();
            toPluginPipe.onNext(PublishStreamRequestUnparsed.PROTOBUF.toBytes(secondBlockHeaderRequest));
            final PublishStreamRequestUnparsed secondBlockRoundRequest = PublishStreamRequestUnparsed.newBuilder()
                    .blockItems(BlockItemSetUnparsed.newBuilder()
                            .blockItems(secondBlock[1])
                            .build())
                    .build();
            toPluginPipe.onNext(PublishStreamRequestUnparsed.PROTOBUF.toBytes(secondBlockRoundRequest));
            parkNanos(200_000_000L);
            toPluginPipe.clientEndStreamReceived();
            parkNanos(200_000_000L);
            fromPluginBytes.clear();
            historicalBlockFacility.clearDisablePlugin();
            // Open a fresh stream to simulate a new publisher connection carrying on with
            // block 1.
            reopenPublishStream();

            // Resend block 1 in the usual three batches (header, round, proof). With the bug
            // fixed the plugin should now accept the resend and acknowledge block 1.
            final PublishStreamRequestUnparsed retryHeaderRequest = PublishStreamRequestUnparsed.newBuilder()
                    .blockItems(BlockItemSetUnparsed.newBuilder()
                            .blockItems(secondBlock[0])
                            .build())
                    .build();
            toPluginPipe.onNext(PublishStreamRequestUnparsed.PROTOBUF.toBytes(retryHeaderRequest));
            final PublishStreamRequestUnparsed retryRoundRequest = PublishStreamRequestUnparsed.newBuilder()
                    .blockItems(BlockItemSetUnparsed.newBuilder()
                            .blockItems(secondBlock[1])
                            .build())
                    .build();
            toPluginPipe.onNext(PublishStreamRequestUnparsed.PROTOBUF.toBytes(retryRoundRequest));
            final PublishStreamRequestUnparsed retryProofRequest = PublishStreamRequestUnparsed.newBuilder()
                    .blockItems(BlockItemSetUnparsed.newBuilder()
                            .blockItems(secondBlock[2])
                            .build())
                    .build();
            toPluginPipe.onNext(PublishStreamRequestUnparsed.PROTOBUF.toBytes(retryProofRequest));
            parkNanos(500_000_000L);

            assertThat(fromPluginBytes).isNotEmpty();
            final PublishStreamResponse response = bytesToPublishStreamResponseMapper.apply(fromPluginBytes.getLast());
            assertThat(response)
                    .isNotNull()
                    .returns(ResponseOneOfType.ACKNOWLEDGEMENT, responseKindExtractor)
                    .returns(1L, acknowledgementBlockNumberExtractor);
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
            historicalBlockFacility = new SimpleInMemoryHistoricalBlockFacility();
        }

        private void activatePlugin(final long earliestManagedBlock) {
            final StreamPublisherPlugin toTest = new StreamPublisherPlugin();
            final Map<String, String> configOverrides =
                    Map.ofEntries(Map.entry("block.node.earliestManagedBlock", Long.toString(earliestManagedBlock)));
            start(toTest, toTest.methods().getFirst(), historicalBlockFacility, configOverrides);
            // Assert that the earliest managed block is set to 10
            final long earliestManagedBlockFromConfig = blockNodeContext
                    .configuration()
                    .getConfigData(NodeConfig.class)
                    .earliestManagedBlock();
            assertThat(earliestManagedBlockFromConfig).isGreaterThan(-1L).isEqualTo(earliestManagedBlock);
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
            activatePlugin(10L);
            // Build a PublishStreamRequest with a valid block as items
            final BlockItemUnparsed[] block = SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsed(0, 1);
            final BlockItemSetUnparsed blockItems =
                    BlockItemSetUnparsed.newBuilder().blockItems(block).build();
            final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                    .blockItems(blockItems)
                    .build();
            // Send the request to the pipeline
            toPluginPipe.onNext(PublishStreamRequestUnparsed.PROTOBUF.toBytes(request));
            endThisBlock(toPluginPipe, 0L);
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
            // Add all the blocks to the historical block facility.
            for (final Block block : blocks) {
                historicalBlockFacility.handleBlockItemsReceived(toBlockItems(block.items()), false);
            }
            // Activate the plugin with the earliest managed block of 10.
            activatePlugin(10L);
            // Assert that the historical block facility has blocks 0-5
            assertThat(blockNodeContext
                            .historicalBlockProvider()
                            .availableBlocks()
                            .contains(0, 5))
                    .isTrue();
            // Build a PublishStreamRequest with a valid block as items prior to earliestManagedBlock && after history
            final BlockItemUnparsed[] block = SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsed(6, 6);
            final BlockItemSetUnparsed blockItems =
                    BlockItemSetUnparsed.newBuilder().blockItems(block).build();
            final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                    .blockItems(blockItems)
                    .build();
            // Send the request to the pipeline
            toPluginPipe.onNext(PublishStreamRequestUnparsed.PROTOBUF.toBytes(request));
            endThisBlock(toPluginPipe, 0L);
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
         * This test aims to assert that streaming a valid block prior to the
         * earliestManagedBlock is not possible when that block is prior to
         * available history, which is also prior to the earliestManagedBlock.
         * No block can be streamed before the latest persisted block, no matter
         * if that value is before, same as or after the earliestManagedBlock.
         */
        @Test
        @DisplayName(
                "Test publish a valid block as items prior to earliestManagedBlock, with history, start before history")
        void testStreamPriorToEarliestManagedBlockWithHistoryStartBeforeHistory() {
            // First, we need to ensure we have some history.
            final int earliestPersistedBlock = 3;
            final int expectedLatestPersistedBlock = 5;
            final List<Block> blocks = SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksBatched(
                    earliestPersistedBlock, expectedLatestPersistedBlock);
            // Add all the blocks to the historical block facility.
            for (final Block block : blocks) {
                historicalBlockFacility.handleBlockItemsReceived(toBlockItems(block.items()), false);
            }
            activatePlugin(10L);
            // Assert that the historical block facility has blocks 3-5
            assertThat(blockNodeContext
                            .historicalBlockProvider()
                            .availableBlocks()
                            .contains(earliestPersistedBlock, expectedLatestPersistedBlock))
                    .isTrue();
            // Build a PublishStreamRequest with a valid block as items prior to earliestManagedBlock && history
            final BlockItemUnparsed[] block = SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsed(2, 2);
            final BlockItemSetUnparsed blockItems =
                    BlockItemSetUnparsed.newBuilder().blockItems(block).build();
            final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                    .blockItems(blockItems)
                    .build();
            // Send the request to the pipeline
            toPluginPipe.onNext(PublishStreamRequestUnparsed.PROTOBUF.toBytes(request));
            endThisBlock(toPluginPipe, 0L);
            // Await to ensure async execution and assert response
            parkNanos(500_000_000L);
            // Assert that the block has been successfully streamed
            assertThat(fromPluginBytes)
                    .hasSize(1)
                    .first()
                    .extracting(bytesToPublishStreamResponseMapper)
                    .isNotNull()
                    .returns(ResponseOneOfType.END_STREAM, responseKindExtractor)
                    .returns(Code.DUPLICATE_BLOCK, endStreamResponseCodeExtractor)
                    .returns((long) expectedLatestPersistedBlock, endStreamResponseBlockNumberExtractor);
        }

        /**
         * This test aims to assert that streaming a valid block prior to the
         * earliestManagedBlock is not possible when that block is in the middle
         * of available history, which is also prior to the
         * earliestManagedBlock. No block can be streamed before the latest
         * persisted block, no matter if that value is before, same as or after
         * the earliestManagedBlock.
         */
        @Test
        @DisplayName(
                "Test publish a valid block as items prior to earliestManagedBlock, with history, start mid history")
        void testStreamPriorToEarliestManagedBlockWithHistoryStartMidHistory() {
            // First, we need to ensure we have some history.
            final int earliestPersistedBlock = 0;
            final int latestPersistedBlock = 5;
            final List<Block> blocks = SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksBatched(
                    earliestPersistedBlock, latestPersistedBlock);
            // Add all the blocks to the historical block facility.
            for (final Block block : blocks) {
                historicalBlockFacility.handleBlockItemsReceived(toBlockItems(block.items()), false);
            }
            activatePlugin(10L);
            // Assert that the historical block facility has blocks 0-5
            assertThat(blockNodeContext
                            .historicalBlockProvider()
                            .availableBlocks()
                            .contains(earliestPersistedBlock, latestPersistedBlock))
                    .isTrue();
            // Build a PublishStreamRequest with a valid block as items prior to earliestManagedBlock && mid history
            final BlockItemUnparsed[] block = SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsed(3, 3);
            final BlockItemSetUnparsed blockItems =
                    BlockItemSetUnparsed.newBuilder().blockItems(block).build();
            final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                    .blockItems(blockItems)
                    .build();
            // Send the request to the pipeline
            toPluginPipe.onNext(PublishStreamRequestUnparsed.PROTOBUF.toBytes(request));
            endThisBlock(toPluginPipe, 0L);
            // Await to ensure async execution and assert response
            parkNanos(500_000_000L);
            // Assert that the block has been successfully streamed
            assertThat(fromPluginBytes)
                    .hasSize(1)
                    .first()
                    .extracting(bytesToPublishStreamResponseMapper)
                    .isNotNull()
                    .returns(ResponseOneOfType.END_STREAM, responseKindExtractor)
                    .returns(Code.DUPLICATE_BLOCK, endStreamResponseCodeExtractor)
                    .returns((long) latestPersistedBlock, endStreamResponseBlockNumberExtractor);
        }

        /**
         * This test aims to assert that a valid block could NOT be streamed to
         * the plugin if it is prior to the earliestManagedBlock and there is
         * prior block history where the latest historical block passes or
         * is equal to the earliestManagedBlock.
         */
        @Test
        @DisplayName(
                "Test publish a valid block as items prior to earliestManagedBlock, with history, latest historical block >= earliestManagedBlock")
        void testStreamPriorToEarliestManagedBlockHistorySurpass() {
            // First, we need to ensure we have some history where the latest historical block is >= the earliest
            // managed block.
            final int expectedLatestPersistedBlockNumber = 10;
            final List<Block> blocks = SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksBatched(
                    expectedLatestPersistedBlockNumber, expectedLatestPersistedBlockNumber);
            for (final Block block : blocks) {
                historicalBlockFacility.handleBlockItemsReceived(toBlockItems(block.items()), false);
            }
            activatePlugin(10L);
            // Assert that the historical block facility has block 10
            assertThat(blockNodeContext
                            .historicalBlockProvider()
                            .availableBlocks()
                            .contains(expectedLatestPersistedBlockNumber))
                    .isTrue();
            // Build a PublishStreamRequest with a valid block as items prior to earliestManagedBlock
            final BlockItemUnparsed[] block = SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsed(3, 3);
            final BlockItemSetUnparsed blockItems =
                    BlockItemSetUnparsed.newBuilder().blockItems(block).build();
            final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                    .blockItems(blockItems)
                    .build();
            // Send the request to the pipeline
            toPluginPipe.onNext(PublishStreamRequestUnparsed.PROTOBUF.toBytes(request));
            endThisBlock(toPluginPipe, 0L);
            // Await to ensure async execution and assert response
            parkNanos(500_000_000L);
            // Assert that the block has been successfully streamed
            assertThat(fromPluginBytes)
                    .hasSize(1)
                    .first()
                    .extracting(bytesToPublishStreamResponseMapper)
                    .isNotNull()
                    .returns(ResponseOneOfType.END_STREAM, responseKindExtractor)
                    .returns(Code.DUPLICATE_BLOCK, endStreamResponseCodeExtractor)
                    .returns((long) expectedLatestPersistedBlockNumber, endStreamResponseBlockNumberExtractor);
        }

        /**
         * This test aims to verify that once a block has been streamed to the
         * plugin prior to the earliest managed block, the chain of blocks
         * must then be followed strictly. Here, we want to make sure that
         * sending the next block which does continue the chain is possible.
         */
        @Test
        @DisplayName(
                "Test publish a valid block as items prior to earliestManagedBlock, next blocks continue the chain")
        void testStreamPriorToEarliestManagedBlockFollowUpContinuesChain() {
            final BlockItemUnparsed[] block0 = SimpleTestBlockItemBuilder.createSimpleBlockUnparsedWithNumber(0);
            // Activate the plugin with the earliest managed block of 10.
            activatePlugin(10L);
            // Then, we need to stream the first block
            final BlockItemSetUnparsed firstRequestSet =
                    BlockItemSetUnparsed.newBuilder().blockItems(block0).build();
            final PublishStreamRequestUnparsed firstRequest = PublishStreamRequestUnparsed.newBuilder()
                    .blockItems(firstRequestSet)
                    .build();
            toPluginPipe.onNext(PublishStreamRequestUnparsed.PROTOBUF.toBytes(firstRequest));
            endThisBlock(toPluginPipe, 0L);
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
            // Clear the plugin pipe
            fromPluginBytes.clear();
            // Now attempt to send the next block
            final BlockItemUnparsed[] block1 = SimpleTestBlockItemBuilder.createSimpleBlockUnparsedWithNumber(1);
            final BlockItemSetUnparsed secondRequestSet =
                    BlockItemSetUnparsed.newBuilder().blockItems(block1).build();
            final PublishStreamRequestUnparsed secondRequest = PublishStreamRequestUnparsed.newBuilder()
                    .blockItems(secondRequestSet)
                    .build();
            toPluginPipe.onNext(PublishStreamRequestUnparsed.PROTOBUF.toBytes(secondRequest));
            // Await to ensure async execution and assert response
            parkNanos(500_000_000L);
            // Assert that the block has been successfully streamed
            assertThat(fromPluginBytes)
                    .hasSize(1)
                    .first()
                    .extracting(bytesToPublishStreamResponseMapper)
                    .isNotNull()
                    .returns(ResponseOneOfType.ACKNOWLEDGEMENT, responseKindExtractor)
                    .returns(1L, acknowledgementBlockNumberExtractor);
        }

        /**
         * This test aims to verify that once a block has been streamed to the
         * plugin prior to the earliest managed block, the chain of blocks
         * must then be followed strictly. Here, we want to make sure that
         * sending the next block which does not continue the chain will not
         * be possible, be that prior to the first block sent, equal to it, or
         * after it but not continuing the chain.
         */
        @Test
        @DisplayName(
                "Test publish a valid block as items prior to earliestManagedBlock, next blocks must continue chain")
        void testStreamPriorToEarliestManagedBlockMustContinueChain() {
            final BlockItemUnparsed[] block0 = SimpleTestBlockItemBuilder.createSimpleBlockUnparsedWithNumber(0);
            // Activate the plugin with the earliest managed block of 10.
            activatePlugin(10L);
            // Then, we need to stream the first block
            final BlockItemSetUnparsed firstRequestSet =
                    BlockItemSetUnparsed.newBuilder().blockItems(block0).build();
            final PublishStreamRequestUnparsed firstRequest = PublishStreamRequestUnparsed.newBuilder()
                    .blockItems(firstRequestSet)
                    .build();
            toPluginPipe.onNext(PublishStreamRequestUnparsed.PROTOBUF.toBytes(firstRequest));
            endThisBlock(toPluginPipe, 0L);
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
            // Clear the plugin pipe
            fromPluginBytes.clear();
            // Now attempt to send the same request again, that should not be possible
            toPluginPipe.onNext(PublishStreamRequestUnparsed.PROTOBUF.toBytes(firstRequest));
            // Assert end stream
            assertThat(fromPluginBytes)
                    .hasSize(1)
                    .first()
                    .extracting(bytesToPublishStreamResponseMapper)
                    .isNotNull()
                    .returns(ResponseOneOfType.END_STREAM, responseKindExtractor)
                    .returns(Code.DUPLICATE_BLOCK, endStreamResponseCodeExtractor)
                    .returns(0L, endStreamResponseBlockNumberExtractor);
        }

        /**
         * This test aims to verify that once a block has been streamed to the
         * plugin prior to the earliest managed block, the chain of blocks
         * must then be followed strictly. Here, we want to make sure that
         * sending the next block which does not continue the chain will not
         * be possible, be that prior to the first block sent, equal to it, or
         * after it but not continuing the chain. This test covers an edge case
         * where we land on the earliest managed block exactly, and the history
         * has just caught up. It should not be allowed to repeat that block.
         */
        @Test
        @DisplayName(
                "Test publish a valid block as items prior to earliestManagedBlock, next blocks must continue chain, with history")
        void testStreamPriorToEarliestManagedBlockMustContinueChainWithHistoryEdge() {
            final BlockItemUnparsed[] block0 = SimpleTestBlockItemBuilder.createSimpleBlockUnparsedWithNumber(0);
            // Activate the plugin with the earliest managed block of 1. This will allow us to hit the edge case.
            activatePlugin(1L);
            // Then, we need to stream the first block
            final BlockItemSetUnparsed firstRequestSet =
                    BlockItemSetUnparsed.newBuilder().blockItems(block0).build();
            final PublishStreamRequestUnparsed firstRequest = PublishStreamRequestUnparsed.newBuilder()
                    .blockItems(firstRequestSet)
                    .build();
            toPluginPipe.onNext(PublishStreamRequestUnparsed.PROTOBUF.toBytes(firstRequest));
            endThisBlock(toPluginPipe, 0L);
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
            // Clear the plugin pipe
            fromPluginBytes.clear();
            // Now attempt to send the same request again, that should not be possible
            toPluginPipe.onNext(PublishStreamRequestUnparsed.PROTOBUF.toBytes(firstRequest));
            // Await to ensure async execution and assert response
            parkNanos(500_000_000L);
            // Assert end stream
            assertThat(fromPluginBytes)
                    .hasSize(1)
                    .first()
                    .extracting(bytesToPublishStreamResponseMapper)
                    .isNotNull()
                    .returns(ResponseOneOfType.END_STREAM, responseKindExtractor)
                    .returns(Code.DUPLICATE_BLOCK, endStreamResponseCodeExtractor)
                    .returns(0L, endStreamResponseBlockNumberExtractor);
        }

        // @todo(1693) add tests:
        //    - add e2e test cases to test plan
    }
}
