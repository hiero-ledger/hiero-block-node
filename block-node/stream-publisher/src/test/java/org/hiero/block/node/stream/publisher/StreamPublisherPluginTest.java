// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.stream.publisher;

import static java.util.concurrent.locks.LockSupport.parkNanos;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hiero.block.node.app.fixtures.TestUtils.enableDebugLogging;
import static org.hiero.block.node.base.ParseHelper.standardParse;
import static org.hiero.block.node.stream.publisher.fixtures.PublishApiUtility.endThisBlock;

import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.UncheckedParseException;
import com.hedera.pbj.runtime.grpc.Pipeline;
import com.hedera.pbj.runtime.grpc.ServiceInterface;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Function;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.hiero.block.api.BlockItemSet;
import org.hiero.block.api.BlockRange;
import org.hiero.block.api.PublishStreamRequest;
import org.hiero.block.api.PublishStreamResponse;
import org.hiero.block.api.PublishStreamResponse.EndOfStream.Code;
import org.hiero.block.api.PublishStreamResponse.ResponseOneOfType;
import org.hiero.block.internal.BlockItemSetUnparsed;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.internal.PublishStreamRequestUnparsed;
import org.hiero.block.node.app.config.node.NodeConfig;
import org.hiero.block.node.app.fixtures.async.ScheduledBlockingExecutor;
import org.hiero.block.node.app.fixtures.blocks.TestBlock;
import org.hiero.block.node.app.fixtures.blocks.TestBlockBuilder;
import org.hiero.block.node.app.fixtures.plugintest.GrpcPluginTestBase;
import org.hiero.block.node.app.fixtures.plugintest.RecordingServiceBuilder;
import org.hiero.block.node.app.fixtures.plugintest.SimpleBlockRangeSet;
import org.hiero.block.node.app.fixtures.plugintest.SimpleInMemoryHistoricalBlockFacility;
import org.hiero.block.node.app.fixtures.plugintest.TestVerificationPlugin;
import org.hiero.block.node.app.fixtures.plugintest.VerificationHandlingHistoricalBlockFacility;
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification.FailureType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/// Tests for the [StreamPublisherPlugin].
@DisplayName("StreamPublisherPlugin Tests")
class StreamPublisherPluginTest {
    // ASSERTION MAPPERS
    private static final Function<Bytes, PublishStreamResponse> bytesToPublishStreamResponseMapper = bytes -> {
        try {
            return standardParse(PublishStreamResponse.PROTOBUF, bytes);
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
    private static final Function<PublishStreamResponse, Long> resendBlockNumberExtractor =
            response -> Objects.requireNonNull(response.resendBlock()).blockNumber();
    private static final Function<PublishStreamResponse, Long> skipBlockNumberExtractor =
            response -> Objects.requireNonNull(response.skipBlock()).blockNumber();

    /// The historical block facility to use when testing.
    private SimpleInMemoryHistoricalBlockFacility historicalBlockFacility;
    private TestVerificationPlugin verificationPlugin;

    /// Enable debug logging for each test.
    @BeforeEach
    void setup() {
        enableDebugLogging();
    }

    /// Test for the [StreamPublisherPlugin] plugin.
    @Nested
    @DisplayName("Plugin Tests")
    class PluginTest extends GrpcPluginTestBase<StreamPublisherPlugin, ExecutorService, ScheduledBlockingExecutor> {
        /// Constructor for the plugin tests.
        PluginTest() {
            super(Executors.newSingleThreadExecutor(), new ScheduledBlockingExecutor(new LinkedBlockingQueue<>()));
            historicalBlockFacility = new SimpleInMemoryHistoricalBlockFacility();
            final StreamPublisherPlugin toTest = new StreamPublisherPlugin();
            verificationPlugin = new TestVerificationPlugin();
            final List<BlockNodePlugin> additionalPlugins = List.of(verificationPlugin);
            start(toTest, toTest.methods().getFirst(), historicalBlockFacility, additionalPlugins);
        }

        /// Verifies that the service interface correctly registers and exposes
        /// the server status method.
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

        /// This test aims to verify that when null block items are published to
        /// the pipeline, an
        /// [PublishStreamResponse.EndOfStream]
        /// response is returned with code [Code#INVALID_REQUEST].
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

        /// This test aims to verify that when empty block items are published to
        /// the pipeline, an
        /// [PublishStreamResponse.EndOfStream]
        /// response is returned with code [Code#INVALID_REQUEST].
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

        /// This test aims to verify that when a request with unset oneOf is
        /// published to the pipeline, an
        /// [PublishStreamResponse.EndOfStream]
        /// response is returned with code [Code#ERROR].
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

        /// This test aims to verify that when a valid block is published to the
        /// pipeline, a [PublishStreamResponse.BlockAcknowledgement]
        /// response is returned.
        @Test
        @DisplayName("Test publish a valid block as items")
        void testPublishValidBlock() {
            final long blockNumber = 0L;
            final TestBlock block = TestBlockBuilder.generateBlockWithNumber(blockNumber);
            // Build a PublishStreamRequest with a valid block as items
            final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                    .blockItems(block.asItemSetUnparsed())
                    .build();
            // Send the request to the pipeline
            toPluginPipe.onNext(PublishStreamRequestUnparsed.PROTOBUF.toBytes(request));
            endThisBlock(toPluginPipe, blockNumber);
            // Await to ensure async execution and assert response
            awaitPluginResponses(1);
            assertThat(fromPluginBytes)
                    .hasSize(1)
                    .first()
                    .extracting(bytesToPublishStreamResponseMapper)
                    .isNotNull()
                    .returns(ResponseOneOfType.ACKNOWLEDGEMENT, responseKindExtractor)
                    .returns(blockNumber, acknowledgementBlockNumberExtractor);
        }

        /// Verifies that {@code open()} with an oversized {@code hiero-correlation-id} header
        /// truncates the ID and still opens the pipeline successfully (covers the warning log branch).
        @Test
        @DisplayName("open() with oversized correlation ID truncates and returns a valid pipeline")
        void testOpenWithOversizedCorrelationIdTruncates() {
            final String oversizedId = "X".repeat(StreamPublisherPlugin.MAX_CORRELATION_ID_LENGTH + 10);
            final ServiceInterface.RequestOptions optionsWithOversizedId = new ServiceInterface.RequestOptions() {
                @Override
                public Optional<String> authority() {
                    return Optional.empty();
                }

                @Override
                public String contentType() {
                    return "application/grpc";
                }

                @Override
                public Map<String, String> metadata() {
                    return Map.of("hiero-correlation-id", oversizedId);
                }
            };
            final Pipeline<? super Bytes> pipe = serviceInterface.open(method, optionsWithOversizedId, fromPluginPipe);
            assertThat(pipe).isNotNull();
        }

        @Test
        @DisplayName("Test resend block after incomplete stream and reconnect")
        void testResendBlockAfterIncompleteStreamReconnect() {
            // Stream block 0 to completion and verify the acknowledgement. This establishes
            // normal behaviour before we simulate a mid-stream disconnect.
            final TestBlock block0 = TestBlockBuilder.generateBlockWithNumber(0);
            final BlockUnparsed firstBlock = block0.blockUnparsed();
            final PublishStreamRequestUnparsed firstRequest = PublishStreamRequestUnparsed.newBuilder()
                    .blockItems(BlockItemSetUnparsed.newBuilder()
                            .blockItems(firstBlock.blockItems())
                            .build())
                    .build();
            toPluginPipe.onNext(PublishStreamRequestUnparsed.PROTOBUF.toBytes(firstRequest));
            endThisBlock(toPluginPipe, block0.number());
            awaitPluginResponses(1);
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
            final TestBlock block1 = TestBlockBuilder.generateBlockWithNumber(1);
            final BlockUnparsed secondBlock = block1.blockUnparsed();
            final List<BlockItemUnparsed> secondBlockItems = secondBlock.blockItems();
            final PublishStreamRequestUnparsed secondBlockHeaderRequest = PublishStreamRequestUnparsed.newBuilder()
                    .blockItems(BlockItemSetUnparsed.newBuilder()
                            .blockItems(secondBlockItems.getFirst())
                            .build())
                    .build();
            toPluginPipe.onNext(PublishStreamRequestUnparsed.PROTOBUF.toBytes(secondBlockHeaderRequest));
            final PublishStreamRequestUnparsed secondBlockRoundRequest = PublishStreamRequestUnparsed.newBuilder()
                    .blockItems(BlockItemSetUnparsed.newBuilder()
                            .blockItems(secondBlockItems.get(1))
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
            setupNewPipelines();
            // Resend block 1 in the usual three batches (header, round, proof). With the bug
            // fixed the plugin should now accept the resend and acknowledge block 1.
            final PublishStreamRequestUnparsed retryHeaderRequest = PublishStreamRequestUnparsed.newBuilder()
                    .blockItems(BlockItemSetUnparsed.newBuilder()
                            .blockItems(secondBlockItems.getFirst())
                            .build())
                    .build();
            toPluginPipe.onNext(PublishStreamRequestUnparsed.PROTOBUF.toBytes(retryHeaderRequest));
            final PublishStreamRequestUnparsed retryRoundRequest = PublishStreamRequestUnparsed.newBuilder()
                    .blockItems(BlockItemSetUnparsed.newBuilder()
                            .blockItems(secondBlockItems.get(1))
                            .build())
                    .build();
            toPluginPipe.onNext(PublishStreamRequestUnparsed.PROTOBUF.toBytes(retryRoundRequest));
            final PublishStreamRequestUnparsed retryProofRequest = PublishStreamRequestUnparsed.newBuilder()
                    .blockItems(BlockItemSetUnparsed.newBuilder()
                            .blockItems(secondBlockItems.getLast())
                            .build())
                    .build();
            toPluginPipe.onNext(PublishStreamRequestUnparsed.PROTOBUF.toBytes(retryProofRequest));
            endThisBlock(toPluginPipe, block1.number());
            awaitPluginResponses(1);
            assertThat(fromPluginBytes).isNotEmpty();
            final PublishStreamResponse response = bytesToPublishStreamResponseMapper.apply(fromPluginBytes.getLast());
            assertThat(response)
                    .isNotNull()
                    .returns(ResponseOneOfType.ACKNOWLEDGEMENT, responseKindExtractor)
                    .returns(1L, acknowledgementBlockNumberExtractor);
        }
    }

    /// Test for the [StreamPublisherPlugin] plugin when publishing a block
    /// prior to the earliest managed block.
    @Nested
    @DisplayName("Plugin Tests Pre Earliest Managed Block")
    class PluginTestsPreEarliestManagedBlock
            extends GrpcPluginTestBase<StreamPublisherPlugin, ExecutorService, ScheduledBlockingExecutor> {
        /// Constructor for the plugin tests.
        PluginTestsPreEarliestManagedBlock() {
            super(Executors.newSingleThreadExecutor(), new ScheduledBlockingExecutor(new LinkedBlockingQueue<>()));
            historicalBlockFacility = new SimpleInMemoryHistoricalBlockFacility();
            verificationPlugin = new TestVerificationPlugin();
        }

        private void activatePlugin(final long earliestManagedBlock) {
            final StreamPublisherPlugin toTest = new StreamPublisherPlugin();
            // Pin the duplicate-block skip window to its minimum so duplicates more than one
            // block behind still produce EndOfStream(DUPLICATE_BLOCK), preserving the legacy
            // assertions in this class. Distance-zero re-publishes still SKIP because that is
            // covered by the window even at its smallest valid value; see the explicit
            // SKIP_BLOCK assertions in the chain-continuation tests below.
            final Map<String, String> configOverrides = Map.ofEntries(
                    Map.entry("block.node.earliestManagedBlock", Long.toString(earliestManagedBlock)),
                    Map.entry("producer.duplicateBlockSkipWindow", "1"));
            final List<BlockNodePlugin> additionalPlugins = List.of(verificationPlugin);
            start(toTest, toTest.methods().getFirst(), historicalBlockFacility, additionalPlugins, configOverrides);
            // Assert that the earliest managed block is set to 10
            final long earliestManagedBlockFromConfig = blockNodeContext
                    .configuration()
                    .getConfigData(NodeConfig.class)
                    .earliestManagedBlock();
            assertThat(earliestManagedBlockFromConfig).isGreaterThan(-1L).isEqualTo(earliestManagedBlock);
        }

        /// This test aims to assert that a valid block could be streamed to the
        /// plugin even if it is prior to the earliestManagedBlock, granted that
        /// this is the first block ever published after the plugin has started.
        /// Here, we have no prior block history.
        @Test
        @DisplayName("Test publish a valid block as items prior to earliestManagedBlock, no history")
        void testStreamPriorToEarliestManagedBlockNoHistory() {
            activatePlugin(10L);
            // Build a PublishStreamRequest with a valid block as items
            final TestBlock block = TestBlockBuilder.generateBlockWithNumber(0);
            final BlockItemSetUnparsed blockItems = block.asItemSetUnparsed();
            final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                    .blockItems(blockItems)
                    .build();
            // Send the request to the pipeline
            toPluginPipe.onNext(PublishStreamRequestUnparsed.PROTOBUF.toBytes(request));
            endThisBlock(toPluginPipe, block.number());
            // Await to ensure async execution and assert response
            awaitPluginResponses(1);
            // Assert that the block has been successfully streamed
            assertThat(fromPluginBytes)
                    .hasSize(1)
                    .first()
                    .extracting(bytesToPublishStreamResponseMapper)
                    .isNotNull()
                    .returns(ResponseOneOfType.ACKNOWLEDGEMENT, responseKindExtractor)
                    .returns(0L, acknowledgementBlockNumberExtractor);
        }

        /// This test aims to assert that a valid block could be streamed to the
        /// plugin even if it is prior to the earliestManagedBlock, granted that
        /// there is prior block history and the start of the stream is after the
        /// history.
        @Test
        @DisplayName(
                "Test publish a valid block as items prior to earliestManagedBlock, with history, start after history")
        void testStreamPriorToEarliestManagedBlockWithHistoryStartAfterHistory() {
            // First, we need to ensure we have some history.
            final List<TestBlock> blocks = TestBlockBuilder.generateBlocksInRange(0, 5);
            // Add all the blocks to the historical block facility.
            for (final TestBlock block : blocks) {
                historicalBlockFacility.handleBlockItemsReceived(block.asBlockItems(), false);
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
            final TestBlock block = TestBlockBuilder.generateBlockWithNumber(6L);
            final long blockNumber = block.number();
            final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                    .blockItems(block.asItemSetUnparsed())
                    .build();
            // Send the request to the pipeline
            toPluginPipe.onNext(PublishStreamRequestUnparsed.PROTOBUF.toBytes(request));
            endThisBlock(toPluginPipe, blockNumber);
            // Await to ensure async execution and assert response
            awaitPluginResponses(1);
            // Assert that the block has been successfully streamed
            assertThat(fromPluginBytes)
                    .hasSize(1)
                    .first()
                    .extracting(bytesToPublishStreamResponseMapper)
                    .isNotNull()
                    .returns(ResponseOneOfType.ACKNOWLEDGEMENT, responseKindExtractor)
                    .returns(blockNumber, acknowledgementBlockNumberExtractor);
        }

        /// This test aims to assert that streaming a valid block prior to the
        /// earliestManagedBlock is not possible when that block is prior to
        /// available history, which is also prior to the earliestManagedBlock.
        /// No block can be streamed before the latest persisted block, no matter
        /// if that value is before, same as or after the earliestManagedBlock.
        @Test
        @DisplayName(
                "Test publish a valid block as items prior to earliestManagedBlock, with history, start before history")
        void testStreamPriorToEarliestManagedBlockWithHistoryStartBeforeHistory() {
            // First, we need to ensure we have some history.
            final int earliestPersistedBlock = 3;
            final int expectedLatestPersistedBlock = 5;
            final List<TestBlock> blocks =
                    TestBlockBuilder.generateBlocksInRange(earliestPersistedBlock, expectedLatestPersistedBlock);
            // Add all the blocks to the historical block facility.
            for (final TestBlock block : blocks) {
                historicalBlockFacility.handleBlockItemsReceived(block.asBlockItems(), false);
            }
            // Mirrors production, where BlockNodeApp merges availableBlocks into storedBlocks
            // before a plugin ever sees the context.
            storedBlocks = List.of(new BlockRange(earliestPersistedBlock, expectedLatestPersistedBlock));
            activatePlugin(10L);
            // Assert that the historical block facility has blocks 3-5
            assertThat(blockNodeContext
                            .historicalBlockProvider()
                            .availableBlocks()
                            .contains(earliestPersistedBlock, expectedLatestPersistedBlock))
                    .isTrue();
            // Build a PublishStreamRequest with a valid block as items prior to earliestManagedBlock && history
            final TestBlock block = TestBlockBuilder.generateBlockWithNumber(2);
            final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                    .blockItems(block.asItemSetUnparsed())
                    .build();
            // Send the request to the pipeline
            toPluginPipe.onNext(PublishStreamRequestUnparsed.PROTOBUF.toBytes(request));
            // Await to ensure async execution and assert response
            awaitPluginResponses(1);
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

        /// This test aims to assert that streaming a valid block prior to the
        /// earliestManagedBlock is not possible when that block is in the middle
        /// of available history, which is also prior to the
        /// earliestManagedBlock. No block can be streamed before the latest
        /// persisted block, no matter if that value is before, same as or after
        /// the earliestManagedBlock.
        @Test
        @DisplayName(
                "Test publish a valid block as items prior to earliestManagedBlock, with history, start mid history")
        void testStreamPriorToEarliestManagedBlockWithHistoryStartMidHistory() {
            // First, we need to ensure we have some history.
            final int earliestPersistedBlock = 0;
            final int latestPersistedBlock = 5;
            final List<TestBlock> blocks =
                    TestBlockBuilder.generateBlocksInRange(earliestPersistedBlock, latestPersistedBlock);
            // Add all the blocks to the historical block facility.
            for (final TestBlock block : blocks) {
                historicalBlockFacility.handleBlockItemsReceived(block.asBlockItems(), false);
            }
            // Mirrors production, where BlockNodeApp merges availableBlocks into storedBlocks
            // before a plugin ever sees the context.
            storedBlocks = List.of(new BlockRange(earliestPersistedBlock, latestPersistedBlock));
            activatePlugin(10L);
            // Assert that the historical block facility has blocks 0-5
            assertThat(blockNodeContext
                            .historicalBlockProvider()
                            .availableBlocks()
                            .contains(earliestPersistedBlock, latestPersistedBlock))
                    .isTrue();
            // Build a PublishStreamRequest with a valid block as items prior to earliestManagedBlock && mid history
            final TestBlock block = TestBlockBuilder.generateBlockWithNumber(3);
            final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                    .blockItems(block.asItemSetUnparsed())
                    .build();
            // Send the request to the pipeline
            toPluginPipe.onNext(PublishStreamRequestUnparsed.PROTOBUF.toBytes(request));
            // Await to ensure async execution and assert response
            awaitPluginResponses(1);
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

        /// This test aims to assert that a valid block could NOT be streamed to
        /// the plugin if it is prior to the earliestManagedBlock and there is
        /// prior block history where the latest historical block passes or
        /// is equal to the earliestManagedBlock.
        @Test
        @DisplayName(
                "Test publish a valid block as items prior to earliestManagedBlock, with history, latest historical block >= earliestManagedBlock")
        void testStreamPriorToEarliestManagedBlockHistorySurpass() {
            // First, we need to ensure we have some history where the latest historical block is >= the earliest
            // managed block.
            final int expectedLatestPersistedBlockNumber = 10;
            final TestBlock block10 = TestBlockBuilder.generateBlockWithNumber(expectedLatestPersistedBlockNumber);
            historicalBlockFacility.handleBlockItemsReceived(block10.asBlockItems(), false);
            // Mirrors production, where BlockNodeApp merges availableBlocks into storedBlocks
            // before a plugin ever sees the context.
            storedBlocks =
                    List.of(new BlockRange(expectedLatestPersistedBlockNumber, expectedLatestPersistedBlockNumber));
            activatePlugin(10L);
            // Assert that the historical block facility has block 10
            assertThat(blockNodeContext
                            .historicalBlockProvider()
                            .availableBlocks()
                            .contains(expectedLatestPersistedBlockNumber))
                    .isTrue();
            // Build a PublishStreamRequest with a valid block as items prior to earliestManagedBlock
            final TestBlock block = TestBlockBuilder.generateBlockWithNumber(3);
            final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                    .blockItems(block.asItemSetUnparsed())
                    .build();
            // Send the request to the pipeline
            toPluginPipe.onNext(PublishStreamRequestUnparsed.PROTOBUF.toBytes(request));
            // Await to ensure async execution and assert response
            awaitPluginResponses(1);
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

        /// This test aims to verify that once a block has been streamed to the
        /// plugin prior to the earliest managed block, the chain of blocks
        /// must then be followed strictly. Here, we want to make sure that
        /// sending the next block which does continue the chain is possible.
        @Test
        @DisplayName(
                "Test publish a valid block as items prior to earliestManagedBlock, next blocks continue the chain")
        void testStreamPriorToEarliestManagedBlockFollowUpContinuesChain() {
            final TestBlock block0 = TestBlockBuilder.generateBlockWithNumber(0);
            // Activate the plugin with the earliest managed block of 10.
            activatePlugin(10L);
            // Then, we need to stream the first block
            final PublishStreamRequestUnparsed firstRequest = PublishStreamRequestUnparsed.newBuilder()
                    .blockItems(block0.asItemSetUnparsed())
                    .build();
            toPluginPipe.onNext(PublishStreamRequestUnparsed.PROTOBUF.toBytes(firstRequest));
            endThisBlock(toPluginPipe, block0.number());
            // Await to ensure async execution and assert response
            awaitPluginResponses(1);
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
            final TestBlock block1 = TestBlockBuilder.generateBlockWithNumber(1);
            final PublishStreamRequestUnparsed secondRequest = PublishStreamRequestUnparsed.newBuilder()
                    .blockItems(block1.asItemSetUnparsed())
                    .build();
            toPluginPipe.onNext(PublishStreamRequestUnparsed.PROTOBUF.toBytes(secondRequest));
            endThisBlock(toPluginPipe, block1.number());
            // Await to ensure async execution and assert response
            awaitPluginResponses(1);
            // Assert that the block has been successfully streamed
            assertThat(fromPluginBytes)
                    .hasSize(1)
                    .first()
                    .extracting(bytesToPublishStreamResponseMapper)
                    .isNotNull()
                    .returns(ResponseOneOfType.ACKNOWLEDGEMENT, responseKindExtractor)
                    .returns(1L, acknowledgementBlockNumberExtractor);
        }

        /// This test aims to verify that once a block has been streamed to the
        /// plugin prior to the earliest managed block, the chain of blocks
        /// must then be followed strictly. Here, we want to make sure that
        /// sending the next block which does not continue the chain will not
        /// be possible, be that prior to the first block sent, equal to it, or
        /// after it but not continuing the chain.
        @Test
        @DisplayName(
                "Test publish a valid block as items prior to earliestManagedBlock, next blocks must continue chain")
        void testStreamPriorToEarliestManagedBlockMustContinueChain() {
            final BlockUnparsed block0 =
                    TestBlockBuilder.generateBlockWithNumber(0).blockUnparsed();
            // Activate the plugin with the earliest managed block of 10.
            activatePlugin(10L);
            // Then, we need to stream the first block
            final BlockItemSetUnparsed firstRequestSet = BlockItemSetUnparsed.newBuilder()
                    .blockItems(block0.blockItems())
                    .build();
            final PublishStreamRequestUnparsed firstRequest = PublishStreamRequestUnparsed.newBuilder()
                    .blockItems(firstRequestSet)
                    .build();
            toPluginPipe.onNext(PublishStreamRequestUnparsed.PROTOBUF.toBytes(firstRequest));
            endThisBlock(toPluginPipe, 0L);
            // Await to ensure async execution and assert response
            awaitPluginResponses(1);
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
            // Distance-zero duplicates fall inside the configured duplicateBlockSkipWindow, so
            // the publisher is told to skip the block rather than having its stream closed.
            assertThat(fromPluginBytes).hasSize(2);
            assertThat(fromPluginBytes)
                    .first()
                    .extracting(bytesToPublishStreamResponseMapper)
                    .isNotNull()
                    .returns(ResponseOneOfType.SKIP_BLOCK, responseKindExtractor)
                    .returns(0L, skipBlockNumberExtractor);
            assertThat(fromPluginBytes)
                    .last()
                    .extracting(bytesToPublishStreamResponseMapper)
                    .isNotNull()
                    .returns(ResponseOneOfType.ACKNOWLEDGEMENT, responseKindExtractor);
        }

        /// This test aims to verify that once a block has been streamed to the
        /// plugin prior to the earliest managed block, the chain of blocks
        /// must then be followed strictly. Here, we want to make sure that
        /// sending the next block which does not continue the chain will not
        /// be possible, be that prior to the first block sent, equal to it, or
        /// after it but not continuing the chain. This test covers an edge case
        /// where we land on the earliest managed block exactly, and the history
        /// has just caught up. It should not be allowed to repeat that block.
        @Test
        @DisplayName(
                "Test publish a valid block as items prior to earliestManagedBlock, next blocks must continue chain, with history")
        void testStreamPriorToEarliestManagedBlockMustContinueChainWithHistoryEdge() {
            final BlockUnparsed block0 =
                    TestBlockBuilder.generateBlockWithNumber(0).blockUnparsed();
            // Activate the plugin with the earliest managed block of 1. This will allow us to hit the edge case.
            activatePlugin(1L);
            // Then, we need to stream the first block
            final BlockItemSetUnparsed firstRequestSet = BlockItemSetUnparsed.newBuilder()
                    .blockItems(block0.blockItems())
                    .build();
            final PublishStreamRequestUnparsed firstRequest = PublishStreamRequestUnparsed.newBuilder()
                    .blockItems(firstRequestSet)
                    .build();
            toPluginPipe.onNext(PublishStreamRequestUnparsed.PROTOBUF.toBytes(firstRequest));
            endThisBlock(toPluginPipe, 0L);
            // Await to ensure async execution and assert response
            awaitPluginResponses(1);
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
            awaitPluginResponses(1);
            // Distance-zero duplicates fall inside the configured duplicateBlockSkipWindow, so
            // the publisher is told to skip the block rather than having its stream closed.
            assertThat(fromPluginBytes).hasSize(2);
            assertThat(fromPluginBytes)
                    .first()
                    .extracting(bytesToPublishStreamResponseMapper)
                    .isNotNull()
                    .returns(ResponseOneOfType.SKIP_BLOCK, responseKindExtractor)
                    .returns(0L, skipBlockNumberExtractor);
            assertThat(fromPluginBytes)
                    .last()
                    .extracting(bytesToPublishStreamResponseMapper)
                    .isNotNull()
                    .returns(ResponseOneOfType.ACKNOWLEDGEMENT, responseKindExtractor);
        }
    }

    /// Tests for failed block verification
    @Nested
    @DisplayName("Failed Verification Tests")
    class PluginTestsFailedVerification
            extends GrpcPluginTestBase<StreamPublisherPlugin, ExecutorService, ScheduledBlockingExecutor> {
        private final VerificationHandlingHistoricalBlockFacility historicalBlockFacility;

        protected PluginTestsFailedVerification() {
            super(Executors.newSingleThreadExecutor(), new ScheduledBlockingExecutor(new LinkedBlockingQueue<>()));
            historicalBlockFacility = new VerificationHandlingHistoricalBlockFacility();
            final StreamPublisherPlugin toTest = new StreamPublisherPlugin();
            verificationPlugin = new TestVerificationPlugin();
            final List<BlockNodePlugin> additionalPlugins = List.of(verificationPlugin);
            start(toTest, toTest.methods().getFirst(), historicalBlockFacility, additionalPlugins);
        }

        /// This test aims to asser that if a block fails verification, it will be scheduled to be resent.
        /// When an active publisher finishes the current block it streams, it must receive the ResendBlock
        /// message for the block that failed verification.
        @RepeatedTest(value = 250, failureThreshold = 1)
        @DisplayName(
                "Test receive the ResendBlock message on block that failed verification when a publisher ends it's current block")
        void testResendBlockReceived() {
            // First, tell the verification plugin to fail once we get block 1. This will also result in the
            // block not being persisted.
            verificationPlugin.failBlocks(1L);
            // Create a second publisher, the first one is automatically created by the plugin test base
            final TestPipeline secondPublisher = createNewPipeline();
            // In the first stage, both publishers expect an acknowledgement for the first streamed block that
            // successfully passes verification and is persisted successfully
            final List<List<Bytes>> ackReceivers = List.of(fromPluginBytes, secondPublisher.fromPluginBytes());
            // Create the test blocks
            final List<TestBlock> blocks0To2 = TestBlockBuilder.generateBlocksInRange(0, 2);
            // Stream block 0, verification will be successful, also the block will be persisted, this will trigger
            // the acknowledgement of the block, we expect every connected publisher to receive the acknowledgement
            streamBlockAndAwaitAcknowledgement(secondPublisher.toPluginPipe(), ackReceivers, blocks0To2.get(0));
            // Now start streaming block 1, do not end it yet
            final TestBlock block1 = blocks0To2.get(1);
            sendBlock(secondPublisher.toPluginPipe(), block1);
            // Now we have to start streaming the next expected block from the first publisher, we want to leave it
            // in a state where it is mid-block. Do not end this yet.
            final TestBlock block2 = blocks0To2.get(2);
            sendBlock(toPluginPipe, block2);
            // End block 1, this will trigger the test verification plugin to fail the verification of block 1.
            endThisBlock(secondPublisher.toPluginPipe(), block1.number());
            // Await and ensure block has failed and the publisher is now closed
            awaitEndOfStream(secondPublisher.fromPluginBytes(), block1, Code.BAD_BLOCK_PROOF);
            // Now we can end streaming block 2, we expect to receive the ResendBlock message because the block
            // that failed should be scheduled for a resend.
            endThisBlock(toPluginPipe, block2.number());
            awaitResend(fromPluginBytes, block1);
        }

        /// This test aims to assert that a failed verification of a Block Node fault or capability
        /// limitation type ends every connected publisher's stream with the ERROR code. No retry against
        /// this node can succeed for these types, so both publishers, including the one that did not
        /// supply the failed block, must receive the EndOfStream message, encouraging them to connect to
        /// a healthy Block Node.
        @ParameterizedTest
        @EnumSource(
                value = FailureType.class,
                names = {
                    "MISSING_VERIFICATION_DATA",
                    "UNKNOWN_ERROR",
                    "UNRECOGNIZED_PROOF_TYPE",
                    "UNSUPPORTED_HAPI_VERSION",
                    "UNSUPPORTED_ITEM_TYPE"
                })
        @DisplayName("Test EndOfStream ERROR to all publishers on Block Node fault failure types")
        void testEndAllStreamsOnBlockNodeFaultTypes(final FailureType failureType) {
            // Tell the verification plugin to fail block 1 with the parameterized failure type.
            verificationPlugin.setFailureType(failureType);
            verificationPlugin.failBlocks(1L);
            // Create a second publisher, the first one is automatically created by the plugin test base
            final TestPipeline secondPublisher = createNewPipeline();
            final List<List<Bytes>> ackReceivers = List.of(fromPluginBytes, secondPublisher.fromPluginBytes());
            final List<TestBlock> blocks0To2 = TestBlockBuilder.generateBlocksInRange(0, 2);
            // Stream block 0 successfully so both publishers are acknowledged.
            streamBlockAndAwaitAcknowledgement(secondPublisher.toPluginPipe(), ackReceivers, blocks0To2.get(0));
            // Start streaming block 1 from the second publisher, do not end it yet.
            final TestBlock block1 = blocks0To2.get(1);
            sendBlock(secondPublisher.toPluginPipe(), block1);
            // Leave the first publisher mid-block on block 2.
            final TestBlock block2 = blocks0To2.get(2);
            sendBlock(toPluginPipe, block2);
            // End block 1, this will trigger the test verification plugin to fail its verification.
            endThisBlock(secondPublisher.toPluginPipe(), block1.number());
            // Await and ensure the block has failed and both streams are ended with ERROR, the failure
            // is a Block Node fault, so it is not attributable to the supplying publisher.
            awaitEndOfStream(secondPublisher.fromPluginBytes(), block1, Code.ERROR);
            awaitEndOfStream(fromPluginBytes, block1, Code.ERROR);
        }

        /// This test aims to assert that a failed verification of a resend only type keeps the
        /// supplying stream open: no EndOfStream is sent, but the failed block is still scheduled for a
        /// resend. These are the unparseable or malformed block types, where the block is not proven bad
        /// and there is no indication of malice, so a retry may succeed, and the cancelled type, where
        /// the complete block was received but the session was cancelled before producing a result. When
        /// an active publisher finishes its current block, it must receive the ResendBlock message for
        /// the failed block.
        @ParameterizedTest
        @EnumSource(
                value = FailureType.class,
                names = {
                    "UNABLE_TO_PARSE",
                    "MISSING_MANDATORY_FIELD",
                    "MISSING_MANDATORY_ITEM",
                    "UNSUPPORTED_STREAM_FORMAT",
                    "CANCELLED"
                })
        @DisplayName("Test no EndOfStream but ResendBlock on resend only failure types, stream stays open")
        void testResendOnlyKeepsSupplyingStreamOpen(final FailureType failureType) {
            // Tell the verification plugin to fail block 1 with the parameterized failure type.
            verificationPlugin.setFailureType(failureType);
            verificationPlugin.failBlocks(1L);
            // Create a second publisher, the first one is automatically created by the plugin test base
            final TestPipeline secondPublisher = createNewPipeline();
            final List<List<Bytes>> ackReceivers = List.of(fromPluginBytes, secondPublisher.fromPluginBytes());
            final List<TestBlock> blocks0To2 = TestBlockBuilder.generateBlocksInRange(0, 2);
            // Stream block 0 successfully so both publishers are acknowledged.
            streamBlockAndAwaitAcknowledgement(secondPublisher.toPluginPipe(), ackReceivers, blocks0To2.get(0));
            // Start streaming block 1 from the second publisher, do not end it yet.
            final TestBlock block1 = blocks0To2.get(1);
            sendBlock(secondPublisher.toPluginPipe(), block1);
            // Leave the first publisher mid-block on block 2.
            final TestBlock block2 = blocks0To2.get(2);
            sendBlock(toPluginPipe, block2);
            // End block 1, this will trigger the test verification plugin to fail its verification.
            endThisBlock(secondPublisher.toPluginPipe(), block1.number());
            awaitBlockFailure(block1);
            // The supplying publisher must not receive any response, its stream stays open.
            assertThat(secondPublisher.fromPluginBytes()).isEmpty();
            // End block 2, we expect the ResendBlock message because the failed block is scheduled for a resend.
            endThisBlock(toPluginPipe, block2.number());
            awaitResend(fromPluginBytes, block1);
            // Assert again that the supplying publisher has received nothing throughout.
            assertThat(secondPublisher.fromPluginBytes()).isEmpty();
        }

        /// This test aims to assert that an informational failure of type BAD_BLOCK_PROOF is handled the
        /// same as a standard one, except that no resend is scheduled: the supplying stream is ended with
        /// the BAD_BLOCK_PROOF code, and when an active publisher finishes its current block, it must
        /// receive the acknowledgement for that block instead of a ResendBlock message, because no
        /// pending resend clamps the acknowledgement.
        @Test
        @DisplayName("Test EndOfStream without ResendBlock on an informational BAD_BLOCK_PROOF failure")
        void testInformationalEndStreamNoResend() {
            // Tell the verification plugin to fail block 1 informationally with a bad proof.
            verificationPlugin.setFailureType(FailureType.BAD_BLOCK_PROOF);
            verificationPlugin.setFailureInformational(true);
            verificationPlugin.failBlocks(1L);
            // Create a second publisher, the first one is automatically created by the plugin test base
            final TestPipeline secondPublisher = createNewPipeline();
            final List<List<Bytes>> ackReceivers = List.of(fromPluginBytes, secondPublisher.fromPluginBytes());
            final List<TestBlock> blocks0To2 = TestBlockBuilder.generateBlocksInRange(0, 2);
            // Stream block 0 successfully so both publishers are acknowledged.
            streamBlockAndAwaitAcknowledgement(secondPublisher.toPluginPipe(), ackReceivers, blocks0To2.get(0));
            // Start streaming block 1 from the second publisher, do not end it yet.
            final TestBlock block1 = blocks0To2.get(1);
            sendBlock(secondPublisher.toPluginPipe(), block1);
            // Leave the first publisher mid-block on block 2.
            final TestBlock block2 = blocks0To2.get(2);
            sendBlock(toPluginPipe, block2);
            // End block 1, this will trigger the test verification plugin to fail its verification.
            endThisBlock(secondPublisher.toPluginPipe(), block1.number());
            // Await and ensure the block has failed and the supplying stream is ended with the expected code.
            awaitEndOfStream(secondPublisher.fromPluginBytes(), block1, Code.BAD_BLOCK_PROOF);
            // End block 2. No resend is scheduled for the failed block, so the acknowledgement for block 2
            // must not be clamped and must arrive instead of a ResendBlock message.
            endThisBlock(toPluginPipe, block2.number());
            awaitAcknowledgements(List.of(fromPluginBytes), block2);
        }

        /// This test aims to assert that an informational failure of a Block Node fault or capability
        /// limitation type is handled the same as a standard one. The Block Node fault is present
        /// regardless of the same block having been verified successfully within reasonable recency, so
        /// both publishers must still receive the EndOfStream message with the ERROR code.
        @ParameterizedTest
        @EnumSource(
                value = FailureType.class,
                names = {
                    "MISSING_VERIFICATION_DATA",
                    "UNKNOWN_ERROR",
                    "UNRECOGNIZED_PROOF_TYPE",
                    "UNSUPPORTED_HAPI_VERSION",
                    "UNSUPPORTED_ITEM_TYPE"
                })
        @DisplayName("Test EndOfStream ERROR to all publishers on informational Block Node fault failure types")
        void testInformationalEndAllStreams(final FailureType failureType) {
            // Tell the verification plugin to fail block 1 informationally with the parameterized failure type.
            verificationPlugin.setFailureType(failureType);
            verificationPlugin.setFailureInformational(true);
            verificationPlugin.failBlocks(1L);
            // Create a second publisher, the first one is automatically created by the plugin test base
            final TestPipeline secondPublisher = createNewPipeline();
            final List<List<Bytes>> ackReceivers = List.of(fromPluginBytes, secondPublisher.fromPluginBytes());
            final List<TestBlock> blocks0To2 = TestBlockBuilder.generateBlocksInRange(0, 2);
            // Stream block 0 successfully so both publishers are acknowledged.
            streamBlockAndAwaitAcknowledgement(secondPublisher.toPluginPipe(), ackReceivers, blocks0To2.get(0));
            // Start streaming block 1 from the second publisher, do not end it yet.
            final TestBlock block1 = blocks0To2.get(1);
            sendBlock(secondPublisher.toPluginPipe(), block1);
            // Leave the first publisher mid-block on block 2.
            final TestBlock block2 = blocks0To2.get(2);
            sendBlock(toPluginPipe, block2);
            // End block 1, this will trigger the test verification plugin to fail its verification.
            endThisBlock(secondPublisher.toPluginPipe(), block1.number());
            // Await and ensure the block has failed and both streams are ended with ERROR, the failure
            // is a Block Node fault, so it is not attributable to the supplying publisher.
            awaitEndOfStream(secondPublisher.fromPluginBytes(), block1, Code.ERROR);
            awaitEndOfStream(fromPluginBytes, block1, Code.ERROR);
        }

        /// This test aims to assert that an informational failure of an unparseable or malformed block
        /// type produces no observable action: no EndOfStream is sent, no resend is scheduled, and both
        /// publishers stay open and receive the acknowledgement for the next persisted block.
        @ParameterizedTest
        @EnumSource(
                value = FailureType.class,
                names = {
                    "UNABLE_TO_PARSE",
                    "MISSING_MANDATORY_FIELD",
                    "MISSING_MANDATORY_ITEM",
                    "UNSUPPORTED_STREAM_FORMAT"
                })
        @DisplayName("Test no responses at all on informational failures of possibly transient types")
        void testInformationalResendOnlyNoAction(final FailureType failureType) {
            // Tell the verification plugin to fail block 1 informationally with the parameterized failure type.
            verificationPlugin.setFailureType(failureType);
            verificationPlugin.setFailureInformational(true);
            verificationPlugin.failBlocks(1L);
            // Create a second publisher, the first one is automatically created by the plugin test base
            final TestPipeline secondPublisher = createNewPipeline();
            final List<List<Bytes>> ackReceivers = List.of(fromPluginBytes, secondPublisher.fromPluginBytes());
            final List<TestBlock> blocks0To2 = TestBlockBuilder.generateBlocksInRange(0, 2);
            // Stream block 0 successfully so both publishers are acknowledged.
            streamBlockAndAwaitAcknowledgement(secondPublisher.toPluginPipe(), ackReceivers, blocks0To2.get(0));
            // Start streaming block 1 from the second publisher, do not end it yet.
            final TestBlock block1 = blocks0To2.get(1);
            sendBlock(secondPublisher.toPluginPipe(), block1);
            // Leave the first publisher mid-block on block 2.
            final TestBlock block2 = blocks0To2.get(2);
            sendBlock(toPluginPipe, block2);
            // End block 1, this will trigger the test verification plugin to fail its verification.
            endThisBlock(secondPublisher.toPluginPipe(), block1.number());
            awaitBlockFailure(block1);
            // The supplying publisher must not receive any response, its stream stays open.
            assertThat(secondPublisher.fromPluginBytes()).isEmpty();
            // End block 2. No resend is scheduled, so both publishers stay open and must receive the
            // acknowledgement for block 2 instead of a ResendBlock message.
            endThisBlock(toPluginPipe, block2.number());
            awaitAcknowledgements(ackReceivers, block2);
        }

        /// This test aims to assert that a failed verification of type CANCELLED_INCOMPLETE is only logged: no
        /// EndOfStream is sent, no resend is scheduled, and both publishers stay open and receive the
        /// acknowledgement for the next persisted block. An incomplete session means the block was never
        /// fully received, so it is already handled and no action is needed.
        @Test
        @DisplayName("Test no responses at all on a CANCELLED_INCOMPLETE failure, both streams stay open")
        void testIncompleteNoAction() {
            // Tell the verification plugin to fail block 1 with the CANCELLED_INCOMPLETE failure type.
            verificationPlugin.setFailureType(FailureType.CANCELLED_INCOMPLETE);
            verificationPlugin.failBlocks(1L);
            // Create a second publisher, the first one is automatically created by the plugin test base
            final TestPipeline secondPublisher = createNewPipeline();
            final List<List<Bytes>> ackReceivers = List.of(fromPluginBytes, secondPublisher.fromPluginBytes());
            final List<TestBlock> blocks0To2 = TestBlockBuilder.generateBlocksInRange(0, 2);
            // Stream block 0 successfully so both publishers are acknowledged.
            streamBlockAndAwaitAcknowledgement(secondPublisher.toPluginPipe(), ackReceivers, blocks0To2.get(0));
            // Start streaming block 1 from the second publisher, do not end it yet.
            final TestBlock block1 = blocks0To2.get(1);
            sendBlock(secondPublisher.toPluginPipe(), block1);
            // Leave the first publisher mid-block on block 2.
            final TestBlock block2 = blocks0To2.get(2);
            sendBlock(toPluginPipe, block2);
            // End block 1, this will trigger the test verification plugin to fail its verification.
            endThisBlock(secondPublisher.toPluginPipe(), block1.number());
            awaitBlockFailure(block1);
            // The supplying publisher must not receive any response, its stream stays open.
            assertThat(secondPublisher.fromPluginBytes()).isEmpty();
            // End block 2. No resend is scheduled, so both publishers stay open and must receive the
            // acknowledgement for block 2 instead of a ResendBlock message.
            endThisBlock(toPluginPipe, block2.number());
            awaitAcknowledgements(ackReceivers, block2);
        }

        private static void sendBlock(final Pipeline<? super Bytes> requestSender, final TestBlock block) {
            final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                    .blockItems(block.asItemSetUnparsed())
                    .build();
            requestSender.onNext(PublishStreamRequestUnparsed.PROTOBUF.toBytes(request));
        }

        private void streamBlockAndAwaitAcknowledgement(
                final Pipeline<? super Bytes> requestSender,
                final List<List<Bytes>> acknowledgementReceivers,
                final TestBlock block) {
            sendBlock(requestSender, block);
            endThisBlock(requestSender, block.number());
            awaitAcknowledgements(acknowledgementReceivers, block);
        }

        private void awaitAcknowledgements(final List<List<Bytes>> acknowledgementReceivers, final TestBlock block) {
            // Await to ensure async execution and assert response
            awaitPluginResponses(acknowledgementReceivers, 1);
            // Assert that the block has been successfully streamed
            assertThat(acknowledgementReceivers).allSatisfy(receiver -> {
                assertThat(receiver)
                        .hasSize(1)
                        .first()
                        .extracting(bytesToPublishStreamResponseMapper)
                        .isNotNull()
                        .returns(ResponseOneOfType.ACKNOWLEDGEMENT, responseKindExtractor)
                        .returns(block.number(), acknowledgementBlockNumberExtractor);
            });
            acknowledgementReceivers.forEach(List::clear);
        }

        /// Waits until the verification plugin has failed the given block and
        /// asserts that exactly one failure has been recorded for it.
        private void awaitBlockFailure(final TestBlock block) {
            final long timeout = 5_000_000_000L; // 5 seconds
            final long deadline = System.nanoTime() + timeout;
            while (verificationPlugin.blockFailures(block.number()) <= 0 && System.nanoTime() < deadline) {
                parkNanos(1_000_000L);
            }
            // Assert that the block has failed verification
            assertThat(verificationPlugin.blockFailures(block.number())).isOne();
        }

        private void awaitEndOfStream(
                final List<Bytes> endOfStreamReceiver, final TestBlock block, final Code expectedCode) {
            awaitBlockFailure(block);
            awaitPluginResponses(List.of(endOfStreamReceiver), 1);
            // Assert the end of stream is received by the publisher that has supplied the failing block
            assertThat(endOfStreamReceiver)
                    .hasSize(1)
                    .first()
                    .extracting(bytesToPublishStreamResponseMapper)
                    .isNotNull()
                    .returns(ResponseOneOfType.END_STREAM, responseKindExtractor)
                    .returns(expectedCode, endStreamResponseCodeExtractor)
                    .returns(historicalBlockFacility.availableBlocks().max(), endStreamResponseBlockNumberExtractor);
            endOfStreamReceiver.clear();
        }

        private void awaitResend(final List<Bytes> resendReceiver, final TestBlock block) {
            awaitBlockFailure(block);
            awaitPluginResponses(List.of(resendReceiver), 1);
            final List<PublishStreamResponse> filteredResponses = resendReceiver.stream()
                    .map(bytesToPublishStreamResponseMapper)
                    .filter(PublishStreamResponse::hasResendBlock)
                    .toList();
            // Assert resend received
            assertThat(filteredResponses)
                    .hasSize(1)
                    .first()
                    .isNotNull()
                    .returns(ResponseOneOfType.RESEND_BLOCK, responseKindExtractor)
                    .returns(block.number(), resendBlockNumberExtractor);
            resendReceiver.clear();
        }
    }

    /// Verifies [StreamPublisherPlugin] reacts to `onContextUpdate()` so the publisher
    /// watermark is seeded from the stored-block range delivered after `init()`, not the
    /// stale context captured at `init()` time. Drives `init()` -> `onContextUpdate()` ->
    /// `start()` via [#doInit] and [#replaceStoredBlocks], mirroring production startup order.
    @Nested
    @DisplayName("Plugin Tests ASF Watermark Seeding")
    class PluginTestsAsfWatermarkSeeding
            extends GrpcPluginTestBase<StreamPublisherPlugin, ExecutorService, ScheduledBlockingExecutor> {
        /// Does not call `start()` — the test method drives `doInit()`/`replaceStoredBlocks()`/
        /// `doStart()` in sequence instead.
        PluginTestsAsfWatermarkSeeding() {
            super(Executors.newSingleThreadExecutor(), new ScheduledBlockingExecutor(new LinkedBlockingQueue<>()));
            historicalBlockFacility = new SimpleInMemoryHistoricalBlockFacility();
            verificationPlugin = new TestVerificationPlugin();
        }

        /// Like [GrpcPluginTestBase#start], but splits `init()`/`start()` so
        /// [#replaceStoredBlocks] can run in between.
        private void activatePluginWithStoredBlocksDeliveredBeforeStart(
                final StreamPublisherPlugin toTest, final List<BlockRange> storedBlocksAfterInit) {
            doInit(toTest, historicalBlockFacility, List.of(verificationPlugin), null, Map.of());
            replaceStoredBlocks(storedBlocksAfterInit);
            doStart();
            method = toTest.methods().getFirst();
            if (webserviceBuilder instanceof RecordingServiceBuilder recordingBuilder
                    && !recordingBuilder.grpcServiceRegistrations().isEmpty()) {
                serviceInterface =
                        recordingBuilder.grpcServiceRegistrations().getLast().service();
            }
            setupNewPipelines();
        }

        /// Historical provider has blocks 0-10; the stored-block range delivered before
        /// `start()` reports up to 50. If the watermark seeded from 50, publishing block
        /// 11 is rejected as a duplicate carrying watermark 50. If the update was missed,
        /// the watermark stays at 10 and block 11 is accepted as the next expected block.
        @Test
        @DisplayName("start() seeds watermark from ASF stored blocks delivered via onContextUpdate before start()")
        void testWatermarkSeedsFromStoredBlocksDeliveredBeforeStart() {
            final SimpleBlockRangeSet availableBlocks = new SimpleBlockRangeSet();
            availableBlocks.add(0, 10);
            historicalBlockFacility.setTemporaryAvailableBlocks(availableBlocks);
            final StreamPublisherPlugin toTest = new StreamPublisherPlugin();
            activatePluginWithStoredBlocksDeliveredBeforeStart(toTest, List.of(new BlockRange(0L, 50L)));

            final long staleBlockNumber = 11L;
            final TestBlock block = TestBlockBuilder.generateBlockWithNumber(staleBlockNumber);
            final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                    .blockItems(block.asItemSetUnparsed())
                    .build();
            toPluginPipe.onNext(PublishStreamRequestUnparsed.PROTOBUF.toBytes(request));
            awaitPluginResponses(1);
            assertThat(fromPluginBytes)
                    .hasSize(1)
                    .first()
                    .extracting(bytesToPublishStreamResponseMapper)
                    .isNotNull()
                    .returns(ResponseOneOfType.END_STREAM, responseKindExtractor)
                    .returns(Code.DUPLICATE_BLOCK, endStreamResponseCodeExtractor)
                    .returns(50L, endStreamResponseBlockNumberExtractor);
        }
    }

    /// Tests for {@link StreamPublisherPlugin#truncateCorrelationId(String)}.
    @Nested
    @DisplayName("truncateCorrelationId() Tests")
    class TruncateCorrelationIdTest {

        @Test
        @DisplayName("Value within limit is returned unchanged")
        void testValueWithinLimit() {
            final String id = "N3-STR1";
            assertThat(StreamPublisherPlugin.truncateCorrelationId(id)).isEqualTo(id);
        }

        @Test
        @DisplayName("Empty string is returned unchanged")
        void testEmptyString() {
            assertThat(StreamPublisherPlugin.truncateCorrelationId("")).isEmpty();
        }

        @Test
        @DisplayName("Value exactly at limit is returned unchanged")
        void testValueAtLimit() {
            final String id = "A".repeat(StreamPublisherPlugin.MAX_CORRELATION_ID_LENGTH);
            assertThat(StreamPublisherPlugin.truncateCorrelationId(id)).isEqualTo(id);
        }

        @Test
        @DisplayName("Value exceeding limit is truncated to MAX_CORRELATION_ID_LENGTH characters")
        void testValueExceedingLimit() {
            final String id = "A".repeat(StreamPublisherPlugin.MAX_CORRELATION_ID_LENGTH + 10);
            final String result = StreamPublisherPlugin.truncateCorrelationId(id);
            assertThat(result).hasSize(StreamPublisherPlugin.MAX_CORRELATION_ID_LENGTH);
            assertThat(result).isEqualTo("A".repeat(StreamPublisherPlugin.MAX_CORRELATION_ID_LENGTH));
        }

        @Test
        @DisplayName("Truncated value preserves the first MAX_CORRELATION_ID_LENGTH characters")
        void testTruncationPreservesPrefix() {
            final String prefix = "N3-STR1-";
            final String id = prefix + "X".repeat(StreamPublisherPlugin.MAX_CORRELATION_ID_LENGTH);
            final String result = StreamPublisherPlugin.truncateCorrelationId(id);
            assertThat(result).startsWith(prefix);
            assertThat(result).hasSize(StreamPublisherPlugin.MAX_CORRELATION_ID_LENGTH);
        }

        @Test
        @DisplayName("Null is treated as absent and returns empty string")
        void testNullReturnsEmpty() {
            assertThat(StreamPublisherPlugin.truncateCorrelationId(null)).isEmpty();
        }
    }
}
