// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.stream.subscriber;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.hiero.block.node.app.fixtures.blocks.TestBlockBuilder.generateBlockWithNumber;
import static org.hiero.block.node.app.fixtures.blocks.TestBlockBuilder.sampleHeaderUnparsed;
import static org.hiero.block.node.app.fixtures.blocks.TestBlockBuilder.sampleProofUnparsed;
import static org.hiero.block.node.app.fixtures.blocks.TestBlockBuilder.sampleRoundHeaderUnparsed;

import com.swirlds.config.api.Configuration;
import com.swirlds.config.api.ConfigurationBuilder;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.stream.Stream;
import org.hiero.block.api.SubscribeStreamRequest;
import org.hiero.block.api.SubscribeStreamResponse.Code;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.SubscribeStreamResponseUnparsed;
import org.hiero.block.internal.SubscribeStreamResponseUnparsed.ResponseOneOfType;
import org.hiero.block.node.app.fixtures.pipeline.TestResponsePipeline;
import org.hiero.block.node.app.fixtures.plugintest.SimpleBlockRangeSet;
import org.hiero.block.node.app.fixtures.plugintest.SimpleInMemoryHistoricalBlockFacility;
import org.hiero.block.node.app.fixtures.plugintest.TestBlockMessagingFacility;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.NoBackPressureBlockItemHandler;
import org.hiero.block.node.spi.historicalblocks.BlockRangeSet;
import org.hiero.block.node.stream.subscriber.BlockStreamSubscriberSession.SessionContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Unit tests for {@link BlockStreamSubscriberSession}.
 */
@DisplayName("BlockStreamSubscriberSession Tests")
class BlockStreamSubscriberSessionTest {
    // EXTRACTORS
    private static final Function<SubscribeStreamResponseUnparsed, ResponseOneOfType> responseTypeExtractor =
            response -> response.response().kind();
    private static final Function<SubscribeStreamResponseUnparsed, Code> responseStatusExtractor =
            SubscribeStreamResponseUnparsed::status;

    // SESSION FIELDS
    /** Client id of the session. */
    private long clientId;
    /** Response pipeline for the session. */
    private TestResponsePipeline<SubscribeStreamResponseUnparsed> responsePipeline;
    /** Historical block facility for the block node context. */
    private SimpleInMemoryHistoricalBlockFacility historicalBlockFacility;
    /** Default subscriber configuration for the block node context. */
    private SubscriberConfig defaultSubscriberConfig;
    /** Block node context for the session. */
    private BlockNodeContext blockNodeContext;
    /** Session ready latch for the session. */
    private CountDownLatch sessionReadyLatch;

    /**
     * Environment setup before each test.
     */
    @BeforeEach
    void setup() {
        clientId = 0L;
        responsePipeline = new TestResponsePipeline<>();
        historicalBlockFacility = new SimpleInMemoryHistoricalBlockFacility();
        final Configuration configuration = ConfigurationBuilder.create()
                .withConfigDataType(SubscriberConfig.class)
                .build();
        defaultSubscriberConfig = configuration.getConfigData(SubscriberConfig.class);
        final TestBlockMessagingFacility messagingFacility = new TestBlockMessagingFacility();
        blockNodeContext = generateContext(configuration, messagingFacility, historicalBlockFacility);
        sessionReadyLatch = new CountDownLatch(1);
    }

    /**
     * Tests for the constructor of {@link BlockStreamSubscriberSession}.
     */
    @SuppressWarnings("all")
    @Nested
    @DisplayName("Constructor Tests")
    class ConstructorTests {
        /** Valid session context. */
        private SessionContext sessionContext;

        /**
         * Environment setup before each test.
         */
        @BeforeEach
        void setup() {
            final SubscribeStreamRequest validRequest = SubscribeStreamRequest.newBuilder()
                    .startBlockNumber(-1L)
                    .endBlockNumber(-1L)
                    .build();
            sessionContext = SessionContext.create(clientId, validRequest, blockNodeContext);
        }

        /**
         * This test aims to assert that the constructor does not throw
         * exceptions when provided with valid parameters.
         */
        @Test
        @DisplayName("Test Constructor with Valid Parameters")
        void testValidParameters() {
            assertThatNoException()
                    .isThrownBy(() -> new BlockStreamSubscriberSession(
                            sessionContext, responsePipeline, blockNodeContext, sessionReadyLatch));
        }

        /**
         * This test aims to assert that the constructor throws a
         * {@link NullPointerException} when provided with a null session
         * context.
         */
        @Test
        @DisplayName("Test Constructor with Null Session Context")
        void testNullRequest() {
            assertThatNullPointerException()
                    .isThrownBy(() -> new BlockStreamSubscriberSession(
                            null, responsePipeline, blockNodeContext, sessionReadyLatch));
        }

        /**
         * This test aims to assert that the constructor throws a
         * {@link NullPointerException} when provided with a null response
         * pipeline.
         */
        @Test
        @DisplayName("Test Constructor with Null Response Pipeline")
        void testNullResponsePipeline() {
            assertThatNullPointerException()
                    .isThrownBy(() -> new BlockStreamSubscriberSession(
                            sessionContext, null, blockNodeContext, sessionReadyLatch));
        }

        /**
         * This test aims to assert that the constructor throws a
         * {@link NullPointerException} when provided with a null block node
         * context.
         */
        @Test
        @DisplayName("Test Constructor with Null Block Node Context")
        void testNullBlockNodeContext() {
            assertThatNullPointerException()
                    .isThrownBy(() -> new BlockStreamSubscriberSession(
                            sessionContext, responsePipeline, null, sessionReadyLatch));
        }

        /**
         * This test aims to assert that the constructor throws a
         * {@link NullPointerException} when provided with a null session
         * ready latch.
         */
        @Test
        @DisplayName("Test Constructor with Null Session Ready Latch")
        void testNullSessionReadyLatch() {
            assertThatNullPointerException()
                    .isThrownBy(() ->
                            new BlockStreamSubscriberSession(sessionContext, responsePipeline, blockNodeContext, null));
        }
    }

    /**
     * Validation tests for {@link BlockStreamSubscriberSession}.
     */
    @Nested
    @DisplayName("Validation Tests")
    class ValidationTests {
        /**
         * Tests for valid requests.
         */
        @Nested
        @DisplayName("Valid Request Tests")
        class ValidRequestTests {
            /**
             * This test aims to assert that the
             * {@link BlockStreamSubscriberSession#validateRequest} method returns
             * {@code true} if the request is valid for live blocks, i.e. when both
             * start and end block numbers are
             * {@value org.hiero.block.node.spi.BlockNodePlugin#UNKNOWN_BLOCK_NUMBER}.
             * No responses are expected to be sent to the response pipeline.
             */
            @Test
            @DisplayName("Test Validate Request - Valid Request: start == -1L && end == -1L")
            void testValidRequestForLiveBlocks() {
                // First we create a valid request for live blocks
                final SubscribeStreamRequest request = SubscribeStreamRequest.newBuilder()
                        .startBlockNumber(-1L)
                        .endBlockNumber(-1L)
                        .build();
                // Then, we create a valid session to test
                final BlockStreamSubscriberSession toTest = generateSession(request);
                // Call
                final boolean actual = toTest.validateRequest();
                // Assert that the request is valid
                assertThat(actual).isTrue();
                // Assert that no responses were sent to the response pipeline
                assertThat(responsePipeline.getClientEndStreamCalls()).hasValue(0);
                assertThat(responsePipeline.getOnCompleteCalls()).hasValue(0);
                assertThat(responsePipeline.getOnNextCalls()).isEmpty();
                assertThat(responsePipeline.getOnSubscriptionCalls()).isEmpty();
                assertThat(responsePipeline.getOnErrorCalls()).isEmpty();
            }

            /**
             * This test aims to assert that the
             * {@link BlockStreamSubscriberSession#validateRequest} method returns
             * {@code true} if the request is valid for blocks in range, starting
             * from the first available, up to a closed range (whole number), i.e.
             * when start is
             * {@value org.hiero.block.node.spi.BlockNodePlugin#UNKNOWN_BLOCK_NUMBER}
             * and end is a whole number. No responses are expected to be sent to
             * the response pipeline.
             */
            @Test
            @DisplayName("Test Validate Request - Valid Request: start == -1L && end >= 0L")
            void testValidRequestEarliestAvailableClosedRange() {
                // First we create a valid request for blocks in range, starting from the earliest available
                final SubscribeStreamRequest request = SubscribeStreamRequest.newBuilder()
                        .startBlockNumber(-1L)
                        .endBlockNumber(10L)
                        .build();
                // Then, we create a valid session to test
                final BlockStreamSubscriberSession toTest = generateSession(request);
                // Call
                final boolean actual = toTest.validateRequest();
                // Assert that the request is valid
                assertThat(actual).isTrue();
                // Assert that no responses were sent to the response pipeline
                assertThat(responsePipeline.getClientEndStreamCalls()).hasValue(0);
                assertThat(responsePipeline.getOnCompleteCalls()).hasValue(0);
                assertThat(responsePipeline.getOnNextCalls()).isEmpty();
                assertThat(responsePipeline.getOnSubscriptionCalls()).isEmpty();
                assertThat(responsePipeline.getOnErrorCalls()).isEmpty();
            }

            /**
             * This test aims to assert that the
             * {@link BlockStreamSubscriberSession#validateRequest} method returns
             * {@code true} if the request is valid for blocks in range, starting
             * from a valid block number and is open ranged (stream indefinitely).
             * No responses are expected to be sent to the response pipeline.
             */
            @Test
            @DisplayName("Test Validate Request - Valid Request: start >=  0L && end == -1L")
            void testValidRequestFromSpecificBlockOpenRange() {
                // First we create a valid request for blocks in range, starting from a specific block number
                final SubscribeStreamRequest request = SubscribeStreamRequest.newBuilder()
                        .startBlockNumber(5L)
                        .endBlockNumber(-1L)
                        .build();
                // Then, we create a valid session to test
                final BlockStreamSubscriberSession toTest = generateSession(request);
                // Call
                final boolean actual = toTest.validateRequest();
                // Assert that the request is valid
                assertThat(actual).isTrue();
                // Assert that no responses were sent to the response pipeline
                assertThat(responsePipeline.getClientEndStreamCalls()).hasValue(0);
                assertThat(responsePipeline.getOnCompleteCalls()).hasValue(0);
                assertThat(responsePipeline.getOnNextCalls()).isEmpty();
                assertThat(responsePipeline.getOnSubscriptionCalls()).isEmpty();
                assertThat(responsePipeline.getOnErrorCalls()).isEmpty();
            }

            /**
             * This test aims to assert that the
             * {@link BlockStreamSubscriberSession#validateRequest} method
             * returns {@code true} if the request is valid for blocks in range,
             * starting from a valid block number up to a closed range and both
             * start and end are the same number. No responses are expected to
             * be sent to the response pipeline.
             */
            @Test
            @DisplayName("Test Validate Request - Valid Request: start >= 0L && start == end")
            void testValidRequestFromSpecificBlockSingleBlock() {
                // First we create a valid request for blocks in range, starting from a specific block number
                final SubscribeStreamRequest request = SubscribeStreamRequest.newBuilder()
                        .startBlockNumber(5L)
                        .endBlockNumber(5L)
                        .build();
                // Then, we create a valid session to test
                final BlockStreamSubscriberSession toTest = generateSession(request);
                // Call
                final boolean actual = toTest.validateRequest();
                // Assert that the request is valid
                assertThat(actual).isTrue();
                // Assert that no responses were sent to the response pipeline
                assertThat(responsePipeline.getClientEndStreamCalls()).hasValue(0);
                assertThat(responsePipeline.getOnCompleteCalls()).hasValue(0);
                assertThat(responsePipeline.getOnNextCalls()).isEmpty();
                assertThat(responsePipeline.getOnSubscriptionCalls()).isEmpty();
                assertThat(responsePipeline.getOnErrorCalls()).isEmpty();
            }

            /**
             * This test aims to assert that the
             * {@link BlockStreamSubscriberSession#validateRequest} method
             * returns {@code true} if the request is valid for blocks in range,
             * starting from a valid block number up to a closed range. No
             * responses are expected to be sent to the response pipeline.
             */
            @Test
            @DisplayName("Test Validate Request - Valid Request: start >= 0L && end > start")
            void testValidRequestFromSpecificBlockClosedRange() {
                // First we create a valid request for blocks in range, starting from a specific block number
                final SubscribeStreamRequest request = SubscribeStreamRequest.newBuilder()
                        .startBlockNumber(5L)
                        .endBlockNumber(10L)
                        .build();
                // Then, we create a valid session to test
                final BlockStreamSubscriberSession toTest = generateSession(request);
                // Call
                final boolean actual = toTest.validateRequest();
                // Assert that the request is valid
                assertThat(actual).isTrue();
                // Assert that no responses were sent to the response pipeline
                assertThat(responsePipeline.getClientEndStreamCalls()).hasValue(0);
                assertThat(responsePipeline.getOnCompleteCalls()).hasValue(0);
                assertThat(responsePipeline.getOnNextCalls()).isEmpty();
                assertThat(responsePipeline.getOnSubscriptionCalls()).isEmpty();
                assertThat(responsePipeline.getOnErrorCalls()).isEmpty();
            }
        }

        /**
         * Tests for invalid requests.
         */
        @Nested
        @DisplayName("Invalid Request Tests")
        class InvalidRequestTests {
            /**
             * This test aims to assert that the
             * {@link BlockStreamSubscriberSession#validateRequest} method
             * returns {@code false} if the request is invalid due to start
             * being lower than
             * {@value org.hiero.block.node.spi.BlockNodePlugin#UNKNOWN_BLOCK_NUMBER}.
             * It is also expected that a response with status code
             * {@link Code#INVALID_START_BLOCK_NUMBER} is sent to the
             * subscriber. The session and the connection with the subscriber
             * is expected to be closed.
             */
            @Test
            @DisplayName("Test Validate Request - Invalid Request: start < -1L")
            void testInvalidRequestStartLessThanNegativeOne() {
                // First we create an invalid request with start < -1L
                final SubscribeStreamRequest request = SubscribeStreamRequest.newBuilder()
                        .startBlockNumber(-2L)
                        .endBlockNumber(-1L)
                        .build();
                // Then, we create a valid session to test
                final BlockStreamSubscriberSession toTest = generateSession(request);
                // Call
                final boolean actual = toTest.validateRequest();
                // Assert that the request is invalid
                assertThat(actual).isFalse();
                // Assert response sent to the response pipeline
                assertThat(responsePipeline.getOnNextCalls())
                        .isNotEmpty()
                        .hasSize(1)
                        .first()
                        .returns(ResponseOneOfType.STATUS, responseTypeExtractor)
                        .returns(Code.INVALID_START_BLOCK_NUMBER, responseStatusExtractor);
                // Assert that the stream was completed (closed)
                assertThat(responsePipeline.getOnCompleteCalls()).hasValue(1);
                // Assert that no responses were sent to the response pipeline
                assertThat(responsePipeline.getClientEndStreamCalls()).hasValue(0);
                assertThat(responsePipeline.getOnSubscriptionCalls()).isEmpty();
                assertThat(responsePipeline.getOnErrorCalls()).isEmpty();
            }

            /**
             * This test aims to assert that the
             * {@link BlockStreamSubscriberSession#validateRequest} method
             * returns {@code false} if the request is invalid due to end
             * being lower than
             * {@value org.hiero.block.node.spi.BlockNodePlugin#UNKNOWN_BLOCK_NUMBER}.
             * It is also expected that a response with status code
             * {@link Code#INVALID_END_BLOCK_NUMBER} is sent to the
             * subscriber. The session and the connection with the subscriber
             * is expected to be closed.
             */
            @Test
            @DisplayName("Test Validate Request - Invalid Request: end < -1L")
            void testInvalidRequestEndLessThanNegativeOne() {
                // First we create an invalid request with end < -1L
                final SubscribeStreamRequest request = SubscribeStreamRequest.newBuilder()
                        .startBlockNumber(-1L)
                        .endBlockNumber(-2L)
                        .build();
                // Then, we create a valid session to test
                final BlockStreamSubscriberSession toTest = generateSession(request);
                // Call
                final boolean actual = toTest.validateRequest();
                // Assert that the request is invalid
                assertThat(actual).isFalse();
                // Assert response sent to the response pipeline
                assertThat(responsePipeline.getOnNextCalls())
                        .isNotEmpty()
                        .hasSize(1)
                        .first()
                        .returns(ResponseOneOfType.STATUS, responseTypeExtractor)
                        .returns(Code.INVALID_END_BLOCK_NUMBER, responseStatusExtractor);
                // Assert that the stream was completed (closed)
                assertThat(responsePipeline.getOnCompleteCalls()).hasValue(1);
                // Assert that no responses were sent to the response pipeline
                assertThat(responsePipeline.getClientEndStreamCalls()).hasValue(0);
                assertThat(responsePipeline.getOnSubscriptionCalls()).isEmpty();
                assertThat(responsePipeline.getOnErrorCalls()).isEmpty();
            }

            /**
             * This test aims to assert that the
             * {@link BlockStreamSubscriberSession#validateRequest} method
             * returns {@code false} if the request is invalid due to end being
             * lower than start, when both are whole numbers. It is also
             * expected that a response with status code
             * {@link Code#INVALID_END_BLOCK_NUMBER} is sent to the
             * subscriber. The session and the connection with the subscriber
             * is expected to be closed.
             */
            @Test
            @DisplayName("Test Validate Request - Invalid Request: end >= 0 && end < start")
            void testInvalidRequestEndLessThanStartBothWholeNumbers() {
                // First we create an invalid request with end < start, when both are whole numbers
                final SubscribeStreamRequest request = SubscribeStreamRequest.newBuilder()
                        .startBlockNumber(10L)
                        .endBlockNumber(5L)
                        .build();
                // Then, we create a valid session to test
                final BlockStreamSubscriberSession toTest = generateSession(request);
                // Call
                final boolean actual = toTest.validateRequest();
                // Assert that the request is invalid
                assertThat(actual).isFalse();
                // Assert response sent to the response pipeline
                assertThat(responsePipeline.getOnNextCalls())
                        .isNotEmpty()
                        .hasSize(1)
                        .first()
                        .returns(ResponseOneOfType.STATUS, responseTypeExtractor)
                        .returns(Code.INVALID_END_BLOCK_NUMBER, responseStatusExtractor);
                // Assert that the stream was completed (closed)
                assertThat(responsePipeline.getOnCompleteCalls()).hasValue(1);
                // Assert that no responses were sent to the response pipeline
                assertThat(responsePipeline.getClientEndStreamCalls()).hasValue(0);
                assertThat(responsePipeline.getOnSubscriptionCalls()).isEmpty();
                assertThat(responsePipeline.getOnErrorCalls()).isEmpty();
            }
        }
    }

    /**
     * Request fulfillment tests for {@link BlockStreamSubscriberSession}.
     */
    @Nested
    @DisplayName("Request Fulfillment Tests")
    class RequestFulfillmentTests {
        /**
         * Positive tests for request fulfillment.
         */
        @Nested
        @DisplayName("Positive Fulfillment Tests")
        class PositiveFulfillmentTests {
            /**
             * This test aims to assert that the
             * {@link BlockStreamSubscriberSession#canFulfillRequest} method
             * returns {@code true} if the request is valid and is for a single
             * block, i.e. when both start and end are the same whole number.
             * The block must be available. No responses are expected to be
             * sent to the response pipeline. The next block to send is
             * expected to be properly resolved.
             */
            @Test
            @DisplayName("Test Can Fulfill Request - Positive: start >= 0 && start == end, block available")
            void testSingleBlockAvailable() {
                // First we create the request
                final long targetBlockNumber = 0L;
                final SubscribeStreamRequest request = SubscribeStreamRequest.newBuilder()
                        .startBlockNumber(targetBlockNumber)
                        .endBlockNumber(targetBlockNumber)
                        .build();
                // Then, we add the block to the historical block facility
                final SimpleBlockRangeSet temporaryAvailableBlocks = new SimpleBlockRangeSet();
                temporaryAvailableBlocks.add(targetBlockNumber);
                historicalBlockFacility.setTemporaryAvailableBlocks(temporaryAvailableBlocks);
                // Assert that the historical block facility has the block stored before call
                assertThat(historicalBlockFacility.availableBlocks())
                        .returns(1L, BlockRangeSet::size)
                        .returns(true, set -> set.contains(targetBlockNumber));
                // Then, we create a valid session to test
                final BlockStreamSubscriberSession toTest = generateSession(request);
                // Call
                final boolean actual = toTest.canFulfillRequest();
                // Assert that the request can be fulfilled
                assertThat(actual).isTrue();
                // Assert that no responses were sent to the response pipeline
                assertThat(responsePipeline.getClientEndStreamCalls()).hasValue(0);
                assertThat(responsePipeline.getOnCompleteCalls()).hasValue(0);
                assertThat(responsePipeline.getOnNextCalls()).isEmpty();
                assertThat(responsePipeline.getOnSubscriptionCalls()).isEmpty();
                assertThat(responsePipeline.getOnErrorCalls()).isEmpty();
                // Assert that the next block to send is properly resolved
                assertThat(toTest.getNextBlockToSend()).isEqualTo(targetBlockNumber);
            }

            /**
             * This test aims to assert that the
             * {@link BlockStreamSubscriberSession#canFulfillRequest} method
             * returns {@code true} if the request is valid and is for a
             * closed range of blocks, i.e. when both start and end are whole
             * numbers and end is greater than start and the first available
             * block must be lower than or equal to the first requested block.
             * This test also covers the case for requesting future blocks,
             * meaning that the latest know block is lower than the first
             * requested block and the last permitted block (future start
             * config) is greater than or equal to the first requested block.
             * No responses are expected to be sent to the response pipeline.
             * The next block to send is expected to be properly resolved.
             */
            @ParameterizedTest
            @ValueSource(longs = {0L, 1L, 2L, 3L, 4L, 5L})
            @DisplayName(
                    "Test Can Fulfill Request - Positive: start >= 0 && end > start, first available block < start")
            void testClosedRange(final long firstAvailableBlock) {
                // First we create the request
                final long startBlockNumber = 5L;
                final long endBlockNumber = 10L;
                final SubscribeStreamRequest request = SubscribeStreamRequest.newBuilder()
                        .startBlockNumber(startBlockNumber)
                        .endBlockNumber(endBlockNumber)
                        .build();
                // Then, we add some blocks to the historical block facility
                final SimpleBlockRangeSet temporaryAvailableBlocks = new SimpleBlockRangeSet();
                temporaryAvailableBlocks.add(firstAvailableBlock, endBlockNumber);
                historicalBlockFacility.setTemporaryAvailableBlocks(temporaryAvailableBlocks);
                // Assert that the historical block facility has the blocks stored before call
                assertThat(historicalBlockFacility.availableBlocks())
                        .returns((endBlockNumber - firstAvailableBlock) + 1, BlockRangeSet::size)
                        .returns(true, set -> set.contains(firstAvailableBlock, endBlockNumber));
                // Then, we create a valid session to test
                final BlockStreamSubscriberSession toTest = generateSession(request);
                // Call
                final boolean actual = toTest.canFulfillRequest();
                // Assert that the request can be fulfilled
                assertThat(actual).isTrue();
                // Assert that no responses were sent to the response pipeline
                assertThat(responsePipeline.getClientEndStreamCalls()).hasValue(0);
                assertThat(responsePipeline.getOnCompleteCalls()).hasValue(0);
                assertThat(responsePipeline.getOnNextCalls()).isEmpty();
                assertThat(responsePipeline.getOnSubscriptionCalls()).isEmpty();
                assertThat(responsePipeline.getOnErrorCalls()).isEmpty();
                // Assert that the next block to send is properly resolved
                assertThat(toTest.getNextBlockToSend()).isEqualTo(startBlockNumber);
            }

            /**
             * This test aims to assert that the
             * {@link BlockStreamSubscriberSession#canFulfillRequest} method
             * returns {@code true} if the request is valid and is for the live
             * stream, i.e. when both start and end are
             * {@value org.hiero.block.node.spi.BlockNodePlugin#UNKNOWN_BLOCK_NUMBER}.
             * No blocks need to be available, as we will start with the next
             * live block received.
             * The next block to send is expected to also be
             * {@value org.hiero.block.node.spi.BlockNodePlugin#UNKNOWN_BLOCK_NUMBER}.
             * No responses are expected to be sent to the response pipeline.
             */
            @Test
            @DisplayName("Test Can Fulfill Request - Positive: start == -1L && end == -1L")
            void testLiveStreamRequest() {
                // First we create the request
                final SubscribeStreamRequest request = SubscribeStreamRequest.newBuilder()
                        .startBlockNumber(-1L)
                        .endBlockNumber(-1L)
                        .build();
                // Assert that the historical block facility has no blocks stored before call
                // (not needed for live stream requests)
                assertThat(historicalBlockFacility.availableBlocks().size()).isZero();
                // Then, we create a valid session to test
                final BlockStreamSubscriberSession toTest = generateSession(request);
                // Call
                final boolean actual = toTest.canFulfillRequest();
                // Assert that the request can be fulfilled
                assertThat(actual).isTrue();
                // Assert that no responses were sent to the response pipeline
                assertThat(responsePipeline.getClientEndStreamCalls()).hasValue(0);
                assertThat(responsePipeline.getOnCompleteCalls()).hasValue(0);
                assertThat(responsePipeline.getOnNextCalls()).isEmpty();
                assertThat(responsePipeline.getOnSubscriptionCalls()).isEmpty();
                assertThat(responsePipeline.getOnErrorCalls()).isEmpty();
                // Assert that the next block to send is properly resolved
                assertThat(toTest.getNextBlockToSend()).isEqualTo(BlockNodePlugin.UNKNOWN_BLOCK_NUMBER);
            }

            /**
             * This test aims to assert that the
             * {@link BlockStreamSubscriberSession#canFulfillRequest} method
             * returns {@code true} if the request is valid and is for blocks in
             * range, starting from the first available, up to a closed range
             * (whole number), i.e. when start is
             * {@value org.hiero.block.node.spi.BlockNodePlugin#UNKNOWN_BLOCK_NUMBER}
             * and end is a whole number. The first available block must be
             * lower than or equal to the last requested block, otherwise we
             * cannot serve at least one block.
             * The next block to send is expected to be properly resolved.
             * No responses are expected to be sent to the response pipeline.
             */
            @ParameterizedTest
            @ValueSource(longs = {0L, 5L, 10L})
            @DisplayName("Test Can Fulfill Request - Positive: start == -1L && end >= 0L")
            void testEarliestAvailableClosedRange(final long firstAvailableBlock) {
                // First we create the request
                final SubscribeStreamRequest request = SubscribeStreamRequest.newBuilder()
                        .startBlockNumber(-1L)
                        .endBlockNumber(10L)
                        .build();
                // Then, we add some blocks to the historical block facility
                final SimpleBlockRangeSet temporaryAvailableBlocks = new SimpleBlockRangeSet();
                temporaryAvailableBlocks.add(firstAvailableBlock);
                historicalBlockFacility.setTemporaryAvailableBlocks(temporaryAvailableBlocks);
                // Assert that the historical block facility has the blocks stored before call
                assertThat(historicalBlockFacility.availableBlocks())
                        .returns(1L, BlockRangeSet::size)
                        .returns(true, set -> set.contains(firstAvailableBlock));
                // Then, we create a valid session to test
                final BlockStreamSubscriberSession toTest = generateSession(request);
                // Call
                final boolean actual = toTest.canFulfillRequest();
                // Assert that the request can be fulfilled
                assertThat(actual).isTrue();
                // Assert that no responses were sent to the response pipeline
                assertThat(responsePipeline.getClientEndStreamCalls()).hasValue(0);
                assertThat(responsePipeline.getOnCompleteCalls()).hasValue(0);
                assertThat(responsePipeline.getOnNextCalls()).isEmpty();
                assertThat(responsePipeline.getOnSubscriptionCalls()).isEmpty();
                assertThat(responsePipeline.getOnErrorCalls()).isEmpty();
                // Assert that the next block to send is properly resolved
                assertThat(toTest.getNextBlockToSend()).isEqualTo(firstAvailableBlock);
            }

            /**
             * This test aims to assert that the
             * {@link BlockStreamSubscriberSession#canFulfillRequest} method
             * returns {@code true} if the request is valid and is for blocks in
             * range, starting from a valid block number and is open ranged
             * (stream indefinitely). The first available block must be lower
             * than or equal to the requested start block, otherwise we cannot
             * serve at least one block.
             * The next block to send is expected to be properly resolved.
             * No responses are expected to be sent to the response pipeline.
             */
            @ParameterizedTest
            @ValueSource(longs = {0L, 1L, 2L, 3L, 4L, 5L})
            @DisplayName("Test Can Fulfill Request - Positive: start >=  0L && end == -1L")
            void testFromSpecificBlockOpenRange(final long firstAvailableBlock) {
                // First we create the request
                final long startBlockNumber = 5L;
                final long endBlockNumber = -1L;
                final SubscribeStreamRequest request = SubscribeStreamRequest.newBuilder()
                        .startBlockNumber(startBlockNumber)
                        .endBlockNumber(endBlockNumber)
                        .build();
                // Then, we add some blocks to the historical block facility
                final SimpleBlockRangeSet temporaryAvailableBlocks = new SimpleBlockRangeSet();
                temporaryAvailableBlocks.add(firstAvailableBlock);
                historicalBlockFacility.setTemporaryAvailableBlocks(temporaryAvailableBlocks);
                // Assert that the historical block facility has the blocks stored before call
                assertThat(historicalBlockFacility.availableBlocks())
                        .returns(1L, BlockRangeSet::size)
                        .returns(true, set -> set.contains(firstAvailableBlock));
                // Then, we create a valid session to test
                final BlockStreamSubscriberSession toTest = generateSession(request);
                // Call
                final boolean actual = toTest.canFulfillRequest();
                // Assert that the request can be fulfilled
                assertThat(actual).isTrue();
                // Assert that no responses were sent to the response pipeline
                assertThat(responsePipeline.getClientEndStreamCalls()).hasValue(0);
                assertThat(responsePipeline.getOnCompleteCalls()).hasValue(0);
                assertThat(responsePipeline.getOnNextCalls()).isEmpty();
                assertThat(responsePipeline.getOnSubscriptionCalls()).isEmpty();
                assertThat(responsePipeline.getOnErrorCalls()).isEmpty();
                // Assert that the next block to send is properly resolved
                assertThat(toTest.getNextBlockToSend()).isEqualTo(startBlockNumber);
            }
        }

        /**
         * Negative tests for request fulfillment.
         */
        @Nested
        @DisplayName("Negative Fulfillment Tests")
        class NegativeFulfillmentTests {
            /**
             * This test aims to assert that the
             * {@link BlockStreamSubscriberSession#canFulfillRequest} method
             * returns {@code false} if the request is valid and is for a single
             * block, i.e. when both start and end are the same whole number,
             * but the block is not available. It is also expected that a
             * response with status code {@link Code#NOT_AVAILABLE} is sent to
             * the subscriber. The session and the connection with the
             * subscriber is expected to be closed.
             */
            @Test
            @DisplayName("Test Can Fulfill Request - Negative: start >= 0 && start == end, block not available")
            void testSingleBlockNotAvailable() {
                // First we create the request
                final SubscribeStreamRequest request = SubscribeStreamRequest.newBuilder()
                        .startBlockNumber(0L)
                        .endBlockNumber(0L)
                        .build();
                // Assert that the historical block facility has no blocks stored before call
                assertThat(historicalBlockFacility.availableBlocks().size()).isZero();
                // Then, we create a valid session to test
                final BlockStreamSubscriberSession toTest = generateSession(request);
                // Call
                final boolean actual = toTest.canFulfillRequest();
                // Assert that the request is invalid
                assertThat(actual).isFalse();
                // Assert response sent to the response pipeline
                assertThat(responsePipeline.getOnNextCalls())
                        .isNotEmpty()
                        .hasSize(1)
                        .first()
                        .returns(ResponseOneOfType.STATUS, responseTypeExtractor)
                        .returns(Code.NOT_AVAILABLE, responseStatusExtractor);
                // Assert that the stream was completed (closed)
                assertThat(responsePipeline.getOnCompleteCalls()).hasValue(1);
                // Assert that no responses were sent to the response pipeline
                assertThat(responsePipeline.getClientEndStreamCalls()).hasValue(0);
                assertThat(responsePipeline.getOnSubscriptionCalls()).isEmpty();
                assertThat(responsePipeline.getOnErrorCalls()).isEmpty();
            }

            /**
             * This test aims to assert that the
             * {@link BlockStreamSubscriberSession#canFulfillRequest} method
             * returns {@code false} if the request is valid and is for a
             * closed range of blocks, i.e. when both start and end are whole
             * numbers and end is greater than start, but no blocks are
             * available. It is also expected that a response with status code
             * {@link Code#NOT_AVAILABLE} is sent to the subscriber. The
             * session and the connection with the subscriber is expected to be
             * closed.
             */
            @Test
            @DisplayName("Test Can Fulfill Request - Negative: start >= 0 && end > start, blocks not available")
            void testClosedRangeNoBlocksAvailable() {
                // First we create the request
                final SubscribeStreamRequest request = SubscribeStreamRequest.newBuilder()
                        .startBlockNumber(0L)
                        .endBlockNumber(10L)
                        .build();
                // Assert that the historical block facility has no blocks stored before call
                assertThat(historicalBlockFacility.availableBlocks().size()).isZero();
                // Then, we create a valid session to test
                final BlockStreamSubscriberSession toTest = generateSession(request);
                // Call
                final boolean actual = toTest.canFulfillRequest();
                // Assert that the request is invalid
                assertThat(actual).isFalse();
                // Assert response sent to the response pipeline
                assertThat(responsePipeline.getOnNextCalls())
                        .isNotEmpty()
                        .hasSize(1)
                        .first()
                        .returns(ResponseOneOfType.STATUS, responseTypeExtractor)
                        .returns(Code.NOT_AVAILABLE, responseStatusExtractor);
                // Assert that the stream was completed (closed)
                assertThat(responsePipeline.getOnCompleteCalls()).hasValue(1);
                // Assert that no responses were sent to the response pipeline
                assertThat(responsePipeline.getClientEndStreamCalls()).hasValue(0);
                assertThat(responsePipeline.getOnSubscriptionCalls()).isEmpty();
                assertThat(responsePipeline.getOnErrorCalls()).isEmpty();
            }

            /**
             * This test aims to assert that the
             * {@link BlockStreamSubscriberSession#canFulfillRequest} method
             * returns {@code false} if the request is valid and is for a
             * closed range of blocks, i.e. when both start and end are whole
             * numbers and end is greater than start, but the first available
             * block is greater than the start of the requested range.
             * It is also expected that a response with status code
             * {@link Code#NOT_AVAILABLE} is sent to the subscriber. The
             * session and the connection with the subscriber is expected to be
             * closed.
             */
            @Test
            @DisplayName(
                    "Test Can Fulfill Request - Negative: start >= 0 && end > start, first available block > start")
            void testClosedRangeFirstAvailableBlockGreaterThanStart() {
                // First we create the request
                final SubscribeStreamRequest request = SubscribeStreamRequest.newBuilder()
                        .startBlockNumber(0L)
                        .endBlockNumber(10L)
                        .build();
                // Then, we add some blocks to the historical block facility, starting from block number 5
                final SimpleBlockRangeSet temporaryAvailableBlocks = new SimpleBlockRangeSet();
                temporaryAvailableBlocks.add(5L, 10L);
                historicalBlockFacility.setTemporaryAvailableBlocks(temporaryAvailableBlocks);
                // Assert that the historical block facility has blocks stored before call
                assertThat(historicalBlockFacility.availableBlocks())
                        .returns(6L, BlockRangeSet::size)
                        .returns(true, set -> set.contains(5L, 10L));
                // Then, we create a valid session to test
                final BlockStreamSubscriberSession toTest = generateSession(request);
                // Call
                final boolean actual = toTest.canFulfillRequest();
                // Assert that the request is invalid
                assertThat(actual).isFalse();
                // Assert response sent to the response pipeline
                assertThat(responsePipeline.getOnNextCalls())
                        .isNotEmpty()
                        .hasSize(1)
                        .first()
                        .returns(ResponseOneOfType.STATUS, responseTypeExtractor)
                        .returns(Code.NOT_AVAILABLE, responseStatusExtractor);
                // Assert that the stream was completed (closed)
                assertThat(responsePipeline.getOnCompleteCalls()).hasValue(1);
                // Assert that no responses were sent to the response pipeline
                assertThat(responsePipeline.getClientEndStreamCalls()).hasValue(0);
                assertThat(responsePipeline.getOnSubscriptionCalls()).isEmpty();
                assertThat(responsePipeline.getOnErrorCalls()).isEmpty();
            }

            /**
             * This test aims to assert that the
             * {@link BlockStreamSubscriberSession#canFulfillRequest} method
             * returns {@code false} if the request is valid and is for a
             * start of first available
             * (i.e. {@value org.hiero.block.node.spi.BlockNodePlugin#UNKNOWN_BLOCK_NUMBER})
             * and up to a closed range of blocks, i.e. when end is a whole
             * number, but no blocks are available. It is also expected that a
             * response with status code {@link Code#NOT_AVAILABLE} is sent to
             * the subscriber. The session and the connection with the
             * subscriber is expected to be closed.
             */
            @Test
            @DisplayName("Test Can Fulfill Request - Negative: start == -1L && end >= 0, blocks not available")
            void testEarliestAvailableClosedRangeBlocksNotAvailable() {
                // First we create the request
                final SubscribeStreamRequest request = SubscribeStreamRequest.newBuilder()
                        .startBlockNumber(-1L)
                        .endBlockNumber(10L)
                        .build();
                // Assert that the historical block facility has no blocks stored before call
                assertThat(historicalBlockFacility.availableBlocks().size()).isZero();
                // Then, we create a valid session to test
                final BlockStreamSubscriberSession toTest = generateSession(request);
                // Call
                final boolean actual = toTest.canFulfillRequest();
                // Assert that the request is invalid
                assertThat(actual).isFalse();
                // Assert response sent to the response pipeline
                assertThat(responsePipeline.getOnNextCalls())
                        .isNotEmpty()
                        .hasSize(1)
                        .first()
                        .returns(ResponseOneOfType.STATUS, responseTypeExtractor)
                        .returns(Code.NOT_AVAILABLE, responseStatusExtractor);
                // Assert that the stream was completed (closed)
                assertThat(responsePipeline.getOnCompleteCalls()).hasValue(1);
                // Assert that no responses were sent to the response pipeline
                assertThat(responsePipeline.getClientEndStreamCalls()).hasValue(0);
                assertThat(responsePipeline.getOnSubscriptionCalls()).isEmpty();
                assertThat(responsePipeline.getOnErrorCalls()).isEmpty();
            }

            /**
             * This test aims to assert that the
             * {@link BlockStreamSubscriberSession#canFulfillRequest} method
             * returns {@code false} if the request is valid and is for a start
             * of first available
             * (i.e. {@value org.hiero.block.node.spi.BlockNodePlugin#UNKNOWN_BLOCK_NUMBER})
             * and up to a closed range of blocks, i.e. when end is a whole
             * number, but the first available block is greater than the end of
             * the requested range. It is also expected that a response with
             * status code {@link Code#NOT_AVAILABLE} is sent to the
             * subscriber. The session and the connection with the subscriber
             * is expected to be closed.
             */
            @Test
            @DisplayName("Test Can Fulfill Request - Negative: start == -1L && end >= 0, first available block > end")
            void testEarliestAvailableClosedRangeFirstAvailableBlockGreaterThanEnd() {
                // First we create the request
                final SubscribeStreamRequest request = SubscribeStreamRequest.newBuilder()
                        .startBlockNumber(-1L)
                        .endBlockNumber(10L)
                        .build();
                // Then, we add some blocks to the historical block facility, starting from block number 15
                final SimpleBlockRangeSet temporaryAvailableBlocks = new SimpleBlockRangeSet();
                temporaryAvailableBlocks.add(11L, 20L);
                historicalBlockFacility.setTemporaryAvailableBlocks(temporaryAvailableBlocks);
                // Assert that the historical block facility has blocks stored before call
                assertThat(historicalBlockFacility.availableBlocks())
                        .returns(10L, BlockRangeSet::size)
                        .returns(true, set -> set.contains(11L, 20L));
                // Then, we create a valid session to test
                final BlockStreamSubscriberSession toTest = generateSession(request);
                // Call
                final boolean actual = toTest.canFulfillRequest();
                // Assert that the request is invalid
                assertThat(actual).isFalse();
                // Assert response sent to the response pipeline
                assertThat(responsePipeline.getOnNextCalls())
                        .isNotEmpty()
                        .hasSize(1)
                        .first()
                        .returns(ResponseOneOfType.STATUS, responseTypeExtractor)
                        .returns(Code.NOT_AVAILABLE, responseStatusExtractor);
                // Assert that the stream was completed (closed)
                assertThat(responsePipeline.getOnCompleteCalls()).hasValue(1);
                // Assert that no responses were sent to the response pipeline
                assertThat(responsePipeline.getClientEndStreamCalls()).hasValue(0);
                assertThat(responsePipeline.getOnSubscriptionCalls()).isEmpty();
                assertThat(responsePipeline.getOnErrorCalls()).isEmpty();
            }

            /**
             * This test aims to assert that the
             * {@link BlockStreamSubscriberSession#canFulfillRequest} method
             * returns {@code false} if the request is valid and is for a
             * specific start, i.e. when start is a whole number, and an open
             * range (i.e. {@value org.hiero.block.node.spi.BlockNodePlugin#UNKNOWN_BLOCK_NUMBER}),
             * but no blocks are available. It is also expected that a response
             * with status code {@link Code#NOT_AVAILABLE} is sent to the
             * subscriber. The session and the connection with the subscriber
             * is expected to be closed.
             */
            @Test
            @DisplayName("Test Can Fulfill Request - Negative: start >= 0 && end == -1L, blocks not available")
            void testSpecificBlockOpenRangeBlocksNotAvailable() {
                // First we create the request
                final SubscribeStreamRequest request = SubscribeStreamRequest.newBuilder()
                        .startBlockNumber(0L)
                        .endBlockNumber(-1L)
                        .build();
                // Assert that the historical block facility has no blocks stored before call
                assertThat(historicalBlockFacility.availableBlocks().size()).isZero();
                // Then, we create a valid session to test
                final BlockStreamSubscriberSession toTest = generateSession(request);
                // Call
                final boolean actual = toTest.canFulfillRequest();
                // Assert that the request is invalid
                assertThat(actual).isFalse();
                // Assert response sent to the response pipeline
                assertThat(responsePipeline.getOnNextCalls())
                        .isNotEmpty()
                        .hasSize(1)
                        .first()
                        .returns(ResponseOneOfType.STATUS, responseTypeExtractor)
                        .returns(Code.NOT_AVAILABLE, responseStatusExtractor);
                // Assert that the stream was completed (closed)
                assertThat(responsePipeline.getOnCompleteCalls()).hasValue(1);
                // Assert that no responses were sent to the response pipeline
                assertThat(responsePipeline.getClientEndStreamCalls()).hasValue(0);
                assertThat(responsePipeline.getOnSubscriptionCalls()).isEmpty();
                assertThat(responsePipeline.getOnErrorCalls()).isEmpty();
            }

            /**
             * This test aims to assert that the
             * {@link BlockStreamSubscriberSession#canFulfillRequest} method
             * returns {@code false} if the request is valid and is for a
             * specific start, i.e. when start is a whole number, and an open
             * range (i.e. {@value org.hiero.block.node.spi.BlockNodePlugin#UNKNOWN_BLOCK_NUMBER}),
             * but the first available block is greater than the start of the
             * requested range. It is also expected that a response with status
             * code {@link Code#NOT_AVAILABLE} is sent to the subscriber. The
             * session and the connection with the subscriber is expected to be
             * closed.
             */
            @Test
            @DisplayName("Test Can Fulfill Request - Negative: start >= 0 && end == -1L, first available block > start")
            void testSpecificBlockOpenRangeFirstAvailableBlockGreaterThanStart() {
                // First we create the request
                final SubscribeStreamRequest request = SubscribeStreamRequest.newBuilder()
                        .startBlockNumber(0L)
                        .endBlockNumber(-1L)
                        .build();
                // Then, we add some blocks to the historical block facility, starting from block number 5
                final SimpleBlockRangeSet temporaryAvailableBlocks = new SimpleBlockRangeSet();
                temporaryAvailableBlocks.add(5L, 10L);
                historicalBlockFacility.setTemporaryAvailableBlocks(temporaryAvailableBlocks);
                // Assert that the historical block facility has blocks stored before call
                assertThat(historicalBlockFacility.availableBlocks())
                        .returns(6L, BlockRangeSet::size)
                        .returns(true, set -> set.contains(5L, 10L));
                // Then, we create a valid session to test
                final BlockStreamSubscriberSession toTest = generateSession(request);
                // Call
                final boolean actual = toTest.canFulfillRequest();
                // Assert that the request is invalid
                assertThat(actual).isFalse();
                // Assert response sent to the response pipeline
                assertThat(responsePipeline.getOnNextCalls())
                        .isNotEmpty()
                        .hasSize(1)
                        .first()
                        .returns(ResponseOneOfType.STATUS, responseTypeExtractor)
                        .returns(Code.NOT_AVAILABLE, responseStatusExtractor);
                // Assert that the stream was completed (closed)
                assertThat(responsePipeline.getOnCompleteCalls()).hasValue(1);
                // Assert that no responses were sent to the response pipeline
                assertThat(responsePipeline.getClientEndStreamCalls()).hasValue(0);
                assertThat(responsePipeline.getOnSubscriptionCalls()).isEmpty();
                assertThat(responsePipeline.getOnErrorCalls()).isEmpty();
            }

            /**
             * This test aims to assert that the
             * {@link BlockStreamSubscriberSession#canFulfillRequest} method
             * returns {@code false} if the request is valid and is for a
             * specific start, but the start is greater than the
             * lastPermittedStart, which is calculated as:
             * {@link SubscriberConfig#maximumFutureRequest()} + latestKnownBlock
             * be that historical or live, and there are no blocks available.
             * It is also expected that a response with status code
             * {@link Code#NOT_AVAILABLE} is sent to the subscriber. The session
             * and the connection with the subscriber is expected to be closed.
             */
            @Test
            @DisplayName("Test Can Fulfill Request - Negative: start > lastPermittedStart, blocks not available")
            void testStartGreaterThanLastPermittedStartNoBlocks() {
                // First we create the request
                final long start = defaultSubscriberConfig.maximumFutureRequest() + 1L;
                final SubscribeStreamRequest request = SubscribeStreamRequest.newBuilder()
                        .startBlockNumber(start)
                        .endBlockNumber(start + 1L)
                        .build();
                // Assert that the historical block facility has no blocks stored before call
                assertThat(historicalBlockFacility.availableBlocks().size()).isZero();
                // Then, we create a valid session to test
                final BlockStreamSubscriberSession toTest = generateSession(request);
                // Call
                final boolean actual = toTest.canFulfillRequest();
                // Assert that the request is invalid
                assertThat(actual).isFalse();
                // Assert response sent to the response pipeline
                assertThat(responsePipeline.getOnNextCalls())
                        .isNotEmpty()
                        .hasSize(1)
                        .first()
                        .returns(ResponseOneOfType.STATUS, responseTypeExtractor)
                        .returns(Code.NOT_AVAILABLE, responseStatusExtractor);
                // Assert that the stream was completed (closed)
                assertThat(responsePipeline.getOnCompleteCalls()).hasValue(1);
                // Assert that no responses were sent to the response pipeline
                assertThat(responsePipeline.getClientEndStreamCalls()).hasValue(0);
                assertThat(responsePipeline.getOnSubscriptionCalls()).isEmpty();
                assertThat(responsePipeline.getOnErrorCalls()).isEmpty();
            }

            /**
             * This test aims to assert that the
             * {@link BlockStreamSubscriberSession#canFulfillRequest} method
             * returns {@code false} if the request is valid and is for a
             * specific start, but the start is greater than the
             * lastPermittedStart, which is calculated as:
             * {@link SubscriberConfig#maximumFutureRequest()} + latestKnownBlock
             * be that historical or live, and the first available block is
             * in the past.
             * It is also expected that a response with status code
             * {@link Code#NOT_AVAILABLE} is sent to the subscriber. The session
             * and the connection with the subscriber is expected to be closed.
             */
            @Test
            @DisplayName(
                    "Test Can Fulfill Request - Negative: start > lastPermittedStart, first available block in the past")
            void testStartGreaterThanLastPermittedStartFirstAvailableBlockInThePast() {
                // First we create the request
                final long start = defaultSubscriberConfig.maximumFutureRequest() + 1L;
                final SubscribeStreamRequest request = SubscribeStreamRequest.newBuilder()
                        .startBlockNumber(start)
                        .endBlockNumber(start + 1L)
                        .build();
                // Then, we add some blocks to the historical block facility, starting from block number 5
                final SimpleBlockRangeSet temporaryAvailableBlocks = new SimpleBlockRangeSet();
                temporaryAvailableBlocks.add(0L);
                historicalBlockFacility.setTemporaryAvailableBlocks(temporaryAvailableBlocks);
                // Assert that the historical block facility has blocks stored before call
                assertThat(historicalBlockFacility.availableBlocks())
                        .returns(1L, BlockRangeSet::size)
                        .returns(true, set -> set.contains(0L));
                // Then, we create a valid session to test
                final BlockStreamSubscriberSession toTest = generateSession(request);
                // Call
                final boolean actual = toTest.canFulfillRequest();
                // Assert that the request is invalid
                assertThat(actual).isFalse();
                // Assert response sent to the response pipeline
                assertThat(responsePipeline.getOnNextCalls())
                        .isNotEmpty()
                        .hasSize(1)
                        .first()
                        .returns(ResponseOneOfType.STATUS, responseTypeExtractor)
                        .returns(Code.NOT_AVAILABLE, responseStatusExtractor);
                // Assert that the stream was completed (closed)
                assertThat(responsePipeline.getOnCompleteCalls()).hasValue(1);
                // Assert that no responses were sent to the response pipeline
                assertThat(responsePipeline.getClientEndStreamCalls()).hasValue(0);
                assertThat(responsePipeline.getOnSubscriptionCalls()).isEmpty();
                assertThat(responsePipeline.getOnErrorCalls()).isEmpty();
            }

            /**
             * This test aims to assert that the
             * {@link BlockStreamSubscriberSession#canFulfillRequest} method
             * returns {@code false} if the request is invalid. This is an
             * edge case that should not happen in practice because the method
             * should only be called after the request has been validated to be
             * correct, but nevertheless we must guard against it.
             * It is also expected that a response with status code
             * {@link Code#NOT_AVAILABLE} is sent to the subscriber. The session
             * and the connection with the subscriber is expected to be closed.
             */
            @ParameterizedTest
            @MethodSource("invalidRequests")
            @DisplayName("Test Can Fulfill Request - Negative: Invalid Request")
            void testCanFulfillRequestInvalidRequest(final SubscribeStreamRequest request) {
                // We add some blocks to the historical block facility, starting from block number 5
                final SimpleBlockRangeSet temporaryAvailableBlocks = new SimpleBlockRangeSet();
                temporaryAvailableBlocks.add(0L, 20L);
                historicalBlockFacility.setTemporaryAvailableBlocks(temporaryAvailableBlocks);
                // Assert that the historical block facility has blocks stored before call
                assertThat(historicalBlockFacility.availableBlocks())
                        .returns(21L, BlockRangeSet::size)
                        .returns(true, set -> set.contains(0L, 20L));
                // We create a valid session to test
                final BlockStreamSubscriberSession toTest = generateSession(request);
                // Call
                final boolean actual = toTest.canFulfillRequest();
                // Assert that the request is invalid
                assertThat(actual).isFalse();
                // Assert response sent to the response pipeline
                assertThat(responsePipeline.getOnNextCalls())
                        .isNotEmpty()
                        .hasSize(1)
                        .first()
                        .returns(ResponseOneOfType.STATUS, responseTypeExtractor)
                        .returns(Code.NOT_AVAILABLE, responseStatusExtractor);
                // Assert that the stream was completed (closed)
                assertThat(responsePipeline.getOnCompleteCalls()).hasValue(1);
                // Assert that no responses were sent to the response pipeline
                assertThat(responsePipeline.getClientEndStreamCalls()).hasValue(0);
                assertThat(responsePipeline.getOnSubscriptionCalls()).isEmpty();
                assertThat(responsePipeline.getOnErrorCalls()).isEmpty();
            }

            /**
             * All types of invalid requests. Used in parameterized tests.
             */
            private static Stream<SubscribeStreamRequest> invalidRequests() {
                return Stream.of(
                        // start < -1L
                        SubscribeStreamRequest.newBuilder()
                                .startBlockNumber(-2L)
                                .endBlockNumber(-1L)
                                .build(),
                        // end < -1L
                        SubscribeStreamRequest.newBuilder()
                                .startBlockNumber(-1L)
                                .endBlockNumber(-2L)
                                .build(),
                        // end >= 0 && end < start, both whole numbers
                        SubscribeStreamRequest.newBuilder()
                                .startBlockNumber(10L)
                                .endBlockNumber(5L)
                                .build());
            }
        }
    }

    /**
     * Functionality tests for {@link BlockStreamSubscriberSession}.
     */
    @Nested
    @DisplayName("Functionality Tests")
    class FunctionalityTests {
        /**
         * This test aims to assert that the
         * {@link BlockStreamSubscriberSession#clientId()} correctly returns
         * the client id provided to the constructor.
         */
        @Test
        @DisplayName("Test Client ID Retrieval")
        void testClientIdRetrieval() {
            // build a valid request
            final SubscribeStreamRequest request = SubscribeStreamRequest.newBuilder()
                    .startBlockNumber(-1L)
                    .endBlockNumber(-1L)
                    .build();
            // create a session to test
            final BlockStreamSubscriberSession toTest = generateSession(request);
            // Assert that the client id is correctly retrieved
            assertThat(toTest.clientId()).isEqualTo(clientId);
        }
    }

    /**
     * Tests for block item chunking functionality.
     */
    @Nested
    @DisplayName("Block Item Chunking Tests")
    class BlockItemChunkingTests {
        /** Small chunk size to force chunking in tests (100KB - minimum allowed). */
        private static final int TEST_CHUNK_SIZE_BYTES = 100_000;

        /** Response pipeline for chunking tests. */
        private TestResponsePipeline<SubscribeStreamResponseUnparsed> chunkingPipeline;
        /** Block node context with custom chunk size config. */
        private BlockNodeContext chunkingContext;

        @BeforeEach
        void setupChunkingTests() {
            chunkingPipeline = new TestResponsePipeline<>();
            final Configuration chunkingConfig = ConfigurationBuilder.create()
                    .withConfigDataType(SubscriberConfig.class)
                    .withValue("subscriber.maxChunkSizeBytes", String.valueOf(TEST_CHUNK_SIZE_BYTES))
                    .build();
            final TestBlockMessagingFacility messagingFacility = new TestBlockMessagingFacility();
            chunkingContext = generateContext(chunkingConfig, messagingFacility, historicalBlockFacility);
        }

        /**
         * This test verifies that when block items fit within a single chunk,
         * only one BLOCK_ITEMS response is sent followed by END_OF_BLOCK.
         */
        @Test
        @DisplayName("should send small block in single chunk")
        void testSmallBlockSingleChunk() {
            // Create a small block that fits in one chunk (3 items, ~200 bytes total)
            // 100KB chunk size easily fits these small items
            final List<BlockItemUnparsed> smallBlock =
                    List.of(sampleHeaderUnparsed(0), sampleRoundHeaderUnparsed(0), sampleProofUnparsed(0));

            // Create session with chunking config
            final SubscribeStreamRequest request = SubscribeStreamRequest.newBuilder()
                    .startBlockNumber(0L)
                    .endBlockNumber(0L)
                    .build();
            final BlockStreamSubscriberSession session = new BlockStreamSubscriberSession(
                    SessionContext.create(clientId, request, chunkingContext),
                    chunkingPipeline,
                    chunkingContext,
                    sessionReadyLatch);

            // Call the chunking method directly
            session.sendBlockItemsChunked(smallBlock);

            // Verify: should have exactly 2 responses (1 BLOCK_ITEMS + 1 END_OF_BLOCK)
            assertThat(chunkingPipeline.getOnNextCalls()).hasSize(2);

            // First response should be BLOCK_ITEMS with all 3 items
            final SubscribeStreamResponseUnparsed firstResponse =
                    chunkingPipeline.getOnNextCalls().get(0);
            assertThat(firstResponse.response().kind()).isEqualTo(ResponseOneOfType.BLOCK_ITEMS);
            assertThat(firstResponse.blockItems().blockItems()).hasSize(3);

            // Second response should be END_OF_BLOCK
            final SubscribeStreamResponseUnparsed secondResponse =
                    chunkingPipeline.getOnNextCalls().get(1);
            assertThat(secondResponse.response().kind()).isEqualTo(ResponseOneOfType.END_OF_BLOCK);
        }

        /**
         * This test verifies that when block items exceed the chunk size,
         * they are split into multiple BLOCK_ITEMS responses with a single END_OF_BLOCK at the end.
         */
        @Test
        @DisplayName("should split large block into multiple chunks")
        void testLargeBlockMultipleChunks() {
            // Create a block with large items that will exceed the 100KB chunk size
            // Each item is ~50KB, so 3 items should require at least 2 chunks
            final List<BlockItemUnparsed> largeBlock = new java.util.ArrayList<>();
            largeBlock.add(sampleHeaderUnparsed(0));
            largeBlock.add(createLargeBlockItem(50_000)); // ~50KB
            largeBlock.add(createLargeBlockItem(50_000)); // ~50KB
            largeBlock.add(createLargeBlockItem(50_000)); // ~50KB
            largeBlock.add(sampleProofUnparsed(0));

            // Create session with chunking config
            final SubscribeStreamRequest request = SubscribeStreamRequest.newBuilder()
                    .startBlockNumber(0L)
                    .endBlockNumber(0L)
                    .build();
            final BlockStreamSubscriberSession session = new BlockStreamSubscriberSession(
                    SessionContext.create(clientId, request, chunkingContext),
                    chunkingPipeline,
                    chunkingContext,
                    sessionReadyLatch);

            // Call the chunking method directly
            session.sendBlockItemsChunked(largeBlock);

            // Verify: should have multiple responses
            final var responses = chunkingPipeline.getOnNextCalls();
            assertThat(responses.size()).isGreaterThan(2);

            // Count BLOCK_ITEMS and END_OF_BLOCK responses
            long blockItemsCount = responses.stream()
                    .filter(r -> r.response().kind() == ResponseOneOfType.BLOCK_ITEMS)
                    .count();
            long endOfBlockCount = responses.stream()
                    .filter(r -> r.response().kind() == ResponseOneOfType.END_OF_BLOCK)
                    .count();

            // Should have multiple BLOCK_ITEMS chunks
            assertThat(blockItemsCount).isGreaterThan(1);
            // Should have exactly one END_OF_BLOCK
            assertThat(endOfBlockCount).isEqualTo(1);

            // The last response should be END_OF_BLOCK
            assertThat(responses.getLast().response().kind()).isEqualTo(ResponseOneOfType.END_OF_BLOCK);

            // Verify total items sent equals input items
            int totalItemsSent = responses.stream()
                    .filter(r -> r.response().kind() == ResponseOneOfType.BLOCK_ITEMS)
                    .mapToInt(r -> r.blockItems().blockItems().size())
                    .sum();
            assertThat(totalItemsSent).isEqualTo(largeBlock.size());
        }

        /**
         * This test verifies that an item larger than the chunk size is sent by itself.
         */
        @Test
        @DisplayName("should send oversized item alone")
        void testOversizedItemShipsAlone() {
            // Create a block with one small item and one large item that exceeds chunk size (100KB)
            final BlockItemUnparsed smallItem = sampleHeaderUnparsed(0);
            final BlockItemUnparsed largeItem = createLargeBlockItem(TEST_CHUNK_SIZE_BYTES + 1000);
            final BlockItemUnparsed anotherSmallItem = sampleProofUnparsed(0);

            final List<BlockItemUnparsed> mixedBlock = List.of(smallItem, largeItem, anotherSmallItem);

            // Create session with chunking config
            final SubscribeStreamRequest request = SubscribeStreamRequest.newBuilder()
                    .startBlockNumber(0L)
                    .endBlockNumber(0L)
                    .build();
            final BlockStreamSubscriberSession session = new BlockStreamSubscriberSession(
                    SessionContext.create(clientId, request, chunkingContext),
                    chunkingPipeline,
                    chunkingContext,
                    sessionReadyLatch);

            // Call the chunking method directly
            session.sendBlockItemsChunked(mixedBlock);

            // Verify responses
            final var responses = chunkingPipeline.getOnNextCalls();

            // Should have multiple BLOCK_ITEMS responses due to the oversized item
            long blockItemsCount = responses.stream()
                    .filter(r -> r.response().kind() == ResponseOneOfType.BLOCK_ITEMS)
                    .count();
            assertThat(blockItemsCount).isGreaterThanOrEqualTo(2);

            // Find the response containing the large item (should have exactly 1 item)
            boolean foundLargeItemAlone = responses.stream()
                    .filter(r -> r.response().kind() == ResponseOneOfType.BLOCK_ITEMS)
                    .anyMatch(r -> {
                        final var items = r.blockItems().blockItems();
                        return items.size() == 1
                                && BlockItemUnparsed.PROTOBUF.measureRecord(items.getFirst()) > TEST_CHUNK_SIZE_BYTES;
                    });
            assertThat(foundLargeItemAlone).isTrue();

            // Verify exactly one END_OF_BLOCK
            long endOfBlockCount = responses.stream()
                    .filter(r -> r.response().kind() == ResponseOneOfType.END_OF_BLOCK)
                    .count();
            assertThat(endOfBlockCount).isEqualTo(1);
        }

        /**
         * This test verifies that chunked sending stops as soon as the session is closed,
         * instead of sending every remaining chunk of a large block.
         */
        @Test
        @DisplayName("should stop sending further chunks once the session is closed")
        void testChunkedSendStopsWhenClosed() {
            // Large block requiring multiple chunks (same shape as testLargeBlockMultipleChunks).
            final List<BlockItemUnparsed> largeBlock = new java.util.ArrayList<>();
            largeBlock.add(sampleHeaderUnparsed(0));
            largeBlock.add(createLargeBlockItem(50_000));
            largeBlock.add(createLargeBlockItem(50_000));
            largeBlock.add(createLargeBlockItem(50_000));
            largeBlock.add(sampleProofUnparsed(0));

            final SubscribeStreamRequest request = SubscribeStreamRequest.newBuilder()
                    .startBlockNumber(0L)
                    .endBlockNumber(0L)
                    .build();
            final BlockStreamSubscriberSession session = new BlockStreamSubscriberSession(
                    SessionContext.create(clientId, request, chunkingContext),
                    chunkingPipeline,
                    chunkingContext,
                    sessionReadyLatch);

            // Simulate the session already being closed when a chunked send is in progress (e.g.
            // a concurrent close(), or a future force-cancellation of this thread).
            Thread.currentThread().interrupt();
            try {
                session.sendBlockItemsChunked(largeBlock);
            } finally {
                // Clear the interrupt so it doesn't leak into any other test sharing this thread.
                assertThat(Thread.interrupted()).isTrue();
            }

            // Exactly one chunk should have been sent before the closed check stopped the loop.
            final var responses = chunkingPipeline.getOnNextCalls();
            assertThat(responses).hasSize(1);
            assertThat(responses.getFirst().response().kind()).isEqualTo(ResponseOneOfType.BLOCK_ITEMS);
        }

        /**
         * This test verifies that an empty block list results in no responses.
         */
        @Test
        @DisplayName("should handle empty block gracefully")
        void testEmptyBlockNoResponses() {
            final List<BlockItemUnparsed> emptyBlock = List.of();

            // Create session with chunking config
            final SubscribeStreamRequest request = SubscribeStreamRequest.newBuilder()
                    .startBlockNumber(0L)
                    .endBlockNumber(0L)
                    .build();
            final BlockStreamSubscriberSession session = new BlockStreamSubscriberSession(
                    SessionContext.create(clientId, request, chunkingContext),
                    chunkingPipeline,
                    chunkingContext,
                    sessionReadyLatch);

            // Call the chunking method directly
            session.sendBlockItemsChunked(emptyBlock);

            // Verify: no responses for empty input
            assertThat(chunkingPipeline.getOnNextCalls()).isEmpty();
        }

        /**
         * Creates a block item with padding to reach approximately the target size.
         */
        private BlockItemUnparsed createLargeBlockItem(final int targetSizeBytes) {
            // Create padding bytes to reach the target size
            final byte[] padding = new byte[targetSizeBytes];
            java.util.Arrays.fill(padding, (byte) 'X');
            return BlockItemUnparsed.newBuilder()
                    .roundHeader(com.hedera.pbj.runtime.io.buffer.Bytes.wrap(padding))
                    .build();
        }
    }

    /**
     * Tests for how the session reacts when sending to the client fails.
     */
    @Nested
    @DisplayName("Send Failure Tests")
    class SendFailureTests {
        /** Upper bound for waits, so a regression fails the test instead of hanging it. */
        private static final long WAIT_TIMEOUT_SECONDS = 10L;

        /**
         * This test verifies that once a send to the client fails, the session
         * stops sending the live batches already queued. Each send to a failed
         * HTTP/2 stream blocks for the full flow control timeout, so continuing
         * would keep the session thread busy on a dead stream.
         */
        @Test
        @DisplayName("should stop sending queued live batches after a send failure")
        void testQueuedLiveBatchesNotSentAfterSendFailure() throws Exception {
            final StallingFailingResponsePipeline failingPipeline = new StallingFailingResponsePipeline();
            final TestBlockMessagingFacility messagingFacility = new TestBlockMessagingFacility();
            final BlockNodeContext context =
                    generateContext(blockNodeContext.configuration(), messagingFacility, historicalBlockFacility);
            final SubscribeStreamRequest liveRequest = SubscribeStreamRequest.newBuilder()
                    .startBlockNumber(-1L)
                    .endBlockNumber(-1L)
                    .build();
            final BlockStreamSubscriberSession session = new BlockStreamSubscriberSession(
                    SessionContext.create(clientId, liveRequest, context), failingPipeline, context, sessionReadyLatch);

            try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
                final Future<BlockStreamSubscriberSession> sessionFuture = executor.submit(session);
                assertThat(sessionReadyLatch.await(WAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS))
                        .isTrue();

                messagingFacility.sendBlockItems(new BlockItems(List.of(sampleHeaderUnparsed(0)), 0L, true, false));
                assertThat(failingPipeline.firstSendStarted.await(WAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS))
                        .isTrue();
                // Queue more batches while the first send is blocked, as happens during a real stall.
                messagingFacility.sendBlockItems(
                        new BlockItems(List.of(sampleRoundHeaderUnparsed(0)), 0L, false, false));
                messagingFacility.sendBlockItems(new BlockItems(List.of(sampleProofUnparsed(0)), 0L, false, true));
                failingPipeline.releaseFirstSend.countDown();

                sessionFuture.get(WAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }

            assertThat(failingPipeline.getOnNextCalls()).hasSize(1);
        }
    }

    /**
     * Tests for how a session waiting to resolve the first live block reacts to being closed.
     */
    @Nested
    @DisplayName("Live Tip Cancellation Tests")
    class LiveTipCancellationTests {
        /** Upper bound for waits, so a regression fails the test instead of hanging it. */
        private static final long WAIT_TIMEOUT_SECONDS = 10L;

        /**
         * This test verifies that a live-stream session that is closed before any live block
         * has reached the head of the queue still terminates. Before the fix, the wait for the
         * first live block ignored the closed/interrupted flag and parked forever, leaking the
         * session (and its thread) whenever a client disconnected while no block was flowing yet.
         */
        @Test
        @DisplayName("should stop waiting for the first live block once the session is closed")
        void testResolveLiveNextBlockStopsOnClose() throws Exception {
            final SubscribeStreamRequest liveRequest = SubscribeStreamRequest.newBuilder()
                    .startBlockNumber(-1L)
                    .endBlockNumber(-1L)
                    .build();
            final BlockStreamSubscriberSession session = new BlockStreamSubscriberSession(
                    SessionContext.create(clientId, liveRequest, blockNodeContext),
                    responsePipeline,
                    blockNodeContext,
                    sessionReadyLatch);

            final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
            try {
                final Future<BlockStreamSubscriberSession> sessionFuture = executor.submit(session);
                assertThat(sessionReadyLatch.await(WAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS))
                        .isTrue();
                // No live block is ever sent, so without the fix the session would park here forever.
                session.close(Code.SUCCESS);

                assertThatNoException().isThrownBy(() -> sessionFuture.get(WAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS));
            } finally {
                // shutdownNow(), not close(), so a regression fails fast instead of hanging the build.
                executor.shutdownNow();
            }
        }

        /**
         * This test verifies that a live-stream session stops waiting for the first live block
         * when its thread is interrupted directly, even though nothing ever calls close(). This
         * covers a future force-cancellation mechanism (e.g. a watchdog calling
         * {@code Future.cancel(true)} on a stuck session) that stops a session without going
         * through the normal close() path.
         */
        @Test
        @DisplayName("should stop waiting for the first live block on a raw thread interrupt, without close()")
        void testResolveLiveNextBlockStopsOnRawInterrupt() throws Exception {
            final SubscribeStreamRequest liveRequest = SubscribeStreamRequest.newBuilder()
                    .startBlockNumber(-1L)
                    .endBlockNumber(-1L)
                    .build();
            final BlockStreamSubscriberSession session = new BlockStreamSubscriberSession(
                    SessionContext.create(clientId, liveRequest, blockNodeContext),
                    responsePipeline,
                    blockNodeContext,
                    sessionReadyLatch);

            final Thread sessionThread = new Thread(session::call, "test-session-thread");
            sessionThread.start();
            assertThat(sessionReadyLatch.await(WAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS))
                    .isTrue();
            // No live block is ever sent and close() is never called - only a raw interrupt, as a
            // future force-cancellation mechanism would do.
            sessionThread.interrupt();
            sessionThread.join(TimeUnit.SECONDS.toMillis(WAIT_TIMEOUT_SECONDS));

            assertThat(sessionThread.isAlive()).isFalse();
        }
    }

    /**
     * Tests for re-registering the live block handler after it fell too far behind.
     */
    @Nested
    @DisplayName("Live Handler Re-registration Tests")
    class LiveHandlerReregistrationTests {
        /** Upper bound for waits, so a regression fails the test instead of hanging it. */
        private static final long WAIT_TIMEOUT_SECONDS = 10L;

        /**
         * This test verifies that a session closed partway through a loop iteration does not
         * re-register its live block handler. close() unregisters the handler, so registering it
         * again would leave it receiving live blocks with no session to drain its queue.
         */
        @Test
        @DisplayName("should not re-register the live block handler after the session is closed")
        void testClosedSessionDoesNotReregisterHandler() throws Exception {
            historicalBlockFacility.handleBlockItemsReceived(
                    generateBlockWithNumber(0L).asBlockItems(), false);
            final RegistrationCountingMessagingFacility messagingFacility = new RegistrationCountingMessagingFacility();
            final BlockNodeContext context =
                    generateContext(blockNodeContext.configuration(), messagingFacility, historicalBlockFacility);
            final HandlerBehindFailingResponsePipeline pipeline =
                    new HandlerBehindFailingResponsePipeline(messagingFacility);
            final SubscribeStreamRequest request = SubscribeStreamRequest.newBuilder()
                    .startBlockNumber(0L)
                    .endBlockNumber(-1L)
                    .build();
            final BlockStreamSubscriberSession session = new BlockStreamSubscriberSession(
                    SessionContext.create(clientId, request, context), pipeline, context, sessionReadyLatch);

            try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
                executor.submit(session).get(WAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }

            assertThat(messagingFacility.registrationCount.get()).isEqualTo(1);
        }
    }

    /**
     * Tests for the idle session limit, which closes sessions that have sent nothing to their
     * client for {@link SubscriberConfig#idleSessionTimeoutSeconds()}. A client that disconnects
     * while its session waits for blocks is otherwise never detected, leaking the session.
     */
    @Nested
    @DisplayName("Idle Session Timeout Tests")
    class IdleSessionTimeoutTests {
        /** Upper bound for waits, so a regression fails the test instead of hanging it. */
        private static final long WAIT_TIMEOUT_SECONDS = 10L;
        /** The default idle session limit. */
        private static final long IDLE_TIMEOUT_SECONDS = 60L;
        /**
         * Clock reads that prove the session evaluated the idle limit at least once at the
         * current time; one read may still have seen the previous time.
         */
        private static final int READS_FOR_ONE_FULL_CHECK = 3;

        private FakeClock clock;
        private ExecutorService executor;

        @BeforeEach
        void setupIdleTests() {
            clock = new FakeClock();
            executor = Executors.newVirtualThreadPerTaskExecutor();
        }

        @AfterEach
        void tearDownIdleTests() {
            // shutdownNow(), not close(), so a regression fails fast instead of hanging the build.
            executor.shutdownNow();
        }

        @Test
        @DisplayName("should close a live-tip session with SUCCESS when no live block arrives within the limit")
        void testLiveTipSessionClosesWhenIdle() throws Exception {
            final BlockStreamSubscriberSession session = createSession(-1L, -1L, blockNodeContext, responsePipeline);
            final Future<BlockStreamSubscriberSession> sessionFuture = executor.submit(session);
            assertThat(sessionReadyLatch.await(WAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS))
                    .isTrue();

            clock.advanceSeconds(IDLE_TIMEOUT_SECONDS);
            sessionFuture.get(WAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS);

            assertThat(responsePipeline.getOnNextCalls())
                    .last()
                    .extracting(responseStatusExtractor)
                    .isEqualTo(Code.SUCCESS);
        }

        @Test
        @DisplayName("should close a caught-up open-ended historical session with SUCCESS when idle")
        void testCaughtUpHistoricalSessionClosesWhenIdle() throws Exception {
            for (long blockNumber = 0L; blockNumber <= 2L; blockNumber++) {
                historicalBlockFacility.handleBlockItemsReceived(
                        generateBlockWithNumber(blockNumber).asBlockItems(), false);
            }
            // Each block is sent as one batch of items followed by an end of block.
            final LatchedResponsePipeline pipeline = new LatchedResponsePipeline(6);
            final BlockStreamSubscriberSession session = createSession(0L, -1L, blockNodeContext, pipeline);
            final Future<BlockStreamSubscriberSession> sessionFuture = executor.submit(session);
            assertThat(pipeline.expectedSends.await(WAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS))
                    .isTrue();

            clock.advanceSeconds(IDLE_TIMEOUT_SECONDS);
            sessionFuture.get(WAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS);

            assertThat(pipeline.getOnNextCalls())
                    .last()
                    .extracting(responseStatusExtractor)
                    .isEqualTo(Code.SUCCESS);
        }

        @Test
        @DisplayName("should measure the idle limit from the last send, not from the session start")
        void testIdleLimitMeasuredFromLastSend() throws Exception {
            final TestBlockMessagingFacility messagingFacility = new TestBlockMessagingFacility();
            final BlockNodeContext context =
                    generateContext(blockNodeContext.configuration(), messagingFacility, historicalBlockFacility);
            // One block is sent as one batch of items followed by an end of block.
            final LatchedResponsePipeline pipeline = new LatchedResponsePipeline(2);
            final BlockStreamSubscriberSession session = createSession(-1L, -1L, context, pipeline);
            final Future<BlockStreamSubscriberSession> sessionFuture = executor.submit(session);
            assertThat(sessionReadyLatch.await(WAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS))
                    .isTrue();
            clock.advanceSeconds(IDLE_TIMEOUT_SECONDS - 10L);
            messagingFacility.sendBlockItems(generateBlockWithNumber(0L).asBlockItems());
            assertThat(pipeline.expectedSends.await(WAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS))
                    .isTrue();

            // Past the limit since the session started, but within it since the last send.
            clock.advanceSeconds(IDLE_TIMEOUT_SECONDS - 10L);
            assertThat(clock.awaitReads(READS_FOR_ONE_FULL_CHECK, WAIT_TIMEOUT_SECONDS))
                    .isTrue();

            assertThat(session.closedForIdle()).isFalse();
            session.close(Code.SUCCESS);
            sessionFuture.get(WAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }

        @Test
        @DisplayName("should keep a future-start session open while the live stream advances toward its start block")
        void testFutureStartSessionNotClosedWhileLiveAdvances() throws Exception {
            for (long blockNumber = 0L; blockNumber <= 2L; blockNumber++) {
                historicalBlockFacility.handleBlockItemsReceived(
                        generateBlockWithNumber(blockNumber).asBlockItems(), false);
            }
            final TestBlockMessagingFacility messagingFacility = new TestBlockMessagingFacility();
            final BlockNodeContext context =
                    generateContext(blockNodeContext.configuration(), messagingFacility, historicalBlockFacility);
            final BlockStreamSubscriberSession session = createSession(5L, -1L, context, responsePipeline);
            final Future<BlockStreamSubscriberSession> sessionFuture = executor.submit(session);
            assertThat(sessionReadyLatch.await(WAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS))
                    .isTrue();
            clock.advanceSeconds(IDLE_TIMEOUT_SECONDS - 10L);
            // Block 3 precedes the requested start block 5, so the session skips it without sending.
            messagingFacility.sendBlockItems(generateBlockWithNumber(3L).asBlockItems());
            assertThat(clock.awaitReads(READS_FOR_ONE_FULL_CHECK, WAIT_TIMEOUT_SECONDS))
                    .isTrue();

            // Past the limit since the session started, but within it since the skipped live block.
            clock.advanceSeconds(IDLE_TIMEOUT_SECONDS - 10L);
            assertThat(clock.awaitReads(READS_FOR_ONE_FULL_CHECK, WAIT_TIMEOUT_SECONDS))
                    .isTrue();

            assertThat(session.closedForIdle()).isFalse();
            session.close(Code.SUCCESS);
            sessionFuture.get(WAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }

        @Test
        @DisplayName("should start the idle limit when the session starts serving, not when it is created")
        void testIdleLimitExcludesTimeQueuedBeforeServing() throws Exception {
            final BlockStreamSubscriberSession session = createSession(-1L, -1L, blockNodeContext, responsePipeline);
            // The session waited the full limit for a worker thread before it started serving.
            clock.advanceSeconds(IDLE_TIMEOUT_SECONDS);
            final Future<BlockStreamSubscriberSession> sessionFuture = executor.submit(session);
            assertThat(sessionReadyLatch.await(WAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS))
                    .isTrue();
            assertThat(clock.awaitReads(READS_FOR_ONE_FULL_CHECK, WAIT_TIMEOUT_SECONDS))
                    .isTrue();

            assertThat(session.closedForIdle()).isFalse();
            session.close(Code.SUCCESS);
            sessionFuture.get(WAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }

        @Test
        @DisplayName("should never close an idle session when the limit is 0")
        void testZeroLimitDisablesIdleClose() throws Exception {
            final Configuration configuration = ConfigurationBuilder.create()
                    .withConfigDataType(SubscriberConfig.class)
                    .withValue("subscriber.idleSessionTimeoutSeconds", "0")
                    .build();
            final BlockNodeContext context =
                    generateContext(configuration, new TestBlockMessagingFacility(), historicalBlockFacility);
            final BlockStreamSubscriberSession session = createSession(-1L, -1L, context, responsePipeline);
            final Future<BlockStreamSubscriberSession> sessionFuture = executor.submit(session);
            assertThat(sessionReadyLatch.await(WAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS))
                    .isTrue();

            clock.advanceSeconds(TimeUnit.DAYS.toSeconds(365L));
            assertThat(clock.awaitReads(READS_FOR_ONE_FULL_CHECK, WAIT_TIMEOUT_SECONDS))
                    .isTrue();

            assertThat(session.closedForIdle()).isFalse();
            session.close(Code.SUCCESS);
            sessionFuture.get(WAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }

        private BlockStreamSubscriberSession createSession(
                final long startBlock,
                final long endBlock,
                final BlockNodeContext context,
                final TestResponsePipeline<SubscribeStreamResponseUnparsed> pipeline) {
            final SubscribeStreamRequest request = SubscribeStreamRequest.newBuilder()
                    .startBlockNumber(startBlock)
                    .endBlockNumber(endBlock)
                    .build();
            return new BlockStreamSubscriberSession(
                    SessionContext.create(clientId, request, context), pipeline, context, sessionReadyLatch, clock);
        }
    }

    /**
     * Manually advanced monotonic clock that counts its reads, so a test can wait until the
     * session has looked at the time after an advance instead of sleeping.
     */
    private static final class FakeClock implements LongSupplier {
        private final AtomicLong nanos = new AtomicLong(0L);
        private final Semaphore reads = new Semaphore(0);

        @Override
        public long getAsLong() {
            final long now = nanos.get();
            reads.release();
            return now;
        }

        void advanceSeconds(final long seconds) {
            reads.drainPermits();
            nanos.addAndGet(TimeUnit.SECONDS.toNanos(seconds));
        }

        boolean awaitReads(final int count, final long timeoutSeconds) throws InterruptedException {
            return reads.tryAcquire(count, timeoutSeconds, TimeUnit.SECONDS);
        }
    }

    /**
     * Response pipeline that counts down a latch on each response, so a test can wait for an
     * expected number of sends.
     */
    private static final class LatchedResponsePipeline extends TestResponsePipeline<SubscribeStreamResponseUnparsed> {
        private final CountDownLatch expectedSends;

        private LatchedResponsePipeline(final int expectedSendCount) {
            expectedSends = new CountDownLatch(expectedSendCount);
        }

        @Override
        public void onNext(final SubscribeStreamResponseUnparsed item) {
            super.onNext(item);
            expectedSends.countDown();
        }
    }

    /**
     * Messaging facility that counts registrations of no-backpressure handlers and keeps the last
     * one registered, so a test can mark it as fallen behind.
     */
    private static final class RegistrationCountingMessagingFacility extends TestBlockMessagingFacility {
        private final AtomicLong registrationCount = new AtomicLong(0L);
        private volatile NoBackPressureBlockItemHandler lastRegisteredHandler;

        @Override
        public void registerNoBackpressureBlockItemHandler(
                final NoBackPressureBlockItemHandler handler,
                final boolean cpuIntensiveHandler,
                final String handlerName) {
            registrationCount.incrementAndGet();
            lastRegisteredHandler = handler;
            super.registerNoBackpressureBlockItemHandler(handler, cpuIntensiveHandler, handlerName);
        }
    }

    /**
     * Response pipeline whose sends fail after marking the session's live block handler as
     * fallen behind, so the session closes itself while its handler is eligible to re-register.
     */
    private static final class HandlerBehindFailingResponsePipeline
            extends TestResponsePipeline<SubscribeStreamResponseUnparsed> {
        private final RegistrationCountingMessagingFacility messagingFacility;

        private HandlerBehindFailingResponsePipeline(final RegistrationCountingMessagingFacility messagingFacility) {
            this.messagingFacility = messagingFacility;
        }

        @Override
        public void onNext(final SubscribeStreamResponseUnparsed item) {
            super.onNext(item);
            if (item.hasBlockItems()) {
                messagingFacility.setHandlerBehind(messagingFacility.lastRegisteredHandler);
                throw new RuntimeException("Stream reset by client.");
            }
        }
    }

    /**
     * Response pipeline that fails every send. The first send blocks until
     * released, like a stream waiting for an HTTP/2 window update that never
     * arrives, so the test can queue more batches before the failure.
     */
    private static final class StallingFailingResponsePipeline
            extends TestResponsePipeline<SubscribeStreamResponseUnparsed> {
        private final CountDownLatch firstSendStarted = new CountDownLatch(1);
        private final CountDownLatch releaseFirstSend = new CountDownLatch(1);

        @Override
        public void onNext(final SubscribeStreamResponseUnparsed item) {
            super.onNext(item);
            firstSendStarted.countDown();
            try {
                releaseFirstSend.await();
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            throw new RuntimeException("Flow control update wait time-out.");
        }
    }

    /**
     * Generate a basic BlockNodeContext for testing purposes.
     */
    private BlockNodeContext generateContext(
            final Configuration configuration,
            final TestBlockMessagingFacility messagingFacility,
            final SimpleInMemoryHistoricalBlockFacility historicalBlockFacility) {
        return new BlockNodeContext(
                configuration, null, null, messagingFacility, historicalBlockFacility, null, null, null, null);
    }

    /**
     * Generate an instance of the {@link BlockStreamSubscriberSession} to be
     * tested.
     */
    private BlockStreamSubscriberSession generateSession(final SubscribeStreamRequest request) {
        return new BlockStreamSubscriberSession(
                SessionContext.create(clientId, request, blockNodeContext),
                responsePipeline,
                blockNodeContext,
                sessionReadyLatch);
    }
}
