// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.stream.subscriber;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import com.swirlds.config.api.Configuration;
import com.swirlds.config.api.ConfigurationBuilder;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;
import java.util.stream.Stream;
import org.hiero.block.api.SubscribeStreamRequest;
import org.hiero.block.api.SubscribeStreamResponse.Code;
import org.hiero.block.internal.SubscribeStreamResponseUnparsed;
import org.hiero.block.internal.SubscribeStreamResponseUnparsed.ResponseOneOfType;
import org.hiero.block.node.app.fixtures.pipeline.TestResponsePipeline;
import org.hiero.block.node.app.fixtures.plugintest.SimpleBlockRangeSet;
import org.hiero.block.node.app.fixtures.plugintest.SimpleInMemoryHistoricalBlockFacility;
import org.hiero.block.node.app.fixtures.plugintest.TestBlockMessagingFacility;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.block.node.spi.historicalblocks.BlockRangeSet;
import org.hiero.block.node.stream.subscriber.BlockStreamSubscriberSession.SessionContext;
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
     * Generate a basic BlockNodeContext for testing purposes.
     */
    private BlockNodeContext generateContext(
            final Configuration configuration,
            final TestBlockMessagingFacility messagingFacility,
            final SimpleInMemoryHistoricalBlockFacility historicalBlockFacility) {
        return new BlockNodeContext(configuration, null, null, messagingFacility, historicalBlockFacility, null, null);
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
