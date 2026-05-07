// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.stream.publisher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatCode;
import static org.hiero.block.node.stream.publisher.fixtures.PublishApiUtility.endThisBlock;

import com.hedera.pbj.runtime.grpc.Pipeline;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Flow.Subscription;
import java.util.function.Function;
import java.util.stream.Stream;
import org.hiero.block.api.PublishStreamRequest.EndStream;
import org.hiero.block.api.PublishStreamResponse;
import org.hiero.block.api.PublishStreamResponse.EndOfStream.Code;
import org.hiero.block.api.PublishStreamResponse.ResponseOneOfType;
import org.hiero.block.internal.BlockItemSetUnparsed;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.internal.PublishStreamRequestUnparsed;
import org.hiero.block.node.app.fixtures.TestMetricsExporter;
import org.hiero.block.node.app.fixtures.blocks.TestBlock;
import org.hiero.block.node.app.fixtures.blocks.TestBlockBuilder;
import org.hiero.block.node.app.fixtures.pipeline.TestResponsePipeline;
import org.hiero.block.node.app.fixtures.plugintest.TestBlockMessagingFacility;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.NewestBlockKnownToNetworkNotification;
import org.hiero.block.node.spi.blockmessaging.PersistedNotification;
import org.hiero.block.node.stream.publisher.PublisherHandler.MetricsHolder;
import org.hiero.block.node.stream.publisher.StreamPublisherManager.ActionForBlock;
import org.hiero.block.node.stream.publisher.StreamPublisherManager.BlockAction;
import org.hiero.block.node.stream.publisher.fixtures.TestStreamPublisherManager;
import org.hiero.metrics.core.MetricRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

/// Tests for [PublisherHandler].
@DisplayName("PublisherHandler Tests")
class PublisherHandlerTest {

    private TestMetricsExporter metricsExporter;

    /**
     * Constructor tests for {@link PublisherHandler}.
     */
    @Nested
    @DisplayName("Constructor Tests")
    @SuppressWarnings("all")
    class ConstructorTest {
        /** Handler ID used for the tests. */
        private long validNextId;
        /** Test response pipeline used for asserting the handler's responses. */
        private TestResponsePipeline<PublishStreamResponse> validReplyPipeline;
        /** Metrics holder used for asserting the handler's metrics. */
        private MetricsHolder validMetricsHodler;
        /** Test publisher manager used within the handler to test. */
        private TestStreamPublisherManager validPublisherManager;
        /** Transfer queue used for asserting the items offered by the handler. */
        private BlockingQueue<BlockItemSetUnparsed> validTranserQueue;

        /**
         * Environment setup executed before each test in this nested class.
         */
        @BeforeEach
        void setup() {
            validNextId = 1L;
            validReplyPipeline = new TestResponsePipeline();
            validMetricsHodler = createMetrics();
            validPublisherManager = new TestStreamPublisherManager(new TestBlockMessagingFacility());
        }

        /**
         * This test aims to assert that the constructor of {@link PublisherHandler} does not
         * throw any exceptions when provided with valid parameters.
         */
        @Test
        @DisplayName("Test constructor with valid parameters")
        void testValidParameters() {
            assertThatNoException().isThrownBy(() -> {
                new PublisherHandler(validNextId, validReplyPipeline, validMetricsHodler, validPublisherManager, null);
            });
        }

        /**
         * This test aims to assert that the constructor of {@link PublisherHandler} throws a
         * {@link NullPointerException} when the reply pipeline is null.
         */
        @Test
        @DisplayName("Test constructor with null reply pipeline")
        void testNullReplyPipeline() {
            assertThatNullPointerException().isThrownBy(() -> {
                new PublisherHandler(validNextId, null, validMetricsHodler, validPublisherManager, "");
            });
        }

        /**
         * This test aims to assert that the constructor of {@link PublisherHandler} throws a
         * {@link NullPointerException} when the metrics holder is null.
         */
        @Test
        @DisplayName("Test constructor with null metrics holder")
        void testNullMetricsHolder() {
            assertThatNullPointerException().isThrownBy(() -> {
                new PublisherHandler(validNextId, validReplyPipeline, null, validPublisherManager, "");
            });
        }

        /**
         * This test aims to assert that the constructor of {@link PublisherHandler} throws a
         * {@link NullPointerException} when the publisher manager is null.
         */
        @Test
        @DisplayName("Test constructor with null publisher manager")
        void testNullPublisherManager() {
            assertThatNullPointerException().isThrownBy(() -> {
                new PublisherHandler(validNextId, validReplyPipeline, validMetricsHodler, null, "");
            });
        }

        /**
         * This test aims to assert that the constructor of {@link PublisherHandler} does not
         * throw any exceptions when the correlation ID is null (null is treated as absent).
         */
        @Test
        @DisplayName("Test constructor with null correlation ID is accepted")
        void testNullCorrelationId() {
            assertThatNoException().isThrownBy(() -> {
                new PublisherHandler(validNextId, validReplyPipeline, validMetricsHodler, validPublisherManager, null);
            });
        }
    }

    /// Tests verifying that the correlation ID from the gRPC header is prefixed on all log messages.
    @Nested
    @DisplayName("Correlation ID Logging Tests")
    class CorrelationIdLoggingTest {
        private static final String TEST_CORRELATION_ID = "N3-STR1";

        private TestResponsePipeline<PublishStreamResponse> repliesPipeline;
        private MetricsHolder metrics;
        private TestStreamPublisherManager manager;
        private java.util.logging.Logger julLogger;
        private TestLogHandler logHandler;

        @BeforeEach
        void setup() {
            repliesPipeline = new TestResponsePipeline<>();
            metrics = createMetrics();
            manager = new TestStreamPublisherManager(new TestBlockMessagingFacility());

            julLogger = java.util.logging.Logger.getLogger(PublisherHandler.class.getName());
            logHandler = new TestLogHandler();
            julLogger.addHandler(logHandler);
            julLogger.setLevel(java.util.logging.Level.ALL);
        }

        @org.junit.jupiter.api.AfterEach
        void tearDown() {
            julLogger.removeHandler(logHandler);
        }

        /**
         * Verifies that when a correlation ID is set, all log messages emitted by
         * {@link PublisherHandler} are prefixed with {@code [correlationId] }.
         */
        @Test
        @DisplayName("Log messages are prefixed with correlation ID when set")
        void testCorrelationIdPrefixedInLogs() {
            final PublisherHandler handler =
                    new PublisherHandler(1L, repliesPipeline, metrics, manager, TEST_CORRELATION_ID);
            manager.addHandler(handler);

            handler.handleFailedVerification(42L);

            assertThat(logHandler.getLogMessages()).anyMatch(msg -> msg.startsWith("[" + TEST_CORRELATION_ID + "] "));
        }

        /**
         * Verifies that when no correlation ID is set (null), log messages start with
         * empty brackets "[]" and do not contain a real correlation ID prefix.
         */
        @Test
        @DisplayName("Log messages have empty brackets when correlation ID is null")
        void testNoCorrelationIdPrefix() {
            final PublisherHandler handler = new PublisherHandler(1L, repliesPipeline, metrics, manager, null);
            manager.addHandler(handler);

            handler.handleFailedVerification(42L);

            assertThat(logHandler.getLogMessages())
                    .anyMatch(msg -> msg.startsWith("[]"))
                    .noneMatch(msg -> msg.matches("\\[.+\\].*"));
        }

        /**
         * Verifies that the correlation ID prefix appears on multiple distinct log call-sites,
         * covering both DEBUG and TRACE level messages.
         */
        @Test
        @DisplayName("Correlation ID prefix appears on both DEBUG and TRACE log messages")
        void testCorrelationIdPrefixOnMultipleLevels() {
            final PublisherHandler handler =
                    new PublisherHandler(1L, repliesPipeline, metrics, manager, TEST_CORRELATION_ID);
            manager.addHandler(handler);

            // Triggers a DEBUG log: "Handler {0} handling failed verification for block {1}"
            handler.handleFailedVerification(10L);
            // FLAKEY WARNING - Tests that rely on log output are rarely reliable.
            // Triggers a DEBUG log: "[{0}] Handler {1} ending with code {2}"
            handler.endStreamWithCode(Code.PERSISTENCE_FAILED, false);

            final long prefixedCount = logHandler.getLogMessages().stream()
                    .filter(msg -> msg.startsWith("[" + TEST_CORRELATION_ID + "] "))
                    .count();
            assertThat(prefixedCount).isGreaterThanOrEqualTo(2);
        }

        /**
         * Verifies that the correct correlation ID value from the gRPC header is embedded
         * in the log prefix, distinguishing multiple concurrent handler instances.
         */
        @Test
        @DisplayName("Each handler uses its own correlation ID in log prefix")
        void testDistinctCorrelationIdsPerHandler() {
            final String idA = "N1-STR10";
            final String idB = "N2-STR20";
            final PublisherHandler handlerA = new PublisherHandler(1L, repliesPipeline, metrics, manager, idA);
            final PublisherHandler handlerB =
                    new PublisherHandler(2L, new TestResponsePipeline<>(), metrics, manager, idB);
            manager.addHandler(handlerA);
            manager.addHandler(handlerB);

            handlerA.handleFailedVerification(1L);
            handlerB.handleFailedVerification(2L);

            assertThat(logHandler.getLogMessages())
                    .anyMatch(msg -> msg.startsWith("[" + idA + "] "))
                    .anyMatch(msg -> msg.startsWith("[" + idB + "] "));
        }
    }

    /// Functionality tests for [PublisherHandler].
    @Nested
    @DisplayName("Functionality Tests")
    class FunctionalityTest {
        /// Handler ID used for the tests.
        private long handlerId;
        /// Test response pipeline used for asserting the handler's responses.
        private TestResponsePipeline<PublishStreamResponse> repliesPipeline;
        /// Metrics holder used for asserting the handler's metrics.
        private MetricsHolder metrics;
        /// Test publisher manager used within the handler to test.
        private TestStreamPublisherManager manager;
        /// The handler under test.
        private PublisherHandler toTest;

        // ASSERTION EXTRACTORS
        private final Function<PublishStreamResponse, ResponseOneOfType> responseKindExtractor =
                response -> response.response().kind();
        private final Function<PublishStreamResponse, Code> endStreamResponseCodeExtractor =
                response -> Objects.requireNonNull(response.endStream()).status();
        private final Function<PublishStreamResponse, Long> nodeBehindBlockNumberExtractor = response ->
                Objects.requireNonNull(response.nodeBehindPublisher()).blockNumber();
        private final Function<PublishStreamResponse, Long> endStreamBlockNumberExtractor =
                response -> Objects.requireNonNull(response.endStream()).blockNumber();
        private final Function<PublishStreamResponse, Long> skipBlockNumberExtractor =
                response -> Objects.requireNonNull(response.skipBlock()).blockNumber();
        private final Function<PublishStreamResponse, Long> resendBlockNumberExtractor =
                response -> Objects.requireNonNull(response.resendBlock()).blockNumber();
        private final Function<PublishStreamResponse, Long> acknowledgementBlockNumberExtractor =
                response -> Objects.requireNonNull(response.acknowledgement()).blockNumber();

        /// Environment setup executed before each test in this nested class.
        @BeforeEach
        void setup() {
            handlerId = 1L;
            repliesPipeline = new TestResponsePipeline();
            metrics = createMetrics();
            manager = new TestStreamPublisherManager(new TestBlockMessagingFacility());
            toTest = new PublisherHandler(handlerId, repliesPipeline, metrics, manager, null);
            manager.addHandler(toTest);
        }

        /// Tests for [PublisherHandler#onNext(PublishStreamRequestUnparsed)].
        @Nested
        @DisplayName("onNext() Tests")
        class OnNextTest {
            /// This test aims to assert that the {@link PublisherHandler} correctly handles a received request
            /// {@link PublisherHandler#onNext(PublishStreamRequestUnparsed)} happy
            /// path scenario. Here we stream a single complete valid block as items.
            /// The request's first item is the block header for the streamed block,
            /// and the last item is the block proof for the streamed block.
            /// We expect that when the {@link StreamPublisherManager} returns
            /// {@link BlockAction#ACCEPT} for the streamed block number, the items
            /// will be offered to the transfer queue.
            @Test
            @DisplayName("Test onNext() with valid full block request queue created for block when action is ACCEPT")
            void testOnNextACCEPT() {
                // Setup request to send, in this case a single complete valid block
                // as items, starting with header and ending with proof
                final TestBlock block = TestBlockBuilder.generateBlockWithNumber(0L);
                final BlockItemSetUnparsed blockItemSet = block.asItemSetUnparsed();
                final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(blockItemSet)
                        .build();
                // Train the manager to return ACCEPT for the block number
                manager.setBlockActionForBlock(BlockAction.ACCEPT);
                // Assert queue not yet created
                assertThat(manager.getQueueForBlock(block.number())).isNull();
                // Call
                toTest.onNext(request);
                // Assert queue created
                assertThat(manager.getQueueForBlock(block.number())).isNotNull();
            }

            /// This test aims to assert that the [PublisherHandler] correctly
            /// handles a received request
            /// [PublisherHandler#onNext(PublishStreamRequestUnparsed)] happy
            /// path scenario. Here we stream a single complete valid block as items.
            /// The request's first item is the block header for the streamed block,
            /// and the last item is the block proof for the streamed block.
            /// We expect that when the [StreamPublisherManager] returns
            /// [BlockAction#ACCEPT] for the streamed block number, the items
            /// will be offered to the transfer queue.
            @Test
            @DisplayName(
                    "Test onNext() with valid request with a complete single block items streamed - happy path ACCEPT")
            void testOnNextHappyPathACCEPT() {
                // Setup request to send, in this case a single complete valid block
                // as items, starting with header and ending with proof
                final TestBlock block = TestBlockBuilder.generateBlockWithNumber(0L);
                final BlockItemSetUnparsed blockItemSet = block.asItemSetUnparsed();
                final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(blockItemSet)
                        .build();
                // Train the manager to return ACCEPT for the block number
                manager.setBlockActionForBlock(BlockAction.ACCEPT);
                // Call
                toTest.onNext(request);
                // Assert items offered to the transfer queue
                assertThat(manager.getQueueForBlock(block.number())).hasSize(1).containsExactly(blockItemSet);
            }

            /// This test aims to assert that the [PublisherHandler] correctly
            /// handles a received request
            /// [PublisherHandler#onNext(PublishStreamRequestUnparsed)] happy
            /// path scenario. Here we stream a single complete valid block as items.
            /// The request's first item is the block header for the streamed block,
            /// and the last item is the block proof for the streamed block.
            /// We expect that when the [StreamPublisherManager] returns
            /// [BlockAction#ACCEPT] for the streamed block number no replies
            /// to the pipeline are made.
            @Test
            @DisplayName(
                    "Test onNext() with valid request with a complete single block no replies to pipeline - happy path ACCEPT")
            void testOnNextNoRepliesHappyPathACCEPT() {
                // Setup request to send, in this case a single complete valid block
                // as items, starting with header and ending with proof
                final TestBlock block = TestBlockBuilder.generateBlockWithNumber(0L);
                final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(block.asItemSetUnparsed())
                        .build();
                // Train the manager to return ACCEPT for the block number
                manager.setBlockActionForBlock(BlockAction.ACCEPT);
                // Call
                toTest.onNext(request);
                // Assert no replies sent to the pipeline
                assertThat(repliesPipeline.getOnNextCalls()).isEmpty();
                assertThat(repliesPipeline.getOnErrorCalls()).isEmpty();
                assertThat(repliesPipeline.getOnSubscriptionCalls()).isEmpty();
                assertThat(repliesPipeline.getOnCompleteCalls().get()).isEqualTo(0);
                assertThat(repliesPipeline.getClientEndStreamCalls().get()).isEqualTo(0);
            }

            /// This test aims to assert that the [PublisherHandler] correctly
            /// handles a received request
            /// [PublisherHandler#onNext(PublishStreamRequestUnparsed)] happy
            /// path scenario. Here we stream a single complete valid block as items.
            /// The request's first item is the block header for the streamed block,
            /// and the last item is the block proof for the streamed block.
            /// We expect that when the [StreamPublisherManager] returns
            /// [BlockAction#ACCEPT] for the streamed block number the metrics
            /// will be properly updated.
            @Test
            @DisplayName(
                    "Test onNext() with valid request with a complete single block metrics updated - happy path ACCEPT")
            void testOnNextMetricsHappyPathACCEPT() {
                // Setup request to send, in this case a single complete valid block
                // as items, starting with header and ending with proof
                final TestBlock block = TestBlockBuilder.generateBlockWithNumber(0L);
                final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(block.asItemSetUnparsed())
                        .build();
                // Train the manager to return ACCEPT for the block number
                manager.setBlockActionForBlock(BlockAction.ACCEPT);
                // Call
                toTest.onNext(request);
                // Assert metrics updated
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCK_ITEMS_RECEIVED))
                        .isEqualTo(block.asBlockItemUnparsedArray().length);
                // Assert other metrics unchanged
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCKS_ACK_SENT))
                        .isEqualTo(0);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_STREAM_ERRORS))
                        .isEqualTo(0);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCKS_SKIPS_SENT))
                        .isEqualTo(0);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCKS_RESEND_SENT))
                        .isEqualTo(0);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCK_ENDOFSTREAM_SENT))
                        .isEqualTo(0);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCK_SEND_RESPONSE_FAILED))
                        .isEqualTo(0);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCK_ENDSTREAM_RECEIVED))
                        .isEqualTo(0);
            }

            /// This test aims to assert that the [PublisherHandler] correctly
            /// handles a received request
            /// [PublisherHandler#onNext(PublishStreamRequestUnparsed)] happy
            /// path scenario. Here we stream a single complete valid block as items.
            /// The request's first item is the block header for the streamed block,
            /// and the last item is the block proof for the streamed block.
            /// We expect that when the [StreamPublisherManager] returns
            /// [BlockAction#ACCEPT] for the streamed block number no replies
            /// to the pipeline are made. After a [PersistedNotification] for
            /// the streamed block is published to the manager, the handler will
            /// send an acknowledgement response.
            @Test
            @DisplayName(
                    "Test onNext() with valid request with a complete single block sends acknowledgement after persisted - happy path ACCEPT")
            void testOnNextHappyPathResponseACCEPT() {
                // Setup request to send, in this case a single complete valid block
                // as items, starting with header and ending with proof
                final TestBlock block = TestBlockBuilder.generateBlockWithNumber(0L);
                final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(block.asItemSetUnparsed())
                        .build();
                // Train the manager to return ACCEPT for the block number
                manager.setBlockActionForBlock(BlockAction.ACCEPT);
                // Call
                toTest.onNext(request);
                // Assert no replies sent to the pipeline
                assertThat(repliesPipeline.getOnNextCalls()).isEmpty();
                assertThat(repliesPipeline.getOnErrorCalls()).isEmpty();
                assertThat(repliesPipeline.getOnSubscriptionCalls()).isEmpty();
                assertThat(repliesPipeline.getOnCompleteCalls().get()).isEqualTo(0);
                assertThat(repliesPipeline.getClientEndStreamCalls().get()).isEqualTo(0);
                // Publish the persisted notification for the streamed block
                manager.handlePersisted(new PersistedNotification(block.number(), true, 0, BlockSource.PUBLISHER));
                // Assert the acknowledgement response is sent to the pipeline
                // Assert that an acknowledgement was sent for the valid request
                assertThat(repliesPipeline.getOnNextCalls())
                        .hasSize(1)
                        .first()
                        .returns(ResponseOneOfType.ACKNOWLEDGEMENT, responseKindExtractor)
                        .returns(block.number(), acknowledgementBlockNumberExtractor);
                // Assert no other responses sent to the pipeline
                assertThat(repliesPipeline.getOnErrorCalls()).isEmpty();
                assertThat(repliesPipeline.getOnSubscriptionCalls()).isEmpty();
                assertThat(repliesPipeline.getOnCompleteCalls().get()).isEqualTo(0);
                assertThat(repliesPipeline.getClientEndStreamCalls().get()).isEqualTo(0);
            }

            /// This test aims to assert that the [PublisherHandler] correctly
            /// handles received requests
            /// [PublisherHandler#onNext(PublishStreamRequestUnparsed)] happy
            /// path scenario. Here we stream a single complete valid block as items.
            /// We stream two requests. The first request's first item is the block
            /// header for the streamed block. The second request's last item is the
            /// block proof for the streamed block. We expect that when the
            /// [StreamPublisherManager] returns [BlockAction#ACCEPT] for
            /// the streamed block number, the items will be offered to the transfer
            /// queue.
            @Test
            @DisplayName(
                    "Test onNext() with valid two requests with a complete single block items streamed - happy path ACCEPT")
            void testOnNextConsecutiveRequestsHappyPathACCEPT() {
                // Create the block to stream, a single complete valid block
                final TestBlock block = TestBlockBuilder.generateBlockWithNumber(0L);
                final BlockItemUnparsed[] blockItems = block.asBlockItemUnparsedArray();
                final int mid = blockItems.length / 2;
                // Build the first request with the first half of the block items
                final BlockItemSetUnparsed blockItemSet1 = BlockItemSetUnparsed.newBuilder()
                        .blockItems(Arrays.copyOfRange(blockItems, 0, mid))
                        .build();
                final PublishStreamRequestUnparsed request1 = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(blockItemSet1)
                        .build();
                // Train the manager to return ACCEPT for the block number
                manager.setBlockActionForBlock(BlockAction.ACCEPT);
                // First call
                toTest.onNext(request1);
                // Assert items offered to the transfer queue
                final Deque<BlockItemSetUnparsed> block0Queue = manager.getQueueForBlock(block.number());
                assertThat(block0Queue).hasSize(1).containsExactly(blockItemSet1);
                // Build the second request with the second half of the block items
                final BlockItemSetUnparsed blockItemSet2 = BlockItemSetUnparsed.newBuilder()
                        .blockItems(Arrays.copyOfRange(blockItems, mid, blockItems.length))
                        .build();
                final PublishStreamRequestUnparsed request2 = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(blockItemSet2)
                        .build();
                // Second call
                toTest.onNext(request2);
                manager.setBlockActionForEndOfBlock(BlockAction.ACCEPT, block.number());
                endThisBlock(toTest, block.number());
                // Assert items offered to the transfer queue
                assertThat(block0Queue).hasSize(2).containsExactly(blockItemSet1, blockItemSet2);
            }

            /// This test aims to assert that the [PublisherHandler] correctly
            /// handles received requests
            /// [PublisherHandler#onNext(PublishStreamRequestUnparsed)] happy
            /// path scenario. Here we stream a single complete valid block as items.
            /// We stream two requests. The first request's first item is the block
            /// header for the streamed block. The second request's last item is the
            /// block proof for the streamed block. We expect that when the
            /// [StreamPublisherManager] returns [BlockAction#ACCEPT] for
            /// the streamed block number the metrics will be properly updated.
            @Test
            @DisplayName(
                    "Test onNext() with valid two requests with a complete single block metrics updated - happy path ACCEPT")
            void testOnNextConsecutiveRequestsMetricsHappyPathACCEPT() {
                // Create the block to stream, a single complete valid block
                final TestBlock block = TestBlockBuilder.generateBlockWithNumber(0L);
                final BlockItemUnparsed[] blockItems = block.asBlockItemUnparsedArray();
                final int mid = blockItems.length / 2;
                // Build the first request with the first half of the block items
                final BlockItemSetUnparsed blockItemSet1 = BlockItemSetUnparsed.newBuilder()
                        .blockItems(Arrays.copyOfRange(blockItems, 0, mid))
                        .build();
                final PublishStreamRequestUnparsed request1 = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(blockItemSet1)
                        .build();
                // Train the manager to return ACCEPT for the block number
                manager.setBlockActionForBlock(BlockAction.ACCEPT);
                // First call
                toTest.onNext(request1);
                // Assert metrics updated
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCK_ITEMS_RECEIVED))
                        .isEqualTo(mid);
                // Build the second request with the second half of the block items
                final BlockItemSetUnparsed blockItemSet2 = BlockItemSetUnparsed.newBuilder()
                        .blockItems(Arrays.copyOfRange(blockItems, mid, blockItems.length))
                        .build();
                final PublishStreamRequestUnparsed request2 = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(blockItemSet2)
                        .build();
                // Second call
                toTest.onNext(request2);
                manager.setBlockActionForEndOfBlock(BlockAction.ACCEPT, block.number());
                endThisBlock(toTest, block.number());
                // Assert metrics updated
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCK_ITEMS_RECEIVED))
                        .isEqualTo(blockItems.length);
                // Assert other metrics unchanged
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCKS_ACK_SENT))
                        .isZero();
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_STREAM_ERRORS))
                        .isZero();
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCKS_SKIPS_SENT))
                        .isZero();
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCKS_RESEND_SENT))
                        .isZero();
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCK_ENDOFSTREAM_SENT))
                        .isZero();
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCK_SEND_RESPONSE_FAILED))
                        .isZero();
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCK_ENDSTREAM_RECEIVED))
                        .isZero();
            }

            /// This test aims to assert that the {@link PublisherHandler} correctly handles a received request
            /// {@link PublisherHandler#onNext(PublishStreamRequestUnparsed)}.
            /// Here we stream a single complete valid block as items.
            /// The request's first item is the block header for the streamed block,
            /// and the last item is the block proof for the streamed block.
            /// We expect that when the {@link StreamPublisherManager} returns
            /// {@link BlockAction#SKIP} for the streamed block number no queue will be
            /// created for the streamed block.
            @Test
            @DisplayName(
                    "Test onNext() with valid request with a complete single block no queue created when action is SKIP")
            void testOnNextSKIP() {
                // Setup request to send, in this case a single complete valid block
                // as items, starting with header and ending with proof
                final TestBlock block = TestBlockBuilder.generateBlockWithNumber(0L);
                final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(block.asItemSetUnparsed())
                        .build();
                // Train the manager to return SKIP for the block number
                manager.setBlockActionForBlock(BlockAction.SKIP);
                // Assert queue not yet created
                assertThat(manager.getQueueForBlock(block.number())).isNull();
                // Call
                toTest.onNext(request);
                manager.setBlockActionForEndOfBlock(BlockAction.ACCEPT, block.number());
                endThisBlock(toTest, block.number());
                // Assert no queue created
                assertThat(manager.getQueueForBlock(block.number())).isNull();
            }

            /// This test aims to assert that the [PublisherHandler] correctly
            /// handles a received request
            /// [PublisherHandler#onNext(PublishStreamRequestUnparsed)].
            /// Here we stream a single complete valid block as items.
            /// The request's first item is the block header for the streamed block,
            /// and the last item is the block proof for the streamed block.
            /// We expect that when the [StreamPublisherManager] returns
            /// [BlockAction#SKIP] for the streamed block number the handler
            /// will reply with a
            /// [org.hiero.block.api.PublishStreamResponse.SkipBlock].
            @Test
            @DisplayName("Test onNext() with valid request with a complete single block response SkipBlock - SKIP")
            void testOnNextResponseSKIP() {
                // Setup request to send, in this case a single complete valid block
                // as items, starting with header and ending with proof
                final TestBlock block = TestBlockBuilder.generateBlockWithNumber(0L);
                final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(block.asItemSetUnparsed())
                        .build();
                // Train the manager to return SKIP for the block number
                manager.setBlockActionForBlock(BlockAction.SKIP);
                // Call
                toTest.onNext(request);
                // End the block, this should not cause failure.
                // Here we simulate network latency such that the block completes and ends
                // before skip gets back to publisher.
                manager.setBlockActionForEndOfBlock(BlockAction.ACCEPT, block.number());
                endThisBlock(toTest, block.number());
                // Assert single response is SkipBlock with block number same as streamed
                assertThat(repliesPipeline.getOnNextCalls())
                        .hasSize(1)
                        .first()
                        .returns(ResponseOneOfType.SKIP_BLOCK, responseKindExtractor)
                        .returns(block.number(), skipBlockNumberExtractor);
                // Assert no other responses sent
                assertThat(repliesPipeline.getOnErrorCalls()).isEmpty();
                assertThat(repliesPipeline.getOnSubscriptionCalls()).isEmpty();
                assertThat(repliesPipeline.getOnCompleteCalls().get()).isEqualTo(0);
                assertThat(repliesPipeline.getClientEndStreamCalls().get()).isEqualTo(0);
            }

            /// This test aims to assert that the [PublisherHandler] correctly
            /// handles a received request
            /// [PublisherHandler#onNext(PublishStreamRequestUnparsed)].
            /// Here we stream a single complete valid block as items.
            /// The request's first item is the block header for the streamed block,
            /// and the last item is the block proof for the streamed block.
            /// We expect that when the [StreamPublisherManager] returns
            /// [BlockAction#SKIP] for the streamed block number the metrics
            /// will be properly updated.
            @Test
            @DisplayName("Test onNext() with valid request with a complete single block metrics updated - SKIP")
            void testOnNextMetricsSKIP() {
                // Setup request to send, in this case a single complete valid block
                // as items, starting with header and ending with proof
                final TestBlock block = TestBlockBuilder.generateBlockWithNumber(0L);
                final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(block.asItemSetUnparsed())
                        .build();
                // Train the manager to return SKIP for the block number
                manager.setBlockActionForBlock(BlockAction.SKIP);
                // Call
                toTest.onNext(request);
                // No end block, should get SKIP _before_ ending the block
                // Assert metrics updated
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCKS_SKIPS_SENT))
                        .isEqualTo(1);
                // Assert other metrics unchanged
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCK_ITEMS_RECEIVED))
                        .isEqualTo(0);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCKS_ACK_SENT))
                        .isEqualTo(0);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_STREAM_ERRORS))
                        .isEqualTo(0);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCKS_RESEND_SENT))
                        .isEqualTo(0);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCK_ENDOFSTREAM_SENT))
                        .isEqualTo(0);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCK_SEND_RESPONSE_FAILED))
                        .isEqualTo(0);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCK_ENDSTREAM_RECEIVED))
                        .isEqualTo(0);
            }

            /// This test aims to assert that the {@link PublisherHandler} correctly handles a received request
            /// {@link PublisherHandler#onNext(PublishStreamRequestUnparsed)}.
            /// Here we stream a single complete valid block as items.
            /// The request's first item is the block header for the streamed block,
            /// and the last item is the block proof for the streamed block.
            /// We expect that when the {@link StreamPublisherManager} returns
            /// {@link BlockAction#RESEND} for the streamed block number no queue will be
            /// created for the streamed block.
            /// The Resend message is never expected to be received as an action for header/block.
            @Test
            @DisplayName(
                    "Test onNext() with valid request with a complete single block no queue created when action is RESEND")
            void testOnNextRESEND() {
                // Setup request to send, in this case a single complete valid block
                // as items, starting with header and ending with proof
                final TestBlock block = TestBlockBuilder.generateBlockWithNumber(0L);
                final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(block.asItemSetUnparsed())
                        .build();
                // Train the manager to return RESEND for the block number
                manager.setBlockActionForBlock(BlockAction.RESEND);
                // Assert queue not yet created
                assertThat(manager.getQueueForBlock(block.number())).isNull();
                // Call
                toTest.onNext(request);
                // Assert no queue created
                assertThat(manager.getQueueForBlock(block.number())).isNull();
            }

            /// This test aims to assert that the [PublisherHandler] correctly
            /// handles a received request
            /// [PublisherHandler#onNext(PublishStreamRequestUnparsed)].
            /// Here we stream a single complete valid block as items.
            /// The request's first item is the block header for the streamed block,
            /// and the last item is the block proof for the streamed block.
            /// We expect that when the [StreamPublisherManager] returns
            /// [BlockAction#RESEND] for the streamed block number the handler
            /// will reply with a
            /// [org.hiero.block.api.PublishStreamResponse.EndOfStream]
            /// with [org.hiero.block.api.PublishStreamResponse.EndOfStream.Code#ERROR]
            /// and the latest persisted block number.
            /// The Resend message is never expected to be received as an action for header/block.
            @Test
            @DisplayName("Test onNext() with valid request with a complete single block response ResendBlock - RESEND")
            void testOnNextResponseRESEND() {
                // Setup request to send, in this case a single complete valid block
                // as items, starting with header and ending with proof
                final TestBlock block = TestBlockBuilder.generateBlockWithNumber(0L);
                final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(block.asItemSetUnparsed())
                        .build();
                // Tell the manager to return ACCEPT for the block number
                manager.setBlockActionForBlock(BlockAction.ACCEPT);
                // Train the manager to return the expected latest block number
                final long latestBlockNumber = 10L; // Example latest block number
                manager.setLatestBlockNumber(latestBlockNumber);
                // Call
                toTest.onNext(request);
                // RESEND is only ever sent in response to end-of-block.
                manager.setBlockActionForEndOfBlock(BlockAction.RESEND, block.number());
                endThisBlock(toTest, block.number());
                // Assert sent responses
                assertThat(repliesPipeline.getOnNextCalls())
                        .hasSize(1)
                        .first()
                        .returns(ResponseOneOfType.RESEND_BLOCK, responseKindExtractor)
                        .returns(block.number(), resendBlockNumberExtractor);
                assertThat(repliesPipeline.getOnCompleteCalls().get()).isEqualTo(0);
                // Assert no other responses sent
                assertThat(repliesPipeline.getOnErrorCalls()).isEmpty();
                assertThat(repliesPipeline.getOnSubscriptionCalls()).isEmpty();
                assertThat(repliesPipeline.getClientEndStreamCalls().get()).isEqualTo(0);
            }

            /// This test aims to assert that the [PublisherHandler] correctly
            /// handles a received request
            /// [PublisherHandler#onNext(PublishStreamRequestUnparsed)].
            /// Here we stream a single complete valid block as items.
            /// The request's first item is the block header for the streamed block,
            /// and the last item is the block proof for the streamed block.
            /// We expect that when the [StreamPublisherManager] returns
            /// [BlockAction#RESEND] for the streamed block number the metrics
            /// will be properly updated. We expect that the connection will be closed with an error.
            /// The Resend message is never expected to be received as an action for header/block.
            @Test
            @DisplayName(
                    "Test onNext() with valid request with a complete single block metrics updated ResendBlock - RESEND")
            void testOnNextMetricsRESEND() {
                // Setup request to send, in this case a single complete valid block
                // as items, starting with header and ending with proof
                final TestBlock block = TestBlockBuilder.generateBlockWithNumber(0L);
                final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(block.asItemSetUnparsed())
                        .build();
                // Train the manager to return RESEND for the block number
                manager.setBlockActionForBlock(BlockAction.RESEND);
                // Train the manager to return the expected latest block number
                final long latestBlockNumber = 10L; // Example latest block number
                manager.setLatestBlockNumber(latestBlockNumber);
                // Call
                toTest.onNext(request);
                // Assert metrics updated
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_STREAM_ERRORS))
                        .isEqualTo(1);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCK_ENDOFSTREAM_SENT))
                        .isEqualTo(1);
                // Assert other metrics unchanged
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCK_ITEMS_RECEIVED))
                        .isEqualTo(0);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCKS_ACK_SENT))
                        .isEqualTo(0);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCKS_RESEND_SENT))
                        .isEqualTo(0);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCKS_SKIPS_SENT))
                        .isEqualTo(0);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCK_SEND_RESPONSE_FAILED))
                        .isEqualTo(0);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCK_ENDSTREAM_RECEIVED))
                        .isEqualTo(0);
            }

            /// This test aims to assert that the [PublisherHandler] correctly
            /// handles a received request
            /// [PublisherHandler#onNext(PublishStreamRequestUnparsed)].
            /// Here we stream a single complete valid block as items.
            /// The request's first item is the block header for the streamed block,
            /// and the last item is the block proof for the streamed block.
            /// We expect that when the [StreamPublisherManager] returns
            /// [BlockAction#END_DUPLICATE] for the streamed block number no
            /// items will be offered to the transfer queue.
            @Test
            @DisplayName(
                    "Test onNext() with valid request with a complete single block no queue created when action is END_DUPLICATE")
            void testOnNextDUPLICATE() {
                // Setup request to send, in this case a single complete valid block
                // as items, starting with header and ending with proof
                final TestBlock block = TestBlockBuilder.generateBlockWithNumber(0L);
                final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(block.asItemSetUnparsed())
                        .build();
                // Train the manager to return END_DUPLICATE for the block number
                manager.setBlockActionForBlock(BlockAction.END_DUPLICATE);
                // Assert queue not yet created
                assertThat(manager.getQueueForBlock(block.number())).isNull();
                // Call
                toTest.onNext(request);
                // Assert no queue created
                assertThat(manager.getQueueForBlock(block.number())).isNull();
            }

            /// This test aims to assert that the [PublisherHandler] correctly
            /// handles a received request
            /// [PublisherHandler#onNext(PublishStreamRequestUnparsed)].
            /// Here we stream a single complete valid block as items.
            /// The request's first item is the block header for the streamed block,
            /// and the last item is the block proof for the streamed block.
            /// We expect that when the [StreamPublisherManager] returns
            /// [BlockAction#END_DUPLICATE] for the streamed block number the
            /// handler will reply with a
            /// [org.hiero.block.api.PublishStreamResponse.EndOfStream]
            /// with [org.hiero.block.api.PublishStreamResponse.EndOfStream.Code#DUPLICATE_BLOCK].
            @Test
            @DisplayName(
                    "Test onNext() with valid request with a complete single block response EndOfStream, Code DUPLICATE_BLOCK - EndOfStream, Code DUPLICATE_BLOCK")
            void testOnNextResponseDUPLICATE() {
                // Setup request to send, in this case a single complete valid block
                // as items, starting with header and ending with proof
                final TestBlock block = TestBlockBuilder.generateBlockWithNumber(0L);
                final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(block.asItemSetUnparsed())
                        .build();
                // Train the manager to return END_DUPLICATE for the block number
                manager.setBlockActionForBlock(BlockAction.END_DUPLICATE);
                // Train the manager to return the expected latest block number
                final long expectedLatestBlockNumber = 10L; // Example latest block number
                manager.setLatestBlockNumber(expectedLatestBlockNumber);
                // Call
                toTest.onNext(request);
                // Assert single response is DUPLICATE_BLOCK with block number latest known and onComplete is called
                // (shutdown)
                assertThat(repliesPipeline.getOnNextCalls())
                        .hasSize(1)
                        .first()
                        .returns(ResponseOneOfType.END_STREAM, responseKindExtractor)
                        .returns(Code.DUPLICATE_BLOCK, endStreamResponseCodeExtractor)
                        .returns(expectedLatestBlockNumber, endStreamBlockNumberExtractor);
                assertThat(repliesPipeline.getOnCompleteCalls().get()).isEqualTo(1);
                // Assert no other responses sent
                assertThat(repliesPipeline.getOnErrorCalls()).isEmpty();
                assertThat(repliesPipeline.getOnSubscriptionCalls()).isEmpty();
                assertThat(repliesPipeline.getClientEndStreamCalls().get()).isEqualTo(0);
            }

            /// This test aims to assert that the [PublisherHandler] correctly
            /// handles a received request
            /// [PublisherHandler#onNext(PublishStreamRequestUnparsed)].
            /// Here we stream a single complete valid block as items.
            /// The request's first item is the block header for the streamed block,
            /// and the last item is the block proof for the streamed block.
            /// We expect that when the [StreamPublisherManager] returns
            /// [BlockAction#END_DUPLICATE] for the streamed block number the
            /// metrics will be properly updated.
            @Test
            @DisplayName(
                    "Test onNext() with valid request with a complete single block metrics updated EndOfStream, Code DUPLICATE_BLOCK - EndOfStream, Code DUPLICATE_BLOCK")
            void testOnNextMetricsDUPLICATE() {
                // Setup request to send, in this case a single complete valid block
                // as items, starting with header and ending with proof
                final TestBlock block = TestBlockBuilder.generateBlockWithNumber(0L);
                final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(block.asItemSetUnparsed())
                        .build();
                // Train the manager to return END_DUPLICATE for the block number
                manager.setBlockActionForBlock(BlockAction.END_DUPLICATE);
                // Train the manager to return the expected latest block number
                final long expectedLatestBlockNumber = 10L; // Example latest block number
                manager.setLatestBlockNumber(expectedLatestBlockNumber);
                // Call
                toTest.onNext(request);
                // Assert metrics updated
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCK_ENDOFSTREAM_SENT))
                        .isEqualTo(1);
                // Assert other metrics unchanged
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCK_ITEMS_RECEIVED))
                        .isEqualTo(0);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCKS_ACK_SENT))
                        .isEqualTo(0);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_STREAM_ERRORS))
                        .isEqualTo(0);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCKS_SKIPS_SENT))
                        .isEqualTo(0);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCKS_RESEND_SENT))
                        .isEqualTo(0);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCK_SEND_RESPONSE_FAILED))
                        .isEqualTo(0);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCK_ENDSTREAM_RECEIVED))
                        .isEqualTo(0);
            }

            /// This test aims to assert that the {@link PublisherHandler} correctly handles a received request
            /// {@link PublisherHandler#onNext(PublishStreamRequestUnparsed)}.
            /// Here we stream a single complete valid block as items.
            /// The request's first item is the block header for the streamed block,
            /// and the last item is the block proof for the streamed block.
            /// We expect that when the {@link StreamPublisherManager} returns
            /// {@link BlockAction#SEND_BEHIND} for the streamed block number no queue
            /// will be created for the streamed block.
            @Test
            @DisplayName(
                    "Test onNext() with valid request with a complete single block no queue created when action is SEND_BEHIND")
            void testOnNextBEHIND() {
                // Setup request to send, in this case a single complete valid block
                // as items, starting with header and ending with proof
                final TestBlock block = TestBlockBuilder.generateBlockWithNumber(0L);
                final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(block.asItemSetUnparsed())
                        .build();
                // Train the manager to return SEND_BEHIND for the block number
                manager.setBlockActionForBlock(BlockAction.SEND_BEHIND);
                // Assert queue not yet created
                assertThat(manager.getQueueForBlock(block.number())).isNull();
                // Call
                toTest.onNext(request);
                // Assert no queue created
                assertThat(manager.getQueueForBlock(block.number())).isNull();
            }

            /// This test aims to assert that the [PublisherHandler] correctly
            /// handles a received request
            /// [PublisherHandler#onNext(PublishStreamRequestUnparsed)].
            /// Here we stream a single complete valid block as items.
            /// The request's first item is the block header for the streamed block,
            /// and the last item is the block proof for the streamed block.
            /// We expect that when the [StreamPublisherManager] returns
            /// [BlockAction#SEND_BEHIND] for the streamed block number the
            /// handler will reply with a
            /// [org.hiero.block.api.PublishStreamResponse.BehindPublisher].
            @Test
            @DisplayName("Test onNext() with valid request with a complete single block response BehindPublisher")
            void testOnNextResponseBEHIND() {
                // Setup request to send, in this case a single complete valid block
                // as items, starting with header and ending with proof
                final TestBlock block = TestBlockBuilder.generateBlockWithNumber(0L);
                final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(block.asItemSetUnparsed())
                        .build();
                // Train the manager to return SEND_BEHIND for the block number
                manager.setBlockActionForBlock(BlockAction.SEND_BEHIND);
                // Train the manager to return the expected latest block number
                final long expectedLatestBlockNumber = 10L; // Example latest block number
                manager.setLatestBlockNumber(expectedLatestBlockNumber);
                // Call
                toTest.onNext(request);
                // test that we can still end a block when behind is expected
                manager.setBlockActionForEndOfBlock(BlockAction.ACCEPT, block.number());
                endThisBlock(toTest, block.number());
                // Assert single response is BEHIND with block number same as latest known
                assertThat(repliesPipeline.getOnNextCalls())
                        .hasSize(1)
                        .first()
                        .returns(ResponseOneOfType.NODE_BEHIND_PUBLISHER, responseKindExtractor)
                        .returns(expectedLatestBlockNumber, nodeBehindBlockNumberExtractor);
            }

            /// This test aims to assert that the [PublisherHandler] correctly
            /// handles a received request
            /// [PublisherHandler#onNext(PublishStreamRequestUnparsed)].
            /// Here we stream a single complete valid block as items.
            /// The request's first item is the block header for the streamed block,
            /// and the last item is the block proof for the streamed block.
            /// We expect that when the [StreamPublisherManager] returns
            /// [BlockAction#SEND_BEHIND] for the streamed block number the
            /// metrics will be properly updated.
            @Test
            @DisplayName(
                    "Test onNext() with valid request with a complete single block metrics updated returns BehindPublisher")
            void testOnNextMetricsBEHIND() {
                // Setup request to send, in this case a single complete valid block
                // as items, starting with header and ending with proof
                final TestBlock block = TestBlockBuilder.generateBlockWithNumber(0L);
                final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(block.asItemSetUnparsed())
                        .build();
                // Train the manager to return END_BEHIND for the block number
                manager.setBlockActionForBlock(BlockAction.SEND_BEHIND);
                // Train the manager to return the expected latest block number
                final long latestBlockNumber = 10L; // Example latest block number
                manager.setLatestBlockNumber(latestBlockNumber);
                // Call
                toTest.onNext(request);
                // Assert metrics updated
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCK_ENDOFSTREAM_SENT))
                        .isEqualTo(0);
                // Assert other metrics unchanged
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCK_ITEMS_RECEIVED))
                        .isEqualTo(0);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCKS_ACK_SENT))
                        .isEqualTo(0);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_STREAM_ERRORS))
                        .isEqualTo(0);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCK_NODE_BEHIND_SENT))
                        .isEqualTo(1);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCKS_SKIPS_SENT))
                        .isEqualTo(0);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCKS_RESEND_SENT))
                        .isEqualTo(0);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCK_SEND_RESPONSE_FAILED))
                        .isEqualTo(0);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCK_ENDSTREAM_RECEIVED))
                        .isEqualTo(0);
            }

            /// This test aims to assert that the {@link PublisherHandler} correctly handles a received request
            /// {@link PublisherHandler#onNext(PublishStreamRequestUnparsed)}.
            /// Here we stream a single complete valid block as items.
            /// The request's first item is the block header for the streamed block,
            /// and the last item is the block proof for the streamed block.
            /// We expect that when the {@link StreamPublisherManager} returns
            /// {@link BlockAction#END_ERROR} for the streamed block number no queue is created
            /// for the streamed block.
            @Test
            @DisplayName(
                    "Test onNext() with valid request with a complete single block no queue created when action is END_ERROR")
            void testOnNextERROR() {
                // Setup request to send, in this case a single complete valid block
                // as items, starting with header and ending with proof
                final TestBlock block = TestBlockBuilder.generateBlockWithNumber(0L);
                final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(block.asItemSetUnparsed())
                        .build();
                // Train the manager to return END_ERROR for the block number
                manager.setBlockActionForBlock(BlockAction.END_ERROR);
                // Assert queue not yet created
                assertThat(manager.getQueueForBlock(block.number())).isNull();
                // Call
                toTest.onNext(request);
                // Assert no queue created
                assertThat(manager.getQueueForBlock(block.number())).isNull();
            }

            /// This test aims to assert that the [PublisherHandler] correctly
            /// handles a received request
            /// [PublisherHandler#onNext(PublishStreamRequestUnparsed)].
            /// Here we stream a single complete valid block as items.
            /// The request's first item is the block header for the streamed block,
            /// and the last item is the block proof for the streamed block.
            /// We expect that when the [StreamPublisherManager] returns
            /// [BlockAction#END_ERROR] for the streamed block number the
            /// handler will reply with a
            /// [org.hiero.block.api.PublishStreamResponse.EndOfStream]
            /// with [org.hiero.block.api.PublishStreamResponse.EndOfStream.Code#ERROR].
            @Test
            @DisplayName(
                    "Test onNext() with valid request with a complete single block response EndOfStream, Code ERROR - EndOfStream, Code ERROR")
            void testOnNextResponseERROR() {
                // Setup request to send, in this case a single complete valid block
                // as items, starting with header and ending with proof
                final TestBlock block = TestBlockBuilder.generateBlockWithNumber(0L);
                final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(block.asItemSetUnparsed())
                        .build();
                // Train the manager to return END_ERROR for the block number
                manager.setBlockActionForBlock(BlockAction.END_ERROR);
                // Train the manager to return the expected latest block number
                final long expectedLatestBlockNumber = 10L; // Example latest block number
                manager.setLatestBlockNumber(expectedLatestBlockNumber);
                // Call
                toTest.onNext(request);
                // Assert single response is ERROR and onComplete is called (shutdown)
                assertThat(repliesPipeline.getOnNextCalls())
                        .hasSize(1)
                        .first()
                        .returns(ResponseOneOfType.END_STREAM, responseKindExtractor)
                        .returns(Code.ERROR, endStreamResponseCodeExtractor)
                        .returns(expectedLatestBlockNumber, endStreamBlockNumberExtractor);
                assertThat(repliesPipeline.getOnCompleteCalls().get()).isEqualTo(1);
                // Assert no other responses sent
                assertThat(repliesPipeline.getOnErrorCalls()).isEmpty();
                assertThat(repliesPipeline.getOnSubscriptionCalls()).isEmpty();
                assertThat(repliesPipeline.getClientEndStreamCalls().get()).isEqualTo(0);
            }

            /// This test aims to assert that the [PublisherHandler] correctly
            /// handles a received request
            /// [PublisherHandler#onNext(PublishStreamRequestUnparsed)].
            /// Here we stream a single complete valid block as items.
            /// The request's first item is the block header for the streamed block,
            /// and the last item is the block proof for the streamed block.
            /// We expect that when the [StreamPublisherManager] returns
            /// [BlockAction#END_ERROR] for the streamed block number the
            /// metrics will be properly updated.
            @Test
            @DisplayName(
                    "Test onNext() with valid request with a complete single block metrics updated EndOfStream, Code ERROR - EndOfStream, Code ERROR")
            void testOnNextMetricsERROR() {
                // Setup request to send, in this case a single complete valid block
                // as items, starting with header and ending with proof
                final TestBlock block = TestBlockBuilder.generateBlockWithNumber(0L);
                final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(block.asItemSetUnparsed())
                        .build();
                // Train the manager to return END_ERROR for the block number
                manager.setBlockActionForBlock(BlockAction.END_ERROR);
                // Train the manager to return the expected latest block number
                final long expectedLatestBlockNumber = 10L; // Example latest block number
                manager.setLatestBlockNumber(expectedLatestBlockNumber);
                // Call
                toTest.onNext(request);
                // Assert metrics updated
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCK_ENDOFSTREAM_SENT))
                        .isEqualTo(1);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_STREAM_ERRORS))
                        .isEqualTo(1);
                // Assert other metrics unchanged
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCK_ITEMS_RECEIVED))
                        .isEqualTo(0);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCKS_ACK_SENT))
                        .isEqualTo(0);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCKS_SKIPS_SENT))
                        .isEqualTo(0);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCKS_RESEND_SENT))
                        .isEqualTo(0);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCK_SEND_RESPONSE_FAILED))
                        .isEqualTo(0);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCK_ENDSTREAM_RECEIVED))
                        .isEqualTo(0);
            }

            /// This test aims to assert that the [PublisherHandler] correctly
            /// handles a received invalid request
            /// [PublisherHandler#onNext(PublishStreamRequestUnparsed)] when
            /// block header is expected, but it is not present. Here, the handler
            /// is in a state where it expects a block header to be the first item
            /// in next request, but the request does not contain a block header as
            /// first item. We expect that the handler will simply return w/o
            /// creating any queues.
            @Test
            @DisplayName("Test onNext() with invalid request - no header, but expected - no items are propagated")
            void testOnNextReturnOnInvalidRequest() {
                // Setup request to send, in this case a single complete valid block
                // as items, starting with header and ending with proof
                final TestBlock block = TestBlockBuilder.generateBlockWithNumber(0L);
                final BlockItemUnparsed[] blockItems = block.asBlockItemUnparsedArray();
                // Remove the first item, which is the block header
                final BlockItemUnparsed[] blockItemsToSend = Arrays.copyOfRange(blockItems, 1, blockItems.length);
                final BlockItemSetUnparsed blockItemSet = BlockItemSetUnparsed.newBuilder()
                        .blockItems(blockItemsToSend)
                        .build();
                final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(blockItemSet)
                        .build();
                // Call
                toTest.onNext(request);
                // Assert no items offered to the transfer queue
                assertThat(manager.getQueueByBlockMap()).isEmpty();
            }

            /// This test aims to assert that the [PublisherHandler] correctly
            /// handles a received invalid request
            /// [PublisherHandler#onNext(PublishStreamRequestUnparsed)] when
            /// block header is expected, but it is not present. Here, the handler
            /// is in a state where it expects a block header to be the first item
            /// in next request, but the request does not contain a block header as
            /// first item. We expect that the handler will simply return w/o
            /// making any responses to the reply pipeline.
            @Test
            @DisplayName("Test onNext() with invalid request - no header, but expected - no responses sent")
            void testOnNextReturnOnInvalidRequestNoResponse() {
                // Setup request to send, in this case a single complete valid block
                // as items, starting with header and ending with proof
                final TestBlock block = TestBlockBuilder.generateBlockWithNumber(0L);
                final BlockItemUnparsed[] blockItems = block.asBlockItemUnparsedArray();
                // Remove the first item, which is the block header
                final BlockItemUnparsed[] blockItemsToSend = Arrays.copyOfRange(blockItems, 1, blockItems.length);
                final BlockItemSetUnparsed blockItemSet = BlockItemSetUnparsed.newBuilder()
                        .blockItems(blockItemsToSend)
                        .build();
                final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(blockItemSet)
                        .build();
                // Call
                toTest.onNext(request);
                // Assert no responses sent to the reply pipeline
                assertThat(repliesPipeline.getOnNextCalls()).isEmpty();
                assertThat(repliesPipeline.getOnErrorCalls()).isEmpty();
                assertThat(repliesPipeline.getOnSubscriptionCalls()).isEmpty();
                assertThat(repliesPipeline.getOnCompleteCalls().get()).isEqualTo(0);
                assertThat(repliesPipeline.getClientEndStreamCalls().get()).isEqualTo(0);
            }

            /// This test aims to assert that the [PublisherHandler] correctly
            /// handles a received invalid request
            /// [PublisherHandler#onNext(PublishStreamRequestUnparsed)] when
            /// block header is expected, but it is not present. Here, the handler
            /// is in a state where it expects a block header to be the first item
            /// in next request, but the request does not contain a block header as
            /// first item. We expect that the handler will simply return w/o
            /// updating any metrics.
            @Test
            @DisplayName("Test onNext() with invalid request - no header, but expected - no metrics updated")
            void testOnNextReturnOnInvalidRequestMetrics() {
                // Setup request to send, in this case a single complete valid block
                // as items, starting with header and ending with proof
                final TestBlock block = TestBlockBuilder.generateBlockWithNumber(0L);
                final BlockItemUnparsed[] blockItems = block.asBlockItemUnparsedArray();
                // Remove the first item, which is the block header
                final BlockItemUnparsed[] blockItemsToSend = Arrays.copyOfRange(blockItems, 1, blockItems.length);
                final BlockItemSetUnparsed blockItemSet = BlockItemSetUnparsed.newBuilder()
                        .blockItems(blockItemsToSend)
                        .build();
                final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(blockItemSet)
                        .build();
                // Call
                toTest.onNext(request);
                // Assert no metrics updated
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCK_ITEMS_RECEIVED))
                        .isEqualTo(0);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCKS_ACK_SENT))
                        .isEqualTo(0);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_STREAM_ERRORS))
                        .isEqualTo(0);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCKS_SKIPS_SENT))
                        .isEqualTo(0);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCKS_RESEND_SENT))
                        .isEqualTo(0);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCK_ENDOFSTREAM_SENT))
                        .isEqualTo(0);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCK_SEND_RESPONSE_FAILED))
                        .isEqualTo(0);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCK_ENDSTREAM_RECEIVED))
                        .isEqualTo(0);
            }

            /// This test aims to assert that the [PublisherHandler] correctly
            /// handles a received invalid request
            /// [PublisherHandler#onNext(PublishStreamRequestUnparsed)] when
            /// block header is expected, but it is not present. Here, the handler
            /// is in a state where it expects a block header to be the first item
            /// in next request, but the request does not contain a block header as
            /// first item. We expect that the handler will simply return w/o
            /// creating a queue for a block. Subsequent requests,
            /// if valid, will be processed normally and items will be propagated
            /// to the transfer queue.
            @Test
            @DisplayName(
                    "Test onNext() with invalid request - no header, but expected - subsequent valid request items propagated")
            void testOnNextInvalidRequestValidSubsequentRequest() {
                // Setup request to send, in this case a single complete valid block
                // as items, starting with header and ending with proof
                final TestBlock block = TestBlockBuilder.generateBlockWithNumber(0L);
                final BlockItemUnparsed[] blockItems = block.asBlockItemUnparsedArray();
                // Remove the first item, which is the block header
                final BlockItemUnparsed[] blockItemsToSend = Arrays.copyOfRange(blockItems, 1, blockItems.length);
                final BlockItemSetUnparsed blockItemSet = BlockItemSetUnparsed.newBuilder()
                        .blockItems(blockItemsToSend)
                        .build();
                final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(blockItemSet)
                        .build();
                // Call
                toTest.onNext(request);
                // Assert no items offered to the transfer queue
                assertThat(manager.getQueueByBlockMap()).isEmpty();
                // Now send a valid request, which contains a block header
                final BlockItemSetUnparsed validBlockItemSet =
                        BlockItemSetUnparsed.newBuilder().blockItems(blockItems).build();
                final PublishStreamRequestUnparsed validRequest = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(validBlockItemSet)
                        .build();
                // Train the manager to expect return ACCEPT
                manager.setBlockActionForBlock(BlockAction.ACCEPT);
                // Call with valid request
                toTest.onNext(validRequest);
                manager.setBlockActionForEndOfBlock(BlockAction.ACCEPT, block.number());
                endThisBlock(toTest, block.number());
                // Assert items were propagated to the transfer queue
                assertThat(manager.getQueueForBlock(block.number())).hasSize(1).containsExactly(validBlockItemSet);
            }

            /// This test aims to assert that the [PublisherHandler] correctly
            /// handles a received invalid request
            /// [PublisherHandler#onNext(PublishStreamRequestUnparsed)] when
            /// block header is expected, but it is not present. Here, the handler
            /// is in a state where it expects a block header to be the first item
            /// in next request, but the request does not contain a block header as
            /// first item. We expect that the handler will simply return w/o
            /// propagating any items to the transfer queue. Subsequent requests,
            /// if valid, will be processed normally and acknowledgements will be
            /// sent.
            @Test
            @DisplayName(
                    "Test onNext() with invalid request - no header, but expected - subsequent valid request sends acknowledgement")
            void testOnNextInvalidRequestValidSubsequentRequestResponse() {
                // Setup request to send, in this case a single complete valid block
                // as items, starting with header and ending with proof
                final TestBlock block = TestBlockBuilder.generateBlockWithNumber(0L);
                final BlockItemUnparsed[] blockItems = block.asBlockItemUnparsedArray();
                // Remove the first item, which is the block header
                final BlockItemUnparsed[] blockItemsToSend = Arrays.copyOfRange(blockItems, 1, blockItems.length);
                final BlockItemSetUnparsed blockItemSet = BlockItemSetUnparsed.newBuilder()
                        .blockItems(blockItemsToSend)
                        .build();
                final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(blockItemSet)
                        .build();
                // Call
                toTest.onNext(request);
                // Assert no responses sent to the reply pipeline
                assertThat(repliesPipeline.getOnNextCalls()).isEmpty();
                // Now send a valid request, which contains a block header
                final BlockItemSetUnparsed validBlockItemSet =
                        BlockItemSetUnparsed.newBuilder().blockItems(blockItems).build();
                final PublishStreamRequestUnparsed validRequest = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(validBlockItemSet)
                        .build();
                // Train the manager to expect return ACCEPT
                manager.setBlockActionForBlock(BlockAction.ACCEPT);
                // Call with valid request
                toTest.onNext(validRequest);
                // Send a PersistedNotification for the streamed block number
                manager.handlePersisted(new PersistedNotification(block.number(), true, 0, BlockSource.PUBLISHER));
                // Assert that an acknowledgement was sent for the valid request
                assertThat(repliesPipeline.getOnNextCalls())
                        .hasSize(1)
                        .first()
                        .returns(ResponseOneOfType.ACKNOWLEDGEMENT, responseKindExtractor)
                        .returns(block.number(), acknowledgementBlockNumberExtractor);
                // Assert no replies sent to the pipeline
                assertThat(repliesPipeline.getOnErrorCalls()).isEmpty();
                assertThat(repliesPipeline.getOnSubscriptionCalls()).isEmpty();
                assertThat(repliesPipeline.getOnCompleteCalls().get()).isEqualTo(0);
                assertThat(repliesPipeline.getClientEndStreamCalls().get()).isEqualTo(0);
            }

            /// This test aims to assert that the [PublisherHandler] correctly
            /// handles a received invalid request
            /// [PublisherHandler#onNext(PublishStreamRequestUnparsed)] when
            /// block header is expected, but it is not present. Here, the handler
            /// is in a state where it expects a block header to be the first item
            /// in next request, but the request does not contain a block header as
            /// first item. We expect that the handler will simply return w/o
            /// propagating any items to the transfer queue. Subsequent requests,
            /// if valid, will be processed normally and metrics will be updated.
            @Test
            @DisplayName(
                    "Test onNext() with invalid request - no header, but expected - subsequent valid request updates metrics")
            void testOnNextInvalidRequestValidSubsequentRequestMetrics() {
                // Setup request to send, in this case a single complete valid block
                // as items, starting with header and ending with proof
                final TestBlock block = TestBlockBuilder.generateBlockWithNumber(0L);
                final BlockItemUnparsed[] blockItems = block.asBlockItemUnparsedArray();
                // Remove the first item, which is the block header
                final BlockItemUnparsed[] blockItemsToSend = Arrays.copyOfRange(blockItems, 1, blockItems.length);
                final BlockItemSetUnparsed blockItemSet = BlockItemSetUnparsed.newBuilder()
                        .blockItems(blockItemsToSend)
                        .build();
                final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(blockItemSet)
                        .build();
                // Call
                toTest.onNext(request);
                // Assert no metrics updated
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCK_ITEMS_RECEIVED))
                        .isEqualTo(0);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCKS_ACK_SENT))
                        .isEqualTo(0);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_STREAM_ERRORS))
                        .isEqualTo(0);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCKS_SKIPS_SENT))
                        .isEqualTo(0);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCKS_RESEND_SENT))
                        .isEqualTo(0);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCK_ENDOFSTREAM_SENT))
                        .isEqualTo(0);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCK_SEND_RESPONSE_FAILED))
                        .isEqualTo(0);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCK_ENDSTREAM_RECEIVED))
                        .isEqualTo(0);
                // Now send a valid request, which contains a block header
                final BlockItemSetUnparsed validBlockItemSet =
                        BlockItemSetUnparsed.newBuilder().blockItems(blockItems).build();
                final PublishStreamRequestUnparsed validRequest = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(validBlockItemSet)
                        .build();
                // Train the manager to expect return ACCEPT
                manager.setBlockActionForBlock(BlockAction.ACCEPT);
                // Call with valid request
                toTest.onNext(validRequest);
                // Send a PersistedNotification for the streamed block number
                manager.handlePersisted(new PersistedNotification(block.number(), true, 0, BlockSource.PUBLISHER));
                // Assert live items received updated and acknowledgement sent updated
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCK_ITEMS_RECEIVED))
                        .isEqualTo(blockItems.length);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCKS_ACK_SENT))
                        .isEqualTo(1);
                // Assert other metrics unchanged
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_STREAM_ERRORS))
                        .isEqualTo(0);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCKS_SKIPS_SENT))
                        .isEqualTo(0);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCKS_RESEND_SENT))
                        .isEqualTo(0);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCK_ENDOFSTREAM_SENT))
                        .isEqualTo(0);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCK_SEND_RESPONSE_FAILED))
                        .isEqualTo(0);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCK_ENDSTREAM_RECEIVED))
                        .isEqualTo(0);
            }

            /// This test aims to assert that the [PublisherHandler] correctly
            /// handles a received request
            /// [PublisherHandler#onNext(PublishStreamRequestUnparsed)] when
            /// premature header is received, i.e. the request contains a block
            /// header, but the current block is not yet streamed in full. A header
            /// indicates that the current requests initiates the streaming of a new
            /// block. When a header is sent prematurely, the handler is expected to
            /// not propagate any items to the transfer queue.
            @Test
            @DisplayName(
                    "Test onNext() with premature header received, no items propagated - EndOfStream, Code INVALID_REQUEST")
            void testOnNextPrematureHeader() {
                // Setup request to send, in this case a single complete valid block
                // as items, starting with header and ending with proof
                final TestBlock block = TestBlockBuilder.generateBlockWithNumber(0L);
                final BlockItemUnparsed[] blockItems = block.asBlockItemUnparsedArray();
                // Send the first half of the items, header included
                final BlockItemUnparsed[] blockItemsToSend = Arrays.copyOfRange(blockItems, 0, blockItems.length / 2);
                final BlockItemSetUnparsed blockItemSet = BlockItemSetUnparsed.newBuilder()
                        .blockItems(blockItemsToSend)
                        .build();
                final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(blockItemSet)
                        .build();
                // Train the manager to expect return ACCEPT
                manager.setBlockActionForBlock(BlockAction.ACCEPT);
                // Call, we expect that everything is well when we've first sent this valid request
                toTest.onNext(request);
                final Deque<BlockItemSetUnparsed> block0Queue = manager.getQueueForBlock(block.number());
                // Assert items were propagated (first request was valid and passed)
                assertThat(block0Queue).hasSize(1).containsExactly(blockItemSet);
                // Call again, now we expect that the handler will not propagate any items
                // because the request would be invalid, we are sending a header, but the
                // current block is not yet streamed in full as we sent only half of the items.
                toTest.onNext(request);
                // Assert that queue is unchanged (1st request was valid)
                assertThat(block0Queue).hasSize(1).containsExactly(blockItemSet);
            }

            /// This test aims to assert that the [PublisherHandler] correctly
            /// handles a received request
            /// [PublisherHandler#onNext(PublishStreamRequestUnparsed)] when a
            /// premature header is received, i.e. the request contains a block
            /// header, but the current block is not yet streamed in full. A header
            /// indicates that the current request initiates the streaming of a new
            /// block. When a header is sent prematurely, the handler is expected to
            /// start streaming the new block.
            @Test
            @DisplayName("Test onNext() with premature header received, start streaming new block")
            void testOnNextPrematureHeaderNewBlockStreamed() {
                // Setup request to send, in this case a single complete valid block
                // as items, starting with header and ending with proof
                final TestBlock block1 = TestBlockBuilder.generateBlockWithNumber(1L);
                final BlockItemSetUnparsed block1ItemSet = block1.asItemSetUnparsed();
                final PublishStreamRequestUnparsed requestBlock1 = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(block1ItemSet)
                        .build();
                // Train the manager to expect return ACCEPT
                manager.setBlockActionForBlock(BlockAction.ACCEPT);
                // Call; we expect that everything is well when we've first sent this valid request
                // We do not end the block here, it must remain active and currently streaming
                toTest.onNext(requestBlock1);
                // Assert items were propagated (the first request was valid and passed)
                assertThat(manager.getQueueForBlock(block1.number())).hasSize(1).containsExactly(block1ItemSet);
                // Now start streaming another block, say block 0
                final TestBlock block0 = TestBlockBuilder.generateBlockWithNumber(0L);
                final BlockItemSetUnparsed block0ItemSet = block0.asItemSetUnparsed();
                final PublishStreamRequestUnparsed requestBlock0 = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(block0ItemSet)
                        .build();
                toTest.onNext(requestBlock0);
                // Assert items were propagated
                assertThat(manager.getQueueForBlock(block0.number())).hasSize(1).containsExactly(block0ItemSet);
            }

            /// This test aims to assert that the [PublisherHandler] correctly
            /// handles a received request
            /// [PublisherHandler#onNext(PublishStreamRequestUnparsed)] when a
            /// premature header is received, i.e. the request contains a block
            /// header, but the current block is not yet streamed in full. A header
            /// indicates that the current request initiates the streaming of a new
            /// block. When a header is sent prematurely, the handler is expected to
            /// end the current one mid-block.
            @Test
            @DisplayName("Test onNext() with premature header received, end current block mid-block")
            void testOnNextPrematureHeaderNewBlockEndCurrentOne() {
                // Setup request to send, in this case a single complete valid block
                // as items, starting with header and ending with proof
                final TestBlock block1 = TestBlockBuilder.generateBlockWithNumber(1L);
                final BlockItemSetUnparsed block1ItemSet = block1.asItemSetUnparsed();
                final PublishStreamRequestUnparsed requestBlock1 = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(block1ItemSet)
                        .build();
                // Train the manager to expect return ACCEPT
                manager.setBlockActionForBlock(BlockAction.ACCEPT);
                // Call; we expect that everything is well when we've first sent this valid request
                // We do not end the block here, it must remain active and currently streaming
                toTest.onNext(requestBlock1);
                // Assert that no blocks have ended mid-block yet
                assertThat(manager.getBlocksEndedMidBlock()).isEmpty();
                // Now start streaming another block, say block 0
                final TestBlock block0 = TestBlockBuilder.generateBlockWithNumber(0L);
                final BlockItemSetUnparsed block0ItemSet = block0.asItemSetUnparsed();
                final PublishStreamRequestUnparsed requestBlock0 = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(block0ItemSet)
                        .build();
                toTest.onNext(requestBlock0);
                // Assert that block 1 has ended mid-block
                assertThat(manager.getBlocksEndedMidBlock()).containsExactly(block1.number());
            }

            /// This test aims to assert that the [PublisherHandler] correctly
            /// handles a received request
            /// [PublisherHandler#onNext(PublishStreamRequestUnparsed)] when a
            /// premature header is received, i.e. the request contains a block
            /// header, but the current block is not yet streamed in full. A header
            /// indicates that the current request initiates the streaming of a new
            /// block. When a header is sent prematurely, the handler is expected to
            /// not send any responses in case the new block that starts streaming is
            /// accepted.
            @Test
            @DisplayName("Test onNext() with premature header received, no responses sent when new block is accepted")
            void testOnNextPrematureHeaderNewBlockNoResponsesSent() {
                // Setup request to send, in this case a single complete valid block
                // as items, starting with header and ending with proof
                final TestBlock block1 = TestBlockBuilder.generateBlockWithNumber(1L);
                final BlockItemSetUnparsed block1ItemSet = block1.asItemSetUnparsed();
                final PublishStreamRequestUnparsed requestBlock1 = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(block1ItemSet)
                        .build();
                // Train the manager to expect return ACCEPT
                manager.setBlockActionForBlock(BlockAction.ACCEPT);
                // Call; we expect that everything is well when we've first sent this valid request
                // We do not end the block here, it must remain active and currently streaming
                toTest.onNext(requestBlock1);
                // Now start streaming another block, say block 0
                final TestBlock block0 = TestBlockBuilder.generateBlockWithNumber(0L);
                final BlockItemSetUnparsed block0ItemSet = block0.asItemSetUnparsed();
                final PublishStreamRequestUnparsed requestBlock0 = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(block0ItemSet)
                        .build();
                toTest.onNext(requestBlock0);
                // Assert no responses on the pipeline are made, but also that no calls of onComplete are called
                // indicating the handler is still active
                assertThat(repliesPipeline.getOnNextCalls()).isEmpty();
                assertThat(repliesPipeline.getOnCompleteCalls().get()).isEqualTo(0);
                // Assert no other responses sent
                assertThat(repliesPipeline.getOnErrorCalls()).isEmpty();
                assertThat(repliesPipeline.getOnSubscriptionCalls()).isEmpty();
                assertThat(repliesPipeline.getClientEndStreamCalls().get()).isEqualTo(0);
            }

            /// This test aims to assert that the [PublisherHandler] correctly
            /// handles a received request
            /// [PublisherHandler#onNext(PublishStreamRequestUnparsed)] when a
            /// premature header is received, i.e. the request contains a block
            /// header, but the current block is not yet streamed in full. A header
            /// indicates that the current request initiates the streaming of a new
            /// block. When a header is sent prematurely, the handler is expected to
            /// update metrics accordingly.
            @Test
            @DisplayName("Test onNext() with premature header received, metrics updated")
            void testOnNextPrematureHeaderMetricsUpdated() {
                // Setup request to send, in this case a single complete valid block
                // as items, starting with header and ending with proof
                final TestBlock block1 = TestBlockBuilder.generateBlockWithNumber(1L);
                final BlockItemSetUnparsed block1ItemSet = block1.asItemSetUnparsed();
                final PublishStreamRequestUnparsed requestBlock1 = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(block1ItemSet)
                        .build();
                // Train the manager to expect return ACCEPT
                manager.setBlockActionForBlock(BlockAction.ACCEPT);
                // Call; we expect that everything is well when we've first sent this valid request
                // We do not end the block here, it must remain active and currently streaming
                toTest.onNext(requestBlock1);
                // Now start streaming another block, say block 0
                final TestBlock block0 = TestBlockBuilder.generateBlockWithNumber(0L);
                final BlockItemSetUnparsed block0ItemSet = block0.asItemSetUnparsed();
                final PublishStreamRequestUnparsed requestBlock0 = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(block0ItemSet)
                        .build();
                toTest.onNext(requestBlock0);
                // Assert metrics updated
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCK_ITEMS_RECEIVED))
                        .isEqualTo(block1.blockSize() + block0.blockSize());
                // Assert other metrics unchanged
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCK_ENDOFSTREAM_SENT))
                        .isEqualTo(0);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCKS_ACK_SENT))
                        .isEqualTo(0);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_STREAM_ERRORS))
                        .isEqualTo(0);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCKS_SKIPS_SENT))
                        .isEqualTo(0);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCKS_RESEND_SENT))
                        .isEqualTo(0);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCK_SEND_RESPONSE_FAILED))
                        .isEqualTo(0);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCK_ENDSTREAM_RECEIVED))
                        .isEqualTo(0);
            }

            /// This test aims to assert that the [PublisherHandler] correctly
            /// handles a received invalid request
            /// [PublisherHandler#onNext(PublishStreamRequestUnparsed)] when
            /// block header is broken, cannot be parsed. We expect that the handler
            /// will not create any queues.
            @Test
            @DisplayName("Test onNext() with invalid request - broken header - no items propagated")
            void testOnNextBrokenHeader() {
                // Setup request to send, in this case a single complete block
                // as items, starting with a broken header and ending with proof
                final long streamedBlockNumber = 0L;
                final BlockUnparsed block = TestBlockBuilder.generateBlockWithBrokenHeader(streamedBlockNumber);
                final BlockItemSetUnparsed blockItemSet = BlockItemSetUnparsed.newBuilder()
                        .blockItems(block.blockItems())
                        .build();
                final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(blockItemSet)
                        .build();
                // Call
                toTest.onNext(request);
                // Assert no items offered to the transfer queue
                assertThat(manager.getQueueByBlockMap()).isEmpty();
            }

            /// This test aims to assert that the [PublisherHandler] correctly
            /// handles a received invalid request
            /// [PublisherHandler#onNext(PublishStreamRequestUnparsed)] when
            /// block header is broken, cannot be parsed. We expect that the handler
            /// will simply reply with a
            /// [org.hiero.block.api.PublishStreamResponse.EndOfStream]
            /// with [org.hiero.block.api.PublishStreamResponse.EndOfStream.Code#INVALID_REQUEST].
            @Test
            @DisplayName(
                    "Test onNext() with invalid request - broken header - respond with EndOfStream, Code INVALID_REQUEST")
            void testOnNextBrokenHeaderResponse() {
                // Setup request to send, in this case a single complete block
                // as items, starting with a broken header and ending with proof
                final long streamedBlockNumber = 0L;
                final BlockUnparsed block = TestBlockBuilder.generateBlockWithBrokenHeader(streamedBlockNumber);
                final BlockItemSetUnparsed blockItemSet = BlockItemSetUnparsed.newBuilder()
                        .blockItems(block.blockItems())
                        .build();
                final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(blockItemSet)
                        .build();
                // Call
                toTest.onNext(request);
                // Assert single response is EndOfStream with Code INVALID_REQUEST and onComplete is called (shutdown)
                assertThat(repliesPipeline.getOnNextCalls())
                        .hasSize(1)
                        .first()
                        .returns(ResponseOneOfType.END_STREAM, responseKindExtractor)
                        .returns(Code.INVALID_REQUEST, endStreamResponseCodeExtractor)
                        .returns(manager.getLatestBlockNumber(), endStreamBlockNumberExtractor);
                assertThat(repliesPipeline.getOnCompleteCalls().get()).isEqualTo(1);
                // Assert no other responses sent
                assertThat(repliesPipeline.getOnErrorCalls()).isEmpty();
                assertThat(repliesPipeline.getOnSubscriptionCalls()).isEmpty();
                assertThat(repliesPipeline.getClientEndStreamCalls().get()).isEqualTo(0);
            }

            /// This test aims to assert that the [PublisherHandler] correctly
            /// handles a received invalid request
            /// [PublisherHandler#onNext(PublishStreamRequestUnparsed)] when
            /// block header is broken, cannot be parsed. We expect that the handler
            /// will update metrics.
            @Test
            @DisplayName("Test onNext() with invalid request - broken header - metrics updated")
            void testOnNextBrokenHeaderMetrics() {
                // Setup request to send, in this case a single complete block
                // as items, starting with a broken header and ending with proof
                final long streamedBlockNumber = 0L;
                final BlockUnparsed block = TestBlockBuilder.generateBlockWithBrokenHeader(streamedBlockNumber);
                final BlockItemSetUnparsed blockItemSet = BlockItemSetUnparsed.newBuilder()
                        .blockItems(block.blockItems())
                        .build();
                final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(blockItemSet)
                        .build();
                // Call
                toTest.onNext(request);
                // Assert metrics updated
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCK_ENDOFSTREAM_SENT))
                        .isEqualTo(1);
                // Assert other metrics unchanged
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCK_ITEMS_RECEIVED))
                        .isEqualTo(0);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCKS_ACK_SENT))
                        .isEqualTo(0);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_STREAM_ERRORS))
                        .isEqualTo(0);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCKS_SKIPS_SENT))
                        .isEqualTo(0);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCKS_RESEND_SENT))
                        .isEqualTo(0);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCK_SEND_RESPONSE_FAILED))
                        .isEqualTo(0);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCK_ENDSTREAM_RECEIVED))
                        .isEqualTo(0);
            }

            /// This test aims to assert that the [PublisherHandler] correctly
            /// handles a received invalid request
            /// [PublisherHandler#onNext(PublishStreamRequestUnparsed)] when
            /// block header null. We expect that the handler will not create any
            /// queues.
            @Test
            @DisplayName("Test onNext() with invalid request - null header - no items propagated")
            void testOnNextNullHeader() {
                // Setup request to send, in this case a single complete block
                // as items, starting with a null header and ending with proof
                final long streamedBlockNumber = 0L;
                final BlockUnparsed block = TestBlockBuilder.generateBlockWithNullHeaderBytes(streamedBlockNumber);
                final BlockItemSetUnparsed blockItemSet = BlockItemSetUnparsed.newBuilder()
                        .blockItems(block.blockItems())
                        .build();
                final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(blockItemSet)
                        .build();
                // Call
                toTest.onNext(request);
                // Assert no items offered to the transfer queue
                assertThat(manager.getQueueByBlockMap()).isEmpty();
            }

            /// This test aims to assert that the [PublisherHandler] correctly
            /// handles a received invalid request
            /// [PublisherHandler#onNext(PublishStreamRequestUnparsed)] when
            /// block header null. We expect that the handler
            /// will simply reply with a
            /// [org.hiero.block.api.PublishStreamResponse.EndOfStream]
            /// with [org.hiero.block.api.PublishStreamResponse.EndOfStream.Code#ERROR].
            @Test
            @DisplayName("Test onNext() with invalid request - null header - respond with EndOfStream, Code ERROR")
            void testOnNextNullHeaderResponse() {
                // Setup request to send, in this case a single complete block
                // as items, starting with a null header and ending with proof
                final long streamedBlockNumber = 0L;
                final BlockUnparsed block = TestBlockBuilder.generateBlockWithNullHeaderBytes(streamedBlockNumber);
                final BlockItemSetUnparsed blockItemSet = BlockItemSetUnparsed.newBuilder()
                        .blockItems(block.blockItems())
                        .build();
                final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(blockItemSet)
                        .build();
                // Call
                toTest.onNext(request);
                // Assert single response is EndOfStream with Code ERROR and onComplete is called (shutdown)
                assertThat(repliesPipeline.getOnNextCalls())
                        .hasSize(1)
                        .first()
                        .returns(ResponseOneOfType.END_STREAM, responseKindExtractor)
                        .returns(Code.ERROR, endStreamResponseCodeExtractor)
                        .returns(manager.getLatestBlockNumber(), endStreamBlockNumberExtractor);
                assertThat(repliesPipeline.getOnCompleteCalls().get()).isEqualTo(1);
                // Assert no other responses sent
                assertThat(repliesPipeline.getOnErrorCalls()).isEmpty();
                assertThat(repliesPipeline.getOnSubscriptionCalls()).isEmpty();
                assertThat(repliesPipeline.getClientEndStreamCalls().get()).isEqualTo(0);
            }

            /// This test aims to assert that the [PublisherHandler] correctly
            /// handles a received invalid request
            /// [PublisherHandler#onNext(PublishStreamRequestUnparsed)] when
            /// block header null. We expect that the handler will update metrics.
            @Test
            @DisplayName("Test onNext() with invalid request - null header - metrics updated")
            void testOnNextNullHeaderMetrics() {
                // Setup request to send, in this case a single complete block
                // as items, starting with a null header and ending with proof
                final long streamedBlockNumber = 0L;
                final BlockUnparsed block = TestBlockBuilder.generateBlockWithNullHeaderBytes(streamedBlockNumber);
                final BlockItemSetUnparsed blockItemSet = BlockItemSetUnparsed.newBuilder()
                        .blockItems(block.blockItems())
                        .build();
                final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(blockItemSet)
                        .build();
                // Call
                toTest.onNext(request);
                // Assert metrics updated
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCK_ENDOFSTREAM_SENT))
                        .isEqualTo(1);
                // Assert other metrics unchanged
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCK_ITEMS_RECEIVED))
                        .isEqualTo(0);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCKS_ACK_SENT))
                        .isEqualTo(0);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_STREAM_ERRORS))
                        .isEqualTo(0);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCKS_SKIPS_SENT))
                        .isEqualTo(0);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCKS_RESEND_SENT))
                        .isEqualTo(0);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCK_SEND_RESPONSE_FAILED))
                        .isEqualTo(0);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCK_ENDSTREAM_RECEIVED))
                        .isEqualTo(0);
            }

            /// This test aims to verify that the
            /// [PublisherHandler#onNext(PublishStreamRequestUnparsed)]
            /// correctly handles a request with null block items. We expect that
            /// [PublishStreamResponse.EndOfStream]
            /// response is returned with code [Code#INVALID_REQUEST].
            @Test
            @DisplayName("Test onNext() with null block items")
            void testOnNextNullItems() {
                // Build a PublishStreamRequest with null block items
                final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(BlockItemSetUnparsed.newBuilder()
                                .blockItems((List<BlockItemUnparsed>) null)
                                .build())
                        .build();
                // Send the request to the pipeline
                toTest.onNext(request);
                // Assert response and onComplete is called (shutdown)
                assertThat(repliesPipeline.getOnNextCalls())
                        .hasSize(1)
                        .first()
                        .isNotNull()
                        .returns(ResponseOneOfType.END_STREAM, responseKindExtractor)
                        .returns(Code.INVALID_REQUEST, endStreamResponseCodeExtractor)
                        .returns(manager.getLatestBlockNumber(), endStreamBlockNumberExtractor);
                assertThat(repliesPipeline.getOnCompleteCalls().get()).isEqualTo(1);
                // Assert no other responses sent
                assertThat(repliesPipeline.getOnErrorCalls()).isEmpty();
                assertThat(repliesPipeline.getOnSubscriptionCalls()).isEmpty();
                assertThat(repliesPipeline.getClientEndStreamCalls().get()).isEqualTo(0);
            }

            /// This test aims to verify that the
            /// [PublisherHandler#onNext(PublishStreamRequestUnparsed)]
            /// correctly handles a request with empty block items. We expect
            /// that [PublishStreamResponse.EndOfStream]
            /// response is returned with code [Code#INVALID_REQUEST].
            @Test
            @DisplayName("Test onNext() with empty block items")
            void testOnNextEmptyItems() {
                // Build a PublishStreamRequest with empty block items
                final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(BlockItemSetUnparsed.newBuilder()
                                .blockItems(Collections.emptyList())
                                .build())
                        .build();
                // Send the request to the pipeline
                toTest.onNext(request);
                // Assert response
                assertThat(repliesPipeline.getOnNextCalls())
                        .hasSize(1)
                        .first()
                        .isNotNull()
                        .returns(ResponseOneOfType.END_STREAM, responseKindExtractor)
                        .returns(Code.INVALID_REQUEST, endStreamResponseCodeExtractor)
                        .returns(manager.getLatestBlockNumber(), endStreamBlockNumberExtractor);
                assertThat(repliesPipeline.getOnCompleteCalls().get()).isEqualTo(1);
                // Assert no other responses sent
                assertThat(repliesPipeline.getOnErrorCalls()).isEmpty();
                assertThat(repliesPipeline.getOnSubscriptionCalls()).isEmpty();
                assertThat(repliesPipeline.getClientEndStreamCalls().get()).isEqualTo(0);
            }

            /// This test aims to verify that the
            /// [PublisherHandler#onNext(PublishStreamRequestUnparsed)]
            /// correctly handles a request with unset oneOf We expect
            /// that [PublishStreamResponse.EndOfStream]
            /// response is returned with code [Code#INVALID_REQUEST].
            @Test
            @DisplayName("Test onNext() with unset oneOf")
            void testOnNextUnsetOneOf() {
                // Build a PublishStreamRequest with an unset oneOf
                final PublishStreamRequestUnparsed request =
                        PublishStreamRequestUnparsed.newBuilder().build();
                // Send the request to the pipeline
                toTest.onNext(request);
                // Assert response
                assertThat(repliesPipeline.getOnNextCalls())
                        .hasSize(1)
                        .first()
                        .isNotNull()
                        .returns(ResponseOneOfType.END_STREAM, responseKindExtractor)
                        .returns(Code.ERROR, endStreamResponseCodeExtractor)
                        .returns(manager.getLatestBlockNumber(), endStreamBlockNumberExtractor);
                assertThat(repliesPipeline.getOnCompleteCalls().get()).isEqualTo(1);
                // Assert no other responses sent
                assertThat(repliesPipeline.getOnErrorCalls()).isEmpty();
                assertThat(repliesPipeline.getOnSubscriptionCalls()).isEmpty();
                assertThat(repliesPipeline.getClientEndStreamCalls().get()).isEqualTo(0);
            }

            /// This test aims to verify that the
            /// [PublisherHandler#onNext(PublishStreamRequestUnparsed)]
            /// correctly handles a request with EndStream shutdown. We expect
            /// that [PublishStreamResponse.EndOfStream]
            /// response is returned with any code to shut down the handler.
            @ParameterizedTest()
            @EnumSource(EndStream.Code.class)
            @DisplayName("Test onNext() with EndStream shutdown - Code {code}")
            void testOnNextEndStreamShutdown(final EndStream.Code code) {
                final EndStream endStream = EndStream.newBuilder()
                        .endCode(code)
                        .earliestBlockNumber(0L)
                        .latestBlockNumber(0L)
                        .build();
                // Build a PublishStreamRequest with an unset oneOf
                final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                        .endStream(endStream)
                        .build();
                // Send the request to the pipeline
                toTest.onNext(request);
                // Assert response
                assertThat(repliesPipeline.getOnCompleteCalls().get()).isEqualTo(1);
                // Assert no other responses sent
                assertThat(repliesPipeline.getOnNextCalls()).isEmpty();
                assertThat(repliesPipeline.getOnErrorCalls()).isEmpty();
                assertThat(repliesPipeline.getOnSubscriptionCalls()).isEmpty();
                assertThat(repliesPipeline.getClientEndStreamCalls().get()).isEqualTo(0);
            }

            /// This test aims to verify that the
            /// [PublisherHandler#onNext(PublishStreamRequestUnparsed)]
            /// correctly handles a request with EndStream shutdown. We expect
            /// that [PublishStreamResponse.EndOfStream]
            /// response is returned with any code to shut down the handler even
            /// if the request is invalid.
            @ParameterizedTest()
            @EnumSource(EndStream.Code.class)
            @DisplayName("Test onNext() with EndStream shutdown - Code {code}")
            void testOnNextEndStreamShutdownInvalidRequest(final EndStream.Code code) {
                // Make an invalid end stream request
                final EndStream endStream = EndStream.newBuilder()
                        .endCode(code)
                        .earliestBlockNumber(100L)
                        .latestBlockNumber(50L)
                        .build();
                // Build a PublishStreamRequest with an unset oneOf
                final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                        .endStream(endStream)
                        .build();
                // Send the request to the pipeline
                toTest.onNext(request);
                // Assert response
                assertThat(repliesPipeline.getOnCompleteCalls().get()).isEqualTo(1);
                // Assert no other responses sent
                assertThat(repliesPipeline.getOnNextCalls()).isEmpty();
                assertThat(repliesPipeline.getOnErrorCalls()).isEmpty();
                assertThat(repliesPipeline.getOnSubscriptionCalls()).isEmpty();
                assertThat(repliesPipeline.getClientEndStreamCalls().get()).isEqualTo(0);
            }

            /// This test aims to verify that the
            /// [PublisherHandler#onNext(PublishStreamRequestUnparsed)]
            /// correctly handles a request with EndStream shutdown with code
            /// [EndStream.Code#TOO_FAR_BEHIND]. We expect that the handler
            /// will send a notification with the latest block known to the network
            /// if the node is truly behind.
            @Test
            @DisplayName("Test onNext() with EndStream TOO_FAR_BEHIND sends newest block known to network notification")
            void testOnNextEndStreamTooFarBehind() {
                // Set the latest block number in the manager
                final long managerLatestKnownBlock = 50L;
                manager.setLatestBlockNumber(managerLatestKnownBlock);
                // Create an EndStream with TOO_FAR_BEHIND code
                final EndStream endStream = EndStream.newBuilder()
                        .endCode(EndStream.Code.TOO_FAR_BEHIND)
                        .earliestBlockNumber(0L)
                        .latestBlockNumber(100L)
                        .build();
                // Build a PublishStreamRequest with the EndStream
                final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                        .endStream(endStream)
                        .build();
                // Send the request to the pipeline
                toTest.onNext(request);
                // Assert notification sent
                final List<NewestBlockKnownToNetworkNotification> sentNotifications =
                        manager.getBlockMessagingFacility().getSentNewestBlockKnownToNetworkNotifications();
                assertThat(sentNotifications).hasSize(1);
            }

            /// This test aims to verify that the
            /// [PublisherHandler#onNext(PublishStreamRequestUnparsed)]
            /// correctly handles a request with EndStream shutdown with code
            /// [EndStream.Code#TOO_FAR_BEHIND]. We expect that the handler
            /// will not send a notification with the latest block known to the
            /// network if the node is not truly behind.
            @Test
            @DisplayName(
                    "Test onNext() with EndStream TOO_FAR_BEHIND does not send newest block known to network notification")
            void testOnNextEndStreamTooFarBehindNoNotification() {
                // Set the latest block number in the manager
                final long managerLatestKnownBlock = 100L;
                manager.setLatestBlockNumber(managerLatestKnownBlock);
                // Create an EndStream with TOO_FAR_BEHIND code
                final EndStream endStream = EndStream.newBuilder()
                        .endCode(EndStream.Code.TOO_FAR_BEHIND)
                        .earliestBlockNumber(0L)
                        .latestBlockNumber(50L)
                        .build();
                // Build a PublishStreamRequest with the EndStream
                final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                        .endStream(endStream)
                        .build();
                // Send the request to the pipeline
                toTest.onNext(request);
                // Assert no notifications sent
                final List<NewestBlockKnownToNetworkNotification> sentNotifications =
                        manager.getBlockMessagingFacility().getSentNewestBlockKnownToNetworkNotifications();
                assertThat(sentNotifications).hasSize(0);
            }

            /// Verifies that [PublisherHandler#handleSkip] covers the false branch of
            /// [PublisherHandler#sendResponse]: when the reply pipeline throws on [onNext],
            /// the send-response-failed metric is incremented and the handler shuts down.
            @Test
            @DisplayName("handleSkip - sendResponse failure increments SEND_RESPONSE_FAILED metric")
            void testHandleSkipSendResponseFailure() {
                final Pipeline<PublishStreamResponse> throwingPipeline = new Pipeline<>() {
                    @Override
                    public void onNext(final PublishStreamResponse item) {
                        throw new RuntimeException("Simulated pipeline failure");
                    }

                    @Override
                    public void onSubscribe(final Subscription subscription) {}

                    @Override
                    public void onError(final Throwable throwable) {}

                    @Override
                    public void onComplete() {}

                    @Override
                    public void clientEndStreamReceived() {}
                };
                final PublisherHandler handler = new PublisherHandler(2L, throwingPipeline, metrics, manager, null);
                manager.addHandler(handler);
                manager.setBlockActionForBlock(BlockAction.SKIP);
                final TestBlock block = TestBlockBuilder.generateBlockWithNumber(5L);
                handler.onNext(PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(block.asItemSetUnparsed())
                        .build());
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCK_SEND_RESPONSE_FAILED))
                        .isEqualTo(1);
            }

            /// Verifies that [PublisherHandler#sendEndOfStream] covers the false branch
            /// of [PublisherHandler#sendResponse]: when END_ERROR is returned and the reply
            /// pipeline throws on [onNext], the send-response-failed metric is incremented.
            @Test
            @DisplayName("handleEndError - sendEndOfStream false branch when pipeline throws")
            void testHandleEndErrorSendEndOfStreamFailure() {
                final Pipeline<PublishStreamResponse> throwingPipeline = new Pipeline<>() {
                    @Override
                    public void onNext(final PublishStreamResponse item) {
                        throw new RuntimeException("Simulated pipeline failure");
                    }

                    @Override
                    public void onSubscribe(final Subscription subscription) {}

                    @Override
                    public void onError(final Throwable throwable) {}

                    @Override
                    public void onComplete() {}

                    @Override
                    public void clientEndStreamReceived() {}
                };
                final PublisherHandler handler = new PublisherHandler(2L, throwingPipeline, metrics, manager, null);
                manager.addHandler(handler);
                manager.setBlockActionForBlock(BlockAction.END_ERROR);
                final TestBlock block = TestBlockBuilder.generateBlockWithNumber(5L);
                handler.onNext(PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(block.asItemSetUnparsed())
                        .build());
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCK_SEND_RESPONSE_FAILED))
                        .isEqualTo(1);
            }

            /// Verifies that [PublisherHandler#handleSendBehind] covers the false branch of
            /// [PublisherHandler#sendResponse]: when the reply pipeline throws on [onNext],
            /// the send-response-failed metric is incremented and the handler shuts down.
            @Test
            @DisplayName("handleSendBehind - sendResponse failure increments SEND_RESPONSE_FAILED metric")
            void testHandleSendBehindSendResponseFailure() {
                final Pipeline<PublishStreamResponse> throwingPipeline = new Pipeline<>() {
                    @Override
                    public void onNext(final PublishStreamResponse item) {
                        throw new RuntimeException("Simulated pipeline failure");
                    }

                    @Override
                    public void onSubscribe(final Subscription subscription) {}

                    @Override
                    public void onError(final Throwable throwable) {}

                    @Override
                    public void onComplete() {}

                    @Override
                    public void clientEndStreamReceived() {}
                };
                final PublisherHandler handler = new PublisherHandler(2L, throwingPipeline, metrics, manager, null);
                manager.addHandler(handler);
                manager.setBlockActionForBlock(BlockAction.SEND_BEHIND);
                final TestBlock block = TestBlockBuilder.generateBlockWithNumber(5L);
                handler.onNext(PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(block.asItemSetUnparsed())
                        .build());
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCK_SEND_RESPONSE_FAILED))
                        .isEqualTo(1);
            }
        }

        /// Tests for the [PublisherHandler#onError(Throwable)] method.
        @Nested
        @DisplayName("onError() Tests")
        class OnErrorTests {
            /// This test aims to assert that the [PublisherHandler] correctly handles a received error response
            /// when the [PublisherHandler#onError(Throwable)] gets called. Here we expect that the handler
            /// will respond to the publisher with an
            /// [org.hiero.block.api.PublishStreamResponse.EndOfStream]
            /// with [org.hiero.block.api.PublishStreamResponse.EndOfStream.Code#ERROR] and will proceed to
            /// orderly shutdown.
            @Test
            @DisplayName("Test onError() with error response - EndOfStream, Code ERROR")
            void testOnErrorResponse() {
                // set an arbitrary valid latest streamed block number to expect in the response
                final long expectedLatestStreamedBlockNumber = 0L;
                manager.setLatestBlockNumber(expectedLatestStreamedBlockNumber);
                // Call onError with an arbitrary Throwable
                toTest.onError(new RuntimeException());
                // Assert single response is EndOfStream with Code ERROR and onComplete is called (shutdown)
                assertThat(repliesPipeline.getOnNextCalls())
                        .hasSize(1)
                        .first()
                        .returns(ResponseOneOfType.END_STREAM, responseKindExtractor)
                        .returns(Code.ERROR, endStreamResponseCodeExtractor)
                        .returns(expectedLatestStreamedBlockNumber, endStreamBlockNumberExtractor);
                assertThat(repliesPipeline.getOnCompleteCalls().get()).isEqualTo(1);
                // Assert no other responses sent
                assertThat(repliesPipeline.getOnErrorCalls()).isEmpty();
                assertThat(repliesPipeline.getOnSubscriptionCalls()).isEmpty();
                assertThat(repliesPipeline.getClientEndStreamCalls().get()).isEqualTo(0);
            }

            /// This test aims to assert that the [PublisherHandler] correctly handles a received error response
            /// when the [PublisherHandler#onError(Throwable)] gets called. Here we expect that metrics will
            /// be updated accordingly to reflect the received error.
            @Test
            @DisplayName("Test onError() metrics updated - EndOfStream, Code ERROR")
            void testOnErrorMetrics() {
                // set an arbitrary valid latest streamed block number
                final long latestStreamedBlockNumber = 0L;
                manager.setLatestBlockNumber(latestStreamedBlockNumber);
                // Call onError with an arbitrary Throwable
                toTest.onError(new RuntimeException());
                // Assert metrics updated
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCK_ENDOFSTREAM_SENT))
                        .isEqualTo(1);
                // Assert other metrics unchanged
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCK_ITEMS_RECEIVED))
                        .isEqualTo(0);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCKS_ACK_SENT))
                        .isEqualTo(0);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_STREAM_ERRORS))
                        .isEqualTo(0);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCKS_SKIPS_SENT))
                        .isEqualTo(0);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCKS_RESEND_SENT))
                        .isEqualTo(0);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCK_SEND_RESPONSE_FAILED))
                        .isEqualTo(0);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCK_ENDSTREAM_RECEIVED))
                        .isEqualTo(0);
            }
        }

        /// Requirement: When [PublisherHandler#onComplete()] is invoked, it must signal the completion of the reply
        /// pipeline exactly once and perform no other side effects.
        /// Verification strategy: Begin from a clean state, invoke [PublisherHandler#onComplete()], then assert
        /// exactly one completion event and that onNext/onError/onSubscription/client-end-stream counters
        /// remain unchanged.
        @Nested
        @DisplayName("onComplete() Tests")
        class OnCompleteTests {
            /// Verifies that onComplete() completes the reply pipeline without any extra interactions.
            @Test
            @DisplayName("Test onComplete() completes pipeline without extra interactions")
            void testOnCompleteRemovesHandlerAndCompletesPipeline() {
                // Call onComplete
                toTest.onComplete();

                // Assert pipeline completed exactly once
                assertThat(repliesPipeline.getOnCompleteCalls().get()).isEqualTo(1);

                // Assert no other pipeline interactions occurred
                assertThat(repliesPipeline.getOnNextCalls()).isEmpty();
                assertThat(repliesPipeline.getOnErrorCalls()).isEmpty();
                assertThat(repliesPipeline.getOnSubscriptionCalls()).isEmpty();
                assertThat(repliesPipeline.getClientEndStreamCalls().get()).isEqualTo(0);
            }
        }

        /// Requirement: [PublisherHandler#onSubscribe(Subscription)] must be a no-op:
        /// it should not throw, and it must produce no protocol responses or reply-pipeline side effects.
        /// Verification strategy: Start from a clean reply pipeline, pass a minimal no-op Subscription
        /// to `onSubscribe(...)`, assert no exception is thrown, and then assert zero interactions across
        /// onNext/onError/onSubscription/onComplete and client-end-stream counters.
        @Nested
        @DisplayName("onSubscribe() Tests")
        class OnSubscribeTests {
            /// Verifies that onSubscribe() throws no exceptions and leaves the reply pipeline untouched.
            @Test
            @DisplayName("onSubscribe() performs no side effects")
            void testOnSubscribeDoesNothing() {
                // Pre-assert: no reply pipeline interactions yet
                assertThat(repliesPipeline.getOnCompleteCalls().get()).isZero();
                assertThat(repliesPipeline.getOnNextCalls()).isEmpty();
                assertThat(repliesPipeline.getOnErrorCalls()).isEmpty();
                assertThat(repliesPipeline.getOnSubscriptionCalls()).isEmpty();
                assertThat(repliesPipeline.getClientEndStreamCalls().get()).isZero();

                // Minimal Subscription
                Subscription sub = new Subscription() {
                    @Override
                    public void request(long n) {
                        // no-op: method intentionally left blank for testing
                    }

                    @Override
                    public void cancel() {
                        // no-op: method intentionally left blank for testing
                    }
                };

                // Call and ensure no exception
                assertThatCode(() -> toTest.onSubscribe(sub)).doesNotThrowAnyException();

                // Post-assert: still no interactions with the replies pipeline
                assertThat(repliesPipeline.getOnCompleteCalls().get()).isZero();
                assertThat(repliesPipeline.getOnNextCalls()).isEmpty();
                assertThat(repliesPipeline.getOnErrorCalls()).isEmpty();
                assertThat(repliesPipeline.getOnSubscriptionCalls()).isEmpty();
                assertThat(repliesPipeline.getClientEndStreamCalls().get()).isZero();
            }
        }

        /// Requirement: Client end-of-stream must complete the reply pipeline and emit
        /// no protocol responses.
        /// How this test proves it: Pre-assert the pipeline has had no prior
        /// interactions, call [PublisherHandler#clientEndStreamReceived()], then
        /// assert exactly one completion and that onNext/onError/onSubscription and
        /// client-end-stream counters remain unchanged.
        @Nested
        @DisplayName("clientEndStreamReceived() Tests")
        class ClientEndStreamReceivedTests {

            /// Verifies that calling clientEndStreamReceived() completes the pipeline and sends no other responses.
            @Test
            @DisplayName("clientEndStreamReceived() completes pipeline without extra interactions")
            void testClientEndStreamReceivedCompletesPipeline() {
                // Pre-assert: no reply pipeline interactions yet
                assertThat(repliesPipeline.getOnCompleteCalls().get()).isZero();
                assertThat(repliesPipeline.getOnNextCalls()).isEmpty();
                assertThat(repliesPipeline.getOnErrorCalls()).isEmpty();
                assertThat(repliesPipeline.getOnSubscriptionCalls()).isEmpty();
                assertThat(repliesPipeline.getClientEndStreamCalls().get()).isZero();

                // Invoke the method under test
                toTest.clientEndStreamReceived();

                // Post-assert: pipeline completed exactly once
                assertThat(repliesPipeline.getOnCompleteCalls().get()).isEqualTo(1);

                // Still no other interactions
                assertThat(repliesPipeline.getOnNextCalls()).isEmpty();
                assertThat(repliesPipeline.getOnErrorCalls()).isEmpty();
                assertThat(repliesPipeline.getOnSubscriptionCalls()).isEmpty();
                assertThat(repliesPipeline.getClientEndStreamCalls().get()).isZero();
            }
        }

        /// Requirement: When an acknowledgement is sent for a given block number, the handler must emit exactly one
        /// [PublishStreamResponse] response containing the same block number
        /// and increment the `blockAcknowledgementsSent` metric by one.
        /// Verification strategy: Begin from a clean state, invoke
        /// [PublisherHandler#sendAcknowledgement(long)], then
        /// assert that a single ACK response with the expected block number was produced and metrics updated
        /// accordingly.
        @Nested
        @DisplayName("sendAcknowledgement() Tests")
        class SendAcknowledgementTests {
            /// Verifies that sendAcknowledgement() emits a single ACK for the given block number and increments
            /// metrics.
            @Test
            @DisplayName("sends ACK response and updates metrics for given block number")
            void testSendAcknowledgementSendsAckAndUpdatesMetrics() {
                // Pre-assert: clean state
                assertThat(repliesPipeline.getOnNextCalls()).isEmpty();
                assertThat(repliesPipeline.getOnErrorCalls()).isEmpty();
                assertThat(repliesPipeline.getOnSubscriptionCalls()).isEmpty();
                assertThat(repliesPipeline.getOnCompleteCalls().get()).isZero();
                final long beforeAcks = getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCKS_ACK_SENT);

                // Act
                final long blockNumber = 123L;
                toTest.sendAcknowledgement(blockNumber);

                // Assert: single ACK with the same block number
                assertThat(repliesPipeline.getOnNextCalls())
                        .hasSize(1)
                        .first()
                        .returns(ResponseOneOfType.ACKNOWLEDGEMENT, responseKindExtractor)
                        .returns(blockNumber, acknowledgementBlockNumberExtractor);

                // Metrics: +1 ack, others unchanged
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCKS_ACK_SENT))
                        .isEqualTo(beforeAcks + 1);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCK_ITEMS_RECEIVED))
                        .isEqualTo(0);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_STREAM_ERRORS))
                        .isEqualTo(0);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCKS_SKIPS_SENT))
                        .isEqualTo(0);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCKS_RESEND_SENT))
                        .isEqualTo(0);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCK_ENDOFSTREAM_SENT))
                        .isEqualTo(0);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCK_SEND_RESPONSE_FAILED))
                        .isEqualTo(0);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCK_ENDSTREAM_RECEIVED))
                        .isEqualTo(0);

                // No other pipeline interactions
                assertThat(repliesPipeline.getOnErrorCalls()).isEmpty();
                assertThat(repliesPipeline.getOnSubscriptionCalls()).isEmpty();
                assertThat(repliesPipeline.getOnCompleteCalls().get()).isZero();
            }

            /// Verifies that two sequential sendAcknowledgement calls emit two ACKs in order and
            /// increment the acknowledgements metric by 2.
            @Test
            @DisplayName("multiple calls produce multiple ACKs in order and increment metrics per call")
            void testSendAcknowledgementMultipleCalls() {
                // Pre-assert: capture baseline for ack metric
                final long beforeAcks = getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCKS_ACK_SENT);

                // Act: two acknowledgements
                final long b0 = 1L;
                final long b1 = 2L;
                toTest.sendAcknowledgement(b0);
                toTest.sendAcknowledgement(b1);

                // Assert: two ACKs in order with correct block numbers
                assertThat(repliesPipeline.getOnNextCalls()).hasSize(2);
                assertThat(repliesPipeline.getOnNextCalls().get(0))
                        .returns(ResponseOneOfType.ACKNOWLEDGEMENT, responseKindExtractor)
                        .returns(b0, acknowledgementBlockNumberExtractor);
                assertThat(repliesPipeline.getOnNextCalls().get(1))
                        .returns(ResponseOneOfType.ACKNOWLEDGEMENT, responseKindExtractor)
                        .returns(b1, acknowledgementBlockNumberExtractor);

                // Metrics increment by 2
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCKS_ACK_SENT))
                        .isEqualTo(beforeAcks + 2);

                // No completion/error side effects from acks
                assertThat(repliesPipeline.getOnCompleteCalls().get()).isZero();
                assertThat(repliesPipeline.getOnErrorCalls()).isEmpty();
                assertThat(repliesPipeline.getOnSubscriptionCalls()).isEmpty();
            }

            /// Verifies that [PublisherHandler#sendAcknowledgement] covers the false branch of
            /// [PublisherHandler#sendResponse]: when the reply pipeline throws on [onNext],
            /// the send-response-failed metric is incremented.
            @Test
            @DisplayName("sendAcknowledgement - sendResponse failure increments SEND_RESPONSE_FAILED metric")
            void testSendAcknowledgementSendResponseFailure() {
                final Pipeline<PublishStreamResponse> throwingPipeline = new Pipeline<>() {
                    @Override
                    public void onNext(final PublishStreamResponse item) {
                        throw new RuntimeException("Simulated pipeline failure");
                    }

                    @Override
                    public void onSubscribe(final Subscription subscription) {}

                    @Override
                    public void onError(final Throwable throwable) {}

                    @Override
                    public void onComplete() {}

                    @Override
                    public void clientEndStreamReceived() {}
                };
                final PublisherHandler handler = new PublisherHandler(2L, throwingPipeline, metrics, manager, null);
                handler.sendAcknowledgement(1L);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCK_SEND_RESPONSE_FAILED))
                        .isEqualTo(1);
            }
        }

        /// Tests for the [PublisherHandler#handleFailedVerification(long)]method.
        @Nested
        @DisplayName("handleFailedVerification() Tests")
        class HandleVerificationTests {
            /// This test aims to assert that the
            /// [PublisherHandler#handleFailedVerification(long)] will
            /// correctly handle a failed verification by sending no response,
            /// when the publisher has not sent the block that failed verification.
            @Test
            @DisplayName(
                    "handleFailedVerification() - no response when handler did not send the block that failed verification")
            void testHandleVerificationNoResponse() {
                final long expectedResponseBlockNumber = 0;
                // Call
                toTest.handleFailedVerification(expectedResponseBlockNumber);
                // Assert no response is sent and shared metrics is not updated
                assertThat(repliesPipeline.getOnNextCalls()).isEmpty();
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCKS_RESEND_SENT))
                        .isEqualTo(0);
                // Assert no other responses sent
                assertThat(repliesPipeline.getOnErrorCalls()).isEmpty();
                assertThat(repliesPipeline.getOnSubscriptionCalls()).isEmpty();
                assertThat(repliesPipeline.getOnCompleteCalls().get()).isEqualTo(0);
                assertThat(repliesPipeline.getClientEndStreamCalls().get()).isEqualTo(0);
            }

            /// This test aims to assert that the
            /// [PublisherHandler#handleFailedVerification(long)] will
            /// correctly handle a failed verification by sending a
            /// [org.hiero.block.api.PublishStreamResponse.EndOfStream]
            /// response with code
            /// [org.hiero.block.api.PublishStreamResponse.EndOfStream.Code#BAD_BLOCK_PROOF]
            /// and then scheduling the handler to be shutdown.
            @Test
            @DisplayName(
                    "handleFailedVerification() - EndOfStream response with Code BAD_BLOCK_PROOF when handler sent the block that failed verification")
            void testHandleVerificationBadProof() {
                // Generate any block, we do not have an actual verification, we will simulate a failure
                final TestBlock block = TestBlockBuilder.generateBlockWithNumber(0L);
                final long expectedBlockNumber = block.number();
                // Send a request to the handler with the block, this will update
                // the internal state of the handler and will flag that the block has been sent
                // by the handler under test. This will be important when we simulate the failed
                // verification.
                final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(block.asItemSetUnparsed())
                        .build();
                // Train the manager to expect return ACCEPT
                manager.setBlockActionForBlock(BlockAction.ACCEPT);
                // Call onNext with the request, this will update the internal state of the handler
                toTest.onNext(request);
                manager.setBlockActionForEndOfBlock(BlockAction.ACCEPT, block.number());
                endThisBlock(toTest, expectedBlockNumber);
                // Train the manager to return the expected latest block number
                final long latestBlockNumber = expectedBlockNumber - 1L;
                manager.setLatestBlockNumber(latestBlockNumber);
                // Call
                toTest.handleFailedVerification(expectedBlockNumber);
                // Assert single response is EndOfStream with Code BAD_BLOCK_PROOF
                assertThat(repliesPipeline.getOnNextCalls())
                        .hasSize(1)
                        .first()
                        .returns(ResponseOneOfType.END_STREAM, responseKindExtractor)
                        .returns(Code.BAD_BLOCK_PROOF, endStreamResponseCodeExtractor)
                        .returns(latestBlockNumber, endStreamBlockNumberExtractor);
                // We expect a shutdown to be scheduled
                // We need to send any request or trigger any pipeline method
                // to do the actual shutdown
                toTest.onNext(request);
                // Assert onComplete is called (shutdown that was scheduled)
                assertThat(repliesPipeline.getOnCompleteCalls().get()).isEqualTo(1);
                // Assert no other responses sent
                assertThat(repliesPipeline.getOnErrorCalls()).isEmpty();
                assertThat(repliesPipeline.getOnSubscriptionCalls()).isEmpty();
                assertThat(repliesPipeline.getClientEndStreamCalls().get()).isEqualTo(0);
            }
        }

        /// Tests for receiving the [org.hiero.block.api.PublishStreamRequest.RequestOneOfType#END_OF_BLOCK] requests.
        @Nested
        @DisplayName("endOfBlock() Tests")
        class EndOfBlockTests {
            /// This test aims to assert that when an action for block with [BlockAction#ACCEPT] and the correct
            /// block number is received by the manager, the handler will send no responses and will still
            /// be active. The block will be ended for the manager.
            @Test
            @DisplayName(
                    "endOfBlock - No response when the manager sends ActionForBlock with ACCEPT and correct block number, block is ended for the manager")
            void testAccept() {
                // First, build a block we want to close.
                final TestBlock block = TestBlockBuilder.generateBlockWithNumber(0L);
                final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(block.asItemSetUnparsed())
                        .build();
                // Now send the request. This will correctly update the current streaming block of the handler.
                manager.setBlockActionForBlock(BlockAction.ACCEPT);
                toTest.onNext(request);
                // As a pre-check, assert that no block has yet been ended for the manager
                assertThat(manager.getEndOfBlocksReceived()).isEmpty();
                // Call
                manager.setBlockActionForEndOfBlock(new ActionForBlock(BlockAction.ACCEPT, block.number()));
                endThisBlock(toTest, block.number());
                // Assert that the block was ended for the manager
                assertThat(manager.getEndOfBlocksReceived()).containsExactly(block.number());
                // Assert no responses
                assertThat(repliesPipeline.getOnNextCalls()).isEmpty();
                assertThat(repliesPipeline.getOnCompleteCalls().get()).isEqualTo(0);
                assertThat(repliesPipeline.getOnErrorCalls()).isEmpty();
                assertThat(repliesPipeline.getOnSubscriptionCalls()).isEmpty();
                assertThat(repliesPipeline.getClientEndStreamCalls().get()).isEqualTo(0);
            }

            /// This test aims to assert that when an action for block with [BlockAction#RESEND] and a correct
            /// block number is received by the manager, the handler will send the RESEND response and still
            /// be active. The block will be ended for the manager.
            @Test
            @DisplayName(
                    "endOfBlock - RESEND response when the manager sends ActionForBlock with RESEND and correct block number, block is ended for the manager")
            void testResend() {
                // First, we need to create the block we want to stream and end
                final TestBlock block = TestBlockBuilder.generateBlockWithNumber(0L);
                final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(block.asItemSetUnparsed())
                        .build();
                // Then we need to stream it to the publisher handler. This will correctly update the
                // current streaming block of the handler.
                manager.setBlockActionForBlock(BlockAction.ACCEPT);
                toTest.onNext(request);
                // As a pre-check, assert that no block has yet been ended for the manager
                assertThat(manager.getEndOfBlocksReceived()).isEmpty();
                // Call
                manager.setBlockActionForEndOfBlock(new ActionForBlock(BlockAction.RESEND, block.number()));
                // End the block, we have told the manager to give us a resend for it, we expect a resend
                endThisBlock(toTest, block.number());
                // Assert that the block was ended for the manager
                assertThat(manager.getEndOfBlocksReceived()).containsExactly(block.number());
                // Assert RESEND has been sent
                assertThat(repliesPipeline.getOnNextCalls())
                        .hasSize(1)
                        .first()
                        .returns(ResponseOneOfType.RESEND_BLOCK, responseKindExtractor)
                        .returns(block.number(), resendBlockNumberExtractor);
                // Assert no other responses
                assertThat(repliesPipeline.getOnCompleteCalls().get()).isEqualTo(0);
                assertThat(repliesPipeline.getOnErrorCalls()).isEmpty();
                assertThat(repliesPipeline.getOnSubscriptionCalls()).isEmpty();
                assertThat(repliesPipeline.getClientEndStreamCalls().get()).isEqualTo(0);
            }

            /// This test aims to assert that when an unexpected [ActionForBlock] is received by the manager
            /// when handling an
            /// [org.hiero.block.api.PublishStreamRequest.RequestOneOfType#END_OF_BLOCK] request,
            /// the handler will proceed to end with an
            /// [ResponseOneOfType#END_STREAM] with code [Code#ERROR] and the connection will be closed.
            /// The block will be ended for the manager.
            @ParameterizedTest
            @MethodSource("unexpectedBlockActions")
            @DisplayName(
                    "endOfBlock - EndOfStream response with Code ERROR when manager supplies an unexpected ActionForBlock, block is ended for the manager")
            void testUnexpectedActionForBlockReceived(final ActionForBlock action) {
                // First, we need to create the block we want to stream and end
                final TestBlock block = TestBlockBuilder.generateBlockWithNumber(0L);
                final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(block.asItemSetUnparsed())
                        .build();
                // Then we need to stream it to the publisher handler. This will correctly update the
                // current streaming block of the handler.
                manager.setBlockActionForBlock(BlockAction.ACCEPT);
                toTest.onNext(request);
                // As a pre-check, assert that no block has yet been ended for the manager
                assertThat(manager.getEndOfBlocksReceived()).isEmpty();
                // Call
                manager.setBlockActionForEndOfBlock(action);
                endThisBlock(toTest, block.number());
                // Assert that the block was ended with the manager
                assertThat(manager.getEndOfBlocksReceived()).containsExactly(block.number());
                // Assert single response is EndOfStream with Code ERROR and onComplete is called (shutdown)
                assertThat(repliesPipeline.getOnNextCalls())
                        .hasSize(1)
                        .first()
                        .returns(ResponseOneOfType.END_STREAM, responseKindExtractor)
                        .returns(Code.ERROR, endStreamResponseCodeExtractor)
                        .returns(-1L, endStreamBlockNumberExtractor);
                assertThat(repliesPipeline.getOnCompleteCalls().get()).isEqualTo(1);
                // Assert no other responses sent
                assertThat(repliesPipeline.getOnErrorCalls()).isEmpty();
                assertThat(repliesPipeline.getOnSubscriptionCalls()).isEmpty();
                assertThat(repliesPipeline.getClientEndStreamCalls().get()).isEqualTo(0);
            }

            /// This test aims to assert that when an
            /// [org.hiero.block.api.PublishStreamRequest.RequestOneOfType#END_OF_BLOCK] request
            /// is received by a handler which is not currently streaming a block, it will do nothing. No
            /// blocks will be ended for the manager. No responses will be observed.
            @Test
            @DisplayName("endOfBlock - when handler is not streaming a block, no block will be ended")
            void testDoNothingIfNotStreaming() {
                // As a pre-check, assert that no block has yet been ended for the manager
                assertThat(manager.getEndOfBlocksReceived()).isEmpty();
                // Call
                endThisBlock(toTest, 0L);
                // Assert that the handler has not ended a block for the manager
                assertThat(manager.getEndOfBlocksReceived()).isEmpty();
                // Assert no responses
                assertThat(repliesPipeline.getOnNextCalls()).isEmpty();
                assertThat(repliesPipeline.getOnCompleteCalls().get()).isZero();
                assertThat(repliesPipeline.getOnErrorCalls()).isEmpty();
                assertThat(repliesPipeline.getOnSubscriptionCalls()).isEmpty();
                assertThat(repliesPipeline.getClientEndStreamCalls().get()).isZero();
            }

            /// Verifies that when the manager returns ACCEPT but with a block number that
            /// does not match the currently streaming block, the handler treats it as an
            /// unexpected action and ends with an error (covers the
            /// {@code currentStreamingNumber == actionForBlock.blockNumber()} false branch).
            @Test
            @DisplayName("endOfBlock - EndOfStream(ERROR) when ACCEPT has a valid but mismatched block number")
            void testAcceptWithMismatchedBlockNumber() {
                final TestBlock block = TestBlockBuilder.generateBlockWithNumber(0L);
                manager.setBlockActionForBlock(BlockAction.ACCEPT);
                toTest.onNext(PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(block.asItemSetUnparsed())
                        .build());
                // Manager returns ACCEPT for block 999, but we are streaming block 0.
                manager.setBlockActionForEndOfBlock(new ActionForBlock(BlockAction.ACCEPT, 999L));
                endThisBlock(toTest, block.number());
                assertThat(repliesPipeline.getOnNextCalls())
                        .anySatisfy(r -> assertThat(r.response().kind()).isEqualTo(ResponseOneOfType.END_STREAM));
            }

            /// Verifies that when a [BlockEnd] arrives for a different block number than
            /// the one currently streaming, the mismatch is logged and the block is still
            /// ended using {@code currentStreamingNumber} (covers line 453 true branch).
            @Test
            @DisplayName("endOfBlock - mismatch in BlockEnd block number is logged; block still ended correctly")
            void testEndOfBlockNumberMismatch() {
                final TestBlock block = TestBlockBuilder.generateBlockWithNumber(0L);
                manager.setBlockActionForBlock(BlockAction.ACCEPT);
                toTest.onNext(PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(block.asItemSetUnparsed())
                        .build());
                // Manager returns ACCEPT for current block 0.
                manager.setBlockActionForEndOfBlock(new ActionForBlock(BlockAction.ACCEPT, 0L));
                // Send BlockEnd for block 1 — handler is streaming block 0.
                endThisBlock(toTest, 1L);
                // The block ended is the current streaming block (0), not the one in BlockEnd (1).
                assertThat(manager.getEndOfBlocksReceived()).containsExactly(0L);
            }

            private static Stream<Arguments> unexpectedBlockActions() {
                return Stream.of(
                        // expected action, but invalid number
                        Arguments.of(new ActionForBlock(BlockAction.ACCEPT, -1L)),
                        // expected action, but invalid number
                        Arguments.of(new ActionForBlock(BlockAction.RESEND, -1L)),
                        // not expected action with a valid number
                        Arguments.of(new ActionForBlock(BlockAction.SKIP, 0L)),
                        // not expected action with a valid number
                        Arguments.of(new ActionForBlock(BlockAction.SEND_BEHIND, 0L)),
                        // not expected action with a valid number
                        Arguments.of(new ActionForBlock(BlockAction.END_DUPLICATE, 0L)),
                        // not expected action with a valid number
                        Arguments.of(new ActionForBlock(BlockAction.END_ERROR, 0L)));
            }
        }

        /// Tests for the [LiveStreamPublisherManager#closeBlock(long)] method.
        @Nested
        @DisplayName("closeBlock() Tests")
        class CloseBlockTests {
            /// This test aims to assert that the [PublisherHandler] correctly
            /// handles a received request
            /// [PublisherHandler#onNext(PublishStreamRequestUnparsed)] happy
            /// path scenario. Here we stream a single complete valid block as items.
            /// The request's first item is the block header for the streamed block,
            /// and the last item is the block proof for the streamed block.
            /// We expect that when the [StreamPublisherManager] returns
            /// [BlockAction#ACCEPT] for the streamed block number and when the
            /// streamed items end with a valid block proof, the handler will send
            /// a signal to the manager to close the block.
            ///
            /// **NOTE**:
            /// > For any other action returned by the manager when we start a new block,
            /// we will not have started streaming that block for that manager. It is not
            /// expected to have to end or close anything.
            @Test
            @DisplayName(
                    "closeBlock() - a valid request with a complete single block calls closeBlock on manager when batch ends with valid proof - ACCEPT")
            void testOnNextCloseBlockValidProofHappyPathACCEPT() {
                // Setup request to send, in this case a single complete valid block
                // as items, starting with header and ending with proof
                final TestBlock block = TestBlockBuilder.generateBlockWithNumber(0L);
                final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(block.asItemSetUnparsed())
                        .build();
                // Train the manager to return the current action for the block number
                manager.setBlockActionForBlock(BlockAction.ACCEPT);
                // Call
                toTest.onNext(request);
                manager.setBlockActionForEndOfBlock(BlockAction.ACCEPT, block.number());
                endThisBlock(toTest, block.number());
                // Assert that the manager's closeBlock method was called
                assertThat(manager.closeBlockCallsForHandler(handlerId)).isOne();
            }

            /// This test aims to assert that the [PublisherHandler] correctly
            /// handles a received request
            /// [PublisherHandler#onNext(PublishStreamRequestUnparsed)].
            /// Here we stream a single complete valid block as items.
            /// The request's first item is the block header for the streamed block,
            /// and the last item is the block proof for the streamed block.
            /// We expect that when the [StreamPublisherManager] returns
            /// [BlockAction#ACCEPT] for the streamed block number and when the
            /// streamed items end with an invalid block proof, the handler will send
            /// a signal to the manager to close the block.
            ///
            /// **NOTE**:
            /// > For any other action returned by the manager when we start a new block,
            /// we will not have started streaming that block for that manager. It is not
            /// expected to have to end or close anything.
            @Test
            @DisplayName(
                    "closeBlock() - with valid request with a complete single block calls closeBlock on manager when batch ends with broken proof - ACCEPT")
            void testOnNextCloseBlockBrokenProofACCEPT() {
                // Setup request to send, in this case a single complete valid block
                // as items, starting with header and ending with a broken proof
                final long streamedBlockNumber = 0L;
                final BlockUnparsed block = TestBlockBuilder.generateBlockWithBrokenProof(streamedBlockNumber);
                final BlockItemSetUnparsed blockItemSet = BlockItemSetUnparsed.newBuilder()
                        .blockItems(block.blockItems())
                        .build();
                final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(blockItemSet)
                        .build();
                // Train the manager to return the current action for the block number
                manager.setBlockActionForBlock(BlockAction.ACCEPT);
                // Call
                toTest.onNext(request);
                manager.setBlockActionForEndOfBlock(BlockAction.ACCEPT, streamedBlockNumber);
                endThisBlock(toTest, streamedBlockNumber);
                // Assert that the manager's closeBlock method was called
                assertThat(manager.closeBlockCallsForHandler(handlerId)).isEqualTo(1);
            }

            /// Verifies that [PublisherHandler#handleResend] covers the false branch of
            /// [PublisherHandler#sendResponse]: when the reply pipeline throws on [onNext],
            /// the send-response-failed metric is incremented.
            @Test
            @DisplayName("handleResend - sendResponse failure increments SEND_RESPONSE_FAILED metric")
            void testHandleResendSendResponseFailure() {
                final Pipeline<PublishStreamResponse> throwingPipeline = new Pipeline<>() {
                    @Override
                    public void onNext(final PublishStreamResponse item) {
                        throw new RuntimeException("Simulated pipeline failure");
                    }

                    @Override
                    public void onSubscribe(final Subscription subscription) {}

                    @Override
                    public void onError(final Throwable throwable) {}

                    @Override
                    public void onComplete() {}

                    @Override
                    public void clientEndStreamReceived() {}
                };
                final PublisherHandler handler = new PublisherHandler(2L, throwingPipeline, metrics, manager, null);
                manager.addHandler(handler);
                final TestBlock block = TestBlockBuilder.generateBlockWithNumber(0L);
                manager.setBlockActionForBlock(BlockAction.ACCEPT);
                handler.onNext(PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(block.asItemSetUnparsed())
                        .build());
                manager.setBlockActionForEndOfBlock(new ActionForBlock(BlockAction.RESEND, block.number()));
                endThisBlock(handler, block.number());
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCK_SEND_RESPONSE_FAILED))
                        .isEqualTo(1);
            }

            /// Verifies that [PublisherHandler#handleResend] correctly handles the else branch
            /// when [StreamPublisherManager#endOfBlock] returns a RESEND action with an invalid
            /// (UNKNOWN) block number, triggering an END_STREAM error response.
            @Test
            @DisplayName(
                    "handleResend - END_STREAM error response when endOfBlock returns RESEND with UNKNOWN block number")
            void testHandleResendUnknownBlockNumber() {
                final TestBlock block = TestBlockBuilder.generateBlockWithNumber(0L);
                manager.setBlockActionForBlock(BlockAction.ACCEPT);
                toTest.onNext(PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(block.asItemSetUnparsed())
                        .build());
                // Return RESEND with UNKNOWN_BLOCK_NUMBER (-1) to trigger the else branch in handleResend
                manager.setBlockActionForEndOfBlock(new ActionForBlock(BlockAction.RESEND, -1L));
                endThisBlock(toTest, block.number());
                assertThat(repliesPipeline.getOnNextCalls())
                        .anySatisfy(r -> assertThat(r.response().kind()).isEqualTo(ResponseOneOfType.END_STREAM));
                assertThat(repliesPipeline.getOnCompleteCalls().get()).isEqualTo(1);
            }

            /// Verifies that [PublisherHandler#handleEndOfBlock] is a no-op when the handler
            /// is not currently streaming a block (covers the true branch of
            /// currentStreamingNumber <= UNKNOWN_BLOCK_NUMBER).
            @Test
            @DisplayName("endOfBlock - no-op when handler is not currently streaming a block")
            void testEndOfBlockWhenNotStreaming() {
                endThisBlock(toTest, 0L);
                assertThat(repliesPipeline.getOnNextCalls()).isEmpty();
                assertThat(repliesPipeline.getOnCompleteCalls().get()).isZero();
            }
        }
    }

    /// Creates a new [MetricsHolder] with default counters for testing.
    /// These counters could be queried to verify the metrics' states.
    private MetricsHolder createMetrics() {
        metricsExporter = new TestMetricsExporter();
        return MetricsHolder.createMetrics(
                MetricRegistry.builder().setMetricsExporter(metricsExporter).build());
    }

    private long getMetricValue(org.hiero.metrics.core.MetricKey<?> metricKey) {
        return metricsExporter.getMetricValue(metricKey.name());
    }

    /// A simple JUL {@link java.util.logging.Handler} that accumulates formatted log messages
    /// for assertion in tests.
    private static class TestLogHandler extends java.util.logging.Handler {
        private final java.util.List<String> messages = new java.util.ArrayList<>();

        @Override
        public void publish(java.util.logging.LogRecord record) {
            if (record != null && record.getMessage() != null) {
                final Object[] params = record.getParameters();
                final String formatted = (params != null && params.length > 0)
                        ? java.text.MessageFormat.format(record.getMessage(), params)
                        : record.getMessage();
                messages.add(formatted);
            }
        }

        @Override
        public void flush() {}

        @Override
        public void close() {}

        java.util.List<String> getLogMessages() {
            return java.util.Collections.unmodifiableList(messages);
        }
    }
}
