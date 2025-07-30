// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.stream.publisher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import com.swirlds.metrics.api.Counter.Config;
import com.swirlds.metrics.impl.DefaultCounter;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TransferQueue;
import java.util.function.Function;
import org.hiero.block.api.PublishStreamRequest.EndStream;
import org.hiero.block.api.PublishStreamResponse;
import org.hiero.block.api.PublishStreamResponse.EndOfStream.Code;
import org.hiero.block.api.PublishStreamResponse.ResponseOneOfType;
import org.hiero.block.internal.BlockItemSetUnparsed;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.PublishStreamRequestUnparsed;
import org.hiero.block.node.app.fixtures.blocks.SimpleTestBlockItemBuilder;
import org.hiero.block.node.app.fixtures.plugintest.TestBlockMessagingFacility;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.NewestBlockKnownToNetworkNotification;
import org.hiero.block.node.spi.blockmessaging.PersistedNotification;
import org.hiero.block.node.stream.publisher.PublisherHandler.MetricsHolder;
import org.hiero.block.node.stream.publisher.StreamPublisherManager.BlockAction;
import org.hiero.block.node.stream.publisher.fixtures.TestResponsePipeline;
import org.hiero.block.node.stream.publisher.fixtures.TestStreamPublisherManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Tests for {@link PublisherHandler}.
 */
@DisplayName("PublisherHandler Tests")
class PublisherHandlerTest {
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
        private TestResponsePipeline validReplyPipeline;
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
            validTranserQueue = new LinkedBlockingQueue<>();
        }

        /**
         * This test aims to assert that the constructor of {@link PublisherHandler} does not
         * throw any exceptions when provided with valid parameters.
         */
        @Test
        @DisplayName("Test constructor with valid parameters")
        void testValidParameters() {
            assertThatNoException().isThrownBy(() -> {
                new PublisherHandler(
                        validNextId, validReplyPipeline, validMetricsHodler, validPublisherManager, validTranserQueue);
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
                new PublisherHandler(validNextId, null, validMetricsHodler, validPublisherManager, validTranserQueue);
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
                new PublisherHandler(validNextId, validReplyPipeline, null, validPublisherManager, validTranserQueue);
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
                new PublisherHandler(validNextId, validReplyPipeline, validMetricsHodler, null, validTranserQueue);
            });
        }

        /**
         * This test aims to assert that the constructor of {@link PublisherHandler} throws a
         * {@link NullPointerException} when the transfer queue is null.
         */
        @Test
        @DisplayName("Test constructor with null transfer queue")
        void testNullTransferQueue() {
            assertThatNullPointerException().isThrownBy(() -> {
                new PublisherHandler(validNextId, validReplyPipeline, validMetricsHodler, validPublisherManager, null);
            });
        }
    }

    /**
     * Functionality tests for {@link PublisherHandler}.
     */
    @Nested
    @DisplayName("Functionality Tests")
    class FunctionalityTest {
        /** Handler ID used for the tests. */
        private long handlerId;
        /** Test response pipeline used for asserting the handler's responses. */
        private TestResponsePipeline repliesPipeline;
        /** Metrics holder used for asserting the handler's metrics. */
        private MetricsHolder metrics;
        /** Test publisher manager used within the handler to test. */
        private TestStreamPublisherManager manager;
        /** Transfer queue used for asserting the items offered by the handler. */
        private TransferQueue<BlockItemSetUnparsed> transferQueue;
        /** The handler under test. */
        private PublisherHandler toTest;

        // ASSERTION EXTRACTORS
        private final Function<PublishStreamResponse, ResponseOneOfType> responseKindExtractor =
                response -> response.response().kind();
        private final Function<PublishStreamResponse, Code> endStreamResponseCodeExtractor =
                response -> Objects.requireNonNull(response.endStream()).status();
        private final Function<PublishStreamResponse, Long> endStreamBlockNumberExtractor =
                response -> Objects.requireNonNull(response.endStream()).blockNumber();
        private final Function<PublishStreamResponse, Long> skipBlockNumberExtractor =
                response -> Objects.requireNonNull(response.skipBlock()).blockNumber();
        private final Function<PublishStreamResponse, Long> resendBlockNumberExtractor =
                response -> Objects.requireNonNull(response.resendBlock()).blockNumber();
        private final Function<PublishStreamResponse, Long> acknowledgementBlockNumberExtractor =
                response -> Objects.requireNonNull(response.acknowledgement()).blockNumber();

        /**
         * Environment setup executed before each test in this nested class.
         */
        @BeforeEach
        void setup() {
            handlerId = 1L;
            repliesPipeline = new TestResponsePipeline();
            metrics = createMetrics();
            manager = new TestStreamPublisherManager(new TestBlockMessagingFacility());
            transferQueue = new LinkedTransferQueue<>();
            toTest = new PublisherHandler(handlerId, repliesPipeline, metrics, manager, transferQueue);
            manager.addHandler(toTest);
        }

        /**
         * Tests for {@link PublisherHandler#onNext(PublishStreamRequestUnparsed)}.
         */
        @Nested
        @DisplayName("onNext() Tests")
        class OnNextTest {
            /**
             * This test aims to assert that the {@link PublisherHandler} correctly
             * handles a received request
             * {@link PublisherHandler#onNext(PublishStreamRequestUnparsed)} happy
             * path scenario. Here we stream a single complete valid block as items.
             * The request's first item is the block header for the streamed block,
             * and the last item is the block proof for the streamed block.
             * We expect that when the {@link StreamPublisherManager} returns
             * {@link BlockAction#ACCEPT} for the streamed block number, the items
             * will be offered to the transfer queue.
             */
            @Test
            @DisplayName(
                    "Test onNext() with valid request with a complete single block items streamed - happy path ACCEPT")
            void testOnNextHappyPathACCEPT() {
                // Setup request to send, in this case a single complete valid block
                // as items, starting with header and ending with proof
                final int streamedBlockNumber = 0;
                final BlockItemUnparsed[] blockItems =
                        SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsed(
                                streamedBlockNumber, streamedBlockNumber + 1);
                final BlockItemSetUnparsed blockItemSet =
                        BlockItemSetUnparsed.newBuilder().blockItems(blockItems).build();
                final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(blockItemSet)
                        .build();
                // Train the manager to return ACCEPT for the block number
                manager.setBlockAction(BlockAction.ACCEPT);
                // Call
                toTest.onNext(request);
                // Assert items offered to the transfer queue
                assertThat(transferQueue).hasSize(1).containsExactly(blockItemSet);
            }

            /**
             * This test aims to assert that the {@link PublisherHandler} correctly
             * handles a received request
             * {@link PublisherHandler#onNext(PublishStreamRequestUnparsed)} happy
             * path scenario. Here we stream a single complete valid block as items.
             * The request's first item is the block header for the streamed block,
             * and the last item is the block proof for the streamed block.
             * We expect that when the {@link StreamPublisherManager} returns
             * {@link BlockAction#ACCEPT} for the streamed block number no replies
             * to the pipeline are made.
             */
            @Test
            @DisplayName(
                    "Test onNext() with valid request with a complete single block no replies to pipeline - happy path ACCEPT")
            void testOnNextNoRepliesHappyPathACCEPT() {
                // Setup request to send, in this case a single complete valid block
                // as items, starting with header and ending with proof
                final int streamedBlockNumber = 0;
                final BlockItemUnparsed[] blockItems =
                        SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsed(
                                streamedBlockNumber, streamedBlockNumber + 1);
                final BlockItemSetUnparsed blockItemSet =
                        BlockItemSetUnparsed.newBuilder().blockItems(blockItems).build();
                final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(blockItemSet)
                        .build();
                // Train the manager to return ACCEPT for the block number
                manager.setBlockAction(BlockAction.ACCEPT);
                // Call
                toTest.onNext(request);
                // Assert no replies sent to the pipeline
                assertThat(repliesPipeline.getOnNextCalls()).isEmpty();
                assertThat(repliesPipeline.getOnErrorCalls()).isEmpty();
                assertThat(repliesPipeline.getOnSubscriptionCalls()).isEmpty();
                assertThat(repliesPipeline.getOnCompleteCalls().get()).isEqualTo(0);
                assertThat(repliesPipeline.getClientEndStreamCalls().get()).isEqualTo(0);
            }

            /**
             * This test aims to assert that the {@link PublisherHandler} correctly
             * handles a received request
             * {@link PublisherHandler#onNext(PublishStreamRequestUnparsed)} happy
             * path scenario. Here we stream a single complete valid block as items.
             * The request's first item is the block header for the streamed block,
             * and the last item is the block proof for the streamed block.
             * We expect that when the {@link StreamPublisherManager} returns
             * {@link BlockAction#ACCEPT} for the streamed block number the metrics
             * will be properly updated.
             */
            @Test
            @DisplayName(
                    "Test onNext() with valid request with a complete single block metrics updated - happy path ACCEPT")
            void testOnNextMetricsHappyPathACCEPT() {
                // Setup request to send, in this case a single complete valid block
                // as items, starting with header and ending with proof
                final int streamedBlockNumber = 0;
                final BlockItemUnparsed[] blockItems =
                        SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsed(
                                streamedBlockNumber, streamedBlockNumber + 1);
                final BlockItemSetUnparsed blockItemSet =
                        BlockItemSetUnparsed.newBuilder().blockItems(blockItems).build();
                final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(blockItemSet)
                        .build();
                // Train the manager to return ACCEPT for the block number
                manager.setBlockAction(BlockAction.ACCEPT);
                // Call
                toTest.onNext(request);
                // Assert metrics updated
                assertThat(metrics.liveBlockItemsReceived().get()).isEqualTo(blockItems.length);
                // Assert other metrics unchanged
                assertThat(metrics.blockAcknowledgementsSent().get()).isEqualTo(0);
                assertThat(metrics.streamErrors().get()).isEqualTo(0);
                assertThat(metrics.blockSkipsSent().get()).isEqualTo(0);
                assertThat(metrics.blockResendsSent().get()).isEqualTo(0);
                assertThat(metrics.endOfStreamsSent().get()).isEqualTo(0);
                assertThat(metrics.sendResponseFailed().get()).isEqualTo(0);
                assertThat(metrics.endStreamsReceived().get()).isEqualTo(0);
            }

            /**
             * This test aims to assert that the {@link PublisherHandler} correctly
             * handles a received request
             * {@link PublisherHandler#onNext(PublishStreamRequestUnparsed)} happy
             * path scenario. Here we stream a single complete valid block as items.
             * The request's first item is the block header for the streamed block,
             * and the last item is the block proof for the streamed block.
             * We expect that when the {@link StreamPublisherManager} returns
             * {@link BlockAction#ACCEPT} for the streamed block number no replies
             * to the pipeline are made. After a {@link PersistedNotification} for
             * the streamed block is published to the manager, the handler will
             * send an acknowledgement response.
             */
            @Test
            @DisplayName(
                    "Test onNext() with valid request with a complete single block sends acknowledgement after persisted - happy path ACCEPT")
            void testOnNextHappyPathResponseACCEPT() {
                // Setup request to send, in this case a single complete valid block
                // as items, starting with header and ending with proof
                final int expectedStreamedBlockNumber = 0;
                final BlockItemUnparsed[] blockItems =
                        SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsed(
                                expectedStreamedBlockNumber, expectedStreamedBlockNumber + 1);
                final BlockItemSetUnparsed blockItemSet =
                        BlockItemSetUnparsed.newBuilder().blockItems(blockItems).build();
                final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(blockItemSet)
                        .build();
                // Train the manager to return ACCEPT for the block number
                manager.setBlockAction(BlockAction.ACCEPT);
                // Call
                toTest.onNext(request);
                // Assert no replies sent to the pipeline
                assertThat(repliesPipeline.getOnNextCalls()).isEmpty();
                assertThat(repliesPipeline.getOnErrorCalls()).isEmpty();
                assertThat(repliesPipeline.getOnSubscriptionCalls()).isEmpty();
                assertThat(repliesPipeline.getOnCompleteCalls().get()).isEqualTo(0);
                assertThat(repliesPipeline.getClientEndStreamCalls().get()).isEqualTo(0);
                // Publish the persisted notification for the streamed block
                manager.handlePersisted(new PersistedNotification(
                        expectedStreamedBlockNumber, expectedStreamedBlockNumber, 0, BlockSource.PUBLISHER));
                // Assert the acknowledgement response is sent to the pipeline
                // Assert that an acknowledgement was sent for the valid request
                assertThat(repliesPipeline.getOnNextCalls())
                        .hasSize(1)
                        .first()
                        .returns(ResponseOneOfType.ACKNOWLEDGEMENT, responseKindExtractor)
                        .returns((long) expectedStreamedBlockNumber, acknowledgementBlockNumberExtractor);
                // Assert no other responses sent to the pipeline
                assertThat(repliesPipeline.getOnErrorCalls()).isEmpty();
                assertThat(repliesPipeline.getOnSubscriptionCalls()).isEmpty();
                assertThat(repliesPipeline.getOnCompleteCalls().get()).isEqualTo(0);
                assertThat(repliesPipeline.getClientEndStreamCalls().get()).isEqualTo(0);
            }

            /**
             * This test aims to assert that the {@link PublisherHandler} correctly
             * handles a received request
             * {@link PublisherHandler#onNext(PublishStreamRequestUnparsed)} happy
             * path scenario. Here we stream a single complete valid block as items.
             * The request's first item is the block header for the streamed block,
             * and the last item is the block proof for the streamed block.
             * We expect that when the {@link StreamPublisherManager} returns
             * any action for the streamed block number and when the
             * streamed items end with a valid block proof, the handler will send
             * a signal to the manager to close the block.
             */
            @ParameterizedTest
            @EnumSource(value = BlockAction.class)
            @DisplayName(
                    "Test onNext() with valid request with a complete single block calls closeBlock on manager when batch ends with valid proof - happy path ACCEPT")
            void testOnNextCloseBlockValidProofHappyPathACCEPT(final BlockAction action) {
                // Setup request to send, in this case a single complete valid block
                // as items, starting with header and ending with proof
                final int streamedBlockNumber = 0;
                final BlockItemUnparsed[] blockItems =
                        SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsed(
                                streamedBlockNumber, streamedBlockNumber + 1);
                final BlockItemSetUnparsed blockItemSet =
                        BlockItemSetUnparsed.newBuilder().blockItems(blockItems).build();
                final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(blockItemSet)
                        .build();
                // Train the manager to return the current action for the block number
                manager.setBlockAction(action);
                // Call
                toTest.onNext(request);
                // Assert that the manager's closeBlock method was called
                assertThat(manager.closeBlockCallsForHandler(handlerId)).isOne();
                assertThat(manager.nullCloseBlockCallsForHandler(handlerId)).isEqualTo(-1);
            }

            /**
             * This test aims to assert that the {@link PublisherHandler} correctly
             * handles a received request
             * {@link PublisherHandler#onNext(PublishStreamRequestUnparsed)}. Here we stream a single complete valid block as items.
             * The request's first item is the block header for the streamed block,
             * and the last item is the block proof for the streamed block.
             * We expect that when the {@link StreamPublisherManager} returns
             * any action for the streamed block number and when the
             * streamed items end with a valid block proof, the handler will send
             * a signal to the manager to close the block.
             */
            @ParameterizedTest
            @EnumSource(value = BlockAction.class)
            @DisplayName(
                    "Test onNext() with valid request with a complete single block calls closeBlock on manager when batch ends with broken proof - ACCEPT")
            void testOnNextCloseBlockBrokenProofACCEPT(final BlockAction action) {
                // Setup request to send, in this case a single complete valid block
                // as items, starting with header and ending with a broken proof
                final int streamedBlockNumber = 0;
                final BlockItemUnparsed[] blockItems =
                        SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsedWithBrokenProofs(
                                streamedBlockNumber, streamedBlockNumber + 1);
                final BlockItemSetUnparsed blockItemSet =
                        BlockItemSetUnparsed.newBuilder().blockItems(blockItems).build();
                final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(blockItemSet)
                        .build();
                // Train the manager to return the current action for the block number
                manager.setBlockAction(action);
                // Call
                toTest.onNext(request);
                // Assert that the manager's closeBlock method was called
                assertThat(manager.closeBlockCallsForHandler(handlerId)).isEqualTo(-1);
                assertThat(manager.nullCloseBlockCallsForHandler(handlerId)).isOne();
            }

            /**
             * This test aims to assert that the {@link PublisherHandler} correctly
             * handles received requests
             * {@link PublisherHandler#onNext(PublishStreamRequestUnparsed)} happy
             * path scenario. Here we stream a single complete valid block as items.
             * We stream two requests. The first request's first item is the block
             * header for the streamed block. The second request's last item is the
             * block proof for the streamed block. We expect that when the
             * {@link StreamPublisherManager} returns {@link BlockAction#ACCEPT} for
             * the streamed block number, the items will be offered to the transfer
             * queue.
             */
            @Test
            @DisplayName(
                    "Test onNext() with valid two requests with a complete single block items streamed - happy path ACCEPT")
            void testOnNextConsecutiveRequestsHappyPathACCEPT() {
                // Create the block to stream, a single complete valid block
                final int streamedBlockNumber = 0;
                final BlockItemUnparsed[] blockItems =
                        SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsed(
                                streamedBlockNumber, streamedBlockNumber + 1);
                final int mid = blockItems.length / 2;
                // Build the first request with the first half of the block items
                final BlockItemSetUnparsed blockItemSet1 = BlockItemSetUnparsed.newBuilder()
                        .blockItems(Arrays.copyOfRange(blockItems, 0, mid))
                        .build();
                final PublishStreamRequestUnparsed request1 = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(blockItemSet1)
                        .build();
                // Train the manager to return ACCEPT for the block number
                manager.setBlockAction(BlockAction.ACCEPT);
                // First call
                toTest.onNext(request1);
                // Assert items offered to the transfer queue
                assertThat(transferQueue).hasSize(1).containsExactly(blockItemSet1);
                // Build the second request with the second half of the block items
                final BlockItemSetUnparsed blockItemSet2 = BlockItemSetUnparsed.newBuilder()
                        .blockItems(Arrays.copyOfRange(blockItems, mid, blockItems.length))
                        .build();
                final PublishStreamRequestUnparsed request2 = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(blockItemSet2)
                        .build();
                // Second call
                toTest.onNext(request2);
                // Assert items offered to the transfer queue
                assertThat(transferQueue).hasSize(2).containsExactly(blockItemSet1, blockItemSet2);
            }

            /**
             * This test aims to assert that the {@link PublisherHandler} correctly
             * handles received requests
             * {@link PublisherHandler#onNext(PublishStreamRequestUnparsed)} happy
             * path scenario. Here we stream a single complete valid block as items.
             * We stream two requests. The first request's first item is the block
             * header for the streamed block. The second request's last item is the
             * block proof for the streamed block. We expect that when the
             * {@link StreamPublisherManager} returns {@link BlockAction#ACCEPT} for
             * the streamed block number the metrics will be properly updated.
             */
            @Test
            @DisplayName(
                    "Test onNext() with valid two requests with a complete single block metrics updated - happy path ACCEPT")
            void testOnNextConsecutiveRequestsMetricsHappyPathACCEPT() {
                // Create the block to stream, a single complete valid block
                final int streamedBlockNumber = 0;
                final BlockItemUnparsed[] blockItems =
                        SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsed(
                                streamedBlockNumber, streamedBlockNumber + 1);
                final int mid = blockItems.length / 2;
                // Build the first request with the first half of the block items
                final BlockItemSetUnparsed blockItemSet1 = BlockItemSetUnparsed.newBuilder()
                        .blockItems(Arrays.copyOfRange(blockItems, 0, mid))
                        .build();
                final PublishStreamRequestUnparsed request1 = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(blockItemSet1)
                        .build();
                // Train the manager to return ACCEPT for the block number
                manager.setBlockAction(BlockAction.ACCEPT);
                // First call
                toTest.onNext(request1);
                // Assert metrics updated
                assertThat(metrics.liveBlockItemsReceived().get()).isEqualTo(mid);
                // Build the second request with the second half of the block items
                final BlockItemSetUnparsed blockItemSet2 = BlockItemSetUnparsed.newBuilder()
                        .blockItems(Arrays.copyOfRange(blockItems, mid, blockItems.length))
                        .build();
                final PublishStreamRequestUnparsed request2 = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(blockItemSet2)
                        .build();
                // Second call
                toTest.onNext(request2);
                // Assert metrics updated
                assertThat(metrics.liveBlockItemsReceived().get()).isEqualTo(blockItems.length);
                // Assert other metrics unchanged
                assertThat(metrics.blockAcknowledgementsSent().get()).isEqualTo(0);
                assertThat(metrics.streamErrors().get()).isEqualTo(0);
                assertThat(metrics.blockSkipsSent().get()).isEqualTo(0);
                assertThat(metrics.blockResendsSent().get()).isEqualTo(0);
                assertThat(metrics.endOfStreamsSent().get()).isEqualTo(0);
                assertThat(metrics.sendResponseFailed().get()).isEqualTo(0);
                assertThat(metrics.endStreamsReceived().get()).isEqualTo(0);
            }

            /**
             * This test aims to assert that the {@link PublisherHandler} correctly
             * handles a received request
             * {@link PublisherHandler#onNext(PublishStreamRequestUnparsed)}. Here we stream a single complete valid block as items.
             * The request's first item is the block header for the streamed block,
             * and the last item is the block proof for the streamed block.
             * We expect that when the {@link StreamPublisherManager} returns
             * {@link BlockAction#SKIP} for the streamed block number no items will
             * be offered to the transfer queue.
             */
            @Test
            @DisplayName(
                    "Test onNext() with valid request with a complete single block no items offered to transfer queue - SKIP")
            void testOnNextSKIP() {
                // Setup request to send, in this case a single complete valid block
                // as items, starting with header and ending with proof
                final int streamedBlockNumber = 0;
                final BlockItemUnparsed[] blockItems =
                        SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsed(
                                streamedBlockNumber, streamedBlockNumber + 1);
                final BlockItemSetUnparsed blockItemSet =
                        BlockItemSetUnparsed.newBuilder().blockItems(blockItems).build();
                final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(blockItemSet)
                        .build();
                // Train the manager to return SKIP for the block number
                manager.setBlockAction(BlockAction.SKIP);
                // Call
                toTest.onNext(request);
                // Assert no items offered to the transfer queue
                assertThat(transferQueue).isEmpty();
            }

            /**
             * This test aims to assert that the {@link PublisherHandler} correctly
             * handles a received request
             * {@link PublisherHandler#onNext(PublishStreamRequestUnparsed)}. Here we stream a single complete valid block as items.
             * The request's first item is the block header for the streamed block,
             * and the last item is the block proof for the streamed block.
             * We expect that when the {@link StreamPublisherManager} returns
             * {@link BlockAction#SKIP} for the streamed block number the handler
             * will reply with a
             * {@link org.hiero.block.api.PublishStreamResponse.SkipBlock}.
             */
            @Test
            @DisplayName("Test onNext() with valid request with a complete single block response SkipBlock - SKIP")
            void testOnNextResponseSKIP() {
                // Setup request to send, in this case a single complete valid block
                // as items, starting with header and ending with proof
                final int streamedBlockNumber = 0;
                final BlockItemUnparsed[] blockItems =
                        SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsed(
                                streamedBlockNumber, streamedBlockNumber + 1);
                final BlockItemSetUnparsed blockItemSet =
                        BlockItemSetUnparsed.newBuilder().blockItems(blockItems).build();
                final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(blockItemSet)
                        .build();
                // Train the manager to return SKIP for the block number
                manager.setBlockAction(BlockAction.SKIP);
                // Call
                toTest.onNext(request);
                // Assert single response is SkipBlock with block number same as streamed
                assertThat(repliesPipeline.getOnNextCalls())
                        .hasSize(1)
                        .first()
                        .returns(ResponseOneOfType.SKIP_BLOCK, responseKindExtractor)
                        .returns((long) streamedBlockNumber, skipBlockNumberExtractor);
                // Assert no other responses sent
                assertThat(repliesPipeline.getOnErrorCalls()).isEmpty();
                assertThat(repliesPipeline.getOnSubscriptionCalls()).isEmpty();
                assertThat(repliesPipeline.getOnCompleteCalls().get()).isEqualTo(0);
                assertThat(repliesPipeline.getClientEndStreamCalls().get()).isEqualTo(0);
            }

            /**
             * This test aims to assert that the {@link PublisherHandler} correctly
             * handles a received request
             * {@link PublisherHandler#onNext(PublishStreamRequestUnparsed)}. Here we stream a single complete valid block as items.
             * The request's first item is the block header for the streamed block,
             * and the last item is the block proof for the streamed block.
             * We expect that when the {@link StreamPublisherManager} returns
             * {@link BlockAction#SKIP} for the streamed block number the metrics
             * will be properly updated.
             */
            @Test
            @DisplayName("Test onNext() with valid request with a complete single block metrics updated - SKIP")
            void testOnNextMetricsSKIP() {
                // Setup request to send, in this case a single complete valid block
                // as items, starting with header and ending with proof
                final int streamedBlockNumber = 0;
                final BlockItemUnparsed[] blockItems =
                        SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsed(
                                streamedBlockNumber, streamedBlockNumber + 1);
                final BlockItemSetUnparsed blockItemSet =
                        BlockItemSetUnparsed.newBuilder().blockItems(blockItems).build();
                final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(blockItemSet)
                        .build();
                // Train the manager to return SKIP for the block number
                manager.setBlockAction(BlockAction.SKIP);
                // Call
                toTest.onNext(request);
                // Assert metrics updated
                assertThat(metrics.blockSkipsSent().get()).isEqualTo(1);
                // Assert other metrics unchanged
                assertThat(metrics.liveBlockItemsReceived().get()).isEqualTo(0);
                assertThat(metrics.blockAcknowledgementsSent().get()).isEqualTo(0);
                assertThat(metrics.streamErrors().get()).isEqualTo(0);
                assertThat(metrics.blockResendsSent().get()).isEqualTo(0);
                assertThat(metrics.endOfStreamsSent().get()).isEqualTo(0);
                assertThat(metrics.sendResponseFailed().get()).isEqualTo(0);
                assertThat(metrics.endStreamsReceived().get()).isEqualTo(0);
            }

            /**
             * This test aims to assert that the {@link PublisherHandler} correctly
             * handles a received request
             * {@link PublisherHandler#onNext(PublishStreamRequestUnparsed)}. Here we stream a single complete valid block as items.
             * The request's first item is the block header for the streamed block,
             * and the last item is the block proof for the streamed block.
             * We expect that when the {@link StreamPublisherManager} returns
             * {@link BlockAction#RESEND} for the streamed block number no items
             * will be offered to the transfer queue.
             */
            @Test
            @DisplayName(
                    "Test onNext() with valid request with a complete single block no items offered to transfer queue - RESEND")
            void testOnNextRESEND() {
                // Setup request to send, in this case a single complete valid block
                // as items, starting with header and ending with proof
                final int streamedBlockNumber = 0;
                final BlockItemUnparsed[] blockItems =
                        SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsed(
                                streamedBlockNumber, streamedBlockNumber + 1);
                final BlockItemSetUnparsed blockItemSet =
                        BlockItemSetUnparsed.newBuilder().blockItems(blockItems).build();
                final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(blockItemSet)
                        .build();
                // Train the manager to return RESEND for the block number
                manager.setBlockAction(BlockAction.RESEND);
                // Call
                toTest.onNext(request);
                // Assert no items offered to the transfer queue
                assertThat(transferQueue).isEmpty();
            }

            /**
             * This test aims to assert that the {@link PublisherHandler} correctly
             * handles a received request
             * {@link PublisherHandler#onNext(PublishStreamRequestUnparsed)}. Here we stream a single complete valid block as items.
             * The request's first item is the block header for the streamed block,
             * and the last item is the block proof for the streamed block.
             * We expect that when the {@link StreamPublisherManager} returns
             * {@link BlockAction#RESEND} for the streamed block number the handler
             * will reply with a
             * {@link org.hiero.block.api.PublishStreamResponse.ResendBlock}.
             */
            @Test
            @DisplayName("Test onNext() with valid request with a complete single block response ResendBlock - RESEND")
            void testOnNextResponseRESEND() {
                // Setup request to send, in this case a single complete valid block
                // as items, starting with header and ending with proof
                final int streamedBlockNumber = 0;
                final BlockItemUnparsed[] blockItems =
                        SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsed(
                                streamedBlockNumber, streamedBlockNumber + 1);
                final BlockItemSetUnparsed blockItemSet =
                        BlockItemSetUnparsed.newBuilder().blockItems(blockItems).build();
                final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(blockItemSet)
                        .build();
                // Train the manager to return RESEND for the block number
                manager.setBlockAction(BlockAction.RESEND);
                // Train the manager to return the expected latest block number
                final long latestBlockNumber = 10L; // Example latest block number
                final long expectedResponseBlockNumber = latestBlockNumber + 1L;
                manager.setLatestBlockNumber(latestBlockNumber);
                // Call
                toTest.onNext(request);
                // Assert single response is ResendBlock with block number same as streamed
                assertThat(repliesPipeline.getOnNextCalls())
                        .hasSize(1)
                        .first()
                        .returns(ResponseOneOfType.RESEND_BLOCK, responseKindExtractor)
                        .returns(expectedResponseBlockNumber, resendBlockNumberExtractor);
                // Assert no other responses sent
                assertThat(repliesPipeline.getOnErrorCalls()).isEmpty();
                assertThat(repliesPipeline.getOnSubscriptionCalls()).isEmpty();
                assertThat(repliesPipeline.getOnCompleteCalls().get()).isEqualTo(0);
                assertThat(repliesPipeline.getClientEndStreamCalls().get()).isEqualTo(0);
            }

            /**
             * This test aims to assert that the {@link PublisherHandler} correctly
             * handles a received request
             * {@link PublisherHandler#onNext(PublishStreamRequestUnparsed)}. Here we stream a single complete valid block as items.
             * The request's first item is the block header for the streamed block,
             * and the last item is the block proof for the streamed block.
             * We expect that when the {@link StreamPublisherManager} returns
             * {@link BlockAction#RESEND} for the streamed block number the metrics
             * will be properly updated.
             */
            @Test
            @DisplayName(
                    "Test onNext() with valid request with a complete single block metrics updated ResendBlock - RESEND")
            void testOnNextMetricsRESEND() {
                // Setup request to send, in this case a single complete valid block
                // as items, starting with header and ending with proof
                final int streamedBlockNumber = 0;
                final BlockItemUnparsed[] blockItems =
                        SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsed(
                                streamedBlockNumber, streamedBlockNumber + 1);
                final BlockItemSetUnparsed blockItemSet =
                        BlockItemSetUnparsed.newBuilder().blockItems(blockItems).build();
                final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(blockItemSet)
                        .build();
                // Train the manager to return RESEND for the block number
                manager.setBlockAction(BlockAction.RESEND);
                // Train the manager to return the expected latest block number
                final long latestBlockNumber = 10L; // Example latest block number
                manager.setLatestBlockNumber(latestBlockNumber);
                // Call
                toTest.onNext(request);
                // Assert metrics updated
                assertThat(metrics.blockResendsSent().get()).isEqualTo(1);
                // Assert other metrics unchanged
                assertThat(metrics.liveBlockItemsReceived().get()).isEqualTo(0);
                assertThat(metrics.blockAcknowledgementsSent().get()).isEqualTo(0);
                assertThat(metrics.streamErrors().get()).isEqualTo(0);
                assertThat(metrics.blockSkipsSent().get()).isEqualTo(0);
                assertThat(metrics.endOfStreamsSent().get()).isEqualTo(0);
                assertThat(metrics.sendResponseFailed().get()).isEqualTo(0);
                assertThat(metrics.endStreamsReceived().get()).isEqualTo(0);
            }

            /**
             * This test aims to assert that the {@link PublisherHandler} correctly
             * handles a received request
             * {@link PublisherHandler#onNext(PublishStreamRequestUnparsed)}. Here we stream a single complete valid block as items.
             * The request's first item is the block header for the streamed block,
             * and the last item is the block proof for the streamed block.
             * We expect that when the {@link StreamPublisherManager} returns
             * {@link BlockAction#END_DUPLICATE} for the streamed block number no
             * items will be offered to the transfer queue.
             */
            @Test
            @DisplayName(
                    "Test onNext() with valid request with a complete single block no items offered to transfer queue - EndOfStream, Code DUPLICATE_BLOCK")
            void testOnNextDUPLICATE() {
                // Setup request to send, in this case a single complete valid block
                // as items, starting with header and ending with proof
                final int streamedBlockNumber = 0;
                final BlockItemUnparsed[] blockItems =
                        SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsed(
                                streamedBlockNumber, streamedBlockNumber + 1);
                final BlockItemSetUnparsed blockItemSet =
                        BlockItemSetUnparsed.newBuilder().blockItems(blockItems).build();
                final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(blockItemSet)
                        .build();
                // Train the manager to return END_DUPLICATE for the block number
                manager.setBlockAction(BlockAction.END_DUPLICATE);
                // Call
                toTest.onNext(request);
                // Assert no items offered to the transfer queue
                assertThat(transferQueue).isEmpty();
            }

            /**
             * This test aims to assert that the {@link PublisherHandler} correctly
             * handles a received request
             * {@link PublisherHandler#onNext(PublishStreamRequestUnparsed)}. Here we stream a single complete valid block as items.
             * The request's first item is the block header for the streamed block,
             * and the last item is the block proof for the streamed block.
             * We expect that when the {@link StreamPublisherManager} returns
             * {@link BlockAction#END_DUPLICATE} for the streamed block number the
             * handler will reply with a
             * {@link org.hiero.block.api.PublishStreamResponse.EndOfStream}
             * with {@link org.hiero.block.api.PublishStreamResponse.EndOfStream.Code#DUPLICATE_BLOCK}.
             */
            @Test
            @DisplayName(
                    "Test onNext() with valid request with a complete single block response EndOfStream, Code DUPLICATE_BLOCK - EndOfStream, Code DUPLICATE_BLOCK")
            void testOnNextResponseDUPLICATE() {
                // Setup request to send, in this case a single complete valid block
                // as items, starting with header and ending with proof
                final int streamedBlockNumber = 0;
                final BlockItemUnparsed[] blockItems =
                        SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsed(
                                streamedBlockNumber, streamedBlockNumber + 1);
                final BlockItemSetUnparsed blockItemSet =
                        BlockItemSetUnparsed.newBuilder().blockItems(blockItems).build();
                final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(blockItemSet)
                        .build();
                // Train the manager to return END_DUPLICATE for the block number
                manager.setBlockAction(BlockAction.END_DUPLICATE);
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

            /**
             * This test aims to assert that the {@link PublisherHandler} correctly
             * handles a received request
             * {@link PublisherHandler#onNext(PublishStreamRequestUnparsed)}. Here we stream a single complete valid block as items.
             * The request's first item is the block header for the streamed block,
             * and the last item is the block proof for the streamed block.
             * We expect that when the {@link StreamPublisherManager} returns
             * {@link BlockAction#END_DUPLICATE} for the streamed block number the
             * metrics will be properly updated.
             */
            @Test
            @DisplayName(
                    "Test onNext() with valid request with a complete single block metrics updated EndOfStream, Code DUPLICATE_BLOCK - EndOfStream, Code DUPLICATE_BLOCK")
            void testOnNextMetricsDUPLICATE() {
                // Setup request to send, in this case a single complete valid block
                // as items, starting with header and ending with proof
                final int streamedBlockNumber = 0;
                final BlockItemUnparsed[] blockItems =
                        SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsed(
                                streamedBlockNumber, streamedBlockNumber + 1);
                final BlockItemSetUnparsed blockItemSet =
                        BlockItemSetUnparsed.newBuilder().blockItems(blockItems).build();
                final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(blockItemSet)
                        .build();
                // Train the manager to return END_DUPLICATE for the block number
                manager.setBlockAction(BlockAction.END_DUPLICATE);
                // Train the manager to return the expected latest block number
                final long expectedLatestBlockNumber = 10L; // Example latest block number
                manager.setLatestBlockNumber(expectedLatestBlockNumber);
                // Call
                toTest.onNext(request);
                // Assert metrics updated
                assertThat(metrics.endOfStreamsSent().get()).isEqualTo(1);
                // Assert other metrics unchanged
                assertThat(metrics.liveBlockItemsReceived().get()).isEqualTo(0);
                assertThat(metrics.blockAcknowledgementsSent().get()).isEqualTo(0);
                assertThat(metrics.streamErrors().get()).isEqualTo(0);
                assertThat(metrics.blockSkipsSent().get()).isEqualTo(0);
                assertThat(metrics.blockResendsSent().get()).isEqualTo(0);
                assertThat(metrics.sendResponseFailed().get()).isEqualTo(0);
                assertThat(metrics.endStreamsReceived().get()).isEqualTo(0);
            }

            /**
             * This test aims to assert that the {@link PublisherHandler} correctly
             * handles a received request
             * {@link PublisherHandler#onNext(PublishStreamRequestUnparsed)}. Here we stream a single complete valid block as items.
             * The request's first item is the block header for the streamed block,
             * and the last item is the block proof for the streamed block.
             * We expect that when the {@link StreamPublisherManager} returns
             * {@link BlockAction#END_BEHIND} for the streamed block number no items
             * will be offered to the transfer queue, and the handler will reply
             */
            @Test
            @DisplayName(
                    "Test onNext() with valid request with a complete single block no items offered to transfer queue - EndOfStream, Code BEHIND")
            void testOnNextBEHIND() {
                // Setup request to send, in this case a single complete valid block
                // as items, starting with header and ending with proof
                final int streamedBlockNumber = 0;
                final BlockItemUnparsed[] blockItems =
                        SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsed(
                                streamedBlockNumber, streamedBlockNumber + 1);
                final BlockItemSetUnparsed blockItemSet =
                        BlockItemSetUnparsed.newBuilder().blockItems(blockItems).build();
                final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(blockItemSet)
                        .build();
                // Train the manager to return END_BEHIND for the block number
                manager.setBlockAction(BlockAction.END_BEHIND);
                // Call
                toTest.onNext(request);
                // Assert no items offered to the transfer queue
                assertThat(transferQueue).isEmpty();
            }

            /**
             * This test aims to assert that the {@link PublisherHandler} correctly
             * handles a received request
             * {@link PublisherHandler#onNext(PublishStreamRequestUnparsed)}. Here we stream a single complete valid block as items.
             * The request's first item is the block header for the streamed block,
             * and the last item is the block proof for the streamed block.
             * We expect that when the {@link StreamPublisherManager} returns
             * {@link BlockAction#END_BEHIND} for the streamed block number the
             * handler will reply with a
             * {@link org.hiero.block.api.PublishStreamResponse.EndOfStream}
             * with {@link org.hiero.block.api.PublishStreamResponse.EndOfStream.Code#BEHIND}.
             */
            @Test
            @DisplayName(
                    "Test onNext() with valid request with a complete single block response EndOfStream, Code BEHIND - EndOfStream, Code BEHIND")
            void testOnNextResponseBEHIND() {
                // Setup request to send, in this case a single complete valid block
                // as items, starting with header and ending with proof
                final int streamedBlockNumber = 0;
                final BlockItemUnparsed[] blockItems =
                        SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsed(
                                streamedBlockNumber, streamedBlockNumber + 1);
                final BlockItemSetUnparsed blockItemSet =
                        BlockItemSetUnparsed.newBuilder().blockItems(blockItems).build();
                final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(blockItemSet)
                        .build();
                // Train the manager to return END_BEHIND for the block number
                manager.setBlockAction(BlockAction.END_BEHIND);
                // Train the manager to return the expected latest block number
                final long expectedLatestBlockNumber = 10L; // Example latest block number
                manager.setLatestBlockNumber(expectedLatestBlockNumber);
                // Call
                toTest.onNext(request);
                // Assert single response is BEHIND with block number same as latest known
                assertThat(repliesPipeline.getOnNextCalls())
                        .hasSize(1)
                        .first()
                        .returns(ResponseOneOfType.END_STREAM, responseKindExtractor)
                        .returns(Code.BEHIND, endStreamResponseCodeExtractor)
                        .returns(expectedLatestBlockNumber, endStreamBlockNumberExtractor);
                assertThat(repliesPipeline.getOnCompleteCalls().get()).isEqualTo(1);
                // Assert no other responses sent
                assertThat(repliesPipeline.getOnErrorCalls()).isEmpty();
                assertThat(repliesPipeline.getOnSubscriptionCalls()).isEmpty();
                assertThat(repliesPipeline.getClientEndStreamCalls().get()).isEqualTo(0);
            }

            /**
             * This test aims to assert that the {@link PublisherHandler} correctly
             * handles a received request
             * {@link PublisherHandler#onNext(PublishStreamRequestUnparsed)}. Here we stream a single complete valid block as items.
             * The request's first item is the block header for the streamed block,
             * and the last item is the block proof for the streamed block.
             * We expect that when the {@link StreamPublisherManager} returns
             * {@link BlockAction#END_BEHIND} for the streamed block number the
             * metrics will be properly updated.
             */
            @Test
            @DisplayName(
                    "Test onNext() with valid request with a complete single block metrics updated EndOfStream, Code BEHIND - EndOfStream, Code BEHIND")
            void testOnNextMetricsBEHIND() {
                // Setup request to send, in this case a single complete valid block
                // as items, starting with header and ending with proof
                final int streamedBlockNumber = 0;
                final BlockItemUnparsed[] blockItems =
                        SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsed(
                                streamedBlockNumber, streamedBlockNumber + 1);
                final BlockItemSetUnparsed blockItemSet =
                        BlockItemSetUnparsed.newBuilder().blockItems(blockItems).build();
                final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(blockItemSet)
                        .build();
                // Train the manager to return END_BEHIND for the block number
                manager.setBlockAction(BlockAction.END_BEHIND);
                // Train the manager to return the expected latest block number
                final long latestBlockNumber = 10L; // Example latest block number
                manager.setLatestBlockNumber(latestBlockNumber);
                // Call
                toTest.onNext(request);
                // Assert metrics updated
                assertThat(metrics.endOfStreamsSent().get()).isEqualTo(1);
                // Assert other metrics unchanged
                assertThat(metrics.liveBlockItemsReceived().get()).isEqualTo(0);
                assertThat(metrics.blockAcknowledgementsSent().get()).isEqualTo(0);
                assertThat(metrics.streamErrors().get()).isEqualTo(0);
                assertThat(metrics.blockSkipsSent().get()).isEqualTo(0);
                assertThat(metrics.blockResendsSent().get()).isEqualTo(0);
                assertThat(metrics.sendResponseFailed().get()).isEqualTo(0);
                assertThat(metrics.endStreamsReceived().get()).isEqualTo(0);
            }

            /**
             * This test aims to assert that the {@link PublisherHandler} correctly
             * handles a received request
             * {@link PublisherHandler#onNext(PublishStreamRequestUnparsed)}. Here we stream a single complete valid block as items.
             * The request's first item is the block header for the streamed block,
             * and the last item is the block proof for the streamed block.
             * We expect that when the {@link StreamPublisherManager} returns
             * {@link BlockAction#END_ERROR} for the streamed block number no items
             * will be offered to the transfer queue.
             */
            @Test
            @DisplayName(
                    "Test onNext() with valid request with a complete single block no items offered to transfer queue - EndOfStream, Code ERROR")
            void testOnNextERROR() {
                // Setup request to send, in this case a single complete valid block
                // as items, starting with header and ending with proof
                final int streamedBlockNumber = 0;
                final BlockItemUnparsed[] blockItems =
                        SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsed(
                                streamedBlockNumber, streamedBlockNumber + 1);
                final BlockItemSetUnparsed blockItemSet =
                        BlockItemSetUnparsed.newBuilder().blockItems(blockItems).build();
                final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(blockItemSet)
                        .build();
                // Train the manager to return END_ERROR for the block number
                manager.setBlockAction(BlockAction.END_ERROR);
                // Call
                toTest.onNext(request);
                // Assert no items propagated to the transfer queue
                assertThat(transferQueue).isEmpty();
            }

            /**
             * This test aims to assert that the {@link PublisherHandler} correctly
             * handles a received request
             * {@link PublisherHandler#onNext(PublishStreamRequestUnparsed)}. Here we stream a single complete valid block as items.
             * The request's first item is the block header for the streamed block,
             * and the last item is the block proof for the streamed block.
             * We expect that when the {@link StreamPublisherManager} returns
             * {@link BlockAction#END_ERROR} for the streamed block number the
             * handler will reply with a
             * {@link org.hiero.block.api.PublishStreamResponse.EndOfStream}
             * with {@link org.hiero.block.api.PublishStreamResponse.EndOfStream.Code#ERROR}.
             */
            @Test
            @DisplayName(
                    "Test onNext() with valid request with a complete single block response EndOfStream, Code ERROR - EndOfStream, Code ERROR")
            void testOnNextResponseERROR() {
                // Setup request to send, in this case a single complete valid block
                // as items, starting with header and ending with proof
                final int streamedBlockNumber = 0;
                final BlockItemUnparsed[] blockItems =
                        SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsed(
                                streamedBlockNumber, streamedBlockNumber + 1);
                final BlockItemSetUnparsed blockItemSet =
                        BlockItemSetUnparsed.newBuilder().blockItems(blockItems).build();
                final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(blockItemSet)
                        .build();
                // Train the manager to return END_ERROR for the block number
                manager.setBlockAction(BlockAction.END_ERROR);
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

            /**
             * This test aims to assert that the {@link PublisherHandler} correctly
             * handles a received request
             * {@link PublisherHandler#onNext(PublishStreamRequestUnparsed)}. Here we stream a single complete valid block as items.
             * The request's first item is the block header for the streamed block,
             * and the last item is the block proof for the streamed block.
             * We expect that when the {@link StreamPublisherManager} returns
             * {@link BlockAction#END_ERROR} for the streamed block number the
             * metrics will be properly updated.
             */
            @Test
            @DisplayName(
                    "Test onNext() with valid request with a complete single block metrics updated EndOfStream, Code ERROR - EndOfStream, Code ERROR")
            void testOnNextMetricsERROR() {
                // Setup request to send, in this case a single complete valid block
                // as items, starting with header and ending with proof
                final int streamedBlockNumber = 0;
                final BlockItemUnparsed[] blockItems =
                        SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsed(
                                streamedBlockNumber, streamedBlockNumber + 1);
                final BlockItemSetUnparsed blockItemSet =
                        BlockItemSetUnparsed.newBuilder().blockItems(blockItems).build();
                final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(blockItemSet)
                        .build();
                // Train the manager to return END_ERROR for the block number
                manager.setBlockAction(BlockAction.END_ERROR);
                // Train the manager to return the expected latest block number
                final long expectedLatestBlockNumber = 10L; // Example latest block number
                manager.setLatestBlockNumber(expectedLatestBlockNumber);
                // Call
                toTest.onNext(request);
                // Assert metrics updated
                assertThat(metrics.endOfStreamsSent().get()).isEqualTo(1);
                assertThat(metrics.streamErrors().get()).isEqualTo(1);
                // Assert other metrics unchanged
                assertThat(metrics.liveBlockItemsReceived().get()).isEqualTo(0);
                assertThat(metrics.blockAcknowledgementsSent().get()).isEqualTo(0);
                assertThat(metrics.blockSkipsSent().get()).isEqualTo(0);
                assertThat(metrics.blockResendsSent().get()).isEqualTo(0);
                assertThat(metrics.sendResponseFailed().get()).isEqualTo(0);
                assertThat(metrics.endStreamsReceived().get()).isEqualTo(0);
            }

            /**
             * This test aims to assert that the {@link PublisherHandler} correctly
             * handles a received invalid request
             * {@link PublisherHandler#onNext(PublishStreamRequestUnparsed)} when
             * block header is expected, but it is not present. Here, the handler
             * is in a state where it expects a block header to be the first item
             * in next request, but the request does not contain a block header as
             * first item. We expect that the handler will simply return w/o
             * propagating any items to the transfer queue.
             */
            @Test
            @DisplayName("Test onNext() with invalid request - no header, but expected - no items are propagated")
            void testOnNextReturnOnInvalidRequest() {
                // Setup request to send, in this case a single complete valid block
                // as items, starting with header and ending with proof
                final int streamedBlockNumber = 0;
                final BlockItemUnparsed[] blockItems =
                        SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsed(
                                streamedBlockNumber, streamedBlockNumber + 1);
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
                assertThat(transferQueue).isEmpty();
            }

            /**
             * This test aims to assert that the {@link PublisherHandler} correctly
             * handles a received invalid request
             * {@link PublisherHandler#onNext(PublishStreamRequestUnparsed)} when
             * block header is expected, but it is not present. Here, the handler
             * is in a state where it expects a block header to be the first item
             * in next request, but the request does not contain a block header as
             * first item. We expect that the handler will simply return w/o
             * making any responses to the reply pipeline.
             */
            @Test
            @DisplayName("Test onNext() with invalid request - no header, but expected - no responses sent")
            void testOnNextReturnOnInvalidRequestNoResponse() {
                // Setup request to send, in this case a single complete valid block
                // as items, starting with header and ending with proof
                final int streamedBlockNumber = 0;
                final BlockItemUnparsed[] blockItems =
                        SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsed(
                                streamedBlockNumber, streamedBlockNumber + 1);
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

            /**
             * This test aims to assert that the {@link PublisherHandler} correctly
             * handles a received invalid request
             * {@link PublisherHandler#onNext(PublishStreamRequestUnparsed)} when
             * block header is expected, but it is not present. Here, the handler
             * is in a state where it expects a block header to be the first item
             * in next request, but the request does not contain a block header as
             * first item. We expect that the handler will simply return w/o
             * updating any metrics.
             */
            @Test
            @DisplayName("Test onNext() with invalid request - no header, but expected - no metrics updated")
            void testOnNextReturnOnInvalidRequestMetrics() {
                // Setup request to send, in this case a single complete valid block
                // as items, starting with header and ending with proof
                final int streamedBlockNumber = 0;
                final BlockItemUnparsed[] blockItems =
                        SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsed(
                                streamedBlockNumber, streamedBlockNumber + 1);
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
                assertThat(metrics.liveBlockItemsReceived().get()).isEqualTo(0);
                assertThat(metrics.blockAcknowledgementsSent().get()).isEqualTo(0);
                assertThat(metrics.streamErrors().get()).isEqualTo(0);
                assertThat(metrics.blockSkipsSent().get()).isEqualTo(0);
                assertThat(metrics.blockResendsSent().get()).isEqualTo(0);
                assertThat(metrics.endOfStreamsSent().get()).isEqualTo(0);
                assertThat(metrics.sendResponseFailed().get()).isEqualTo(0);
                assertThat(metrics.endStreamsReceived().get()).isEqualTo(0);
            }

            /**
             * This test aims to assert that the {@link PublisherHandler} correctly
             * handles a received invalid request
             * {@link PublisherHandler#onNext(PublishStreamRequestUnparsed)} when
             * block header is expected, but it is not present. Here, the handler
             * is in a state where it expects a block header to be the first item
             * in next request, but the request does not contain a block header as
             * first item. We expect that the handler will simply return w/o
             * propagating any items to the transfer queue. Subsequent requests,
             * if valid, will be processed normally and items will be propagated
             * to the transfer queue.
             */
            @Test
            @DisplayName(
                    "Test onNext() with invalid request - no header, but expected - subsequent valid request items propagated")
            void testOnNextInvalidRequestValidSubsequentRequest() {
                // Setup request to send, in this case a single complete valid block
                // as items, starting with header and ending with proof
                final int expectedStreamedBlockNumber = 0;
                final BlockItemUnparsed[] blockItems =
                        SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsed(
                                expectedStreamedBlockNumber, expectedStreamedBlockNumber + 1);
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
                assertThat(transferQueue).isEmpty();
                // Now send a valid request, which contains a block header
                final BlockItemSetUnparsed validBlockItemSet =
                        BlockItemSetUnparsed.newBuilder().blockItems(blockItems).build();
                final PublishStreamRequestUnparsed validRequest = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(validBlockItemSet)
                        .build();
                // Train the manager to expect return ACCEPT
                manager.setBlockAction(BlockAction.ACCEPT);
                // Call with valid request
                toTest.onNext(validRequest);
                // Assert items were propagated to the transfer queue
                assertThat(transferQueue).hasSize(1).containsExactly(validBlockItemSet);
            }

            /**
             * This test aims to assert that the {@link PublisherHandler} correctly
             * handles a received invalid request
             * {@link PublisherHandler#onNext(PublishStreamRequestUnparsed)} when
             * block header is expected, but it is not present. Here, the handler
             * is in a state where it expects a block header to be the first item
             * in next request, but the request does not contain a block header as
             * first item. We expect that the handler will simply return w/o
             * propagating any items to the transfer queue. Subsequent requests,
             * if valid, will be processed normally and acknowledgements will be
             * sent.
             */
            @Test
            @DisplayName(
                    "Test onNext() with invalid request - no header, but expected - subsequent valid request sends acknowledgement")
            void testOnNextInvalidRequestValidSubsequentRequestResponse() {
                // Setup request to send, in this case a single complete valid block
                // as items, starting with header and ending with proof
                final int expectedStreamedBlockNumber = 0;
                final BlockItemUnparsed[] blockItems =
                        SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsed(
                                expectedStreamedBlockNumber, expectedStreamedBlockNumber + 1);
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
                manager.setBlockAction(BlockAction.ACCEPT);
                // Call with valid request
                toTest.onNext(validRequest);
                // Send a PersistedNotification for the streamed block number
                manager.handlePersisted(new PersistedNotification(
                        expectedStreamedBlockNumber, expectedStreamedBlockNumber, 0, BlockSource.PUBLISHER));
                // Assert that an acknowledgement was sent for the valid request
                assertThat(repliesPipeline.getOnNextCalls())
                        .hasSize(1)
                        .first()
                        .returns(ResponseOneOfType.ACKNOWLEDGEMENT, responseKindExtractor)
                        .returns((long) expectedStreamedBlockNumber, acknowledgementBlockNumberExtractor);
                // Assert no replies sent to the pipeline
                assertThat(repliesPipeline.getOnErrorCalls()).isEmpty();
                assertThat(repliesPipeline.getOnSubscriptionCalls()).isEmpty();
                assertThat(repliesPipeline.getOnCompleteCalls().get()).isEqualTo(0);
                assertThat(repliesPipeline.getClientEndStreamCalls().get()).isEqualTo(0);
            }

            /**
             * This test aims to assert that the {@link PublisherHandler} correctly
             * handles a received invalid request
             * {@link PublisherHandler#onNext(PublishStreamRequestUnparsed)} when
             * block header is expected, but it is not present. Here, the handler
             * is in a state where it expects a block header to be the first item
             * in next request, but the request does not contain a block header as
             * first item. We expect that the handler will simply return w/o
             * propagating any items to the transfer queue. Subsequent requests,
             * if valid, will be processed normally and metrics will be updated.
             */
            @Test
            @DisplayName(
                    "Test onNext() with invalid request - no header, but expected - subsequent valid request updates metrics")
            void testOnNextInvalidRequestValidSubsequentRequestMetrics() {
                // Setup request to send, in this case a single complete valid block
                // as items, starting with header and ending with proof
                final int expectedStreamedBlockNumber = 0;
                final BlockItemUnparsed[] blockItems =
                        SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsed(
                                expectedStreamedBlockNumber, expectedStreamedBlockNumber + 1);
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
                assertThat(metrics.liveBlockItemsReceived().get()).isEqualTo(0);
                assertThat(metrics.blockAcknowledgementsSent().get()).isEqualTo(0);
                assertThat(metrics.streamErrors().get()).isEqualTo(0);
                assertThat(metrics.blockSkipsSent().get()).isEqualTo(0);
                assertThat(metrics.blockResendsSent().get()).isEqualTo(0);
                assertThat(metrics.endOfStreamsSent().get()).isEqualTo(0);
                assertThat(metrics.sendResponseFailed().get()).isEqualTo(0);
                assertThat(metrics.endStreamsReceived().get()).isEqualTo(0);
                // Now send a valid request, which contains a block header
                final BlockItemSetUnparsed validBlockItemSet =
                        BlockItemSetUnparsed.newBuilder().blockItems(blockItems).build();
                final PublishStreamRequestUnparsed validRequest = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(validBlockItemSet)
                        .build();
                // Train the manager to expect return ACCEPT
                manager.setBlockAction(BlockAction.ACCEPT);
                // Call with valid request
                toTest.onNext(validRequest);
                // Send a PersistedNotification for the streamed block number
                manager.handlePersisted(new PersistedNotification(
                        expectedStreamedBlockNumber, expectedStreamedBlockNumber, 0, BlockSource.PUBLISHER));
                // Assert live items received updated and acknowledgement sent updated
                assertThat(metrics.liveBlockItemsReceived().get()).isEqualTo(blockItems.length);
                assertThat(metrics.blockAcknowledgementsSent().get()).isEqualTo(1);
                // Assert other metrics unchanged
                assertThat(metrics.streamErrors().get()).isEqualTo(0);
                assertThat(metrics.blockSkipsSent().get()).isEqualTo(0);
                assertThat(metrics.blockResendsSent().get()).isEqualTo(0);
                assertThat(metrics.endOfStreamsSent().get()).isEqualTo(0);
                assertThat(metrics.sendResponseFailed().get()).isEqualTo(0);
                assertThat(metrics.endStreamsReceived().get()).isEqualTo(0);
            }

            /**
             * This test aims to assert that the {@link PublisherHandler} correctly
             * handles a received request
             * {@link PublisherHandler#onNext(PublishStreamRequestUnparsed)} when
             * premature header is received, i.e. the request contains a block
             * header, but the current block is not yet streamed in full. A header
             * indicates that the current requests initiates the streaming of a new
             * block. When a header is sent prematurely, the handler is expected to
             * not propagate any items to the transfer queue.
             */
            @Test
            @DisplayName(
                    "Test onNext() with premature header received, no items propagated - EndOfStream, Code INVALID_REQUEST")
            void testOnNextPrematureHeader() {
                // Setup request to send, in this case a single complete valid block
                // as items, starting with header and ending with proof
                final int streamedBlockNumber = 0;
                final BlockItemUnparsed[] blockItems =
                        SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsed(
                                streamedBlockNumber, streamedBlockNumber + 1);
                // Send the first half of the items, header included
                final BlockItemUnparsed[] blockItemsToSend = Arrays.copyOfRange(blockItems, 0, blockItems.length / 2);
                final BlockItemSetUnparsed blockItemSet = BlockItemSetUnparsed.newBuilder()
                        .blockItems(blockItemsToSend)
                        .build();
                final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(blockItemSet)
                        .build();
                // Train the manager to expect return ACCEPT
                manager.setBlockAction(BlockAction.ACCEPT);
                // Call, we expect that everything is well when we've first sent this valid request
                toTest.onNext(request);
                // Assert items were propagated (first request was valid and passed)
                assertThat(transferQueue).hasSize(1).containsExactly(blockItemSet);
                // Call again, now we expect that the handler will not propagate any items
                // because the request would be invalid, we are sending a header, but the
                // current block is not yet streamed in full as we sent only half of the items.
                toTest.onNext(request);
                // Assert that queue is unchanged (1st request was valid)
                assertThat(transferQueue).hasSize(1).containsExactly(blockItemSet);
            }

            /**
             * This test aims to assert that the {@link PublisherHandler} correctly
             * handles a received request
             * {@link PublisherHandler#onNext(PublishStreamRequestUnparsed)} when
             * premature header is received, i.e. the request contains a block
             * header, but the current block is not yet streamed in full. A header
             * indicates that the current requests initiates the streaming of a new
             * block. When a header is sent prematurely, the handler is expected to
             * respond with an {@link org.hiero.block.api.PublishStreamResponse.EndOfStream}
             * with {@link org.hiero.block.api.PublishStreamResponse.EndOfStream.Code#INVALID_REQUEST}.
             */
            @Test
            @DisplayName("Test onNext() with premature header received, response EndOfStream, Code INVALID_REQUEST")
            void testOnNextPrematureHeaderResponse() {
                // Setup request to send, in this case a single complete valid block
                // as items, starting with header and ending with proof
                final int streamedBlockNumber = 0;
                final BlockItemUnparsed[] blockItems =
                        SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsed(
                                streamedBlockNumber, streamedBlockNumber + 1);
                // Send the first half of the items, header included
                final BlockItemUnparsed[] blockItemsToSend = Arrays.copyOfRange(blockItems, 0, blockItems.length / 2);
                final BlockItemSetUnparsed blockItemSet = BlockItemSetUnparsed.newBuilder()
                        .blockItems(blockItemsToSend)
                        .build();
                final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(blockItemSet)
                        .build();
                // Train the manager to expect return ACCEPT
                manager.setBlockAction(BlockAction.ACCEPT);
                // Call, we expect that everything is well when we've first sent this valid request
                toTest.onNext(request);
                // Assert items were propagated (first request was valid and passed)
                assertThat(transferQueue).hasSize(1).containsExactly(blockItemSet);
                // Call again, now we expect that the handler will not propagate any items
                // because the request would be invalid, we are sending a header, but the
                // current block is not yet streamed in full as we sent only half of the items.
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

            /**
             * This test aims to assert that the {@link PublisherHandler} correctly
             * handles a received request
             * {@link PublisherHandler#onNext(PublishStreamRequestUnparsed)} when
             * premature header is received, i.e. the request contains a block
             * header, but the current block is not yet streamed in full. A header
             * indicates that the current requests initiates the streaming of a new
             * block. When a header is sent prematurely, the handler is expected to
             * update metrics accordingly.
             */
            @Test
            @DisplayName(
                    "Test onNext() with premature header received, metrics updated - EndOfStream, Code INVALID_REQUEST")
            void testOnNextPrematureHeaderMetrics() {
                // Setup request to send, in this case a single complete valid block
                // as items, starting with header and ending with proof
                final int streamedBlockNumber = 0;
                final BlockItemUnparsed[] blockItems =
                        SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsed(
                                streamedBlockNumber, streamedBlockNumber + 1);
                // Send the first half of the items, header included
                final BlockItemUnparsed[] blockItemsToSend = Arrays.copyOfRange(blockItems, 0, blockItems.length / 2);
                final BlockItemSetUnparsed blockItemSet = BlockItemSetUnparsed.newBuilder()
                        .blockItems(blockItemsToSend)
                        .build();
                final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(blockItemSet)
                        .build();
                // Train the manager to expect return ACCEPT
                manager.setBlockAction(BlockAction.ACCEPT);
                // Call, we expect that everything is well when we've first sent this valid request
                toTest.onNext(request);
                // Assert items were propagated (first request was valid and passed)
                assertThat(transferQueue).hasSize(1).containsExactly(blockItemSet);
                // Assert metrics updated for the successful request
                assertThat(metrics.liveBlockItemsReceived().get()).isEqualTo(1);
                // Call again, now we expect that the handler will not propagate any items
                // because the request would be invalid, we are sending a header, but the
                // current block is not yet streamed in full as we sent only half of the items.
                toTest.onNext(request);
                // Assert metrics updated
                assertThat(metrics.endOfStreamsSent().get()).isEqualTo(1);
                // Assert other metrics unchanged
                assertThat(metrics.liveBlockItemsReceived().get()).isEqualTo(1);
                assertThat(metrics.blockAcknowledgementsSent().get()).isEqualTo(0);
                assertThat(metrics.streamErrors().get()).isEqualTo(0);
                assertThat(metrics.blockSkipsSent().get()).isEqualTo(0);
                assertThat(metrics.blockResendsSent().get()).isEqualTo(0);
                assertThat(metrics.sendResponseFailed().get()).isEqualTo(0);
                assertThat(metrics.endStreamsReceived().get()).isEqualTo(0);
            }

            /**
             * This test aims to assert that the {@link PublisherHandler} correctly
             * handles a received invalid request
             * {@link PublisherHandler#onNext(PublishStreamRequestUnparsed)} when
             * block header is broken, cannot be parsed. We expect that the handler
             * will not propagate any items to the transfer queue.
             */
            @Test
            @DisplayName("Test onNext() with invalid request - broken header - no items propagated")
            void testOnNextBrokenHeader() {
                // Setup request to send, in this case a single complete block
                // as items, starting with a broken header and ending with proof
                final int streamedBlockNumber = 0;
                final BlockItemUnparsed[] blockItems =
                        SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsedWithBrokenHeaders(
                                streamedBlockNumber, streamedBlockNumber + 1);
                final BlockItemSetUnparsed blockItemSet =
                        BlockItemSetUnparsed.newBuilder().blockItems(blockItems).build();
                final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(blockItemSet)
                        .build();
                // Call
                toTest.onNext(request);
                // Assert no items offered to the transfer queue
                assertThat(transferQueue).isEmpty();
            }

            /**
             * This test aims to assert that the {@link PublisherHandler} correctly
             * handles a received invalid request
             * {@link PublisherHandler#onNext(PublishStreamRequestUnparsed)} when
             * block header is broken, cannot be parsed. We expect that the handler
             * will simply reply with a
             * {@link org.hiero.block.api.PublishStreamResponse.EndOfStream}
             * with {@link org.hiero.block.api.PublishStreamResponse.EndOfStream.Code#INVALID_REQUEST}.
             */
            @Test
            @DisplayName(
                    "Test onNext() with invalid request - broken header - respond with EndOfStream, Code INVALID_REQUEST")
            void testOnNextBrokenHeaderResponse() {
                // Setup request to send, in this case a single complete block
                // as items, starting with a broken header and ending with proof
                final int streamedBlockNumber = 0;
                final BlockItemUnparsed[] blockItems =
                        SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsedWithBrokenHeaders(
                                streamedBlockNumber, streamedBlockNumber + 1);
                final BlockItemSetUnparsed blockItemSet =
                        BlockItemSetUnparsed.newBuilder().blockItems(blockItems).build();
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

            /**
             * This test aims to assert that the {@link PublisherHandler} correctly
             * handles a received invalid request
             * {@link PublisherHandler#onNext(PublishStreamRequestUnparsed)} when
             * block header is broken, cannot be parsed. We expect that the handler
             * will update metrics.
             */
            @Test
            @DisplayName("Test onNext() with invalid request - broken header - metrics updated")
            void testOnNextBrokenHeaderMetrics() {
                // Setup request to send, in this case a single complete block
                // as items, starting with a broken header and ending with proof
                final int streamedBlockNumber = 0;
                final BlockItemUnparsed[] blockItems =
                        SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsedWithBrokenHeaders(
                                streamedBlockNumber, streamedBlockNumber + 1);
                final BlockItemSetUnparsed blockItemSet =
                        BlockItemSetUnparsed.newBuilder().blockItems(blockItems).build();
                final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(blockItemSet)
                        .build();
                // Call
                toTest.onNext(request);
                // Assert metrics updated
                assertThat(metrics.endOfStreamsSent().get()).isEqualTo(1);
                // Assert other metrics unchanged
                assertThat(metrics.liveBlockItemsReceived().get()).isEqualTo(0);
                assertThat(metrics.blockAcknowledgementsSent().get()).isEqualTo(0);
                assertThat(metrics.streamErrors().get()).isEqualTo(0);
                assertThat(metrics.blockSkipsSent().get()).isEqualTo(0);
                assertThat(metrics.blockResendsSent().get()).isEqualTo(0);
                assertThat(metrics.sendResponseFailed().get()).isEqualTo(0);
                assertThat(metrics.endStreamsReceived().get()).isEqualTo(0);
            }

            /**
             * This test aims to assert that the {@link PublisherHandler} correctly
             * handles a received invalid request
             * {@link PublisherHandler#onNext(PublishStreamRequestUnparsed)} when
             * block header null. We expect that the handler will not propagate any
             * items to the transfer queue.
             */
            @Test
            @DisplayName("Test onNext() with invalid request - null header - no items propagated")
            void testOnNextNullHeader() {
                // Setup request to send, in this case a single complete block
                // as items, starting with a null header and ending with proof
                final int streamedBlockNumber = 0;
                final BlockItemUnparsed[] blockItems =
                        SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsedWithNullHeaderBytes(
                                streamedBlockNumber, streamedBlockNumber + 1);
                final BlockItemSetUnparsed blockItemSet =
                        BlockItemSetUnparsed.newBuilder().blockItems(blockItems).build();
                final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(blockItemSet)
                        .build();
                // Call
                toTest.onNext(request);
                // Assert no items offered to the transfer queue
                assertThat(transferQueue).isEmpty();
            }

            /**
             * This test aims to assert that the {@link PublisherHandler} correctly
             * handles a received invalid request
             * {@link PublisherHandler#onNext(PublishStreamRequestUnparsed)} when
             * block header null. We expect that the handler
             * will simply reply with a
             * {@link org.hiero.block.api.PublishStreamResponse.EndOfStream}
             * with {@link org.hiero.block.api.PublishStreamResponse.EndOfStream.Code#ERROR}.
             */
            @Test
            @DisplayName("Test onNext() with invalid request - null header - respond with EndOfStream, Code ERROR")
            void testOnNextNullHeaderResponse() {
                // Setup request to send, in this case a single complete block
                // as items, starting with a null header and ending with proof
                final int streamedBlockNumber = 0;
                final BlockItemUnparsed[] blockItems =
                        SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsedWithNullHeaderBytes(
                                streamedBlockNumber, streamedBlockNumber + 1);
                final BlockItemSetUnparsed blockItemSet =
                        BlockItemSetUnparsed.newBuilder().blockItems(blockItems).build();
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

            /**
             * This test aims to assert that the {@link PublisherHandler} correctly
             * handles a received invalid request
             * {@link PublisherHandler#onNext(PublishStreamRequestUnparsed)} when
             * block header null. We expect that the handler will update metrics.
             */
            @Test
            @DisplayName("Test onNext() with invalid request - null header - metrics updated")
            void testOnNextNullHeaderMetrics() {
                // Setup request to send, in this case a single complete block
                // as items, starting with a null header and ending with proof
                final int streamedBlockNumber = 0;
                final BlockItemUnparsed[] blockItems =
                        SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsedWithNullHeaderBytes(
                                streamedBlockNumber, streamedBlockNumber + 1);
                final BlockItemSetUnparsed blockItemSet =
                        BlockItemSetUnparsed.newBuilder().blockItems(blockItems).build();
                final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(blockItemSet)
                        .build();
                // Call
                toTest.onNext(request);
                // Assert metrics updated
                assertThat(metrics.endOfStreamsSent().get()).isEqualTo(1);
                // Assert other metrics unchanged
                assertThat(metrics.liveBlockItemsReceived().get()).isEqualTo(0);
                assertThat(metrics.blockAcknowledgementsSent().get()).isEqualTo(0);
                assertThat(metrics.streamErrors().get()).isEqualTo(0);
                assertThat(metrics.blockSkipsSent().get()).isEqualTo(0);
                assertThat(metrics.blockResendsSent().get()).isEqualTo(0);
                assertThat(metrics.sendResponseFailed().get()).isEqualTo(0);
                assertThat(metrics.endStreamsReceived().get()).isEqualTo(0);
            }

            /**
             * This test aims to verify that the
             * {@link PublisherHandler#onNext(PublishStreamRequestUnparsed)}
             * correctly handles a request with null block items. We expect that
             * {@link PublishStreamResponse.EndOfStream}
             * response is returned with code {@link Code#INVALID_REQUEST}.
             */
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

            /**
             * This test aims to verify that the
             * {@link PublisherHandler#onNext(PublishStreamRequestUnparsed)}
             * correctly handles a request with empty block items. We expect
             * that {@link PublishStreamResponse.EndOfStream}
             * response is returned with code {@link Code#INVALID_REQUEST}.
             */
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

            /**
             * This test aims to verify that the
             * {@link PublisherHandler#onNext(PublishStreamRequestUnparsed)}
             * correctly handles a request with unset oneOf We expect
             * that {@link PublishStreamResponse.EndOfStream}
             * response is returned with code {@link Code#INVALID_REQUEST}.
             */
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

            /**
             * This test aims to verify that the
             * {@link PublisherHandler#onNext(PublishStreamRequestUnparsed)}
             * correctly handles a request with EndStream shutdown. We expect
             * that {@link PublishStreamResponse.EndOfStream}
             * response is returned with any code to shut down the handler.
             */
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

            /**
             * This test aims to verify that the
             * {@link PublisherHandler#onNext(PublishStreamRequestUnparsed)}
             * correctly handles a request with EndStream shutdown. We expect
             * that {@link PublishStreamResponse.EndOfStream}
             * response is returned with any code to shut down the handler even
             * if the request is invalid as specified by
             * {@link PublisherHandler#isEndStreamRequestValid(EndStream.Code, long, long)}.
             */
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

            /**
             * This test aims to verify that the
             * {@link PublisherHandler#onNext(PublishStreamRequestUnparsed)}
             * correctly handles a request with EndStream shutdown with code
             * {@link EndStream.Code#TOO_FAR_BEHIND}. We expect that the handler
             * will send a notification with the latest block known to the network
             * if the node is truly behind.
             */
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

            /**
             * This test aims to verify that the
             * {@link PublisherHandler#onNext(PublishStreamRequestUnparsed)}
             * correctly handles a request with EndStream shutdown with code
             * {@link EndStream.Code#TOO_FAR_BEHIND}. We expect that the handler
             * will not send a notification with the latest block known to the
             * network if the node is not truly behind.
             */
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
        }

        /**
         * Tests for the {@link PublisherHandler#onError(Throwable)} method.
         */
        @Nested
        @DisplayName("onError() Tests")
        class OnErrorTests {
            /**
             * This test aims to assert that the {@link PublisherHandler} correctly handles a received error response
             * when the {@link PublisherHandler#onError(Throwable)} gets called. Here we expect that the handler will
             * respond to the publisher with an {@link org.hiero.block.api.PublishStreamResponse.EndOfStream} with
             * {@link org.hiero.block.api.PublishStreamResponse.EndOfStream.Code#ERROR} and will proceed to orderly
             * shutdown.
             */
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

            /**
             * This test aims to assert that the {@link PublisherHandler} correctly handles a received error response
             * when the {@link PublisherHandler#onError(Throwable)} gets called. Here we expect that metrics will be
             * updated accordingly to reflect the received error.
             */
            @Test
            @DisplayName("Test onError() metrics updated - EndOfStream, Code ERROR")
            void testOnErrorMetrics() {
                // set an arbitrary valid latest streamed block number
                final long latestStreamedBlockNumber = 0L;
                manager.setLatestBlockNumber(latestStreamedBlockNumber);
                // Call onError with an arbitrary Throwable
                toTest.onError(new RuntimeException());
                // Assert metrics updated
                assertThat(metrics.endOfStreamsSent().get()).isEqualTo(1);
                // Assert other metrics unchanged
                assertThat(metrics.liveBlockItemsReceived().get()).isEqualTo(0);
                assertThat(metrics.blockAcknowledgementsSent().get()).isEqualTo(0);
                assertThat(metrics.streamErrors().get()).isEqualTo(0);
                assertThat(metrics.blockSkipsSent().get()).isEqualTo(0);
                assertThat(metrics.blockResendsSent().get()).isEqualTo(0);
                assertThat(metrics.sendResponseFailed().get()).isEqualTo(0);
                assertThat(metrics.endStreamsReceived().get()).isEqualTo(0);
            }
        }

        /**
         * Tests for the {@link PublisherHandler#onError(Throwable)} method
         */
        @Nested
        @DisplayName("onComplete() Tests")
        class OnCompleteTests {
            // @todo(1416) add tests for onComplete, not finished yet
        }

        /**
         * Tests for the {@link PublisherHandler#onSubscribe(Subscription)} method.
         */
        @Nested
        @DisplayName("onSubscribe() Tests")
        class OnSubscribeTests {
            // @todo(1416) add tests for onSubscribe, not finished yet
        }

        /**
         * Tests for the {@link PublisherHandler#clientEndStreamReceived()} method.
         */
        @Nested
        @DisplayName("clientEndStreamReceived() Tests")
        class ClientEndStreamReceivedTests {
            // @todo(1416) add tests for clientEndStreamReceived, not finished yet
        }

        /**
         * Tests for the {@link PublisherHandler#sendAcknowledgement(long)} method.
         */
        @Nested
        @DisplayName("sendAcknowledgement() Tests")
        class SendAcknowledgementTests {
            // @todo(1416) add tests for sendAcknowledgement, not finished yet
        }

        /**
         * Tests for the {@link PublisherHandler#handleFailedVerification(long)}method.
         */
        @Nested
        @DisplayName("handleFailedVerification() Tests")
        class HandleVerificationTests {
            /**
             * This test aims to assert that the
             * {@link PublisherHandler#handleFailedVerification(long)} will
             * correctly handle a failed verification by sending a
             * {@link org.hiero.block.api.PublishStreamResponse.ResendBlock}
             * response with the next block number to be resent, when the
             * publisher has not sent the block that failed verification.
             */
            @Test
            @DisplayName(
                    "handleFailedVerification() - ResendBlock response when handler did not send the block that failed verification")
            void testHandleVerificationResend() {
                // Train the manager to return the expected latest block number
                final long latestBlockNumber = 10L; // Example latest block number
                final long expectedResponseBlockNumber = latestBlockNumber + 1L;
                manager.setLatestBlockNumber(latestBlockNumber);
                // Call
                toTest.handleFailedVerification(0);
                // Assert single response is ResentBlock with block number same as streamed
                assertThat(repliesPipeline.getOnNextCalls())
                        .hasSize(1)
                        .first()
                        .returns(ResponseOneOfType.RESEND_BLOCK, responseKindExtractor)
                        .returns(expectedResponseBlockNumber, resendBlockNumberExtractor);
                // Assert no other responses sent
                assertThat(repliesPipeline.getOnErrorCalls()).isEmpty();
                assertThat(repliesPipeline.getOnSubscriptionCalls()).isEmpty();
                assertThat(repliesPipeline.getOnCompleteCalls().get()).isEqualTo(0);
                assertThat(repliesPipeline.getClientEndStreamCalls().get()).isEqualTo(0);
            }

            /**
             * This test aims to assert that the
             * {@link PublisherHandler#handleFailedVerification(long)} will
             * correctly handle a failed verification by sending a
             * {@link org.hiero.block.api.PublishStreamResponse.EndOfStream}
             * response with code
             * {@link org.hiero.block.api.PublishStreamResponse.EndOfStream.Code#BAD_BLOCK_PROOF}
             * when the publisher has sent the block that failed verification.
             */
            @Test
            @DisplayName(
                    "handleFailedVerification() - EndOfStream response with Code BAD_BLOCK_PROOF when handler sent the block that failed verification")
            void testHandleVerificationBadProof() {
                // Generate any block, we do not have an actual verification, we will simulate a failure
                final long expectedBlockNumber = 0L;
                final BlockItemUnparsed[] block = SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsed(
                        (int) expectedBlockNumber, (int) (expectedBlockNumber + 1));
                // Send a request to the handler with the block, this will update
                // the internal state of the handler and will flag that the block has been sent
                // by the handler under test. This will be important when we simulate the failed
                // verification.
                final BlockItemSetUnparsed blockItemSet =
                        BlockItemSetUnparsed.newBuilder().blockItems(block).build();
                final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(blockItemSet)
                        .build();
                // Train the manager to expect return ACCEPT
                manager.setBlockAction(BlockAction.ACCEPT);
                // Call onNext with the request, this will update the internal state of the handler
                toTest.onNext(request);
                // Train the manager to return the expected latest block number
                final long latestBlockNumber = expectedBlockNumber - 1L;
                manager.setLatestBlockNumber(latestBlockNumber);
                // Call
                toTest.handleFailedVerification(expectedBlockNumber);
                // Assert single response is EndOfStream with Code BAD_BLOCK_PROOF and onComplete is called (shutdown)
                assertThat(repliesPipeline.getOnNextCalls())
                        .hasSize(1)
                        .first()
                        .returns(ResponseOneOfType.END_STREAM, responseKindExtractor)
                        .returns(Code.BAD_BLOCK_PROOF, endStreamResponseCodeExtractor)
                        .returns(latestBlockNumber, endStreamBlockNumberExtractor);
                assertThat(repliesPipeline.getOnCompleteCalls().get()).isEqualTo(1);
                // Assert no other responses sent
                assertThat(repliesPipeline.getOnErrorCalls()).isEmpty();
                assertThat(repliesPipeline.getOnSubscriptionCalls()).isEmpty();
                assertThat(repliesPipeline.getClientEndStreamCalls().get()).isEqualTo(0);
            }
        }

        // @todo(1416)
        // @todo(1263)
        //    tests to add:
        //    add tests for EndStream requests when they become available
        //    ...
    }

    /**
     * Creates a new {@link MetricsHolder} with default counters for testing.
     * These counters could be queried to verify the metrics' states.
     */
    private MetricsHolder createMetrics() {
        return new MetricsHolder(
                new DefaultCounter(new Config("category", "name")),
                new DefaultCounter(new Config("category", "name")),
                new DefaultCounter(new Config("category", "name")),
                new DefaultCounter(new Config("category", "name")),
                new DefaultCounter(new Config("category", "name")),
                new DefaultCounter(new Config("category", "name")),
                new DefaultCounter(new Config("category", "name")),
                new DefaultCounter(new Config("category", "name")));
    }
}
