// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.stream.publisher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import com.swirlds.metrics.api.Counter.Config;
import com.swirlds.metrics.impl.DefaultCounter;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TransferQueue;
import java.util.function.Function;
import org.hiero.block.api.PublishStreamResponse;
import org.hiero.block.api.PublishStreamResponse.EndOfStream.Code;
import org.hiero.block.api.PublishStreamResponse.ResponseOneOfType;
import org.hiero.block.internal.BlockItemSetUnparsed;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.PublishStreamRequestUnparsed;
import org.hiero.block.node.app.fixtures.blocks.SimpleTestBlockItemBuilder;
import org.hiero.block.node.stream.publisher.PublisherHandler.MetricsHolder;
import org.hiero.block.node.stream.publisher.StreamPublisherManager.BlockAction;
import org.hiero.block.node.stream.publisher.fixtures.TestResponsePipeline;
import org.hiero.block.node.stream.publisher.fixtures.TestStreamPublisherManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

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
            validPublisherManager = new TestStreamPublisherManager();
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

        /**
         * Environment setup executed before each test in this nested class.
         */
        @BeforeEach
        void setup() {
            handlerId = 1L;
            repliesPipeline = new TestResponsePipeline();
            metrics = createMetrics();
            manager = new TestStreamPublisherManager();
            transferQueue = new LinkedTransferQueue<>();
            toTest = new PublisherHandler(handlerId, repliesPipeline, metrics, manager, transferQueue);
        }

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
        @DisplayName("Test onNext() with valid request with a complete single block items streamed - happy path ACCEPT")
        void testPublishItemsHappyPathACCEPT() {
            // Setup request to send, in this case a single complete valid block
            // as items, starting with header and ending with proof
            final int streamedBlockNumber = 0;
            final BlockItemUnparsed[] blockItems = SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsed(
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
        void testPublishItemsNoRepliesHappyPathACCEPT() {
            // Setup request to send, in this case a single complete valid block
            // as items, starting with header and ending with proof
            final int streamedBlockNumber = 0;
            final BlockItemUnparsed[] blockItems = SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsed(
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
        void testPublishItemsMetricsHappyPathACCEPT() {
            // Setup request to send, in this case a single complete valid block
            // as items, starting with header and ending with proof
            final int streamedBlockNumber = 0;
            final BlockItemUnparsed[] blockItems = SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsed(
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
            assertThat(metrics.endStreamsReceived().get()).isEqualTo(0);
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
        void testPublishItemsHappyPathTwoRequestsACCEPT() {
            // Create the block to stream, a single complete valid block
            final int streamedBlockNumber = 0;
            final BlockItemUnparsed[] blockItems = SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsed(
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
        void testPublishItemsMetricsHappyPathTwoRequestsACCEPT() {
            // Create the block to stream, a single complete valid block
            final int streamedBlockNumber = 0;
            final BlockItemUnparsed[] blockItems = SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsed(
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
         * {@link BlockAction#SKIP} for the streamed block number no items will
         * be offered to the transfer queue.
         */
        @Test
        @DisplayName(
                "Test onNext() with valid request with a complete single block no items offered to transfer queue - happy path SKIP")
        void testHappyPathNoItemsOfferedToTransferQueueSKIP() {
            // Setup request to send, in this case a single complete valid block
            // as items, starting with header and ending with proof
            final int streamedBlockNumber = 0;
            final BlockItemUnparsed[] blockItems = SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsed(
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
         * {@link PublisherHandler#onNext(PublishStreamRequestUnparsed)} happy
         * path scenario. Here we stream a single complete valid block as items.
         * The request's first item is the block header for the streamed block,
         * and the last item is the block proof for the streamed block.
         * We expect that when the {@link StreamPublisherManager} returns
         * {@link BlockAction#SKIP} for the streamed block number the handler
         * will reply with a
         * {@link org.hiero.block.api.PublishStreamResponse.SkipBlock}.
         */
        @Test
        @DisplayName(
                "Test onNext() with valid request with a complete single block response SkipBlock - happy path SKIP")
        void testHappyPathResponseSKIP() {
            // Setup request to send, in this case a single complete valid block
            // as items, starting with header and ending with proof
            final int streamedBlockNumber = 0;
            final BlockItemUnparsed[] blockItems = SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsed(
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
         * {@link PublisherHandler#onNext(PublishStreamRequestUnparsed)} happy
         * path scenario. Here we stream a single complete valid block as items.
         * The request's first item is the block header for the streamed block,
         * and the last item is the block proof for the streamed block.
         * We expect that when the {@link StreamPublisherManager} returns
         * {@link BlockAction#SKIP} for the streamed block number the metrics
         * will be properly updated.
         */
        @Test
        @DisplayName("Test onNext() with valid request with a complete single block metrics updated - happy path SKIP")
        void testHappyPathMetricsSKIP() {
            // Setup request to send, in this case a single complete valid block
            // as items, starting with header and ending with proof
            final int streamedBlockNumber = 0;
            final BlockItemUnparsed[] blockItems = SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsed(
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
         * {@link BlockAction#RESEND} for the streamed block number no items
         * will be offered to the transfer queue.
         */
        @Test
        @DisplayName(
                "Test onNext() with valid request with a complete single block no items offered to transfer queue - happy path RESEND")
        void testHappyPathNoItemsOfferedToTransferQueueRESEND() {
            // Setup request to send, in this case a single complete valid block
            // as items, starting with header and ending with proof
            final int streamedBlockNumber = 0;
            final BlockItemUnparsed[] blockItems = SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsed(
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
         * {@link PublisherHandler#onNext(PublishStreamRequestUnparsed)} happy
         * path scenario. Here we stream a single complete valid block as items.
         * The request's first item is the block header for the streamed block,
         * and the last item is the block proof for the streamed block.
         * We expect that when the {@link StreamPublisherManager} returns
         * {@link BlockAction#RESEND} for the streamed block number the handler
         * will reply with a
         * {@link org.hiero.block.api.PublishStreamResponse.ResendBlock}.
         */
        @Test
        @DisplayName(
                "Test onNext() with valid request with a complete single block response ResendBlock - happy path RESEND")
        void testHappyPathResponseRESEND() {
            // Setup request to send, in this case a single complete valid block
            // as items, starting with header and ending with proof
            final int streamedBlockNumber = 0;
            final BlockItemUnparsed[] blockItems = SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsed(
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
         * This test aims to assert that the {@link PublisherHandler} correctly
         * handles a received request
         * {@link PublisherHandler#onNext(PublishStreamRequestUnparsed)} happy
         * path scenario. Here we stream a single complete valid block as items.
         * The request's first item is the block header for the streamed block,
         * and the last item is the block proof for the streamed block.
         * We expect that when the {@link StreamPublisherManager} returns
         * {@link BlockAction#RESEND} for the streamed block number the metrics
         * will be properly updated.
         */
        @Test
        @DisplayName(
                "Test onNext() with valid request with a complete single block metrics updated ResendBlock - happy path RESEND")
        void testHappyPathMetricsRESEND() {
            // Setup request to send, in this case a single complete valid block
            // as items, starting with header and ending with proof
            final int streamedBlockNumber = 0;
            final BlockItemUnparsed[] blockItems = SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsed(
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
         * {@link BlockAction#END_DUPLICATE} for the streamed block number no
         * items will be offered to the transfer queue.
         */
        @Test
        @DisplayName(
                "Test onNext() with valid request with a complete single block no items offered to transfer queue - happy path EndOfStream, Code DUPLICATE_BLOCK")
        void testHappyPathNoItemsOfferedToTransferQueueDUPLICATE() {
            // Setup request to send, in this case a single complete valid block
            // as items, starting with header and ending with proof
            final int streamedBlockNumber = 0;
            final BlockItemUnparsed[] blockItems = SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsed(
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
         * {@link PublisherHandler#onNext(PublishStreamRequestUnparsed)} happy
         * path scenario. Here we stream a single complete valid block as items.
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
                "Test onNext() with valid request with a complete single block response EndOfStream, Code DUPLICATE_BLOCK - happy path EndOfStream, Code DUPLICATE_BLOCK")
        void testHappyPathResponseDUPLICATE() {
            // Setup request to send, in this case a single complete valid block
            // as items, starting with header and ending with proof
            final int streamedBlockNumber = 0;
            final BlockItemUnparsed[] blockItems = SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsed(
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
            // Assert single response is ResentBlock with block number same as streamed
            assertThat(repliesPipeline.getOnNextCalls())
                    .hasSize(1)
                    .first()
                    .returns(ResponseOneOfType.END_STREAM, responseKindExtractor)
                    .returns(Code.DUPLICATE_BLOCK, endStreamResponseCodeExtractor)
                    .returns(expectedLatestBlockNumber, endStreamBlockNumberExtractor);
            // Assert no other responses sent
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
         * {@link BlockAction#END_DUPLICATE} for the streamed block number the
         * metrics will be properly updated.
         */
        @Test
        @DisplayName(
                "Test onNext() with valid request with a complete single block metrics updated EndOfStream, Code DUPLICATE_BLOCK - happy path EndOfStream, Code DUPLICATE_BLOCK")
        void testHappyPathMetricsDUPLICATE() {
            // Setup request to send, in this case a single complete valid block
            // as items, starting with header and ending with proof
            final int streamedBlockNumber = 0;
            final BlockItemUnparsed[] blockItems = SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsed(
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
         * {@link BlockAction#END_BEHIND} for the streamed block number no items
         * will be offered to the transfer queue, and the handler will reply
         */
        @Test
        @DisplayName(
                "Test onNext() with valid request with a complete single block no items offered to transfer queue - happy path EndOfStream, Code BEHIND")
        void testHappyPathNoItemsOfferedToTransferQueueBEHIND() {
            // Setup request to send, in this case a single complete valid block
            // as items, starting with header and ending with proof
            final int streamedBlockNumber = 0;
            final BlockItemUnparsed[] blockItems = SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsed(
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
         * {@link PublisherHandler#onNext(PublishStreamRequestUnparsed)} happy
         * path scenario. Here we stream a single complete valid block as items.
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
                "Test onNext() with valid request with a complete single block response EndOfStream, Code BEHIND - happy path EndOfStream, Code BEHIND")
        void testHappyPathResponseBEHIND() {
            // Setup request to send, in this case a single complete valid block
            // as items, starting with header and ending with proof
            final int streamedBlockNumber = 0;
            final BlockItemUnparsed[] blockItems = SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsed(
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
            // Assert single response is ResentBlock with block number same as streamed
            assertThat(repliesPipeline.getOnNextCalls())
                    .hasSize(1)
                    .first()
                    .returns(ResponseOneOfType.END_STREAM, responseKindExtractor)
                    .returns(Code.BEHIND, endStreamResponseCodeExtractor)
                    .returns(expectedLatestBlockNumber, endStreamBlockNumberExtractor);
            // Assert no other responses sent
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
         * {@link BlockAction#END_BEHIND} for the streamed block number the
         * metrics will be properly updated.
         */
        @Test
        @DisplayName(
                "Test onNext() with valid request with a complete single block metrics updated EndOfStream, Code BEHIND - happy path EndOfStream, Code BEHIND")
        void testHappyPathMetricsBEHIND() {
            // Setup request to send, in this case a single complete valid block
            // as items, starting with header and ending with proof
            final int streamedBlockNumber = 0;
            final BlockItemUnparsed[] blockItems = SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsed(
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
         * {@link BlockAction#END_ERROR} for the streamed block number no items
         * will be offered to the transfer queue.
         */
        @Test
        @DisplayName(
                "Test onNext() with valid request with a complete single block no items offered to transfer queue - happy path EndOfStream, Code ERROR")
        void testHappyPathNoItemsOfferedToTransferQueueERROR() {
            // Setup request to send, in this case a single complete valid block
            // as items, starting with header and ending with proof
            final int streamedBlockNumber = 0;
            final BlockItemUnparsed[] blockItems = SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsed(
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
         * {@link PublisherHandler#onNext(PublishStreamRequestUnparsed)} happy
         * path scenario. Here we stream a single complete valid block as items.
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
                "Test onNext() with valid request with a complete single block response EndOfStream, Code ERROR - happy path EndOfStream, Code ERROR")
        void testHappyPathResponseERROR() {
            // Setup request to send, in this case a single complete valid block
            // as items, starting with header and ending with proof
            final int streamedBlockNumber = 0;
            final BlockItemUnparsed[] blockItems = SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsed(
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
            // Assert single response is ResentBlock with block number same as streamed
            assertThat(repliesPipeline.getOnNextCalls())
                    .hasSize(1)
                    .first()
                    .returns(ResponseOneOfType.END_STREAM, responseKindExtractor)
                    .returns(Code.ERROR, endStreamResponseCodeExtractor)
                    .returns(expectedLatestBlockNumber, endStreamBlockNumberExtractor);
            // Assert no other responses sent
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
         * {@link BlockAction#END_ERROR} for the streamed block number the
         * metrics will be properly updated.
         */
        @Test
        @DisplayName(
                "Test onNext() with valid request with a complete single block metrics updated EndOfStream, Code ERROR - happy path EndOfStream, Code ERROR")
        void testHappyPathMetricsERROR() {
            // Setup request to send, in this case a single complete valid block
            // as items, starting with header and ending with proof
            final int streamedBlockNumber = 0;
            final BlockItemUnparsed[] blockItems = SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsed(
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
        void testPublishReturnInvalidRequestNoItemsPropagated() {
            // Setup request to send, in this case a single complete valid block
            // as items, starting with header and ending with proof
            final int streamedBlockNumber = 0;
            final BlockItemUnparsed[] blockItems = SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsed(
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
        void testPublishReturnInvalidRequestNoResponses() {
            // Setup request to send, in this case a single complete valid block
            // as items, starting with header and ending with proof
            final int streamedBlockNumber = 0;
            final BlockItemUnparsed[] blockItems = SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsed(
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
        void testPublishReturnInvalidRequestNoMetricsUpdated() {
            // Setup request to send, in this case a single complete valid block
            // as items, starting with header and ending with proof
            final int streamedBlockNumber = 0;
            final BlockItemUnparsed[] blockItems = SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsed(
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
        void testPrematureHeaderReceivedNoItemsPropagated() {
            // Setup request to send, in this case a single complete valid block
            // as items, starting with header and ending with proof
            final int streamedBlockNumber = 0;
            final BlockItemUnparsed[] blockItems = SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsed(
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
        void testPrematureHeaderReceivedResponse() {
            // Setup request to send, in this case a single complete valid block
            // as items, starting with header and ending with proof
            final int streamedBlockNumber = 0;
            final BlockItemUnparsed[] blockItems = SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsed(
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
            // Assert single response is EndOfStream with Code INVALID_REQUEST
            assertThat(repliesPipeline.getOnNextCalls())
                    .hasSize(1)
                    .first()
                    .returns(ResponseOneOfType.END_STREAM, responseKindExtractor)
                    .returns(Code.INVALID_REQUEST, endStreamResponseCodeExtractor)
                    .returns(manager.getLatestBlockNumber(), endStreamBlockNumberExtractor);
            // Assert no other responses sent
            assertThat(repliesPipeline.getOnErrorCalls()).isEmpty();
            assertThat(repliesPipeline.getOnSubscriptionCalls()).isEmpty();
            assertThat(repliesPipeline.getOnCompleteCalls().get()).isEqualTo(0);
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
        void testPrematureHeaderReceivedMetrics() {
            // Setup request to send, in this case a single complete valid block
            // as items, starting with header and ending with proof
            final int streamedBlockNumber = 0;
            final BlockItemUnparsed[] blockItems = SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsed(
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
            assertThat(metrics.endStreamsReceived().get()).isEqualTo(0);
        }

        /**
         * This test aims to assert that the {@link PublisherHandler} correctly
         * handles a received error response when the
         * {@link PublisherHandler#onError(Throwable)} gets called.
         * Here we expect that the handler will respond to the publisher with
         * an {@link org.hiero.block.api.PublishStreamResponse.EndOfStream}
         * with {@link org.hiero.block.api.PublishStreamResponse.EndOfStream.Code#ERROR}
         * and will proceed to orderly shutdown.
         */
        @Test
        @DisplayName("Test onError() with error response - EndOfStream, Code ERROR")
        void testOnErrorResponse() {
            // set an arbitrary valid latest streamed block number to expect in the response
            final long expectedLatestStreamedBlockNumber = 0L;
            manager.setLatestBlockNumber(expectedLatestStreamedBlockNumber);
            // Call onError with an arbitrary Throwable
            toTest.onError(new RuntimeException());
            // Assert single response is EndOfStream with Code ERROR
            assertThat(repliesPipeline.getOnNextCalls())
                    .hasSize(1)
                    .first()
                    .returns(ResponseOneOfType.END_STREAM, responseKindExtractor)
                    .returns(Code.ERROR, endStreamResponseCodeExtractor)
                    .returns(expectedLatestStreamedBlockNumber, endStreamBlockNumberExtractor);
            // Assert no other responses sent
            assertThat(repliesPipeline.getOnErrorCalls()).isEmpty();
            assertThat(repliesPipeline.getOnSubscriptionCalls()).isEmpty();
            assertThat(repliesPipeline.getOnCompleteCalls().get()).isEqualTo(0);
            assertThat(repliesPipeline.getClientEndStreamCalls().get()).isEqualTo(0);
        }

        /**
         * This test aims to assert that the {@link PublisherHandler} correctly
         * handles a received error response when the
         * {@link PublisherHandler#onError(Throwable)} gets called.
         * Here we expect that metrics will be updated accordingly to reflect
         * the received error.
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
            assertThat(metrics.endStreamsReceived().get()).isEqualTo(0);
        }
        // @todo(1419) tests to add:
        //    test onNext() can receive next request if previous request was invalid (header expected but not present)
        //    test broken header (unable to parse) will send invalid response and shutdown
        //    test null header bytes will send error and shutdown
        //    test for sending acks
        //    test for calling closeBlock()
        //    test for handleBlockProof() when parse is not successful
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
                new DefaultCounter(new Config("category", "name")));
    }
}
