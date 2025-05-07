// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.stream.subscriber;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hiero.block.node.app.fixtures.blocks.SimpleTestBlockItemBuilder.createNumberOfSimpleBlockBatches;

import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.grpc.Pipeline;
import com.swirlds.config.api.Configuration;
import com.swirlds.config.api.ConfigurationBuilder;
import com.swirlds.metrics.api.Metrics;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.hiero.block.api.SubscribeStreamRequest;
import org.hiero.block.api.SubscribeStreamResponse;
import org.hiero.block.internal.SubscribeStreamResponseUnparsed;
import org.hiero.block.node.app.fixtures.plugintest.SimpleInMemoryHistoricalBlockFacility;
import org.hiero.block.node.app.fixtures.plugintest.TestBlockMessagingFacility;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.blockmessaging.BlockItemHandler;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.BlockMessagingFacility;
import org.hiero.block.node.spi.historicalblocks.HistoricalBlockFacility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Unit tests for {@link BlockStreamSubscriberSession}.
 * This test class focuses on testing the core functionality of the session management
 * and block streaming capabilities.
 */
@ExtendWith(MockitoExtension.class)
class BlockStreamSubscriberSessionTest {
    private static final int RESPONSE_WAIT_LIMIT = 1000;
    private final HistoricalBlockFacility historicalBlockFacility = new SimpleInMemoryHistoricalBlockFacility();
    private final BlockMessagingFacility blockMessagingFacility = new TestBlockMessagingFacility();
    private final BlockItemHandler providerHandler = (BlockItemHandler)historicalBlockFacility;

    private BlockNodeContext context;
    private ResponsePipeline responsePipeline;
    private BlockStreamSubscriberSession session;
    private CountDownLatch sessionReadyLatch;
    private static final long CLIENT_ID = 0L;

    @Mock
    private Metrics metrics; // If we can use a "real" metrics, or a fixture, instead of this Mock, we should.

    @BeforeEach
    void setUp() {
        sessionReadyLatch = new CountDownLatch(1);
        final Configuration configuration = ConfigurationBuilder.create()
                .withConfigDataType(SubscriberConfig.class)
                .build();
        responsePipeline = new ResponsePipeline();
        context = new BlockNodeContext(configuration, metrics, null,
                blockMessagingFacility, historicalBlockFacility, null);
        historicalBlockFacility.init(context, null);
        blockMessagingFacility.init(context, null); // Probably not needed, but that can change.
    }

    /**
     * Tests related to streaming functionality of the BlockStreamSubscriberSession.
     * This includes testing both historical and live block streaming scenarios.
     */
    @Nested
    @DisplayName("Streaming Functionality Tests")
    class StreamingTests {

        /**
         * Tests the complete flow of streaming both historical and live blocks.
         * Verifies that the session can handle:
         * 1. Historical block streaming
         * 2. Transition to live block streaming
         * 3. Proper pipeline interactions
         */
        @Test
        @DisplayName("Should successfully stream both historical and live blocks in sequence")
        void shouldStreamHistoricalAndLiveBlocksSuccessfully()
                throws InterruptedException, ExecutionException, TimeoutException {
            // Setup test parameters
            final int MIN_AVAILABLE_BLOCK = 0;
            final int MAX_AVAILABLE_BLOCK = 10;
            final int START_BLOCK = 0;
            final int END_BLOCK = 19;

            // Initialize session
            final SubscribeStreamRequest subscribeStreamRequest = createRequest(START_BLOCK, END_BLOCK);
            setupHistoricalBlockProvider(MIN_AVAILABLE_BLOCK, MAX_AVAILABLE_BLOCK + 1);
            session = new BlockStreamSubscriberSession(
                    CLIENT_ID, subscribeStreamRequest, responsePipeline, context, sessionReadyLatch);

            try (final ExecutorService sessionExecutor = Executors.newVirtualThreadPerTaskExecutor()) {
                Future<BlockStreamSubscriberSession> sessionFuture = sessionExecutor.submit(session);
                sessionReadyLatch.await();
                // Phase 1: Historical Block Streaming
                final int expectedResponseCount = END_BLOCK - START_BLOCK;

                // Phase 2: Live Block Streaming
                BlockItems[] liveBatches = createNumberOfSimpleBlockBatches(MAX_AVAILABLE_BLOCK, END_BLOCK + 1);
                for (BlockItems next : liveBatches) {
                    blockMessagingFacility.sendBlockItems(next);
                }
                // Wait for everything to complete.
                // Note, don't try to wait before this; there are no execution guarantees until
                // `get` is called; before that the thread may not run or may be parked indefinitely.
                sessionFuture.get(1L, TimeUnit.SECONDS); // The timeout doesn't work, for some reason, but it's here because we don't have a better alternative.
            }

            // Verify final pipeline state
            assertThat(responsePipeline.getReceivedResponses()).hasSize(21);
            assertThat(responsePipeline.getCompletionCount()).isEqualTo(1);
            assertThat(responsePipeline.getPipelineErrors()).isEmpty();
        }

        /**
         * Tests the historical block streaming functionality in isolation.
         * Verifies that the session can properly stream blocks from the historical provider.
         */
        @Test
        @DisplayName("Should successfully stream historical blocks")
        void shouldStreamHistoricalBlocksSuccessfully() {
            // Setup test parameters
            final int MIN_AVAILABLE_BLOCK = 0;
            final int MAX_AVAILABLE_BLOCK = 20;
            final long START_BLOCK = 1L;
            final long END_BLOCK = 10L;

            // Initialize session
            final SubscribeStreamRequest subscribeStreamRequest = createRequest(START_BLOCK, END_BLOCK);
            session = new BlockStreamSubscriberSession(
                    CLIENT_ID, subscribeStreamRequest, responsePipeline, context, sessionReadyLatch);
            setupHistoricalBlockProvider(MIN_AVAILABLE_BLOCK, MAX_AVAILABLE_BLOCK);
            // Execute session
            session.call();

            // Verify pipeline interactions
            assertThat(responsePipeline.getReceivedResponses()).hasSize(11);
            assertThat(responsePipeline.getCompletionCount()).isEqualTo(1);
            assertThat(responsePipeline.getPipelineErrors()).isEmpty();
        }
    }

    /**
     * Sets up the historical block provider with the specified block range.
     *
     * @param minBlock the minimum available block number
     * @param maxBlock the maximum available block number
     */
    private void setupHistoricalBlockProvider(int minBlock, int maxBlock) {
        if(maxBlock < minBlock || maxBlock - minBlock > 100_000L) {
            throw new IllegalArgumentException("Invalid block range");
        }
        // "publish" blocks from min to max so the historical provider has them.
        final BlockItems[] batchesToLoad = createNumberOfSimpleBlockBatches(minBlock, maxBlock);
        for (BlockItems next : batchesToLoad) {
            providerHandler.handleBlockItemsReceived(next);
        }
    }

    /**
     * Creates a SubscribeStreamRequest with the specified block range.
     *
     * @param startNumber the start block number
     * @param endNumber the end block number
     * @return a new SubscribeStreamRequest
     */
    private SubscribeStreamRequest createRequest(final long startNumber, final long endNumber) {
        return SubscribeStreamRequest.newBuilder()
                .startBlockNumber(startNumber)
                .endBlockNumber(endNumber)
                .build();
    }

    /**
     * A simple implementation of Pipeline for testing purposes.
     */
    private static class ResponsePipeline implements Pipeline<SubscribeStreamResponseUnparsed> {
        /** The GRPC bytes received from the plugin. */
        private final List<SubscribeStreamResponse> receivedResponses = new ArrayList<>();
        private final List<Throwable> pipelineErrors = new ArrayList<>();
        private int completionCount = 0;

        public List<SubscribeStreamResponse> getReceivedResponses() {
            return receivedResponses;
        }

        public List<Throwable> getPipelineErrors() {
            return pipelineErrors;
        }

        public int getCompletionCount() {
            return completionCount;
        }

        @Override
        public void clientEndStreamReceived() {
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {}

        @Override
        public void onNext(SubscribeStreamResponseUnparsed item) {
            try {
                var binary = SubscribeStreamResponseUnparsed.PROTOBUF.toBytes(item);
                var response = SubscribeStreamResponse.PROTOBUF.parse(binary);
                receivedResponses.add(response);
            } catch (ParseException e) {
                throw new RuntimeException("Failed to parse SubscribeStreamResponse", e);
            }
        }

        @Override
        public void onError(Throwable throwable) {
            pipelineErrors.add(throwable);
        }

        @Override
        public void onComplete() {
            completionCount++;
        }
    }
}
