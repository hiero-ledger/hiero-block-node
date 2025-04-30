// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.stream.subscriber;

import static org.hiero.block.node.app.fixtures.blocks.BlockItemUtils.toBlockItemUnparsed;
import static org.hiero.block.node.app.fixtures.blocks.SimpleTestBlockItemBuilder.sampleBlockHeader;
import static org.hiero.block.node.app.fixtures.blocks.SimpleTestBlockItemBuilder.sampleBlockProof;
import static org.hiero.block.node.app.fixtures.blocks.SimpleTestBlockItemBuilder.sampleRoundHeader;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.*;

import com.hedera.pbj.runtime.grpc.Pipeline;
import com.swirlds.config.api.Configuration;
import com.swirlds.config.api.ConfigurationBuilder;
import com.swirlds.metrics.api.Metrics;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Flow;
import org.hiero.block.api.SubscribeStreamRequest;
import org.hiero.block.api.SubscribeStreamResponse;
import org.hiero.block.internal.BlockItemSetUnparsed;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.internal.SubscribeStreamResponseUnparsed;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.BlockMessagingFacility;
import org.hiero.block.node.spi.historicalblocks.BlockAccessor;
import org.hiero.block.node.spi.historicalblocks.BlockRangeSet;
import org.hiero.block.node.spi.historicalblocks.HistoricalBlockFacility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.exceptions.verification.TooFewActualInvocations;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Unit tests for {@link BlockStreamSubscriberSession}.
 * This test class focuses on testing the core functionality of the session management
 * and block streaming capabilities.
 */
@ExtendWith(MockitoExtension.class)
class BlockStreamSubscriberSessionTest {

    @Mock
    private BlockNodeContext context;

    @Mock
    private Metrics metrics;

    @Mock
    private HistoricalBlockFacility historicalBlockFacility;

    @Mock
    private BlockMessagingFacility blockMessagingFacility;

    private Pipeline<? super SubscribeStreamResponseUnparsed> responsePipeline;

    private BlockStreamSubscriberSession session;
    private CountDownLatch sessionReadyLatch;
    private static final long CLIENT_ID = 0L;

    @BeforeEach
    void setUp() {
        sessionReadyLatch = new CountDownLatch(1);
        final Configuration configuration = ConfigurationBuilder.create()
                .withConfigDataType(SubscriberConfig.class)
                .build();
        responsePipeline = spy(new ResponsePipeline());

        when(context.configuration()).thenReturn(configuration);
        when(context.metrics()).thenReturn(metrics);
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
        void shouldStreamHistoricalAndLiveBlocksSuccessfully() throws InterruptedException {
            // Setup test parameters
            final long MIN_AVAILABLE_BLOCK = 0L;
            final long MAX_AVAILABLE_BLOCK = 10L;
            final long START_BLOCK = 0L;
            final long END_BLOCK = 20L;

            // Initialize session and setup mocks
            final SubscribeStreamRequest subscribeStreamRequest = createRequest(START_BLOCK, END_BLOCK);
            session = new BlockStreamSubscriberSession(
                    CLIENT_ID, subscribeStreamRequest, responsePipeline, context, sessionReadyLatch);
            setupHistoricalBlockProvider(MIN_AVAILABLE_BLOCK, MAX_AVAILABLE_BLOCK);

            // Setup historical blocks
            final BlockStreamSubscriberSession.LiveBlockHandler liveBlockHandler = session.getLiveBlockHandler();
            for (long i = MIN_AVAILABLE_BLOCK; i < MAX_AVAILABLE_BLOCK; i++) {
                BlockItems blockItems = createSampleBlockItems(i);
                liveBlockHandler.handleBlockItemsReceived(blockItems);

                final BlockAccessor blockAccessor = mock(BlockAccessor.class);
                final BlockUnparsed blockUnparsed = BlockUnparsed.newBuilder()
                        .blockItems(blockItems.blockItems())
                        .build();

                lenient().when(blockAccessor.blockUnparsed()).thenReturn(blockUnparsed);
                lenient().when(context.historicalBlockProvider().block(i)).thenReturn(blockAccessor);
            }

            // Start session in separate thread
            Thread sessionThread = new Thread(session::call);
            sessionThread.start();

            // Phase 1: Historical Block Streaming
            boolean historicalProvided = false;
            while (!historicalProvided) {
                try {
                    verify(responsePipeline, atLeast((int) MAX_AVAILABLE_BLOCK))
                            .onNext(any(SubscribeStreamResponseUnparsed.class));
                    historicalProvided = true;
                } catch (TooFewActualInvocations e) {
                    Thread.sleep(100);
                }
            }

            // Phase 2: Live Block Streaming
            for (long i = MAX_AVAILABLE_BLOCK; i <= END_BLOCK; i++) {
                BlockItems blockItems = createSampleBlockItems(i);
                liveBlockHandler.handleBlockItemsReceived(blockItems);

                final SubscribeStreamResponseUnparsed.Builder response = SubscribeStreamResponseUnparsed.newBuilder()
                        .blockItems(BlockItemSetUnparsed.newBuilder().blockItems(blockItems.blockItems()));
                Thread.sleep(100);
                verify(responsePipeline, times(1)).onNext(response.build());
            }

            // Verify final pipeline state
            verify(responsePipeline, times(1)).onComplete();
            verify(responsePipeline, never()).onError(any(Throwable.class));
        }

        /**
         * Tests the historical block streaming functionality in isolation.
         * Verifies that the session can properly stream blocks from the historical provider.
         */
        @Test
        @DisplayName("Should successfully stream historical blocks")
        void shouldStreamHistoricalBlocksSuccessfully() {
            // Setup test parameters
            final long MIN_AVAILABLE_BLOCK = 0L;
            final long MAX_AVAILABLE_BLOCK = 20L;
            final long START_BLOCK = 1L;
            final long END_BLOCK = 10L;

            // Initialize session and setup mocks
            final SubscribeStreamRequest subscribeStreamRequest = createRequest(START_BLOCK, END_BLOCK);
            session = new BlockStreamSubscriberSession(
                    CLIENT_ID, subscribeStreamRequest, responsePipeline, context, sessionReadyLatch);
            setupHistoricalBlockProvider(MIN_AVAILABLE_BLOCK, MAX_AVAILABLE_BLOCK);

            // Setup historical blocks
            final BlockStreamSubscriberSession.LiveBlockHandler liveBlockHandler = session.getLiveBlockHandler();
            for (int i = (int) MIN_AVAILABLE_BLOCK; i < MAX_AVAILABLE_BLOCK; i++) {
                BlockItems blockItems = createSampleBlockItems(i);
                liveBlockHandler.handleBlockItemsReceived(blockItems);

                final BlockAccessor blockAccessor = mock(BlockAccessor.class);
                final BlockUnparsed blockUnparsed = BlockUnparsed.newBuilder()
                        .blockItems(blockItems.blockItems())
                        .build();

                lenient().when(blockAccessor.blockUnparsed()).thenReturn(blockUnparsed);
                lenient().when(context.historicalBlockProvider().block(i)).thenReturn(blockAccessor);
            }

            // Execute session
            session.call();

            // Verify pipeline interactions
            verify(responsePipeline, times(11)).onNext(any(SubscribeStreamResponseUnparsed.class));
            verify(responsePipeline, times(1)).onComplete();
            verify(responsePipeline, never()).onError(any(Throwable.class));

            // Verify historical block provider interactions
            verify(context.historicalBlockProvider(), times(10)).block(anyLong());
        }
    }

    /**
     * Tests related to input validation and error handling in the BlockStreamSubscriberSession.
     * This class verifies that the session properly handles invalid input parameters.
     */
    @Nested
    @DisplayName("Input Validation Tests")
    class ValidationTests {

        /**
         * Tests the handling of a start block number that is neither available in history
         * nor in the live stream range.
         */
        @Test
        @DisplayName("Should reject request with start block outside available range")
        void shouldRejectRequestWithInvalidStartBlockRange() {
            // Setup test parameters
            final long MIN_AVAILABLE_BLOCK = 0L;
            final long MAX_AVAILABLE_BLOCK = 20L;
            final long START_BLOCK = 100000L;
            final long END_BLOCK = 200000L;

            // Initialize session and setup mocks
            final SubscribeStreamRequest subscribeStreamRequest = createRequest(START_BLOCK, END_BLOCK);
            setupHistoricalBlockProvider(MIN_AVAILABLE_BLOCK, MAX_AVAILABLE_BLOCK);
            session = new BlockStreamSubscriberSession(
                    CLIENT_ID, subscribeStreamRequest, responsePipeline, context, sessionReadyLatch);

            // Execute session
            session.call();

            // Verify error response
            final SubscribeStreamResponseUnparsed.Builder response = SubscribeStreamResponseUnparsed.newBuilder()
                    .status(SubscribeStreamResponse.Code.READ_STREAM_INVALID_START_BLOCK_NUMBER);

            verify(responsePipeline, times(1)).onNext(response.build());
            verify(responsePipeline, times(1)).onComplete();
            verify(responsePipeline, never()).onError(any(Throwable.class));
        }

        /**
         * Tests the handling of a negative start block number.
         */
        @Test
        @DisplayName("Should reject request with negative start block number")
        void shouldRejectRequestWithNegativeStartBlock() {
            // Setup test parameters
            final long MIN_AVAILABLE_BLOCK = 0L;
            final long MAX_AVAILABLE_BLOCK = 20L;
            final long START_BLOCK = -2L;
            final long END_BLOCK = 10L;

            // Initialize session and setup mocks
            final SubscribeStreamRequest subscribeStreamRequest = createRequest(START_BLOCK, END_BLOCK);
            setupHistoricalBlockProvider(MIN_AVAILABLE_BLOCK, MAX_AVAILABLE_BLOCK);
            session = new BlockStreamSubscriberSession(
                    CLIENT_ID, subscribeStreamRequest, responsePipeline, context, sessionReadyLatch);

            // Execute session
            session.call();

            // Verify error response
            final SubscribeStreamResponseUnparsed.Builder response = SubscribeStreamResponseUnparsed.newBuilder()
                    .status(SubscribeStreamResponse.Code.READ_STREAM_INVALID_START_BLOCK_NUMBER);

            verify(responsePipeline, times(1)).onNext(response.build());
            verify(responsePipeline, times(1)).onComplete();
            verify(responsePipeline, never()).onError(any(Throwable.class));
        }

        /**
         * Tests the handling of a negative end block number.
         */
        @Test
        @DisplayName("Should reject request with negative end block number")
        void shouldRejectRequestWithNegativeEndBlock() {
            // Setup test parameters
            final long MIN_AVAILABLE_BLOCK = 0L;
            final long MAX_AVAILABLE_BLOCK = 20L;
            final long START_BLOCK = -1L;
            final long END_BLOCK = -2L;

            // Initialize session and setup mocks
            final SubscribeStreamRequest subscribeStreamRequest = createRequest(START_BLOCK, END_BLOCK);
            setupHistoricalBlockProvider(MIN_AVAILABLE_BLOCK, MAX_AVAILABLE_BLOCK);
            session = new BlockStreamSubscriberSession(
                    CLIENT_ID, subscribeStreamRequest, responsePipeline, context, sessionReadyLatch);

            // Execute session
            session.call();

            // Verify error response
            final SubscribeStreamResponseUnparsed.Builder response = SubscribeStreamResponseUnparsed.newBuilder()
                    .status(SubscribeStreamResponse.Code.READ_STREAM_INVALID_END_BLOCK_NUMBER);

            verify(responsePipeline, times(1)).onNext(response.build());
            verify(responsePipeline, times(1)).onComplete();
            verify(responsePipeline, never()).onError(any(Throwable.class));
        }

        /**
         * Tests the handling of an end block number that is less than the start block number.
         */
        @Test
        @DisplayName("Should reject request with end block less than start block")
        void shouldRejectRequestWithEndBlockLessThanStartBlock() {
            // Setup test parameters
            final long MIN_AVAILABLE_BLOCK = 0L;
            final long MAX_AVAILABLE_BLOCK = 20L;
            final long START_BLOCK = 10L;
            final long END_BLOCK = 0L;

            // Initialize session and setup mocks
            final SubscribeStreamRequest subscribeStreamRequest = createRequest(START_BLOCK, END_BLOCK);
            setupHistoricalBlockProvider(MIN_AVAILABLE_BLOCK, MAX_AVAILABLE_BLOCK);
            session = new BlockStreamSubscriberSession(
                    CLIENT_ID, subscribeStreamRequest, responsePipeline, context, sessionReadyLatch);

            // Execute session
            session.call();

            // Verify error response
            final SubscribeStreamResponseUnparsed.Builder response = SubscribeStreamResponseUnparsed.newBuilder()
                    .status(SubscribeStreamResponse.Code.READ_STREAM_INVALID_END_BLOCK_NUMBER);

            verify(responsePipeline, times(1)).onNext(response.build());
            verify(responsePipeline, times(1)).onComplete();
            verify(responsePipeline, never()).onError(any(Throwable.class));
        }
    }

    /**
     * Creates a sample block items set for testing.
     *
     * @param blockNumber the block number to create items for
     * @return a BlockItems object containing sample block items
     */
    private BlockItems createSampleBlockItems(long blockNumber) {
        BlockItemUnparsed blockHeader = toBlockItemUnparsed(sampleBlockHeader(blockNumber));
        BlockItemUnparsed roundHeader = toBlockItemUnparsed(sampleRoundHeader(blockNumber));
        BlockItemUnparsed blockProof = toBlockItemUnparsed(sampleBlockProof(blockNumber));
        return new BlockItems(List.of(blockHeader, roundHeader, blockProof), blockNumber);
    }

    /**
     * Sets up the historical block provider mock with the specified block range.
     *
     * @param minBlock the minimum available block number
     * @param maxBlock the maximum available block number
     */
    private void setupHistoricalBlockProvider(long minBlock, long maxBlock) {
        final BlockRangeSet availableBlocks = mock(BlockRangeSet.class);
        when(availableBlocks.min()).thenReturn(minBlock);
        when(availableBlocks.max()).thenReturn(maxBlock);
        when(context.historicalBlockProvider()).thenReturn(historicalBlockFacility);
        when(context.blockMessaging()).thenReturn(blockMessagingFacility);
        when(context.historicalBlockProvider().availableBlocks()).thenReturn(availableBlocks);
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
    private class ResponsePipeline implements Pipeline<SubscribeStreamResponseUnparsed> {

        @Override
        public void clientEndStreamReceived() {
            Pipeline.super.clientEndStreamReceived();
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {}

        @Override
        public void onNext(SubscribeStreamResponseUnparsed item) throws RuntimeException {
            Pipeline.super.onNext(item);
        }

        @Override
        public void onError(Throwable throwable) {}

        @Override
        public void onComplete() {}
    }
}
