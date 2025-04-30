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

    @Nested
    @DisplayName("Streaming Tests")
    class StreamingTests {

        @Test
        @DisplayName("Should handle successfully stream historical and live blocks")
        void startingSessionWithValidRequestForHistoricalAndLiveBlocks() throws InterruptedException {
            final long MIN_AVAILABLE_BLOCK = 0L;
            final long MAX_AVAILABLE_BLOCK = 10L;

            final long START_BLOCK = 0L;
            final long END_BLOCK = 20L;

            final SubscribeStreamRequest subscribeStreamRequest = createRequest(START_BLOCK, END_BLOCK);
            session = new BlockStreamSubscriberSession(
                    CLIENT_ID, subscribeStreamRequest, responsePipeline, context, sessionReadyLatch);
            when(context.historicalBlockProvider()).thenReturn(historicalBlockFacility);
            final BlockRangeSet availableBlocks = mock(BlockRangeSet.class);
            when(availableBlocks.min()).thenReturn(MIN_AVAILABLE_BLOCK);
            when(availableBlocks.max()).thenReturn(MAX_AVAILABLE_BLOCK);
            when(context.historicalBlockProvider().availableBlocks()).thenReturn(availableBlocks);
            when(context.blockMessaging()).thenReturn(blockMessagingFacility);
            final BlockStreamSubscriberSession.LiveBlockHandler liveBlockHandler = session.getLiveBlockHandler();
            for (long i = MIN_AVAILABLE_BLOCK; i < MAX_AVAILABLE_BLOCK; i++) {
                BlockItemUnparsed SAMPLE_BLOCK_HEADER = toBlockItemUnparsed(sampleBlockHeader(i));
                BlockItemUnparsed SAMPLE_ROUND_HEADER = toBlockItemUnparsed(sampleRoundHeader(i));
                BlockItemUnparsed SAMPLE_BLOCK_PROOF = toBlockItemUnparsed(sampleBlockProof(i));
                final BlockItems blockItems =
                        new BlockItems(List.of(SAMPLE_BLOCK_HEADER, SAMPLE_ROUND_HEADER, SAMPLE_BLOCK_PROOF), i);
                liveBlockHandler.handleBlockItemsReceived(blockItems);

                final BlockAccessor blockAccessor = mock(BlockAccessor.class);
                final BlockUnparsed blockUnparsed = BlockUnparsed.newBuilder()
                        .blockItems(List.of(SAMPLE_BLOCK_HEADER, SAMPLE_ROUND_HEADER, SAMPLE_BLOCK_PROOF))
                        .build();

                lenient().when(blockAccessor.blockUnparsed()).thenReturn(blockUnparsed);
                lenient().when(context.historicalBlockProvider().block(i)).thenReturn(blockAccessor);
            }
            Thread sessionThread = new Thread(session::call);
            sessionThread.start();
            boolean historicalProvided = false;
            while (!historicalProvided) {
                try {
                    verify(responsePipeline, atLeast((int) MAX_AVAILABLE_BLOCK))
                            .onNext(any(SubscribeStreamResponseUnparsed.class));
                    historicalProvided = true;
                } catch (TooFewActualInvocations e) {
                    // Wait for consumption of the historically provided blocks
                    Thread.sleep(100);
                }
            }

            verify(responsePipeline, times((int) MAX_AVAILABLE_BLOCK))
                    .onNext(any(SubscribeStreamResponseUnparsed.class));
            for (long i = MAX_AVAILABLE_BLOCK; i <= END_BLOCK; i++) {
                BlockItemUnparsed SAMPLE_BLOCK_HEADER = toBlockItemUnparsed(sampleBlockHeader(i));
                BlockItemUnparsed SAMPLE_ROUND_HEADER = toBlockItemUnparsed(sampleRoundHeader(i));
                BlockItemUnparsed SAMPLE_BLOCK_PROOF = toBlockItemUnparsed(sampleBlockProof(i));
                final BlockItems blockItems =
                        new BlockItems(List.of(SAMPLE_BLOCK_HEADER, SAMPLE_ROUND_HEADER, SAMPLE_BLOCK_PROOF), i);
                liveBlockHandler.handleBlockItemsReceived(blockItems);

                final SubscribeStreamResponseUnparsed.Builder response = SubscribeStreamResponseUnparsed.newBuilder()
                        .blockItems(BlockItemSetUnparsed.newBuilder()
                                .blockItems(List.of(SAMPLE_BLOCK_HEADER, SAMPLE_ROUND_HEADER, SAMPLE_BLOCK_PROOF)));
                Thread.sleep(100);
                verify(responsePipeline, times(1)).onNext(response.build());
            }

            // Verify interactions with the pipeline
            verify(responsePipeline, times(1)).onComplete();
            verify(responsePipeline, never()).onError(any(Throwable.class));
        }

        /**
         * Tests that sessions successfully handles valid request for historical blocks.
         */
        @Test
        @DisplayName("Should handle call of session with valid historical request")
        void callingSessionWithValidRequestOnHistoricalBlocks() {
            final long MIN_AVAILABLE_BLOCK = 0L;
            final long MAX_AVAILABLE_BLOCK = 20L;

            final long START_BLOCK = 1L;
            final long END_BLOCK = 10L;

            final SubscribeStreamRequest subscribeStreamRequest = createRequest(START_BLOCK, END_BLOCK);
            session = new BlockStreamSubscriberSession(
                    CLIENT_ID, subscribeStreamRequest, responsePipeline, context, sessionReadyLatch);
            when(context.historicalBlockProvider()).thenReturn(historicalBlockFacility);
            final BlockRangeSet availableBlocks = mock(BlockRangeSet.class);
            when(availableBlocks.min()).thenReturn(MIN_AVAILABLE_BLOCK);
            when(availableBlocks.max()).thenReturn(MAX_AVAILABLE_BLOCK);
            when(context.historicalBlockProvider().availableBlocks()).thenReturn(availableBlocks);
            when(context.blockMessaging()).thenReturn(blockMessagingFacility);
            final BlockStreamSubscriberSession.LiveBlockHandler liveBlockHandler = session.getLiveBlockHandler();
            for (int i = (int) MIN_AVAILABLE_BLOCK; i < MAX_AVAILABLE_BLOCK; i++) {
                BlockItemUnparsed SAMPLE_BLOCK_HEADER = toBlockItemUnparsed(sampleBlockHeader(i));
                BlockItemUnparsed SAMPLE_ROUND_HEADER = toBlockItemUnparsed(sampleRoundHeader(i));
                BlockItemUnparsed SAMPLE_BLOCK_PROOF = toBlockItemUnparsed(sampleBlockProof(i));
                final BlockItems blockItems =
                        new BlockItems(List.of(SAMPLE_BLOCK_HEADER, SAMPLE_ROUND_HEADER, SAMPLE_BLOCK_PROOF), i);
                liveBlockHandler.handleBlockItemsReceived(blockItems);

                final BlockAccessor blockAccessor = mock(BlockAccessor.class);
                final BlockUnparsed blockUnparsed = BlockUnparsed.newBuilder()
                        .blockItems(List.of(SAMPLE_BLOCK_HEADER, SAMPLE_ROUND_HEADER, SAMPLE_BLOCK_PROOF))
                        .build();

                lenient().when(blockAccessor.blockUnparsed()).thenReturn(blockUnparsed);
                lenient().when(context.historicalBlockProvider().block(i)).thenReturn(blockAccessor);
            }
            session.call();

            // Verify interactions with the pipeline
            verify(responsePipeline, times(11)).onNext(any(SubscribeStreamResponseUnparsed.class));
            verify(responsePipeline, times(1)).onComplete();
            verify(responsePipeline, never()).onError(any(Throwable.class));

            // Verify interactions with the historical block provider
            verify(context.historicalBlockProvider(), times(10)).block(anyLong());
        }
    }

    @Nested
    @DisplayName("Validation Tests")
    class ValidationTests {
        @Test
        @DisplayName(
                "Should end with READ_STREAM_INVALID_START_BLOCK_NUMBER for neither live nor historical start block")
        void shouldEndStreamForNeitherLiveNorHistoryStartBlock() {
            final long MIN_AVAILABLE_BLOCK = 0L;
            final long MAX_AVAILABLE_BLOCK = 20L;

            final long START_BLOCK = 100000L;
            final long END_BLOCK = 200000L;

            final SubscribeStreamRequest subscribeStreamRequest = createRequest(START_BLOCK, END_BLOCK);
            final BlockRangeSet availableBlocks = mock(BlockRangeSet.class);

            when(availableBlocks.min()).thenReturn(MIN_AVAILABLE_BLOCK);
            when(availableBlocks.max()).thenReturn(MAX_AVAILABLE_BLOCK);
            when(context.historicalBlockProvider()).thenReturn(historicalBlockFacility);
            when(context.blockMessaging()).thenReturn(blockMessagingFacility);
            when(context.historicalBlockProvider().availableBlocks()).thenReturn(availableBlocks);

            session = new BlockStreamSubscriberSession(
                    CLIENT_ID, subscribeStreamRequest, responsePipeline, context, sessionReadyLatch);
            session.call();

            final SubscribeStreamResponseUnparsed.Builder response = SubscribeStreamResponseUnparsed.newBuilder()
                    .status(SubscribeStreamResponse.Code.READ_STREAM_INVALID_START_BLOCK_NUMBER);

            verify(responsePipeline, times(1)).onNext(response.build());
            verify(responsePipeline, times(1)).onComplete();
            verify(responsePipeline, never()).onError(any(Throwable.class));
        }

        @Test
        @DisplayName("Should end with READ_STREAM_INVALID_START_BLOCK_NUMBER for invalid start block")
        void shouldEndStreamForInvalidStartBlock() {
            final long MIN_AVAILABLE_BLOCK = 0L;
            final long MAX_AVAILABLE_BLOCK = 20L;

            final long START_BLOCK = -2L;
            final long END_BLOCK = 10L;

            final SubscribeStreamRequest subscribeStreamRequest = createRequest(START_BLOCK, END_BLOCK);
            final BlockRangeSet availableBlocks = mock(BlockRangeSet.class);

            when(availableBlocks.min()).thenReturn(MIN_AVAILABLE_BLOCK);
            when(availableBlocks.max()).thenReturn(MAX_AVAILABLE_BLOCK);
            when(context.historicalBlockProvider()).thenReturn(historicalBlockFacility);
            when(context.blockMessaging()).thenReturn(blockMessagingFacility);
            when(context.historicalBlockProvider().availableBlocks()).thenReturn(availableBlocks);

            session = new BlockStreamSubscriberSession(
                    CLIENT_ID, subscribeStreamRequest, responsePipeline, context, sessionReadyLatch);
            session.call();

            final SubscribeStreamResponseUnparsed.Builder response = SubscribeStreamResponseUnparsed.newBuilder()
                    .status(SubscribeStreamResponse.Code.READ_STREAM_INVALID_START_BLOCK_NUMBER);

            verify(responsePipeline, times(1)).onNext(response.build());
            verify(responsePipeline, times(1)).onComplete();
            verify(responsePipeline, never()).onError(any(Throwable.class));
        }

        @Test
        @DisplayName("Should end with READ_STREAM_INVALID_END_BLOCK_NUMBER for invalid end block")
        void shouldEndStreamForInvalidEndBlock() {
            final long MIN_AVAILABLE_BLOCK = 0L;
            final long MAX_AVAILABLE_BLOCK = 20L;

            final long START_BLOCK = -1L;
            final long END_BLOCK = -2L;

            final SubscribeStreamRequest subscribeStreamRequest = createRequest(START_BLOCK, END_BLOCK);
            final BlockRangeSet availableBlocks = mock(BlockRangeSet.class);

            when(availableBlocks.min()).thenReturn(MIN_AVAILABLE_BLOCK);
            when(availableBlocks.max()).thenReturn(MAX_AVAILABLE_BLOCK);
            when(context.historicalBlockProvider()).thenReturn(historicalBlockFacility);
            when(context.blockMessaging()).thenReturn(blockMessagingFacility);
            when(context.historicalBlockProvider().availableBlocks()).thenReturn(availableBlocks);

            session = new BlockStreamSubscriberSession(
                    CLIENT_ID, subscribeStreamRequest, responsePipeline, context, sessionReadyLatch);
            session.call();

            final SubscribeStreamResponseUnparsed.Builder response = SubscribeStreamResponseUnparsed.newBuilder()
                    .status(SubscribeStreamResponse.Code.READ_STREAM_INVALID_END_BLOCK_NUMBER);

            verify(responsePipeline, times(1)).onNext(response.build());
            verify(responsePipeline, times(1)).onComplete();
            verify(responsePipeline, never()).onError(any(Throwable.class));
        }

        @Test
        @DisplayName("Should end with READ_STREAM_INVALID_END_BLOCK_NUMBER for higher end block than start block")
        void shouldEndStreamForHigherEndBlockThanStartBlock() {
            final long MIN_AVAILABLE_BLOCK = 0L;
            final long MAX_AVAILABLE_BLOCK = 20L;

            final long START_BLOCK = 10L;
            final long END_BLOCK = 0L;

            final SubscribeStreamRequest subscribeStreamRequest = createRequest(START_BLOCK, END_BLOCK);
            final BlockRangeSet availableBlocks = mock(BlockRangeSet.class);

            when(availableBlocks.min()).thenReturn(MIN_AVAILABLE_BLOCK);
            when(availableBlocks.max()).thenReturn(MAX_AVAILABLE_BLOCK);
            when(context.historicalBlockProvider()).thenReturn(historicalBlockFacility);
            when(context.blockMessaging()).thenReturn(blockMessagingFacility);
            when(context.historicalBlockProvider().availableBlocks()).thenReturn(availableBlocks);

            session = new BlockStreamSubscriberSession(
                    CLIENT_ID, subscribeStreamRequest, responsePipeline, context, sessionReadyLatch);
            session.call();

            final SubscribeStreamResponseUnparsed.Builder response = SubscribeStreamResponseUnparsed.newBuilder()
                    .status(SubscribeStreamResponse.Code.READ_STREAM_INVALID_END_BLOCK_NUMBER);

            verify(responsePipeline, times(1)).onNext(response.build());
            verify(responsePipeline, times(1)).onComplete();
            verify(responsePipeline, never()).onError(any(Throwable.class));
        }
    }

    private SubscribeStreamRequest createRequest(final long startNumber, final long endNumber) {
        return SubscribeStreamRequest.newBuilder()
                .startBlockNumber(startNumber)
                .endBlockNumber(endNumber)
                .build();
    }

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
