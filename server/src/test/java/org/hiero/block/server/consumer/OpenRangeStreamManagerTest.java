// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.consumer;

import static org.hiero.block.server.consumer.OpenRangeStreamManager.State.CUE_LIVE_STREAMING;
import static org.hiero.block.server.consumer.OpenRangeStreamManager.State.HISTORIC_STREAMING;
import static org.hiero.block.server.consumer.OpenRangeStreamManager.State.INIT_HISTORIC;
import static org.hiero.block.server.consumer.OpenRangeStreamManager.State.INIT_LIVE;
import static org.hiero.block.server.consumer.OpenRangeStreamManager.State.LIVE_STREAMING;
import static org.hiero.block.server.metrics.BlockNodeMetricTypes.Counter.ClosedRangeHistoricBlocksRetrieved;
import static org.hiero.block.server.metrics.BlockNodeMetricTypes.Counter.HistoricToLiveStreamTransitions;
import static org.hiero.block.server.metrics.BlockNodeMetricTypes.Counter.LiveBlockItemsConsumed;
import static org.hiero.block.server.metrics.BlockNodeMetricTypes.Gauge.CurrentBlockNumberOutbound;
import static org.hiero.block.server.util.PersistTestUtils.generateBlockItemsUnparsedForWithBlockNumber;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.hedera.hapi.block.BlockItemSetUnparsed;
import com.hedera.hapi.block.BlockItemUnparsed;
import com.hedera.hapi.block.BlockUnparsed;
import com.hedera.hapi.block.SubscribeStreamRequest;
import com.hedera.hapi.block.SubscribeStreamResponseUnparsed;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.grpc.Pipeline;
import com.swirlds.metrics.api.Counter;
import com.swirlds.metrics.api.LongGauge;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.hiero.block.server.block.BlockInfo;
import org.hiero.block.server.events.ObjectEvent;
import org.hiero.block.server.mediator.Poller;
import org.hiero.block.server.mediator.SubscriptionHandler;
import org.hiero.block.server.metrics.MetricsService;
import org.hiero.block.server.persistence.storage.read.BlockReader;
import org.hiero.block.server.service.ServiceStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class OpenRangeStreamManagerTest {

    private static final int TIMEOUT_THRESHOLD_MILLIS = 100;
    private static final long TEST_TIME = 1_719_427_664_950L;

    @Mock
    private InstantSource testClock;

    @Mock
    private SubscribeStreamRequest subscribeStreamRequest;

    @Mock
    private SubscriptionHandler<List<BlockItemUnparsed>> subscriptionHandler;

    @Mock
    private BlockReader<BlockUnparsed> blockReader;

    @Mock
    private ServiceStatus serviceStatus;

    @Mock
    private MetricsService metricsService;

    @Mock
    private ConsumerConfig consumerConfig;

    @Mock
    private Counter closedRangeHistoricBlocksRetrieved;

    @Mock
    private Counter historicToLiveStreamTransitions;

    @Mock
    private LongGauge currentBlockNumberOutbound;

    @Mock
    private Counter liveBlockItemsConsumed;

    //    @Mock
    //    private Counter liveToHistoricStreamTransitions;

    @Mock
    private Pipeline<? super SubscribeStreamResponseUnparsed> helidonConsumerObserver;

    @Mock
    private Poller<ObjectEvent<List<BlockItemUnparsed>>> liveBlockItemPoller;

    @BeforeEach
    public void setUp() {
        // Set up the ConsumerConfig
        when(consumerConfig.maxBlockItemBatchSize()).thenReturn(1000);
        when(consumerConfig.timeoutThresholdMillis()).thenReturn(TIMEOUT_THRESHOLD_MILLIS);
    }

    @Test
    public void testHappyPathLiveStream() throws Exception {

        // Set up the test clock within the time range
        when(testClock.millis()).thenReturn(TEST_TIME, TEST_TIME + 1);

        final int NUM_OF_BLOCKS = 15;

        final List<BlockUnparsed> blocks = buildBlocks(NUM_OF_BLOCKS);

        // Prep the poller to mock the "live stream"
        final List<Optional<ObjectEvent<List<BlockItemUnparsed>>>> liveStreamResults = new ArrayList<>();
        for (int i = 0; i < NUM_OF_BLOCKS; i++) {
            final BlockUnparsed blockUnparsed = blocks.get(i);
            ObjectEvent<List<BlockItemUnparsed>> objectEvent = new ObjectEvent<>();
            objectEvent.set(blockUnparsed.blockItems());
            liveStreamResults.add(Optional.of(objectEvent));
        }

        // Prep the poller to return the live stream results in order
        when(liveBlockItemPoller.poll())
                .thenReturn(
                        liveStreamResults.get(0),
                        liveStreamResults.subList(1, liveStreamResults.size()).toArray(new Optional[0]));

        // Prep the subscriptionHandler to return the poller when subscribing
        when(subscriptionHandler.subscribePoller(any())).thenReturn(liveBlockItemPoller);

        // Mock a subscribeStreamRequest indicating a live stream starting
        when(subscribeStreamRequest.startBlockNumber()).thenReturn(0L);
        when(metricsService.get(CurrentBlockNumberOutbound)).thenReturn(currentBlockNumberOutbound);
        when(metricsService.get(LiveBlockItemsConsumed)).thenReturn(liveBlockItemsConsumed);

        final OpenRangeStreamManager streamManager = ConsumerStreamBuilder.buildStreamManager(
                testClock,
                subscribeStreamRequest,
                subscriptionHandler,
                helidonConsumerObserver,
                blockReader,
                serviceStatus,
                metricsService,
                consumerConfig);

        // INIT_LIVE - transition
        assertEquals(INIT_LIVE, streamManager.getState());
        assertTrue(streamManager.execute());
        assertEquals(LIVE_STREAMING, streamManager.getState());

        // Verify all blocks are streamed
        for (int i = 0; i < NUM_OF_BLOCKS; i++) {
            streamManager.execute();
            assertEquals(LIVE_STREAMING, streamManager.getState());
        }

        verify(helidonConsumerObserver, times(NUM_OF_BLOCKS)).onNext(any());
    }

    @Test
    public void testHappyPathHistoricToLiveStreamTransition() throws Exception {

        // Set up the test clock within the time range
        when(testClock.millis()).thenReturn(TEST_TIME, TEST_TIME + 1);

        final int NUM_OF_BLOCKS = 16;
        final int liveStreamInitialBlock = 8;
        final List<BlockUnparsed> blocks = buildBlocks(NUM_OF_BLOCKS);

        // Prep the blockReader to return 8 historical blocks
        // for the first part of the test when we are streaming historic
        for (int i = 1; i < 9; i++) {
            when(blockReader.read(i)).thenReturn(Optional.of(blocks.get(i)));
        }

        // Prep the blockReader to return 2 historical blocks
        // for the last part of the test when we transition back to historic
        //        for (int i = 14; i < NUM_OF_BLOCKS; i++) {
        //            when(blockReader.read(i)).thenReturn(Optional.of(blocks.get(i)));
        //        }

        // Prep the serviceStatus to return the last acked block number
        // Block 5 is acked
        when(serviceStatus.getLatestAckedBlock()).thenReturn(new BlockInfo(5));

        // Prep the poller to mock the "live stream"
        final List<Optional<ObjectEvent<List<BlockItemUnparsed>>>> liveStreamResults = new ArrayList<>();
        for (int i = liveStreamInitialBlock; i < NUM_OF_BLOCKS; i++) {
            final BlockUnparsed blockUnparsed = blocks.get(i);
            ObjectEvent<List<BlockItemUnparsed>> objectEvent = new ObjectEvent<>();
            objectEvent.set(blockUnparsed.blockItems());
            liveStreamResults.add(Optional.of(objectEvent));
        }

        // Prep the poller to return the live stream results in order
        when(liveBlockItemPoller.poll())
                .thenReturn(
                        liveStreamResults.get(0),
                        liveStreamResults.subList(1, liveStreamResults.size()).toArray(new Optional[0]));

        // Prep the subscriptionHandler to return the poller when subscribing
        when(subscriptionHandler.subscribePoller(any())).thenReturn(liveBlockItemPoller);

        // Mock a subscribeStreamRequest indicating an historic stream starting
        // from block 1 and to live stream once it catches up.
        when(subscribeStreamRequest.startBlockNumber()).thenReturn(1L);

        //        when(consumerConfig.cueHistoricStreamingPaddingBlocks()).thenReturn(3);

        // Prep the metrics
        when(metricsService.get(ClosedRangeHistoricBlocksRetrieved)).thenReturn(closedRangeHistoricBlocksRetrieved);
        when(metricsService.get(HistoricToLiveStreamTransitions)).thenReturn(historicToLiveStreamTransitions);
        when(metricsService.get(CurrentBlockNumberOutbound)).thenReturn(currentBlockNumberOutbound);
        //        when(metricsService.get(LiveToHistoricStreamTransitions)).thenReturn(liveToHistoricStreamTransitions);
        when(metricsService.get(LiveBlockItemsConsumed)).thenReturn(liveBlockItemsConsumed);

        final OpenRangeStreamManager streamManager = ConsumerStreamBuilder.buildStreamManager(
                testClock,
                subscribeStreamRequest,
                subscriptionHandler,
                helidonConsumerObserver,
                blockReader,
                serviceStatus,
                metricsService,
                consumerConfig);

        // INIT_HISTORIC
        assertEquals(INIT_HISTORIC, streamManager.getState());
        assertTrue(streamManager.execute());
        assertEquals(HISTORIC_STREAMING, streamManager.getState());

        // STREAMING_HISTORIC - block 1
        assertTrue(streamManager.execute());
        assertEquals(HISTORIC_STREAMING, streamManager.getState());
        verify(helidonConsumerObserver, times(1))
                .onNext(buildResponse(blocks.get(1).blockItems()));

        // STREAMING_HISTORIC - block 2
        assertTrue(streamManager.execute());
        assertEquals(HISTORIC_STREAMING, streamManager.getState());
        verify(helidonConsumerObserver, times(1))
                .onNext(buildResponse(blocks.get(2).blockItems()));

        // Simulate block 6 being acked
        when(serviceStatus.getLatestAckedBlock()).thenReturn(new BlockInfo(6));

        // STREAMING_HISTORIC - block 3
        assertTrue(streamManager.execute());
        assertEquals(HISTORIC_STREAMING, streamManager.getState());
        verify(helidonConsumerObserver, times(1))
                .onNext(buildResponse(blocks.get(3).blockItems()));

        // STREAMING_HISTORIC - block 4
        assertTrue(streamManager.execute());
        assertEquals(HISTORIC_STREAMING, streamManager.getState());
        verify(helidonConsumerObserver, times(1))
                .onNext(buildResponse(blocks.get(4).blockItems()));

        // STREAMING_HISTORIC - block 5
        assertTrue(streamManager.execute());
        assertEquals(HISTORIC_STREAMING, streamManager.getState());
        verify(helidonConsumerObserver, times(1))
                .onNext(buildResponse(blocks.get(5).blockItems()));

        // Simulate block 7 being acked
        when(serviceStatus.getLatestAckedBlock()).thenReturn(new BlockInfo(7));

        // STREAMING_HISTORIC - block 6
        assertTrue(streamManager.execute());
        assertEquals(HISTORIC_STREAMING, streamManager.getState());
        verify(helidonConsumerObserver, times(1))
                .onNext(buildResponse(blocks.get(6).blockItems()));

        // State transition to INIT_LIVE
        assertTrue(streamManager.execute());
        assertEquals(INIT_LIVE, streamManager.getState());

        // State transition to CUE_LIVE_STREAMING
        assertTrue(streamManager.execute());
        assertEquals(CUE_LIVE_STREAMING, streamManager.getState());

        // State transition back to
        // STREAMING_HISTORIC after initializing
        // live stream
        assertTrue(streamManager.execute());
        assertEquals(HISTORIC_STREAMING, streamManager.getState());

        // STREAMING_HISTORIC - block 7 - Historic should be caught up to acked block
        assertTrue(streamManager.execute());
        assertEquals(HISTORIC_STREAMING, streamManager.getState());
        verify(helidonConsumerObserver, times(1))
                .onNext(buildResponse(blocks.get(7).blockItems()));

        // STREAMING_HISTORIC - block 8
        assertTrue(streamManager.execute());
        assertEquals(HISTORIC_STREAMING, streamManager.getState());
        verify(helidonConsumerObserver, times(1))
                .onNext(buildResponse(blocks.get(8).blockItems()));

        // STREAMING_LIVE - transition to the live stream
        assertTrue(streamManager.execute());
        assertEquals(LIVE_STREAMING, streamManager.getState());

        // STREAMING_LIVE - blocks 9, 10, 11, 12
        for (int i = 9; i < 13; i++) {
            assertTrue(streamManager.execute());
            assertEquals(LIVE_STREAMING, streamManager.getState());
            verify(helidonConsumerObserver, times(1))
                    .onNext(buildResponse(blocks.get(i).blockItems()));
        }

        // @todo(#750) - Re-enable this test once the slow consumer logic is implemented
        // Simulate a slow consumer - flag that the threshold has been exceeded
        //        when(liveBlockItemPoller.exceedsThreshold()).thenReturn(true);

        //        // STREAMING_LIVE - transition to DRAIN_LIVE_STREAMING
        //        streamManager.execute();
        //        assertEquals(DRAIN_LIVE_STREAMING, streamManager.getState());
        //
        //        // DRAIN_LIVE_STREAMING - blocks 13, transition back to INIT_HISTORIC
        //        streamManager.execute();
        //        assertEquals(INIT_HISTORIC, streamManager.getState());
        //        verify(helidonConsumerObserver, times(1))
        //                .onNext(buildResponse(blocks.get(13).blockItems()));
        //
        //        for (int i = 8; i < 15 + 3; i++) {
        //            // Verify the streamManager stays on the
        //            // CUE_HISTORIC_STREAMING state until the
        //            // blocks acked exceed the next live block number
        //            // + the default padding of 3
        //            streamManager.execute();
        //            assertEquals(CUE_HISTORIC_STREAMING, streamManager.getState());
        //
        //            // Simulate blocks being acked
        //            when(serviceStatus.getLatestAckedBlock()).thenReturn(new BlockInfo(i));
        //        }
        //
        //        // Simulate block 18 being acked
        //        when(serviceStatus.getLatestAckedBlock()).thenReturn(new BlockInfo(18));
        //
        //        // Blocks up to 18 should be acked now - transition to HISTORIC_STREAMING
        //        streamManager.execute();
        //        assertEquals(HISTORIC_STREAMING, streamManager.getState());
        //
        //        // STREAMING_HISTORIC - block 14
        //        streamManager.execute();
        //        assertEquals(HISTORIC_STREAMING, streamManager.getState());
        //        verify(helidonConsumerObserver, times(1))
        //                .onNext(buildResponse(blocks.get(14).blockItems()));
        //
        //        // STREAMING_HISTORIC - block 15
        //        streamManager.execute();
        //        assertEquals(HISTORIC_STREAMING, streamManager.getState());
        //        verify(helidonConsumerObserver, times(1))
        //                .onNext(buildResponse(blocks.get(15).blockItems()));
    }

    @Test
    public void testSubscribeAndUnsubscribeHandlingWithTimeout() {

        // Prep the subscriptionHandler to return the poller when subscribing
        when(subscriptionHandler.subscribePoller(any())).thenReturn(liveBlockItemPoller);

        // Mock a subscribeStreamRequest indicating a live-streaming request
        when(subscribeStreamRequest.startBlockNumber()).thenReturn(0L);

        // Set up the test clock with a timeout
        when(testClock.millis())
                .thenReturn(TEST_TIME, TEST_TIME + TIMEOUT_THRESHOLD_MILLIS, TEST_TIME + TIMEOUT_THRESHOLD_MILLIS + 1);

        final OpenRangeStreamManager streamManager = ConsumerStreamBuilder.buildStreamManager(
                testClock,
                subscribeStreamRequest,
                subscriptionHandler,
                helidonConsumerObserver,
                blockReader,
                serviceStatus,
                metricsService,
                consumerConfig);

        // INIT_LIVE - transition
        assertEquals(INIT_LIVE, streamManager.getState());

        // First execute() should subscribe to the poller
        assertTrue(streamManager.execute());
        assertEquals(LIVE_STREAMING, streamManager.getState());

        // Second execute() should fail because of the timeout
        assertFalse(streamManager.execute());

        // Verify the subscriptionHandler is called to subscribe
        verify(subscriptionHandler, times(1)).subscribePoller(any());

        // Verify the subscriptionHandler is called to unsubscribe
        verify(subscriptionHandler, times(1)).unsubscribePoller(any());
    }

    private static long parseBlockNumber(List<BlockItemUnparsed> blockItems) throws ParseException {
        if (blockItems.getFirst().hasBlockHeader()) {
            return BlockHeader.PROTOBUF
                    .parse(blockItems.getFirst().blockHeader())
                    .number();
        }

        return -1L;
    }

    private static List<BlockUnparsed> buildBlocks(int numOfBlocks) {

        List<BlockUnparsed> blocks = new ArrayList<>();
        for (int i = 0; i < numOfBlocks; i++) {
            final List<BlockItemUnparsed> blockItems = generateBlockItemsUnparsedForWithBlockNumber(i);
            blocks.add(new BlockUnparsed(blockItems));
        }

        return blocks;
    }

    private static SubscribeStreamResponseUnparsed buildResponse(List<BlockItemUnparsed> blockItems) {
        return SubscribeStreamResponseUnparsed.newBuilder()
                .blockItems(
                        BlockItemSetUnparsed.newBuilder().blockItems(blockItems).build())
                .build();
    }
}
