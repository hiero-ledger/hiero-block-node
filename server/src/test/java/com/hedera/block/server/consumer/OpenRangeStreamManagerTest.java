// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.server.consumer;

import static com.hedera.block.server.consumer.OpenRangeStreamManager.State.CUE_LIVE_STREAMING;
import static com.hedera.block.server.consumer.OpenRangeStreamManager.State.HISTORIC_STREAMING;
import static com.hedera.block.server.consumer.OpenRangeStreamManager.State.INIT_HISTORIC;
import static com.hedera.block.server.consumer.OpenRangeStreamManager.State.INIT_LIVE;
import static com.hedera.block.server.consumer.OpenRangeStreamManager.State.LIVE_STREAMING;
import static com.hedera.block.server.metrics.BlockNodeMetricTypes.Counter.ClosedRangeHistoricBlocksRetrieved;
import static com.hedera.block.server.metrics.BlockNodeMetricTypes.Counter.HistoricToLiveStreamTransitions;
import static com.hedera.block.server.metrics.BlockNodeMetricTypes.Counter.LiveBlockItemsConsumed;
import static com.hedera.block.server.metrics.BlockNodeMetricTypes.Counter.LiveToHistoricStreamTransitions;
import static com.hedera.block.server.metrics.BlockNodeMetricTypes.Gauge.CurrentBlockNumberOutbound;
import static com.hedera.block.server.util.PersistTestUtils.generateBlockItemsUnparsedForWithBlockNumber;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.hedera.block.server.block.BlockInfo;
import com.hedera.block.server.events.ObjectEvent;
import com.hedera.block.server.mediator.Poller;
import com.hedera.block.server.mediator.SubscriptionHandler;
import com.hedera.block.server.metrics.MetricsService;
import com.hedera.block.server.persistence.storage.read.BlockReader;
import com.hedera.block.server.service.ServiceStatus;
import com.hedera.hapi.block.BlockItemSetUnparsed;
import com.hedera.hapi.block.BlockItemUnparsed;
import com.hedera.hapi.block.BlockUnparsed;
import com.hedera.hapi.block.SubscribeStreamRequest;
import com.hedera.hapi.block.SubscribeStreamResponseUnparsed;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.grpc.Pipeline;
import com.swirlds.config.api.Configuration;
import com.swirlds.metrics.api.Counter;
import com.swirlds.metrics.api.LongGauge;
import java.time.Clock;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class OpenRangeStreamManagerTest {

    private static final long TEST_TIME = 1_719_427_664_950L;
    private static final long TIMEOUT_THRESHOLD_MILLIS = 100L;

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
    private Configuration configuration;

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

    @Mock
    private Counter liveToHistoricStreamTransitions;

    @Mock
    private Pipeline<? super SubscribeStreamResponseUnparsed> helidonConsumerObserver;

    @Mock
    private Poller<ObjectEvent<List<BlockItemUnparsed>>> liveBlockItemPoller;

    @Test
    public void testHappyPathLiveStream() throws Exception {

        final int numberOfBlocks = 15;

        final List<BlockUnparsed> blocks = buildBlocks(numberOfBlocks);

        // Prep the poller to mock the "live stream"
        final List<Optional<ObjectEvent<List<BlockItemUnparsed>>>> liveStreamResults = new ArrayList<>();
        for (int i = 0; i < numberOfBlocks; i++) {
            final BlockUnparsed blockUnparsed = blocks.get(i);
            ObjectEvent<List<BlockItemUnparsed>> objectEvent = new ObjectEvent<>();
            objectEvent.set(blockUnparsed.blockItems());
            liveStreamResults.add(Optional.of(objectEvent));
        }

        when(testClock.millis()).thenReturn(TEST_TIME, TEST_TIME + TIMEOUT_THRESHOLD_MILLIS);

        // Prep the poller to return the live stream results in order
        when(liveBlockItemPoller.poll())
                .thenReturn(
                        liveStreamResults.get(0),
                        liveStreamResults.subList(1, liveStreamResults.size()).toArray(new Optional[0]));

        // Prep the subscriptionHandler to return the poller when subscribing
        when(subscriptionHandler.subscribePoller(any())).thenReturn(liveBlockItemPoller);

        // Prep the HistoricDataPoller to return maxBlockItemBatchSize
        when(configuration.getConfigData(ConsumerConfig.class)).thenReturn(consumerConfig);
        when(consumerConfig.maxBlockItemBatchSize()).thenReturn(1000);
        when(consumerConfig.timeoutThresholdMillis()).thenReturn(1500);

        // Mock a subscribeStreamRequest indicating a live stream starting
        when(subscribeStreamRequest.startBlockNumber()).thenReturn(0L);
        when(metricsService.get(CurrentBlockNumberOutbound)).thenReturn(currentBlockNumberOutbound);
        when(metricsService.get(LiveBlockItemsConsumed)).thenReturn(liveBlockItemsConsumed);

        final HistoricDataPoller<List<BlockItemUnparsed>> historicDataPoller =
                new HistoricDataPollerImpl(blockReader, metricsService, configuration);

        final ConsumerStreamResponseObserver consumerStreamResponseObserver =
                new ConsumerStreamResponseObserver(testClock, helidonConsumerObserver, metricsService, configuration);

        final OpenRangeStreamManager streamManager = new OpenRangeStreamManager(
                subscribeStreamRequest,
                subscriptionHandler,
                historicDataPoller,
                consumerStreamResponseObserver,
                serviceStatus,
                metricsService,
                configuration);

        // INIT_LIVE - transition
        assertEquals(INIT_LIVE, streamManager.getState());
        streamManager.execute();
        assertEquals(LIVE_STREAMING, streamManager.getState());

        // Verify all blocks are streamed
        for (int i = 0; i < numberOfBlocks; i++) {
            streamManager.execute();
            assertEquals(LIVE_STREAMING, streamManager.getState());
        }

        verify(helidonConsumerObserver, times(numberOfBlocks)).onNext(any());
    }

    @Test
    @Disabled("@todo(751) - adapt this test to the new streaming consumer model")
    public void testHappyPathHistoricToLiveStreamTransition() throws Exception {

        final int numberOfBlocks = 16;
        final int liveStreamInitialBlock = 8;
        final List<BlockUnparsed> blocks = buildBlocks(numberOfBlocks);

        // Prep the blockReader to return 8 historical blocks
        // for the first part of the test when we are streaming historic
        for (int i = 1; i < 9; i++) {
            when(blockReader.read(i)).thenReturn(Optional.of(blocks.get(i)));
        }

        // Prep the blockReader to return 2 historical blocks
        // for the last part of the test when we transition back to historic
        for (int i = 14; i < numberOfBlocks; i++) {
            when(blockReader.read(i)).thenReturn(Optional.of(blocks.get(i)));
        }

        // Prep the serviceStatus to return the last acked block number
        // Block 5 is acked
        when(serviceStatus.getLatestAckedBlock()).thenReturn(new BlockInfo(5));

        // Prep the poller to mock the "live stream"
        final List<Optional<ObjectEvent<List<BlockItemUnparsed>>>> liveStreamResults = new ArrayList<>();
        for (int i = liveStreamInitialBlock; i < numberOfBlocks; i++) {
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

        // Prep the HistoricDataPoller to return maxBlockItemBatchSize
        // and the cueHistoricStreamingPaddingBlocks
        when(configuration.getConfigData(ConsumerConfig.class)).thenReturn(consumerConfig);
        when(consumerConfig.maxBlockItemBatchSize()).thenReturn(1000);
        when(consumerConfig.cueHistoricStreamingPaddingBlocks()).thenReturn(3);

        // Prep the metrics
        when(metricsService.get(ClosedRangeHistoricBlocksRetrieved)).thenReturn(closedRangeHistoricBlocksRetrieved);
        when(metricsService.get(HistoricToLiveStreamTransitions)).thenReturn(historicToLiveStreamTransitions);
        when(metricsService.get(CurrentBlockNumberOutbound)).thenReturn(currentBlockNumberOutbound);
        when(metricsService.get(LiveToHistoricStreamTransitions)).thenReturn(liveToHistoricStreamTransitions);
        when(metricsService.get(LiveBlockItemsConsumed)).thenReturn(liveBlockItemsConsumed);

        // Build the Open-Range Stream Manager
        final HistoricDataPoller<List<BlockItemUnparsed>> historicDataPoller =
                new HistoricDataPollerImpl(blockReader, metricsService, configuration);

        final ConsumerStreamResponseObserver consumerStreamResponseObserver = new ConsumerStreamResponseObserver(
                Clock.systemDefaultZone(), helidonConsumerObserver, metricsService, configuration);

        final OpenRangeStreamManager streamManager = new OpenRangeStreamManager(
                subscribeStreamRequest,
                subscriptionHandler,
                historicDataPoller,
                consumerStreamResponseObserver,
                serviceStatus,
                metricsService,
                configuration);

        // INIT_HISTORIC
        assertEquals(INIT_HISTORIC, streamManager.getState());
        streamManager.execute();
        assertEquals(HISTORIC_STREAMING, streamManager.getState());

        // STREAMING_HISTORIC - block 1
        streamManager.execute();
        assertEquals(HISTORIC_STREAMING, streamManager.getState());
        verify(helidonConsumerObserver, times(1))
                .onNext(buildResponse(blocks.get(1).blockItems()));

        // STREAMING_HISTORIC - block 2
        streamManager.execute();
        assertEquals(HISTORIC_STREAMING, streamManager.getState());
        verify(helidonConsumerObserver, times(1))
                .onNext(buildResponse(blocks.get(2).blockItems()));

        // Simulate block 6 being acked
        when(serviceStatus.getLatestAckedBlock()).thenReturn(new BlockInfo(6));

        // STREAMING_HISTORIC - block 3
        streamManager.execute();
        assertEquals(HISTORIC_STREAMING, streamManager.getState());
        verify(helidonConsumerObserver, times(1))
                .onNext(buildResponse(blocks.get(3).blockItems()));

        // STREAMING_HISTORIC - block 4
        streamManager.execute();
        assertEquals(HISTORIC_STREAMING, streamManager.getState());
        verify(helidonConsumerObserver, times(1))
                .onNext(buildResponse(blocks.get(4).blockItems()));

        // STREAMING_HISTORIC - block 5
        streamManager.execute();
        assertEquals(HISTORIC_STREAMING, streamManager.getState());
        verify(helidonConsumerObserver, times(1))
                .onNext(buildResponse(blocks.get(5).blockItems()));

        // Simulate block 7 being acked
        when(serviceStatus.getLatestAckedBlock()).thenReturn(new BlockInfo(7));

        // STREAMING_HISTORIC - block 6
        streamManager.execute();
        assertEquals(HISTORIC_STREAMING, streamManager.getState());
        verify(helidonConsumerObserver, times(1))
                .onNext(buildResponse(blocks.get(6).blockItems()));

        // State transition to INIT_LIVE
        streamManager.execute();
        assertEquals(INIT_LIVE, streamManager.getState());

        // State transition to CUE_LIVE_STREAMING
        streamManager.execute();
        assertEquals(CUE_LIVE_STREAMING, streamManager.getState());

        // State transition back to
        // STREAMING_HISTORIC after initializing
        // live stream
        streamManager.execute();
        assertEquals(HISTORIC_STREAMING, streamManager.getState());

        // STREAMING_HISTORIC - block 7 - Historic should be caught up to acked block
        streamManager.execute();
        assertEquals(HISTORIC_STREAMING, streamManager.getState());
        verify(helidonConsumerObserver, times(1))
                .onNext(buildResponse(blocks.get(7).blockItems()));

        // STREAMING_HISTORIC - block 8
        streamManager.execute();
        assertEquals(HISTORIC_STREAMING, streamManager.getState());
        verify(helidonConsumerObserver, times(1))
                .onNext(buildResponse(blocks.get(8).blockItems()));

        // STREAMING_LIVE - transition to the live stream
        streamManager.execute();
        assertEquals(LIVE_STREAMING, streamManager.getState());

        // STREAMING_LIVE - blocks 9, 10, 11, 12
        for (int i = 9; i < 13; i++) {
            streamManager.execute();
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
