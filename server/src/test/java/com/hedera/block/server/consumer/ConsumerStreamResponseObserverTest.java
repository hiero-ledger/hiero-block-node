// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.server.consumer;

import com.hedera.block.server.config.BlockNodeContext;
import com.hedera.block.server.events.ObjectEvent;
import com.hedera.block.server.mediator.StreamMediator;
import com.hedera.block.server.metrics.MetricsService;
import com.hedera.block.server.persistence.storage.read.BlockReader;
import com.hedera.block.server.service.ServiceStatus;
import com.hedera.block.server.util.TestConfigUtil;
import com.hedera.hapi.block.BlockItemUnparsed;
import com.hedera.hapi.block.BlockUnparsed;
import com.hedera.hapi.block.SubscribeStreamRequest;
import com.hedera.hapi.block.SubscribeStreamResponseUnparsed;
import com.hedera.pbj.runtime.grpc.Pipeline;
import com.swirlds.config.api.Configuration;
import java.io.IOException;
import java.time.InstantSource;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class ConsumerStreamResponseObserverTest {

    private final long TIMEOUT_THRESHOLD_MILLIS = 50L;
    private final long TEST_TIME = 1_719_427_664_950L;

    private static final int testTimeout = 1000;

    @Mock
    private StreamMediator<BlockItemUnparsed, List<BlockItemUnparsed>> streamMediator;

    @Mock
    private Pipeline<? super SubscribeStreamResponseUnparsed> responseStreamObserver;

    @Mock
    private ObjectEvent<List<BlockItemUnparsed>> objectEvent;

    @Mock
    private InstantSource testClock;

    @Mock
    private SubscribeStreamRequest subscribeStreamRequest;

    @Mock
    private ServiceStatus serviceStatus;

    @Mock
    private BlockReader<BlockUnparsed> blockReader;

    final MetricsService metricsService;

    final Configuration configuration;

    public ConsumerStreamResponseObserverTest() throws IOException {
        final BlockNodeContext testContext = TestConfigUtil.getTestBlockNodeContext(
                Map.of(TestConfigUtil.CONSUMER_TIMEOUT_THRESHOLD_KEY, String.valueOf(TIMEOUT_THRESHOLD_MILLIS)));
        this.metricsService = testContext.metricsService();
        this.configuration = testContext.configuration();
    }

    //    @Disabled("@todo(751) - adapt these tests to the new streaming consumer model")
    //    @Test
    //    public void testProducerTimeoutWithinWindow() throws Exception {
    //
    //        when(testClock.millis()).thenReturn(TEST_TIME, TEST_TIME + TIMEOUT_THRESHOLD_MILLIS);
    //
    //        // Mock live streaming
    //        when(subscribeStreamRequest.startBlockNumber()).thenReturn(0L);
    //        when(subscribeStreamRequest.endBlockNumber()).thenReturn(0L);
    //
    //        final var consumerBlockItemObserver = ConsumerStreamBuilder.build(
    //                testClock,
    //                subscribeStreamRequest,
    //                streamMediator,
    //                responseStreamObserver,
    //                blockReader,
    //                serviceStatus,
    //                metricsService,
    //                configuration);
    //
    //        final BlockHeader blockHeader = BlockHeader.newBuilder().number(1).build();
    //        final BlockItemUnparsed blockItem = BlockItemUnparsed.newBuilder()
    //                .blockHeader(BlockHeader.PROTOBUF.toBytes(blockHeader))
    //                .build();
    //
    //        List<BlockItemUnparsed> blockItems = List.of(blockItem);
    //        when(objectEvent.get()).thenReturn(blockItems);
    //
    //        consumerBlockItemObserver.onEvent(objectEvent, 0, true);
    //
    //        final BlockItemSetUnparsed blockItemSet =
    //                BlockItemSetUnparsed.newBuilder().blockItems(blockItems).build();
    //        final SubscribeStreamResponseUnparsed subscribeStreamResponse =
    // SubscribeStreamResponseUnparsed.newBuilder()
    //                .blockItems(blockItemSet)
    //                .build();
    //
    //        // verify the observer is called with the next BlockItem
    //        verify(responseStreamObserver, timeout(testTimeout)).onNext(subscribeStreamResponse);
    //
    //        // verify the mediator is NOT called to unsubscribe the observer
    //        verify(streamMediator, timeout(testTimeout).times(0)).unsubscribe(consumerBlockItemObserver);
    //    }

    //    @Test
    //    public void testProducerTimeoutOutsideWindow() throws Exception {
    //
    //        // Mock a clock with 2 different return values in response to anticipated
    //        // millis() calls. Here the second call will always be outside the timeout window.
    //        when(testClock.millis()).thenReturn(TEST_TIME, TEST_TIME + TIMEOUT_THRESHOLD_MILLIS + 1);
    //
    //        final var consumerBlockItemObserver = ConsumerStreamBuilder.build(
    //                testClock,
    //                streamMediator,
    //                responseStreamObserver,
    //                metricsService,
    //                configuration));
    //
    //        final List<BlockItemUnparsed> blockItems =
    //                List.of(BlockItemUnparsed.newBuilder().build());
    //        final ObjectEvent<List<BlockItemUnparsed>> objectEvent = new ObjectEvent<>();
    //        objectEvent.set(blockItems);
    //        consumerBlockItemObserver.onEvent(objectEvent, 0, true);
    //        verify(streamMediator, timeout(testTimeout)).unsubscribe(consumerBlockItemObserver);
    //    }

    //    @Test
    //    public void testConsumerNotToSendBeforeBlockHeader() throws Exception {
    //
    //        // Mock a clock with 2 different return values in response to anticipated
    //        // millis() calls. Here the second call will always be inside the timeout window.
    //        when(testClock.millis()).thenReturn(TEST_TIME, TEST_TIME + TIMEOUT_THRESHOLD_MILLIS);
    //
    //        final var consumerBlockItemObserver = ConsumerStreamBuilder.build(
    //                testClock,
    //                streamMediator,
    //                responseStreamObserver,
    //                metricsService,
    //                configuration);
    //
    //        // Send non-header BlockItems to validate that the observer does not send them
    //        for (int i = 1; i <= 10; i++) {
    //
    //            if (i % 2 == 0) {
    //                final Bytes eventHeader =
    //                        EventHeader.PROTOBUF.toBytes(EventHeader.newBuilder().build());
    //                final BlockItemUnparsed blockItem =
    //                        BlockItemUnparsed.newBuilder().eventHeader(eventHeader).build();
    //                lenient().when(objectEvent.get()).thenReturn(List.of(blockItem));
    //            } else {
    //                final Bytes blockProof = BlockProof.PROTOBUF.toBytes(
    //                        BlockProof.newBuilder().block(i).build());
    //                final BlockItemUnparsed blockItem =
    //                        BlockItemUnparsed.newBuilder().blockProof(blockProof).build();
    //                when(objectEvent.get()).thenReturn(List.of(blockItem));
    //            }
    //
    //            consumerBlockItemObserver.onEvent(objectEvent, 0, true);
    //        }
    //
    //        final BlockItemUnparsed blockItem = BlockItemUnparsed.newBuilder().build();
    //        final BlockItemSetUnparsed blockItemSet =
    //                BlockItemSetUnparsed.newBuilder().blockItems(blockItem).build();
    //        final SubscribeStreamResponseUnparsed subscribeStreamResponse =
    // SubscribeStreamResponseUnparsed.newBuilder()
    //                .blockItems(blockItemSet)
    //                .build();
    //
    //        // Confirm that the observer was called with the next BlockItem
    //        // since we never send a BlockItem with a Header to start the stream.
    //        verify(responseStreamObserver, timeout(testTimeout).times(0)).onNext(subscribeStreamResponse);
    //    }

    //    @Test
    //    public void testUncheckedIOExceptionException() throws Exception {
    //        final BlockHeader blockHeader = BlockHeader.newBuilder().number(1).build();
    //        final BlockItemUnparsed blockItem = BlockItemUnparsed.newBuilder()
    //                .blockHeader(BlockHeader.PROTOBUF.toBytes(blockHeader))
    //                .build();
    //
    //        when(objectEvent.get()).thenReturn(List.of(blockItem));
    //
    //        final BlockItemSetUnparsed blockItemSet =
    //                BlockItemSetUnparsed.newBuilder().blockItems(blockItem).build();
    //        final SubscribeStreamResponseUnparsed subscribeStreamResponse =
    // SubscribeStreamResponseUnparsed.newBuilder()
    //                .blockItems(blockItemSet)
    //                .build();
    //        doThrow(UncheckedIOException.class).when(responseStreamObserver).onNext(subscribeStreamResponse);
    //
    //        final var consumerBlockItemObserver = ConsumerStreamBuilder.build(
    //                testClock,
    //                streamMediator,
    //                responseStreamObserver,
    //                metricsService,
    //                configuration);
    //
    //        // This call will throw an exception but, because of the async
    //        // service executor, the exception will not get caught until the
    //        // next call.
    //        consumerBlockItemObserver.onEvent(objectEvent, 0, true);
    //        Thread.sleep(testTimeout);
    //
    //        // This second call will throw the exception.
    //        consumerBlockItemObserver.onEvent(objectEvent, 0, true);
    //
    //        verify(streamMediator, timeout(testTimeout).times(1)).unsubscribe(any());
    //    }

    //    @Test
    //    public void testRuntimeException() throws Exception {
    //        final BlockHeader blockHeader = BlockHeader.newBuilder().number(1).build();
    //        final BlockItemUnparsed blockItem = BlockItemUnparsed.newBuilder()
    //                .blockHeader(BlockHeader.PROTOBUF.toBytes(blockHeader))
    //                .build();
    //        final BlockItemSetUnparsed blockItemSet =
    //                BlockItemSetUnparsed.newBuilder().blockItems(blockItem).build();
    //        final SubscribeStreamResponseUnparsed subscribeStreamResponse =
    // SubscribeStreamResponseUnparsed.newBuilder()
    //                .blockItems(blockItemSet)
    //                .build();
    //        when(objectEvent.get()).thenReturn(List.of(blockItem));
    //        doThrow(RuntimeException.class).when(responseStreamObserver).onNext(subscribeStreamResponse);
    //
    //        final var consumerBlockItemObserver = ConsumerStreamBuilder.build(
    //                testClock,
    //                streamMediator,
    //                responseStreamObserver,
    //                metricsService,
    //                configuration);
    //
    //        // This call will throw an exception but, because of the async
    //        // service executor, the exception will not get caught until the
    //        // next call.
    //        consumerBlockItemObserver.onEvent(objectEvent, 0, true);
    //        Thread.sleep(testTimeout);
    //
    //        // This second call will throw the exception.
    //        consumerBlockItemObserver.onEvent(objectEvent, 0, true);
    //
    //        verify(streamMediator, timeout(testTimeout).times(1)).unsubscribe(any());
    //    }
}
