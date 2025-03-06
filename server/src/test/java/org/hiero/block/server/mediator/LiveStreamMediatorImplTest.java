// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.mediator;

import static org.hiero.block.server.metrics.BlockNodeMetricTypes.Counter.LiveBlockItems;
import static org.hiero.block.server.util.PersistTestUtils.PERSISTENCE_STORAGE_ARCHIVE_ROOT_PATH_KEY;
import static org.hiero.block.server.util.PersistTestUtils.PERSISTENCE_STORAGE_LIVE_ROOT_PATH_KEY;
import static org.hiero.block.server.util.TestConfigUtil.CONSUMER_TIMEOUT_THRESHOLD_KEY;
import static org.hiero.block.server.util.TestConfigUtil.MEDIATOR_RING_BUFFER_SIZE_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.hedera.hapi.block.BlockItemSetUnparsed;
import com.hedera.hapi.block.BlockItemUnparsed;
import com.hedera.hapi.block.BlockUnparsed;
import com.hedera.hapi.block.SubscribeStreamRequest;
import com.hedera.hapi.block.SubscribeStreamResponseUnparsed;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.pbj.runtime.grpc.Pipeline;
import com.swirlds.config.api.Configuration;
import java.io.IOException;
import java.nio.file.Path;
import java.time.InstantSource;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import org.hiero.block.server.ack.AckHandler;
import org.hiero.block.server.config.BlockNodeContext;
import org.hiero.block.server.consumer.ConsumerStreamBuilder;
import org.hiero.block.server.consumer.StreamManager;
import org.hiero.block.server.events.BlockNodeEventHandler;
import org.hiero.block.server.events.ObjectEvent;
import org.hiero.block.server.metrics.MetricsService;
import org.hiero.block.server.notifier.Notifier;
import org.hiero.block.server.persistence.StreamPersistenceHandlerImpl;
import org.hiero.block.server.persistence.storage.PersistenceStorageConfig;
import org.hiero.block.server.persistence.storage.archive.LocalBlockArchiver;
import org.hiero.block.server.persistence.storage.read.BlockReader;
import org.hiero.block.server.persistence.storage.write.AsyncBlockWriterFactory;
import org.hiero.block.server.persistence.storage.write.AsyncNoOpWriterFactory;
import org.hiero.block.server.service.ServiceStatus;
import org.hiero.block.server.service.ServiceStatusImpl;
import org.hiero.block.server.util.PersistTestUtils;
import org.hiero.block.server.util.TestConfigUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class LiveStreamMediatorImplTest {
    private static final long TIMEOUT_THRESHOLD_MILLIS = 100L;
    private static final long TEST_TIME = 1_719_427_664_950L;
    private static final int TEST_TIMEOUT = 1000;

    @Mock
    private BlockNodeEventHandler<ObjectEvent<List<BlockItemUnparsed>>> observer1;

    @Mock
    private BlockNodeEventHandler<ObjectEvent<List<BlockItemUnparsed>>> observer2;

    @Mock
    private BlockNodeEventHandler<ObjectEvent<List<BlockItemUnparsed>>> observer3;

    @Mock
    private Notifier notifier;

    @Mock
    private Pipeline<? super SubscribeStreamResponseUnparsed> helidonResponseStreamObserver1;

    @Mock
    private Pipeline<? super SubscribeStreamResponseUnparsed> helidonResponseStreamObserver2;

    @Mock
    private Pipeline<? super SubscribeStreamResponseUnparsed> helidonResponseStreamObserver3;

    @Mock
    private InstantSource testClock;

    @Mock
    private AckHandler ackHandlerMock;

    @Mock
    private AsyncBlockWriterFactory asyncBlockWriterFactoryMock;

    @Mock
    private Executor executorMock;

    @Mock
    private LocalBlockArchiver archiverMock;

    @Mock
    private SubscribeStreamRequest subscribeStreamRequest;

    @Mock
    private BlockReader<BlockUnparsed> blockReader;

    @TempDir
    private Path testTempDir;

    private PersistenceStorageConfig persistenceStorageConfig;
    private BlockNodeContext testContext;
    private MetricsService metricsService;
    private Configuration configuration;

    @BeforeEach
    void setup() throws IOException {
        final Map<String, String> properties = new HashMap<>();
        properties.put(PERSISTENCE_STORAGE_LIVE_ROOT_PATH_KEY, testTempDir.toString());
        properties.put(PERSISTENCE_STORAGE_ARCHIVE_ROOT_PATH_KEY, testTempDir.toString());
        properties.put(CONSUMER_TIMEOUT_THRESHOLD_KEY, String.valueOf(TIMEOUT_THRESHOLD_MILLIS));
        properties.put(MEDIATOR_RING_BUFFER_SIZE_KEY, String.valueOf(1024));

        testContext = TestConfigUtil.getTestBlockNodeContext(properties);
        metricsService = testContext.metricsService();
        configuration = testContext.configuration();
        persistenceStorageConfig = testContext.configuration().getConfigData(PersistenceStorageConfig.class);
    }

    @Test
    void testUnsubscribeEach() throws InterruptedException, IOException {
        final BlockNodeContext blockNodeContext = TestConfigUtil.getTestBlockNodeContext();
        final LiveStreamMediatorBuilder streamMediatorBuilder =
                LiveStreamMediatorBuilder.newBuilder(blockNodeContext, new ServiceStatusImpl(blockNodeContext));
        final LiveStreamMediator streamMediator = streamMediatorBuilder.build();

        // Set up the subscribers
        streamMediator.subscribe(observer1);
        streamMediator.subscribe(observer2);
        streamMediator.subscribe(observer3);

        assertTrue(streamMediator.isSubscribed(observer1), "Expected the mediator to have observer1 subscribed");
        assertTrue(streamMediator.isSubscribed(observer2), "Expected the mediator to have observer2 subscribed");
        assertTrue(streamMediator.isSubscribed(observer3), "Expected the mediator to have observer3 subscribed");

        Thread.sleep(100);

        streamMediator.unsubscribe(observer1);
        assertFalse(streamMediator.isSubscribed(observer1), "Expected the mediator to have unsubscribed observer1");

        streamMediator.unsubscribe(observer2);
        assertFalse(streamMediator.isSubscribed(observer2), "Expected the mediator to have unsubscribed observer2");

        streamMediator.unsubscribe(observer3);
        assertFalse(streamMediator.isSubscribed(observer3), "Expected the mediator to have unsubscribed observer3");

        // Confirm the counter was never incremented
        assertEquals(0, blockNodeContext.metricsService().get(LiveBlockItems).get());
    }

    @Test
    void testMediatorPersistenceWithoutSubscribers() throws IOException {
        final BlockNodeContext blockNodeContext = TestConfigUtil.getTestBlockNodeContext();
        final ServiceStatus serviceStatus = new ServiceStatusImpl(blockNodeContext);
        final LiveStreamMediator streamMediator = LiveStreamMediatorBuilder.newBuilder(blockNodeContext, serviceStatus)
                .build();

        final List<BlockItemUnparsed> blockItemUnparsed =
                PersistTestUtils.generateBlockItemsUnparsedForWithBlockNumber(1);

        // register the stream validator
        final AsyncNoOpWriterFactory writerFactory =
                new AsyncNoOpWriterFactory(ackHandlerMock, blockNodeContext.metricsService());
        final StreamPersistenceHandlerImpl handler = new StreamPersistenceHandlerImpl(
                streamMediator,
                notifier,
                blockNodeContext,
                serviceStatus,
                ackHandlerMock,
                writerFactory,
                executorMock,
                archiverMock,
                persistenceStorageConfig);
        streamMediator.subscribe(handler);

        // Acting as a producer, notify the mediator of a new block
        streamMediator.publish(blockItemUnparsed);

        // Verify the counter was incremented
        assertEquals(10, blockNodeContext.metricsService().get(LiveBlockItems).get());

        // @todo(642) we need to employ the same technique here to inject a writer that will ensure
        // the tasks are complete before we can verify the metrics for blocks persisted
        // the test will pass without flaking if we do that
        // assertEquals(1, blockNodeContext.metricsService().get(BlocksPersisted).get());
    }

    @Test
    void testMediatorPublishEventToSubscribers() throws IOException {
        final BlockNodeContext blockNodeContext = TestConfigUtil.getTestBlockNodeContext();
        final ServiceStatus serviceStatus = new ServiceStatusImpl(blockNodeContext);
        final LiveStreamMediator streamMediator = LiveStreamMediatorBuilder.newBuilder(blockNodeContext, serviceStatus)
                .build();

        when(testClock.millis()).thenReturn(TEST_TIME, TEST_TIME + TIMEOUT_THRESHOLD_MILLIS);

        // Mock live streaming
        when(subscribeStreamRequest.startBlockNumber()).thenReturn(0L);

        final StreamManager streamManager1 = ConsumerStreamBuilder.buildStreamManager(
                testClock,
                subscribeStreamRequest,
                streamMediator,
                helidonResponseStreamObserver1,
                blockReader,
                serviceStatus,
                metricsService,
                configuration);

        final StreamManager streamManager2 = ConsumerStreamBuilder.buildStreamManager(
                testClock,
                subscribeStreamRequest,
                streamMediator,
                helidonResponseStreamObserver2,
                blockReader,
                serviceStatus,
                metricsService,
                configuration);

        final StreamManager streamManager3 = ConsumerStreamBuilder.buildStreamManager(
                testClock,
                subscribeStreamRequest,
                streamMediator,
                helidonResponseStreamObserver3,
                blockReader,
                serviceStatus,
                metricsService,
                configuration);

        assertFalse(streamMediator.isSubscribed(streamManager1), "StreamManager1 should not be subscribed yet");
        assertFalse(streamMediator.isSubscribed(streamManager2), "StreamManager2 should not be subscribed yet");
        assertFalse(streamMediator.isSubscribed(streamManager3), "StreamManager3 should not be subscribed yet");

        // Simulate the first request into
        // the managers to set up the stream
        // and their subscriptions to the
        // live stream
        streamManager1.execute();
        streamManager2.execute();
        streamManager3.execute();

        // Retest - they should now be subscribed
        assertTrue(streamMediator.isSubscribed(streamManager1), "StreamManager1 should now be subscribed");
        assertTrue(streamMediator.isSubscribed(streamManager2), "StreamManager2 should now be subscribed");
        assertTrue(streamMediator.isSubscribed(streamManager3), "StreamManager3 should now be subscribed");

        final BlockItemUnparsed blockItem = BlockItemUnparsed.newBuilder()
                .blockHeader(BlockHeader.PROTOBUF.toBytes(
                        BlockHeader.newBuilder().number(1).build()))
                .build();
        final SubscribeStreamResponseUnparsed subscribeStreamResponse = SubscribeStreamResponseUnparsed.newBuilder()
                .blockItems(
                        BlockItemSetUnparsed.newBuilder().blockItems(blockItem).build())
                .build();

        // register the stream validator
        final StreamPersistenceHandlerImpl handler = new StreamPersistenceHandlerImpl(
                streamMediator,
                notifier,
                blockNodeContext,
                serviceStatus,
                ackHandlerMock,
                asyncBlockWriterFactoryMock,
                executorMock,
                archiverMock,
                persistenceStorageConfig);
        streamMediator.subscribe(handler);

        // Acting as a producer, notify the mediator of a new block
        streamMediator.publish(List.of(blockItem));
        assertEquals(1, blockNodeContext.metricsService().get(LiveBlockItems).get());

        // Simulate the runner looping
        // by calling execute for each
        streamManager1.execute();
        streamManager2.execute();
        streamManager3.execute();

        // Confirm each subscriber was notified of the new block
        verify(helidonResponseStreamObserver1, timeout(TEST_TIMEOUT).times(1)).onNext(subscribeStreamResponse);
        verify(helidonResponseStreamObserver2, timeout(TEST_TIMEOUT).times(1)).onNext(subscribeStreamResponse);
        verify(helidonResponseStreamObserver3, timeout(TEST_TIMEOUT).times(1)).onNext(subscribeStreamResponse);

        // Confirm Writer created
        verify(asyncBlockWriterFactoryMock, timeout(TEST_TIMEOUT).times(1)).create(1L);
    }

    @Disabled("@todo(751) - adapt these tests to the new streaming consumer model")
    @Test
    void testSubAndUnsubHandling() throws IOException {
        //        final BlockNodeContext blockNodeContext = TestConfigUtil.getTestBlockNodeContext();
        //        final ServiceStatus serviceStatus = new ServiceStatusImpl(blockNodeContext);
        //        final LiveStreamMediator streamMediator = LiveStreamMediatorBuilder.newBuilder(blockNodeContext,
        // serviceStatus)
        //                .build();
        //
        //        when(testClock.millis()).thenReturn(TEST_TIME, TEST_TIME + TIMEOUT_THRESHOLD_MILLIS);
        //
        //        final var concreteObserver1 = ConsumerStreamBuilder.build(
        //                completionService,
        //                testClock,
        //                streamMediator,
        //                helidonSubscribeStreamObserver1,
        //                testContext.metricsService(),
        //                testContext.configuration());
        //        final var concreteObserver2 = ConsumerStreamBuilder.build(
        //                completionService,
        //                testClock,
        //                streamMediator,
        //                helidonSubscribeStreamObserver2,
        //                testContext.metricsService(),
        //                testContext.configuration());
        //        final var concreteObserver3 = ConsumerStreamBuilder.build(
        //                completionService,
        //                testClock,
        //                streamMediator,
        //                helidonSubscribeStreamObserver3,
        //                testContext.metricsService(),
        //                testContext.configuration());
        //
        //        // Set up the subscribers
        //        streamMediator.subscribe(concreteObserver1);
        //        streamMediator.subscribe(concreteObserver2);
        //        streamMediator.subscribe(concreteObserver3);
        //
        //        streamMediator.unsubscribe(concreteObserver1);
        //        streamMediator.unsubscribe(concreteObserver2);
        //        streamMediator.unsubscribe(concreteObserver3);
        //
        //        // Confirm the counter was never incremented
        //        assertEquals(0, blockNodeContext.metricsService().get(LiveBlockItems).get());
    }

    @Disabled("@todo(751) - adapt these tests to the new streaming consumer model")
    @Test
    void testSubscribeWhenHandlerAlreadySubscribed() throws IOException {
        //        final BlockNodeContext blockNodeContext = TestConfigUtil.getTestBlockNodeContext();
        //        final LongGauge consumersGauge =
        // blockNodeContext.metricsService().get(BlockNodeMetricTypes.Gauge.Consumers);
        //        final ServiceStatus serviceStatus = new ServiceStatusImpl(blockNodeContext);
        //        final LiveStreamMediator streamMediator = LiveStreamMediatorBuilder.newBuilder(blockNodeContext,
        // serviceStatus)
        //                .build();
        //
        //        final BlockNodeEventHandler<ObjectEvent<List<BlockItemUnparsed>>> concreteObserver1 =
        //                ConsumerStreamBuilder.build(
        //                        completionService,
        //                        testClock,
        //                        streamMediator,
        //                        helidonSubscribeStreamObserver1,
        //                        testContext.metricsService(),
        //                        testContext.configuration());
        //
        //        streamMediator.subscribe(concreteObserver1);
        //        assertTrue(streamMediator.isSubscribed(concreteObserver1));
        //        assertEquals(1L, consumersGauge.get());
        //
        //        // Attempt to "re-subscribe" the observer
        //        // Should not increment the counter or change the implementation
        //        streamMediator.subscribe(concreteObserver1);
        //        assertTrue(streamMediator.isSubscribed(concreteObserver1));
        //        assertEquals(1L, consumersGauge.get());
        //
        //        streamMediator.unsubscribe(concreteObserver1);
        //        assertFalse(streamMediator.isSubscribed(concreteObserver1));
        //
        //        // Confirm the counter was decremented
        //        assertEquals(0L, consumersGauge.get());
    }

    @Disabled("@todo(303), @todo(306) - adapt these tests once #303 and #306 are resolved")
    @Test
    void testOnCancelSubscriptionHandling() throws IOException {
        //
        //            final BlockNodeContext blockNodeContext = TestConfigUtil.getTestBlockNodeContext();
        //            final ServiceStatus serviceStatus = new ServiceStatusImpl(blockNodeContext);
        //            final var streamMediator =
        //                    LiveStreamMediatorBuilder.newBuilder(blockNodeContext, serviceStatus).build();
        //
        //            when(testClock.millis()).thenReturn(TEST_TIME, TEST_TIME + TIMEOUT_THRESHOLD_MILLIS);
        //
        //            final List<BlockItemUnparsed> blockItems = generateBlockItemsUnparsed(1);
        //
        //            // register the stream validator
        //            when(blockWriter.write(List.of(blockItems.getFirst()))).thenReturn(Optional.empty());
        //            final var streamValidator =
        //                    new StreamPersistenceHandlerImpl(
        //                            streamMediator, notifier, blockWriter, blockNodeContext, serviceStatus);
        //            streamMediator.subscribe(streamValidator);
        //
        //            // register the test observer
        //            final var testConsumerBlockItemObserver =
        //                    new TestConsumerStreamResponseObserver(
        //                            testClock, streamMediator, serverCallStreamObserver, testContext);
        //
        //            streamMediator.subscribe(testConsumerBlockItemObserver);
        //            assertTrue(streamMediator.isSubscribed(testConsumerBlockItemObserver));
        //
        //            // Simulate the producer notifying the mediator of a new block
        //            streamMediator.publish(blockItems.getFirst());
        //
        //            // Simulate the consumer cancelling the stream
        //            testConsumerBlockItemObserver.getOnCancel().run();
        //
        //            // Verify the block item incremented the counter
        //            assertEquals(1, blockNodeContext.metricsService().get(LiveBlockItems).get());
        //
        //            // Verify the event made it to the consumer
        //            verify(serverCallStreamObserver,
        //     timeout(testTimeout).times(1)).setOnCancelHandler(any());
        //
        //            // Confirm the mediator unsubscribed the consumer
        //            assertFalse(streamMediator.isSubscribed(testConsumerBlockItemObserver));
        //
        //            // Confirm the BlockStorage write method was called
        //            verify(blockWriter, timeout(testTimeout).times(1)).write(blockItems.getFirst());
        //
        //            // Confirm the stream validator is still subscribed
        //            assertTrue(streamMediator.isSubscribed(streamValidator));
    }

    @Disabled("@todo(303), @todo(306) - adapt these tests once #303 and #306 are resolved")
    @Test
    void testOnCloseSubscriptionHandling() throws IOException {
        //
        //        final BlockNodeContext blockNodeContext = TestConfigUtil.getTestBlockNodeContext();
        //        final ServiceStatus serviceStatus = new ServiceStatusImpl(blockNodeContext);
        //        final var streamMediator =
        //                LiveStreamMediatorBuilder.newBuilder(blockNodeContext, serviceStatus).build();
        //
        //        // testClock configured to be outside the timeout window
        //        when(testClock.millis()).thenReturn(TEST_TIME, TEST_TIME + TIMEOUT_THRESHOLD_MILLIS +
        // 1);
        //
        //        final List<BlockItem> blockItems = generateBlockItems(1);
        //
        //        // register the stream validator
        //        when(blockWriter.write(blockItems.getFirst())).thenReturn(Optional.empty());
        //        final var streamValidator =
        //                new StreamPersistenceHandlerImpl(
        //                        streamMediator, notifier, blockWriter, blockNodeContext,
        // serviceStatus);
        //        streamMediator.subscribe(streamValidator);
        //
        //        final var testConsumerBlockItemObserver =
        //                new TestConsumerStreamResponseObserver(
        //                        testClock, streamMediator, serverCallStreamObserver, testContext);
        //
        //        streamMediator.subscribe(testConsumerBlockItemObserver);
        //        assertTrue(streamMediator.isSubscribed(testConsumerBlockItemObserver));
        //
        //        // Simulate the producer notifying the mediator of a new block
        //        streamMediator.publish(blockItems.getFirst());
        //
        //        // Simulate the consumer completing the stream
        //        testConsumerBlockItemObserver.getOnClose().run();
        //
        //        // Verify the block item incremented the counter
        //        assertEquals(1, blockNodeContext.metricsService().get(LiveBlockItems).get());
        //
        //        // Verify the event made it to the consumer
        //        verify(serverCallStreamObserver,
        // timeout(testTimeout).times(1)).setOnCancelHandler(any());
        //
        //        // Confirm the mediator unsubscribed the consumer
        //        assertFalse(streamMediator.isSubscribed(testConsumerBlockItemObserver));
        //
        //        // Confirm the BlockStorage write method was called
        //        verify(blockWriter, timeout(testTimeout).times(1)).write(blockItems.getFirst());
        //
        //        // Confirm the stream validator is still subscribed
        //        assertTrue(streamMediator.isSubscribed(streamValidator));
    }

    @Disabled("@todo(751) - adapt these tests to the new streaming consumer model")
    @Test
    void testMediatorBlocksPublishAfterException() throws IOException, InterruptedException {
        //        final BlockNodeContext blockNodeContext = TestConfigUtil.getTestBlockNodeContext();
        //        final ServiceStatus serviceStatus = new ServiceStatusImpl(blockNodeContext);
        //        final LiveStreamMediator streamMediator = LiveStreamMediatorBuilder.newBuilder(blockNodeContext,
        // serviceStatus)
        //                .build();
        //
        //        final BlockNodeEventHandler<ObjectEvent<List<BlockItemUnparsed>>> concreteObserver1 =
        //                ConsumerStreamBuilder.build(
        //                        completionService,
        //                        testClock,
        //                        streamMediator,
        //                        helidonSubscribeStreamObserver1,
        //                        testContext.metricsService(),
        //                        testContext.configuration());
        //        final BlockNodeEventHandler<ObjectEvent<List<BlockItemUnparsed>>> concreteObserver2 =
        //                ConsumerStreamBuilder.build(
        //                        completionService,
        //                        testClock,
        //                        streamMediator,
        //                        helidonSubscribeStreamObserver2,
        //                        testContext.metricsService(),
        //                        testContext.configuration());
        //        final BlockNodeEventHandler<ObjectEvent<List<BlockItemUnparsed>>> concreteObserver3 =
        //                ConsumerStreamBuilder.build(
        //                        completionService,
        //                        testClock,
        //                        streamMediator,
        //                        helidonSubscribeStreamObserver3,
        //                        testContext.metricsService(),
        //                        testContext.configuration());
        //
        //        // Set up the subscribers
        //        streamMediator.subscribe(concreteObserver1);
        //        streamMediator.subscribe(concreteObserver2);
        //        streamMediator.subscribe(concreteObserver3);
        //
        //        final Notifier notifier = new NotifierImpl(streamMediator, blockNodeContext, serviceStatus);
        //        final StreamPersistenceHandlerImpl handler = new StreamPersistenceHandlerImpl(
        //                streamMediator,
        //                notifier,
        //                blockNodeContext,
        //                serviceStatus,
        //                ackHandlerMock,
        //                asyncBlockWriterFactoryMock,
        //                executorMock,
        //                archiverMock,
        //                persistenceStorageConfig);
        //
        //        // Set up the stream verifier
        //        streamMediator.subscribe(handler);
        //
        //        final List<BlockItemUnparsed> blockItems = generateBlockItemsUnparsed(1);
        //        final BlockItemUnparsed firstBlockItem = blockItems.getFirst();
        //
        //        // Right now, only a single producer calls publishEvent. In
        //        // that case, they will get an IOException bubbled up to them.
        //        // However, we will need to support multiple producers in the
        //        // future. In that case, we need to make sure a second producer
        //        // is not able to publish a block after the first producer fails.
        //        streamMediator.publish(List.of(firstBlockItem));
        //
        //        Thread.sleep(TEST_TIMEOUT);
        //
        //        // Confirm the counter was incremented only once
        //        assertEquals(1, blockNodeContext.metricsService().get(LiveBlockItems).get());
        //
        //        // Confirm the error counter was incremented
        //        assertEquals(
        //                1,
        //                blockNodeContext
        //                        .metricsService()
        //                        .get(LiveBlockStreamMediatorError)
        //                        .get());
        //
        //        // Send another block item after the exception
        //        streamMediator.publish(List.of(firstBlockItem));
        //        final BlockItemSetUnparsed blockItemSet =
        //                BlockItemSetUnparsed.newBuilder().blockItems(firstBlockItem).build();
        //        final SubscribeStreamResponseUnparsed subscribeStreamResponse =
        // SubscribeStreamResponseUnparsed.newBuilder()
        //                .blockItems(blockItemSet)
        //                .build();
        //        verify(helidonSubscribeStreamObserver1,
        // timeout(TEST_TIMEOUT).times(1)).onNext(subscribeStreamResponse);
        //        verify(helidonSubscribeStreamObserver2,
        // timeout(TEST_TIMEOUT).times(1)).onNext(subscribeStreamResponse);
        //        verify(helidonSubscribeStreamObserver3,
        // timeout(TEST_TIMEOUT).times(1)).onNext(subscribeStreamResponse);
        //
        //        // @todo(662): Revisit this code after we implement an error channel
        //        // TODO: Replace READ_STREAM_SUCCESS (2) with a generic error code?
        //        //        final SubscribeStreamResponseUnparsed endOfStreamResponse =
        //        // SubscribeStreamResponseUnparsed.newBuilder()
        //        //                .status(SubscribeStreamResponseCode.READ_STREAM_SUCCESS)
        //        //                .build();
        //        //        verify(helidonSubscribeStreamObserver1,
        // timeout(TEST_TIMEOUT).times(1)).onNext(endOfStreamResponse);
        //        //        verify(helidonSubscribeStreamObserver2,
        // timeout(TEST_TIMEOUT).times(1)).onNext(endOfStreamResponse);
        //        //        verify(helidonSubscribeStreamObserver3,
        // timeout(TEST_TIMEOUT).times(1)).onNext(endOfStreamResponse);
        //
        //        // Confirm Writer created
        //        verify(asyncBlockWriterFactoryMock, timeout(TEST_TIMEOUT).times(1)).create(1L);
    }

    @Disabled("@todo(751) - adapt these tests to the new streaming consumer model")
    @Test
    void testUnsubscribeWhenNotSubscribed() throws IOException {
        //        final BlockNodeContext blockNodeContext = TestConfigUtil.getTestBlockNodeContext();
        //        final ServiceStatus serviceStatus = new ServiceStatusImpl(blockNodeContext);
        //        final LiveStreamMediator streamMediator = LiveStreamMediatorBuilder.newBuilder(blockNodeContext,
        // serviceStatus)
        //                .build();
        //
        //        // register the stream validator
        //        final StreamPersistenceHandlerImpl handler = new StreamPersistenceHandlerImpl(
        //                streamMediator,
        //                notifier,
        //                blockNodeContext,
        //                serviceStatus,
        //                ackHandlerMock,
        //                asyncBlockWriterFactoryMock,
        //                executorMock,
        //                archiverMock,
        //                persistenceStorageConfig);
        //        streamMediator.subscribe(handler);
        //
        //        final BlockNodeEventHandler<ObjectEvent<List<BlockItemUnparsed>>> testConsumerBlockItemObserver =
        //                ConsumerStreamBuilder.build(
        //                        completionService,
        //                        testClock,
        //                        streamMediator,
        //                        helidonSubscribeStreamObserver1,
        //                        testContext.metricsService(),
        //                        testContext.configuration());
        //
        //        // Confirm the observer is not subscribed
        //        assertFalse(streamMediator.isSubscribed(testConsumerBlockItemObserver));
        //
        //        // Attempt to unsubscribe the observer
        //        streamMediator.unsubscribe(testConsumerBlockItemObserver);
        //
        //        // Confirm the observer is still not subscribed
        //        assertFalse(streamMediator.isSubscribed(testConsumerBlockItemObserver));
        //
        //        // Confirm the stream validator is still subscribed
        //        assertTrue(streamMediator.isSubscribed(handler));
    }
}
