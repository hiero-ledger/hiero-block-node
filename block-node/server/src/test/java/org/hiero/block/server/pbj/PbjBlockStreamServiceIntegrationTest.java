// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.pbj;

import static com.hedera.hapi.block.SubscribeStreamResponseCode.READ_STREAM_NOT_AVAILABLE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hiero.block.server.util.PbjProtoTestUtils.buildEmptyPublishStreamRequest;
import static org.hiero.block.server.util.PbjProtoTestUtils.buildLiveStreamSubscribeStreamRequest;
import static org.hiero.block.server.util.PersistTestUtils.PERSISTENCE_STORAGE_ARCHIVE_ROOT_PATH_KEY;
import static org.hiero.block.server.util.PersistTestUtils.PERSISTENCE_STORAGE_LIVE_ROOT_PATH_KEY;
import static org.hiero.block.server.util.PersistTestUtils.PERSISTENCE_STORAGE_UNVERIFIED_ROOT_PATH_KEY;
import static org.hiero.block.server.util.PersistTestUtils.generateBlockItemsUnparsed;
import static org.hiero.block.server.util.TestUtils.onEventIncrementCount;
import static org.hiero.block.server.util.TestUtils.onEventLatchCountdown;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.hedera.hapi.block.BlockItemSetUnparsed;
import com.hedera.hapi.block.BlockItemUnparsed;
import com.hedera.hapi.block.BlockUnparsed;
import com.hedera.hapi.block.EndOfStream;
import com.hedera.hapi.block.PublishStreamRequestUnparsed;
import com.hedera.hapi.block.PublishStreamResponse;
import com.hedera.hapi.block.PublishStreamResponseCode;
import com.hedera.hapi.block.SingleBlockRequest;
import com.hedera.hapi.block.SingleBlockResponseCode;
import com.hedera.hapi.block.SingleBlockResponseUnparsed;
import com.hedera.hapi.block.SubscribeStreamResponseUnparsed;
import com.hedera.pbj.runtime.grpc.Pipeline;
import com.hedera.pbj.runtime.grpc.ServiceInterface;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.lmax.disruptor.BatchEventProcessor;
import com.lmax.disruptor.EventPoller;
import com.swirlds.config.api.Configuration;
import io.helidon.webserver.WebServer;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.hiero.block.server.ack.AckHandler;
import org.hiero.block.server.ack.AckHandlerImpl;
import org.hiero.block.server.block.BlockInfo;
import org.hiero.block.server.consumer.ConsumerConfig;
import org.hiero.block.server.consumer.StreamManager;
import org.hiero.block.server.events.BlockNodeEventHandler;
import org.hiero.block.server.events.ObjectEvent;
import org.hiero.block.server.mediator.LiveStreamMediator;
import org.hiero.block.server.mediator.LiveStreamMediatorBuilder;
import org.hiero.block.server.mediator.MediatorConfig;
import org.hiero.block.server.metrics.MetricsService;
import org.hiero.block.server.notifier.Notifier;
import org.hiero.block.server.notifier.NotifierConfig;
import org.hiero.block.server.notifier.NotifierImpl;
import org.hiero.block.server.persistence.StreamPersistenceHandlerImpl;
import org.hiero.block.server.persistence.storage.PersistenceStorageConfig;
import org.hiero.block.server.persistence.storage.archive.LocalBlockArchiver;
import org.hiero.block.server.persistence.storage.compression.NoOpCompression;
import org.hiero.block.server.persistence.storage.path.BlockAsLocalFilePathResolver;
import org.hiero.block.server.persistence.storage.path.BlockPathResolver;
import org.hiero.block.server.persistence.storage.read.BlockReader;
import org.hiero.block.server.persistence.storage.remove.BlockAsLocalFileRemover;
import org.hiero.block.server.persistence.storage.remove.BlockRemover;
import org.hiero.block.server.persistence.storage.write.AsyncBlockAsLocalFileWriterFactory;
import org.hiero.block.server.persistence.storage.write.AsyncBlockWriterFactory;
import org.hiero.block.server.persistence.storage.write.AsyncNoOpWriterFactory;
import org.hiero.block.server.producer.ProducerConfig;
import org.hiero.block.server.service.ServiceConfig;
import org.hiero.block.server.service.ServiceStatus;
import org.hiero.block.server.service.ServiceStatusImpl;
import org.hiero.block.server.util.BlockingExecutorService;
import org.hiero.block.server.util.TestConfigUtil;
import org.hiero.block.server.verification.StreamVerificationHandlerImpl;
import org.hiero.block.server.verification.VerificationConfig;
import org.hiero.block.server.verification.service.BlockVerificationService;
import org.hiero.block.server.verification.service.BlockVerificationServiceImpl;
import org.hiero.block.server.verification.session.BlockVerificationSessionFactory;
import org.hiero.block.server.verification.signature.SignatureVerifierDummy;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@SuppressWarnings("FieldCanBeLocal")
@ExtendWith(MockitoExtension.class)
class PbjBlockStreamServiceIntegrationTest {
    private static final int JUNIT_TIMEOUT = 5_000;
    private static final int testTimeout = 2_000;

    private static final int POLLER_SUBSCRIBER_SLEEP = 100;

    @Mock
    private Pipeline<? super Bytes> helidonPublishStreamObserver1;

    @Mock
    private Pipeline<? super Bytes> helidonPublishStreamObserver2;

    @Mock
    private Pipeline<? super Bytes> helidonPublishStreamObserver3;

    @Mock
    private Pipeline<? super Bytes> subscribeStreamObserver1;

    @Mock
    private Pipeline<? super Bytes> subscribeStreamObserver2;

    @Mock
    private Pipeline<? super Bytes> subscribeStreamObserver3;

    @Mock
    private Pipeline<? super Bytes> subscribeStreamObserver4;

    @Mock
    private Pipeline<? super Bytes> subscribeStreamObserver5;

    @Mock
    private Pipeline<? super Bytes> subscribeStreamObserver6;

    @Mock
    private Notifier notifierMock;

    @Mock
    private WebServer webServerMock;

    @Mock
    private BlockReader<BlockUnparsed> blockReaderMock;

    @Mock
    private ServiceInterface.RequestOptions optionsMock;

    @Mock
    private AckHandler ackHandlerMock;

    @Mock
    private AsyncBlockWriterFactory asyncBlockWriterFactoryMock;

    @Mock
    private Executor executorMock;

    @Mock
    private LocalBlockArchiver archiverMock;

    @Mock
    private BlockPathResolver pathResolverMock;

    @TempDir
    private Path testTempDir;

    private MetricsService metricsService;
    private ServiceConfig serviceConfig;
    private ConsumerConfig consumerConfig;
    private ProducerConfig producerConfig;
    private NotifierConfig notifierConfig;
    private VerificationConfig verificationConfig;
    private MediatorConfig mediatorConfig;
    private PersistenceStorageConfig persistenceStorageConfig;

    @BeforeEach
    void setUp() throws IOException {
        final Map<String, String> properties = new HashMap<>();
        final Path testLiveRootPath = testTempDir.resolve("live");
        final Path testArchiveRootPath = testTempDir.resolve("archive");
        final Path testUnverifiedRootPath = testTempDir.resolve("unverified");
        properties.put(PERSISTENCE_STORAGE_LIVE_ROOT_PATH_KEY, testLiveRootPath.toString());
        properties.put(PERSISTENCE_STORAGE_ARCHIVE_ROOT_PATH_KEY, testArchiveRootPath.toString());
        properties.put(PERSISTENCE_STORAGE_UNVERIFIED_ROOT_PATH_KEY, testUnverifiedRootPath.toString());
        Configuration config = TestConfigUtil.getTestBlockNodeConfiguration(properties);
        metricsService = TestConfigUtil.getTestBlockNodeMetricsService(config);
        persistenceStorageConfig = config.getConfigData(PersistenceStorageConfig.class);
        final Path testConfigLiveRootPath = persistenceStorageConfig.liveRootPath();
        assertThat(testConfigLiveRootPath).isEqualTo(testLiveRootPath);
        final Path testConfigArchiveRootPath = persistenceStorageConfig.archiveRootPath();
        assertThat(testConfigArchiveRootPath).isEqualTo(testArchiveRootPath);
        final Path testConfigUnverifiedRootPath = persistenceStorageConfig.unverifiedRootPath();
        assertThat(testConfigUnverifiedRootPath).isEqualTo(testUnverifiedRootPath);

        serviceConfig = config.getConfigData(ServiceConfig.class);
        consumerConfig = config.getConfigData(ConsumerConfig.class);
        producerConfig = config.getConfigData(ProducerConfig.class);
        notifierConfig = config.getConfigData(NotifierConfig.class);
        verificationConfig = config.getConfigData(VerificationConfig.class);
        mediatorConfig = config.getConfigData(MediatorConfig.class);
    }

    @Disabled
    @Test
    @Timeout(value = JUNIT_TIMEOUT, unit = TimeUnit.MILLISECONDS)
    void testPublishBlockStreamRegistrationAndExecution() throws IOException, InterruptedException {
        final int numberOfBlocks = 1;

        final ExecutorService persistenceExecutor = Executors.newFixedThreadPool(numberOfBlocks);
        final BlockingExecutorService subscriberExecutor1 = new BlockingExecutorService(1, 1);
        final BlockingExecutorService subscriberExecutor2 = new BlockingExecutorService(1, 1);
        final BlockingExecutorService subscriberExecutor3 = new BlockingExecutorService(1, 1);

        // The PublishStreamObserver logic is executed via lmax BatchEventProcessors internally,
        // so BlockingExecutorService approach is not applicable.
        CountDownLatch publishStreamObserversLatch = new CountDownLatch(3);
        doAnswer(onEventLatchCountdown(publishStreamObserversLatch))
                .when(helidonPublishStreamObserver1)
                .onNext(any());
        doAnswer(onEventLatchCountdown(publishStreamObserversLatch))
                .when(helidonPublishStreamObserver2)
                .onNext(any());
        doAnswer(onEventLatchCountdown(publishStreamObserversLatch))
                .when(helidonPublishStreamObserver3)
                .onNext(any());

        final PbjBlockStreamServiceProxy pbjBlockStreamServiceProxy =
                buildBlockStreamService(blockReaderMock, persistenceExecutor, false, 0);

        // Register 3 producers - Opening a pipeline is not enough to register a producer.
        // pipeline.onNext() must be invoked to register the producer at the Helidon PBJ layer.
        final Pipeline<? super Bytes> producerPipeline = pbjBlockStreamServiceProxy.open(
                PbjBlockStreamService.BlockStreamMethod.publishBlockStream, optionsMock, helidonPublishStreamObserver1);
        pbjBlockStreamServiceProxy
                .open(
                        PbjBlockStreamService.BlockStreamMethod.publishBlockStream,
                        optionsMock,
                        helidonPublishStreamObserver2)
                .onNext(buildEmptyPublishStreamRequest());
        pbjBlockStreamServiceProxy
                .open(
                        PbjBlockStreamService.BlockStreamMethod.publishBlockStream,
                        optionsMock,
                        helidonPublishStreamObserver3)
                .onNext(buildEmptyPublishStreamRequest());

        // Register 3 consumers - Opening a pipeline is not enough to register a consumer.
        // pipeline.onNext() must be invoked to register the consumer at the Helidon PBJ layer.
        pbjBlockStreamServiceProxy
                .open(
                        PbjBlockStreamService.BlockStreamMethod.subscribeBlockStream,
                        optionsMock,
                        subscribeStreamObserver1,
                        subscriberExecutor1,
                        subscriberExecutor1)
                .onNext(buildLiveStreamSubscribeStreamRequest());
        pbjBlockStreamServiceProxy
                .open(
                        PbjBlockStreamService.BlockStreamMethod.subscribeBlockStream,
                        optionsMock,
                        subscribeStreamObserver2,
                        subscriberExecutor2,
                        subscriberExecutor2)
                .onNext(buildLiveStreamSubscribeStreamRequest());
        pbjBlockStreamServiceProxy
                .open(
                        PbjBlockStreamService.BlockStreamMethod.subscribeBlockStream,
                        optionsMock,
                        subscribeStreamObserver3,
                        subscriberExecutor3,
                        subscriberExecutor3)
                .onNext(buildLiveStreamSubscribeStreamRequest());

        final List<BlockItemUnparsed> blockItems = generateBlockItemsUnparsed(numberOfBlocks);
        for (final BlockItemUnparsed blockItem : blockItems) {
            // Calling onNext() as Helidon does
            final BlockItemSetUnparsed blockItemSet =
                    BlockItemSetUnparsed.newBuilder().blockItems(blockItem).build();
            final PublishStreamRequestUnparsed publishStreamRequest = PublishStreamRequestUnparsed.newBuilder()
                    .blockItems(blockItemSet)
                    .build();
            producerPipeline.onNext(PublishStreamRequestUnparsed.PROTOBUF.toBytes(publishStreamRequest));
        }

        // Close the stream as Helidon does
        helidonPublishStreamObserver1.onComplete();

        // Wait for subscribers and publishers to finish execution before assertions
        subscriberExecutor1.waitTasksToComplete();
        subscriberExecutor2.waitTasksToComplete();
        subscriberExecutor3.waitTasksToComplete();
        publishStreamObserversLatch.await();

        // Verify all 10 BlockItems were sent to each of the 3 consumers
        verify(subscribeStreamObserver1, times(1)).onNext(buildSubscribeStreamResponse(blockItems.getFirst()));
        verify(subscribeStreamObserver1, times(8)).onNext(buildSubscribeStreamResponse(blockItems.get(1)));
        verify(subscribeStreamObserver1, times(1)).onNext(buildSubscribeStreamResponse(blockItems.get(9)));

        verify(subscribeStreamObserver2, times(1)).onNext(buildSubscribeStreamResponse(blockItems.getFirst()));
        verify(subscribeStreamObserver2, times(8)).onNext(buildSubscribeStreamResponse(blockItems.get(1)));
        verify(subscribeStreamObserver2, times(1)).onNext(buildSubscribeStreamResponse(blockItems.get(9)));

        verify(subscribeStreamObserver3, times(1)).onNext(buildSubscribeStreamResponse(blockItems.getFirst()));
        verify(subscribeStreamObserver3, times(8)).onNext(buildSubscribeStreamResponse(blockItems.get(1)));
        verify(subscribeStreamObserver3, times(1)).onNext(buildSubscribeStreamResponse(blockItems.get(9)));

        verify(helidonPublishStreamObserver1, times(1)).onNext(any());
        verify(helidonPublishStreamObserver2, times(1)).onNext(any());
        verify(helidonPublishStreamObserver3, times(1)).onNext(any());

        // verify the onCompleted() method is invoked on the wrapped StreamObserver
        verify(helidonPublishStreamObserver1, times(1)).onComplete();
    }

    @Test
    @Timeout(value = JUNIT_TIMEOUT, unit = TimeUnit.MILLISECONDS)
    void testFullProducerConsumerHappyPath() throws IOException, InterruptedException {
        final int numberOfBlocks = 5;

        final ExecutorService persistenceExecutor = Executors.newFixedThreadPool(numberOfBlocks);
        final BlockingExecutorService subscriberExecutor1 = new BlockingExecutorService(1, 1);
        final BlockingExecutorService subscriberExecutor2 = new BlockingExecutorService(1, 1);
        final BlockingExecutorService subscriberExecutor3 = new BlockingExecutorService(1, 1);

        // The PublishStreamObserver logic is executed via lmax BatchEventProcessors internally,
        // so BlockingExecutorService approach is not applicable.
        CountDownLatch publishStreamObserversLatch = new CountDownLatch(3 * numberOfBlocks);
        doAnswer(onEventLatchCountdown(publishStreamObserversLatch))
                .when(helidonPublishStreamObserver1)
                .onNext(any());
        doAnswer(onEventLatchCountdown(publishStreamObserversLatch))
                .when(helidonPublishStreamObserver2)
                .onNext(any());
        doAnswer(onEventLatchCountdown(publishStreamObserversLatch))
                .when(helidonPublishStreamObserver3)
                .onNext(any());

        // Use a real BlockWriter to test the full integration
        final PbjBlockStreamServiceProxy pbjBlockStreamServiceProxy =
                buildBlockStreamService(blockReaderMock, persistenceExecutor, false, 0);

        // Register 3 producers - Opening a pipeline is not enough to register a producer.
        // pipeline.onNext() must be invoked to register the producer at the Helidon PBJ layer.
        final Pipeline<? super Bytes> producerPipeline = pbjBlockStreamServiceProxy.open(
                PbjBlockStreamService.BlockStreamMethod.publishBlockStream, optionsMock, helidonPublishStreamObserver1);
        pbjBlockStreamServiceProxy
                .open(
                        PbjBlockStreamService.BlockStreamMethod.publishBlockStream,
                        optionsMock,
                        helidonPublishStreamObserver2)
                .onNext(buildEmptyPublishStreamRequest());
        pbjBlockStreamServiceProxy
                .open(
                        PbjBlockStreamService.BlockStreamMethod.publishBlockStream,
                        optionsMock,
                        helidonPublishStreamObserver3)
                .onNext(buildEmptyPublishStreamRequest());

        // Register 3 consumers - Opening a pipeline is not enough to register a consumer.
        // pipeline.onNext() must be invoked to register the consumer at the Helidon PBJ layer.
        pbjBlockStreamServiceProxy
                .open(
                        PbjBlockStreamService.BlockStreamMethod.subscribeBlockStream,
                        optionsMock,
                        subscribeStreamObserver1,
                        subscriberExecutor1,
                        subscriberExecutor1)
                .onNext(buildLiveStreamSubscribeStreamRequest());
        pbjBlockStreamServiceProxy
                .open(
                        PbjBlockStreamService.BlockStreamMethod.subscribeBlockStream,
                        optionsMock,
                        subscribeStreamObserver2,
                        subscriberExecutor2,
                        subscriberExecutor2)
                .onNext(buildLiveStreamSubscribeStreamRequest());
        pbjBlockStreamServiceProxy
                .open(
                        PbjBlockStreamService.BlockStreamMethod.subscribeBlockStream,
                        optionsMock,
                        subscribeStreamObserver3,
                        subscriberExecutor3,
                        subscriberExecutor3)
                .onNext(buildLiveStreamSubscribeStreamRequest());

        final List<BlockItemUnparsed> blockItems = generateBlockItemsUnparsed(numberOfBlocks);
        for (BlockItemUnparsed blockItem : blockItems) {
            // Calling onNext() as Helidon does
            final BlockItemSetUnparsed blockItemSet =
                    BlockItemSetUnparsed.newBuilder().blockItems(blockItem).build();
            final PublishStreamRequestUnparsed publishStreamRequest = PublishStreamRequestUnparsed.newBuilder()
                    .blockItems(blockItemSet)
                    .build();
            producerPipeline.onNext(PublishStreamRequestUnparsed.PROTOBUF.toBytes(publishStreamRequest));
        }

        subscriberExecutor1.waitTasksToComplete();
        subscriberExecutor2.waitTasksToComplete();
        subscriberExecutor3.waitTasksToComplete();
        publishStreamObserversLatch.await();

        // Verify the subscribers received the data
        verifySubscribeStreamResponse(numberOfBlocks, 0, numberOfBlocks, subscribeStreamObserver1, blockItems, false);
        verifySubscribeStreamResponse(numberOfBlocks, 0, numberOfBlocks, subscribeStreamObserver2, blockItems, false);
        verifySubscribeStreamResponse(numberOfBlocks, 0, numberOfBlocks, subscribeStreamObserver3, blockItems, false);

        // Verify the producers received all the responses
        verify(helidonPublishStreamObserver1, times(numberOfBlocks)).onNext(any());
        verify(helidonPublishStreamObserver2, times(numberOfBlocks)).onNext(any());
        verify(helidonPublishStreamObserver3, times(numberOfBlocks)).onNext(any());
    }

    @Test
    @Timeout(value = JUNIT_TIMEOUT, unit = TimeUnit.MILLISECONDS)
    void testFullWithSubscribersAddedDynamically() throws IOException, InterruptedException {
        final int numberOfBlocks = 100;

        final BlockingExecutorService subscriberExecutor1 = new BlockingExecutorService(1, 1);
        final BlockingExecutorService subscriberExecutor2 = new BlockingExecutorService(1, 1);
        final BlockingExecutorService subscriberExecutor3 = new BlockingExecutorService(1, 1);
        final BlockingExecutorService subscriberExecutor4 = new BlockingExecutorService(1, 1);
        final BlockingExecutorService subscriberExecutor5 = new BlockingExecutorService(1, 1);
        final BlockingExecutorService subscriberExecutor6 = new BlockingExecutorService(1, 1);
        final BlockingExecutorService persistenceExecutor = new BlockingExecutorService(numberOfBlocks, numberOfBlocks);

        // These latches will be used to await subscription of dynamically added subscribers
        CountDownLatch subscriber4Latch = new CountDownLatch(1);
        CountDownLatch subscriber5Latch = new CountDownLatch(1);
        CountDownLatch subscriber6Latch = new CountDownLatch(1);
        doAnswer(onEventLatchCountdown(subscriber4Latch))
                .when(subscribeStreamObserver4)
                .onSubscribe(any());
        doAnswer(onEventLatchCountdown(subscriber5Latch))
                .when(subscribeStreamObserver5)
                .onSubscribe(any());
        doAnswer(onEventLatchCountdown(subscriber6Latch))
                .when(subscribeStreamObserver6)
                .onSubscribe(any());

        // These counters will be used to determine the exact amount of items passed to the subscribers
        // because the amount starts to accumulate after the beginning of a new block
        AtomicInteger sub4CallCount = new AtomicInteger();
        doAnswer(onEventIncrementCount(sub4CallCount))
                .when(subscribeStreamObserver4)
                .onNext(any());
        AtomicInteger sub5CallCount = new AtomicInteger();
        doAnswer(onEventIncrementCount(sub5CallCount))
                .when(subscribeStreamObserver5)
                .onNext(any());
        AtomicInteger sub6CallCount = new AtomicInteger();
        doAnswer(onEventIncrementCount(sub6CallCount))
                .when(subscribeStreamObserver6)
                .onNext(any());

        final PbjBlockStreamServiceProxy pbjBlockStreamServiceProxy =
                buildBlockStreamService(blockReaderMock, persistenceExecutor, false, 1);

        // Register a producer
        final Pipeline<? super Bytes> producerPipeline = pbjBlockStreamServiceProxy.open(
                PbjBlockStreamService.BlockStreamMethod.publishBlockStream, optionsMock, helidonPublishStreamObserver1);

        // Register 3 consumers - Opening a pipeline is not enough to register a consumer.
        // pipeline.onNext() must be invoked to register the consumer at the Helidon PBJ layer.
        pbjBlockStreamServiceProxy
                .open(
                        PbjBlockStreamService.BlockStreamMethod.subscribeBlockStream,
                        optionsMock,
                        subscribeStreamObserver1,
                        subscriberExecutor1,
                        subscriberExecutor1)
                .onNext(buildLiveStreamSubscribeStreamRequest());
        pbjBlockStreamServiceProxy
                .open(
                        PbjBlockStreamService.BlockStreamMethod.subscribeBlockStream,
                        optionsMock,
                        subscribeStreamObserver2,
                        subscriberExecutor2,
                        subscriberExecutor2)
                .onNext(buildLiveStreamSubscribeStreamRequest());
        pbjBlockStreamServiceProxy
                .open(
                        PbjBlockStreamService.BlockStreamMethod.subscribeBlockStream,
                        optionsMock,
                        subscribeStreamObserver3,
                        subscriberExecutor3,
                        subscriberExecutor3)
                .onNext(buildLiveStreamSubscribeStreamRequest());

        final List<BlockItemUnparsed> blockItems = generateBlockItemsUnparsed(numberOfBlocks);
        for (int i = 0; i < blockItems.size(); i++) {
            final PublishStreamRequestUnparsed publishStreamRequest = PublishStreamRequestUnparsed.newBuilder()
                    .blockItems(new BlockItemSetUnparsed(List.of(blockItems.get(i))))
                    .build();
            // Add a new subscriber
            if (i == 51) {
                pbjBlockStreamServiceProxy
                        .open(
                                PbjBlockStreamService.BlockStreamMethod.subscribeBlockStream,
                                optionsMock,
                                subscribeStreamObserver4,
                                subscriberExecutor4,
                                subscriberExecutor4)
                        .onNext(buildLiveStreamSubscribeStreamRequest());
                // Pause here for the StreamManager to
                // subscribe to the StreamMediator
                subscriber4Latch.await();
            }
            // Transmit the BlockItem
            producerPipeline.onNext(PublishStreamRequestUnparsed.PROTOBUF.toBytes(publishStreamRequest));
            // Add a new subscriber
            if (i == 76) {
                pbjBlockStreamServiceProxy
                        .open(
                                PbjBlockStreamService.BlockStreamMethod.subscribeBlockStream,
                                optionsMock,
                                subscribeStreamObserver5,
                                subscriberExecutor5,
                                subscriberExecutor5)
                        .onNext(buildLiveStreamSubscribeStreamRequest());
                // Pause here for the StreamManager to
                // subscribe to the StreamMediator
                subscriber5Latch.await();
            }
            // Add a new subscriber
            if (i == 88) {
                pbjBlockStreamServiceProxy
                        .open(
                                PbjBlockStreamService.BlockStreamMethod.subscribeBlockStream,
                                optionsMock,
                                subscribeStreamObserver6,
                                subscriberExecutor6,
                                subscriberExecutor6)
                        .onNext(buildLiveStreamSubscribeStreamRequest());
                // Pause here for the StreamManager to
                // subscribe to the StreamMediator
                subscriber6Latch.await();
            }
        }
        persistenceExecutor.waitTasksToComplete();
        subscriberExecutor1.waitTasksToComplete();
        subscriberExecutor2.waitTasksToComplete();
        subscriberExecutor3.waitTasksToComplete();
        subscriberExecutor4.waitTasksToComplete();
        subscriberExecutor5.waitTasksToComplete();
        subscriberExecutor6.waitTasksToComplete();

        // Verify subscribers who were listening before the stream started
        verifySubscribeStreamResponse(numberOfBlocks, 0, numberOfBlocks, subscribeStreamObserver1, blockItems, false);
        verifySubscribeStreamResponse(numberOfBlocks, 0, numberOfBlocks, subscribeStreamObserver2, blockItems, false);
        verifySubscribeStreamResponse(numberOfBlocks, 0, numberOfBlocks, subscribeStreamObserver3, blockItems, false);

        // Verify subscribers added while the stream was in progress.
        // The Helidon-provided StreamObserver onNext() method will only
        // be called once a Header BlockItem is reached. So, pass in
        // the number of BlockItems to wait to verify that the method
        // was called.
        verifySubscribeStreamResponse(
                numberOfBlocks,
                blockItems.size() - sub4CallCount.get(),
                numberOfBlocks,
                subscribeStreamObserver4,
                blockItems,
                false);
        verifySubscribeStreamResponse(
                numberOfBlocks,
                blockItems.size() - sub5CallCount.get(),
                numberOfBlocks,
                subscribeStreamObserver5,
                blockItems,
                false);
        verifySubscribeStreamResponse(
                numberOfBlocks,
                blockItems.size() - sub6CallCount.get(),
                numberOfBlocks,
                subscribeStreamObserver6,
                blockItems,
                false);
    }

    @Disabled
    @Test
    @Timeout(value = JUNIT_TIMEOUT, unit = TimeUnit.MILLISECONDS)
    void testSubAndUnsubWhileStreaming() throws InterruptedException, IOException {
        final int numberOfBlocks = 100;
        final Map<
                        BlockNodeEventHandler<ObjectEvent<List<BlockItemUnparsed>>>,
                        BatchEventProcessor<ObjectEvent<List<BlockItemUnparsed>>>>
                consumers = new ConcurrentHashMap<>();
        final LinkedHashMap<StreamManager, EventPoller<ObjectEvent<List<BlockItemUnparsed>>>> pollConsumers =
                new LinkedHashMap<>();
        final ServiceStatus serviceStatus = new ServiceStatusImpl(serviceConfig);
        final BlockInfo blockInfo = new BlockInfo(1L);
        serviceStatus.setLatestAckedBlock(blockInfo);
        final LiveStreamMediator streamMediator = buildStreamMediator(consumers, pollConsumers, serviceStatus);
        final AsyncNoOpWriterFactory writerFactory = new AsyncNoOpWriterFactory(ackHandlerMock, metricsService);
        final StreamPersistenceHandlerImpl blockNodeEventHandler = new StreamPersistenceHandlerImpl(
                streamMediator,
                notifierMock,
                metricsService,
                serviceStatus,
                ackHandlerMock,
                writerFactory,
                executorMock,
                archiverMock,
                pathResolverMock,
                persistenceStorageConfig);
        final StreamVerificationHandlerImpl streamVerificationHandler = new StreamVerificationHandlerImpl(
                streamMediator, notifierMock, metricsService, serviceStatus, mock(BlockVerificationService.class));
        final PbjBlockStreamServiceProxy pbjBlockStreamServiceProxy = new PbjBlockStreamServiceProxy(
                streamMediator,
                serviceStatus,
                blockNodeEventHandler,
                streamVerificationHandler,
                blockReaderMock,
                notifierMock,
                metricsService,
                consumerConfig,
                producerConfig);

        final Pipeline<? super Bytes> producerPipeline = pbjBlockStreamServiceProxy.open(
                PbjBlockStreamService.BlockStreamMethod.publishBlockStream, optionsMock, helidonPublishStreamObserver1);

        // Register 3 consumers - Opening a pipeline is not enough to register a consumer.
        // pipeline.onNext() must be invoked to register the consumer at the Helidon PBJ layer.
        pbjBlockStreamServiceProxy
                .open(
                        PbjBlockStreamService.BlockStreamMethod.subscribeBlockStream,
                        optionsMock,
                        subscribeStreamObserver1)
                .onNext(buildLiveStreamSubscribeStreamRequest());
        pbjBlockStreamServiceProxy
                .open(
                        PbjBlockStreamService.BlockStreamMethod.subscribeBlockStream,
                        optionsMock,
                        subscribeStreamObserver2)
                .onNext(buildLiveStreamSubscribeStreamRequest());
        pbjBlockStreamServiceProxy
                .open(
                        PbjBlockStreamService.BlockStreamMethod.subscribeBlockStream,
                        optionsMock,
                        subscribeStreamObserver3)
                .onNext(buildLiveStreamSubscribeStreamRequest());

        final List<BlockItemUnparsed> blockItems = generateBlockItemsUnparsed(numberOfBlocks);
        for (int i = 0; i < blockItems.size(); i++) {

            // Transmit the BlockItem
            final PublishStreamRequestUnparsed publishStreamRequest = PublishStreamRequestUnparsed.newBuilder()
                    .blockItems(new BlockItemSetUnparsed(List.of(blockItems.get(i))))
                    .build();
            producerPipeline.onNext(PublishStreamRequestUnparsed.PROTOBUF.toBytes(publishStreamRequest));

            // Remove 1st subscriber
            if (i == 10) {
                // Pause here to ensure the last sent block item is received.
                // This makes the test deterministic.
                Thread.sleep(POLLER_SUBSCRIBER_SLEEP);
                final StreamManager k = pollConsumers.firstEntry().getKey();
                streamMediator.unsubscribePoller(k);
            }

            // Remove 2nd subscriber
            if (i == 60) {
                // Pause here to ensure the last sent block item is received.
                // This makes the test deterministic.
                Thread.sleep(POLLER_SUBSCRIBER_SLEEP);
                final StreamManager k = pollConsumers.firstEntry().getKey();
                streamMediator.unsubscribePoller(k);
            }

            // Add a new subscriber
            if (i == 51) {
                pbjBlockStreamServiceProxy
                        .open(
                                PbjBlockStreamService.BlockStreamMethod.subscribeBlockStream,
                                optionsMock,
                                subscribeStreamObserver4)
                        .onNext(buildLiveStreamSubscribeStreamRequest());
                Thread.sleep(POLLER_SUBSCRIBER_SLEEP);
            }

            // Remove 3rd subscriber
            if (i == 70) {
                // Pause here to ensure the last sent block item is received.
                // This makes the test deterministic.
                Thread.sleep(POLLER_SUBSCRIBER_SLEEP);
                final StreamManager k = pollConsumers.firstEntry().getKey();
                streamMediator.unsubscribePoller(k);
                Thread.sleep(POLLER_SUBSCRIBER_SLEEP);
            }

            // Add a new subscriber
            if (i == 76) {
                pbjBlockStreamServiceProxy
                        .open(
                                PbjBlockStreamService.BlockStreamMethod.subscribeBlockStream,
                                optionsMock,
                                subscribeStreamObserver5)
                        .onNext(buildLiveStreamSubscribeStreamRequest());
                Thread.sleep(POLLER_SUBSCRIBER_SLEEP);
            }

            // Add a new subscriber
            if (i == 88) {
                pbjBlockStreamServiceProxy
                        .open(
                                PbjBlockStreamService.BlockStreamMethod.subscribeBlockStream,
                                optionsMock,
                                subscribeStreamObserver6)
                        .onNext(buildLiveStreamSubscribeStreamRequest());
                Thread.sleep(POLLER_SUBSCRIBER_SLEEP);
            }
        }

        // Verify subscribers who were listening before the stream started
        verifySubscribeStreamResponse(numberOfBlocks, 0, 10, subscribeStreamObserver1, blockItems, true);
        verifySubscribeStreamResponse(numberOfBlocks, 0, 60, subscribeStreamObserver2, blockItems, true);
        verifySubscribeStreamResponse(numberOfBlocks, 0, 70, subscribeStreamObserver3, blockItems, true);

        // Verify subscribers added while the stream was in progress.
        // The Helidon-provided StreamObserver onNext() method will only
        // be called once a Header BlockItem is reached. So, pass in
        // the number of BlockItems to wait to verify that the method
        // was called.
        verifySubscribeStreamResponse(numberOfBlocks, 59, numberOfBlocks, subscribeStreamObserver4, blockItems, true);
        verifySubscribeStreamResponse(numberOfBlocks, 79, numberOfBlocks, subscribeStreamObserver5, blockItems, true);
        verifySubscribeStreamResponse(numberOfBlocks, 89, numberOfBlocks, subscribeStreamObserver6, blockItems, true);

        producerPipeline.onComplete();
    }

    @Test
    @Timeout(value = JUNIT_TIMEOUT, unit = TimeUnit.MILLISECONDS)
    void testMediatorExceptionHandlingWhenPersistenceFailure() throws IOException, InterruptedException {
        final Map<
                        BlockNodeEventHandler<ObjectEvent<List<BlockItemUnparsed>>>,
                        BatchEventProcessor<ObjectEvent<List<BlockItemUnparsed>>>>
                consumers = new ConcurrentHashMap<>();
        final ConcurrentHashMap<StreamManager, EventPoller<ObjectEvent<List<BlockItemUnparsed>>>> pollConsumers =
                new ConcurrentHashMap<>();
        // Use a spy to use the real object but also verify the behavior.
        final ServiceStatus serviceStatus = spy(new ServiceStatusImpl(serviceConfig));
        final BlockInfo blockInfo = new BlockInfo(1L);
        serviceStatus.setLatestAckedBlock(blockInfo);
        doCallRealMethod().when(serviceStatus).setWebServer(webServerMock);
        doCallRealMethod().when(serviceStatus).isRunning();
        doCallRealMethod().when(serviceStatus).stopWebServer(any());
        serviceStatus.setWebServer(webServerMock);

        final List<BlockItemUnparsed> blockItems = generateBlockItemsUnparsed(1);

        // the mocked factory will throw a npe
        final LiveStreamMediator streamMediator = buildStreamMediator(consumers, pollConsumers, serviceStatus);
        final Notifier notifier =
                new NotifierImpl(streamMediator, metricsService, notifierConfig, mediatorConfig, serviceStatus);
        final StreamPersistenceHandlerImpl blockNodeEventHandler = new StreamPersistenceHandlerImpl(
                streamMediator,
                notifier,
                metricsService,
                serviceStatus,
                ackHandlerMock,
                asyncBlockWriterFactoryMock,
                executorMock,
                archiverMock,
                pathResolverMock,
                persistenceStorageConfig);
        final StreamVerificationHandlerImpl streamVerificationHandler = new StreamVerificationHandlerImpl(
                streamMediator, notifier, metricsService, serviceStatus, mock(BlockVerificationService.class));
        final PbjBlockStreamServiceProxy pbjBlockStreamServiceProxy = new PbjBlockStreamServiceProxy(
                streamMediator,
                serviceStatus,
                blockNodeEventHandler,
                streamVerificationHandler,
                blockReaderMock,
                notifier,
                metricsService,
                consumerConfig,
                producerConfig);

        // Register a producer
        final Pipeline<? super Bytes> producerPipeline = pbjBlockStreamServiceProxy.open(
                PbjBlockStreamService.BlockStreamMethod.publishBlockStream, optionsMock, helidonPublishStreamObserver1);

        // Register 3 consumers - Opening a pipeline is not enough to register a consumer.
        // pipeline.onNext() must be invoked to register the consumer at the Helidon PBJ layer.
        pbjBlockStreamServiceProxy
                .open(
                        PbjBlockStreamService.BlockStreamMethod.subscribeBlockStream,
                        optionsMock,
                        subscribeStreamObserver1)
                .onNext(buildLiveStreamSubscribeStreamRequest());
        pbjBlockStreamServiceProxy
                .open(
                        PbjBlockStreamService.BlockStreamMethod.subscribeBlockStream,
                        optionsMock,
                        subscribeStreamObserver2)
                .onNext(buildLiveStreamSubscribeStreamRequest());
        pbjBlockStreamServiceProxy
                .open(
                        PbjBlockStreamService.BlockStreamMethod.subscribeBlockStream,
                        optionsMock,
                        subscribeStreamObserver3)
                .onNext(buildLiveStreamSubscribeStreamRequest());

        Thread.sleep(POLLER_SUBSCRIBER_SLEEP);

        // 3 subscribers + 2 static handlers
        assertEquals(5, pollConsumers.size() + consumers.size());

        // Transmit a BlockItem
        final Bytes publishStreamRequest =
                PublishStreamRequestUnparsed.PROTOBUF.toBytes(PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(new BlockItemSetUnparsed(blockItems))
                        .build());
        producerPipeline.onNext(publishStreamRequest);

        // Use verify to make sure the serviceStatus.stopRunning() method is called
        // before the next block is transmitted.
        verify(serviceStatus, timeout(testTimeout).times(2)).stopRunning(any());

        // Simulate another producer attempting to connect to the Block Node after the exception.
        // Later, verify they received a response indicating the stream is closed.
        final Pipeline<? super Bytes> expectedNoOpProducerPipeline = pbjBlockStreamServiceProxy.open(
                PbjBlockStreamService.BlockStreamMethod.publishBlockStream, optionsMock, helidonPublishStreamObserver2);

        expectedNoOpProducerPipeline.onNext(publishStreamRequest);

        verify(helidonPublishStreamObserver2, timeout(testTimeout).times(1)).onNext(buildEndOfStreamResponse());

        // Build a request to invoke the singleBlock service
        final SingleBlockRequest singleBlockRequest =
                SingleBlockRequest.newBuilder().blockNumber(1).build();

        final PbjBlockAccessServiceProxy pbjBlockAccessServiceProxy =
                new PbjBlockAccessServiceProxy(serviceStatus, blockReaderMock, metricsService);

        // Simulate a consumer attempting to connect to the Block Node after the exception.
        final SingleBlockResponseUnparsed singleBlockResponse =
                pbjBlockAccessServiceProxy.singleBlock(singleBlockRequest);

        // Build a request to invoke the subscribeBlockStream service
        // Simulate a consumer attempting to connect to the Block Node after the exception.
        pbjBlockStreamServiceProxy
                .open(
                        PbjBlockStreamService.BlockStreamMethod.subscribeBlockStream,
                        optionsMock,
                        subscribeStreamObserver4)
                .onNext(buildLiveStreamSubscribeStreamRequest());

        // The BlockItem expected to pass through since it was published
        // before the IOException was thrown.
        final BlockItemSetUnparsed blockItemSet =
                BlockItemSetUnparsed.newBuilder().blockItems(blockItems).build();

        final Bytes subscribeStreamResponse =
                SubscribeStreamResponseUnparsed.PROTOBUF.toBytes(SubscribeStreamResponseUnparsed.newBuilder()
                        .blockItems(blockItemSet)
                        .build());
        verify(subscribeStreamObserver1, timeout(testTimeout).times(1)).onNext(subscribeStreamResponse);
        verify(subscribeStreamObserver2, timeout(testTimeout).times(1)).onNext(subscribeStreamResponse);
        verify(subscribeStreamObserver3, timeout(testTimeout).times(1)).onNext(subscribeStreamResponse);

        // Adding extra time to allow the service to stop given
        // the built-in delay.
        verify(webServerMock, timeout(testTimeout).times(1)).stop();

        assertEquals(SingleBlockResponseCode.READ_BLOCK_NOT_AVAILABLE, singleBlockResponse.status());

        final Bytes expectedSubscriberStreamNotAvailable =
                SubscribeStreamResponseUnparsed.PROTOBUF.toBytes(SubscribeStreamResponseUnparsed.newBuilder()
                        .status(READ_STREAM_NOT_AVAILABLE)
                        .build());

        verify(subscribeStreamObserver4, timeout(testTimeout).times(1)).onNext(expectedSubscriberStreamNotAvailable);
    }

    private static void verifySubscribeStreamResponse(
            int numberOfBlocks,
            int blockItemsToWait,
            int blockItemsToSkip,
            Pipeline<? super Bytes> pipeline,
            List<BlockItemUnparsed> blockItems,
            boolean verifyWithTimeout) {
        // Each block has 10 BlockItems. Verify all the BlockItems
        // in a given block per iteration.
        for (int block = 0; block < numberOfBlocks; block += 10) {
            if (block < blockItemsToWait || block >= blockItemsToSkip) {
                continue;
            }
            final BlockItemUnparsed headerBlockItem = blockItems.get(block);
            final Bytes headerSubStreamResponse = buildSubscribeStreamResponse(headerBlockItem);
            final BlockItemUnparsed bodyBlockItem = blockItems.get(block + 1);
            final Bytes bodySubStreamResponse = buildSubscribeStreamResponse(bodyBlockItem);
            final BlockItemUnparsed stateProofBlockItem = blockItems.get(block + 9);
            final Bytes stateProofStreamResponse = buildSubscribeStreamResponse(stateProofBlockItem);
            if (verifyWithTimeout) {
                verify(pipeline, timeout(testTimeout).times(1)).onNext(headerSubStreamResponse);
                verify(pipeline, timeout(testTimeout).times(8)).onNext(bodySubStreamResponse);
                verify(pipeline, timeout(testTimeout).times(1)).onNext(stateProofStreamResponse);
            } else {
                verify(pipeline, times(1)).onNext(headerSubStreamResponse);
                verify(pipeline, times(8)).onNext(bodySubStreamResponse);
                verify(pipeline, times(1)).onNext(stateProofStreamResponse);
            }
        }
    }

    private static Bytes buildSubscribeStreamResponse(final BlockItemUnparsed blockItem) {
        return SubscribeStreamResponseUnparsed.PROTOBUF.toBytes(SubscribeStreamResponseUnparsed.newBuilder()
                .blockItems(
                        BlockItemSetUnparsed.newBuilder().blockItems(blockItem).build())
                .build());
    }

    private static Bytes buildEndOfStreamResponse() {
        final EndOfStream endOfStream = EndOfStream.newBuilder()
                .status(PublishStreamResponseCode.STREAM_ITEMS_INTERNAL_ERROR)
                .blockNumber(1L)
                .build();
        return PublishStreamResponse.PROTOBUF.toBytes(
                PublishStreamResponse.newBuilder().status(endOfStream).build());
    }

    private BlockVerificationSessionFactory getBlockVerificationSessionFactory() {
        final SignatureVerifierDummy signatureVerifier = mock(SignatureVerifierDummy.class);
        final ExecutorService executorService = ForkJoinPool.commonPool();
        lenient().when(signatureVerifier.verifySignature(any(), any())).thenReturn(true);
        return new BlockVerificationSessionFactory(
                verificationConfig, metricsService, signatureVerifier, executorService);
    }

    private PbjBlockStreamServiceProxy buildBlockStreamService(
            final BlockReader<BlockUnparsed> blockReader,
            final ExecutorService persistenceExecutor,
            final boolean mockPersistence,
            final long lastAckedBlock)
            throws IOException {
        final BlockRemover blockRemover = mock(BlockRemover.class);
        final ServiceStatus serviceStatus = new ServiceStatusImpl(serviceConfig);
        serviceStatus.setLatestAckedBlock(new BlockInfo(lastAckedBlock));
        final LiveStreamMediator streamMediator =
                buildStreamMediator(new ConcurrentHashMap<>(32), new ConcurrentHashMap<>(32), serviceStatus);
        final Notifier notifier =
                new NotifierImpl(streamMediator, metricsService, notifierConfig, mediatorConfig, serviceStatus);
        final AckHandler blockManager =
                new AckHandlerImpl(notifier, false, serviceStatus, blockRemover, metricsService);
        final BlockVerificationSessionFactory blockVerificationSessionFactory = getBlockVerificationSessionFactory();
        final BlockVerificationService BlockVerificationService =
                new BlockVerificationServiceImpl(metricsService, blockVerificationSessionFactory, blockManager);
        final BlockAsLocalFilePathResolver pathResolver = new BlockAsLocalFilePathResolver(persistenceStorageConfig);
        final AsyncBlockWriterFactory writerFactory;
        if (mockPersistence) {
            writerFactory = new AsyncNoOpWriterFactory(blockManager, metricsService);
        } else {
            final BlockAsLocalFileRemover blockRemoverReal = new BlockAsLocalFileRemover(pathResolver);
            writerFactory = new AsyncBlockAsLocalFileWriterFactory(
                    pathResolver, blockRemoverReal, NoOpCompression.newInstance(), blockManager, metricsService);
        }

        final StreamPersistenceHandlerImpl blockNodeEventHandler = new StreamPersistenceHandlerImpl(
                streamMediator,
                notifier,
                metricsService,
                serviceStatus,
                blockManager,
                writerFactory,
                persistenceExecutor,
                archiverMock,
                mockPersistence ? pathResolverMock : pathResolver,
                persistenceStorageConfig);
        final StreamVerificationHandlerImpl streamVerificationHandler = new StreamVerificationHandlerImpl(
                streamMediator, notifier, metricsService, serviceStatus, BlockVerificationService);
        return new PbjBlockStreamServiceProxy(
                streamMediator,
                serviceStatus,
                blockNodeEventHandler,
                streamVerificationHandler,
                blockReader,
                notifier,
                metricsService,
                consumerConfig,
                producerConfig);
    }

    private LiveStreamMediator buildStreamMediator(
            final Map<
                            BlockNodeEventHandler<ObjectEvent<List<BlockItemUnparsed>>>,
                            BatchEventProcessor<ObjectEvent<List<BlockItemUnparsed>>>>
                    subscribers,
            final Map<StreamManager, EventPoller<ObjectEvent<List<BlockItemUnparsed>>>> pollSubscribers,
            final ServiceStatus serviceStatus) {
        serviceStatus.setWebServer(webServerMock);
        return LiveStreamMediatorBuilder.newBuilder(metricsService, mediatorConfig, serviceStatus)
                .subscribers(subscribers)
                .pollSubscribers(pollSubscribers)
                .build();
    }
}
