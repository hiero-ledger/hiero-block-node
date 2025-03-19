// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.notifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hiero.block.server.metrics.BlockNodeMetricTypes.Gauge.Producers;
import static org.hiero.block.server.util.PbjProtoTestUtils.buildEmptyPublishStreamRequest;
import static org.hiero.block.server.util.PersistTestUtils.PERSISTENCE_STORAGE_ARCHIVE_ROOT_PATH_KEY;
import static org.hiero.block.server.util.PersistTestUtils.PERSISTENCE_STORAGE_LIVE_ROOT_PATH_KEY;
import static org.hiero.block.server.util.PersistTestUtils.PERSISTENCE_STORAGE_UNVERIFIED_ROOT_PATH_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.hedera.hapi.block.Acknowledgement;
import com.hedera.hapi.block.BlockItemUnparsed;
import com.hedera.hapi.block.BlockUnparsed;
import com.hedera.hapi.block.PublishStreamResponse;
import com.hedera.pbj.runtime.grpc.Pipeline;
import com.hedera.pbj.runtime.grpc.ServiceInterface;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.lmax.disruptor.BatchEventProcessor;
import com.swirlds.config.api.Configuration;
import io.helidon.webserver.WebServer;
import java.io.IOException;
import java.nio.file.Path;
import java.security.NoSuchAlgorithmException;
import java.time.InstantSource;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import org.hiero.block.server.ack.AckHandler;
import org.hiero.block.server.consumer.ConsumerConfig;
import org.hiero.block.server.events.BlockNodeEventHandler;
import org.hiero.block.server.events.ObjectEvent;
import org.hiero.block.server.mediator.LiveStreamMediator;
import org.hiero.block.server.mediator.LiveStreamMediatorBuilder;
import org.hiero.block.server.mediator.MediatorConfig;
import org.hiero.block.server.mediator.Publisher;
import org.hiero.block.server.mediator.SubscriptionHandler;
import org.hiero.block.server.metrics.MetricsService;
import org.hiero.block.server.pbj.PbjBlockStreamService;
import org.hiero.block.server.pbj.PbjBlockStreamServiceProxy;
import org.hiero.block.server.persistence.StreamPersistenceHandlerImpl;
import org.hiero.block.server.persistence.storage.PersistenceStorageConfig;
import org.hiero.block.server.persistence.storage.archive.LocalBlockArchiver;
import org.hiero.block.server.persistence.storage.path.BlockPathResolver;
import org.hiero.block.server.persistence.storage.read.BlockReader;
import org.hiero.block.server.persistence.storage.write.AsyncBlockWriterFactory;
import org.hiero.block.server.producer.ProducerBlockItemObserver;
import org.hiero.block.server.producer.ProducerConfig;
import org.hiero.block.server.service.ServiceConfig;
import org.hiero.block.server.service.ServiceStatus;
import org.hiero.block.server.service.ServiceStatusImpl;
import org.hiero.block.server.service.WebServerStatus;
import org.hiero.block.server.util.TestConfigUtil;
import org.hiero.block.server.verification.StreamVerificationHandlerImpl;
import org.hiero.block.server.verification.service.BlockVerificationService;
import org.hiero.block.server.verification.service.NoOpBlockVerificationService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class NotifierImplTest {
    private static final int TEST_TIMEOUT = 1000;

    @Mock
    private Notifiable mediator;

    @Mock
    private Publisher<List<BlockItemUnparsed>> publisher;

    @Mock
    private ServiceStatus serviceStatus;

    @Mock
    private WebServerStatus webServerStatus;

    @Mock
    private SubscriptionHandler<PublishStreamResponse> subscriptionHandler;

    @Mock
    private Pipeline<? super Bytes> helidonPublishStreamObserver1;

    @Mock
    private Pipeline<? super Bytes> helidonPublishStreamObserver2;

    @Mock
    private Pipeline<? super Bytes> helidonPublishStreamObserver3;

    @Mock
    private Pipeline<? super PublishStreamResponse> publishStreamObserver1;

    @Mock
    private Pipeline<? super PublishStreamResponse> publishStreamObserver2;

    @Mock
    private Pipeline<? super PublishStreamResponse> publishStreamObserver3;

    @Mock
    private InstantSource testClock;

    @Mock
    private WebServer webServer;

    @Mock
    private ServiceInterface.RequestOptions options;

    @Mock
    private AckHandler ackHandler;

    @Mock
    private BlockReader<BlockUnparsed> blockReader;

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
    private PersistenceStorageConfig persistenceStorageConfig;
    private NotifierConfig notifierConfig;
    private ConsumerConfig consumerConfig;
    private ProducerConfig producerConfig;
    private ServiceConfig serviceConfig;
    private MediatorConfig mediatorConfig;

    @BeforeEach
    void setUp() throws IOException {
        final Map<String, String> properties = new HashMap<>();
        final Path testLiveRootPath = testTempDir.resolve("live");
        final Path testArchiveRootPath = testTempDir.resolve("archive");
        final Path testUnverifiedRootPath = testTempDir.resolve("unverified");
        properties.put(PERSISTENCE_STORAGE_LIVE_ROOT_PATH_KEY, testLiveRootPath.toString());
        properties.put(PERSISTENCE_STORAGE_ARCHIVE_ROOT_PATH_KEY, testArchiveRootPath.toString());
        properties.put(PERSISTENCE_STORAGE_UNVERIFIED_ROOT_PATH_KEY, testUnverifiedRootPath.toString());
        Configuration configuration = TestConfigUtil.getTestBlockNodeConfiguration(properties);
        persistenceStorageConfig = configuration.getConfigData(PersistenceStorageConfig.class);
        final Path testConfigLiveRootPath = persistenceStorageConfig.liveRootPath();
        assertThat(testConfigLiveRootPath).isEqualTo(testLiveRootPath);
        final Path testConfigArchiveRootPath = persistenceStorageConfig.archiveRootPath();
        assertThat(testConfigArchiveRootPath).isEqualTo(testArchiveRootPath);
        final Path testConfigUnverifiedRootPath = persistenceStorageConfig.unverifiedRootPath();
        assertThat(testConfigUnverifiedRootPath).isEqualTo(testUnverifiedRootPath);

        metricsService = TestConfigUtil.getTestBlockNodeMetricsService(configuration);
        notifierConfig = configuration.getConfigData(NotifierConfig.class);
        consumerConfig = configuration.getConfigData(ConsumerConfig.class);
        producerConfig = configuration.getConfigData(ProducerConfig.class);
        serviceConfig = configuration.getConfigData(ServiceConfig.class);
        mediatorConfig = configuration.getConfigData(MediatorConfig.class);
    }

    @Test
    void testRegistration() throws IOException {
        when(webServerStatus.isRunning()).thenReturn(true);

        final NotifierImpl notifier = new NotifierImpl(
                mediator, metricsService, notifierConfig, mediatorConfig, serviceStatus, webServerStatus);
        final PbjBlockStreamServiceProxy pbjBlockStreamServiceProxy = buildBlockStreamService(notifier);

        // Register 3 producers - Opening a pipeline is not enough to register a producer.
        // pipeline.onNext() must be invoked to register the producer at the Helidon PBJ layer.
        final Pipeline<? super Bytes> producerPipeline1 = pbjBlockStreamServiceProxy.open(
                PbjBlockStreamService.BlockStreamMethod.publishBlockStream, options, helidonPublishStreamObserver1);
        producerPipeline1.onNext(buildEmptyPublishStreamRequest());
        final Pipeline<? super Bytes> producerPipeline2 = pbjBlockStreamServiceProxy.open(
                PbjBlockStreamService.BlockStreamMethod.publishBlockStream, options, helidonPublishStreamObserver2);
        producerPipeline2.onNext(buildEmptyPublishStreamRequest());
        final Pipeline<? super Bytes> producerPipeline3 = pbjBlockStreamServiceProxy.open(
                PbjBlockStreamService.BlockStreamMethod.publishBlockStream, options, helidonPublishStreamObserver3);
        producerPipeline3.onNext(buildEmptyPublishStreamRequest());

        long producers = metricsService.get(Producers).get();
        assertEquals(3, producers, "Expected 3 producers to be registered");

        final Bytes blockHash = Bytes.wrap("1234");
        final long blockNumber = 2L;
        final boolean isDuplicated = false;

        notifier.sendAck(blockNumber, blockHash, isDuplicated);

        final Acknowledgement blockAcknowledgement = notifier.buildAck(blockHash, blockNumber, isDuplicated);

        // Verify once the serviceStatus is not running that we do not publish the responses
        final PublishStreamResponse publishStreamResponse = PublishStreamResponse.newBuilder()
                .acknowledgement(blockAcknowledgement)
                .build();

        // Verify the response was received by all observers
        final Bytes publishStreamResponseBytes = PublishStreamResponse.PROTOBUF.toBytes(publishStreamResponse);

        verify(helidonPublishStreamObserver1, timeout(TEST_TIMEOUT).times(1)).onNext(publishStreamResponseBytes);
        verify(helidonPublishStreamObserver2, timeout(TEST_TIMEOUT).times(1)).onNext(publishStreamResponseBytes);
        verify(helidonPublishStreamObserver3, timeout(TEST_TIMEOUT).times(1)).onNext(publishStreamResponseBytes);

        // Unsubscribe the observers
        producerPipeline1.onComplete();
        producerPipeline2.onComplete();
        producerPipeline3.onComplete();

        producers = metricsService.get(Producers).get();
        assertEquals(0, producers, "Expected 0 producers to be registered");
    }

    @Test
    void testServiceStatusNotRunning() throws NoSuchAlgorithmException {
        // Set the web server status to not running
        when(webServerStatus.isRunning()).thenReturn(false);
        final NotifierImpl notifier = new NotifierImpl(
                mediator, metricsService, notifierConfig, mediatorConfig, serviceStatus, webServerStatus);
        final ProducerBlockItemObserver concreteObserver1 = new ProducerBlockItemObserver(
                testClock,
                publisher,
                subscriptionHandler,
                publishStreamObserver1,
                serviceStatus,
                webServerStatus,
                consumerConfig,
                metricsService);
        final ProducerBlockItemObserver concreteObserver2 = new ProducerBlockItemObserver(
                testClock,
                publisher,
                subscriptionHandler,
                publishStreamObserver2,
                serviceStatus,
                webServerStatus,
                consumerConfig,
                metricsService);
        final ProducerBlockItemObserver concreteObserver3 = new ProducerBlockItemObserver(
                testClock,
                publisher,
                subscriptionHandler,
                publishStreamObserver3,
                serviceStatus,
                webServerStatus,
                consumerConfig,
                metricsService);

        notifier.subscribe(concreteObserver1);
        notifier.subscribe(concreteObserver2);
        notifier.subscribe(concreteObserver3);

        assertTrue(notifier.isSubscribed(concreteObserver1), "Expected the notifier to have observer1 subscribed");
        assertTrue(notifier.isSubscribed(concreteObserver2), "Expected the notifier to have observer2 subscribed");
        assertTrue(notifier.isSubscribed(concreteObserver3), "Expected the notifier to have observer3 subscribed");

        final Bytes blockHash = Bytes.wrap("1234");
        final long blockNumber = 2L;
        final boolean isDuplicated = false;

        notifier.sendAck(blockNumber, blockHash, isDuplicated);

        final Acknowledgement blockAcknowledgement = notifier.buildAck(blockHash, blockNumber, isDuplicated);

        // Verify once the serviceStatus is not running that we do not publish the responses
        final PublishStreamResponse publishStreamResponse = PublishStreamResponse.newBuilder()
                .acknowledgement(blockAcknowledgement)
                .build();

        verify(publishStreamObserver1, timeout(TEST_TIMEOUT).times(0)).onNext(publishStreamResponse);
        verify(publishStreamObserver2, timeout(TEST_TIMEOUT).times(0)).onNext(publishStreamResponse);
        verify(publishStreamObserver3, timeout(TEST_TIMEOUT).times(0)).onNext(publishStreamResponse);
    }

    private PbjBlockStreamServiceProxy buildBlockStreamService(final Notifier notifier) throws IOException {
        final ServiceStatus serviceStatus = new ServiceStatusImpl(serviceConfig);
        final LiveStreamMediator streamMediator =
                buildStreamMediator(new ConcurrentHashMap<>(32), serviceStatus, webServerStatus);
        final StreamPersistenceHandlerImpl blockNodeEventHandler = new StreamPersistenceHandlerImpl(
                streamMediator,
                notifier,
                metricsService,
                serviceStatus,
                webServerStatus,
                ackHandler,
                asyncBlockWriterFactoryMock,
                executorMock,
                archiverMock,
                pathResolverMock,
                persistenceStorageConfig);
        final BlockVerificationService blockVerificationService = new NoOpBlockVerificationService();
        final StreamVerificationHandlerImpl streamVerificationHandler = new StreamVerificationHandlerImpl(
                streamMediator, notifier, metricsService, serviceStatus, webServerStatus, blockVerificationService);
        return new PbjBlockStreamServiceProxy(
                streamMediator,
                serviceStatus,
                webServerStatus,
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
            final ServiceStatus serviceStatus,
            final WebServerStatus webServerStatus) {
        webServerStatus.setWebServer(webServer);
        return LiveStreamMediatorBuilder.newBuilder(metricsService, mediatorConfig, serviceStatus, webServerStatus)
                .subscribers(subscribers)
                .build();
    }
}
