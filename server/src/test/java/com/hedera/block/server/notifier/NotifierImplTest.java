// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.server.notifier;

import static com.hedera.block.server.metrics.BlockNodeMetricTypes.Gauge.Producers;
import static com.hedera.block.server.util.PbjProtoTestUtils.buildEmptyPublishStreamRequest;
import static com.hedera.block.server.util.PersistTestUtils.PERSISTENCE_STORAGE_ARCHIVE_ROOT_PATH_KEY;
import static com.hedera.block.server.util.PersistTestUtils.PERSISTENCE_STORAGE_LIVE_ROOT_PATH_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.hedera.block.server.ack.AckHandler;
import com.hedera.block.server.config.BlockNodeContext;
import com.hedera.block.server.events.BlockNodeEventHandler;
import com.hedera.block.server.events.ObjectEvent;
import com.hedera.block.server.mediator.LiveStreamMediator;
import com.hedera.block.server.mediator.LiveStreamMediatorBuilder;
import com.hedera.block.server.mediator.Publisher;
import com.hedera.block.server.mediator.SubscriptionHandler;
import com.hedera.block.server.pbj.PbjBlockStreamService;
import com.hedera.block.server.pbj.PbjBlockStreamServiceProxy;
import com.hedera.block.server.persistence.StreamPersistenceHandlerImpl;
import com.hedera.block.server.persistence.storage.PersistenceStorageConfig;
import com.hedera.block.server.persistence.storage.archive.LocalBlockArchiver;
import com.hedera.block.server.persistence.storage.read.BlockReader;
import com.hedera.block.server.persistence.storage.write.AsyncBlockWriterFactory;
import com.hedera.block.server.producer.ProducerBlockItemObserver;
import com.hedera.block.server.service.ServiceStatus;
import com.hedera.block.server.service.ServiceStatusImpl;
import com.hedera.block.server.util.TestConfigUtil;
import com.hedera.block.server.verification.StreamVerificationHandlerImpl;
import com.hedera.block.server.verification.service.BlockVerificationService;
import com.hedera.block.server.verification.service.NoOpBlockVerificationService;
import com.hedera.hapi.block.Acknowledgement;
import com.hedera.hapi.block.BlockItemUnparsed;
import com.hedera.hapi.block.BlockUnparsed;
import com.hedera.hapi.block.PublishStreamResponse;
import com.hedera.pbj.runtime.grpc.Pipeline;
import com.hedera.pbj.runtime.grpc.ServiceInterface;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.lmax.disruptor.BatchEventProcessor;
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
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

    @TempDir
    private Path testTempDir;

    private BlockNodeContext blockNodeContext;
    private PersistenceStorageConfig persistenceStorageConfig;

    @BeforeEach
    void setUp() throws IOException {

        final Map<String, String> properties = new HashMap<>();
        properties.put(PERSISTENCE_STORAGE_LIVE_ROOT_PATH_KEY, testTempDir.toString());
        properties.put(PERSISTENCE_STORAGE_ARCHIVE_ROOT_PATH_KEY, testTempDir.toString());
        blockNodeContext = TestConfigUtil.getTestBlockNodeContext(properties);
        persistenceStorageConfig = blockNodeContext.configuration().getConfigData(PersistenceStorageConfig.class);
    }

    @Test
    @Disabled("@todo(751) - adapt this test to the new streaming consumer model")
    void testRegistration() throws IOException {
        when(serviceStatus.isRunning()).thenReturn(true);

        final NotifierImpl notifier = new NotifierImpl(mediator, blockNodeContext, serviceStatus);
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

        long producers = blockNodeContext.metricsService().get(Producers).get();
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

        producers = blockNodeContext.metricsService().get(Producers).get();
        assertEquals(0, producers, "Expected 0 producers to be registered");
    }

    @Test
    void testServiceStatusNotRunning() throws NoSuchAlgorithmException {
        // Set the serviceStatus to not running
        when(serviceStatus.isRunning()).thenReturn(false);
        final NotifierImpl notifier = new NotifierImpl(mediator, blockNodeContext, serviceStatus);
        final ProducerBlockItemObserver concreteObserver1 = new ProducerBlockItemObserver(
                testClock, publisher, subscriptionHandler, publishStreamObserver1, blockNodeContext, serviceStatus);
        final ProducerBlockItemObserver concreteObserver2 = new ProducerBlockItemObserver(
                testClock, publisher, subscriptionHandler, publishStreamObserver2, blockNodeContext, serviceStatus);
        final ProducerBlockItemObserver concreteObserver3 = new ProducerBlockItemObserver(
                testClock, publisher, subscriptionHandler, publishStreamObserver3, blockNodeContext, serviceStatus);

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
        final ServiceStatus serviceStatus = new ServiceStatusImpl(blockNodeContext);
        final LiveStreamMediator streamMediator = buildStreamMediator(new ConcurrentHashMap<>(32), serviceStatus);
        final StreamPersistenceHandlerImpl blockNodeEventHandler = new StreamPersistenceHandlerImpl(
                streamMediator,
                notifier,
                blockNodeContext,
                serviceStatus,
                ackHandler,
                asyncBlockWriterFactoryMock,
                executorMock,
                archiverMock,
                persistenceStorageConfig);
        final BlockVerificationService blockVerificationService = new NoOpBlockVerificationService();
        final StreamVerificationHandlerImpl streamVerificationHandler = new StreamVerificationHandlerImpl(
                streamMediator, notifier, blockNodeContext.metricsService(), serviceStatus, blockVerificationService);
        return new PbjBlockStreamServiceProxy(
                streamMediator,
                serviceStatus,
                blockNodeEventHandler,
                streamVerificationHandler,
                blockReader,
                notifier,
                blockNodeContext);
    }

    private LiveStreamMediator buildStreamMediator(
            final Map<
                            BlockNodeEventHandler<ObjectEvent<List<BlockItemUnparsed>>>,
                            BatchEventProcessor<ObjectEvent<List<BlockItemUnparsed>>>>
                    subscribers,
            final ServiceStatus serviceStatus) {
        serviceStatus.setWebServer(webServer);
        return LiveStreamMediatorBuilder.newBuilder(blockNodeContext, serviceStatus)
                .subscribers(subscribers)
                .build();
    }
}
