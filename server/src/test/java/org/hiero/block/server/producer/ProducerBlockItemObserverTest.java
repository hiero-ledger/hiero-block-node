// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.producer;

import static org.hiero.block.server.util.PersistTestUtils.generateBlockItemsUnparsed;
import static org.hiero.block.server.util.PersistTestUtils.generateBlockItemsUnparsedForWithBlockNumber;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.hedera.hapi.block.Acknowledgement;
import com.hedera.hapi.block.BlockAcknowledgement;
import com.hedera.hapi.block.BlockItemUnparsed;
import com.hedera.hapi.block.EndOfStream;
import com.hedera.hapi.block.PublishStreamResponse;
import com.hedera.hapi.block.PublishStreamResponseCode;
import com.hedera.pbj.runtime.grpc.Pipeline;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.config.api.Configuration;
import java.io.IOException;
import java.time.InstantSource;
import java.util.List;
import java.util.Map;
import org.hiero.block.server.block.BlockInfo;
import org.hiero.block.server.consumer.ConsumerConfig;
import org.hiero.block.server.events.ObjectEvent;
import org.hiero.block.server.mediator.Publisher;
import org.hiero.block.server.mediator.SubscriptionHandler;
import org.hiero.block.server.metrics.MetricsService;
import org.hiero.block.server.service.ServiceConfig;
import org.hiero.block.server.service.ServiceStatus;
import org.hiero.block.server.service.ServiceStatusImpl;
import org.hiero.block.server.service.WebServerStatus;
import org.hiero.block.server.service.WebServerStatusImpl;
import org.hiero.block.server.util.TestConfigUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class ProducerBlockItemObserverTest {

    @Mock
    private InstantSource testClock;

    @Mock
    private Publisher<List<BlockItemUnparsed>> publisher;

    @Mock
    private SubscriptionHandler<PublishStreamResponse> subscriptionHandler;

    @Mock
    private Pipeline<PublishStreamResponse> helidonPublishPipeline;

    @Mock
    private ServiceStatus serviceStatus;

    @Mock
    private WebServerStatus webServerStatus;

    @Mock
    private ObjectEvent<PublishStreamResponse> objectEvent;

    private final long TEST_TIME = 1_719_427_664_950L;
    private final long TIMEOUT_THRESHOLD_MILLIS = 50L;
    private static final int testTimeout = 1000;

    private MetricsService metricsService;
    private ConsumerConfig consumerConfig;
    private ServiceConfig serviceConfig;

    @BeforeEach
    public void setUp() throws IOException {
        Map<String, String> configMap =
                Map.of(TestConfigUtil.CONSUMER_TIMEOUT_THRESHOLD_KEY, String.valueOf(TIMEOUT_THRESHOLD_MILLIS));
        Configuration config = TestConfigUtil.getTestBlockNodeConfiguration(configMap);
        consumerConfig = config.getConfigData(ConsumerConfig.class);
        serviceConfig = config.getConfigData(ServiceConfig.class);
        metricsService = TestConfigUtil.getTestBlockNodeMetricsService(config);
    }

    @Test
    public void testConfirmOnErrorNotCalled() {

        final ProducerBlockItemObserver producerBlockItemObserver = new ProducerBlockItemObserver(
                testClock,
                publisher,
                subscriptionHandler,
                helidonPublishPipeline,
                serviceStatus,
                webServerStatus,
                consumerConfig,
                metricsService);

        // Confirm that onError will call the handler
        // to unsubscribe but make sure onError is never
        // called on the helidonPublishPipeline.
        // Calling onError() on the helidonPublishPipeline
        // passed by the Helidon PBJ plugin may cause
        // a loop of calls.
        final Throwable t = new Throwable("Test error");
        producerBlockItemObserver.onError(t);
        verify(subscriptionHandler, timeout(testTimeout).times(1)).unsubscribe(any());
        verify(helidonPublishPipeline, never()).onError(t);
    }

    @Test
    public void testOnEventCallsUnsubscribeOnExpiration() {

        when(testClock.millis()).thenReturn(TEST_TIME, TEST_TIME + TIMEOUT_THRESHOLD_MILLIS + 1);
        final ProducerBlockItemObserver producerBlockItemObserver = new ProducerBlockItemObserver(
                testClock,
                publisher,
                subscriptionHandler,
                helidonPublishPipeline,
                serviceStatus,
                webServerStatus,
                consumerConfig,
                metricsService);

        producerBlockItemObserver.onEvent(objectEvent, 0, true);
        producerBlockItemObserver.onEvent(objectEvent, 0, true);

        verify(subscriptionHandler, timeout(testTimeout).times(1)).unsubscribe(producerBlockItemObserver);
    }

    @Test
    public void testOnSubscribe() {

        final ProducerBlockItemObserver producerBlockItemObserver = new ProducerBlockItemObserver(
                testClock,
                publisher,
                subscriptionHandler,
                helidonPublishPipeline,
                serviceStatus,
                webServerStatus,
                consumerConfig,
                metricsService);

        // Currently, our implementation of onSubscribe() is a
        // no-op.
        producerBlockItemObserver.onSubscribe(null);
    }

    @Test
    public void testEmptyBlockItems() {

        final ProducerBlockItemObserver producerBlockItemObserver = new ProducerBlockItemObserver(
                testClock,
                publisher,
                subscriptionHandler,
                helidonPublishPipeline,
                serviceStatus,
                webServerStatus,
                consumerConfig,
                metricsService);

        producerBlockItemObserver.onNext(List.of());
        verify(publisher, never()).publish(any());
    }

    @Test
    public void testOnlyErrorStreamResponseAllowedAfterStatusChange() {

        final ServiceStatus serviceStatus = new ServiceStatusImpl(serviceConfig);

        final WebServerStatus webServerStatus1 = new WebServerStatusImpl(serviceConfig);

        final ProducerBlockItemObserver producerBlockItemObserver = new ProducerBlockItemObserver(
                testClock,
                publisher,
                subscriptionHandler,
                helidonPublishPipeline,
                serviceStatus,
                webServerStatus1,
                consumerConfig,
                metricsService);

        final List<BlockItemUnparsed> blockItems = generateBlockItemsUnparsed(1);

        // Send a request
        producerBlockItemObserver.onNext(blockItems);

        // Change the status of the service
        webServerStatus1.stopRunning(getClass().getName());

        // Send another request
        producerBlockItemObserver.onNext(blockItems);

        // Confirm that closing the observer allowed only 1 response to be sent.
        verify(helidonPublishPipeline, timeout(testTimeout).times(1)).onNext(any());
    }

    @Test
    public void testClientEndStreamReceived() {

        final ProducerBlockItemObserver producerBlockItemObserver = new ProducerBlockItemObserver(
                testClock,
                publisher,
                subscriptionHandler,
                helidonPublishPipeline,
                serviceStatus,
                webServerStatus,
                consumerConfig,
                metricsService);

        producerBlockItemObserver.clientEndStreamReceived();

        // Confirm that the observer was unsubscribed
        verify(subscriptionHandler, timeout(testTimeout).times(1)).unsubscribe(producerBlockItemObserver);
    }

    @Test
    @DisplayName("Test duplicate block items")
    public void testDuplicateBlockReceived() {

        // given
        when(webServerStatus.isRunning()).thenReturn(true);
        long latestAckedBlockNumber = 10L;
        Bytes fakeHash = Bytes.wrap("fake_hash");
        BlockInfo latestAckedBlock = new BlockInfo(latestAckedBlockNumber);
        latestAckedBlock.setBlockHash(fakeHash);
        when(serviceStatus.getLatestAckedBlock()).thenReturn(latestAckedBlock);
        when(serviceStatus.getLatestReceivedBlockNumber()).thenReturn(latestAckedBlockNumber);

        final List<BlockItemUnparsed> blockItems = generateBlockItemsUnparsedForWithBlockNumber(10);
        final ProducerBlockItemObserver producerBlockItemObserver = new ProducerBlockItemObserver(
                testClock,
                publisher,
                subscriptionHandler,
                helidonPublishPipeline,
                serviceStatus,
                webServerStatus,
                consumerConfig,
                metricsService);

        // when
        producerBlockItemObserver.onNext(blockItems);

        // then
        final BlockAcknowledgement blockAcknowledgement = BlockAcknowledgement.newBuilder()
                .blockNumber(10L)
                .blockAlreadyExists(true)
                .blockRootHash(fakeHash)
                .build();

        final Acknowledgement acknowledgement =
                Acknowledgement.newBuilder().blockAck(blockAcknowledgement).build();

        final PublishStreamResponse publishStreamResponse = PublishStreamResponse.newBuilder()
                .acknowledgement(acknowledgement)
                .build();

        // verify helidonPublishPipeline.onNext() is called once with publishStreamResponse
        verify(helidonPublishPipeline, timeout(testTimeout).times(1)).onNext(publishStreamResponse);
        // verify that the duplicate block is not published
        verify(publisher, never()).publish(any());
    }

    @Test
    @DisplayName("Test duplicate block items with null blockHash of ACK variation")
    public void testDuplicateBlockReceived_NullAckHashVariation() {

        // given
        when(webServerStatus.isRunning()).thenReturn(true);
        long latestAckedBlockNumber = 10L;
        BlockInfo latestAckedBlock = new BlockInfo(latestAckedBlockNumber);
        when(serviceStatus.getLatestAckedBlock()).thenReturn(latestAckedBlock);
        when(serviceStatus.getLatestReceivedBlockNumber()).thenReturn(latestAckedBlockNumber);

        final List<BlockItemUnparsed> blockItems = generateBlockItemsUnparsedForWithBlockNumber(10);
        final ProducerBlockItemObserver producerBlockItemObserver = new ProducerBlockItemObserver(
                testClock,
                publisher,
                subscriptionHandler,
                helidonPublishPipeline,
                serviceStatus,
                webServerStatus,
                consumerConfig,
                metricsService);

        // when
        producerBlockItemObserver.onNext(blockItems);

        // then
        final BlockAcknowledgement blockAcknowledgement = BlockAcknowledgement.newBuilder()
                .blockNumber(10L)
                .blockAlreadyExists(true)
                .build();

        final Acknowledgement acknowledgement =
                Acknowledgement.newBuilder().blockAck(blockAcknowledgement).build();

        final PublishStreamResponse publishStreamResponse = PublishStreamResponse.newBuilder()
                .acknowledgement(acknowledgement)
                .build();

        // verify helidonPublishPipeline.onNext() is called once with publishStreamResponse
        verify(helidonPublishPipeline, timeout(testTimeout).times(1)).onNext(publishStreamResponse);
        // verify that the duplicate block is not published
        verify(publisher, never()).publish(any());
    }

    @Test
    @DisplayName("Test duplicate block items with null ACK variation")
    public void testDuplicateBlockReceived_NullAckVariation() {

        // given
        when(webServerStatus.isRunning()).thenReturn(true);
        when(serviceStatus.getLatestAckedBlock()).thenReturn(null);
        when(serviceStatus.getLatestReceivedBlockNumber()).thenReturn(10L);

        final List<BlockItemUnparsed> blockItems = generateBlockItemsUnparsedForWithBlockNumber(10);
        final ProducerBlockItemObserver producerBlockItemObserver = new ProducerBlockItemObserver(
                testClock,
                publisher,
                subscriptionHandler,
                helidonPublishPipeline,
                serviceStatus,
                webServerStatus,
                consumerConfig,
                metricsService);

        // when
        producerBlockItemObserver.onNext(blockItems);

        // then
        final BlockAcknowledgement blockAcknowledgement = BlockAcknowledgement.newBuilder()
                .blockNumber(10L)
                .blockAlreadyExists(true)
                .build();

        final Acknowledgement acknowledgement =
                Acknowledgement.newBuilder().blockAck(blockAcknowledgement).build();

        final PublishStreamResponse publishStreamResponse = PublishStreamResponse.newBuilder()
                .acknowledgement(acknowledgement)
                .build();

        // verify helidonPublishPipeline.onNext() is called once with publishStreamResponse
        verify(helidonPublishPipeline, timeout(testTimeout).times(1)).onNext(publishStreamResponse);
        // verify that the duplicate block is not published
        verify(publisher, never()).publish(any());
    }

    @Test
    @DisplayName("Test duplicate block items with different ACK number variation")
    public void testDuplicateBlockReceived_DiffAckNumberVariation() {

        // given
        when(webServerStatus.isRunning()).thenReturn(true);
        long latestAckedBlockNumber = 10L;
        long latestReceivedBlockNumber = 11L;
        Bytes fakeHash = Bytes.wrap("fake_hash");
        BlockInfo latestAckedBlock = new BlockInfo(latestAckedBlockNumber);
        latestAckedBlock.setBlockHash(fakeHash);
        when(serviceStatus.getLatestAckedBlock()).thenReturn(latestAckedBlock);
        when(serviceStatus.getLatestReceivedBlockNumber()).thenReturn(latestReceivedBlockNumber);

        final List<BlockItemUnparsed> blockItems =
                generateBlockItemsUnparsedForWithBlockNumber(latestReceivedBlockNumber);
        final ProducerBlockItemObserver producerBlockItemObserver = new ProducerBlockItemObserver(
                testClock,
                publisher,
                subscriptionHandler,
                helidonPublishPipeline,
                serviceStatus,
                webServerStatus,
                consumerConfig,
                metricsService);

        // when
        producerBlockItemObserver.onNext(blockItems);

        // then
        final BlockAcknowledgement blockAcknowledgement = BlockAcknowledgement.newBuilder()
                .blockNumber(latestReceivedBlockNumber)
                .blockAlreadyExists(true)
                .build();

        final Acknowledgement acknowledgement =
                Acknowledgement.newBuilder().blockAck(blockAcknowledgement).build();

        final PublishStreamResponse publishStreamResponse = PublishStreamResponse.newBuilder()
                .acknowledgement(acknowledgement)
                .build();

        // verify helidonPublishPipeline.onNext() is called once with publishStreamResponse
        verify(helidonPublishPipeline, timeout(testTimeout).times(1)).onNext(publishStreamResponse);
        // verify that the duplicate block is not published
        verify(publisher, never()).publish(any());
    }

    @Test
    @DisplayName("Test future (ahead of expected) block received")
    public void testFutureBlockReceived() {
        // given
        when(webServerStatus.isRunning()).thenReturn(true);
        long latestAckedBlockNumber = 10L;
        Bytes fakeHash = Bytes.wrap("fake_hash");
        BlockInfo latestAckedBlock = new BlockInfo(latestAckedBlockNumber);
        latestAckedBlock.setBlockHash(fakeHash);
        when(serviceStatus.getLatestAckedBlock()).thenReturn(latestAckedBlock);
        when(serviceStatus.getLatestReceivedBlockNumber()).thenReturn(10L);
        final List<BlockItemUnparsed> blockItems = generateBlockItemsUnparsedForWithBlockNumber(12);
        final ProducerBlockItemObserver producerBlockItemObserver = new ProducerBlockItemObserver(
                testClock,
                publisher,
                subscriptionHandler,
                helidonPublishPipeline,
                serviceStatus,
                webServerStatus,
                consumerConfig,
                metricsService);

        // when
        producerBlockItemObserver.onNext(blockItems);

        // then
        final EndOfStream endOfStream = EndOfStream.newBuilder()
                .status(PublishStreamResponseCode.STREAM_ITEMS_BEHIND)
                .blockNumber(10L)
                .build();
        final PublishStreamResponse publishStreamResponse =
                PublishStreamResponse.newBuilder().status(endOfStream).build();

        // verify helidonPublishPipeline.onNext() is called once with publishStreamResponse
        verify(helidonPublishPipeline, timeout(testTimeout).times(1)).onNext(publishStreamResponse);
        // verify that .onNext() is called only once.
        verify(helidonPublishPipeline, timeout(testTimeout).times(1)).onNext(any());
        // verify that the future block is not published
        verify(publisher, never()).publish(any());
    }
}
