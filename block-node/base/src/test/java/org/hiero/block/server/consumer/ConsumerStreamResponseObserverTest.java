// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.consumer;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.hedera.hapi.block.BlockItemSetUnparsed;
import com.hedera.hapi.block.BlockItemUnparsed;
import com.hedera.hapi.block.BlockUnparsed;
import com.hedera.hapi.block.SubscribeStreamRequest;
import com.hedera.hapi.block.SubscribeStreamResponseUnparsed;
import com.hedera.hapi.block.stream.BlockProof;
import com.hedera.hapi.block.stream.input.EventHeader;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.pbj.runtime.grpc.Pipeline;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.config.api.Configuration;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.InstantSource;
import java.util.List;
import java.util.Map;
import org.hiero.block.server.mediator.LiveStreamMediator;
import org.hiero.block.server.mediator.LiveStreamMediatorBuilder;
import org.hiero.block.server.mediator.MediatorConfig;
import org.hiero.block.server.metrics.MetricsService;
import org.hiero.block.server.persistence.storage.read.BlockReader;
import org.hiero.block.server.service.ServiceStatus;
import org.hiero.block.server.service.WebServerStatus;
import org.hiero.block.server.util.TestConfigUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class ConsumerStreamResponseObserverTest {

    private final long TIMEOUT_THRESHOLD_MILLIS = 50L;
    private final long TEST_TIME = 1_719_427_664_950L;

    private static final int testTimeout = 1000;

    @Mock
    private Pipeline<? super SubscribeStreamResponseUnparsed> helidonResponseStreamObserver;

    @Mock
    private InstantSource testClock;

    @Mock
    private SubscribeStreamRequest subscribeStreamRequest;

    @Mock
    private ServiceStatus serviceStatus;

    @Mock
    private WebServerStatus webServerStatus;

    @Mock
    private BlockReader<BlockUnparsed> blockReader;

    private MetricsService metricsService;
    private MediatorConfig mediatorConfig;
    private ConsumerConfig consumerConfig;

    @BeforeEach
    public void setup() throws IOException {
        Map<String, String> configMap =
                Map.of(TestConfigUtil.CONSUMER_TIMEOUT_THRESHOLD_KEY, String.valueOf(TIMEOUT_THRESHOLD_MILLIS));
        Configuration config = TestConfigUtil.getTestBlockNodeConfiguration(configMap);
        this.metricsService = TestConfigUtil.getTestBlockNodeMetricsService(config);
        this.mediatorConfig = config.getConfigData(MediatorConfig.class);
        this.consumerConfig = config.getConfigData(ConsumerConfig.class);
    }

    @Test
    public void testProducerTimeoutWithinWindow() {

        final LiveStreamMediator streamMediator = LiveStreamMediatorBuilder.newBuilder(
                        metricsService, mediatorConfig, serviceStatus, webServerStatus)
                .build();
        when(testClock.millis()).thenReturn(TEST_TIME, TEST_TIME + TIMEOUT_THRESHOLD_MILLIS);

        // Mock live streaming
        when(subscribeStreamRequest.startBlockNumber()).thenReturn(0L);
        when(webServerStatus.isRunning()).thenReturn(true);

        final StreamManager streamManager = ConsumerStreamBuilder.buildStreamManager(
                testClock,
                subscribeStreamRequest,
                streamMediator,
                helidonResponseStreamObserver,
                blockReader,
                serviceStatus,
                metricsService,
                consumerConfig);

        // Create a block item to publish
        final BlockHeader blockHeader = BlockHeader.newBuilder().number(1).build();
        final BlockItemUnparsed blockItem = BlockItemUnparsed.newBuilder()
                .blockHeader(BlockHeader.PROTOBUF.toBytes(blockHeader))
                .build();
        List<BlockItemUnparsed> blockItems = List.of(blockItem);

        // Set up the StreamManager to poll for
        // block items
        assertTrue(streamManager.execute());

        // Now publish the block items to the mediator
        streamMediator.publish(blockItems);

        // Call the StreamManager to poll for
        // the block items and send them to the
        // client
        assertTrue(streamManager.execute());

        final BlockItemSetUnparsed blockItemSet =
                BlockItemSetUnparsed.newBuilder().blockItems(blockItems).build();
        final SubscribeStreamResponseUnparsed subscribeStreamResponse = SubscribeStreamResponseUnparsed.newBuilder()
                .blockItems(blockItemSet)
                .build();

        // verify the Helidon observer is called with the next BlockItem
        verify(helidonResponseStreamObserver, timeout(testTimeout)).onNext(subscribeStreamResponse);

        // verify the mediator is NOT called to unsubscribe the observer
        assertTrue(streamMediator.isSubscribed(streamManager));
    }

    @Test
    public void testProducerTimeoutOutsideWindow() {

        final LiveStreamMediator streamMediator = LiveStreamMediatorBuilder.newBuilder(
                        metricsService, mediatorConfig, serviceStatus, webServerStatus)
                .build();
        when(testClock.millis())
                .thenReturn(TEST_TIME, TEST_TIME + TIMEOUT_THRESHOLD_MILLIS, TEST_TIME + TIMEOUT_THRESHOLD_MILLIS + 1);

        // Mock live streaming
        when(subscribeStreamRequest.startBlockNumber()).thenReturn(0L);
        when(webServerStatus.isRunning()).thenReturn(true);

        final StreamManager streamManager = ConsumerStreamBuilder.buildStreamManager(
                testClock,
                subscribeStreamRequest,
                streamMediator,
                helidonResponseStreamObserver,
                blockReader,
                serviceStatus,
                metricsService,
                consumerConfig);

        // Create a block item to publish
        final BlockHeader blockHeader = BlockHeader.newBuilder().number(1).build();
        final BlockItemUnparsed blockItem = BlockItemUnparsed.newBuilder()
                .blockHeader(BlockHeader.PROTOBUF.toBytes(blockHeader))
                .build();
        List<BlockItemUnparsed> blockItems = List.of(blockItem);

        // Set up the StreamManager to poll for
        // block items
        assertTrue(streamManager.execute());

        // Now publish the block items to the mediator
        streamMediator.publish(blockItems);

        // Call the StreamManager to poll for
        // the block items and send them to the
        // client
        assertFalse(streamManager.execute());

        // verify the mediator unsubscribed the observer
        assertFalse(streamMediator.isSubscribed(streamManager));
    }

    @Test
    public void testConsumerNotToSendBeforeBlockHeader() {

        final LiveStreamMediator streamMediator = LiveStreamMediatorBuilder.newBuilder(
                        metricsService, mediatorConfig, serviceStatus, webServerStatus)
                .build();
        when(testClock.millis()).thenReturn(TEST_TIME, TEST_TIME + TIMEOUT_THRESHOLD_MILLIS);

        // Mock live streaming
        when(subscribeStreamRequest.startBlockNumber()).thenReturn(0L);
        when(webServerStatus.isRunning()).thenReturn(true);

        final StreamManager streamManager = ConsumerStreamBuilder.buildStreamManager(
                testClock,
                subscribeStreamRequest,
                streamMediator,
                helidonResponseStreamObserver,
                blockReader,
                serviceStatus,
                metricsService,
                consumerConfig);

        // Set up the StreamManager to poll for
        // block items
        streamManager.execute();

        // Send non-header BlockItems to validate that the observer does not send them
        for (int i = 1; i <= 10; i++) {

            if (i % 2 == 0) {
                final Bytes eventHeader =
                        EventHeader.PROTOBUF.toBytes(EventHeader.newBuilder().build());
                final BlockItemUnparsed blockItem =
                        BlockItemUnparsed.newBuilder().eventHeader(eventHeader).build();

                // Now publish the block items to the mediator
                streamMediator.publish(List.of(blockItem));
            } else {
                final Bytes blockProof = BlockProof.PROTOBUF.toBytes(
                        BlockProof.newBuilder().block(i).build());
                final BlockItemUnparsed blockItem =
                        BlockItemUnparsed.newBuilder().blockProof(blockProof).build();
                streamMediator.publish(List.of(blockItem));
            }

            // Trigger the StreamManager to poll for
            // block items
            streamManager.execute();
        }

        final BlockItemUnparsed blockItem = BlockItemUnparsed.newBuilder().build();
        final BlockItemSetUnparsed blockItemSet =
                BlockItemSetUnparsed.newBuilder().blockItems(blockItem).build();
        final SubscribeStreamResponseUnparsed subscribeStreamResponse = SubscribeStreamResponseUnparsed.newBuilder()
                .blockItems(blockItemSet)
                .build();

        // Confirm that the observer was called with the next BlockItem
        // since we never send a BlockItem with a Header to start the stream.
        verify(helidonResponseStreamObserver, timeout(testTimeout).times(0)).onNext(subscribeStreamResponse);
    }

    @ParameterizedTest
    @ValueSource(classes = {RuntimeException.class, UncheckedIOException.class})
    public void testClientDisconnectWithUncheckedIOException(Class<RuntimeException> runtimeException) {
        final BlockHeader blockHeader = BlockHeader.newBuilder().number(1).build();
        final BlockItemUnparsed blockItem = BlockItemUnparsed.newBuilder()
                .blockHeader(BlockHeader.PROTOBUF.toBytes(blockHeader))
                .build();

        final BlockItemSetUnparsed blockItemSet =
                BlockItemSetUnparsed.newBuilder().blockItems(blockItem).build();
        final SubscribeStreamResponseUnparsed subscribeStreamResponse = SubscribeStreamResponseUnparsed.newBuilder()
                .blockItems(blockItemSet)
                .build();

        doThrow(runtimeException).when(helidonResponseStreamObserver).onNext(subscribeStreamResponse);

        // Create a stream mediator
        final LiveStreamMediator streamMediator = LiveStreamMediatorBuilder.newBuilder(
                        metricsService, mediatorConfig, serviceStatus, webServerStatus)
                .build();
        when(testClock.millis()).thenReturn(TEST_TIME, TEST_TIME + TIMEOUT_THRESHOLD_MILLIS);

        // Mock live streaming
        when(subscribeStreamRequest.startBlockNumber()).thenReturn(0L);
        when(webServerStatus.isRunning()).thenReturn(true);

        final StreamManager streamManager = ConsumerStreamBuilder.buildStreamManager(
                testClock,
                subscribeStreamRequest,
                streamMediator,
                helidonResponseStreamObserver,
                blockReader,
                serviceStatus,
                metricsService,
                consumerConfig);

        // Set up the StreamManager to poll for
        // block items
        streamManager.execute();

        // Now publish the block items to the mediator
        streamMediator.publish(List.of(blockItem));

        // The helidonResponseStreamObserver is expected to throw an UncheckedIOException
        // simulation that the client disconnected.
        streamManager.execute();

        // It's expected that the exception will be caught and the
        // streamManager will be unsubscribed
        assertFalse(streamMediator.isSubscribed(streamManager));
    }
}
