// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.grpc.impl;

import static org.hiero.block.simulator.TestUtils.getTestMetrics;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import com.hedera.hapi.block.stream.output.protoc.BlockHeader;
import com.hedera.hapi.block.stream.protoc.BlockItem;
import com.hedera.hapi.block.stream.protoc.BlockProof;
import com.swirlds.config.api.Configuration;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.concurrent.CountDownLatch;
import org.hiero.block.api.protoc.BlockItemSet;
import org.hiero.block.api.protoc.SubscribeStreamResponse;
import org.hiero.block.api.protoc.SubscribeStreamResponse.Code;
import org.hiero.block.simulator.TestUtils;
import org.hiero.block.simulator.metrics.MetricsService;
import org.hiero.block.simulator.metrics.MetricsServiceImpl;
import org.hiero.block.simulator.metrics.SimulatorMetricTypes.Counter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ConsumerStreamObserverTest {

    private MetricsService metricsService;
    private CountDownLatch streamLatch;
    private ArrayDeque<String> lastKnownStatuses;
    private ConsumerStreamObserver observer;
    private int lastKnownStatusesCapacity;

    @BeforeEach
    void setUp() throws IOException {
        Configuration config = TestUtils.getTestConfiguration();

        metricsService = spy(new MetricsServiceImpl(getTestMetrics(config)));
        streamLatch = mock(CountDownLatch.class);
        ArrayDeque<String> lastKnownStatuses = new ArrayDeque<>();
        lastKnownStatusesCapacity = 10;

        observer =
                new ConsumerStreamObserver(metricsService, streamLatch, lastKnownStatuses, lastKnownStatusesCapacity);
    }

    @Test
    void testConstructorWithNullArguments() {
        assertThrows(
                NullPointerException.class,
                () -> new ConsumerStreamObserver(null, streamLatch, lastKnownStatuses, lastKnownStatusesCapacity));
        assertThrows(
                NullPointerException.class,
                () -> new ConsumerStreamObserver(metricsService, null, lastKnownStatuses, lastKnownStatusesCapacity));
        assertThrows(
                NullPointerException.class,
                () -> new ConsumerStreamObserver(metricsService, streamLatch, null, lastKnownStatusesCapacity));
    }

    @Test
    void testOnNextWithStatusResponse() {
        SubscribeStreamResponse response =
                SubscribeStreamResponse.newBuilder().setStatus(Code.SUCCESS).build();

        observer.onNext(response);

        verifyNoInteractions(metricsService);
        verifyNoInteractions(streamLatch);
    }

    @Test
    void testOnNextWithBlockItemsResponse() {
        BlockItem blockItemHeader = BlockItem.newBuilder()
                .setBlockHeader(BlockHeader.newBuilder().setNumber(0).build())
                .build();
        BlockItem blockItemProof = BlockItem.newBuilder()
                .setBlockProof(BlockProof.newBuilder().setBlock(0).build())
                .build();
        BlockItem blockItemProof1 = BlockItem.newBuilder()
                .setBlockProof(BlockProof.newBuilder().setBlock(1).build())
                .build();

        BlockItemSet blockItemsSet = BlockItemSet.newBuilder()
                .addBlockItems(blockItemHeader)
                .addBlockItems(blockItemProof)
                .addBlockItems(blockItemProof1)
                .build();

        SubscribeStreamResponse response = SubscribeStreamResponse.newBuilder()
                .setBlockItems(blockItemsSet)
                .build();
        assertEquals(0, metricsService.get(Counter.LiveBlocksConsumed).get());

        observer.onNext(response);

        assertEquals(2, metricsService.get(Counter.LiveBlocksConsumed).get());
        verifyNoInteractions(streamLatch);
    }

    @Test
    void testOnNextWithUnknownResponseType() {
        SubscribeStreamResponse response = SubscribeStreamResponse.newBuilder().build();

        IllegalArgumentException exception =
                assertThrows(IllegalArgumentException.class, () -> observer.onNext(response));

        assertEquals("Unknown response type: RESPONSE_NOT_SET", exception.getMessage());
        verifyNoInteractions(metricsService);
        verifyNoInteractions(streamLatch);
    }

    @Test
    void testOnError() {
        Throwable testException = new RuntimeException("Test exception");

        observer.onError(testException);

        verify(streamLatch).countDown();
        verifyNoInteractions(metricsService);
    }

    @Test
    void testOnCompleted() {
        observer.onCompleted();

        verify(streamLatch).countDown();
        verifyNoInteractions(metricsService);
    }
}
