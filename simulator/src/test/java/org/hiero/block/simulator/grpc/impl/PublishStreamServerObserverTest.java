// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.grpc.impl;

import static org.hiero.block.simulator.TestUtils.getTestMetrics;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.hedera.hapi.block.protoc.BlockItemSet;
import com.hedera.hapi.block.protoc.PublishStreamRequest;
import com.hedera.hapi.block.protoc.PublishStreamResponse;
import com.hedera.hapi.block.stream.protoc.BlockItem;
import com.hedera.hapi.block.stream.protoc.BlockProof;
import com.swirlds.config.api.Configuration;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.ArrayDeque;
import org.hiero.block.simulator.TestUtils;
import org.hiero.block.simulator.metrics.MetricsService;
import org.hiero.block.simulator.metrics.MetricsServiceImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class PublishStreamServerObserverTest {

    private StreamObserver<PublishStreamResponse> responseObserver;
    private MetricsService metricsService;
    private ArrayDeque<String> lastKnownStatuses;
    private PublishStreamServerObserver observer;
    private static final int CAPACITY = 10;

    @BeforeEach
    void setUp() throws IOException {
        final Configuration config = TestUtils.getTestConfiguration();
        metricsService = new MetricsServiceImpl(getTestMetrics(config));

        responseObserver = mock(StreamObserver.class);
        lastKnownStatuses = new ArrayDeque<>();
        observer = new PublishStreamServerObserver(responseObserver, metricsService, lastKnownStatuses, CAPACITY);
    }

    @Test
    void testConstructorWithNullArguments() {
        assertThrows(
                NullPointerException.class,
                () -> new PublishStreamServerObserver(null, metricsService, lastKnownStatuses, CAPACITY));
        assertThrows(
                NullPointerException.class,
                () -> new PublishStreamServerObserver(responseObserver, metricsService, null, CAPACITY));
    }

    @Test
    void testOnNextWithBlockProof() {
        // Create a BlockProof
        BlockProof blockProof = BlockProof.newBuilder().setBlock(123L).build();

        // Create a BlockItem with the BlockProof
        BlockItem blockItem = BlockItem.newBuilder().setBlockProof(blockProof).build();

        // Create BlockItemSet
        BlockItemSet blockItemSet =
                BlockItemSet.newBuilder().addBlockItems(blockItem).build();

        // Create PublishStreamRequest
        PublishStreamRequest request =
                PublishStreamRequest.newBuilder().setBlockItems(blockItemSet).build();

        // Call onNext
        observer.onNext(request);

        // Verify the response was sent
        ArgumentCaptor<PublishStreamResponse> responseCaptor = ArgumentCaptor.forClass(PublishStreamResponse.class);
        verify(responseObserver).onNext(responseCaptor.capture());

        // Verify the response contains correct block number
        PublishStreamResponse capturedResponse = responseCaptor.getValue();
        assertEquals(123L, capturedResponse.getAcknowledgement().getBlockAck().getBlockNumber());

        // Verify status was stored
        assertEquals(1, lastKnownStatuses.size());
        assertEquals(request.toString(), lastKnownStatuses.getLast());
    }

    @Test
    void testOnNextWithoutBlockItems() {
        PublishStreamRequest request = PublishStreamRequest.newBuilder().build();
        observer.onNext(request);

        // Verify no response was sent
        verify(responseObserver, never()).onNext(any());

        // Verify status was stored
        assertEquals(1, lastKnownStatuses.size());
        assertEquals(request.toString(), lastKnownStatuses.getLast());
    }

    @Test
    void testStatusHistoryCapacity() {
        PublishStreamRequest request = PublishStreamRequest.newBuilder().build();

        // Fill beyond capacity
        for (int i = 0; i < CAPACITY + 5; i++) {
            observer.onNext(request);
        }

        // Verify size is maintained at capacity
        assertEquals(CAPACITY, lastKnownStatuses.size());
    }

    @Test
    void testOnError() {
        Throwable error = new RuntimeException("Test error");
        observer.onError(error);

        verifyNoInteractions(responseObserver);
    }

    @Test
    void testOnCompleted() {
        observer.onCompleted();
        verify(responseObserver).onCompleted();
    }
}
