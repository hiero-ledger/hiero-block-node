// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.grpc.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doThrow;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import org.hiero.block.api.protoc.PublishStreamResponse;
import org.hiero.block.simulator.startup.SimulatorStartupData;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class PublishStreamObserverTest {
    @Mock
    private SimulatorStartupData startupDataMock;

    @Test
    void onNext() {
        PublishStreamResponse response = PublishStreamResponse.newBuilder()
                .setAcknowledgement(
                        PublishStreamResponse.BlockAcknowledgement.newBuilder().setBlockNumber(12345L))
                .build();
        AtomicBoolean streamEnabled = new AtomicBoolean(true);
        ArrayDeque<String> lastKnownStatuses = new ArrayDeque<>();
        final int lastKnownStatusesCapacity = 10;
        PublishStreamObserver publishStreamObserver =
                new PublishStreamObserver(startupDataMock, streamEnabled, lastKnownStatuses, lastKnownStatusesCapacity);

        publishStreamObserver.onNext(response);
        assertTrue(streamEnabled.get(), "streamEnabled should remain true after onCompleted");
        assertEquals(1, lastKnownStatuses.size(), "lastKnownStatuses should have one element after onNext");
    }

    @Test
    void onError() {
        AtomicBoolean streamEnabled = new AtomicBoolean(true);
        ArrayDeque<String> lastKnownStatuses = new ArrayDeque<>();
        final int lastKnownStatusesCapacity = 10;
        PublishStreamObserver publishStreamObserver =
                new PublishStreamObserver(startupDataMock, streamEnabled, lastKnownStatuses, lastKnownStatusesCapacity);

        publishStreamObserver.onError(new Throwable());
        assertFalse(streamEnabled.get(), "streamEnabled should be set to false after onError");
        assertEquals(1, lastKnownStatuses.size(), "lastKnownStatuses should have one element after onError");
    }

    @Test
    void onCompleted() {
        AtomicBoolean streamEnabled = new AtomicBoolean(true);
        ArrayDeque<String> lastKnownStatuses = new ArrayDeque<>();
        final int lastKnownStatusesCapacity = 10;
        PublishStreamObserver publishStreamObserver =
                new PublishStreamObserver(startupDataMock, streamEnabled, lastKnownStatuses, lastKnownStatusesCapacity);

        publishStreamObserver.onCompleted();
        assertTrue(streamEnabled.get(), "streamEnabled should remain true after onCompleted");
        assertEquals(0, lastKnownStatuses.size(), "lastKnownStatuses should not have elements after onCompleted");
    }

    @Test
    void verifyUpdateLatestAckBlockStartupDataHandlesIOException() throws Exception {
        PublishStreamResponse response = PublishStreamResponse.newBuilder()
                .setAcknowledgement(
                        PublishStreamResponse.BlockAcknowledgement.newBuilder().setBlockNumber(12345L))
                .build();
        AtomicBoolean streamEnabled = new AtomicBoolean(true);
        ArrayDeque<String> lastKnownStatuses = new ArrayDeque<>();
        final int lastKnownStatusesCapacity = 10;
        PublishStreamObserver publishStreamObserver =
                new PublishStreamObserver(startupDataMock, streamEnabled, lastKnownStatuses, lastKnownStatusesCapacity);

        doThrow(new IOException("Test exception")).when(startupDataMock).updateLatestAckBlockStartupData(anyLong());

        assertThrows(UncheckedIOException.class, () -> publishStreamObserver.onNext(response));
    }
}
