// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.grpc.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hedera.hapi.block.protoc.PublishStreamResponse;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import org.hiero.block.simulator.config.data.BlockStreamConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PublishStreamObserverTest {
    @TempDir
    private Path testTempDir;

    private BlockStreamConfig blockStreamConfig;

    @BeforeEach
    void setUp() {
        blockStreamConfig = BlockStreamConfig.builder()
                .latestAckBlockNumberPath(testTempDir.resolve("latestAckBlockNumber"))
                .latestAckBlockHashPath(testTempDir.resolve("latestAckBlockHash"))
                .build();
    }

    @Test
    void onNext() {
        PublishStreamResponse response = PublishStreamResponse.newBuilder().build();
        AtomicBoolean streamEnabled = new AtomicBoolean(true);
        ArrayDeque<String> lastKnownStatuses = new ArrayDeque<>();
        final int lastKnownStatusesCapacity = 10;
        PublishStreamObserver publishStreamObserver = new PublishStreamObserver(
                blockStreamConfig, streamEnabled, lastKnownStatuses, lastKnownStatusesCapacity);

        publishStreamObserver.onNext(response);
        assertTrue(streamEnabled.get(), "streamEnabled should remain true after onCompleted");
        assertEquals(1, lastKnownStatuses.size(), "lastKnownStatuses should have one element after onNext");
    }

    @Test
    void onError() {
        AtomicBoolean streamEnabled = new AtomicBoolean(true);
        ArrayDeque<String> lastKnownStatuses = new ArrayDeque<>();
        final int lastKnownStatusesCapacity = 10;
        PublishStreamObserver publishStreamObserver = new PublishStreamObserver(
                blockStreamConfig, streamEnabled, lastKnownStatuses, lastKnownStatusesCapacity);

        publishStreamObserver.onError(new Throwable());
        assertFalse(streamEnabled.get(), "streamEnabled should be set to false after onError");
        assertEquals(1, lastKnownStatuses.size(), "lastKnownStatuses should have one element after onError");
    }

    @Test
    void onCompleted() {
        AtomicBoolean streamEnabled = new AtomicBoolean(true);
        ArrayDeque<String> lastKnownStatuses = new ArrayDeque<>();
        final int lastKnownStatusesCapacity = 10;
        PublishStreamObserver publishStreamObserver = new PublishStreamObserver(
                blockStreamConfig, streamEnabled, lastKnownStatuses, lastKnownStatusesCapacity);

        publishStreamObserver.onCompleted();
        assertTrue(streamEnabled.get(), "streamEnabled should remain true after onCompleted");
        assertEquals(0, lastKnownStatuses.size(), "lastKnownStatuses should not have elements after onCompleted");
    }
}
