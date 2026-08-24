// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.throttle;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import org.hiero.metrics.core.MetricRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class BlockReadBulkheadTest {
    private MetricRegistry metricRegistry;

    @BeforeEach
    void setUp() {
        metricRegistry = MetricRegistry.builder()
                .setMetricsExporter(new NoOpMetricsExporter())
                .build();
    }

    @Test
    @DisplayName("A permit is acquired when the pool has capacity")
    void tryAcquireSucceedsWhenPermitsAvailable() {
        final BlockReadBulkhead bulkhead = new BlockReadBulkhead(1, metricRegistry);
        assertTrue(bulkhead.tryAcquire());
    }

    @Test
    @DisplayName("A non-blocking acquire fails immediately once the pool is exhausted")
    void tryAcquireFailsWhenExhausted() {
        final BlockReadBulkhead bulkhead = new BlockReadBulkhead(1, metricRegistry);
        assertTrue(bulkhead.tryAcquire());
        assertFalse(bulkhead.tryAcquire());
    }

    @Test
    @DisplayName("Releasing a permit makes it available to a later acquirer")
    void releaseFreesThePermit() {
        final BlockReadBulkhead bulkhead = new BlockReadBulkhead(1, metricRegistry);
        assertTrue(bulkhead.tryAcquire());
        assertFalse(bulkhead.tryAcquire());
        bulkhead.release();
        assertTrue(bulkhead.tryAcquire());
    }

    @Test
    @DisplayName("A bounded-wait acquire succeeds if a permit is released within the wait window")
    void boundedWaitAcquireSucceedsOncePermitIsReleased() throws InterruptedException {
        final BlockReadBulkhead bulkhead = new BlockReadBulkhead(1, metricRegistry);
        assertTrue(bulkhead.tryAcquire());

        final Thread releaser = new Thread(() -> {
            try {
                Thread.sleep(50);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            bulkhead.release();
        });
        releaser.start();

        assertTrue(bulkhead.tryAcquire(Duration.ofSeconds(2)));
        releaser.join();
    }

    @Test
    @DisplayName("A bounded-wait acquire fails once the wait elapses with no permit released")
    void boundedWaitAcquireFailsWhenNoPermitBecomesAvailable() {
        final BlockReadBulkhead bulkhead = new BlockReadBulkhead(1, metricRegistry);
        assertTrue(bulkhead.tryAcquire());
        assertFalse(bulkhead.tryAcquire(Duration.ofMillis(50)));
    }

    @Test
    @DisplayName("A non-positive permit count is rejected at construction")
    void nonPositivePermitCountIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> new BlockReadBulkhead(0, metricRegistry));
        assertThrows(IllegalArgumentException.class, () -> new BlockReadBulkhead(-1, metricRegistry));
    }

    @Test
    @DisplayName("totalPermits() reports the fixed pool size regardless of current usage")
    void totalPermitsIsFixed() {
        final BlockReadBulkhead bulkhead = new BlockReadBulkhead(3, metricRegistry);
        assertTrue(bulkhead.tryAcquire());
        org.junit.jupiter.api.Assertions.assertEquals(3, bulkhead.totalPermits());
    }

    /// A no-op metrics exporter so tests don't need a real metrics backend.
    private static final class NoOpMetricsExporter implements org.hiero.metrics.core.MetricsExporter {
        @Override
        public void setSnapshotSupplier(
                final java.util.function.Supplier<org.hiero.metrics.core.MetricRegistrySnapshot> supplier) {}

        @Override
        public void close() {}
    }
}
