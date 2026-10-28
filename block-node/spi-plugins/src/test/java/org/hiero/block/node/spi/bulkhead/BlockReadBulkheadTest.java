// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.bulkhead;

import static org.hiero.block.node.spi.bulkhead.BlockReadBulkhead.Method.GET_BLOCK;
import static org.hiero.block.node.spi.bulkhead.BlockReadBulkhead.Method.SUBSCRIBER_CATCH_UP;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.function.Supplier;
import org.hiero.metrics.LongCounter;
import org.hiero.metrics.core.LongMeasurementSnapshot;
import org.hiero.metrics.core.MeasurementSnapshot;
import org.hiero.metrics.core.MetricRegistry;
import org.hiero.metrics.core.MetricRegistrySnapshot;
import org.hiero.metrics.core.MetricSnapshot;
import org.hiero.metrics.core.MetricsExporter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class BlockReadBulkheadTest {
    private CapturingMetricsExporter exporter;
    private MetricRegistry metricRegistry;

    @BeforeEach
    void setUp() {
        exporter = new CapturingMetricsExporter();
        metricRegistry = MetricRegistry.builder().setMetricsExporter(exporter).build();
    }

    @Test
    @DisplayName("A permit is acquired when the pool has capacity")
    void tryAcquireSucceedsWhenPermitsAvailable() {
        final BlockReadBulkhead bulkhead = new BlockReadBulkhead(1, metricRegistry);
        assertTrue(bulkhead.tryAcquire(GET_BLOCK));
    }

    @Test
    @DisplayName("A non-blocking acquire fails immediately once the pool is exhausted")
    void tryAcquireFailsWhenExhausted() {
        final BlockReadBulkhead bulkhead = new BlockReadBulkhead(1, metricRegistry);
        assertTrue(bulkhead.tryAcquire(GET_BLOCK));
        assertFalse(bulkhead.tryAcquire(GET_BLOCK));
    }

    @Test
    @DisplayName("Releasing a permit makes it available to a later acquirer")
    void releaseFreesThePermit() {
        final BlockReadBulkhead bulkhead = new BlockReadBulkhead(1, metricRegistry);
        assertTrue(bulkhead.tryAcquire(GET_BLOCK));
        assertFalse(bulkhead.tryAcquire(GET_BLOCK));
        bulkhead.release();
        assertTrue(bulkhead.tryAcquire(GET_BLOCK));
    }

    @Test
    @DisplayName("A bounded-wait acquire succeeds if a permit is released within the wait window")
    void boundedWaitAcquireSucceedsOncePermitIsReleased() throws InterruptedException {
        final BlockReadBulkhead bulkhead = new BlockReadBulkhead(1, metricRegistry);
        assertTrue(bulkhead.tryAcquire(GET_BLOCK));

        final Thread releaser = new Thread(() -> {
            try {
                Thread.sleep(50);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            bulkhead.release();
        });
        releaser.start();

        assertTrue(bulkhead.tryAcquire(SUBSCRIBER_CATCH_UP, Duration.ofSeconds(2)));
        releaser.join();
    }

    @Test
    @DisplayName("A bounded-wait acquire fails once the wait elapses with no permit released")
    void boundedWaitAcquireFailsWhenNoPermitBecomesAvailable() {
        final BlockReadBulkhead bulkhead = new BlockReadBulkhead(1, metricRegistry);
        assertTrue(bulkhead.tryAcquire(GET_BLOCK));
        assertFalse(bulkhead.tryAcquire(SUBSCRIBER_CATCH_UP, Duration.ofMillis(50)));
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
        assertTrue(bulkhead.tryAcquire(GET_BLOCK));
        assertEquals(3, bulkhead.totalPermits());
    }

    @Test
    @DisplayName("Every method/status request counter exports 0 from construction")
    void requestCountersStartAtZero() {
        new BlockReadBulkhead(1, metricRegistry);
        for (final BlockReadBulkhead.Method method : BlockReadBulkhead.Method.values()) {
            assertEquals(0, requestCount(method, BlockReadBulkhead.STATUS_ACQUIRED));
            assertEquals(0, requestCount(method, BlockReadBulkhead.STATUS_REJECTED));
        }
    }

    @Test
    @DisplayName("Acquired and rejected attempts are counted per method, for both acquire variants")
    void requestCountersTrackOutcomePerMethod() {
        final BlockReadBulkhead bulkhead = new BlockReadBulkhead(1, metricRegistry);
        assertTrue(bulkhead.tryAcquire(GET_BLOCK));
        assertFalse(bulkhead.tryAcquire(GET_BLOCK));
        assertFalse(bulkhead.tryAcquire(SUBSCRIBER_CATCH_UP, Duration.ofMillis(10)));
        bulkhead.release();
        assertTrue(bulkhead.tryAcquire(SUBSCRIBER_CATCH_UP, Duration.ofMillis(10)));

        assertEquals(1, requestCount(GET_BLOCK, BlockReadBulkhead.STATUS_ACQUIRED));
        assertEquals(1, requestCount(GET_BLOCK, BlockReadBulkhead.STATUS_REJECTED));
        assertEquals(1, requestCount(SUBSCRIBER_CATCH_UP, BlockReadBulkhead.STATUS_ACQUIRED));
        assertEquals(1, requestCount(SUBSCRIBER_CATCH_UP, BlockReadBulkhead.STATUS_REJECTED));
    }

    @Test
    @DisplayName("An interrupted bounded-wait acquire is counted as rejected and keeps the interrupt flag")
    void interruptedBoundedWaitIsRejected() {
        final BlockReadBulkhead bulkhead = new BlockReadBulkhead(1, metricRegistry);
        assertTrue(bulkhead.tryAcquire(GET_BLOCK));
        Thread.currentThread().interrupt();
        try {
            assertFalse(bulkhead.tryAcquire(SUBSCRIBER_CATCH_UP, Duration.ofSeconds(5)));
            assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted(); // clear the flag so it can't leak into other tests
        }
        assertEquals(1, requestCount(SUBSCRIBER_CATCH_UP, BlockReadBulkhead.STATUS_REJECTED));
    }

    @Test
    @DisplayName("bulkhead_read_in_use reports held permits under the block resource label")
    void inUseGaugeTracksHeldPermits() {
        final BlockReadBulkhead bulkhead = new BlockReadBulkhead(3, metricRegistry);
        assertEquals(0, inUse());
        assertTrue(bulkhead.tryAcquire(GET_BLOCK));
        assertTrue(bulkhead.tryAcquire(GET_BLOCK));
        assertEquals(2, inUse());
        bulkhead.release();
        assertEquals(1, inUse());
    }

    private long requestCount(final BlockReadBulkhead.Method method, final String status) {
        final LongCounter counter = metricRegistry.getMetric(BlockReadBulkhead.METRIC_REQUESTS);
        return counter.getOrCreateLabeled(
                        BlockReadBulkhead.LABEL_RESOURCE,
                        BlockReadBulkhead.RESOURCE_BLOCK,
                        BlockReadBulkhead.LABEL_METHOD,
                        method.labelValue(),
                        BlockReadBulkhead.LABEL_STATUS,
                        status)
                .get();
    }

    private long inUse() {
        for (final MetricSnapshot metric : exporter.snapshotSupplier.get()) {
            if (metric.name().endsWith(BlockReadBulkhead.METRIC_IN_USE.name())) {
                for (final MeasurementSnapshot measurement : metric) {
                    assertTrue(
                            measurement.getDynamicLabelValues().toString().contains(BlockReadBulkhead.RESOURCE_BLOCK));
                    return ((LongMeasurementSnapshot) measurement).get();
                }
            }
        }
        throw new AssertionError("bulkhead_read_in_use not found");
    }

    /// An exporter that exports nothing but keeps the snapshot supplier so tests can read gauge values.
    private static final class CapturingMetricsExporter implements MetricsExporter {
        private Supplier<MetricRegistrySnapshot> snapshotSupplier;

        @Override
        public void setSnapshotSupplier(final Supplier<MetricRegistrySnapshot> supplier) {
            this.snapshotSupplier = supplier;
        }

        @Override
        public void close() {}
    }
}
