// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.throttle;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
import org.hiero.metrics.core.LongMeasurementSnapshot;
import org.hiero.metrics.core.MeasurementSnapshot;
import org.hiero.metrics.core.MetricRegistry;
import org.hiero.metrics.core.MetricRegistrySnapshot;
import org.hiero.metrics.core.MetricSnapshot;
import org.hiero.metrics.core.MetricsExporter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Verifies the throttle metric names and values: no counter carries a `_total` suffix (the
/// exporter appends it), and the in-flight gauges and reaped counter report what the mechanism did.
class ThrottleMetricsTest {
    private static final long NOW = 1_000_000_000L;
    private static final long SECOND = 1_000_000_000L;
    /// The registry prefixes every metric name with the node's metrics namespace.
    private static final String PREFIX = "blocknode_";

    private SumReadingExporter exporter;
    private ClientThrottle throttle;

    @BeforeEach
    void setUp() {
        exporter = new SumReadingExporter();
        final MetricRegistry registry =
                MetricRegistry.builder().setMetricsExporter(exporter).build();
        final ThrottleMetrics metrics = new ThrottleMetrics(registry);
        final GlobalConcurrencyGate gate = new GlobalConcurrencyGate(10);
        metrics.inFlightGaugeFor("svc", "method").accept(gate::inFlight);
        throttle = new ClientThrottle(
                new PerClientThrottleSettings(1_000, 1_000, 5, 10), gate, metrics, "svc", "method", Duration.ofDays(1));
    }

    @Test
    @DisplayName("No throttle metric name ends in _total, since the exporter appends it to counters")
    void metricNamesDoNotCarryTotalSuffix() {
        throttle.tryAdmit("client", NOW);
        throttle.reapExpiredPermits(NOW + 11 * SECOND);

        final Map<String, Long> values = exporter.sums();
        assertTrue(values.containsKey(PREFIX + "throttle_calls"));
        assertTrue(values.containsKey(PREFIX + "throttle_permits_reaped"));
        assertTrue(values.containsKey(PREFIX + "throttle_in_flight"));
        assertTrue(values.containsKey(PREFIX + "throttle_clients_in_flight"));
        assertTrue(values.containsKey(PREFIX + "throttle_client_state_count"));
        assertFalse(
                values.keySet().stream().anyMatch(name -> name.endsWith("_total")),
                "counter names must not include _total, the exporter adds it");
    }

    @Test
    @DisplayName("In-flight gauges and the reaped counter track admitted, held and reaped permits")
    void inFlightGaugesAndReapedCounterTrackPermits() {
        throttle.tryAdmit("client-a", NOW);
        throttle.tryAdmit("client-a", NOW);
        throttle.tryAdmit("client-b", NOW);

        assertEquals(3, exporter.sums().get(PREFIX + "throttle_in_flight"));
        assertEquals(2, exporter.sums().get(PREFIX + "throttle_clients_in_flight"));
        assertEquals(3, exporter.sums().get(PREFIX + "throttle_calls"));

        throttle.reapExpiredPermits(NOW + 11 * SECOND);

        assertEquals(0, exporter.sums().get(PREFIX + "throttle_in_flight"));
        assertEquals(0, exporter.sums().get(PREFIX + "throttle_clients_in_flight"));
        assertEquals(3, exporter.sums().get(PREFIX + "throttle_permits_reaped"));
    }

    /// A test [MetricsExporter] that sums each metric's long measurements across its label sets.
    private static final class SumReadingExporter implements MetricsExporter {
        private Supplier<MetricRegistrySnapshot> snapshotSupplier;

        @Override
        public void setSnapshotSupplier(final Supplier<MetricRegistrySnapshot> snapshotSupplier) {
            this.snapshotSupplier = snapshotSupplier;
        }

        Map<String, Long> sums() {
            final Map<String, Long> sums = new HashMap<>();
            for (final MetricSnapshot snapshot : snapshotSupplier.get()) {
                long sum = 0;
                for (final MeasurementSnapshot measurement : snapshot) {
                    if (measurement instanceof LongMeasurementSnapshot lm) {
                        sum += lm.get();
                    }
                }
                sums.put(snapshot.name(), sum);
            }
            return sums;
        }

        @Override
        public void close() {}
    }
}
