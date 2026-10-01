// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.throttle;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.function.Supplier;
import org.hiero.metrics.LongCounter;
import org.hiero.metrics.core.MetricRegistry;
import org.hiero.metrics.core.MetricRegistrySnapshot;
import org.hiero.metrics.core.MetricsExporter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Verifies that [ThrottleMetrics] binds its shared, once-registered metrics to distinct
/// `(service, method, weightClass, outcome)` combinations via labels on one counter
/// (`throttle_calls_total`), instead of a separately-named counter per outcome, and that the
/// client-state-count gauge keeps its own narrower label set. See
/// `docs/design/apis/api-throttling.md`'s Metrics section and PR #3637's review thread 2.
class ThrottleMetricsTest {

    private ThrottleMetrics throttleMetrics;

    @BeforeEach
    void setUp() {
        final MetricRegistry metricRegistry = MetricRegistry.builder()
                .setMetricsExporter(new NoOpMetricsExporter())
                .build();
        throttleMetrics = new ThrottleMetrics(metricRegistry);
    }

    @Test
    @DisplayName("Two different services get independent counts under the one shared counter")
    void differentServicesGetIndependentCounts() {
        final LongCounter.Measurement blockAccess = throttleMetrics.recordCall(
                "BlockAccessService", "getBlock", WeightClass.HEAVY, ThrottleMetrics.Outcome.ADMITTED);
        final LongCounter.Measurement serverStatus = throttleMetrics.recordCall(
                "BlockNodeService", "serverStatus", WeightClass.STANDARD, ThrottleMetrics.Outcome.ADMITTED);

        assertNotSame(
                blockAccess,
                serverStatus,
                "different (service, method, weightClass, outcome) combinations must not share a measurement");

        throttleMetrics.recordCall(
                "BlockAccessService", "getBlock", WeightClass.HEAVY, ThrottleMetrics.Outcome.ADMITTED);

        assertEquals(2, blockAccess.get());
        assertEquals(1, serverStatus.get(), "incrementing one service's count must not affect another's");
    }

    @Test
    @DisplayName("Two methods sharing one service/weightClass (e.g. BlockNodeService) still get independent counts")
    void differentMethodsOfSameServiceGetIndependentCounts() {
        // Mirrors BlockNodeService, whose serverStatus and serverStatusDetail methods share one
        // SingleWeightThrottle instance (one rate bucket, one concurrency ceiling) but must still
        // be distinguishable in metrics.
        final LongCounter.Measurement serverStatus = throttleMetrics.recordCall(
                "BlockNodeService", "serverStatus", WeightClass.STANDARD, ThrottleMetrics.Outcome.ADMITTED);
        final LongCounter.Measurement serverStatusDetail = throttleMetrics.recordCall(
                "BlockNodeService", "serverStatusDetail", WeightClass.STANDARD, ThrottleMetrics.Outcome.ADMITTED);

        assertNotSame(serverStatus, serverStatusDetail);
        assertEquals(1, serverStatus.get());
        assertEquals(
                1,
                serverStatusDetail.get(),
                "an admitted call for one method must not count against a different method sharing the same "
                        + "throttle instance");
    }

    @Test
    @DisplayName("Different outcomes for the same (service, method, weightClass) get independent counts")
    void differentOutcomesGetIndependentCounts() {
        final LongCounter.Measurement admitted = throttleMetrics.recordCall(
                "BlockAccessService", "getBlock", WeightClass.STANDARD, ThrottleMetrics.Outcome.ADMITTED);
        final LongCounter.Measurement rejectedRate = throttleMetrics.recordCall(
                "BlockAccessService", "getBlock", WeightClass.STANDARD, ThrottleMetrics.Outcome.REJECTED_RATE);

        assertNotSame(admitted, rejectedRate, "outcome is part of the label set, not folded into one count");

        throttleMetrics.recordCall(
                "BlockAccessService", "getBlock", WeightClass.STANDARD, ThrottleMetrics.Outcome.REJECTED_RATE);

        assertEquals(1, admitted.get());
        assertEquals(2, rejectedRate.get());
    }

    @Test
    @DisplayName("Two weight classes of the same service/method get independent counts")
    void differentWeightClassesOfSameMethodGetIndependentCounts() {
        final LongCounter.Measurement live = throttleMetrics.recordCall(
                "BlockAccessService", "getBlock", WeightClass.STANDARD, ThrottleMetrics.Outcome.REJECTED_RATE);
        final LongCounter.Measurement historical = throttleMetrics.recordCall(
                "BlockAccessService", "getBlock", WeightClass.HEAVY, ThrottleMetrics.Outcome.REJECTED_RATE);

        assertNotSame(live, historical);
        assertEquals(1, live.get());
        assertEquals(1, historical.get(), "a rejection in one weight class must not count in another");
    }

    @Test
    @DisplayName(
            "Asking for the same (service, method, weightClass, outcome) combination again reuses the same measurement")
    void sameCombinationReusesTheSameMeasurement() {
        final LongCounter.Measurement first = throttleMetrics.recordCall(
                "BlockAccessService", "getBlock", WeightClass.STANDARD, ThrottleMetrics.Outcome.ADMITTED);
        final LongCounter.Measurement second = throttleMetrics.recordCall(
                "BlockAccessService", "getBlock", WeightClass.STANDARD, ThrottleMetrics.Outcome.ADMITTED);

        assertSame(
                first,
                second,
                "requesting the same labels twice must not attempt to register a duplicate metric, and must "
                        + "share state rather than silently tracking two independent counts");
        assertEquals(2, first.get());
    }

    @Test
    @DisplayName(
            "The client-state-count gauge is labeled by service/weightClass only, with no method or outcome dimension")
    void clientStateGaugeHasNoMethodOrOutcomeLabel() {
        // Registering the gauge for the same (service, weightClass) twice is the observable proxy
        // for "this has no method/outcome label": a per-method or per-outcome label would let
        // multiple observers register without conflict, but the client-state table is a property
        // of the whole (service, weightClass) instance, so a second registration for the same
        // combination must collide.
        throttleMetrics.gaugeFor("BlockNodeService", WeightClass.STANDARD).accept(() -> 0L);

        assertThrows(
                IllegalArgumentException.class,
                () -> throttleMetrics
                        .gaugeFor("BlockNodeService", WeightClass.STANDARD)
                        .accept(() -> 0L),
                "a second observer for the same (service, weightClass) must collide, since "
                        + "SingleWeightThrottle only ever registers one per instance");
    }

    /// A no-op metrics exporter so tests don't need a real metrics backend.
    private static final class NoOpMetricsExporter implements MetricsExporter {
        @Override
        public void setSnapshotSupplier(final Supplier<MetricRegistrySnapshot> supplier) {}

        @Override
        public void close() {}
    }
}
