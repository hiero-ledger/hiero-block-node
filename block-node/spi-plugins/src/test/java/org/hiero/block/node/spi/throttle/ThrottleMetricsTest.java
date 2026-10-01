// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.throttle;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.function.Supplier;
import org.hiero.metrics.core.MetricRegistry;
import org.hiero.metrics.core.MetricRegistrySnapshot;
import org.hiero.metrics.core.MetricsExporter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Verifies that [ThrottleMetrics] binds its shared, once-registered metrics to distinct
/// `(service, method, weightClass)` combinations via labels, instead of each [SingleWeightThrottle]
/// instance baking its own identity into a uniquely-named metric — which gave no way to see a
/// specific method's rejections independent of its service, and broke down entirely for a service
/// like `BlockNodeService` (`serverStatus` / `serverStatusDetail`) that shares one instance across
/// more than one method. See `docs/design/apis/api-throttling.md`'s Metrics section and PR #3637's
/// review thread 2.
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
    @DisplayName("Two different services get independent counters, labeled by service/method/weightClass")
    void differentServicesGetIndependentCounters() {
        final ThrottleMetrics.Counters blockAccess =
                throttleMetrics.countersFor("BlockAccessService", "getBlock", WeightClass.HEAVY);
        final ThrottleMetrics.Counters serverStatus =
                throttleMetrics.countersFor("BlockNodeService", "serverStatus", WeightClass.STANDARD);

        assertNotSame(
                blockAccess.admitted(),
                serverStatus.admitted(),
                "different (service, method, weightClass) combinations must not share a measurement");

        blockAccess.admitted().increment();
        blockAccess.admitted().increment();
        serverStatus.admitted().increment();

        assertEquals(2, blockAccess.admitted().get());
        assertEquals(1, serverStatus.admitted().get(), "incrementing one service's counter must not affect another's");
    }

    @Test
    @DisplayName("Two methods sharing one service/weightClass (e.g. BlockNodeService) still get independent counters")
    void differentMethodsOfSameServiceGetIndependentCounters() {
        // Mirrors BlockNodeService, whose serverStatus and serverStatusDetail methods share one
        // SingleWeightThrottle instance (one rate bucket, one concurrency ceiling) but must still
        // be distinguishable in metrics.
        final ThrottleMetrics.Counters serverStatus =
                throttleMetrics.countersFor("BlockNodeService", "serverStatus", WeightClass.STANDARD);
        final ThrottleMetrics.Counters serverStatusDetail =
                throttleMetrics.countersFor("BlockNodeService", "serverStatusDetail", WeightClass.STANDARD);

        assertNotSame(serverStatus.admitted(), serverStatusDetail.admitted());

        serverStatus.admitted().increment();

        assertEquals(1, serverStatus.admitted().get());
        assertEquals(
                0,
                serverStatusDetail.admitted().get(),
                "an admitted call for one method must not count against a different method sharing the same "
                        + "throttle instance");
    }

    @Test
    @DisplayName("Two weight classes of the same service/method get independent counters")
    void differentWeightClassesOfSameMethodGetIndependentCounters() {
        final ThrottleMetrics.Counters live =
                throttleMetrics.countersFor("BlockAccessService", "getBlock", WeightClass.STANDARD);
        final ThrottleMetrics.Counters historical =
                throttleMetrics.countersFor("BlockAccessService", "getBlock", WeightClass.HEAVY);

        assertNotSame(live.rejectedRate(), historical.rejectedRate());

        live.rejectedRate().increment();

        assertEquals(1, live.rejectedRate().get());
        assertEquals(0, historical.rejectedRate().get(), "a rejection in one weight class must not count in another");
    }

    @Test
    @DisplayName("Asking for the same (service, method, weightClass) combination again reuses the same measurement")
    void sameCombinationReusesTheSameMeasurement() {
        final ThrottleMetrics.Counters first =
                throttleMetrics.countersFor("BlockAccessService", "getBlock", WeightClass.STANDARD);
        final ThrottleMetrics.Counters second =
                throttleMetrics.countersFor("BlockAccessService", "getBlock", WeightClass.STANDARD);

        assertSame(
                first.admitted(),
                second.admitted(),
                "requesting the same labels twice must not attempt to register a duplicate metric, and must "
                        + "share state rather than silently tracking two independent counters");
    }

    @Test
    @DisplayName("The client-state-count gauge is labeled by service/weightClass only, with no method dimension")
    void clientStateGaugeHasNoMethodLabel() {
        // Registering the gauge for the same (service, weightClass) twice is the observable proxy
        // for "this has no method label": a per-method label would let two methods each register
        // their own observer without conflict, but the client-state table is a property of the
        // whole (service, weightClass) instance, so a second registration for the same combination
        // must collide.
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
