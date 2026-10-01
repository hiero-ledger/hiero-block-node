// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.throttle;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Locale;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.metrics.LongCounter;
import org.hiero.metrics.ObservableGauge;
import org.hiero.metrics.core.MetricKey;
import org.hiero.metrics.core.MetricRegistry;

/// Registers the throttle mechanism's admitted/rejected/client-state-count metrics exactly once,
/// shared by every [SingleWeightThrottle] instance the node creates — one instance per
/// `(service, weight-class)` combination. See `docs/design/apis/api-throttling.md`'s Metrics
/// section.
///
/// Admitted/rejected counters are labeled by `service`, `method`, and `weightClass` — resolved
/// fresh on every call via [#countersFor], using the specific [ServiceInterface.Method] that call
/// hit, since one throttled service can (and, for `BlockNodeService`'s `serverStatus` /
/// `serverStatusDetail`, already does) expose more than one method sharing a single
/// [SingleWeightThrottle] instance's rate bucket and concurrency ceiling. The client-state-count
/// gauge, by contrast, reflects a property of the whole `(service, weight-class)` instance's
/// shared state table — not any one method — so it is labeled by `service` and `weightClass`
/// only, registered once via [#gaugeFor] at the owning instance's construction.
///
/// One instance of this class is shared across every throttled service registration — it must be
/// constructed exactly once per [MetricRegistry], since registering the same metric name twice
/// throws.
public final class ThrottleMetrics {
    static final String LABEL_SERVICE = "service";
    static final String LABEL_METHOD = "method";
    static final String LABEL_WEIGHT_CLASS = "weightClass";

    private final LongCounter admittedCounter;
    private final LongCounter rejectedGlobalConcurrencyCounter;
    private final LongCounter rejectedClientConcurrencyCounter;
    private final LongCounter rejectedRateCounter;
    private final ObservableGauge clientStateCountGauge;

    /// @param metricRegistry the registry to register this mechanism's shared metrics with
    public ThrottleMetrics(@NonNull final MetricRegistry metricRegistry) {
        admittedCounter =
                metricRegistry.register(LongCounter.builder(MetricKey.of("throttle_admitted_total", LongCounter.class)
                                .addCategory(BlockNodePlugin.METRICS_CATEGORY))
                        .setDescription("Calls admitted by the throttle")
                        .addDynamicLabelNames(LABEL_SERVICE, LABEL_METHOD, LABEL_WEIGHT_CLASS));
        rejectedGlobalConcurrencyCounter = metricRegistry.register(
                LongCounter.builder(MetricKey.of("throttle_rejected_global_concurrency_total", LongCounter.class)
                                .addCategory(BlockNodePlugin.METRICS_CATEGORY))
                        .setDescription("Calls rejected by the node-wide concurrency ceiling")
                        .addDynamicLabelNames(LABEL_SERVICE, LABEL_METHOD, LABEL_WEIGHT_CLASS));
        rejectedClientConcurrencyCounter = metricRegistry.register(
                LongCounter.builder(MetricKey.of("throttle_rejected_client_concurrency_total", LongCounter.class)
                                .addCategory(BlockNodePlugin.METRICS_CATEGORY))
                        .setDescription("Calls rejected by the per-client concurrency ceiling")
                        .addDynamicLabelNames(LABEL_SERVICE, LABEL_METHOD, LABEL_WEIGHT_CLASS));
        rejectedRateCounter = metricRegistry.register(
                LongCounter.builder(MetricKey.of("throttle_rejected_rate_total", LongCounter.class)
                                .addCategory(BlockNodePlugin.METRICS_CATEGORY))
                        .setDescription("Calls rejected by the per-client rate limit")
                        .addDynamicLabelNames(LABEL_SERVICE, LABEL_METHOD, LABEL_WEIGHT_CLASS));
        clientStateCountGauge = metricRegistry.register(
                ObservableGauge.builder(MetricKey.of("throttle_client_state_count", ObservableGauge.class)
                                .addCategory(BlockNodePlugin.METRICS_CATEGORY))
                        .setDescription("Number of distinct clients currently tracked by the throttle")
                        .addDynamicLabelNames(LABEL_SERVICE, LABEL_WEIGHT_CLASS));
    }

    /// Resolves this registry's shared admitted/rejected counters for one call, labeled by the
    /// specific method that call hit.
    ///
    /// @param service the throttled service's name, e.g. `delegate.serviceName()`
    /// @param method the specific method this call hit, e.g. `Method#name()` from the call's own
    ///     `open()`/`onNext` — not assumed to be the service's only method
    /// @param weightClass the weight class this call was classified into
    /// @return the four admitted/rejected counter measurements for this `(service, method,
    ///     weightClass)` combination
    @NonNull
    Counters countersFor(
            @NonNull final String service, @NonNull final String method, @NonNull final WeightClass weightClass) {
        final String tier = weightClass.name().toLowerCase(Locale.ROOT);
        return new Counters(
                admittedCounter.getOrCreateLabeled(
                        LABEL_SERVICE, service, LABEL_METHOD, method, LABEL_WEIGHT_CLASS, tier),
                rejectedGlobalConcurrencyCounter.getOrCreateLabeled(
                        LABEL_SERVICE, service, LABEL_METHOD, method, LABEL_WEIGHT_CLASS, tier),
                rejectedClientConcurrencyCounter.getOrCreateLabeled(
                        LABEL_SERVICE, service, LABEL_METHOD, method, LABEL_WEIGHT_CLASS, tier),
                rejectedRateCounter.getOrCreateLabeled(
                        LABEL_SERVICE, service, LABEL_METHOD, method, LABEL_WEIGHT_CLASS, tier));
    }

    /// Registers the owning [SingleWeightThrottle] instance's client-state-table-size supplier
    /// against the shared gauge for this `(service, weightClass)` combination. Called exactly
    /// once, at that instance's construction — unlike [#countersFor], this has no `method` label,
    /// since the client-state table it observes is a property of the whole instance, shared across
    /// every method that instance throttles.
    ///
    /// @param service the throttled service's name
    /// @param weightClass the weight class this instance enforces a policy for
    /// @return a consumer the caller invokes exactly once, with its client-state-table-size
    ///     supplier
    @NonNull
    Consumer<LongSupplier> gaugeFor(@NonNull final String service, @NonNull final WeightClass weightClass) {
        final String tier = weightClass.name().toLowerCase(Locale.ROOT);
        return supplier -> clientStateCountGauge.observe(supplier, LABEL_SERVICE, service, LABEL_WEIGHT_CLASS, tier);
    }

    /// The admitted/rejected measurement handles one call should increment, bound to that call's
    /// `(service, method, weightClass)` labels.
    record Counters(
            @NonNull LongCounter.Measurement admitted,
            @NonNull LongCounter.Measurement rejectedGlobalConcurrency,
            @NonNull LongCounter.Measurement rejectedClientConcurrency,
            @NonNull LongCounter.Measurement rejectedRate) {}
}
