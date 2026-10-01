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

/// Registers the throttle mechanism's call-outcome and client-state-count metrics exactly once,
/// shared by every [SingleWeightThrottle] and [GlobalConcurrencyGate] instance the node creates.
/// See `docs/design/apis/api-throttling.md`'s Metrics section.
///
/// Every call outcome (admitted, or rejected by one of the three checks) is recorded against one
/// counter, `throttle_calls_total`, labeled by `service`, `method`, `weightClass`, and `outcome` —
/// not four separately-named counters, one per outcome. Cardinality is identical either way (the
/// same `service × method × weightClass × outcome` combinations exist regardless of how they're
/// split across metric names); one counter means a query for "total calls" or "rejection rate" for
/// a given service/method sums one metric name rather than enumerating every outcome's own metric
/// name — and stays correct if a new rejection reason is ever added, where a hardcoded list of
/// metric names would not. Labels are resolved fresh on every call via [#recordCall], using the
/// specific [ServiceInterface.Method] that call hit — including for a method with no
/// [SingleWeightThrottle] of its own, admitted only via the shared [GlobalConcurrencyGate].
///
/// The client-state-count gauge reflects one [SingleWeightThrottle] instance's own table size —
/// labeled by `service`, `method`, and `weightClass`, registered once via [#gaugeFor] at that
/// instance's construction. It carries a `method` label (unlike earlier revisions of this class)
/// because a `(service, weightClass)` pair is no longer guaranteed to back exactly one table: two
/// independently-configured methods at the same weight class now get two independent tables, and
/// each needs its own gauge registration to avoid colliding on the same labels.
///
/// One instance of this class is shared across every throttled service registration — it must be
/// constructed exactly once per [MetricRegistry], since registering the same metric name twice
/// throws.
public final class ThrottleMetrics {
    static final String LABEL_SERVICE = "service";
    static final String LABEL_METHOD = "method";
    static final String LABEL_WEIGHT_CLASS = "weightClass";
    static final String LABEL_OUTCOME = "outcome";

    private final LongCounter callsCounter;
    private final ObservableGauge clientStateCountGauge;

    /// @param metricRegistry the registry to register this mechanism's shared metrics with
    public ThrottleMetrics(@NonNull final MetricRegistry metricRegistry) {
        callsCounter =
                metricRegistry.register(LongCounter.builder(MetricKey.of("throttle_calls_total", LongCounter.class)
                                .addCategory(BlockNodePlugin.METRICS_CATEGORY))
                        .setDescription("Calls to a throttled method, by outcome")
                        .addDynamicLabelNames(LABEL_SERVICE, LABEL_METHOD, LABEL_WEIGHT_CLASS, LABEL_OUTCOME));
        clientStateCountGauge = metricRegistry.register(
                ObservableGauge.builder(MetricKey.of("throttle_client_state_count", ObservableGauge.class)
                                .addCategory(BlockNodePlugin.METRICS_CATEGORY))
                        .setDescription("Number of distinct clients currently tracked by the throttle")
                        .addDynamicLabelNames(LABEL_SERVICE, LABEL_METHOD, LABEL_WEIGHT_CLASS));
    }

    /// Records one call's outcome against the shared counter, labeled by the specific method that
    /// call hit.
    ///
    /// @param service the throttled service's name, e.g. `delegate.serviceName()`
    /// @param method the specific method this call hit, e.g. `Method#name()` from the call's own
    ///     `open()`/`onNext` — not assumed to be the service's only method
    /// @param weightClass the weight class this call was classified into
    /// @param outcome what happened to this call
    /// @return the measurement just incremented, for this `(service, method, weightClass,
    ///     outcome)` combination — most callers can ignore this; it exists so tests can assert on
    ///     it without a separate read-back API
    @NonNull
    LongCounter.Measurement recordCall(
            @NonNull final String service,
            @NonNull final String method,
            @NonNull final WeightClass weightClass,
            @NonNull final Outcome outcome) {
        final LongCounter.Measurement measurement = callsCounter.getOrCreateLabeled(
                LABEL_SERVICE,
                service,
                LABEL_METHOD,
                method,
                LABEL_WEIGHT_CLASS,
                weightClass.name().toLowerCase(Locale.ROOT),
                LABEL_OUTCOME,
                outcome.name().toLowerCase(Locale.ROOT));
        measurement.increment();
        return measurement;
    }

    /// Registers one [SingleWeightThrottle] instance's client-state-table-size supplier against
    /// the shared gauge for its `(service, method, weightClass)` combination. Called exactly
    /// once, at that instance's construction.
    ///
    /// @param service the throttled service's name
    /// @param method the one method the owning instance enforces a policy for
    /// @param weightClass the weight class the owning instance enforces a policy for
    /// @return a consumer the caller invokes exactly once, with its client-state-table-size
    ///     supplier
    @NonNull
    Consumer<LongSupplier> gaugeFor(
            @NonNull final String service, @NonNull final String method, @NonNull final WeightClass weightClass) {
        final String tier = weightClass.name().toLowerCase(Locale.ROOT);
        return supplier -> clientStateCountGauge.observe(
                supplier, LABEL_SERVICE, service, LABEL_METHOD, method, LABEL_WEIGHT_CLASS, tier);
    }

    /// What happened to one call, for [#recordCall]'s `outcome` label. [#ADMITTED] is listed
    /// alongside the rejection reasons, not tracked as a separate metric, since it is one more
    /// possible outcome of the same event (a call attempt) that the rejection reasons are.
    enum Outcome {
        /// The call passed every admission check.
        ADMITTED,
        /// Rejected because the node-wide concurrency ceiling for this method/weight class was
        /// already reached.
        REJECTED_GLOBAL_CONCURRENCY,
        /// Rejected because this client had already reached its own concurrency ceiling.
        REJECTED_CLIENT_CONCURRENCY,
        /// Rejected because this client was calling faster than its allowed rate.
        REJECTED_RATE
    }
}
