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
/// shared by every [ClientThrottle] and [GlobalConcurrencyGate] instance the node creates. See
/// `docs/design/apis/api-throttling.md`'s Metrics section.
///
/// Every call outcome (admitted, or rejected by one of the three checks) is recorded against one
/// counter, `throttle_calls_total`, labeled by `service`, `method`, and `outcome` — not four
/// separately-named counters, one per outcome.
///
/// One instance of this class is shared across every throttled service registration — it must be
/// constructed exactly once per [MetricRegistry], since registering the same metric name twice
/// throws.
public final class ThrottleMetrics {
    static final String LABEL_SERVICE = "service";
    static final String LABEL_METHOD = "method";
    static final String LABEL_OUTCOME = "outcome";

    private final LongCounter callsCounter;
    private final ObservableGauge clientStateCountGauge;

    /// @param metricRegistry the registry to register this mechanism's shared metrics with
    public ThrottleMetrics(@NonNull final MetricRegistry metricRegistry) {
        callsCounter =
                metricRegistry.register(LongCounter.builder(MetricKey.of("throttle_calls_total", LongCounter.class)
                                .addCategory(BlockNodePlugin.METRICS_CATEGORY))
                        .setDescription("Calls to a throttled method, by outcome")
                        .addDynamicLabelNames(LABEL_SERVICE, LABEL_METHOD, LABEL_OUTCOME));
        clientStateCountGauge = metricRegistry.register(
                ObservableGauge.builder(MetricKey.of("throttle_client_state_count", ObservableGauge.class)
                                .addCategory(BlockNodePlugin.METRICS_CATEGORY))
                        .setDescription("Number of distinct clients currently tracked by the throttle")
                        .addDynamicLabelNames(LABEL_SERVICE, LABEL_METHOD));
    }

    /// Records one call's outcome against the shared counter.
    ///
    /// @param service the throttled service's name, e.g. `delegate.serviceName()`
    /// @param method the specific method this call hit
    /// @param outcome what happened to this call
    /// @return the measurement just incremented; most callers can ignore this
    @NonNull
    LongCounter.Measurement recordCall(
            @NonNull final String service, @NonNull final String method, @NonNull final Outcome outcome) {
        final LongCounter.Measurement measurement = callsCounter.getOrCreateLabeled(
                LABEL_SERVICE,
                service,
                LABEL_METHOD,
                method,
                LABEL_OUTCOME,
                outcome.name().toLowerCase(Locale.ROOT));
        measurement.increment();
        return measurement;
    }

    /// Registers one [ClientThrottle] instance's client-state-table-size supplier against the
    /// shared gauge for its `(service, method)` combination. Called exactly once, at that
    /// instance's construction.
    ///
    /// @param service the throttled service's name
    /// @param method the one method the owning instance enforces a policy for
    /// @return a consumer the caller invokes exactly once, with its client-state-table-size
    ///     supplier
    @NonNull
    Consumer<LongSupplier> gaugeFor(@NonNull final String service, @NonNull final String method) {
        return supplier -> clientStateCountGauge.observe(supplier, LABEL_SERVICE, service, LABEL_METHOD, method);
    }

    /// What happened to one call, for [#recordCall]'s `outcome` label.
    enum Outcome {
        /// The call passed every admission check.
        ADMITTED,
        /// Rejected because the node-wide concurrency ceiling was already reached.
        REJECTED_GLOBAL_CONCURRENCY,
        /// Rejected because this client had already reached its own concurrency ceiling.
        REJECTED_CLIENT_CONCURRENCY,
        /// Rejected because this client was calling faster than its allowed rate.
        REJECTED_RATE
    }
}
