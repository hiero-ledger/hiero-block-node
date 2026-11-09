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

/// Registers the throttle mechanism's call-outcome, in-flight, client-state and reaper metrics exactly once,
/// shared by every [ClientThrottle] and [GlobalConcurrencyGate] instance the node creates. See
/// `docs/design/apis/api-throttling.md`'s Metrics section.
///
/// Every call outcome is recorded against one counter, `throttle_calls`, labeled by
/// `service`, `method`, and `outcome`, rather than a separate counter per outcome.
///
/// Must be constructed exactly once per [MetricRegistry], shared across every throttled service
/// registration, since registering the same metric name twice throws.
public final class ThrottleMetrics {
    static final String LABEL_SERVICE = "service";
    static final String LABEL_METHOD = "method";
    static final String LABEL_OUTCOME = "outcome";

    private final LongCounter callsCounter;
    private final LongCounter permitsReapedCounter;
    private final ObservableGauge clientStateCountGauge;
    private final ObservableGauge inFlightGauge;
    private final ObservableGauge clientsInFlightGauge;

    /// @param metricRegistry the registry to register this mechanism's shared metrics with
    public ThrottleMetrics(@NonNull final MetricRegistry metricRegistry) {
        callsCounter = metricRegistry.register(LongCounter.builder(
                        MetricKey.of("throttle_calls", LongCounter.class).addCategory(BlockNodePlugin.METRICS_CATEGORY))
                .setDescription("Calls to a throttled method, by outcome")
                .addDynamicLabelNames(LABEL_SERVICE, LABEL_METHOD, LABEL_OUTCOME));
        permitsReapedCounter =
                metricRegistry.register(LongCounter.builder(MetricKey.of("throttle_permits_reaped", LongCounter.class)
                                .addCategory(BlockNodePlugin.METRICS_CATEGORY))
                        .setDescription("Concurrency permits force-released after exceeding the permit hold limit")
                        .addDynamicLabelNames(LABEL_SERVICE, LABEL_METHOD));
        inFlightGauge = metricRegistry.register(
                ObservableGauge.builder(MetricKey.of("throttle_in_flight", ObservableGauge.class)
                                .addCategory(BlockNodePlugin.METRICS_CATEGORY))
                        .setDescription("Node-wide in-flight calls holding a concurrency permit, per method")
                        .addDynamicLabelNames(LABEL_SERVICE, LABEL_METHOD));
        clientsInFlightGauge = metricRegistry.register(
                ObservableGauge.builder(MetricKey.of("throttle_clients_in_flight", ObservableGauge.class)
                                .addCategory(BlockNodePlugin.METRICS_CATEGORY))
                        .setDescription("Distinct clients currently holding at least one in-flight call, per method")
                        .addDynamicLabelNames(LABEL_SERVICE, LABEL_METHOD));
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

    /// Registers the node-wide in-flight supplier for one `(service, method)`; see [#gaugeFor].
    ///
    /// @param service the throttled service's name
    /// @param method the method the gate enforces a ceiling for
    /// @return a consumer the caller invokes exactly once, with the gate's in-flight supplier
    @NonNull
    Consumer<LongSupplier> inFlightGaugeFor(@NonNull final String service, @NonNull final String method) {
        return supplier -> inFlightGauge.observe(supplier, LABEL_SERVICE, service, LABEL_METHOD, method);
    }

    /// Registers the distinct-clients-in-flight supplier for one `(service, method)`; see
    /// [#gaugeFor].
    ///
    /// @param service the throttled service's name
    /// @param method the method the owning instance enforces a policy for
    /// @return a consumer the caller invokes exactly once, with its supplier
    @NonNull
    Consumer<LongSupplier> clientsInFlightGaugeFor(@NonNull final String service, @NonNull final String method) {
        return supplier -> clientsInFlightGauge.observe(supplier, LABEL_SERVICE, service, LABEL_METHOD, method);
    }

    /// Records permits force-released by the reaper for one `(service, method)`.
    ///
    /// @param service the throttled service's name
    /// @param method the method the permits belonged to
    /// @param count how many permits were reaped
    void recordReaped(@NonNull final String service, @NonNull final String method, final long count) {
        permitsReapedCounter
                .getOrCreateLabeled(LABEL_SERVICE, service, LABEL_METHOD, method)
                .increment(count);
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
