// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.bulkhead;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.time.Duration;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.metrics.LongCounter;
import org.hiero.metrics.LongCounter.Measurement;
import org.hiero.metrics.ObservableGauge;
import org.hiero.metrics.core.MetricKey;
import org.hiero.metrics.core.MetricRegistry;

/// A single, shared, bounded pool of permits protecting every read against block storage,
/// regardless of client or API, rather than a per-client admission check.
///
/// Two call paths share one instance: a request/response exchange that gets a permit now or is
/// rejected ([#tryAcquire(Method)]), and a standing session that retries instead of disconnecting over
/// one momentary saturation instant ([#tryAcquire(Method, Duration)]). Callers must call [#release()]
/// exactly once per successful acquire, typically in a `finally` block.
public final class BlockReadBulkhead {
    /// The caller of a bulkhead acquire, used as the `method` label on the requests metric. A closed
    /// set rather than a free-form string so label cardinality stays bounded.
    public enum Method {
        /// The `getBlock` request/response API.
        GET_BLOCK("get_block"),
        /// A subscriber session's historical catch-up read.
        SUBSCRIBER_CATCH_UP("subscriber_catch_up");

        private final String labelValue;

        Method(final String labelValue) {
            this.labelValue = labelValue;
        }

        /// @return the value of the `method` label for this caller
        public String labelValue() {
            return labelValue;
        }
    }

    /// Metric key for permits currently held, labelled by `resource`.
    static final MetricKey<ObservableGauge> METRIC_IN_USE =
            MetricKey.of("bulkhead_read_in_use", ObservableGauge.class).addCategory(BlockNodePlugin.METRICS_CATEGORY);
    /// Metric key for acquire attempts, labelled by `resource`, `method` and `status`.
    static final MetricKey<LongCounter> METRIC_REQUESTS =
            MetricKey.of("bulkhead_read_requests", LongCounter.class).addCategory(BlockNodePlugin.METRICS_CATEGORY);
    static final String LABEL_RESOURCE = "resource";
    static final String LABEL_METHOD = "method";
    static final String LABEL_STATUS = "status";
    /// The `resource` label value for this bulkhead. Other bulkheaded resources may share these
    /// metrics later under their own value.
    static final String RESOURCE_BLOCK = "block";

    static final String STATUS_ACQUIRED = "acquired";
    static final String STATUS_REJECTED = "rejected";

    private final Semaphore permits;
    private final int totalPermits;
    private final Map<Method, Measurement> acquiredCounters = new EnumMap<>(Method.class);
    private final Map<Method, Measurement> rejectedCounters = new EnumMap<>(Method.class);

    /// @param totalPermits the fixed size of this bulkhead's permit pool; must be positive
    /// @param metricRegistry the registry to register this instance's metrics with
    public BlockReadBulkhead(final int totalPermits, @NonNull final MetricRegistry metricRegistry) {
        if (totalPermits <= 0) {
            throw new IllegalArgumentException("totalPermits must be positive, was " + totalPermits);
        }
        this.totalPermits = totalPermits;
        this.permits = new Semaphore(totalPermits, false);

        metricRegistry
                .register(ObservableGauge.builder(METRIC_IN_USE)
                        .setDescription("Permits currently held from a shared read bulkhead, by resource")
                        .addDynamicLabelNames(LABEL_RESOURCE))
                .observe(() -> (long) (totalPermits - permits.availablePermits()), LABEL_RESOURCE, RESOURCE_BLOCK);
        final LongCounter requests = metricRegistry.register(LongCounter.builder(METRIC_REQUESTS)
                .setDescription("Read bulkhead acquire attempts by resource, method (get_block|subscriber_catch_up)"
                        + " and status (acquired|rejected)")
                .addDynamicLabelNames(LABEL_RESOURCE, LABEL_METHOD, LABEL_STATUS));
        // Pre-create every combination so each exports as 0 from startup.
        for (final Method method : Method.values()) {
            acquiredCounters.put(method, requestCounter(requests, method, STATUS_ACQUIRED));
            rejectedCounters.put(method, requestCounter(requests, method, STATUS_REJECTED));
        }
    }

    private static Measurement requestCounter(final LongCounter requests, final Method method, final String status) {
        return requests.getOrCreateLabeled(
                LABEL_RESOURCE, RESOURCE_BLOCK, LABEL_METHOD, method.labelValue(), LABEL_STATUS, status);
    }

    private boolean record(final Method method, final boolean acquired) {
        (acquired ? acquiredCounters : rejectedCounters).get(method).increment();
        return acquired;
    }

    /// Attempts to acquire a permit without waiting.
    ///
    /// @param method the caller, recorded on the requests metric
    /// @return {@code true} if a permit was acquired (the caller must [#release()] it); {@code
    ///     false} if none is currently available
    public boolean tryAcquire(@NonNull final Method method) {
        return record(method, permits.tryAcquire());
    }

    /// Attempts to acquire a permit, waiting up to {@code maxWait} if none is immediately
    /// available. This wait is purely internal scheduling for already-admitted work; it never
    /// affects any admission decision.
    ///
    /// @param method the caller, recorded on the requests metric
    /// @param maxWait the maximum time to wait for a permit to become available
    /// @return {@code true} if a permit was acquired (the caller must [#release()] it); {@code
    ///     false} if none became available within {@code maxWait}
    public boolean tryAcquire(@NonNull final Method method, @NonNull final Duration maxWait) {
        try {
            return record(method, permits.tryAcquire(maxWait.toNanos(), TimeUnit.NANOSECONDS));
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            return record(method, false);
        }
    }

    /// Releases a permit previously acquired via [#tryAcquire(Method)] or [#tryAcquire(Method, Duration)].
    public void release() {
        permits.release();
    }

    /// The fixed size of this bulkhead's permit pool.
    public int totalPermits() {
        return totalPermits;
    }
}
