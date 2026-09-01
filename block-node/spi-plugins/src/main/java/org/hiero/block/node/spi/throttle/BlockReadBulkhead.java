// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.throttle;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.time.Duration;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.metrics.ObservableGauge;
import org.hiero.metrics.core.MetricKey;
import org.hiero.metrics.core.MetricRegistry;

/// Component B from `docs/design/apis/api-throttling.md`: a single, shared, bounded,
/// non-client-keyed pool of permits protecting every read against block storage, regardless of
/// which API or client triggered it. Unlike Component A (the per-client admission decorator), this
/// does not know or care about client identity — it protects the resource itself.
///
/// Two call paths share one instance: `getBlock` ([#tryAcquire()], non-blocking — a single
/// request/response exchange either gets a permit now or is rejected immediately) and a
/// subscriber session catching up on historical blocks ([#tryAcquire(Duration)], a brief bounded
/// wait — a standing session should retry rather than be disconnected over one momentary
/// saturation instant). Callers must call [#release()] exactly once for every successful acquire,
/// typically in a `finally` block.
public final class BlockReadBulkhead {
    private final Semaphore permits;
    private final int totalPermits;

    /// @param totalPermits the fixed size of this bulkhead's permit pool; must be positive
    /// @param metricRegistry the registry to register this instance's metrics with
    public BlockReadBulkhead(final int totalPermits, @NonNull final MetricRegistry metricRegistry) {
        if (totalPermits <= 0) {
            throw new IllegalArgumentException("totalPermits must be positive, was " + totalPermits);
        }
        this.totalPermits = totalPermits;
        this.permits = new Semaphore(totalPermits, false);

        metricRegistry
                .register(ObservableGauge.builder(MetricKey.of("block_read_bulkhead_in_use", ObservableGauge.class)
                                .addCategory(BlockNodePlugin.METRICS_CATEGORY))
                        .setDescription("Permits currently held from the shared block-storage read bulkhead"))
                .observe(() -> (long) (totalPermits - permits.availablePermits()));
        metricRegistry
                .register(ObservableGauge.builder(MetricKey.of("block_read_bulkhead_available", ObservableGauge.class)
                                .addCategory(BlockNodePlugin.METRICS_CATEGORY))
                        .setDescription("Permits currently available in the shared block-storage read bulkhead"))
                .observe(() -> (long) permits.availablePermits());
    }

    /// Attempts to acquire a permit without waiting.
    ///
    /// @return {@code true} if a permit was acquired (the caller must [#release()] it); {@code
    ///     false} if none is currently available
    public boolean tryAcquire() {
        return permits.tryAcquire();
    }

    /// Attempts to acquire a permit, waiting up to {@code maxWait} if none is immediately
    /// available. This wait is purely internal scheduling for already-admitted work — it never
    /// affects any admission decision.
    ///
    /// @param maxWait the maximum time to wait for a permit to become available
    /// @return {@code true} if a permit was acquired (the caller must [#release()] it); {@code
    ///     false} if none became available within {@code maxWait}
    public boolean tryAcquire(@NonNull final Duration maxWait) {
        try {
            return permits.tryAcquire(maxWait.toNanos(), TimeUnit.NANOSECONDS);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /// Releases a permit previously acquired via [#tryAcquire()] or [#tryAcquire(Duration)].
    public void release() {
        permits.release();
    }

    /// The fixed size of this bulkhead's permit pool.
    public int totalPermits() {
        return totalPermits;
    }
}
