// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.throttle;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.metrics.LongCounter;
import org.hiero.metrics.ObservableGauge;
import org.hiero.metrics.core.MetricKey;
import org.hiero.metrics.core.MetricRegistry;

/// The admission decision and per-client bookkeeping for exactly one (service, weight-class)
/// combination: one GCRA-rate-limited, concurrency-capped, TTL-evicted pool of client state.
/// Shared by [ThrottledServiceInterface] (a service with one policy) and
/// [WeightedThrottledServiceInterface] (a service with one policy per [WeightClass]) so the core
/// admission-decision order and eviction logic exist in exactly one place.
///
/// On every [#tryAdmit] call, in order — the first check that rejects wins, and no later check
/// runs: (1) the node-wide concurrency ceiling, (2) the per-client concurrency ceiling, (3) the
/// GCRA rate check, which is the only state-mutating check and therefore runs last (see
/// `docs/design/apis/api-throttling.md` for why).
final class SingleWeightThrottle implements StaleClientSweepable {
    private final ThrottlePolicy policy;
    private final String description;
    private final long clientStateTtlNanos;
    private final ConcurrentHashMap<String, ClientState> clientStates = new ConcurrentHashMap<>();
    private final AtomicInteger globalInFlight = new AtomicInteger();

    private final LongCounter.Measurement admittedCounter;
    private final LongCounter.Measurement rejectedGlobalConcurrencyCounter;
    private final LongCounter.Measurement rejectedClientConcurrencyCounter;
    private final LongCounter.Measurement rejectedRateCounter;

    /// @param policy the resolved per-client + node-wide policy this instance enforces
    /// @param metricRegistry the registry to register this instance's metrics with
    /// @param metricPrefix a unique-per-instance prefix for this instance's metric names
    /// @param description a human-readable label for this instance, used in rejection reasons and
    ///     metric descriptions (e.g. {@code "BlockAccessService.getBlock (historical)"})
    /// @param clientStateTtl how long a client's state is kept after its last-seen call before it
    ///     becomes eligible for eviction (lazily on next lookup, or via [#sweepStaleClients])
    SingleWeightThrottle(
            @NonNull final ThrottlePolicy policy,
            @NonNull final MetricRegistry metricRegistry,
            @NonNull final String metricPrefix,
            @NonNull final String description,
            @NonNull final Duration clientStateTtl) {
        this.policy = policy;
        this.description = description;
        this.clientStateTtlNanos = clientStateTtl.toNanos();

        admittedCounter = metricRegistry
                .register(LongCounter.builder(MetricKey.of(metricPrefix + "_admitted_total", LongCounter.class)
                                .addCategory(BlockNodePlugin.METRICS_CATEGORY))
                        .setDescription("Calls admitted by the throttle for " + description))
                .getOrCreateNotLabeled();
        rejectedGlobalConcurrencyCounter = metricRegistry
                .register(LongCounter.builder(
                                MetricKey.of(metricPrefix + "_rejected_global_concurrency_total", LongCounter.class)
                                        .addCategory(BlockNodePlugin.METRICS_CATEGORY))
                        .setDescription("Calls rejected by the node-wide concurrency ceiling for " + description))
                .getOrCreateNotLabeled();
        rejectedClientConcurrencyCounter = metricRegistry
                .register(LongCounter.builder(
                                MetricKey.of(metricPrefix + "_rejected_client_concurrency_total", LongCounter.class)
                                        .addCategory(BlockNodePlugin.METRICS_CATEGORY))
                        .setDescription("Calls rejected by the per-client concurrency ceiling for " + description))
                .getOrCreateNotLabeled();
        rejectedRateCounter = metricRegistry
                .register(LongCounter.builder(MetricKey.of(metricPrefix + "_rejected_rate_total", LongCounter.class)
                                .addCategory(BlockNodePlugin.METRICS_CATEGORY))
                        .setDescription("Calls rejected by the per-client rate limit for " + description))
                .getOrCreateNotLabeled();
        metricRegistry
                .register(ObservableGauge.builder(
                                MetricKey.of(metricPrefix + "_client_state_count", ObservableGauge.class)
                                        .addCategory(BlockNodePlugin.METRICS_CATEGORY))
                        .setDescription(
                                "Number of distinct clients currently tracked by the throttle for " + description))
                .observe(clientStates::mappingCount);
    }

    /// Attempts to admit a call from `clientKey`, applying the decision order described in the
    /// class-level documentation.
    ///
    /// @param clientKey the caller's key, from a [ClientKeyExtractor]
    /// @param nowNanos the current time from a monotonic clock (e.g. [System#nanoTime()])
    /// @return the admission result — see [AdmissionResult]
    @NonNull
    AdmissionResult tryAdmit(@NonNull final String clientKey, final long nowNanos) {
        // Atomic per-key compute: either reuse a live/still-fresh entry, or replace a stale,
        // currently-unused one with a fresh limiter. A client that hasn't been seen in a while
        // deserves a clean rate-limit history, not one artificially constrained by ancient calls.
        final ClientState state = clientStates.compute(clientKey, (ignoredKey, existing) -> {
            if (existing != null && !(isStale(existing, nowNanos) && existing.inFlight.get() == 0)) {
                return existing;
            }
            return new ClientState(new GcraLimiter(policy.ratePerSecond(), policy.burstTolerance()));
        });
        state.lastSeenNanos = nowNanos;

        if (globalInFlight.get() >= policy.maxConcurrentGlobal()) {
            rejectedGlobalConcurrencyCounter.increment();
            return AdmissionResult.rejected("node-wide concurrency limit reached for " + description);
        }
        if (state.inFlight.get() >= policy.maxConcurrentPerClient()) {
            rejectedClientConcurrencyCounter.increment();
            return AdmissionResult.rejected("per-client concurrency limit reached for " + description);
        }
        if (!state.limiter.tryAcquire(nowNanos)) {
            rejectedRateCounter.increment();
            return AdmissionResult.rejected("rate limit exceeded for " + description);
        }

        globalInFlight.incrementAndGet();
        state.inFlight.incrementAndGet();
        admittedCounter.increment();

        final AtomicBoolean released = new AtomicBoolean(false);
        final Runnable releasePermit = () -> {
            if (released.compareAndSet(false, true)) {
                globalInFlight.decrementAndGet();
                state.inFlight.decrementAndGet();
            }
        };
        return AdmissionResult.admitted(releasePermit);
    }

    /// {@inheritDoc}
    @Override
    public int sweepStaleClients(final long nowNanos) {
        final AtomicInteger evictedCount = new AtomicInteger();
        clientStates.entrySet().removeIf(entry -> {
            final ClientState state = entry.getValue();
            final boolean evict = isStale(state, nowNanos) && state.inFlight.get() == 0;
            if (evict) {
                evictedCount.incrementAndGet();
            }
            return evict;
        });
        return evictedCount.get();
    }

    private boolean isStale(@NonNull final ClientState state, final long nowNanos) {
        return nowNanos - state.lastSeenNanos >= clientStateTtlNanos;
    }

    /// Per-client state: the client's own rate limiter, its current in-flight call count, and
    /// when it was last seen (for eviction, see [#sweepStaleClients]).
    private static final class ClientState {
        private final GcraLimiter limiter;
        private final AtomicInteger inFlight = new AtomicInteger();
        private volatile long lastSeenNanos;

        private ClientState(final GcraLimiter limiter) {
            this.limiter = limiter;
        }
    }
}
