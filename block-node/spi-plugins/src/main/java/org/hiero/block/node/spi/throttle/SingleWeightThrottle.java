// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.throttle;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/// The admission decision and per-client bookkeeping for exactly one (service, weight-class)
/// combination: one GCRA-rate-limited, concurrency-capped, TTL-evicted pool of client state.
/// Shared by [ThrottledServiceInterface] (a service with one policy) and
/// [WeightedThrottledServiceInterface] (a service with one policy per [WeightClass]) so the core
/// admission-decision order and eviction logic exist in exactly one place.
///
/// This instance's rate bucket, concurrency ceiling, and client-state table are shared across
/// every method of the wrapped service — there is no method dimension in the admission state
/// itself (e.g. `BlockNodeService`'s `serverStatus` and `serverStatusDetail` share one instance
/// today). [#tryAdmit] takes the calling method only to label metrics per method — see
/// [ThrottleMetrics] — not to decide admission differently per method.
///
/// On every [#tryAdmit] call, in order — the first check that rejects wins, and no later check
/// runs: (1) the node-wide concurrency ceiling, (2) the per-client concurrency ceiling, (3) the
/// GCRA rate check, which is the only state-mutating check and therefore runs last (see
/// `docs/design/apis/api-throttling.md` for why).
final class SingleWeightThrottle implements StaleClientSweepable {
    private final ThrottlePolicy policy;
    private final ThrottleMetrics throttleMetrics;
    private final String service;
    private final WeightClass weightClass;
    private final long clientStateTtlNanos;
    private final ConcurrentHashMap<String, ClientState> clientStates = new ConcurrentHashMap<>();
    private final AtomicInteger globalInFlight = new AtomicInteger();

    /// @param policy the resolved per-client + node-wide policy this instance enforces
    /// @param throttleMetrics the shared, once-registered metrics this instance's calls report
    ///     into — see [ThrottleMetrics]
    /// @param service the throttled service's name, used to label this instance's metrics and
    ///     rejection reasons
    /// @param weightClass the weight class this instance enforces a policy for
    /// @param clientStateTtl how long a client's state is kept after its last-seen call before it
    ///     becomes eligible for eviction (lazily on next lookup, or via [#sweepStaleClients])
    SingleWeightThrottle(
            @NonNull final ThrottlePolicy policy,
            @NonNull final ThrottleMetrics throttleMetrics,
            @NonNull final String service,
            @NonNull final WeightClass weightClass,
            @NonNull final Duration clientStateTtl) {
        this.policy = policy;
        this.throttleMetrics = throttleMetrics;
        this.service = service;
        this.weightClass = weightClass;
        this.clientStateTtlNanos = clientStateTtl.toNanos();

        throttleMetrics.gaugeFor(service, weightClass).accept(clientStates::mappingCount);
    }

    /// Attempts to admit a call from `clientKey`, applying the decision order described in the
    /// class-level documentation.
    ///
    /// @param clientKey the caller's key, from a [ClientKeyExtractor]
    /// @param method the specific method this call hit — see the class-level documentation on why
    ///     this labels metrics only, and does not change the admission decision
    /// @param nowNanos the current time from a monotonic clock (e.g. [System#nanoTime()])
    /// @return the admission result — see [AdmissionResult]
    @NonNull
    AdmissionResult tryAdmit(@NonNull final String clientKey, @NonNull final String method, final long nowNanos) {
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

        final String description = service + "." + method;
        if (globalInFlight.get() >= policy.maxConcurrentGlobal()) {
            throttleMetrics.recordCall(
                    service, method, weightClass, ThrottleMetrics.Outcome.REJECTED_GLOBAL_CONCURRENCY);
            return AdmissionResult.rejected("node-wide concurrency limit reached for " + description);
        }
        if (state.inFlight.get() >= policy.maxConcurrentPerClient()) {
            throttleMetrics.recordCall(
                    service, method, weightClass, ThrottleMetrics.Outcome.REJECTED_CLIENT_CONCURRENCY);
            return AdmissionResult.rejected("per-client concurrency limit reached for " + description);
        }
        if (!state.limiter.tryAcquire(nowNanos)) {
            throttleMetrics.recordCall(service, method, weightClass, ThrottleMetrics.Outcome.REJECTED_RATE);
            return AdmissionResult.rejected("rate limit exceeded for " + description);
        }

        globalInFlight.incrementAndGet();
        state.inFlight.incrementAndGet();
        throttleMetrics.recordCall(service, method, weightClass, ThrottleMetrics.Outcome.ADMITTED);

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
