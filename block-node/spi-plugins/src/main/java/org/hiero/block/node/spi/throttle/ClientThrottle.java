// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.throttle;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/// Per-client admission decision and state for one `(service, method)` combination: a
/// GCRA-rate-limited, concurrency-capped, TTL-evicted pool of client state, shared by
/// [ThrottledServiceInterface]. The node-wide ceiling lives separately in [GlobalConcurrencyGate].
///
/// [#tryAdmit] checks in order, first rejection wins: global concurrency, per-client concurrency,
/// then the GCRA rate check last since it's the only state-mutating check.
final class ClientThrottle {
    private final PerClientThrottleSettings settings;
    private final GlobalConcurrencyGate globalGate;
    private final ThrottleMetrics throttleMetrics;
    private final String service;
    private final String method;
    private final long clientStateTtlNanos;
    private final long maxPermitHoldNanos;
    private final ConcurrentHashMap<String, ClientState> clientStates = new ConcurrentHashMap<>();
    /// Permits admitted and not yet released. Membership ends on release, so the set is bounded by
    /// in-flight calls, never by traffic; [#reapExpiredPermits] scans it for permits held too long.
    private final Set<Permit> livePermits = ConcurrentHashMap.newKeySet();

    /// @param settings this instance's per-client rate/concurrency settings
    /// @param globalGate the shared node-wide concurrency gate for this service, possibly shared
    ///     with sibling methods on the same service; see [GlobalConcurrencyGate]
    /// @param throttleMetrics the shared, once-registered metrics this instance's calls report
    ///     into, see [ThrottleMetrics]
    /// @param service the throttled service's name, used to label this instance's metrics and
    ///     rejection reasons
    /// @param method the one method this instance enforces a policy for
    /// @param clientStateTtl how long a client's state is kept after its last-seen call before it
    ///     becomes eligible for eviction (lazily on next lookup, or via [#sweepStaleClients])
    ClientThrottle(
            @NonNull final PerClientThrottleSettings settings,
            @NonNull final GlobalConcurrencyGate globalGate,
            @NonNull final ThrottleMetrics throttleMetrics,
            @NonNull final String service,
            @NonNull final String method,
            @NonNull final Duration clientStateTtl) {
        this.settings = settings;
        this.globalGate = globalGate;
        this.throttleMetrics = throttleMetrics;
        this.service = service;
        this.method = method;
        this.clientStateTtlNanos = clientStateTtl.toNanos();
        this.maxPermitHoldNanos =
                Duration.ofSeconds(settings.maxPermitHoldSeconds()).toNanos();

        throttleMetrics.gaugeFor(service, method).accept(clientStates::mappingCount);
        throttleMetrics.clientsInFlightGaugeFor(service, method).accept(this::clientsInFlight);
    }

    /// Attempts to admit a call from `clientKey`, applying the decision order described in the
    /// class-level documentation.
    ///
    /// Like [GlobalConcurrencyGate#tryAdmit], reservation and release happen at two different
    /// points in time this method has no visibility into: the permits are reserved the instant
    /// this method is called, but must stay held for the entire in-flight call. An admitted call is
    /// handed a `Runnable` (wrapped in [AdmissionResult#admitted]) that the caller invokes whenever
    /// the call actually completes, rather than this method releasing the permits itself.
    ///
    /// @param clientKey the caller's key, from a [ClientKeyExtractor]
    /// @param nowNanos the current time from a monotonic clock (e.g. [System#nanoTime()])
    /// @return the admission result, see [AdmissionResult]
    @NonNull
    AdmissionResult tryAdmit(@NonNull final String clientKey, final long nowNanos) {
        // Reuse a live entry or replace a stale, unused one with a fresh limiter. Stamp lastSeenNanos
        // inside this callback so a concurrent sweep can't see an unstamped fresh entry.
        final ClientState state = clientStates.compute(clientKey, (ignoredKey, existing) -> {
            final ClientState current = (existing != null
                            && !(isStale(existing, nowNanos) && existing.inFlight.get() == 0))
                    ? existing
                    : new ClientState(new GcraLimiter(settings.ratePerSecond(), settings.burstTolerance(), nowNanos));
            current.lastSeenNanos = nowNanos;
            return current;
        });

        final String description = service + "." + method;
        // Each check reserves atomically; a later rejection rolls back what this call already reserved.
        if (!globalGate.tryReserve()) {
            throttleMetrics.recordCall(service, method, ThrottleMetrics.Outcome.REJECTED_GLOBAL_CONCURRENCY);
            return AdmissionResult.rejected("node-wide concurrency limit reached for " + description);
        }
        if (!tryReserve(state.inFlight, settings.maxConcurrentPerClient())) {
            // Only roll back the global slot: the client slot isn't held yet at this point, this
            // check is what just failed to reserve it.
            globalGate.release();
            throttleMetrics.recordCall(service, method, ThrottleMetrics.Outcome.REJECTED_CLIENT_CONCURRENCY);
            return AdmissionResult.rejected("per-client concurrency limit reached for " + description);
        }
        if (!state.limiter.tryAcquire(nowNanos)) {
            // Roll back both: the global slot and the client slot were both reserved by the two
            // checks above before this one ran.
            globalGate.release();
            state.inFlight.decrementAndGet();
            throttleMetrics.recordCall(service, method, ThrottleMetrics.Outcome.REJECTED_RATE);
            return AdmissionResult.rejected("rate limit exceeded for " + description);
        }

        throttleMetrics.recordCall(service, method, ThrottleMetrics.Outcome.ADMITTED);
        final Permit permit = new Permit(state, nowNanos);
        livePermits.add(permit);
        return AdmissionResult.admitted(permit::release);
    }

    /// Force-releases every live permit held for at least `maxPermitHoldSeconds`, a no-op when
    /// that setting is `0`. See [StaleClientSweepable#reapExpiredPermits].
    ///
    /// @param nowNanos the current time from a monotonic clock (e.g. [System#nanoTime()])
    /// @return the number of permits force-released
    int reapExpiredPermits(final long nowNanos) {
        if (maxPermitHoldNanos <= 0) {
            return 0;
        }
        int reaped = 0;
        for (final Permit permit : livePermits) {
            if (nowNanos - permit.admittedAtNanos >= maxPermitHoldNanos && permit.release()) {
                reaped++;
            }
        }
        if (reaped > 0) {
            throttleMetrics.recordReaped(service, method, reaped);
        }
        return reaped;
    }

    /// @return this method's configured permit hold limit in seconds, `0` if it never reaps
    int permitHoldSeconds() {
        return settings.maxPermitHoldSeconds();
    }

    /// @return how many distinct clients currently hold at least one in-flight call
    private long clientsInFlight() {
        return clientStates.values().stream()
                .filter(state -> state.inFlight.get() > 0)
                .count();
    }

    /// Atomically reserves a slot on `counter` if below `max`, the per-client equivalent of
    /// [GlobalConcurrencyGate#tryReserve].
    private static boolean tryReserve(@NonNull final AtomicInteger counter, final int max) {
        while (true) {
            final int current = counter.get();
            if (current >= max) {
                return false;
            }
            if (counter.compareAndSet(current, current + 1)) {
                return true;
            }
        }
    }

    /// Removes idle, not-in-flight client-state entries; see [StaleClientSweepable#sweepStaleClients].
    ///
    /// @param nowNanos the current time from a monotonic clock (e.g. [System#nanoTime()])
    /// @return the number of entries evicted
    int sweepStaleClients(final long nowNanos) {
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

    /// One admitted call's hold on the node-wide and per-client concurrency slots. [#release] is
    /// idempotent, so a real completion racing or following a reap releases at most once.
    private final class Permit {
        private final ClientState state;
        private final long admittedAtNanos;
        private final AtomicBoolean released = new AtomicBoolean(false);

        private Permit(final ClientState state, final long admittedAtNanos) {
            this.state = state;
            this.admittedAtNanos = admittedAtNanos;
        }

        /// @return `true` if this call released the permit, `false` if it was already released
        private boolean release() {
            if (!released.compareAndSet(false, true)) {
                return false;
            }
            livePermits.remove(this);
            globalGate.release();
            state.inFlight.decrementAndGet();
            return true;
        }
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
