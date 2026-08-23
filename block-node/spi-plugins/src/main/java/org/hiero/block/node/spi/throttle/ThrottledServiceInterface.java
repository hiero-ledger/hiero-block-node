// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.throttle;

import com.hedera.pbj.runtime.grpc.GrpcException;
import com.hedera.pbj.runtime.grpc.GrpcStatus;
import com.hedera.pbj.runtime.grpc.Pipeline;
import com.hedera.pbj.runtime.grpc.Pipelines;
import com.hedera.pbj.runtime.grpc.ServiceInterface;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.metrics.LongCounter;
import org.hiero.metrics.ObservableGauge;
import org.hiero.metrics.core.MetricKey;
import org.hiero.metrics.core.MetricRegistry;

/// Wraps a plugin's [ServiceInterface] with per-client rate and concurrency admission control, as
/// described in `docs/design/apis/api-throttling.md`.
///
/// On every call, in order — the first check that rejects wins, and no later check runs:
/// 1. Global concurrency check — is the node-wide concurrency ceiling for this method reached?
/// 2. Per-client concurrency check — has this client reached its own concurrency ceiling?
/// 3. Rate check (GCRA) — is this client calling faster than its allowed rate? This is the only
///    state-mutating check, so it runs last: a call that's going to be rejected by a cheaper check
///    must not be allowed to advance the rate limiter's clock first.
///
/// If admitted, a concurrency permit is held for the lifetime of the call and released exactly
/// once when the call ends. The permit is attached to the *outgoing* `responses` pipeline passed
/// into [ServiceInterface#open], not the pipeline `open()` returns — for a server-streaming call,
/// the client half-closing its request side does not mean the call has finished, so only the
/// outgoing pipeline's completion callbacks reliably fire exactly once when the call actually
/// ends, for every call shape (unary, streaming, or bidi). See the design doc for the full
/// reasoning.
///
/// Per-client state is bounded: an entry idle for at least `clientStateTtl` is evicted lazily the
/// next time that client's key is looked up (see [#open]), and — to catch a client that connects
/// once and is never looked up again — [#sweepStaleClients] provides a backstop a caller is
/// expected to invoke periodically (see `docs/design/apis/api-throttling.md` §5). An entry with a
/// call currently in flight is never evicted, however stale its last-seen time.
public final class ThrottledServiceInterface implements ServiceInterface {
    private final ServiceInterface delegate;
    private final ThrottlePolicy policy;
    private final ClientKeyExtractor keyExtractor;
    private final long clientStateTtlNanos;
    private final ConcurrentHashMap<String, ClientState> clientStates = new ConcurrentHashMap<>();
    private final AtomicInteger globalInFlight = new AtomicInteger();

    private final LongCounter.Measurement admittedCounter;
    private final LongCounter.Measurement rejectedGlobalConcurrencyCounter;
    private final LongCounter.Measurement rejectedClientConcurrencyCounter;
    private final LongCounter.Measurement rejectedRateCounter;

    /// Wraps {@code delegate} with admission control, registering metrics under names derived
    /// from the delegate's own service name so multiple throttled services don't collide.
    ///
    /// @param delegate the real plugin service implementation to protect
    /// @param policy the resolved per-client + node-wide policy for this service's methods
    /// @param keyExtractor derives the per-client key from each call's request options
    /// @param metricRegistry the registry to register this instance's metrics with
    /// @param clientStateTtl how long a client's state is kept after its last-seen call before it
    ///     becomes eligible for eviction (lazily on next lookup, or via [#sweepStaleClients])
    public ThrottledServiceInterface(
            @NonNull final ServiceInterface delegate,
            @NonNull final ThrottlePolicy policy,
            @NonNull final ClientKeyExtractor keyExtractor,
            @NonNull final MetricRegistry metricRegistry,
            @NonNull final Duration clientStateTtl) {
        this.delegate = delegate;
        this.policy = policy;
        this.keyExtractor = keyExtractor;
        this.clientStateTtlNanos = clientStateTtl.toNanos();

        final String metricPrefix = "throttle_" + delegate.serviceName();
        admittedCounter = metricRegistry
                .register(LongCounter.builder(MetricKey.of(metricPrefix + "_admitted_total", LongCounter.class)
                                .addCategory(BlockNodePlugin.METRICS_CATEGORY))
                        .setDescription("Calls admitted by the throttle for " + delegate.serviceName()))
                .getOrCreateNotLabeled();
        rejectedGlobalConcurrencyCounter = metricRegistry
                .register(LongCounter.builder(
                                MetricKey.of(metricPrefix + "_rejected_global_concurrency_total", LongCounter.class)
                                        .addCategory(BlockNodePlugin.METRICS_CATEGORY))
                        .setDescription(
                                "Calls rejected by the node-wide concurrency ceiling for " + delegate.serviceName()))
                .getOrCreateNotLabeled();
        rejectedClientConcurrencyCounter = metricRegistry
                .register(LongCounter.builder(
                                MetricKey.of(metricPrefix + "_rejected_client_concurrency_total", LongCounter.class)
                                        .addCategory(BlockNodePlugin.METRICS_CATEGORY))
                        .setDescription(
                                "Calls rejected by the per-client concurrency ceiling for " + delegate.serviceName()))
                .getOrCreateNotLabeled();
        rejectedRateCounter = metricRegistry
                .register(LongCounter.builder(MetricKey.of(metricPrefix + "_rejected_rate_total", LongCounter.class)
                                .addCategory(BlockNodePlugin.METRICS_CATEGORY))
                        .setDescription("Calls rejected by the per-client rate limit for " + delegate.serviceName()))
                .getOrCreateNotLabeled();
        metricRegistry
                .register(ObservableGauge.builder(
                                MetricKey.of(metricPrefix + "_client_state_count", ObservableGauge.class)
                                        .addCategory(BlockNodePlugin.METRICS_CATEGORY))
                        .setDescription("Number of distinct clients currently tracked by the throttle for "
                                + delegate.serviceName()))
                .observe(clientStates::mappingCount);
    }

    @NonNull
    @Override
    public String serviceName() {
        return delegate.serviceName();
    }

    @NonNull
    @Override
    public String fullName() {
        return delegate.fullName();
    }

    @NonNull
    @Override
    public List<Method> methods() {
        return delegate.methods();
    }

    @NonNull
    @Override
    public Pipeline<? super Bytes> open(
            @NonNull final Method method,
            @NonNull final RequestOptions options,
            @NonNull final Pipeline<? super Bytes> replies) {
        final String clientKey = keyExtractor.extractKey(options);
        final long now = System.nanoTime();
        // Atomic per-key compute: either reuse a live/still-fresh entry, or replace a stale,
        // currently-unused one with a fresh limiter. A client that hasn't been seen in a while
        // deserves a clean rate-limit history, not one artificially constrained by ancient calls.
        final ClientState state = clientStates.compute(clientKey, (ignoredKey, existing) -> {
            if (existing != null && !(isStale(existing, now) && existing.inFlight.get() == 0)) {
                return existing;
            }
            return new ClientState(new GcraLimiter(policy.ratePerSecond(), policy.burstTolerance()));
        });
        state.lastSeenNanos = now;

        if (globalInFlight.get() >= policy.maxConcurrentGlobal()) {
            rejectedGlobalConcurrencyCounter.increment();
            return reject(replies, "node-wide concurrency limit reached for " + delegate.serviceName());
        }
        if (state.inFlight.get() >= policy.maxConcurrentPerClient()) {
            rejectedClientConcurrencyCounter.increment();
            return reject(replies, "per-client concurrency limit reached for " + delegate.serviceName());
        }
        if (!state.limiter.tryAcquire(now)) {
            rejectedRateCounter.increment();
            return reject(replies, "rate limit exceeded for " + delegate.serviceName());
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
        return delegate.open(method, options, new ReleasingPipeline(replies, releasePermit));
    }

    @NonNull
    private Pipeline<? super Bytes> reject(
            @NonNull final Pipeline<? super Bytes> replies, @NonNull final String reason) {
        replies.onError(new GrpcException(GrpcStatus.RESOURCE_EXHAUSTED, reason));
        return Pipelines.noop();
    }

    /// Removes every client-state entry that has been idle for at least the configured TTL and has
    /// no call currently in flight. Lazy eviction in [#open] already replaces a stale entry the
    /// next time that specific client is looked up; this sweep is the backstop for a client that
    /// connects once and is never looked up again, which lazy eviction alone would never catch. A
    /// caller (e.g. the code that registers this service) is expected to invoke this periodically,
    /// at a low frequency — this class does not schedule anything itself.
    ///
    /// @param nowNanos the current time from a monotonic clock (e.g. [System#nanoTime()])
    /// @return the number of entries evicted
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

    /// Per-client state for one throttled method: the client's own rate limiter, its current
    /// in-flight call count, and when it was last seen (for eviction, see [#sweepStaleClients]).
    private static final class ClientState {
        private final GcraLimiter limiter;
        private final AtomicInteger inFlight = new AtomicInteger();
        private volatile long lastSeenNanos;

        private ClientState(final GcraLimiter limiter) {
            this.limiter = limiter;
        }
    }

    /// Wraps the outgoing `responses` pipeline so the admitted call's concurrency permit is
    /// released exactly once, whichever of [#onComplete] or [#onError] fires first — both are
    /// reliable, single-fire-per-call completion signals for every RPC shape, unlike the pipeline
    /// [ServiceInterface#open] returns (see the class-level documentation above).
    private static final class ReleasingPipeline implements Pipeline<Bytes> {
        private final Pipeline<? super Bytes> delegate;
        private final Runnable releasePermit;

        private ReleasingPipeline(final Pipeline<? super Bytes> delegate, final Runnable releasePermit) {
            this.delegate = delegate;
            this.releasePermit = releasePermit;
        }

        @Override
        public void onSubscribe(final Flow.Subscription subscription) {
            delegate.onSubscribe(subscription);
        }

        @Override
        public void onNext(final Bytes item) {
            delegate.onNext(item);
        }

        @Override
        public void onError(final Throwable throwable) {
            releasePermit.run();
            delegate.onError(throwable);
        }

        @Override
        public void onComplete() {
            releasePermit.run();
            delegate.onComplete();
        }

        @Override
        public void clientEndStreamReceived() {
            delegate.clientEndStreamReceived();
        }

        @Override
        public void closeConnection() {
            delegate.closeConnection();
        }
    }
}
