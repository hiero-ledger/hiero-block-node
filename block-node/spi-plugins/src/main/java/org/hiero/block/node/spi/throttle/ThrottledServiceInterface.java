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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/// Wraps a plugin's [ServiceInterface] with a per-client rate/concurrency admission policy, as
/// described in `docs/design/apis/api-throttling.md`. Attaches at `open()`, the one method every
/// gRPC call (unary or streaming) passes through, so the mechanism doesn't depend on which web
/// server hosts the service.
///
/// Each of `delegate`'s methods is gated independently: a method named in `perClientSettings` (or
/// covered by `defaultPerClientSettings`) gets its own [ClientThrottle] with its own rate bucket,
/// concurrency ceiling, and client-state table. A method with neither still gets the shared
/// [GlobalConcurrencyGate].
///
/// The concurrency permit is released via the *outgoing* `responses` pipeline passed into
/// `open()`, not the pipeline `open()` returns: for a server-streaming call the returned pipeline
/// can complete well before the call itself is done, so releasing on it would free the permit too
/// early. `onComplete`/`onError` on the outgoing pipeline are the only signals reliable for every
/// call shape.
///
/// This is the only class in the throttle mechanism that references PBJ's
/// `ServiceInterface`/`Pipeline` types; [ClientThrottle] and everything it uses take only a client
/// key and a clock reading, with no knowledge of gRPC.
public final class ThrottledServiceInterface implements ServiceInterface, StaleClientSweepable {
    private final ServiceInterface delegate;
    private final ClientKeyExtractor keyExtractor;
    private final ThrottleMetrics throttleMetrics;
    private final GlobalConcurrencyGate globalGate;
    private final Map<String, ClientThrottle> perMethodThrottles;

    /// @param delegate the real plugin service implementation to protect
    /// @param perClientSettings per-method settings for the methods to gate individually, keyed
    ///     by method name, see [ThrottleSpec#perClientSettings]
    /// @param defaultPerClientSettings a fallback applied to any of {@code delegate}'s methods not
    ///     named in {@code perClientSettings}, see [ThrottleSpec#defaultPerClientSettings]
    /// @param maxConcurrentGlobal the node-wide concurrency ceiling shared by every method on this
    ///     service, configured or not
    /// @param keyExtractor derives the per-client key from each call's request options
    /// @param throttleMetrics the shared, once-registered metrics this instance's calls report into
    /// @param clientStateTtl how long a client's state is kept after its last-seen call before
    ///     eviction becomes eligible (lazily on next lookup, or via [#sweepStaleClients])
    public ThrottledServiceInterface(
            @NonNull final ServiceInterface delegate,
            @NonNull final Map<String, PerClientThrottleSettings> perClientSettings,
            @NonNull final Optional<PerClientThrottleSettings> defaultPerClientSettings,
            final int maxConcurrentGlobal,
            @NonNull final ClientKeyExtractor keyExtractor,
            @NonNull final ThrottleMetrics throttleMetrics,
            @NonNull final Duration clientStateTtl) {
        this.delegate = delegate;
        this.keyExtractor = keyExtractor;
        this.throttleMetrics = throttleMetrics;
        this.globalGate = new GlobalConcurrencyGate(maxConcurrentGlobal);

        // Explicit per-method settings always win. Build those first so the computeIfAbsent below
        // only backfills methods with no explicit entry.
        final Map<String, ClientThrottle> throttles = new HashMap<>();
        for (final Map.Entry<String, PerClientThrottleSettings> entry : perClientSettings.entrySet()) {
            final String method = entry.getKey();
            throttles.put(
                    method,
                    new ClientThrottle(
                            entry.getValue(),
                            globalGate,
                            throttleMetrics,
                            delegate.serviceName(),
                            method,
                            clientStateTtl));
        }
        // defaultPerClientSettings, if configured, only fills in the methods not already covered
        // above — it never overrides an explicit per-method entry.
        defaultPerClientSettings.ifPresent(defaults -> {
            for (final Method method : delegate.methods()) {
                throttles.computeIfAbsent(
                        method.name(),
                        name -> new ClientThrottle(
                                defaults, globalGate, throttleMetrics, delegate.serviceName(), name, clientStateTtl));
            }
        });
        this.perMethodThrottles = Map.copyOf(throttles);
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
        final ClientThrottle configured = perMethodThrottles.get(method.name());
        final AdmissionResult result = (configured != null)
                ? configured.tryAdmit(keyExtractor.extractKey(options), System.nanoTime())
                : globalOnlyAdmit(method.name());
        if (!result.admitted()) {
            replies.onError(new GrpcException(GrpcStatus.RESOURCE_EXHAUSTED, result.rejectionReason()));
            return Pipelines.noop();
        }
        return delegate.open(method, options, new ReleasingPipeline(replies, result.releasePermit()));
    }

    /// Admits a call to a method with no per-client gate configured, see
    /// [ThrottleSpec#perClientSettings]. Only the shared node-wide concurrency ceiling applies.
    @NonNull
    private AdmissionResult globalOnlyAdmit(@NonNull final String method) {
        final AdmissionResult result = globalGate.tryAdmit(delegate.serviceName() + "." + method);
        throttleMetrics.recordCall(
                delegate.serviceName(),
                method,
                result.admitted()
                        ? ThrottleMetrics.Outcome.ADMITTED
                        : ThrottleMetrics.Outcome.REJECTED_GLOBAL_CONCURRENCY);
        return result;
    }

    /// {@inheritDoc}
    @Override
    public int sweepStaleClients(final long nowNanos) {
        int evicted = 0;
        for (final ClientThrottle throttle : perMethodThrottles.values()) {
            evicted += throttle.sweepStaleClients(nowNanos);
        }
        return evicted;
    }
}
