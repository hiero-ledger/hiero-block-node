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
/// Each of `delegate`'s methods is gated independently. A method with per-client settings (named
/// in `perClientSettings`, or covered by `defaultPerClientSettings`) gets its own [ClientThrottle]
/// with its own rate bucket, concurrency ceiling, and client-state table. Each method with a
/// node-wide ceiling gets its own [GlobalConcurrencyGate]. A method with a ceiling but no
/// per-client settings is limited by that ceiling alone; a method with neither is not throttled.
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
    private final Map<String, GlobalConcurrencyGate> globalGates;
    private final Map<String, ClientThrottle> perMethodThrottles;

    /// @param delegate the real plugin service implementation to protect
    /// @param perClientSettings per-method settings for the methods to gate individually, keyed
    ///     by method name, see [ThrottleSpec#perClientSettings]
    /// @param defaultPerClientSettings a fallback applied to any of {@code delegate}'s methods not
    ///     named in {@code perClientSettings}, see [ThrottleSpec#defaultPerClientSettings]
    /// @param globalConcurrencyCeilings the node-wide concurrency ceiling per method name, see
    ///     [ThrottleSpec#globalConcurrencyCeilings]; must cover every method that has per-client
    ///     settings
    /// @param keyExtractor derives the per-client key from each call's request options
    /// @param throttleMetrics the shared, once-registered metrics this instance's calls report into
    /// @param clientStateTtl how long a client's state is kept after its last-seen call before
    ///     eviction becomes eligible (lazily on next lookup, or via [#sweepStaleClients])
    public ThrottledServiceInterface(
            @NonNull final ServiceInterface delegate,
            @NonNull final Map<String, PerClientThrottleSettings> perClientSettings,
            @NonNull final Optional<PerClientThrottleSettings> defaultPerClientSettings,
            @NonNull final Map<String, Integer> globalConcurrencyCeilings,
            @NonNull final ClientKeyExtractor keyExtractor,
            @NonNull final ThrottleMetrics throttleMetrics,
            @NonNull final Duration clientStateTtl) {
        this.delegate = delegate;
        this.keyExtractor = keyExtractor;
        this.throttleMetrics = throttleMetrics;

        final Map<String, GlobalConcurrencyGate> gates = new HashMap<>();
        globalConcurrencyCeilings.forEach((method, ceiling) -> {
            final GlobalConcurrencyGate gate = new GlobalConcurrencyGate(ceiling);
            throttleMetrics.inFlightGaugeFor(delegate.serviceName(), method).accept(gate::inFlight);
            gates.put(method, gate);
        });
        this.globalGates = Map.copyOf(gates);

        // Explicit per-method settings always win. Build those first so the computeIfAbsent below
        // only backfills methods with no explicit entry.
        final Map<String, ClientThrottle> throttles = new HashMap<>();
        for (final Map.Entry<String, PerClientThrottleSettings> entry : perClientSettings.entrySet()) {
            final String method = entry.getKey();
            throttles.put(
                    method,
                    new ClientThrottle(
                            entry.getValue(),
                            gateFor(method),
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
                                defaults,
                                gateFor(name),
                                throttleMetrics,
                                delegate.serviceName(),
                                name,
                                clientStateTtl));
            }
        });
        this.perMethodThrottles = Map.copyOf(throttles);
    }

    /// @return the ceiling gate for `method`, failing fast at construction if a per-client gate was
    ///     configured for a method with no node-wide ceiling
    @NonNull
    private GlobalConcurrencyGate gateFor(@NonNull final String method) {
        final GlobalConcurrencyGate gate = globalGates.get(method);
        if (gate == null) {
            throw new IllegalArgumentException(
                    "No node-wide concurrency ceiling configured for " + delegate.serviceName() + "." + method);
        }
        return gate;
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
        final GlobalConcurrencyGate gate = globalGates.get(method.name());
        if (configured == null && gate == null) {
            return delegate.open(method, options, replies);
        }
        final AdmissionResult result = (configured != null)
                ? configured.tryAdmit(keyExtractor.extractKey(options), System.nanoTime())
                : globalOnlyAdmit(gate, method.name());
        if (!result.admitted()) {
            replies.onError(new GrpcException(GrpcStatus.RESOURCE_EXHAUSTED, result.rejectionReason()));
            return Pipelines.noop();
        }
        try {
            return delegate.open(method, options, new ReleasingPipeline(replies, result.releasePermit()));
        } catch (final RuntimeException | Error e) {
            // PBJ answers a throwing open() itself and never completes the pipeline handed in, so the
            // ReleasingPipeline would never release. The release is idempotent, so this is safe even
            // if the delegate already completed `replies` before throwing.
            result.releasePermit().run();
            throw e;
        }
    }

    /// Admits a call to a method with no per-client gate configured, see
    /// [ThrottleSpec#perClientSettings]. Only the method's node-wide concurrency ceiling applies.
    @NonNull
    private AdmissionResult globalOnlyAdmit(@NonNull final GlobalConcurrencyGate gate, @NonNull final String method) {
        final AdmissionResult result = gate.tryAdmit(delegate.serviceName() + "." + method);
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

    /// {@inheritDoc}
    @Override
    public int reapExpiredPermits(final long nowNanos) {
        int reaped = 0;
        for (final ClientThrottle throttle : perMethodThrottles.values()) {
            reaped += throttle.reapExpiredPermits(nowNanos);
        }
        return reaped;
    }

    /// {@inheritDoc}
    @Override
    public int smallestPermitHoldSeconds() {
        return perMethodThrottles.values().stream()
                .mapToInt(ClientThrottle::permitHoldSeconds)
                .filter(seconds -> seconds > 0)
                .min()
                .orElse(0);
    }
}
