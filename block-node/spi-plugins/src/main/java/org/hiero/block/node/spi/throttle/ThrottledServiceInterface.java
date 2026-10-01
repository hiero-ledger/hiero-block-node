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
import java.util.concurrent.atomic.AtomicReference;

/// Wraps a plugin's [ServiceInterface] with a per-client rate/concurrency admission policy, as
/// described in `docs/design/apis/api-throttling.md`. For a service whose methods have more than
/// one cost tier (e.g. `getBlock`'s live-vs-historical distinction), see
/// [WeightedThrottledServiceInterface] instead — the admission-decision order, eviction, and
/// permit-lifecycle logic are shared between both via [SingleWeightThrottle].
///
/// Each of `delegate`'s methods is gated independently: a method named in {@code
/// perClientSettings} (or covered by {@code defaultPerClientSettings}) gets its own
/// [SingleWeightThrottle] — its own rate bucket, concurrency ceiling, and client-state table,
/// entirely independent of any other method on the same service. A method with neither gets no
/// per-client gate at all; it is still subject to the one shared [GlobalConcurrencyGate] every
/// method on this service draws from — see [ThrottleSpec#perClientSettings] for why that ceiling
/// is never split per method even though per-client settings now can be.
///
/// This class is deliberately the *only* place in the throttle mechanism that references PBJ's
/// `ServiceInterface`/`Pipeline` types — [SingleWeightThrottle] (and everything it uses:
/// [GcraLimiter], client-state eviction) takes only a client key and a clock reading, with no
/// knowledge of gRPC, PBJ, or how it's attached to a call. Preserve that split: if a different
/// attachment point ever becomes available (e.g. a future PBJ/Helidon interceptor hook), only this
/// class and [WeightedThrottledServiceInterface] should need to change, not the admission logic.
public final class ThrottledServiceInterface implements ServiceInterface, StaleClientSweepable {
    private final ServiceInterface delegate;
    private final ClientKeyExtractor keyExtractor;
    private final ThrottleMetrics throttleMetrics;
    private final GlobalConcurrencyGate globalGate;
    private final Map<String, SingleWeightThrottle> perMethodThrottles;

    /// @param delegate the real plugin service implementation to protect
    /// @param perClientSettings per-method settings for the methods to gate individually — see
    ///     [ThrottleSpec#perClientSettings]; every key's {@link MethodWeight#weightClass()} must
    ///     be [WeightClass#STANDARD], since this class has no weigher to produce any other class
    /// @param defaultPerClientSettings a fallback applied to any of {@code delegate}'s methods not
    ///     named in {@code perClientSettings} — see [ThrottleSpec#defaultPerClientSettings]
    /// @param maxConcurrentGlobal the node-wide concurrency ceiling shared by every method on this
    ///     service, configured or not
    /// @param keyExtractor derives the per-client key from each call's request options
    /// @param throttleMetrics the shared, once-registered metrics this instance's calls report into
    /// @param clientStateTtl how long a client's state is kept after its last-seen call before it
    ///     becomes eligible for eviction (lazily on next lookup, or via [#sweepStaleClients])
    public ThrottledServiceInterface(
            @NonNull final ServiceInterface delegate,
            @NonNull final Map<MethodWeight, PerClientThrottleSettings> perClientSettings,
            @NonNull final Optional<PerClientThrottleSettings> defaultPerClientSettings,
            final int maxConcurrentGlobal,
            @NonNull final ClientKeyExtractor keyExtractor,
            @NonNull final ThrottleMetrics throttleMetrics,
            @NonNull final Duration clientStateTtl) {
        this.delegate = delegate;
        this.keyExtractor = keyExtractor;
        this.throttleMetrics = throttleMetrics;
        this.globalGate = new GlobalConcurrencyGate(maxConcurrentGlobal);

        final Map<String, SingleWeightThrottle> throttles = new HashMap<>();
        for (final Map.Entry<MethodWeight, PerClientThrottleSettings> entry : perClientSettings.entrySet()) {
            final MethodWeight key = entry.getKey();
            if (key.weightClass() != WeightClass.STANDARD) {
                throw new IllegalArgumentException("ThrottledServiceInterface has no weigher, so every "
                        + "perClientSettings entry must use WeightClass.STANDARD; got " + key + " for "
                        + delegate.serviceName());
            }
            throttles.put(
                    key.method(),
                    new SingleWeightThrottle(
                            entry.getValue(),
                            globalGate,
                            throttleMetrics,
                            delegate.serviceName(),
                            key.method(),
                            WeightClass.STANDARD,
                            clientStateTtl));
        }
        defaultPerClientSettings.ifPresent(defaults -> {
            for (final Method method : delegate.methods()) {
                throttles.computeIfAbsent(
                        method.name(),
                        name -> new SingleWeightThrottle(
                                defaults,
                                globalGate,
                                throttleMetrics,
                                delegate.serviceName(),
                                name,
                                WeightClass.STANDARD,
                                clientStateTtl));
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
        final SingleWeightThrottle configured = perMethodThrottles.get(method.name());
        final AdmissionResult result = (configured != null)
                ? configured.tryAdmit(keyExtractor.extractKey(options), System.nanoTime())
                : globalOnlyAdmit(method.name());
        if (!result.admitted()) {
            replies.onError(new GrpcException(GrpcStatus.RESOURCE_EXHAUSTED, result.rejectionReason()));
            return Pipelines.noop();
        }
        return delegate.open(
                method, options, new ReleasingPipeline(replies, new AtomicReference<>(result.releasePermit())));
    }

    /// Admits a call to a method with no per-client gate configured — see
    /// [ThrottleSpec#perClientSettings]. Only the shared node-wide concurrency ceiling applies.
    @NonNull
    private AdmissionResult globalOnlyAdmit(@NonNull final String method) {
        final AdmissionResult result = globalGate.tryAdmit(delegate.serviceName() + "." + method);
        throttleMetrics.recordCall(
                delegate.serviceName(),
                method,
                WeightClass.STANDARD,
                result.admitted()
                        ? ThrottleMetrics.Outcome.ADMITTED
                        : ThrottleMetrics.Outcome.REJECTED_GLOBAL_CONCURRENCY);
        return result;
    }

    /// {@inheritDoc}
    @Override
    public int sweepStaleClients(final long nowNanos) {
        int evicted = 0;
        for (final SingleWeightThrottle throttle : perMethodThrottles.values()) {
            evicted += throttle.sweepStaleClients(nowNanos);
        }
        return evicted;
    }
}
