// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.throttle;

import com.hedera.pbj.runtime.grpc.GrpcException;
import com.hedera.pbj.runtime.grpc.GrpcStatus;
import com.hedera.pbj.runtime.grpc.Pipeline;
import com.hedera.pbj.runtime.grpc.ServiceInterface;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.time.Duration;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicReference;

/// The admission decorator for a service whose methods have more than one [WeightClass], as
/// described in `docs/design/apis/api-throttling.md` ("Content-aware weighting"). Unlike the
/// single-tier [ThrottledServiceInterface], admission is decided in the returned pipeline's
/// `onNext`, not in `open()`, because the request bytes a [ContentAwareWeigher] needs are not
/// available until then.
///
/// `open()` therefore only extracts the client key and builds the delegate's own inbound pipeline,
/// unconditionally. A weighted method's delegate must keep its `open()` free of side effects
/// (acquiring resources, I/O), since a rejected call never reaches the delegate's `onNext`.
///
/// Classification and the admission checks (see [ClientThrottle]) run together once per call,
/// before the delegate's `onNext`. A weight class with no per-client entry for the method falls
/// back to `STANDARD`'s per-client settings and `STANDARD`'s node-wide gate.
///
/// A rejected call sends `RESOURCE_EXHAUSTED` to the client and then leaves the delegate's inbound
/// pipeline without any terminal call: PBJ's `Pipelines`-built delegates forward `onComplete`
/// straight to the replies pipeline, which would deliver a second terminal signal to the client.
///
/// The concurrency permit is released via the outgoing `replies` pipeline, see
/// [ThrottledServiceInterface] for why.
public final class WeightedThrottledServiceInterface implements ServiceInterface, StaleClientSweepable {
    /// The `method` label used for node-wide gates, which are scoped to the service, not a method.
    private static final String ALL_METHODS = "*";

    private final ServiceInterface delegate;
    private final ClientKeyExtractor keyExtractor;
    private final ContentAwareWeigher weigher;
    private final ThrottleMetrics throttleMetrics;
    private final Map<WeightClass, GlobalConcurrencyGate> globalGates;
    private final Map<MethodWeight, ClientThrottle> throttles;

    /// @param delegate the real plugin service implementation to protect
    /// @param perClientSettings per-`(method, weight class)` settings; every configured method must
    ///     include a [WeightClass#STANDARD] entry
    /// @param globalConcurrencyCeilings the node-wide ceiling per weight class; must include
    ///     [WeightClass#STANDARD] and every weight class that has per-client settings
    /// @param keyExtractor derives the per-client key from each call's request options
    /// @param weigher classifies each call
    /// @param throttleMetrics the shared, once-registered metrics
    /// @param clientStateTtl how long a client's state is kept after its last-seen call
    public WeightedThrottledServiceInterface(
            @NonNull final ServiceInterface delegate,
            @NonNull final Map<MethodWeight, PerClientThrottleSettings> perClientSettings,
            @NonNull final Map<WeightClass, Integer> globalConcurrencyCeilings,
            @NonNull final ClientKeyExtractor keyExtractor,
            @NonNull final ContentAwareWeigher weigher,
            @NonNull final ThrottleMetrics throttleMetrics,
            @NonNull final Duration clientStateTtl) {
        this.delegate = delegate;
        this.keyExtractor = keyExtractor;
        this.weigher = weigher;
        this.throttleMetrics = throttleMetrics;

        if (!globalConcurrencyCeilings.containsKey(WeightClass.STANDARD)) {
            throw new IllegalArgumentException(
                    "No node-wide concurrency ceiling for WeightClass.STANDARD on " + delegate.serviceName());
        }
        final Map<WeightClass, GlobalConcurrencyGate> gates = new EnumMap<>(WeightClass.class);
        globalConcurrencyCeilings.forEach((weightClass, ceiling) -> {
            final GlobalConcurrencyGate gate = new GlobalConcurrencyGate(ceiling);
            throttleMetrics
                    .inFlightGaugeFor(delegate.serviceName(), ALL_METHODS, weightClass)
                    .accept(gate::inFlight);
            gates.put(weightClass, gate);
        });
        this.globalGates = Map.copyOf(gates);

        final Set<String> methods = new HashSet<>();
        final Set<String> methodsWithStandard = new HashSet<>();
        final Map<MethodWeight, ClientThrottle> built = new HashMap<>();
        perClientSettings.forEach((key, settings) -> {
            methods.add(key.method());
            if (key.weightClass() == WeightClass.STANDARD) {
                methodsWithStandard.add(key.method());
            }
            final GlobalConcurrencyGate gate = globalGates.get(key.weightClass());
            if (gate == null) {
                throw new IllegalArgumentException("No node-wide concurrency ceiling for weight class "
                        + key.weightClass() + " (" + key + ") on " + delegate.serviceName());
            }
            built.put(
                    key,
                    new ClientThrottle(
                            settings,
                            gate,
                            throttleMetrics,
                            delegate.serviceName(),
                            key.method(),
                            key.weightClass(),
                            clientStateTtl));
        });
        if (!methodsWithStandard.containsAll(methods)) {
            methods.removeAll(methodsWithStandard);
            throw new IllegalArgumentException("perClientSettings for " + delegate.serviceName()
                    + " must include a WeightClass.STANDARD entry for every configured method; missing for "
                    + methods);
        }
        this.throttles = Map.copyOf(built);
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
        final AtomicReference<Runnable> releasePermit = new AtomicReference<>(() -> {});
        final Pipeline<? super Bytes> delegateInbound =
                delegate.open(method, options, new ReleasingPipeline(replies, () -> releasePermit
                        .get()
                        .run()));
        return new AdmissionGatingPipeline(
                delegateInbound, replies, releasePermit, keyExtractor.extractKey(options), method);
    }

    /// Admits a call to a method with no per-client settings at all, against only the node-wide
    /// ceiling of the classified weight class, or `STANDARD`'s if that class has none.
    @NonNull
    private AdmissionResult globalOnlyAdmit(@NonNull final String method, @NonNull final WeightClass weightClass) {
        final WeightClass effective = globalGates.containsKey(weightClass) ? weightClass : WeightClass.STANDARD;
        final AdmissionResult result =
                globalGates.get(effective).tryAdmit(delegate.serviceName() + "." + method + " (" + effective + ")");
        throttleMetrics.recordCall(
                delegate.serviceName(),
                method,
                effective,
                result.admitted()
                        ? ThrottleMetrics.Outcome.ADMITTED
                        : ThrottleMetrics.Outcome.REJECTED_GLOBAL_CONCURRENCY);
        return result;
    }

    /// {@inheritDoc}
    @Override
    public int sweepStaleClients(final long nowNanos) {
        int evicted = 0;
        for (final ClientThrottle throttle : throttles.values()) {
            evicted += throttle.sweepStaleClients(nowNanos);
        }
        return evicted;
    }

    /// {@inheritDoc}
    @Override
    public int reapExpiredPermits(final long nowNanos) {
        int reaped = 0;
        for (final ClientThrottle throttle : throttles.values()) {
            reaped += throttle.reapExpiredPermits(nowNanos);
        }
        return reaped;
    }

    /// {@inheritDoc}
    @Override
    public int smallestPermitHoldSeconds() {
        return throttles.values().stream()
                .mapToInt(ClientThrottle::permitHoldSeconds)
                .filter(seconds -> seconds > 0)
                .min()
                .orElse(0);
    }

    /// Wraps the delegate's inbound pipeline, deciding admission on the first `onNext`.
    private final class AdmissionGatingPipeline implements Pipeline<Bytes> {
        private final Pipeline<? super Bytes> delegateInbound;
        private final Pipeline<? super Bytes> replies;
        private final AtomicReference<Runnable> releasePermit;
        private final String clientKey;
        private final Method method;
        private volatile boolean decided;
        private volatile boolean rejected;

        private AdmissionGatingPipeline(
                final Pipeline<? super Bytes> delegateInbound,
                final Pipeline<? super Bytes> replies,
                final AtomicReference<Runnable> releasePermit,
                final String clientKey,
                final Method method) {
            this.delegateInbound = delegateInbound;
            this.replies = replies;
            this.releasePermit = releasePermit;
            this.clientKey = clientKey;
            this.method = method;
        }

        @Override
        public void onSubscribe(final Flow.Subscription subscription) {
            delegateInbound.onSubscribe(subscription);
        }

        @Override
        public void onNext(final Bytes requestBytes) {
            if (rejected) {
                return;
            }
            if (decided) {
                delegateInbound.onNext(requestBytes);
                return;
            }
            decided = true;
            final WeightClass weightClass = weigher.classify(method, requestBytes);
            ClientThrottle throttle = throttles.get(new MethodWeight(method.name(), weightClass));
            if (throttle == null) {
                throttle = throttles.get(new MethodWeight(method.name(), WeightClass.STANDARD));
            }
            final AdmissionResult result = (throttle != null)
                    ? throttle.tryAdmit(clientKey, System.nanoTime())
                    : globalOnlyAdmit(method.name(), weightClass);
            if (!result.admitted()) {
                rejected = true;
                replies.onError(new GrpcException(GrpcStatus.RESOURCE_EXHAUSTED, result.rejectionReason()));
                return;
            }
            releasePermit.set(result.releasePermit());
            try {
                delegateInbound.onNext(requestBytes);
            } catch (final RuntimeException | Error e) {
                // Mirrors ThrottledServiceInterface: a throwing handler may never complete `replies`.
                // The release is idempotent, so this is safe even if it already did.
                result.releasePermit().run();
                throw e;
            }
        }

        @Override
        public void onError(final Throwable throwable) {
            if (!rejected) {
                delegateInbound.onError(throwable);
            }
        }

        @Override
        public void onComplete() {
            if (!rejected) {
                delegateInbound.onComplete();
            }
        }

        @Override
        public void clientEndStreamReceived() {
            if (!rejected) {
                delegateInbound.clientEndStreamReceived();
            }
        }

        @Override
        public void closeConnection() {
            if (!rejected) {
                delegateInbound.closeConnection();
            }
        }
    }
}
