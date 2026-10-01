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

/// Wraps a plugin's [ServiceInterface] the same way [ThrottledServiceInterface] does, except a
/// [ContentAwareWeigher] first classifies each call into a [WeightClass], and the corresponding
/// policy for that class governs admission — so, for example, a `getBlock` call for a historical
/// block can be throttled more strictly than one for a live block, using the same client's
/// independent rate/concurrency history per weight class.
///
/// **Classification cannot happen synchronously inside [#open].** The request's content is not
/// yet available there: for both unary and server-streaming calls built with
/// `com.hedera.pbj.runtime.grpc.Pipelines`, the actual request bytes arrive later, via `onNext` on
/// the [Pipeline] `open()` returns — `open()` itself only receives the call's method and options.
/// This class therefore always calls the delegate's `open()` immediately (cheap: for these call
/// shapes that only builds a pipeline, it does not run business logic yet) and defers the
/// admission decision to its own wrapper pipeline's `onNext`, once the real request bytes are in
/// hand. If rejected there, the delegate's inbound pipeline is given a normal, empty completion
/// (`onNext` is never called on it, immediately followed by `onComplete`) rather than being left
/// dangling with no terminal signal at all, so its business logic never runs, but it is still told
/// the call is over.
///
/// Like [ThrottledServiceInterface], every `(method, weight class)` pair named in {@code
/// perClientSettings} gets its own [SingleWeightThrottle]; a classified call with no matching
/// entry falls back to that method's `WeightClass.STANDARD` entry, and a method with no entries
/// at all falls back further still, to the shared [GlobalConcurrencyGate] for whichever weight
/// class the weigher produced — see [ThrottleSpec#perClientSettings].
///
/// This class is deliberately the only place (besides [ContentAwareWeigher] implementations,
/// which only parse request bytes) that references PBJ's `ServiceInterface`/`Pipeline` types —
/// the actual decision logic lives entirely in [SingleWeightThrottle], which knows nothing about
/// gRPC or how it's attached to a call. Preserve that split; see [ThrottledServiceInterface]'s
/// class documentation for why.
public final class WeightedThrottledServiceInterface implements ServiceInterface, StaleClientSweepable {
    private final ServiceInterface delegate;
    private final ClientKeyExtractor keyExtractor;
    private final ContentAwareWeigher weigher;
    private final ThrottleMetrics throttleMetrics;
    private final Map<WeightClass, GlobalConcurrencyGate> globalGates;
    private final Map<MethodWeight, SingleWeightThrottle> throttles;

    /// @param delegate the real plugin service implementation to protect
    /// @param perClientSettings per-`(method, weight class)` settings — see
    ///     [ThrottleSpec#perClientSettings]. For every method with at least one entry, one of its
    ///     entries must use {@link WeightClass#STANDARD}, used as that method's fallback if the
    ///     weigher ever returns a class with no configured entry for it
    /// @param globalConcurrencyCeilings this service's node-wide ceiling per weight class — see
    ///     [ThrottleSpec#globalConcurrencyCeilings]; must cover every weight class {@code weigher}
    ///     can produce
    /// @param keyExtractor derives the per-client key from each call's request options
    /// @param weigher classifies each call's request content into a weight class
    /// @param throttleMetrics the shared, once-registered metrics this instance's calls report into
    /// @param clientStateTtl how long a client's state is kept, per weight class, after its
    ///     last-seen call before it becomes eligible for eviction
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

        final Map<WeightClass, GlobalConcurrencyGate> gates = new EnumMap<>(WeightClass.class);
        for (final Map.Entry<WeightClass, Integer> entry : globalConcurrencyCeilings.entrySet()) {
            gates.put(entry.getKey(), new GlobalConcurrencyGate(entry.getValue()));
        }
        this.globalGates = Map.copyOf(gates);

        final Set<String> methodsSeen = new HashSet<>();
        final Set<String> methodsWithStandard = new HashSet<>();
        final Map<MethodWeight, SingleWeightThrottle> built = new HashMap<>();
        for (final Map.Entry<MethodWeight, PerClientThrottleSettings> entry : perClientSettings.entrySet()) {
            final MethodWeight key = entry.getKey();
            methodsSeen.add(key.method());
            if (key.weightClass() == WeightClass.STANDARD) {
                methodsWithStandard.add(key.method());
            }
            final GlobalConcurrencyGate gate = globalGates.get(key.weightClass());
            if (gate == null) {
                throw new IllegalArgumentException("No globalConcurrencyCeilings entry for weight class "
                        + key.weightClass() + " (" + key + ") on " + delegate.serviceName());
            }
            built.put(
                    key,
                    new SingleWeightThrottle(
                            entry.getValue(),
                            gate,
                            throttleMetrics,
                            delegate.serviceName(),
                            key.method(),
                            key.weightClass(),
                            clientStateTtl));
        }
        if (!methodsWithStandard.containsAll(methodsSeen)) {
            methodsSeen.removeAll(methodsWithStandard);
            throw new IllegalArgumentException("perClientSettings for " + delegate.serviceName()
                    + " must include a WeightClass.STANDARD entry for every configured method; missing for "
                    + methodsSeen);
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
        final String clientKey = keyExtractor.extractKey(options);
        final AtomicReference<Runnable> releasePermit = new AtomicReference<>(() -> {});
        final Pipeline<? super Bytes> delegateInbound =
                delegate.open(method, options, new ReleasingPipeline(replies, releasePermit));
        return new AdmissionGatingPipeline(delegateInbound, replies, releasePermit, clientKey, method);
    }

    /// Admits a call to a method with no entry for its classified weight class — see
    /// [ThrottleSpec#perClientSettings]. Only the shared node-wide concurrency ceiling for that
    /// weight class applies.
    @NonNull
    private AdmissionResult globalOnlyAdmit(@NonNull final String method, @NonNull final WeightClass weightClass) {
        final GlobalConcurrencyGate gate = globalGates.get(weightClass);
        final AdmissionResult result = (gate != null)
                ? gate.tryAdmit(delegate.serviceName() + "." + method)
                : AdmissionResult.rejected(
                        "no node-wide concurrency ceiling configured for weight class " + weightClass);
        throttleMetrics.recordCall(
                delegate.serviceName(),
                method,
                weightClass,
                result.admitted()
                        ? ThrottleMetrics.Outcome.ADMITTED
                        : ThrottleMetrics.Outcome.REJECTED_GLOBAL_CONCURRENCY);
        return result;
    }

    /// {@inheritDoc}
    @Override
    public int sweepStaleClients(final long nowNanos) {
        int evicted = 0;
        for (final SingleWeightThrottle throttle : throttles.values()) {
            evicted += throttle.sweepStaleClients(nowNanos);
        }
        return evicted;
    }

    /// The delegate's inbound (request-side) pipeline is already obtained by the time this is
    /// constructed, but is never fed anything unless/until [#onNext] admits the call — see the
    /// class-level documentation on [WeightedThrottledServiceInterface] for why.
    private final class AdmissionGatingPipeline implements Pipeline<Bytes> {
        private final Pipeline<? super Bytes> delegateInbound;
        private final Pipeline<? super Bytes> replies;
        private final AtomicReference<Runnable> releasePermit;
        private final String clientKey;
        private final Method method;
        private volatile boolean rejected = false;

        private AdmissionGatingPipeline(
                @NonNull final Pipeline<? super Bytes> delegateInbound,
                @NonNull final Pipeline<? super Bytes> replies,
                @NonNull final AtomicReference<Runnable> releasePermit,
                @NonNull final String clientKey,
                @NonNull final Method method) {
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
            final WeightClass weightClass = weigher.classify(method, requestBytes);
            final String methodName = method.name();
            SingleWeightThrottle throttle = throttles.get(new MethodWeight(methodName, weightClass));
            if (throttle == null) {
                throttle = throttles.get(new MethodWeight(methodName, WeightClass.STANDARD));
            }
            final AdmissionResult result = (throttle != null)
                    ? throttle.tryAdmit(clientKey, System.nanoTime())
                    : globalOnlyAdmit(methodName, weightClass);
            if (!result.admitted()) {
                rejected = true;
                replies.onError(new GrpcException(GrpcStatus.RESOURCE_EXHAUSTED, result.rejectionReason()));
                // delegateInbound already exists (open() built it before admission was known, see
                // the class-level documentation on why that's unavoidable) but will never receive
                // onNext now — give it a normal, empty completion instead of leaving it dangling
                // with no terminal signal at all. Valid per Flow.Subscriber: onComplete with zero
                // onNext calls is a normal empty stream, not an error condition.
                delegateInbound.onComplete();
                return;
            }
            releasePermit.set(result.releasePermit());
            delegateInbound.onNext(requestBytes);
        }

        @Override
        public void onError(final Throwable throwable) {
            // If already rejected, replies.onError() was already called above, and the delegate's
            // pipeline never received onNext — there is nothing further to forward to it.
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
