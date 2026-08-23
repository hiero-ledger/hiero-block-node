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
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicReference;
import org.hiero.metrics.core.MetricRegistry;

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
/// hand. If rejected there, the delegate's pipeline never receives `onNext` (or anything else) at
/// all, so its business logic never runs.
public final class WeightedThrottledServiceInterface implements ServiceInterface, StaleClientSweepable {
    private final ServiceInterface delegate;
    private final ClientKeyExtractor keyExtractor;
    private final ContentAwareWeigher weigher;
    private final Map<WeightClass, SingleWeightThrottle> throttlesByWeight;

    /// @param delegate the real plugin service implementation to protect
    /// @param policiesByWeight one policy per weight class this service's weigher can classify
    ///     into; must contain an entry for {@link WeightClass#STANDARD}, used as the fallback if
    ///     the weigher ever returns a class with no configured policy
    /// @param keyExtractor derives the per-client key from each call's request options
    /// @param weigher classifies each call's request content into a weight class
    /// @param metricRegistry the registry to register this instance's metrics with
    /// @param clientStateTtl how long a client's state is kept, per weight class, after its
    ///     last-seen call before it becomes eligible for eviction
    public WeightedThrottledServiceInterface(
            @NonNull final ServiceInterface delegate,
            @NonNull final Map<WeightClass, ThrottlePolicy> policiesByWeight,
            @NonNull final ClientKeyExtractor keyExtractor,
            @NonNull final ContentAwareWeigher weigher,
            @NonNull final MetricRegistry metricRegistry,
            @NonNull final Duration clientStateTtl) {
        this.delegate = delegate;
        this.keyExtractor = keyExtractor;
        this.weigher = weigher;
        if (!policiesByWeight.containsKey(WeightClass.STANDARD)) {
            throw new IllegalArgumentException("policiesByWeight must contain an entry for WeightClass.STANDARD");
        }
        this.throttlesByWeight = new EnumMap<>(WeightClass.class);
        for (final Map.Entry<WeightClass, ThrottlePolicy> entry : policiesByWeight.entrySet()) {
            final String tierName = entry.getKey().name().toLowerCase(Locale.ROOT);
            final String metricPrefix = "throttle_" + delegate.serviceName() + "_" + tierName;
            final String description = delegate.serviceName() + " (" + tierName + ")";
            throttlesByWeight.put(
                    entry.getKey(),
                    new SingleWeightThrottle(
                            entry.getValue(), metricRegistry, metricPrefix, description, clientStateTtl));
        }
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

    /// {@inheritDoc}
    @Override
    public int sweepStaleClients(final long nowNanos) {
        int evicted = 0;
        for (final SingleWeightThrottle throttle : throttlesByWeight.values()) {
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
            final SingleWeightThrottle throttle =
                    throttlesByWeight.getOrDefault(weightClass, throttlesByWeight.get(WeightClass.STANDARD));
            final AdmissionResult result = throttle.tryAdmit(clientKey, System.nanoTime());
            if (!result.admitted()) {
                rejected = true;
                replies.onError(new GrpcException(GrpcStatus.RESOURCE_EXHAUSTED, result.rejectionReason()));
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
