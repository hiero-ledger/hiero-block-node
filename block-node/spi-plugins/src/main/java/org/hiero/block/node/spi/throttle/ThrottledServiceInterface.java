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
import java.util.concurrent.atomic.AtomicReference;
import org.hiero.metrics.core.MetricRegistry;

/// Wraps a plugin's [ServiceInterface] with a single per-client rate/concurrency admission policy,
/// as described in `docs/design/apis/api-throttling.md`. For a service whose methods have more
/// than one cost tier (e.g. `getBlock`'s live-vs-historical distinction), see
/// [WeightedThrottledServiceInterface] instead — the admission-decision order, eviction, and
/// permit-lifecycle logic are shared between both via [SingleWeightThrottle].
public final class ThrottledServiceInterface implements ServiceInterface, StaleClientSweepable {
    private final ServiceInterface delegate;
    private final ClientKeyExtractor keyExtractor;
    private final SingleWeightThrottle throttle;

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
        this.keyExtractor = keyExtractor;
        this.throttle = new SingleWeightThrottle(
                policy, metricRegistry, "throttle_" + delegate.serviceName(), delegate.serviceName(), clientStateTtl);
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
        final AdmissionResult result = throttle.tryAdmit(clientKey, System.nanoTime());
        if (!result.admitted()) {
            replies.onError(new GrpcException(GrpcStatus.RESOURCE_EXHAUSTED, result.rejectionReason()));
            return Pipelines.noop();
        }
        return delegate.open(
                method, options, new ReleasingPipeline(replies, new AtomicReference<>(result.releasePermit())));
    }

    /// {@inheritDoc}
    @Override
    public int sweepStaleClients(final long nowNanos) {
        return throttle.sweepStaleClients(nowNanos);
    }
}
