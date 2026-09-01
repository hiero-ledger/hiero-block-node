// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.throttle;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hedera.pbj.runtime.grpc.GrpcException;
import com.hedera.pbj.runtime.grpc.GrpcStatus;
import com.hedera.pbj.runtime.grpc.Pipeline;
import com.hedera.pbj.runtime.grpc.ServiceInterface;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Flow;
import org.hiero.metrics.core.MetricRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Verifies content-aware weighting: that classification (and therefore admission) is deferred to
/// the wrapper pipeline's `onNext` — since the request bytes are not available inside `open()` —
/// and that each [WeightClass] is throttled independently for the same client, mirroring the
/// requirement that `getBlock` throttle historical requests more strictly than live ones.
class WeightedThrottledServiceInterfaceTest {

    private static final ServiceInterface.Method ONLY_METHOD = () -> "onlyMethod";
    private static final Bytes HEAVY_REQUEST = Bytes.wrap("heavy".getBytes(StandardCharsets.UTF_8));
    private static final Bytes STANDARD_REQUEST = Bytes.wrap("standard".getBytes(StandardCharsets.UTF_8));
    private static final ContentAwareWeigher WEIGHER =
            (method, requestBytes) -> HEAVY_REQUEST.equals(requestBytes) ? WeightClass.HEAVY : WeightClass.STANDARD;

    private RecordingWeightedService recordingService;
    private MetricRegistry metricRegistry;

    @BeforeEach
    void setUp() {
        recordingService = new RecordingWeightedService();
        metricRegistry = MetricRegistry.builder()
                .setMetricsExporter(new NoOpMetricsExporter())
                .build();
    }

    @Test
    @DisplayName("An admitted standard request reaches the delegate's business logic")
    void admittedStandardRequestReachesDelegate() {
        final WeightedThrottledServiceInterface throttled =
                throttledWith(new ThrottlePolicy(100, 10, 5, 5), new ThrottlePolicy(100, 10, 5, 5));
        final Pipeline<? super Bytes> inbound =
                throttled.open(ONLY_METHOD, optionsFor("10.0.0.1"), new CapturingPipeline());

        inbound.onNext(STANDARD_REQUEST);

        assertEquals(1, recordingService.businessLogicInvocations);
    }

    @Test
    @DisplayName("A rejected request never reaches the delegate's business logic")
    void rejectedRequestNeverReachesDelegate() {
        // maxConcurrentPerClient=0 for STANDARD means every standard call is rejected.
        final WeightedThrottledServiceInterface throttled =
                throttledWith(new ThrottlePolicy(100, 10, 0, 5), new ThrottlePolicy(100, 10, 5, 5));
        final CapturingPipeline replies = new CapturingPipeline();
        final Pipeline<? super Bytes> inbound = throttled.open(ONLY_METHOD, optionsFor("10.0.0.1"), replies);

        inbound.onNext(STANDARD_REQUEST);

        assertEquals(0, recordingService.businessLogicInvocations, "a rejected call must never reach the delegate");
        assertEquals(1, replies.errors.size());
        assertInstanceOf(GrpcException.class, replies.errors.getFirst());
        assertEquals(GrpcStatus.RESOURCE_EXHAUSTED, ((GrpcException) replies.errors.getFirst()).status());
    }

    @Test
    @DisplayName("Standard and heavy requests from the same client are throttled independently")
    void standardAndHeavyAreThrottledIndependently() {
        // HEAVY allows only 1 concurrent call; STANDARD allows 5. Both start from the same client.
        final WeightedThrottledServiceInterface throttled =
                throttledWith(new ThrottlePolicy(100, 10, 5, 5), new ThrottlePolicy(100, 10, 1, 5));

        // First heavy call holds HEAVY's one permit.
        final Pipeline<? super Bytes> firstHeavyInbound =
                throttled.open(ONLY_METHOD, optionsFor("10.0.0.1"), new CapturingPipeline());
        firstHeavyInbound.onNext(HEAVY_REQUEST);

        // A second heavy call from the same client is rejected...
        final CapturingPipeline secondHeavyReplies = new CapturingPipeline();
        final Pipeline<? super Bytes> secondHeavyInbound =
                throttled.open(ONLY_METHOD, optionsFor("10.0.0.1"), secondHeavyReplies);
        secondHeavyInbound.onNext(HEAVY_REQUEST);
        assertEquals(1, secondHeavyReplies.errors.size(), "HEAVY's own concurrency ceiling should reject this");

        // ...but a standard call from the same client, at the same time, is unaffected.
        final CapturingPipeline standardReplies = new CapturingPipeline();
        final Pipeline<? super Bytes> standardInbound =
                throttled.open(ONLY_METHOD, optionsFor("10.0.0.1"), standardReplies);
        standardInbound.onNext(STANDARD_REQUEST);
        assertTrue(standardReplies.errors.isEmpty(), "STANDARD's ceiling must not be affected by HEAVY's usage");
    }

    @Test
    @DisplayName("A permit is released via the outgoing pipeline, freeing that weight class for a new call")
    void permitReleaseFreesTheWeightClass() {
        final WeightedThrottledServiceInterface throttled =
                throttledWith(new ThrottlePolicy(100, 10, 5, 5), new ThrottlePolicy(100, 10, 1, 5));

        final Pipeline<? super Bytes> firstInbound =
                throttled.open(ONLY_METHOD, optionsFor("10.0.0.1"), new CapturingPipeline());
        firstInbound.onNext(HEAVY_REQUEST);
        recordingService.lastCapturedReplies.onComplete(); // free the one HEAVY permit

        // Since the permit was released, a second heavy call from the same client should be
        // admitted, not rejected.
        final CapturingPipeline secondReplies = new CapturingPipeline();
        final Pipeline<? super Bytes> secondInbound =
                throttled.open(ONLY_METHOD, optionsFor("10.0.0.1"), secondReplies);
        secondInbound.onNext(HEAVY_REQUEST);

        assertTrue(secondReplies.errors.isEmpty());
        assertEquals(2, recordingService.businessLogicInvocations);
    }

    private WeightedThrottledServiceInterface throttledWith(final ThrottlePolicy standard, final ThrottlePolicy heavy) {
        final Map<WeightClass, ThrottlePolicy> policies = new EnumMap<>(WeightClass.class);
        policies.put(WeightClass.STANDARD, standard);
        policies.put(WeightClass.HEAVY, heavy);
        return new WeightedThrottledServiceInterface(
                recordingService,
                policies,
                new RemoteAddressKeyExtractor(),
                WEIGHER,
                metricRegistry,
                Duration.ofDays(1));
    }

    private static ServiceInterface.RequestOptions optionsFor(final String ipAddress) {
        final InetSocketAddress address = new InetSocketAddress(loopbackLike(ipAddress), 40840);
        return new ServiceInterface.RequestOptions() {
            @Override
            public Optional<String> authority() {
                return Optional.empty();
            }

            @Override
            public String contentType() {
                return ServiceInterface.RequestOptions.APPLICATION_GRPC_PROTO;
            }

            @Override
            public java.net.SocketAddress remoteAddress() {
                return address;
            }
        };
    }

    private static InetAddress loopbackLike(final String ipAddress) {
        try {
            return InetAddress.getByName(ipAddress);
        } catch (final java.net.UnknownHostException e) {
            throw new IllegalStateException(e);
        }
    }

    /// A fake delegate whose returned pipeline, on `onNext`, records that business logic ran and
    /// captures the outgoing pipeline it was given — but does *not* auto-complete the call, so
    /// tests can hold a call "in flight" to exercise concurrency-ceiling rejection, and complete it
    /// explicitly (via [#lastCapturedReplies]) when they need to free the permit.
    private static final class RecordingWeightedService implements ServiceInterface {
        private int businessLogicInvocations;
        private Pipeline<? super Bytes> lastCapturedReplies;

        @Override
        public String serviceName() {
            return "RecordingWeightedService";
        }

        @Override
        public String fullName() {
            return "test.RecordingWeightedService";
        }

        @Override
        public List<Method> methods() {
            return List.of(ONLY_METHOD);
        }

        @Override
        public Pipeline<? super Bytes> open(
                final Method method, final RequestOptions options, final Pipeline<? super Bytes> replies) {
            return new Pipeline<Bytes>() {
                @Override
                public void onSubscribe(final Flow.Subscription subscription) {}

                @Override
                public void onNext(final Bytes item) {
                    businessLogicInvocations++;
                    lastCapturedReplies = replies;
                }

                @Override
                public void onError(final Throwable throwable) {}

                @Override
                public void onComplete() {}
            };
        }
    }

    /// Captures every error signaled to this pipeline, for assertions.
    private static final class CapturingPipeline implements Pipeline<Bytes> {
        private final List<Throwable> errors = new ArrayList<>();

        @Override
        public void onSubscribe(final Flow.Subscription subscription) {}

        @Override
        public void onNext(final Bytes item) {}

        @Override
        public void onError(final Throwable throwable) {
            errors.add(throwable);
        }

        @Override
        public void onComplete() {}
    }

    /// A no-op metrics exporter so tests don't need a real metrics backend.
    private static final class NoOpMetricsExporter implements org.hiero.metrics.core.MetricsExporter {
        @Override
        public void setSnapshotSupplier(
                final java.util.function.Supplier<org.hiero.metrics.core.MetricRegistrySnapshot> supplier) {}

        @Override
        public void close() {}
    }
}
