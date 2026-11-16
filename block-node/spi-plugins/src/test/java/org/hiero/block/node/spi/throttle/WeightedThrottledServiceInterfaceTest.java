// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.throttle;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hedera.pbj.runtime.grpc.GrpcException;
import com.hedera.pbj.runtime.grpc.GrpcStatus;
import com.hedera.pbj.runtime.grpc.Pipeline;
import com.hedera.pbj.runtime.grpc.ServiceInterface;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Flow;
import org.hiero.metrics.core.MetricRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Verifies [WeightedThrottledServiceInterface]: admission is decided in `onNext` from the weigher's
/// classification, weight classes are throttled independently, an unconfigured class falls back to
/// `STANDARD`, and a rejected call produces exactly one terminal signal.
class WeightedThrottledServiceInterfaceTest {
    private static final ServiceInterface.Method METHOD = () -> "getThing";
    private static final Bytes LIVE = Bytes.wrap(new byte[] {0});
    private static final Bytes HISTORICAL = Bytes.wrap(new byte[] {1});
    private static final MethodWeight STANDARD_KEY = new MethodWeight("getThing", WeightClass.STANDARD);
    private static final MethodWeight HEAVY_KEY = new MethodWeight("getThing", WeightClass.HEAVY);
    /// Rate high enough to never limit, so concurrency is the only active limit.
    private static final int HIGH_RATE = 100_000;

    private final ContentAwareWeigher weigher =
            (method, bytes) -> bytes.getByte(0) == 1 ? WeightClass.HEAVY : WeightClass.STANDARD;
    private RecordingService delegate;
    private ThrottleMetrics throttleMetrics;

    @BeforeEach
    void setUp() {
        delegate = new RecordingService();
        throttleMetrics = new ThrottleMetrics(MetricRegistry.builder()
                .setMetricsExporter(new NoOpMetricsExporter())
                .build());
    }

    private WeightedThrottledServiceInterface weighted(
            final Map<MethodWeight, PerClientThrottleSettings> settings, final Map<WeightClass, Integer> ceilings) {
        return new WeightedThrottledServiceInterface(
                delegate,
                settings,
                ceilings,
                new RemoteAddressKeyExtractor(MetricRegistry.builder()
                        .setMetricsExporter(new NoOpMetricsExporter())
                        .build()),
                weigher,
                throttleMetrics,
                Duration.ofMinutes(30));
    }

    private static PerClientThrottleSettings concurrency(final int maxConcurrent) {
        return new PerClientThrottleSettings(HIGH_RATE, 10, maxConcurrent, 0);
    }

    private static Pipeline<? super Bytes> call(
            final WeightedThrottledServiceInterface service,
            final String ip,
            final Bytes request,
            final CapturingPipeline replies) {
        final Pipeline<? super Bytes> inbound = service.open(METHOD, optionsFor(ip), replies);
        inbound.onNext(request);
        return inbound;
    }

    @Test
    @DisplayName("open() does not gate: the delegate is opened before the request bytes arrive")
    void openDelegatesImmediately() {
        final WeightedThrottledServiceInterface service =
                weighted(Map.of(STANDARD_KEY, concurrency(1)), Map.of(WeightClass.STANDARD, 10));
        service.open(METHOD, optionsFor("10.0.0.1"), new CapturingPipeline());
        assertEquals(1, delegate.openCalls);
        assertTrue(delegate.received.isEmpty());
    }

    @Test
    @DisplayName("An admitted call's request reaches the delegate")
    void admittedRequestReachesDelegate() {
        final WeightedThrottledServiceInterface service =
                weighted(Map.of(STANDARD_KEY, concurrency(1)), Map.of(WeightClass.STANDARD, 10));
        final CapturingPipeline replies = new CapturingPipeline();
        call(service, "10.0.0.1", LIVE, replies);
        assertEquals(List.of(LIVE), delegate.received);
        assertTrue(replies.errors.isEmpty());
    }

    @Test
    @DisplayName("Live and historical calls from the same client are throttled independently")
    void weightClassesAreIndependentForSameClient() {
        final WeightedThrottledServiceInterface service = weighted(
                Map.of(STANDARD_KEY, concurrency(5), HEAVY_KEY, concurrency(1)),
                Map.of(WeightClass.STANDARD, 10, WeightClass.HEAVY, 10));

        call(service, "10.0.0.1", HISTORICAL, new CapturingPipeline()); // holds the one HEAVY permit
        final CapturingPipeline secondHistorical = new CapturingPipeline();
        call(service, "10.0.0.1", HISTORICAL, secondHistorical);
        final CapturingPipeline live = new CapturingPipeline();
        call(service, "10.0.0.1", LIVE, live);

        assertEquals(1, secondHistorical.errors.size(), "second historical call exceeds the HEAVY ceiling");
        assertEquals(GrpcStatus.RESOURCE_EXHAUSTED, ((GrpcException) secondHistorical.errors.getFirst()).status());
        assertTrue(live.errors.isEmpty(), "a live call is unaffected by the historical limit");
    }

    @Test
    @DisplayName("A client that only requests live blocks is unaffected by a saturated historical tier")
    void liveOnlyClientUnaffectedByHistoricalLimits() {
        final WeightedThrottledServiceInterface service = weighted(
                Map.of(STANDARD_KEY, concurrency(5), HEAVY_KEY, concurrency(1)),
                Map.of(WeightClass.STANDARD, 10, WeightClass.HEAVY, 1));
        call(service, "10.0.0.2", HISTORICAL, new CapturingPipeline()); // saturates HEAVY node-wide
        final CapturingPipeline live = new CapturingPipeline();
        call(service, "10.0.0.1", LIVE, live);
        assertTrue(live.errors.isEmpty());
        assertEquals(2, delegate.received.size());
    }

    @Test
    @DisplayName("The node-wide ceiling of a weight class rejects calls from any client")
    void globalCeilingPerWeightClass() {
        final WeightedThrottledServiceInterface service = weighted(
                Map.of(STANDARD_KEY, concurrency(5), HEAVY_KEY, concurrency(5)),
                Map.of(WeightClass.STANDARD, 10, WeightClass.HEAVY, 1));
        call(service, "10.0.0.1", HISTORICAL, new CapturingPipeline());
        final CapturingPipeline otherClient = new CapturingPipeline();
        call(service, "10.0.0.2", HISTORICAL, otherClient);
        assertEquals(1, otherClient.errors.size());
    }

    @Test
    @DisplayName("A weight class with no entry falls back to STANDARD's settings and gate")
    void unconfiguredWeightClassFallsBackToStandard() {
        final WeightedThrottledServiceInterface service =
                weighted(Map.of(STANDARD_KEY, concurrency(1)), Map.of(WeightClass.STANDARD, 10));
        call(service, "10.0.0.1", LIVE, new CapturingPipeline()); // consumes STANDARD's one permit
        final CapturingPipeline historical = new CapturingPipeline();
        call(service, "10.0.0.1", HISTORICAL, historical);
        assertEquals(1, historical.errors.size(), "historical shares STANDARD's per-client ceiling");
    }

    @Test
    @DisplayName("A rejected call sends exactly one error and the delegate never sees it")
    void rejectedCallHasSingleTerminal() {
        final WeightedThrottledServiceInterface service =
                weighted(Map.of(STANDARD_KEY, concurrency(1)), Map.of(WeightClass.STANDARD, 10));
        call(service, "10.0.0.1", LIVE, new CapturingPipeline());
        final CapturingPipeline rejected = new CapturingPipeline();
        final Pipeline<? super Bytes> inbound = call(service, "10.0.0.1", LIVE, rejected);
        // The server will typically follow a request with half-close/complete; none may reach the delegate.
        inbound.clientEndStreamReceived();
        inbound.onComplete();

        assertEquals(1, rejected.errors.size());
        assertEquals(0, rejected.completes);
        assertEquals(1, delegate.received.size(), "the rejected request never reached the delegate");
        assertEquals(0, delegate.lastInbound.terminalCalls, "the delegate's inbound pipeline gets no terminal call");
    }

    @Test
    @DisplayName("Completing the outgoing replies pipeline releases the permit exactly once")
    void permitReleasedViaRepliesPipeline() {
        final WeightedThrottledServiceInterface service =
                weighted(Map.of(STANDARD_KEY, concurrency(1)), Map.of(WeightClass.STANDARD, 10));
        call(service, "10.0.0.1", LIVE, new CapturingPipeline());
        final CapturingPipeline blocked = new CapturingPipeline();
        call(service, "10.0.0.1", LIVE, blocked);
        assertEquals(1, blocked.errors.size());

        delegate.replies.getFirst().onComplete();
        delegate.replies.getFirst().onError(new RuntimeException("late duplicate terminal"));

        final CapturingPipeline afterRelease = new CapturingPipeline();
        call(service, "10.0.0.1", LIVE, afterRelease);
        assertTrue(afterRelease.errors.isEmpty(), "permit was released");
        final CapturingPipeline stillOneAtATime = new CapturingPipeline();
        call(service, "10.0.0.1", LIVE, stillOneAtATime);
        assertEquals(1, stillOneAtATime.errors.size(), "a duplicate terminal must not release a second permit");
    }

    @Test
    @DisplayName("A delegate that throws from onNext has its permit released")
    void permitReleasedWhenDelegateThrows() {
        final WeightedThrottledServiceInterface service =
                weighted(Map.of(STANDARD_KEY, concurrency(1)), Map.of(WeightClass.STANDARD, 10));
        delegate.failNextOnNext = true;
        final Pipeline<? super Bytes> inbound = service.open(METHOD, optionsFor("10.0.0.1"), new CapturingPipeline());
        assertThrows(IllegalStateException.class, () -> inbound.onNext(LIVE));

        final CapturingPipeline next = new CapturingPipeline();
        call(service, "10.0.0.1", LIVE, next);
        assertTrue(next.errors.isEmpty());
    }

    @Test
    @DisplayName("A method with no per-client settings is limited by the node-wide ceiling alone")
    void globalOnlyMethod() {
        final WeightedThrottledServiceInterface service = weighted(Map.of(), Map.of(WeightClass.STANDARD, 1));
        call(service, "10.0.0.1", LIVE, new CapturingPipeline());
        final CapturingPipeline other = new CapturingPipeline();
        call(service, "10.0.0.2", LIVE, other);
        assertEquals(1, other.errors.size());
    }

    @Test
    @DisplayName("Construction fails without a STANDARD entry for a configured method")
    void requiresStandardEntry() {
        assertThrows(
                IllegalArgumentException.class,
                () -> weighted(
                        Map.of(HEAVY_KEY, concurrency(1)), Map.of(WeightClass.STANDARD, 1, WeightClass.HEAVY, 1)));
    }

    @Test
    @DisplayName("Construction fails without a node-wide ceiling for a configured weight class")
    void requiresCeilingForConfiguredWeightClass() {
        assertThrows(
                IllegalArgumentException.class,
                () -> weighted(
                        Map.of(STANDARD_KEY, concurrency(1), HEAVY_KEY, concurrency(1)),
                        Map.of(WeightClass.STANDARD, 1)));
        assertThrows(IllegalArgumentException.class, () -> weighted(Map.of(), Map.of(WeightClass.HEAVY, 1)));
    }

    private static ServiceInterface.RequestOptions optionsFor(final String ipAddress) {
        final InetSocketAddress address;
        try {
            address = new InetSocketAddress(InetAddress.getByName(ipAddress), 40840);
        } catch (final java.net.UnknownHostException e) {
            throw new IllegalStateException(e);
        }
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

    private static final class RecordingService implements ServiceInterface {
        private int openCalls;
        private boolean failNextOnNext;
        private final List<Bytes> received = new ArrayList<>();
        private final List<Pipeline<? super Bytes>> replies = new ArrayList<>();
        private RecordingInbound lastInbound;

        @Override
        public String serviceName() {
            return "RecordingService";
        }

        @Override
        public String fullName() {
            return "test.RecordingService";
        }

        @Override
        public List<Method> methods() {
            return List.of(METHOD);
        }

        @Override
        public Pipeline<? super Bytes> open(
                final Method method, final RequestOptions options, final Pipeline<? super Bytes> responses) {
            openCalls++;
            replies.add(responses);
            lastInbound = new RecordingInbound();
            return lastInbound;
        }

        private final class RecordingInbound implements Pipeline<Bytes> {
            private int terminalCalls;

            @Override
            public void onSubscribe(final Flow.Subscription subscription) {}

            @Override
            public void onNext(final Bytes item) {
                if (failNextOnNext) {
                    failNextOnNext = false;
                    throw new IllegalStateException("boom");
                }
                received.add(item);
            }

            @Override
            public void onError(final Throwable throwable) {
                terminalCalls++;
            }

            @Override
            public void onComplete() {
                terminalCalls++;
            }

            @Override
            public void clientEndStreamReceived() {
                terminalCalls++;
            }
        }
    }

    private static final class CapturingPipeline implements Pipeline<Bytes> {
        private final List<Throwable> errors = new ArrayList<>();
        private int completes;

        @Override
        public void onSubscribe(final Flow.Subscription subscription) {}

        @Override
        public void onNext(final Bytes item) {}

        @Override
        public void onError(final Throwable throwable) {
            assertInstanceOf(Throwable.class, throwable);
            errors.add(throwable);
        }

        @Override
        public void onComplete() {
            completes++;
        }
    }

    private static final class NoOpMetricsExporter implements org.hiero.metrics.core.MetricsExporter {
        @Override
        public void setSnapshotSupplier(
                final java.util.function.Supplier<org.hiero.metrics.core.MetricRegistrySnapshot> supplier) {}

        @Override
        public void close() {}
    }
}
