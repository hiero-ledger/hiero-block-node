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

/// Verifies the admission decision order and, most importantly, that the concurrency permit is
/// released via the *outgoing* `responses` pipeline passed into [ServiceInterface#open] rather
/// than the pipeline `open()` returns — the correctness detail called out in
/// `docs/design/apis/api-throttling.md`. A fake delegate returns a pipeline whose completion is
/// deliberately never wired to anything, exactly mimicking how a real server-streaming call's
/// returned pipeline behaves in production; the tests below prove the decorator does not depend
/// on it.
class ThrottledServiceInterfaceTest {

    private static final ServiceInterface.Method ONLY_METHOD = () -> "onlyMethod";
    private static final ServiceInterface.Method OTHER_METHOD = () -> "otherMethod";

    private RecordingService recordingService;
    private ThrottleMetrics throttleMetrics;

    @BeforeEach
    void setUp() {
        recordingService = new RecordingService();
        final MetricRegistry metricRegistry = MetricRegistry.builder()
                .setMetricsExporter(new NoOpMetricsExporter())
                .build();
        throttleMetrics = new ThrottleMetrics(metricRegistry);
    }

    @Test
    @DisplayName("An admitted call is passed through to the delegate")
    void admittedCallReachesDelegate() {
        final ThrottledServiceInterface throttled = throttledWith(new Tier(100, 10, 5, 5));
        final CapturingPipeline replies = new CapturingPipeline();

        throttled.open(ONLY_METHOD, optionsFor("10.0.0.1"), replies);

        assertEquals(1, recordingService.openCalls);
        assertTrue(replies.errors.isEmpty());
    }

    @Test
    @DisplayName("The per-client concurrency ceiling rejects a second concurrent call from the same client")
    void perClientConcurrencyCeilingRejectsSecondCall() {
        final ThrottledServiceInterface throttled = throttledWith(new Tier(100, 10, 1, 5));
        final CapturingPipeline firstCallReplies = new CapturingPipeline();
        final CapturingPipeline secondCallReplies = new CapturingPipeline();

        throttled.open(ONLY_METHOD, optionsFor("10.0.0.1"), firstCallReplies); // holds the one permit
        throttled.open(ONLY_METHOD, optionsFor("10.0.0.1"), secondCallReplies); // should be rejected

        assertEquals(1, recordingService.openCalls, "the rejected call must never reach the delegate");
        assertTrue(firstCallReplies.errors.isEmpty());
        assertEquals(1, secondCallReplies.errors.size());
        assertInstanceOf(GrpcException.class, secondCallReplies.errors.getFirst());
        assertEquals(GrpcStatus.RESOURCE_EXHAUSTED, ((GrpcException) secondCallReplies.errors.getFirst()).status());
    }

    @Test
    @DisplayName("A different client is not affected by another client's concurrency ceiling")
    void differentClientsHaveIndependentCeilings() {
        final ThrottledServiceInterface throttled = throttledWith(new Tier(100, 10, 1, 5));
        final CapturingPipeline clientAReplies = new CapturingPipeline();
        final CapturingPipeline clientBReplies = new CapturingPipeline();

        throttled.open(ONLY_METHOD, optionsFor("10.0.0.1"), clientAReplies);
        throttled.open(ONLY_METHOD, optionsFor("10.0.0.2"), clientBReplies);

        assertEquals(2, recordingService.openCalls);
        assertTrue(clientAReplies.errors.isEmpty());
        assertTrue(clientBReplies.errors.isEmpty());
    }

    @Test
    @DisplayName("Completing the outgoing responses pipeline releases the permit; completing the delegate's "
            + "unrelated returned pipeline does not")
    void permitReleaseAttachesToOutgoingPipelineNotTheReturnValue() {
        final ThrottledServiceInterface throttled = throttledWith(new Tier(100, 10, 1, 5));
        final CapturingPipeline firstCallReplies = new CapturingPipeline();

        final Pipeline<? super Bytes> returnedFromOpen =
                throttled.open(ONLY_METHOD, optionsFor("10.0.0.1"), firstCallReplies);

        // Simulate a server-streaming call: the delegate's returned pipeline completes (as if the
        // client had merely half-closed its request side) while the real call is still in flight.
        // This must NOT release the permit.
        returnedFromOpen.onComplete();
        final CapturingPipeline stillBlockedReplies = new CapturingPipeline();
        throttled.open(ONLY_METHOD, optionsFor("10.0.0.1"), stillBlockedReplies);
        assertEquals(
                1, stillBlockedReplies.errors.size(), "the returned pipeline completing must not release the permit");

        // Now complete the actual outgoing responses pipeline the decorator wrapped — this is the
        // correct, reliable completion signal, and must release the permit.
        recordingService.capturedReplies.onComplete();
        final CapturingPipeline nowAdmittedReplies = new CapturingPipeline();
        throttled.open(ONLY_METHOD, optionsFor("10.0.0.1"), nowAdmittedReplies);
        assertTrue(nowAdmittedReplies.errors.isEmpty(), "completing the outgoing pipeline must release the permit");
        assertEquals(2, recordingService.openCalls, "only the two admitted calls should reach the delegate");
    }

    @Test
    @DisplayName("An error on the outgoing responses pipeline also releases the permit exactly once")
    void errorOnOutgoingPipelineReleasesPermitOnce() {
        final ThrottledServiceInterface throttled = throttledWith(new Tier(100, 10, 1, 5));
        final CapturingPipeline firstCallReplies = new CapturingPipeline();
        throttled.open(ONLY_METHOD, optionsFor("10.0.0.1"), firstCallReplies);

        // Simulate the call failing/being cancelled, then simulate a duplicate completion signal
        // (e.g. cancel immediately followed by a business-logic error) to prove the release is
        // idempotent, not just eventually-consistent.
        recordingService.capturedReplies.onError(new RuntimeException("simulated cancellation"));
        recordingService.capturedReplies.onComplete(); // must be a no-op the second time

        final CapturingPipeline secondCallReplies = new CapturingPipeline();
        throttled.open(ONLY_METHOD, optionsFor("10.0.0.1"), secondCallReplies);
        assertTrue(secondCallReplies.errors.isEmpty(), "the permit must be released after the error");
    }

    @Test
    @DisplayName("The node-wide concurrency ceiling rejects calls once reached, regardless of client")
    void globalConcurrencyCeilingRejectsAcrossClients() {
        final ThrottledServiceInterface throttled = throttledWith(new Tier(100, 10, 5, 1));
        final CapturingPipeline clientAReplies = new CapturingPipeline();
        final CapturingPipeline clientBReplies = new CapturingPipeline();

        throttled.open(ONLY_METHOD, optionsFor("10.0.0.1"), clientAReplies); // consumes the one global permit
        throttled.open(ONLY_METHOD, optionsFor("10.0.0.2"), clientBReplies); // different client, still rejected

        assertTrue(clientAReplies.errors.isEmpty());
        assertEquals(1, clientBReplies.errors.size());
    }

    @Test
    @DisplayName("A client calling faster than its rate limit is rejected without reaching the delegate")
    void rateLimitRejectsFastCalls() {
        // Rate of 1/s with no burst tolerance and a generous concurrency ceiling isolates the rate check.
        final ThrottledServiceInterface throttled = throttledWith(new Tier(1, 0, 100, 100));
        final CapturingPipeline firstCallReplies = new CapturingPipeline();
        final CapturingPipeline secondCallReplies = new CapturingPipeline();

        throttled.open(ONLY_METHOD, optionsFor("10.0.0.1"), firstCallReplies);
        recordingService.capturedReplies.onComplete(); // free up the concurrency slot immediately
        throttled.open(ONLY_METHOD, optionsFor("10.0.0.1"), secondCallReplies); // arrives well within 1s

        assertTrue(firstCallReplies.errors.isEmpty());
        assertEquals(1, secondCallReplies.errors.size());
    }

    @Test
    @DisplayName("A client idle longer than the TTL gets a fresh rate-limit history on its next call")
    void lazyEvictionReplacesStaleClientState() throws InterruptedException {
        // Rate of 1/s with no burst tolerance: an immediate second call from the same client would
        // normally be rejected by the rate limiter, unless its state was reset by TTL-based eviction.
        final ThrottledServiceInterface throttled = throttledWith(new Tier(1, 0, 100, 100), Duration.ofMillis(20));
        throttled.open(ONLY_METHOD, optionsFor("10.0.0.1"), new CapturingPipeline());
        recordingService.capturedReplies.onComplete(); // free the concurrency slot

        Thread.sleep(50); // exceed the 20ms TTL

        final CapturingPipeline secondCallReplies = new CapturingPipeline();
        throttled.open(ONLY_METHOD, optionsFor("10.0.0.1"), secondCallReplies);

        assertTrue(
                secondCallReplies.errors.isEmpty(),
                "a fresh rate limiter after TTL-based eviction should admit this call despite the 1/s rate");
    }

    @Test
    @DisplayName("sweepStaleClients evicts idle entries but never one with a call still in flight")
    void sweepStaleClientsRespectsInFlightCalls() {
        final ThrottledServiceInterface throttled = throttledWith(new Tier(100, 10, 5, 5), Duration.ofMillis(10));

        // Client A: opens and completes immediately, so it is idle with nothing in flight.
        throttled.open(ONLY_METHOD, optionsFor("10.0.0.1"), new CapturingPipeline());
        recordingService.capturedReplies.onComplete();

        // Client B: opens and is deliberately left in flight (never completed).
        throttled.open(ONLY_METHOD, optionsFor("10.0.0.2"), new CapturingPipeline());

        final long farFuture = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        final int evicted = throttled.sweepStaleClients(farFuture);

        assertEquals(1, evicted, "only the idle client should be evicted; the in-flight client must be kept");
    }

    @Test
    @DisplayName("A method with no configured per-client settings is never rejected by a per-client limit")
    void unconfiguredMethodGetsNoPerClientGate() {
        final ThrottledServiceInterface throttled = throttledWithOnlyGlobalCeiling(100);

        for (int i = 0; i < 20; i++) {
            // Every one of these would be rejected by any reasonable per-client concurrency
            // ceiling, since none of them ever complete — but with no per-client gate configured,
            // only the generous global ceiling (100) applies.
            final CapturingPipeline replies = new CapturingPipeline();
            throttled.open(ONLY_METHOD, optionsFor("10.0.0.1"), replies);
            assertTrue(replies.errors.isEmpty(), "call " + i + " should be admitted; no per-client gate exists");
        }
    }

    @Test
    @DisplayName("Two independently configured methods get independent per-client concurrency ceilings")
    void independentlyConfiguredMethodsDoNotSharePerClientState() {
        final ThrottledServiceInterface throttled = new ThrottledServiceInterface(
                recordingService,
                Map.of(
                        new MethodWeight(ONLY_METHOD.name(), WeightClass.STANDARD),
                        new PerClientThrottleSettings(100, 10, 1),
                        new MethodWeight(OTHER_METHOD.name(), WeightClass.STANDARD),
                        new PerClientThrottleSettings(100, 10, 1)),
                Optional.empty(),
                100,
                new RemoteAddressKeyExtractor(),
                throttleMetrics,
                Duration.ofDays(1));

        // Exhaust ONLY_METHOD's one per-client concurrency slot for this client...
        throttled.open(ONLY_METHOD, optionsFor("10.0.0.1"), new CapturingPipeline());
        final CapturingPipeline secondOnlyMethodReplies = new CapturingPipeline();
        throttled.open(ONLY_METHOD, optionsFor("10.0.0.1"), secondOnlyMethodReplies);
        assertEquals(1, secondOnlyMethodReplies.errors.size(), "ONLY_METHOD's own ceiling should reject this");

        // ...but the same client calling OTHER_METHOD is unaffected, since it has its own table.
        final CapturingPipeline otherMethodReplies = new CapturingPipeline();
        throttled.open(OTHER_METHOD, optionsFor("10.0.0.1"), otherMethodReplies);
        assertTrue(
                otherMethodReplies.errors.isEmpty(),
                "OTHER_METHOD must not be affected by ONLY_METHOD's per-client ceiling");
    }

    @Test
    @DisplayName("Unconfigured methods on the same service share one global concurrency pool, not independent ones")
    void unconfiguredMethodsShareOneGlobalCeiling() {
        final ThrottledServiceInterface throttled = throttledWithOnlyGlobalCeiling(1);

        // Client A calls ONLY_METHOD, consuming the one global permit. Never completed, so it
        // stays held.
        throttled.open(ONLY_METHOD, optionsFor("10.0.0.1"), new CapturingPipeline());

        // A completely different client calling a completely different (also unconfigured)
        // method must still be rejected: the global ceiling is one shared pool for the whole
        // service, not one per method.
        final CapturingPipeline replies = new CapturingPipeline();
        throttled.open(OTHER_METHOD, optionsFor("10.0.0.2"), replies);

        assertEquals(
                1,
                replies.errors.size(),
                "the global ceiling must be shared across every method on this service, configured or not");
    }

    @Test
    @DisplayName("defaultPerClientSettings gates an otherwise-unconfigured method instead of leaving it global-only")
    void defaultPerClientSettingsGatesUnconfiguredMethods() {
        final ThrottledServiceInterface throttled = new ThrottledServiceInterface(
                recordingService,
                Map.of(
                        new MethodWeight(ONLY_METHOD.name(), WeightClass.STANDARD),
                        new PerClientThrottleSettings(100, 10, 5)),
                Optional.of(new PerClientThrottleSettings(100, 10, 1)), // default: 1 concurrent per client
                100,
                new RemoteAddressKeyExtractor(),
                throttleMetrics,
                Duration.ofDays(1));

        // OTHER_METHOD was never named in perClientSettings, but the default fills the gap, so it
        // gets a real per-client gate — not just the global-only fallback.
        throttled.open(OTHER_METHOD, optionsFor("10.0.0.1"), new CapturingPipeline());
        final CapturingPipeline secondReplies = new CapturingPipeline();
        throttled.open(OTHER_METHOD, optionsFor("10.0.0.1"), secondReplies);

        assertEquals(
                1,
                secondReplies.errors.size(),
                "the default per-client settings should give OTHER_METHOD a real concurrency ceiling of 1");
    }

    private ThrottledServiceInterface throttledWith(final Tier tier) {
        // A TTL far longer than any test could run keeps eviction (covered separately below) out of
        // the way of every test that isn't specifically exercising it.
        return throttledWith(tier, Duration.ofDays(1));
    }

    private ThrottledServiceInterface throttledWith(final Tier tier, final Duration clientStateTtl) {
        return new ThrottledServiceInterface(
                recordingService,
                Map.of(
                        new MethodWeight(ONLY_METHOD.name(), WeightClass.STANDARD),
                        new PerClientThrottleSettings(
                                tier.ratePerSecond(), tier.burstTolerance(), tier.maxConcurrentPerClient())),
                Optional.empty(),
                tier.maxConcurrentGlobal(),
                new RemoteAddressKeyExtractor(),
                throttleMetrics,
                clientStateTtl);
    }

    /// A method with no entry in `perClientSettings` and no `defaultPerClientSettings` gets no
    /// per-client gate at all — only the shared node-wide ceiling applies.
    private ThrottledServiceInterface throttledWithOnlyGlobalCeiling(final int maxConcurrentGlobal) {
        return new ThrottledServiceInterface(
                recordingService,
                Map.of(),
                Optional.empty(),
                maxConcurrentGlobal,
                new RemoteAddressKeyExtractor(),
                throttleMetrics,
                Duration.ofDays(1));
    }

    /// Per-client settings local to this test file — the equivalent of the production
    /// `PerClientThrottleSettings` plus the node-wide ceiling, bundled together purely so call
    /// sites here stay compact; production code keeps these two concerns separate (see
    /// [ThrottleSpec#globalConcurrencyCeilings] for why).
    private record Tier(int ratePerSecond, int burstTolerance, int maxConcurrentPerClient, int maxConcurrentGlobal) {}

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

    /// A fake delegate service that records every call to `open()` and, on each call, returns a
    /// fresh pipeline whose completion is never wired to anything else — mirroring how a real
    /// server-streaming call's returned pipeline behaves in production (see the class-level
    /// documentation on [ThrottledServiceInterface]).
    private static final class RecordingService implements ServiceInterface {
        private int openCalls;
        private Pipeline<? super Bytes> capturedReplies;
        private final Map<String, Pipeline<? super Bytes>> capturedRepliesByMethod = new java.util.HashMap<>();

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
            return List.of(ONLY_METHOD, OTHER_METHOD);
        }

        @Override
        public Pipeline<? super Bytes> open(
                final Method method, final RequestOptions options, final Pipeline<? super Bytes> replies) {
            openCalls++;
            this.capturedReplies = replies;
            capturedRepliesByMethod.put(method.name(), replies);
            return new InertPipeline();
        }
    }

    /// A pipeline that does nothing and whose completion is deliberately never observed by
    /// anything, standing in for the return value of a real server-streaming call's `open()`.
    private static final class InertPipeline implements Pipeline<Bytes> {
        @Override
        public void onSubscribe(final Flow.Subscription subscription) {}

        @Override
        public void onNext(final Bytes item) {}

        @Override
        public void onError(final Throwable throwable) {}

        @Override
        public void onComplete() {}
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
