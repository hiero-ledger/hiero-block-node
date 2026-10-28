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
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.hiero.metrics.core.MetricRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Verifies the admission decision order and that the concurrency permit is released via the
/// *outgoing* `responses` pipeline passed into [ServiceInterface#open], not the pipeline `open()`
/// returns. The fake delegate's returned pipeline is never wired to anything, mimicking a real
/// server-streaming call, to prove the decorator doesn't depend on it.
class ThrottledServiceInterfaceTest {

    private static final ServiceInterface.Method ONLY_METHOD = () -> "onlyMethod";
    private static final ServiceInterface.Method OTHER_METHOD = () -> "otherMethod";

    private RecordingService recordingService;
    private MetricRegistry metricRegistry;
    private ThrottleMetrics throttleMetrics;

    @BeforeEach
    void setUp() {
        recordingService = new RecordingService();
        metricRegistry = MetricRegistry.builder()
                .setMetricsExporter(new NoOpMetricsExporter())
                .build();
        throttleMetrics = new ThrottleMetrics(metricRegistry);
    }

    @Test
    @DisplayName("An admitted call is passed through to the delegate")
    void admittedCallReachesDelegate() {
        final ThrottledServiceInterface throttled = throttledWith(100, 10, 5, 5);
        final CapturingPipeline replies = new CapturingPipeline();

        throttled.open(ONLY_METHOD, optionsFor("10.0.0.1"), replies);

        assertEquals(1, recordingService.openCalls);
        assertTrue(replies.errors.isEmpty());
    }

    @Test
    @DisplayName("The per-client concurrency ceiling rejects a second concurrent call from the same client")
    void perClientConcurrencyCeilingRejectsSecondCall() {
        final ThrottledServiceInterface throttled = throttledWith(100, 10, 1, 5);
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
        final ThrottledServiceInterface throttled = throttledWith(100, 10, 1, 5);
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
        final ThrottledServiceInterface throttled = throttledWith(100, 10, 1, 5);
        final CapturingPipeline firstCallReplies = new CapturingPipeline();

        final Pipeline<? super Bytes> returnedFromOpen =
                throttled.open(ONLY_METHOD, optionsFor("10.0.0.1"), firstCallReplies);

        // Simulates a server-streaming call where the returned pipeline completes while the call is
        // still in flight (e.g. the client half-closing). Must NOT release the permit.
        returnedFromOpen.onComplete();
        final CapturingPipeline stillBlockedReplies = new CapturingPipeline();
        throttled.open(ONLY_METHOD, optionsFor("10.0.0.1"), stillBlockedReplies);
        assertEquals(
                1, stillBlockedReplies.errors.size(), "the returned pipeline completing must not release the permit");

        // The outgoing responses pipeline is the correct completion signal and must release the permit.
        recordingService.capturedReplies.onComplete();
        final CapturingPipeline nowAdmittedReplies = new CapturingPipeline();
        throttled.open(ONLY_METHOD, optionsFor("10.0.0.1"), nowAdmittedReplies);
        assertTrue(nowAdmittedReplies.errors.isEmpty(), "completing the outgoing pipeline must release the permit");
        assertEquals(2, recordingService.openCalls, "only the two admitted calls should reach the delegate");
    }

    @Test
    @DisplayName("An error on the outgoing responses pipeline also releases the permit exactly once")
    void errorOnOutgoingPipelineReleasesPermitOnce() {
        final ThrottledServiceInterface throttled = throttledWith(100, 10, 1, 5);
        final CapturingPipeline firstCallReplies = new CapturingPipeline();
        throttled.open(ONLY_METHOD, optionsFor("10.0.0.1"), firstCallReplies);

        // A duplicate completion signal follows to prove the release is idempotent.
        recordingService.capturedReplies.onError(new RuntimeException("simulated cancellation"));
        recordingService.capturedReplies.onComplete(); // must be a no-op the second time

        final CapturingPipeline secondCallReplies = new CapturingPipeline();
        throttled.open(ONLY_METHOD, optionsFor("10.0.0.1"), secondCallReplies);
        assertTrue(secondCallReplies.errors.isEmpty(), "the permit must be released after the error");
    }

    @Test
    @DisplayName("The node-wide concurrency ceiling rejects calls once reached, regardless of client")
    void globalConcurrencyCeilingRejectsAcrossClients() {
        final ThrottledServiceInterface throttled = throttledWith(100, 10, 5, 1);
        final CapturingPipeline clientAReplies = new CapturingPipeline();
        final CapturingPipeline clientBReplies = new CapturingPipeline();

        throttled.open(ONLY_METHOD, optionsFor("10.0.0.1"), clientAReplies); // consumes the one global permit
        throttled.open(ONLY_METHOD, optionsFor("10.0.0.2"), clientBReplies); // different client, still rejected

        assertTrue(clientAReplies.errors.isEmpty());
        assertEquals(1, clientBReplies.errors.size());
    }

    @Test
    @DisplayName("Concurrent admissions never collectively overshoot the global concurrency ceiling")
    void concurrentAdmissionsNeverExceedGlobalCeiling() throws InterruptedException, BrokenBarrierException {
        // Regression test for a non-atomic check-then-increment race on the global ceiling. Rate and
        // per-client limits are set far above callerCount so only the global ceiling can reject, and
        // every caller is a distinct client making one call, so GCRA never fires either. Repeated
        // over several rounds, each with its own fresh gate; a CyclicBarrier releases every worker
        // simultaneously right before its one racy call to maximize real contention.
        final int ceiling = 5;
        final int callerCount = 200;
        final int rounds = 10;

        for (int round = 0; round < rounds; round++) {
            // Fresh registry per round: reusing one would throw registering the same metric twice.
            final MetricRegistry roundMetricRegistry = MetricRegistry.builder()
                    .setMetricsExporter(new NoOpMetricsExporter())
                    .build();
            final ThrottledServiceInterface throttled = new ThrottledServiceInterface(
                    recordingService,
                    Map.of(ONLY_METHOD.name(), new PerClientThrottleSettings(1_000_000, 1_000_000, callerCount)),
                    Optional.empty(),
                    Map.of(ONLY_METHOD.name(), ceiling),
                    new RemoteAddressKeyExtractor(roundMetricRegistry),
                    new ThrottleMetrics(roundMetricRegistry),
                    Duration.ofDays(1));
            final CyclicBarrier barrier = new CyclicBarrier(callerCount);
            final CountDownLatch doneLatch = new CountDownLatch(callerCount);
            final AtomicInteger admittedCount = new AtomicInteger();
            final ExecutorService executor = Executors.newFixedThreadPool(callerCount);
            try {
                for (int i = 0; i < callerCount; i++) {
                    final String clientIp = "10.0." + (i / 256) + "." + (i % 256);
                    executor.submit(() -> {
                        final CapturingPipeline replies = new CapturingPipeline();
                        final ServiceInterface.RequestOptions options = optionsFor(clientIp);
                        try {
                            barrier.await(10, TimeUnit.SECONDS);
                            throttled.open(ONLY_METHOD, options, replies);
                            if (replies.errors.isEmpty()) {
                                admittedCount.incrementAndGet();
                            }
                        } catch (final InterruptedException e) {
                            Thread.currentThread().interrupt();
                        } catch (final BrokenBarrierException | TimeoutException e) {
                            throw new IllegalStateException(e);
                        } finally {
                            doneLatch.countDown();
                        }
                    });
                }
                assertTrue(
                        doneLatch.await(10, TimeUnit.SECONDS),
                        "all concurrent callers should finish within the timeout");
            } finally {
                executor.shutdownNow();
            }

            // No permit is ever released, so exactly `ceiling` of `callerCount` attempts must succeed.
            assertEquals(
                    ceiling,
                    admittedCount.get(),
                    "round " + round + ": exactly " + ceiling + " of " + callerCount
                            + " concurrent calls should be admitted, no more");
        }
    }

    @Test
    @DisplayName("A client calling faster than its rate limit is rejected without reaching the delegate")
    void rateLimitRejectsFastCalls() {
        // Rate of 1/s with no burst tolerance and a generous concurrency ceiling isolates the rate check.
        final ThrottledServiceInterface throttled = throttledWith(1, 0, 100, 100);
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
        // 1/s with no burst tolerance: without TTL eviction resetting state, the second call would reject.
        final ThrottledServiceInterface throttled = throttledWith(1, 0, 100, 100, Duration.ofMillis(20));
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
    @DisplayName("A delegate open() that throws releases the permits on both the per-client and global-only paths")
    void throwingDelegateOpenReleasesPermits() {
        // ONLY_METHOD has per-client settings; OTHER_METHOD has only a node-wide ceiling. Every
        // ceiling is 1, so a leaked permit would reject the follow-up call.
        final ThrottledServiceInterface throttled = new ThrottledServiceInterface(
                recordingService,
                Map.of(ONLY_METHOD.name(), new PerClientThrottleSettings(100, 10, 1)),
                Optional.empty(),
                Map.of(ONLY_METHOD.name(), 1, OTHER_METHOD.name(), 1),
                new RemoteAddressKeyExtractor(metricRegistry),
                throttleMetrics,
                Duration.ofDays(1));

        for (final ServiceInterface.Method method : List.of(ONLY_METHOD, OTHER_METHOD)) {
            final GrpcException failure = new GrpcException(GrpcStatus.INVALID_ARGUMENT, "simulated");
            recordingService.failNextOpen = failure;
            final GrpcException thrown = assertThrows(
                    GrpcException.class, () -> throttled.open(method, optionsFor("10.0.0.1"), new CapturingPipeline()));
            assertEquals(failure, thrown, "the delegate's exception must be rethrown unchanged");

            final CapturingPipeline nextCallReplies = new CapturingPipeline();
            throttled.open(method, optionsFor("10.0.0.1"), nextCallReplies);
            assertTrue(
                    nextCallReplies.errors.isEmpty(),
                    method.name() + ": the permit taken by the failed open() must have been released");
        }
    }

    @Test
    @DisplayName("sweepStaleClients evicts idle entries but never one with a call still in flight")
    void sweepStaleClientsRespectsInFlightCalls() {
        final ThrottledServiceInterface throttled = throttledWith(100, 10, 5, 5, Duration.ofMillis(10));

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
    @DisplayName("A method with no configured settings falls back to the shared global ceiling only")
    void unconfiguredMethodFallsBackToGlobalCeilingOnly() {
        // OTHER_METHOD has no entry, so it must be ungated per-client and only subject to the global ceiling.
        final ThrottledServiceInterface throttled = new ThrottledServiceInterface(
                recordingService,
                Map.of(ONLY_METHOD.name(), new PerClientThrottleSettings(100, 10, 1)),
                Optional.empty(),
                Map.of(ONLY_METHOD.name(), 5, OTHER_METHOD.name(), 5),
                new RemoteAddressKeyExtractor(metricRegistry),
                throttleMetrics,
                Duration.ofDays(1));

        throttled.open(
                ONLY_METHOD, optionsFor("10.0.0.1"), new CapturingPipeline()); // consumes the one per-client permit
        final CapturingPipeline sameClientOtherMethod = new CapturingPipeline();
        throttled.open(OTHER_METHOD, optionsFor("10.0.0.1"), sameClientOtherMethod);

        assertTrue(
                sameClientOtherMethod.errors.isEmpty(),
                "an unconfigured method must not be gated by another method's per-client ceiling");
    }

    @Test
    @DisplayName("Each method has its own node-wide ceiling, not one shared across the service")
    void eachMethodHasItsOwnGlobalCeiling() {
        final ThrottledServiceInterface throttled = new ThrottledServiceInterface(
                recordingService,
                Map.of(
                        ONLY_METHOD.name(), new PerClientThrottleSettings(100, 10, 5),
                        OTHER_METHOD.name(), new PerClientThrottleSettings(100, 10, 5)),
                Optional.empty(),
                Map.of(ONLY_METHOD.name(), 1, OTHER_METHOD.name(), 1),
                new RemoteAddressKeyExtractor(metricRegistry),
                throttleMetrics,
                Duration.ofDays(1));

        throttled.open(ONLY_METHOD, optionsFor("10.0.0.1"), new CapturingPipeline()); // fills ONLY_METHOD's ceiling
        final CapturingPipeline otherMethodReplies = new CapturingPipeline();
        throttled.open(OTHER_METHOD, optionsFor("10.0.0.2"), otherMethodReplies);
        final CapturingPipeline sameMethodReplies = new CapturingPipeline();
        throttled.open(ONLY_METHOD, optionsFor("10.0.0.3"), sameMethodReplies);

        assertTrue(
                otherMethodReplies.errors.isEmpty(),
                "a sibling method's node-wide ceiling must be independent of this method's");
        assertEquals(1, sameMethodReplies.errors.size(), "the full method's own ceiling must still reject");
    }

    @Test
    @DisplayName("A per-client gate with no node-wide ceiling for its method fails fast at construction")
    void perClientGateWithoutCeilingFailsFast() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new ThrottledServiceInterface(
                        recordingService,
                        Map.of(ONLY_METHOD.name(), new PerClientThrottleSettings(100, 10, 1)),
                        Optional.empty(),
                        Map.of(),
                        new RemoteAddressKeyExtractor(metricRegistry),
                        throttleMetrics,
                        Duration.ofDays(1)));
    }

    @Test
    @DisplayName("A method with neither per-client settings nor a ceiling is not throttled")
    void methodWithNoConfigurationIsPassedThrough() {
        final ThrottledServiceInterface throttled = new ThrottledServiceInterface(
                recordingService,
                Map.of(),
                Optional.empty(),
                Map.of(),
                new RemoteAddressKeyExtractor(metricRegistry),
                throttleMetrics,
                Duration.ofDays(1));
        final CapturingPipeline replies = new CapturingPipeline();

        throttled.open(ONLY_METHOD, optionsFor("10.0.0.1"), replies);

        assertEquals(1, recordingService.openCalls);
        assertTrue(replies.errors.isEmpty());
    }

    @Test
    @DisplayName("Configured methods maintain independent per-client state for the same client")
    void configuredMethodsHaveIndependentPerClientStateForSameClient() {
        final ThrottledServiceInterface throttled = new ThrottledServiceInterface(
                recordingService,
                Map.of(
                        ONLY_METHOD.name(), new PerClientThrottleSettings(100, 10, 1),
                        OTHER_METHOD.name(), new PerClientThrottleSettings(100, 10, 1)),
                Optional.empty(),
                Map.of(ONLY_METHOD.name(), 5, OTHER_METHOD.name(), 5), // generous, so only per-client is under test
                new RemoteAddressKeyExtractor(metricRegistry),
                throttleMetrics,
                Duration.ofDays(1));

        // If the two methods incorrectly shared one client-state table, ONLY_METHOD's permit would
        // also exhaust OTHER_METHOD's for this same client.
        throttled.open(ONLY_METHOD, optionsFor("10.0.0.1"), new CapturingPipeline());
        final CapturingPipeline otherMethodReplies = new CapturingPipeline();
        throttled.open(OTHER_METHOD, optionsFor("10.0.0.1"), otherMethodReplies);

        assertTrue(
                otherMethodReplies.errors.isEmpty(),
                "a sibling method's per-client state must be independent, even for the same client");
    }

    @Test
    @DisplayName("defaultPerClientSettings gates an otherwise-unconfigured method instead of leaving it ungated")
    void defaultPerClientSettingsGatesUnconfiguredMethod() {
        final ThrottledServiceInterface throttled = new ThrottledServiceInterface(
                recordingService,
                Map.of(),
                Optional.of(new PerClientThrottleSettings(100, 10, 1)),
                Map.of(ONLY_METHOD.name(), 5, OTHER_METHOD.name(), 5),
                new RemoteAddressKeyExtractor(metricRegistry),
                throttleMetrics,
                Duration.ofDays(1));

        throttled.open(
                ONLY_METHOD, optionsFor("10.0.0.1"), new CapturingPipeline()); // consumes the default's one permit
        final CapturingPipeline secondCallReplies = new CapturingPipeline();
        throttled.open(ONLY_METHOD, optionsFor("10.0.0.1"), secondCallReplies);

        assertEquals(
                1, secondCallReplies.errors.size(), "the default settings must gate this method's per-client calls");
    }

    @Test
    @DisplayName("Reaping across methods releases only expired permits, and reports the smallest non-zero hold limit")
    void reapsExpiredPermitsAndReportsSmallestHoldLimit() {
        final ThrottledServiceInterface throttled = new ThrottledServiceInterface(
                recordingService,
                Map.of(
                        ONLY_METHOD.name(), new PerClientThrottleSettings(100, 10, 1, 60),
                        OTHER_METHOD.name(), new PerClientThrottleSettings(100, 10, 1, 10)),
                Optional.empty(),
                Map.of(ONLY_METHOD.name(), 5, OTHER_METHOD.name(), 5),
                new RemoteAddressKeyExtractor(metricRegistry),
                throttleMetrics,
                Duration.ofDays(1));
        throttled.open(ONLY_METHOD, optionsFor("10.0.0.1"), new CapturingPipeline());
        throttled.open(OTHER_METHOD, optionsFor("10.0.0.1"), new CapturingPipeline());

        assertEquals(10, throttled.smallestPermitHoldSeconds());
        final MetricRegistry otherRegistry = MetricRegistry.builder().build();
        final long later = System.nanoTime() + Duration.ofSeconds(11).toNanos();

        assertEquals(1, throttled.reapExpiredPermits(later), "only the method with the 10 s limit has expired");
        assertEquals(
                0,
                new ThrottledServiceInterface(
                                recordingService,
                                Map.of(ONLY_METHOD.name(), new PerClientThrottleSettings(100, 10, 1)),
                                Optional.empty(),
                                Map.of(ONLY_METHOD.name(), 5),
                                new RemoteAddressKeyExtractor(otherRegistry),
                                new ThrottleMetrics(otherRegistry),
                                Duration.ofDays(1))
                        .smallestPermitHoldSeconds(),
                "a service whose methods never reap reports 0");
    }

    private ThrottledServiceInterface throttledWith(
            final int ratePerSecond,
            final int burstTolerance,
            final int maxConcurrentPerClient,
            final int maxConcurrentGlobal) {
        // A TTL far longer than any test run keeps eviction out of the way of tests not exercising it.
        return throttledWith(
                ratePerSecond, burstTolerance, maxConcurrentPerClient, maxConcurrentGlobal, Duration.ofDays(1));
    }

    private ThrottledServiceInterface throttledWith(
            final int ratePerSecond,
            final int burstTolerance,
            final int maxConcurrentPerClient,
            final int maxConcurrentGlobal,
            final Duration clientStateTtl) {
        return new ThrottledServiceInterface(
                recordingService,
                Map.of(
                        ONLY_METHOD.name(),
                        new PerClientThrottleSettings(ratePerSecond, burstTolerance, maxConcurrentPerClient)),
                Optional.empty(),
                Map.of(ONLY_METHOD.name(), maxConcurrentGlobal),
                new RemoteAddressKeyExtractor(metricRegistry),
                throttleMetrics,
                clientStateTtl);
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

    /// A fake delegate that records every `open()` call and returns a fresh pipeline never wired to
    /// anything else, mirroring a real server-streaming call (see [ThrottledServiceInterface]).
    private static final class RecordingService implements ServiceInterface {
        private int openCalls;
        private Pipeline<? super Bytes> capturedReplies;
        /// When set, the next `open()` throws this instead of returning, then clears it.
        private RuntimeException failNextOpen;

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
            if (failNextOpen != null) {
                final RuntimeException failure = failNextOpen;
                failNextOpen = null;
                throw failure;
            }
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
