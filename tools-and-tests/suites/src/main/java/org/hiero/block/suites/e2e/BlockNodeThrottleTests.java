// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.suites.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.pbj.grpc.client.helidon.PbjGrpcClient;
import com.hedera.pbj.grpc.client.helidon.PbjGrpcClientConfig;
import com.hedera.pbj.runtime.grpc.GrpcException;
import com.hedera.pbj.runtime.grpc.GrpcStatus;
import com.hedera.pbj.runtime.grpc.Pipeline;
import com.hedera.pbj.runtime.grpc.ServiceInterface;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import io.helidon.common.tls.Tls;
import io.helidon.http.Method;
import io.helidon.webclient.api.WebClient;
import io.helidon.webclient.grpc.GrpcClientProtocolConfig;
import io.helidon.webclient.http2.Http2Client;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.hiero.block.api.BlockAccessServiceInterface;
import org.hiero.block.api.BlockEnd;
import org.hiero.block.api.BlockItemSet;
import org.hiero.block.api.BlockNodeServiceInterface;
import org.hiero.block.api.BlockRequest;
import org.hiero.block.api.BlockResponse;
import org.hiero.block.api.BlockStreamPublishServiceInterface;
import org.hiero.block.api.BlockStreamSubscribeServiceInterface;
import org.hiero.block.api.PublishStreamRequest;
import org.hiero.block.api.PublishStreamResponse;
import org.hiero.block.api.ServerStatusRequest;
import org.hiero.block.api.ServerStatusResponse;
import org.hiero.block.api.SubscribeStreamRequest;
import org.hiero.block.api.SubscribeStreamResponse;
import org.hiero.block.node.app.BlockNodeApp;
import org.hiero.block.node.spi.ServiceLoaderFunction;
import org.hiero.block.node.spi.health.HealthFacility.State;
import org.hiero.block.suites.utils.BlockItemBuilderUtils;
import org.hiero.block.suites.utils.ResponsePipelineUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * E2E tests validating the admission-control / throttling mechanism (see
 * {@code docs/design/apis/api-throttling.md}) actually rejects and admits calls against a real,
 * fully-wired {@link BlockNodeApp} — not mocks. Each test overrides the relevant throttle
 * configuration to small, easy-to-exceed values via system properties (the same override mechanism
 * every plugin's config already supports) before starting the app, so tests can be short and
 * deterministic instead of racing real-world rate limits.
 * <p>
 * Simple, deterministic scenarios (a rate limit exceeded by two back-to-back calls, or two
 * independently-configured weight classes) are covered here. Scenarios that require holding a call
 * open concurrently with another one (node-wide concurrency ceilings, streaming-permit release on
 * cancellation or deadline) are stubbed with {@link Disabled} and a TODO describing the approach,
 * since they need either an artificially slow {@code HistoricalBlockFacility} or a way to
 * deterministically know the server has admitted a still-open call before firing a second one.
 */
@Tag("api")
public class BlockNodeThrottleTests {
    private static final String BLOCKS_DATA_DIR_PATH = "build/tmp/data";
    private static final Duration DEFAULT_AWAIT_TIMEOUT = Duration.ofSeconds(30);
    private static final ServerStatusRequest SIMPLE_SERVER_STATUS_REQUEST =
            ServerStatusRequest.newBuilder().build();
    private static final Options OPTIONS =
            new Options(Optional.empty(), ServiceInterface.RequestOptions.APPLICATION_GRPC);

    private final String serverPort = System.getenv("SERVER_PORT") == null ? "40840" : System.getenv("SERVER_PORT");

    private record Options(Optional<String> authority, String contentType) implements ServiceInterface.RequestOptions {}

    private BlockNodeApp app;
    /** Every system property key this test set, so {@link #afterEach()} can clear exactly those. */
    private final Set<String> overriddenPropertyKeys = new HashSet<>();

    @BeforeEach
    void beforeEach() throws IOException {
        final Path dataDir = Paths.get(BLOCKS_DATA_DIR_PATH).toAbsolutePath();
        if (Files.exists(dataDir)) {
            Files.walk(dataDir)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(java.io.File::delete);
        }
        BlockItemBuilderUtils.provisionTssBootstrap();
    }

    @AfterEach
    void afterEach() {
        if (app != null && app.blockNodeState() != State.SHUTTING_DOWN) {
            app.shutdown("BlockNodeThrottleTests", "test teardown");
        }
        overriddenPropertyKeys.forEach(System::clearProperty);
        overriddenPropertyKeys.clear();
    }

    /** Sets the given config overrides as system properties, then constructs and starts the app. */
    private void startApp(final Map<String, String> throttleOverrides) throws InterruptedException, IOException {
        throttleOverrides.forEach((key, value) -> {
            System.setProperty(key, value);
            overriddenPropertyKeys.add(key);
        });
        app = new BlockNodeApp(new ServiceLoaderFunction(), false);
        app.start();
        Thread.sleep(200); // short pause to allow async startup tasks to complete
        assertEquals(State.RUNNING, app.blockNodeState());
    }

    private PbjGrpcClient createGrpcClient() {
        final Duration timeoutDuration = Duration.ofSeconds(30);
        final Tls tls = Tls.builder().enabled(false).build();
        final WebClient webClient = WebClient.builder()
                .baseUri("http://localhost:" + serverPort)
                .tls(tls)
                .protocolConfigs(List.of(GrpcClientProtocolConfig.builder()
                        .abortPollTimeExpired(false)
                        .pollWaitTime(timeoutDuration)
                        .build()))
                .connectTimeout(timeoutDuration)
                .keepAlive(true)
                .build();
        final PbjGrpcClientConfig grpcConfig =
                new PbjGrpcClientConfig(timeoutDuration, tls, OPTIONS.authority(), OPTIONS.contentType());
        return new PbjGrpcClient(webClient, grpcConfig);
    }

    /** Publishes blocks {@code 0..count-1}, chained by block hash, and awaits the head block's acknowledgement. */
    private void publishBlocks(final int count) throws InterruptedException {
        final BlockStreamPublishServiceInterface.BlockStreamPublishServiceClient publishClient =
                new BlockStreamPublishServiceInterface.BlockStreamPublishServiceClient(createGrpcClient(), OPTIONS);
        final ResponsePipelineUtils<PublishStreamResponse> observer = new ResponsePipelineUtils<>();
        final Pipeline<? super PublishStreamRequest> stream = publishClient.publishBlockStream(observer);
        final long headBlock = count - 1L;
        final AtomicReference<CountDownLatch> ackLatch = observer.setAndGetOnMatchLatch(
                response -> response.response().kind() == PublishStreamResponse.ResponseOneOfType.ACKNOWLEDGEMENT
                        && response.acknowledgement().blockNumber() >= headBlock);
        Bytes previousBlockHash = null;
        for (long blockNumber = 0; blockNumber < count; blockNumber++) {
            final BlockItem[] items = BlockItemBuilderUtils.createSimpleBlockWithNumber(blockNumber, previousBlockHash);
            stream.onNext(PublishStreamRequest.newBuilder()
                    .blockItems(BlockItemSet.newBuilder().blockItems(items).build())
                    .build());
            stream.onNext(PublishStreamRequest.newBuilder()
                    .endOfBlock(BlockEnd.newBuilder().blockNumber(blockNumber).build())
                    .build());
            previousBlockHash = BlockItemBuilderUtils.computeBlockHash(blockNumber, previousBlockHash);
        }
        awaitLatch(ackLatch, "acknowledgement watermark covering blocks 0.." + headBlock);
        stream.closeConnection();
        publishClient.close();
    }

    private void awaitLatch(final AtomicReference<CountDownLatch> latch, final String description)
            throws InterruptedException {
        latch.get().await(DEFAULT_AWAIT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        assertEquals(0, latch.get().getCount(), "Timed out waiting for " + description);
    }

    /**
     * Repeatedly invokes {@code call} up to {@code attempts} times and asserts at least one
     * invocation is rejected by the throttle. A GCRA rate limit configured at its tightest possible
     * setting (1/second, no burst) still only guarantees rejection of a call arriving within the
     * same ~1-second window as the previous one — a single pair of back-to-back calls can, on a
     * loaded test machine, legitimately land more than a second apart. Repeating the attempt makes
     * the assertion robust to that scheduling jitter without weakening what it actually proves: the
     * throttle does reject calls, it isn't just always admitting everything.
     */
    private static void assertEventuallyRejectedByThrottle(final int attempts, final ThrowingRunnable call) {
        for (int attempt = 0; attempt < attempts; attempt++) {
            try {
                call.run();
            } catch (final RuntimeException e) {
                assertThat(e.getCause()).isInstanceOf(GrpcException.class);
                assertThat(((GrpcException) e.getCause()).status()).isEqualTo(GrpcStatus.RESOURCE_EXHAUSTED);
                return;
            }
        }
        fail("Expected at least one of " + attempts + " rapid back-to-back calls to be rejected by the throttle");
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run();
    }

    /**
     * Issues one bounded {@code subscribeBlockStream} call and returns the RESOURCE_EXHAUSTED
     * status the throttle rejected it with, or empty if it was admitted and completed normally.
     */
    private static Optional<GrpcStatus> subscribeOnce(
            final BlockStreamSubscribeServiceInterface.BlockStreamSubscribeServiceClient client,
            final long blockNumber) {
        final ResponsePipelineUtils<SubscribeStreamResponse> observer = new ResponsePipelineUtils<>();
        client.subscribeBlockStream(
                SubscribeStreamRequest.newBuilder()
                        .startBlockNumber(blockNumber)
                        .endBlockNumber(blockNumber)
                        .build(),
                observer);
        if (observer.getOnErrorCalls().isEmpty()) {
            return Optional.empty();
        }
        final Throwable error = observer.getOnErrorCalls().getFirst();
        assertThat(error).isInstanceOf(GrpcException.class);
        return Optional.of(((GrpcException) error).status());
    }

    /** Same robustness rationale as {@link #assertEventuallyRejectedByThrottle}, for the subscribe client. */
    private static void assertEventuallySubscribeRejected(
            final BlockStreamSubscribeServiceInterface.BlockStreamSubscribeServiceClient client,
            final long blockNumber,
            final int attempts) {
        for (int attempt = 0; attempt < attempts; attempt++) {
            final Optional<GrpcStatus> status = subscribeOnce(client, blockNumber);
            if (status.isPresent()) {
                assertThat(status.get()).isEqualTo(GrpcStatus.RESOURCE_EXHAUSTED);
                return;
            }
        }
        fail("Expected at least one of " + attempts
                + " rapid back-to-back subscriptions to be rejected by the throttle");
    }

    /**
     * Scrapes the node's OpenMetrics HTTP endpoint. This suite's {@code app-test.properties} doesn't
     * override the exporter's hostname/port, so it binds to the library default ({@code localhost:8888}),
     * not the production default ({@code app.properties} sets port 16007) — only enablement is overridden.
     */
    private static String scrapeMetrics() {
        final Http2Client http2Client =
                Http2Client.builder().baseUri("http://localhost:8888").build();
        try (var response =
                http2Client.method(Method.create("GET")).path("/metrics").request()) {
            assertEquals(200, response.status().code());
            return response.as(String.class);
        }
    }

    // ==== serverStatus (Phase 1) =========================================================================

    @Test
    @DisplayName("serverStatus: a second call faster than the configured rate is rejected")
    void serverStatusRateLimitRejectsSecondCallWithinWindow() throws InterruptedException, IOException {
        final Map<String, String> overrides = new HashMap<>();
        overrides.put("throttle.serverStatus.ratePerSecond", "1");
        overrides.put("throttle.serverStatus.burstTolerance", "0");
        overrides.put("throttle.serverStatus.maxConcurrentPerClient", "1000");
        startApp(overrides);

        final BlockNodeServiceInterface.BlockNodeServiceClient client =
                new BlockNodeServiceInterface.BlockNodeServiceClient(createGrpcClient(), OPTIONS);
        try {
            final ServerStatusResponse first = client.serverStatus(SIMPLE_SERVER_STATUS_REQUEST);
            assertNotNull(first);

            assertEventuallyRejectedByThrottle(10, () -> client.serverStatus(SIMPLE_SERVER_STATUS_REQUEST));
        } finally {
            client.close();
        }
    }

    @Test
    @DisplayName("serverStatus: burst tolerance admits a bounded number of rapid calls before rejecting")
    void serverStatusBurstToleranceAdmitsBoundedNumberOfRapidCalls() throws InterruptedException, IOException {
        final int burstTolerance = 3;
        final Map<String, String> overrides = new HashMap<>();
        overrides.put("throttle.serverStatus.ratePerSecond", "1");
        overrides.put("throttle.serverStatus.burstTolerance", String.valueOf(burstTolerance));
        overrides.put("throttle.serverStatus.maxConcurrentPerClient", "1000");
        startApp(overrides);

        final BlockNodeServiceInterface.BlockNodeServiceClient client =
                new BlockNodeServiceInterface.BlockNodeServiceClient(createGrpcClient(), OPTIONS);
        try {
            // burstTolerance pacing intervals of slack means burstTolerance + 1 rapid calls must
            // all be admitted before the throttle catches up — unlike every other rate-limit test
            // in this class, which uses burstTolerance=0 and only ever proves a single admit.
            for (int i = 0; i < burstTolerance + 1; i++) {
                assertNotNull(client.serverStatus(SIMPLE_SERVER_STATUS_REQUEST));
            }

            // The burst allowance is now exhausted; further rapid calls must eventually be rejected.
            assertEventuallyRejectedByThrottle(10, () -> client.serverStatus(SIMPLE_SERVER_STATUS_REQUEST));
        } finally {
            client.close();
        }
    }

    // ==== getBlock (Phase 2) =============================================================================

    @Test
    @DisplayName("getBlock: live and historical requests are rate-limited independently")
    void getBlockHistoricalAndLiveThrottledIndependently() throws InterruptedException, IOException {
        final Map<String, String> overrides = new HashMap<>();
        // The historical tier's rate is exhausted by its first call; the live tier is left generous
        // so a live call right after proves the two tiers don't share throttle state.
        overrides.put("throttle.getBlockHistorical.ratePerSecond", "1");
        overrides.put("throttle.getBlockHistorical.burstTolerance", "0");
        overrides.put("throttle.getBlockHistorical.historicalThresholdBlocks", "1");
        startApp(overrides);
        publishBlocks(5); // blocks 0..4; tip is block 4

        final BlockAccessServiceInterface.BlockAccessServiceClient client =
                new BlockAccessServiceInterface.BlockAccessServiceClient(createGrpcClient(), OPTIONS);
        try {
            // block 0 is 4 blocks behind the tip (> threshold of 1) -> HEAVY, first call admitted.
            final BlockResponse firstHistorical =
                    client.getBlock(BlockRequest.newBuilder().blockNumber(0L).build());
            assertEquals(BlockResponse.Code.SUCCESS, firstHistorical.status());

            // block 1 is also HEAVY, and the historical tier's rate is now exhausted.
            assertEventuallyRejectedByThrottle(
                    10,
                    () -> client.getBlock(
                            BlockRequest.newBuilder().blockNumber(1L).build()));

            // block 4 (the tip) is 0 blocks behind -> STANDARD/live, unaffected by the HEAVY rejection above.
            final BlockResponse liveResponse =
                    client.getBlock(BlockRequest.newBuilder().blockNumber(4L).build());
            assertEquals(BlockResponse.Code.SUCCESS, liveResponse.status());
        } finally {
            client.close();
        }
    }

    @Test
    @DisplayName("getBlock: a request exactly at the historical threshold is live; one block further is historical")
    void getBlockWeigherBoundaryClassifiesExactThresholdAsLiveAndOneBeyondAsHistorical()
            throws InterruptedException, IOException {
        final long threshold = 2;
        final Map<String, String> overrides = new HashMap<>();
        overrides.put("throttle.getBlockHistorical.ratePerSecond", "1");
        overrides.put("throttle.getBlockHistorical.burstTolerance", "0");
        overrides.put("throttle.getBlockHistorical.historicalThresholdBlocks", String.valueOf(threshold));
        startApp(overrides);
        final int blockCount = 5;
        publishBlocks(blockCount); // blocks 0..4; tip is block 4
        final long tip = blockCount - 1L;

        final BlockAccessServiceInterface.BlockAccessServiceClient client =
                new BlockAccessServiceInterface.BlockAccessServiceClient(createGrpcClient(), OPTIONS);
        try {
            // Exactly at the threshold (distance == threshold): classified STANDARD/live, so the
            // tight historical rate limit above must not apply — several rapid calls all succeed.
            final long boundaryBlock = tip - threshold;
            for (int i = 0; i < 3; i++) {
                final BlockResponse response = client.getBlock(
                        BlockRequest.newBuilder().blockNumber(boundaryBlock).build());
                assertEquals(BlockResponse.Code.SUCCESS, response.status());
            }

            // One block further behind (distance == threshold + 1): classified HEAVY/historical, so
            // the tight rate limit does apply.
            final long beyondThresholdBlock = boundaryBlock - 1;
            final BlockResponse firstHistorical = client.getBlock(
                    BlockRequest.newBuilder().blockNumber(beyondThresholdBlock).build());
            assertEquals(BlockResponse.Code.SUCCESS, firstHistorical.status());
            assertEventuallyRejectedByThrottle(
                    10,
                    () -> client.getBlock(BlockRequest.newBuilder()
                            .blockNumber(beyondThresholdBlock)
                            .build()));
        } finally {
            client.close();
        }
    }

    // TODO: node-wide (global) concurrency-ceiling rejection for getBlock needs an artificially slow
    // HistoricalBlockFacility (one whose block() call blocks on a latch this test controls) to hold a
    // permit open long enough to observe a concurrent second call being rejected — getBlock's real
    // in-memory path is too fast to race deterministically otherwise. Come back to this once such a
    // fixture exists.
    @Disabled("needs a controllable slow HistoricalBlockFacility fixture to hold a permit open; see TODO above")
    @Test
    @DisplayName("getBlock: the node-wide concurrency ceiling rejects once saturated")
    void getBlockGlobalConcurrencyCeilingRejectsWhenSaturated() {}

    // ==== Shared block-read bulkhead (Phase 4, Component B) ==============================================

    // TODO: this needs the same controllable slow HistoricalBlockFacility fixture as the stub above, plus
    // wiring it into both BlockAccessServicePlugin and the subscriber's historical catch-up path at once,
    // to prove getBlock and a subscriber session draw from and are capped by the *same* shared pool
    // regardless of how load is split between the two call paths (see docs/design/apis/api-throttling.md,
    // "Component B"). Real unit/plugin-level coverage of this already exists (BlockReadBulkheadTest,
    // BlockAccessServicePluginTest#getBlockRejectedWhenBulkheadExhausted,
    // SubscriberServicePluginTest#testHistoricalReadRetriesWhenBulkheadExhausted) — what's missing here is
    // specifically the *cross-plugin sharing* proof at full e2e fidelity.
    @Disabled("needs a controllable slow HistoricalBlockFacility fixture shared across two plugins; see TODO above")
    @Test
    @DisplayName("Shared block-read bulkhead: getBlock and subscriber catch-up reads draw from the same pool")
    void blockReadBulkheadIsSharedAcrossGetBlockAndSubscriberCatchUp() {}

    // ==== subscribeBlockStream (Phase 3) =================================================================

    @Test
    @DisplayName("subscribeBlockStream: a second subscription faster than the configured rate is rejected")
    void subscribeBlockStreamRateLimitRejectsSecondCallWithinWindow() throws InterruptedException, IOException {
        final Map<String, String> overrides = new HashMap<>();
        overrides.put("throttle.subscribe.liveRatePerSecond", "1");
        overrides.put("throttle.subscribe.liveBurstTolerance", "0");
        overrides.put("throttle.subscribe.liveMaxConcurrentPerClient", "1000");
        startApp(overrides);
        publishBlocks(1); // block 0, so a bounded subscription completes immediately

        final BlockStreamSubscribeServiceInterface.BlockStreamSubscribeServiceClient client =
                new BlockStreamSubscribeServiceInterface.BlockStreamSubscribeServiceClient(createGrpcClient(), OPTIONS);
        try {
            // A bounded, already-available request completes almost immediately once admitted, so
            // every attempt can run sequentially on the same (blocking) client method without
            // needing to hold a session open concurrently. As in the other rate-limit tests above, a
            // single pair of back-to-back calls can legitimately land more than the 1-second window
            // apart on a loaded test machine, so this repeats the attempt until one is rejected.
            boolean rejected = false;
            for (int attempt = 0; attempt < 10 && !rejected; attempt++) {
                final ResponsePipelineUtils<SubscribeStreamResponse> observer = new ResponsePipelineUtils<>();
                client.subscribeBlockStream(
                        SubscribeStreamRequest.newBuilder()
                                .startBlockNumber(0L)
                                .endBlockNumber(0L)
                                .build(),
                        observer);
                if (!observer.getOnErrorCalls().isEmpty()) {
                    assertThat(observer.getOnNextCalls()).isEmpty();
                    assertThat(observer.getOnErrorCalls()).hasSize(1);
                    final Throwable error = observer.getOnErrorCalls().getFirst();
                    assertThat(error).isInstanceOf(GrpcException.class);
                    assertThat(((GrpcException) error).status()).isEqualTo(GrpcStatus.RESOURCE_EXHAUSTED);
                    rejected = true;
                } else {
                    assertThat(observer.getOnCompleteCalls().get()).isEqualTo(1);
                }
            }
            assertThat(rejected)
                    .as("expected at least one of 10 rapid back-to-back subscriptions to be rejected by the throttle")
                    .isTrue();
        } finally {
            client.close();
        }
    }

    @Test
    @DisplayName("subscribeBlockStream: live and historical subscriptions are rate-limited independently")
    void subscribeBlockStreamHistoricalAndLiveThrottledIndependently() throws InterruptedException, IOException {
        final Map<String, String> overrides = new HashMap<>();
        // The historical tier's rate is exhausted by its first call; the live tier is left generous
        // so a live subscription right after proves the two tiers don't share throttle state.
        overrides.put("throttle.subscribe.historicalRatePerSecond", "1");
        overrides.put("throttle.subscribe.historicalBurstTolerance", "0");
        overrides.put("throttle.subscribe.historicalThresholdBlocks", "1");
        overrides.put("throttle.subscribe.liveRatePerSecond", "1000");
        overrides.put("throttle.subscribe.liveBurstTolerance", "1000");
        startApp(overrides);
        publishBlocks(5); // blocks 0..4; tip is block 4

        final BlockStreamSubscribeServiceInterface.BlockStreamSubscribeServiceClient client =
                new BlockStreamSubscribeServiceInterface.BlockStreamSubscribeServiceClient(createGrpcClient(), OPTIONS);
        try {
            // block 0 is 4 blocks behind the tip (> threshold of 1) -> HEAVY, first subscription admitted.
            assertThat(subscribeOnce(client, 0L)).isEmpty();

            // block 1 is also HEAVY, and the historical tier's rate is now exhausted.
            assertEventuallySubscribeRejected(client, 1L, 10);

            // block 4 (the tip) is 0 blocks behind -> STANDARD/live, unaffected by the HEAVY rejection above.
            assertThat(subscribeOnce(client, 4L)).isEmpty();
        } finally {
            client.close();
        }
    }

    @Test
    @DisplayName(
            "subscribeBlockStream: a start block exactly at the historical threshold is live; one further is historical")
    void subscribeBlockStreamWeigherBoundaryClassifiesExactThresholdAsLiveAndOneBeyondAsHistorical()
            throws InterruptedException, IOException {
        final long threshold = 2;
        final Map<String, String> overrides = new HashMap<>();
        overrides.put("throttle.subscribe.historicalRatePerSecond", "1");
        overrides.put("throttle.subscribe.historicalBurstTolerance", "0");
        overrides.put("throttle.subscribe.historicalThresholdBlocks", String.valueOf(threshold));
        overrides.put("throttle.subscribe.liveRatePerSecond", "1000");
        overrides.put("throttle.subscribe.liveBurstTolerance", "1000");
        startApp(overrides);
        final int blockCount = 5;
        publishBlocks(blockCount); // blocks 0..4; tip is block 4
        final long tip = blockCount - 1L;

        final BlockStreamSubscribeServiceInterface.BlockStreamSubscribeServiceClient client =
                new BlockStreamSubscribeServiceInterface.BlockStreamSubscribeServiceClient(createGrpcClient(), OPTIONS);
        try {
            // Exactly at the threshold (distance == threshold): classified STANDARD/live, so several
            // rapid subscriptions all succeed despite the tight historical rate limit above.
            final long boundaryBlock = tip - threshold;
            for (int i = 0; i < 3; i++) {
                assertThat(subscribeOnce(client, boundaryBlock)).isEmpty();
            }

            // One block further behind (distance == threshold + 1): classified HEAVY/historical, so
            // the tight rate limit does apply.
            final long beyondThresholdBlock = boundaryBlock - 1;
            assertThat(subscribeOnce(client, beyondThresholdBlock)).isEmpty();
            assertEventuallySubscribeRejected(client, beyondThresholdBlock, 10);
        } finally {
            client.close();
        }
    }

    // TODO: this is the highest-value remaining test for #3532's acceptance criteria (a concurrency
    // permit released exactly once for a session ended normally / cancelled / past its deadline), but
    // needs a deterministic way to know the server has admitted and registered a still-open live
    // session (subscribing to a not-yet-published future block, so the session blocks indefinitely)
    // before firing a second concurrent call from the same client — otherwise the second call could
    // race ahead of the first one's admission and the test would be flaky. Come back to this with
    // either a short bounded poll-and-retry or a white-box hook exposing open-session count.
    @Disabled("needs a deterministic signal that a held-open live session was admitted server-side; see TODO above")
    @Test
    @DisplayName("subscribeBlockStream: the per-client concurrency ceiling rejects a second concurrent live session")
    void subscribeBlockStreamConcurrencyLimitRejectsSecondConcurrentSession() {}

    @Disabled("needs a way to force client cancellation mid-stream from this harness; see #3532 acceptance criteria")
    @Test
    @DisplayName("subscribeBlockStream: a concurrency permit is released when the client cancels")
    void subscribeBlockStreamConcurrencyLimitReleasesPermitOnClientCancellation() {}

    @Disabled("needs a way to force a deadline-exceeded termination from this harness; see #3532 acceptance criteria")
    @Test
    @DisplayName("subscribeBlockStream: a concurrency permit is released when the call's deadline is exceeded")
    void subscribeBlockStreamConcurrencyLimitReleasesPermitOnDeadlineExceeded() {}

    // ==== Metrics (Phases 1-4) ============================================================================

    @Test
    @DisplayName("A throttle rejection and admission are reflected in the OpenMetrics counters")
    void throttleRejectionAndAdmissionAreReflectedInMetrics() throws InterruptedException, IOException {
        final Map<String, String> overrides = new HashMap<>();
        // The suite's app-test.properties disables the OpenMetrics HTTP endpoint by default (to
        // avoid port conflicts across parallel e2e runs); re-enable it for this test only.
        overrides.put("metrics.exporter.openmetrics.http.enabled", "true");
        overrides.put("throttle.serverStatus.ratePerSecond", "1");
        overrides.put("throttle.serverStatus.burstTolerance", "0");
        overrides.put("throttle.serverStatus.maxConcurrentPerClient", "1000");
        startApp(overrides);

        final BlockNodeServiceInterface.BlockNodeServiceClient client =
                new BlockNodeServiceInterface.BlockNodeServiceClient(createGrpcClient(), OPTIONS);
        try {
            assertNotNull(client.serverStatus(SIMPLE_SERVER_STATUS_REQUEST));
            assertEventuallyRejectedByThrottle(10, () -> client.serverStatus(SIMPLE_SERVER_STATUS_REQUEST));
        } finally {
            client.close();
        }

        // The gRPC response already proves the throttle admitted one call and rejected another; this
        // proves the *metrics* wiring independently records the same thing, via a different code path
        // that could silently regress without any other test noticing.
        final String metrics = scrapeMetrics();
        assertThat(metrics)
                .as("expected an admitted-calls counter for BlockNodeService with a nonzero value")
                .containsPattern("throttle_BlockNodeService_admitted_total\\S*\\s+[1-9]\\d*");
        assertThat(metrics)
                .as("expected a rejected-by-rate counter for BlockNodeService with a nonzero value")
                .containsPattern("throttle_BlockNodeService_rejected_rate_total\\S*\\s+[1-9]\\d*");
    }
}
