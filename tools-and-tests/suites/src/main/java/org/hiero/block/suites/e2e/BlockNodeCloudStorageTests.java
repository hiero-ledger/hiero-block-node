// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.suites.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.adobe.testing.s3mock.testcontainers.S3MockContainer;
import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.pbj.grpc.client.helidon.PbjGrpcClient;
import com.hedera.pbj.grpc.client.helidon.PbjGrpcClientConfig;
import com.hedera.pbj.runtime.grpc.Pipeline;
import com.hedera.pbj.runtime.grpc.ServiceInterface;
import io.helidon.common.tls.Tls;
import io.helidon.webclient.api.WebClient;
import io.helidon.webclient.grpc.GrpcClientProtocolConfig;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.hiero.block.api.BlockEnd;
import org.hiero.block.api.BlockItemSet;
import org.hiero.block.api.BlockStreamPublishServiceInterface;
import org.hiero.block.api.PublishStreamRequest;
import org.hiero.block.api.PublishStreamResponse;
import org.hiero.block.node.app.BlockNodeApp;
import org.hiero.block.node.base.s3.S3Client;
import org.hiero.block.node.spi.ServiceLoaderFunction;
import org.hiero.block.node.spi.health.HealthFacility.State;
import org.hiero.block.suites.utils.BlockItemBuilderUtils;
import org.hiero.block.suites.utils.ResponsePipelineUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * E2E tests that exercise the full stack: gRPC publish → BlockNodeApp processing →
 * ExpandedCloudStoragePlugin upload → confirmed landing in S3Mock object storage.
 *
 * <p>S3Mock (Adobe, Apache 2.0) is used as the S3-compatible test endpoint. A fresh
 * S3Mock container is started for each test to provide clean bucket state.
 *
 * <p>Future migration: swap {@code S3MockContainer} for
 * {@code GenericContainer("chrislusf/seaweedfs")} with {@code .withCommand("server -s3")} to
 * use a real S3-compatible server (SeaweedFS, Apache 2.0). The {@link S3Client} verification
 * and all assertions remain identical.
 *
 * <p>Config injection uses the
 * {@link BlockNodeApp#BlockNodeApp(ServiceLoaderFunction, boolean, Function)} constructor
 * overload which accepts a custom {@code envVarGetter}, passing S3Mock coordinates into
 * {@code AutomaticEnvironmentVariableConfigSource} without touching JVM system properties or
 * real environment variables.
 */
@Tag("api")
@Timeout(value = 120, unit = TimeUnit.SECONDS)
class BlockNodeCloudStorageTests {

    private static final String BUCKET = "e2e-blocks";
    private static final String PREFIX = "blocks";
    private static final String ACCESS_KEY = "e2e-access-key";
    private static final String SECRET_KEY = "e2e-secret-key";
    private static final String S3MOCK_VERSION = "4.11.0";
    private static final Duration AWAIT_TIMEOUT = Duration.ofSeconds(30);
    private static final long S3_POLL_TIMEOUT = 15_000L;

    private static final String SERVER_PORT =
            System.getenv("SERVER_PORT") == null ? "40840" : System.getenv("SERVER_PORT");
    private static final Options OPTIONS =
            new Options(Optional.empty(), ServiceInterface.RequestOptions.APPLICATION_GRPC);

    private record Options(Optional<String> authority, String contentType) implements ServiceInterface.RequestOptions {}

    // Per-test instances
    private S3MockContainer s3MockContainer;
    private S3Client s3Client;
    private String s3Endpoint;
    private BlockNodeApp app;
    private PbjGrpcClient publishClient;

    @BeforeEach
    @SuppressWarnings("resource")
    void setUp() throws Exception {
        // Start a fresh S3Mock container for each test to ensure clean bucket state
        try {
            s3MockContainer = new S3MockContainer(S3MOCK_VERSION).withInitialBuckets(BUCKET);
            s3MockContainer.start();
        } catch (final IllegalStateException e) {
            assumeTrue(false, "Docker not available — skipping cloud storage E2E: " + e.getMessage());
            return;
        }
        s3Endpoint = s3MockContainer.getHttpEndpoint();
        s3Client = new S3Client("us-east-1", s3Endpoint, BUCKET, ACCESS_KEY, SECRET_KEY);

        // Inject S3Mock coordinates via envVarGetter so config picks them up without
        // touching real env vars or system properties.
        final Map<String, String> overrides = Map.of(
                "EXPANDED_CLOUD_STORAGE_ENDPOINT_URL", s3Endpoint,
                "EXPANDED_CLOUD_STORAGE_BUCKET_NAME", BUCKET,
                "EXPANDED_CLOUD_STORAGE_OBJECT_KEY_PREFIX", PREFIX,
                "EXPANDED_CLOUD_STORAGE_ACCESS_KEY", ACCESS_KEY,
                "EXPANDED_CLOUD_STORAGE_SECRET_KEY", SECRET_KEY);
        final Function<String, String> envVarGetter = key -> overrides.getOrDefault(key, System.getenv(key));

        // Clear local block data directory (same pattern as BlockNodeAPITests)
        final Path dataDir = Paths.get("build/tmp/data").toAbsolutePath();
        if (Files.exists(dataDir)) {
            Files.walk(dataDir)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        }

        app = new BlockNodeApp(new ServiceLoaderFunction(), false, envVarGetter);
        app.start();
        final long startDeadline = System.currentTimeMillis() + 10_000L;
        while (app.blockNodeState() != State.RUNNING && System.currentTimeMillis() < startDeadline) {
            Thread.sleep(50);
        }
        assertEquals(State.RUNNING, app.blockNodeState(), "BlockNodeApp must be RUNNING after startup");

        publishClient = createGrpcClient();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (app != null && app.blockNodeState() != State.SHUTTING_DOWN) {
            try {
                app.shutdown("BlockNodeCloudStorageTests", "teardown");
            } catch (final RuntimeException e) {
                // Suppress RejectedExecutionException and similar races that can occur when the
                // messaging thread pool is stopped before publisher handlers finish cleanup.
                // These are benign during test teardown — the JVM is not exiting here.
            }
        }
        if (s3Client != null) s3Client.close();
        if (s3MockContainer != null && s3MockContainer.isRunning()) s3MockContainer.stop();
    }

    // ── Tests ────────────────────────────────────────────────────────────────

    /**
     * Publishes block 0, waits for the block node acknowledgement, then polls S3Mock
     * until the {@code .blk.zstd} object appears. Asserts the key follows the
     * 4/4/4/4/3 folder hierarchy and that the object is present.
     *
     * <p>Sends a duplicate block at the end to force the server to close the connection,
     * then waits for connection closure. This ensures {@code @AfterEach} can call
     * {@code app.shutdown()} without active publisher handlers in the messaging facility.
     */
    @Test
    void blockIsUploadedToS3() throws Exception {
        final long blockNumber = 0L;
        final BlockItem[] items = BlockItemBuilderUtils.createSimpleBlockWithNumber(blockNumber);
        final PublishStreamRequest blockRequest = PublishStreamRequest.newBuilder()
                .blockItems(BlockItemSet.newBuilder().blockItems(items).build())
                .build();

        final BlockStreamPublishServiceInterface.BlockStreamPublishServiceClient publishSvc =
                new BlockStreamPublishServiceInterface.BlockStreamPublishServiceClient(publishClient, OPTIONS);
        final ResponsePipelineUtils<PublishStreamResponse> ackObserver = new ResponsePipelineUtils<>();
        final Pipeline<? super PublishStreamRequest> stream = publishSvc.publishBlockStream(ackObserver);

        final AtomicReference<CountDownLatch> ackLatch = ackObserver.setAndGetOnNextLatch(1);
        stream.onNext(blockRequest);
        endBlock(blockNumber, stream);
        awaitLatch(ackLatch, "block 0 acknowledgement");

        assertThat(ackObserver.getOnNextCalls())
                .hasSize(1)
                .first()
                .returns(PublishStreamResponse.ResponseOneOfType.ACKNOWLEDGEMENT, r -> r.response()
                        .kind())
                .returns(0L, r -> r.acknowledgement().blockNumber());

        // Poll S3Mock until the .blk.zstd object appears (upload is async after ack)
        final String expectedKey = buildExpectedKey(blockNumber);
        awaitS3Object(expectedKey);

        assertThat(listObjectKeys()).contains(expectedKey);

        // Trigger duplicate rejection so the server closes the connection; await closure
        // before returning so @AfterEach finds no active publisher handlers at shutdown.
        final AtomicReference<CountDownLatch> connClosedLatch = ackObserver.setAndGetConnectionEndedLatch(1);
        stream.onNext(blockRequest);
        awaitLatch(connClosedLatch, "connection closed by server after duplicate");
    }

    /**
     * Publishes block 0 twice. The first publish is acknowledged and uploaded.
     * The second (duplicate) causes the block node to close the publisher connection.
     * Asserts exactly one S3 object exists for the block — the duplicate must not
     * trigger a second upload.
     */
    @Test
    void onlyOneS3ObjectCreatedOnDuplicate() throws Exception {
        final long blockNumber = 0L;
        final BlockItem[] items = BlockItemBuilderUtils.createSimpleBlockWithNumber(blockNumber);
        final PublishStreamRequest request = PublishStreamRequest.newBuilder()
                .blockItems(BlockItemSet.newBuilder().blockItems(items).build())
                .build();

        final BlockStreamPublishServiceInterface.BlockStreamPublishServiceClient publishSvc =
                new BlockStreamPublishServiceInterface.BlockStreamPublishServiceClient(publishClient, OPTIONS);
        final ResponsePipelineUtils<PublishStreamResponse> ackObserver = new ResponsePipelineUtils<>();
        final Pipeline<? super PublishStreamRequest> stream = publishSvc.publishBlockStream(ackObserver);

        // First publish — expect acknowledgement
        final AtomicReference<CountDownLatch> ackLatch = ackObserver.setAndGetOnNextLatch(1);
        stream.onNext(request);
        endBlock(blockNumber, stream);
        awaitLatch(ackLatch, "first publish acknowledgement");

        // Confirm first upload landed in S3Mock
        final String expectedKey = buildExpectedKey(blockNumber);
        awaitS3Object(expectedKey);

        // Second publish (duplicate) — block node closes the connection
        final AtomicReference<CountDownLatch> dupClosedLatch = ackObserver.setAndGetConnectionEndedLatch(1);
        stream.onNext(request);
        awaitLatch(dupClosedLatch, "duplicate block connection closed");

        // Allow any residual async work to settle
        Thread.sleep(500);

        assertThat(listObjectKeys())
                .as("Exactly one S3 object must exist — duplicate rejection must not trigger a second upload")
                .containsExactly(expectedKey);
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    private static String buildExpectedKey(final long blockNumber) {
        final String p = String.format("%019d", blockNumber);
        return PREFIX + "/" + p.substring(0, 4) + "/" + p.substring(4, 8) + "/" + p.substring(8, 12) + "/"
                + p.substring(12, 16) + "/" + p.substring(16) + ".blk.zstd";
    }

    private void awaitS3Object(final String key) throws Exception {
        final long deadline = System.currentTimeMillis() + S3_POLL_TIMEOUT;
        while (System.currentTimeMillis() < deadline) {
            if (listObjectKeys().contains(key)) return;
            Thread.sleep(250);
        }
        throw new AssertionError("Timed out after " + S3_POLL_TIMEOUT + "ms waiting for S3 object: " + key);
    }

    private Set<String> listObjectKeys() {
        try {
            return Set.copyOf(s3Client.listObjects("", 1000));
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    private PbjGrpcClient createGrpcClient() {
        final Duration timeout = Duration.ofSeconds(30);
        final Tls tls = Tls.builder().enabled(false).build();
        final WebClient webClient = WebClient.builder()
                .baseUri("http://localhost:" + SERVER_PORT)
                .tls(tls)
                .protocolConfigs(List.of(GrpcClientProtocolConfig.builder()
                        .abortPollTimeExpired(false)
                        .pollWaitTime(timeout)
                        .build()))
                .connectTimeout(timeout)
                .keepAlive(true)
                .build();
        return new PbjGrpcClient(
                webClient, new PbjGrpcClientConfig(timeout, tls, OPTIONS.authority(), OPTIONS.contentType()));
    }

    private void endBlock(final long blockNumber, final Pipeline<? super PublishStreamRequest> stream) {
        stream.onNext(PublishStreamRequest.newBuilder()
                .endOfBlock(BlockEnd.newBuilder().blockNumber(blockNumber).build())
                .build());
    }

    private void awaitLatch(final AtomicReference<CountDownLatch> latch, final String desc)
            throws InterruptedException {
        latch.get().await(AWAIT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        assertEquals(0, latch.get().getCount(), "Timed out waiting for: " + desc);
    }
}
