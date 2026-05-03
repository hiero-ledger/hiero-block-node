// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.suites.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.adobe.testing.s3mock.testcontainers.S3MockContainer;
import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.pbj.grpc.client.helidon.PbjGrpcClient;
import com.hedera.pbj.grpc.client.helidon.PbjGrpcClientConfig;
import com.hedera.pbj.runtime.grpc.Pipeline;
import com.hedera.pbj.runtime.grpc.ServiceInterface;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import io.helidon.common.tls.Tls;
import io.helidon.webclient.api.WebClient;
import io.helidon.webclient.grpc.GrpcClientProtocolConfig;
import java.io.File;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.hiero.block.api.BlockEnd;
import org.hiero.block.api.BlockItemSet;
import org.hiero.block.api.BlockStreamPublishServiceInterface;
import org.hiero.block.api.PublishStreamRequest;
import org.hiero.block.api.PublishStreamResponse;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.app.BlockNodeApp;
import org.hiero.block.node.base.CompressionType;
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
 * <p>Config injection uses {@code System.setProperty} with the raw config property names
 * (e.g. {@code cloud.storage.expanded.endpointUrl}) so that {@code SystemPropertiesConfigSource}
 * (ordinal 400) picks them up ahead of env-var and file sources.
 */
@Tag("api")
@Timeout(value = 120, unit = TimeUnit.SECONDS)
class BlockNodeCloudStorageTests {

    private static final String BUCKET = "e2e-blocks";
    private static final String PREFIX = "blocks";
    private static final String REGION = "us-east-1";
    private static final String ACCESS_KEY = "e2e-access-key";
    private static final String SECRET_KEY = "e2e-secret-key";
    // TODO: this version must match s3MockVersion in hiero-dependency-versions/build.gradle.kts;
    //  there is no runtime mechanism to share it across test classpath boundaries.
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
        final String s3Endpoint = s3MockContainer.getHttpEndpoint();
        s3Client = new S3Client(REGION, s3Endpoint, BUCKET, ACCESS_KEY, SECRET_KEY);

        // Inject S3Mock coordinates via System properties so that SystemPropertiesConfigSource
        // (ordinal 400) picks them up. This is the correct approach: anonymous-class overrides
        // of environment variables do not work because the captured local variables are null during the
        // BlockNodeApp super() constructor when AutomaticEnvironmentVariableConfigSource is built.
        setCloudExpandedProperties(s3Endpoint);

        // Clear local block data directory (same pattern as BlockNodeAPITests)
        final Path dataDir = Paths.get("build/tmp/data").toAbsolutePath();
        if (Files.exists(dataDir)) {
            Files.walk(dataDir)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        }

        app = new BlockNodeApp(new ServiceLoaderFunction(), false);
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
        clearCloudExpandedProperties();
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

    /**
     * Publishes three consecutive blocks (0, 1, 2) and asserts each lands in S3Mock
     * with the correct 4/4/4/4/3 folder-hierarchy key. This exercises sequential
     * key computation across block boundaries.
     */
    @Test
    void sequenceOfBlocksUploadedWithCorrectKeys() throws Exception {
        final long[] blockNumbers = {0L, 1L, 2L};

        final BlockStreamPublishServiceInterface.BlockStreamPublishServiceClient publishSvc =
                new BlockStreamPublishServiceInterface.BlockStreamPublishServiceClient(publishClient, OPTIONS);
        final ResponsePipelineUtils<PublishStreamResponse> ackObserver = new ResponsePipelineUtils<>();
        final Pipeline<? super PublishStreamRequest> stream = publishSvc.publishBlockStream(ackObserver);

        // Chain block hashes: each block's proof must reference the previous block's hash.
        // Without chaining, block 1+ fail verification because the verifier tracks block 0's
        // actual hash and rejects a proof that claims the previous hash was all-zeros.
        Bytes previousHash = null;
        Bytes prevHashForBlock2 = null; // captured for duplicate creation below
        for (final long blockNumber : blockNumbers) {
            if (blockNumber == 2L) prevHashForBlock2 = previousHash;
            final BlockItem[] items = BlockItemBuilderUtils.createSimpleBlockWithNumber(blockNumber, previousHash);
            final PublishStreamRequest request = PublishStreamRequest.newBuilder()
                    .blockItems(BlockItemSet.newBuilder().blockItems(items).build())
                    .build();
            final AtomicReference<CountDownLatch> ackLatch = ackObserver.setAndGetOnNextLatch(1);
            stream.onNext(request);
            endBlock(blockNumber, stream);
            awaitLatch(ackLatch, "acknowledgement for block " + blockNumber);
            previousHash = BlockItemBuilderUtils.computeBlockHash(blockNumber, previousHash);
        }

        // All three objects must appear in S3Mock
        for (final long blockNumber : blockNumbers) {
            final String expectedKey = buildExpectedKey(blockNumber);
            awaitS3Object(expectedKey);
            assertThat(listObjectKeys()).contains(expectedKey);
        }

        // Close the stream with a duplicate of block 2 (same content — same previousHash)
        final BlockItem[] dupItems = BlockItemBuilderUtils.createSimpleBlockWithNumber(2L, prevHashForBlock2);
        final PublishStreamRequest dupRequest = PublishStreamRequest.newBuilder()
                .blockItems(BlockItemSet.newBuilder().blockItems(dupItems).build())
                .build();
        final AtomicReference<CountDownLatch> connClosedLatch = ackObserver.setAndGetConnectionEndedLatch(1);
        stream.onNext(dupRequest);
        awaitLatch(connClosedLatch, "connection closed after duplicate of block 2");
    }

    /**
     * Publishes block 0, waits for it to land in S3Mock, then downloads and decompresses
     * the {@code .blk.zstd} object. Asserts the block number in the parsed protobuf matches
     * what was published. This is the strongest guard against a silent serialisation regression.
     */
    @Test
    void uploadedObjectDecompressesToOriginalBlock() throws Exception {
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
        awaitLatch(ackLatch, "block 0 acknowledgement for round-trip test");

        // Wait for the object to appear
        final String expectedKey = buildExpectedKey(blockNumber);
        awaitS3Object(expectedKey);

        // Download the raw bytes from S3Mock via plain HTTP GET
        // S3Mock is lenient about request signing in test environments.
        final String s3Endpoint = s3MockContainer.getHttpEndpoint();
        final String downloadUrl = s3Endpoint + "/" + BUCKET + "/" + expectedKey;
        final HttpClient http = HttpClient.newHttpClient();
        final HttpResponse<byte[]> response = http.send(
                HttpRequest.newBuilder().uri(URI.create(downloadUrl)).GET().build(),
                HttpResponse.BodyHandlers.ofByteArray());
        assertEquals(200, response.statusCode(), "S3Mock must return 200 for the uploaded object");

        // Decompress ZSTD and parse as BlockUnparsed protobuf
        final byte[] decompressed = CompressionType.ZSTD.decompress(response.body());
        final BlockUnparsed parsed = BlockUnparsed.PROTOBUF.parseStrict(Bytes.wrap(decompressed));
        assertEquals(
                blockNumber,
                BlockHeader.PROTOBUF
                        .parse(parsed.blockItems().getFirst().blockHeaderOrThrow())
                        .number(),
                "Decompressed block header number must match the published block number");

        // Close stream via duplicate
        final AtomicReference<CountDownLatch> connClosedLatch = ackObserver.setAndGetConnectionEndedLatch(1);
        stream.onNext(blockRequest);
        awaitLatch(connClosedLatch, "connection closed after duplicate for round-trip cleanup");
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    /**
     * Sets System properties for all cloud-storage-expanded config values so that
     * {@code SystemPropertiesConfigSource} (ordinal 400) injects them into the
     * {@link BlockNodeApp} config at startup.
     *
     * <p>This is the reliable alternative to overriding environment variables with an anonymous class
     * Anonymous-class captured local variables are null during the {@code BlockNodeApp} super() constructor
     * because Java assigns captured fields only after {@code super()} returns. System properties
     * are globally readable and are not subject to this timing issue.
     */
    private void setCloudExpandedProperties(final String endpoint) {
        System.setProperty("cloud.storage.expanded.endpointUrl", endpoint);
        System.setProperty("cloud.storage.expanded.bucketName", BUCKET);
        System.setProperty("cloud.storage.expanded.objectKeyPrefix", PREFIX);
        System.setProperty("cloud.storage.expanded.regionName", REGION);
        System.setProperty("cloud.storage.expanded.accessKey", ACCESS_KEY);
        System.setProperty("cloud.storage.expanded.secretKey", SECRET_KEY);
    }

    /** Clears all cloud-storage-expanded System properties set by {@link #setCloudExpandedProperties}. */
    private void clearCloudExpandedProperties() {
        System.clearProperty("cloud.storage.expanded.endpointUrl");
        System.clearProperty("cloud.storage.expanded.bucketName");
        System.clearProperty("cloud.storage.expanded.objectKeyPrefix");
        System.clearProperty("cloud.storage.expanded.regionName");
        System.clearProperty("cloud.storage.expanded.accessKey");
        System.clearProperty("cloud.storage.expanded.secretKey");
    }

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
