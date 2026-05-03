// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.cloud.storage.expanded;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.adobe.testing.s3mock.testcontainers.S3MockContainer;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.app.fixtures.blocks.TestBlock;
import org.hiero.block.node.app.fixtures.blocks.TestBlockBuilder;
import org.hiero.block.node.app.fixtures.plugintest.PluginTestBase;
import org.hiero.block.node.app.fixtures.plugintest.SimpleInMemoryHistoricalBlockFacility;
import org.hiero.block.node.base.CompressionType;
import org.hiero.block.node.base.s3.S3Client;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.PersistedNotification;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/// Integration tests for {@link ExpandedCloudStoragePlugin} using a real S3Mock container.
///
/// The S3Mock container is started once for the whole class via {@link BeforeAll}. If Docker
/// is not available the entire class is skipped via {@link org.junit.jupiter.api.Assumptions},
/// leaving the unit tests in {@link ExpandedCloudStoragePluginTest} unaffected.
///
/// Test-side S3 verification uses {@link S3Client} from `org.hiero.block.node.base`
/// which is already on the module path (`cloud-storage-expanded` requires `base`). No
/// additional library dependency is needed.
@Timeout(value = 60, unit = TimeUnit.SECONDS)
class ExpandedCloudStoragePluginIntegrationTest
        extends PluginTestBase<ExpandedCloudStoragePlugin, ExecutorService, ScheduledExecutorService> {

    private static final Instant START_TIME =
            ZonedDateTime.of(2025, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant();
    private static final Duration ONE_DAY = Duration.of(1, ChronoUnit.DAYS);
    private static final String BUCKET_NAME = "test-expanded-blocks";
    // TODO: this version must match s3MockVersion in hiero-dependency-versions/build.gradle.kts;
    //  there is no runtime mechanism to share it across test classpath boundaries.
    private static final String S3MOCK_VERSION = "4.11.0";
    private static final String ACCESS_KEY = "test-access-key";
    private static final String SECRET_KEY = "test-secret-key";

    private static S3MockContainer s3MockContainer;
    private static S3Client s3Client;
    private static String s3Endpoint;

    /// Starts the S3Mock container once for the entire test class.
    ///
    /// If Docker is not available the container start throws {@link IllegalStateException},
    /// which is caught and converted to an {@link org.junit.jupiter.api.Assumptions} skip so
    /// the unit tests in {@link ExpandedCloudStoragePluginTest} remain unaffected.
    @BeforeAll
    @SuppressWarnings("resource")
    static void startS3Mock() throws Exception {
        try {
            s3MockContainer = new S3MockContainer(S3MOCK_VERSION).withInitialBuckets(BUCKET_NAME);
            s3MockContainer.start();
        } catch (final IllegalStateException e) {
            assumeTrue(false, "Docker not available — skipping S3Mock integration tests: " + e.getMessage());
            return;
        }
        s3Endpoint = s3MockContainer.getHttpEndpoint();
        s3Client = new S3Client("us-east-1", s3Endpoint, BUCKET_NAME, ACCESS_KEY, SECRET_KEY);
    }

    /// Closes the test-side {@link S3Client} and stops the S3Mock container after all tests complete.
    @AfterAll
    static void stopS3Mock() {
        if (s3Client != null) s3Client.close();
        if (s3MockContainer != null && s3MockContainer.isRunning()) s3MockContainer.stop();
    }

    /// Provides the single-threaded executors required by {@link PluginTestBase}.
    public ExpandedCloudStoragePluginIntegrationTest() {
        super(Executors.newSingleThreadExecutor(), Executors.newSingleThreadScheduledExecutor());
    }

    // ---- Helpers ------------------------------------------------------------

    /// Generates a single {@link TestBlock} for the given block number using a fixed start time
    /// and one-day duration so block content is deterministic across test runs.
    private TestBlock testBlock(final long blockNumber) {
        return TestBlockBuilder.generateBlocksInRange(blockNumber, blockNumber, START_TIME, ONE_DAY)
                .getFirst();
    }

    /// Builds a {@link VerificationNotification} that reports {@code success=true} for the
    /// given block number and payload — the normal path that triggers an upload.
    private VerificationNotification verifiedNotification(final long blockNumber, final BlockUnparsed block) {
        return new VerificationNotification(true, null, blockNumber, Bytes.EMPTY, block, BlockSource.UNKNOWN);
    }

    // ---- Tests --------------------------------------------------------------

    @Test
    @DisplayName("Integration: blocks are uploaded using folder-hierarchy keys in S3Mock")
    void integrationUploadSingleBlocks() throws InterruptedException {
        start(
                new ExpandedCloudStoragePlugin(),
                new SimpleInMemoryHistoricalBlockFacility(),
                Map.of(
                        "cloud.storage.expanded.endpointUrl", s3Endpoint,
                        "cloud.storage.expanded.bucketName", BUCKET_NAME,
                        "cloud.storage.expanded.objectKeyPrefix", "blocks",
                        "cloud.storage.expanded.regionName", "us-east-1",
                        "cloud.storage.expanded.accessKey", ACCESS_KEY,
                        "cloud.storage.expanded.secretKey", SECRET_KEY));

        final List<TestBlock> blocks = TestBlockBuilder.generateBlocksInRange(100L, 104L, START_TIME, ONE_DAY);
        for (final TestBlock block : blocks) {
            plugin.handleVerification(verifiedNotification(block.number(), block.blockUnparsed()));
        }
        awaitNotifications(5);

        final Set<String> objects = listAllObjects("blocks/");
        for (long i = 100L; i <= 104L; i++) {
            final String padded = String.format("%019d", i);
            final String expectedKey = "blocks/"
                    + padded.substring(0, 4) + "/" + padded.substring(4, 8) + "/"
                    + padded.substring(8, 12) + "/" + padded.substring(12, 16) + "/"
                    + padded.substring(16) + ".blk.zstd";
            assertTrue(objects.contains(expectedKey), "Expected object not found in S3Mock: " + expectedKey);
        }
    }

    @Test
    @DisplayName("Integration: uploaded .blk.zstd is confirmed present and PersistedNotification is published")
    void integrationUploadedObjectsArePresentAndNotified() throws Exception {
        start(
                new ExpandedCloudStoragePlugin(),
                new SimpleInMemoryHistoricalBlockFacility(),
                Map.of(
                        "cloud.storage.expanded.endpointUrl", s3Endpoint,
                        "cloud.storage.expanded.bucketName", BUCKET_NAME,
                        "cloud.storage.expanded.objectKeyPrefix", "intblocks",
                        "cloud.storage.expanded.regionName", "us-east-1",
                        "cloud.storage.expanded.accessKey", ACCESS_KEY,
                        "cloud.storage.expanded.secretKey", SECRET_KEY));

        final TestBlock block = testBlock(200L);
        plugin.handleVerification(verifiedNotification(200L, block.blockUnparsed()));
        awaitNotifications(1);

        final String padded = String.format("%019d", 200L);
        final String key = "intblocks/"
                + padded.substring(0, 4) + "/" + padded.substring(4, 8) + "/"
                + padded.substring(8, 12) + "/" + padded.substring(12, 16) + "/"
                + padded.substring(16) + ".blk.zstd";
        assertTrue(listAllObjects("intblocks/").contains(key), "Uploaded .blk.zstd must be present in S3Mock: " + key);

        final List<PersistedNotification> notifications = blockMessaging.getSentPersistedNotifications();
        assertEquals(1, notifications.size(), "One PersistedNotification expected after upload");
        assertEquals(200L, notifications.getFirst().blockNumber());
        assertTrue(notifications.getFirst().succeeded(), "PersistedNotification must report succeeded=true");
    }

    @Test
    @DisplayName("Integration: empty objectKeyPrefix produces bare hierarchy key in S3Mock")
    void integrationUploadWithNoPrefix() throws InterruptedException {
        start(
                new ExpandedCloudStoragePlugin(),
                new SimpleInMemoryHistoricalBlockFacility(),
                Map.of(
                        "cloud.storage.expanded.endpointUrl", s3Endpoint,
                        "cloud.storage.expanded.bucketName", BUCKET_NAME,
                        "cloud.storage.expanded.objectKeyPrefix", "",
                        "cloud.storage.expanded.regionName", "us-east-1",
                        "cloud.storage.expanded.accessKey", ACCESS_KEY,
                        "cloud.storage.expanded.secretKey", SECRET_KEY));

        final TestBlock block = testBlock(300L);
        plugin.handleVerification(verifiedNotification(300L, block.blockUnparsed()));
        awaitNotifications(1);

        final String padded = String.format("%019d", 300L);
        final String expectedKey = padded.substring(0, 4) + "/" + padded.substring(4, 8) + "/"
                + padded.substring(8, 12) + "/" + padded.substring(12, 16) + "/"
                + padded.substring(16) + ".blk.zstd";
        assertTrue(
                listAllObjects("").contains(expectedKey),
                "Expected bare-hierarchy key (no prefix) not found in S3Mock: " + expectedKey);
    }

    @Test
    @DisplayName("Integration: downloaded .blk.zstd decompresses and parses back to the original block")
    void integrationContentRoundTrip() throws Exception {
        start(
                new ExpandedCloudStoragePlugin(),
                new SimpleInMemoryHistoricalBlockFacility(),
                Map.of(
                        "cloud.storage.expanded.endpointUrl", s3Endpoint,
                        "cloud.storage.expanded.bucketName", BUCKET_NAME,
                        "cloud.storage.expanded.objectKeyPrefix", "roundtrip",
                        "cloud.storage.expanded.regionName", "us-east-1",
                        "cloud.storage.expanded.accessKey", ACCESS_KEY,
                        "cloud.storage.expanded.secretKey", SECRET_KEY));

        final long blockNumber = 400L;
        final TestBlock block = testBlock(blockNumber);
        plugin.handleVerification(verifiedNotification(blockNumber, block.blockUnparsed()));
        awaitNotifications(1);

        // Build the expected S3 key
        final String padded = String.format("%019d", blockNumber);
        final String key = "roundtrip/"
                + padded.substring(0, 4) + "/" + padded.substring(4, 8) + "/"
                + padded.substring(8, 12) + "/" + padded.substring(12, 16) + "/"
                + padded.substring(16) + ".blk.zstd";

        // Download the raw bytes from S3Mock using a plain HTTP GET
        // S3Mock does not require request signing for GET in test mode.
        final String downloadUrl = s3Endpoint + "/" + BUCKET_NAME + "/" + key;
        final HttpClient http = HttpClient.newHttpClient();
        final HttpResponse<byte[]> response = http.send(
                HttpRequest.newBuilder().uri(URI.create(downloadUrl)).GET().build(),
                HttpResponse.BodyHandlers.ofByteArray());
        assertEquals(200, response.statusCode(), "S3Mock must return 200 for the uploaded object");

        // Decompress and parse — must round-trip back to the original block
        final byte[] decompressed = CompressionType.ZSTD.decompress(response.body());
        final BlockUnparsed parsed = BlockUnparsed.PROTOBUF.parseStrict(Bytes.wrap(decompressed));
        assertEquals(
                blockNumber,
                BlockHeader.PROTOBUF
                        .parse(parsed.blockItems().getFirst().blockHeaderOrThrow())
                        .number(),
                "Decompressed block header number must match the original block number");
    }

    /// Disabled because S3Mock is lenient about request authentication — it accepts any
    /// credential values and never returns a 403. Enable this test when running against a
    /// real S3-compatible endpoint (e.g. MinIO in strict mode) that enforces auth.
    @Disabled
    @Test
    @DisplayName("Integration: wrong credentials produce PersistedNotification with succeeded=false")
    void integrationWrongCredentialsProducesFailedNotification() throws InterruptedException {
        start(
                new ExpandedCloudStoragePlugin(),
                new SimpleInMemoryHistoricalBlockFacility(),
                Map.of(
                        "cloud.storage.expanded.endpointUrl", s3Endpoint,
                        "cloud.storage.expanded.bucketName", BUCKET_NAME,
                        "cloud.storage.expanded.objectKeyPrefix", "badcreds",
                        "cloud.storage.expanded.regionName", "us-east-1",
                        "cloud.storage.expanded.accessKey", "WRONG_KEY",
                        "cloud.storage.expanded.secretKey", "WRONG_SECRET"));

        plugin.handleVerification(verifiedNotification(500L, testBlock(500L).blockUnparsed()));
        awaitNotifications(1);

        final List<PersistedNotification> notifications = blockMessaging.getSentPersistedNotifications();
        assertEquals(1, notifications.size(), "Exactly one PersistedNotification must be sent even on auth failure");
        assertFalse(
                notifications.getFirst().succeeded(),
                "PersistedNotification must report succeeded=false when credentials are wrong");
    }

    @Test
    @DisplayName("Integration: 50 concurrent blocks all produce PersistedNotifications with succeeded=true")
    void integrationConcurrentUploadsAllNotified() throws InterruptedException {
        start(
                new ExpandedCloudStoragePlugin(),
                new SimpleInMemoryHistoricalBlockFacility(),
                Map.of(
                        "cloud.storage.expanded.endpointUrl", s3Endpoint,
                        "cloud.storage.expanded.bucketName", BUCKET_NAME,
                        "cloud.storage.expanded.objectKeyPrefix", "concurrent",
                        "cloud.storage.expanded.regionName", "us-east-1",
                        "cloud.storage.expanded.accessKey", ACCESS_KEY,
                        "cloud.storage.expanded.secretKey", SECRET_KEY));

        final List<TestBlock> blocks = TestBlockBuilder.generateBlocksInRange(600L, 649L, START_TIME, ONE_DAY);
        for (final TestBlock block : blocks) {
            plugin.handleVerification(verifiedNotification(block.number(), block.blockUnparsed()));
        }
        awaitNotifications(50);

        final List<PersistedNotification> notifications = blockMessaging.getSentPersistedNotifications();
        assertEquals(50, notifications.size(), "All 50 blocks must produce a PersistedNotification");
        assertTrue(
                notifications.stream().allMatch(PersistedNotification::succeeded),
                "All 50 notifications must report succeeded=true");
    }

    @Test
    @DisplayName("Integration: upload increments cloud_expanded metric counters correctly")
    void integrationUploadsCounterIncremented() throws InterruptedException {
        start(
                new ExpandedCloudStoragePlugin(),
                new SimpleInMemoryHistoricalBlockFacility(),
                Map.of(
                        "cloud.storage.expanded.endpointUrl", s3Endpoint,
                        "cloud.storage.expanded.bucketName", BUCKET_NAME,
                        "cloud.storage.expanded.objectKeyPrefix", "metrics",
                        "cloud.storage.expanded.regionName", "us-east-1",
                        "cloud.storage.expanded.accessKey", ACCESS_KEY,
                        "cloud.storage.expanded.secretKey", SECRET_KEY));

        plugin.handleVerification(verifiedNotification(700L, testBlock(700L).blockUnparsed()));
        awaitNotifications(1);

        assertEquals(
                1L,
                getMetricValue(ExpandedCloudStoragePlugin.METRIC_EXPANDED_CLOUD_STORAGE_TOTAL_UPLOADS),
                "cloud_expanded_total_uploads must be 1 after one successful upload");
        assertEquals(
                0L,
                getMetricValue(ExpandedCloudStoragePlugin.METRIC_EXPANDED_CLOUD_STORAGE_TOTAL_UPLOAD_FAILURES),
                "cloud_expanded_total_upload_failures must be 0 after a successful upload");
        assertTrue(
                getMetricValue(ExpandedCloudStoragePlugin.METRIC_EXPANDED_CLOUD_STORAGE_TOTAL_UPLOADED_BYTES) > 0L,
                "cloud_expanded_total_upload_bytes must be positive after a successful upload");
        assertTrue(
                getMetricValue(ExpandedCloudStoragePlugin.METRIC_EXPANDED_CLOUD_STORAGE_UPLOAD_LATENCY_NS) > 0L,
                "cloud_expanded_upload_latency_ns must be positive after a successful upload");
    }

    // ---- Private helpers ----------------------------------------------------

    /// Drives the plugin's drain loop and polls until at least {@code expectedCount}
    /// {@link PersistedNotification}s have been dispatched, or the 30-second timeout elapses.
    ///
    /// Uses the package-private {@link ExpandedCloudStoragePlugin#drainCompletedTasks()} so
    /// tests do not need a second {@code handleVerification} call to flush upload results.
    private void awaitNotifications(final int expectedCount) throws InterruptedException {
        final long deadline = System.currentTimeMillis() + 30_000L;
        while (System.currentTimeMillis() < deadline) {
            plugin.drainCompletedTasks();
            if (blockMessaging.getSentPersistedNotifications().size() >= expectedCount) return;
            Thread.sleep(50);
        }
    }

    /// Lists all object keys in S3Mock whose key starts with {@code prefix}.
    ///
    /// Pass an empty string to list the entire bucket (no prefix filter).
    private Set<String> listAllObjects(final String prefix) {
        try {
            return Set.copyOf(s3Client.listObjects(prefix.isEmpty() ? "" : prefix, 1000));
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }
}
