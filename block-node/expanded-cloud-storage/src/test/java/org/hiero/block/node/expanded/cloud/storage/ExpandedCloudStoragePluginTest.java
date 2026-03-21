// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.expanded.cloud.storage;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hedera.bucky.S3ClientException;
import com.hedera.bucky.S3ResponseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import io.minio.GetObjectArgs;
import io.minio.ListObjectsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.errors.MinioException;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.app.fixtures.blocks.TestBlock;
import org.hiero.block.node.app.fixtures.blocks.TestBlockBuilder;
import org.hiero.block.node.app.fixtures.plugintest.PluginTestBase;
import org.hiero.block.node.app.fixtures.plugintest.SimpleInMemoryHistoricalBlockFacility;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.PersistedNotification;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.GenericContainer;

/**
 * Unit and integration tests for {@link ExpandedCloudStoragePlugin}.
 *
 * <ul>
 *   <li>Unit tests inject a {@link CapturingS3Client} or {@link NoOpS3Client} via the
 *       package-private constructor and call {@link ExpandedCloudStoragePlugin#handleVerification}
 *       directly — no real S3 endpoint required.</li>
 *   <li>Integration tests spin up a MinIO container via Testcontainers and use the production
 *       {@link BuckyS3ClientAdapter} via the no-arg constructor.</li>
 * </ul>
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS)
class ExpandedCloudStoragePluginTest
        extends PluginTestBase<ExpandedCloudStoragePlugin, ExecutorService, ScheduledExecutorService> {

    private static final Instant START_TIME =
            ZonedDateTime.of(2025, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant();
    private static final Duration ONE_DAY = Duration.of(1, ChronoUnit.DAYS);
    private static final String BUCKET_NAME = "test-expanded-blocks";
    private static final int MINIO_PORT = 9000;
    private static final String MINIO_USER = "minioadmin";
    private static final String MINIO_PASSWORD = "minioadmin";
    /** Millis to wait for virtual-thread upload tasks to complete in unit tests. */
    private static final long DRAIN_TIMEOUT_MS = 5_000L;

    // ---- Capturing S3 client for unit tests ---------------------------------

    private record UploadCall(String objectKey, String storageClass, String contentType) {}

    /**
     * Records {@code uploadFile} calls so unit tests can assert the exact arguments the plugin
     * passes to the S3 client without hitting a real endpoint.
     */
    private static class CapturingS3Client extends NoOpS3Client {
        final List<UploadCall> uploads = new java.util.ArrayList<>();

        @Override
        public void uploadFile(
                final String objectKey,
                final String storageClass,
                final Iterator<byte[]> contentIterable,
                final String contentType)
                throws S3ClientException, IOException {
            uploads.add(new UploadCall(objectKey, storageClass, contentType));
        }
    }

    // ---- MinIO fields for integration tests ---------------------------------

    private final MinioClient minioClient;
    private final String minioEndpoint;

    @SuppressWarnings("resource")
    public ExpandedCloudStoragePluginTest() throws GeneralSecurityException, IOException, MinioException {
        super(Executors.newSingleThreadExecutor(), Executors.newSingleThreadScheduledExecutor());

        final GenericContainer<?> minioContainer = new GenericContainer<>("minio/minio:latest")
                .withCommand("server /data")
                .withExposedPorts(MINIO_PORT)
                .withEnv("MINIO_ROOT_USER", MINIO_USER)
                .withEnv("MINIO_ROOT_PASSWORD", MINIO_PASSWORD);
        minioContainer.start();

        minioEndpoint = "http://" + minioContainer.getHost() + ":" + minioContainer.getMappedPort(MINIO_PORT);
        minioClient = MinioClient.builder()
                .endpoint(minioEndpoint)
                .credentials(MINIO_USER, MINIO_PASSWORD)
                .build();
        minioClient.makeBucket(MakeBucketArgs.builder().bucket(BUCKET_NAME).build());
    }

    // ---- Helpers ------------------------------------------------------------

    private TestBlock testBlock(final long blockNumber) {
        return TestBlockBuilder.generateBlocksInRange(blockNumber, blockNumber, START_TIME, ONE_DAY)
                .getFirst();
    }

    private VerificationNotification verifiedNotification(final long blockNumber, final BlockUnparsed block) {
        return new VerificationNotification(true, blockNumber, Bytes.EMPTY, block, BlockSource.UNKNOWN);
    }

    private VerificationNotification failedNotification(final long blockNumber) {
        return new VerificationNotification(false, blockNumber, Bytes.EMPTY, null, BlockSource.UNKNOWN);
    }

    // ---- Unit tests ---------------------------------------------------------

    @Test
    @DisplayName("Plugin is disabled when endpointUrl is blank — handleVerification is a no-op")
    void pluginDisabledWhenNoEndpoint() throws InterruptedException {
        final CapturingS3Client capturing = new CapturingS3Client();
        start(
                new ExpandedCloudStoragePlugin(capturing),
                new SimpleInMemoryHistoricalBlockFacility(),
                Map.of("expanded.cloud.storage.endpointUrl", ""));

        plugin.handleVerification(verifiedNotification(0L, testBlock(0).blockUnparsed()));
        plugin.awaitAndDrain(DRAIN_TIMEOUT_MS);

        assertEquals(0, capturing.uploads.size(), "No uploads when plugin is disabled");
        assertTrue(blockMessaging.getSentPersistedNotifications().isEmpty(), "No PersistedNotification when disabled");
    }

    @Test
    @DisplayName("Plugin skips upload for a failed VerificationNotification")
    void skipsUploadOnFailedVerification() throws InterruptedException {
        final CapturingS3Client capturing = new CapturingS3Client();
        start(
                new ExpandedCloudStoragePlugin(capturing),
                new SimpleInMemoryHistoricalBlockFacility(),
                Map.of(
                        "expanded.cloud.storage.endpointUrl", "http://fake:9000",
                        "expanded.cloud.storage.accessKey", MINIO_USER,
                        "expanded.cloud.storage.secretKey", MINIO_PASSWORD));

        plugin.handleVerification(failedNotification(0L));
        plugin.awaitAndDrain(DRAIN_TIMEOUT_MS);

        assertEquals(0, capturing.uploads.size(), "No upload for a failed notification");
    }

    @Test
    @DisplayName("Plugin calls uploadFile with correct folder-hierarchy key, storage class, and content type")
    void uploadsBlockWithCorrectParameters() throws InterruptedException {
        final CapturingS3Client capturing = new CapturingS3Client();
        start(
                new ExpandedCloudStoragePlugin(capturing),
                new SimpleInMemoryHistoricalBlockFacility(),
                Map.of(
                        "expanded.cloud.storage.endpointUrl", "http://fake:9000",
                        "expanded.cloud.storage.objectKeyPrefix", "myblocks",
                        "expanded.cloud.storage.storageClass", "STANDARD",
                        "expanded.cloud.storage.accessKey", MINIO_USER,
                        "expanded.cloud.storage.secretKey", MINIO_PASSWORD));

        plugin.handleVerification(verifiedNotification(0L, testBlock(0).blockUnparsed()));
        plugin.awaitAndDrain(DRAIN_TIMEOUT_MS);

        assertEquals(1, capturing.uploads.size(), "Exactly one uploadFile call expected");
        final UploadCall call = capturing.uploads.getFirst();
        // Block 0: 0000000000000000000 → 0000/0000/0000/0000/000
        assertEquals("myblocks/0000/0000/0000/0000/000.blk.zstd", call.objectKey());
        assertEquals("STANDARD", call.storageClass());
        assertEquals("application/octet-stream", call.contentType());
    }

    @Test
    @DisplayName("Object key uses 4-digit folder hierarchy for various block numbers")
    void objectKeyFolderHierarchy() throws InterruptedException {
        final CapturingS3Client capturing = new CapturingS3Client();
        start(
                new ExpandedCloudStoragePlugin(capturing),
                new SimpleInMemoryHistoricalBlockFacility(),
                Map.of(
                        "expanded.cloud.storage.endpointUrl", "http://fake:9000",
                        "expanded.cloud.storage.objectKeyPrefix", "blocks",
                        "expanded.cloud.storage.accessKey", MINIO_USER,
                        "expanded.cloud.storage.secretKey", MINIO_PASSWORD));

        // Block 1:         0000000000000000001 → blocks/0000/0000/0000/0000/001.blk.zstd
        // Block 108273182: 0000000000108273182 → blocks/0000/0000/0010/8273/182.blk.zstd
        // Block 1234567:   0000000000001234567 → blocks/0000/0000/0000/1234/567.blk.zstd
        plugin.handleVerification(verifiedNotification(1L, testBlock(1).blockUnparsed()));
        plugin.handleVerification(verifiedNotification(108273182L, testBlock(108273182L).blockUnparsed()));
        plugin.handleVerification(verifiedNotification(1234567L, testBlock(1234567L).blockUnparsed()));
        plugin.awaitAndDrain(DRAIN_TIMEOUT_MS);

        assertEquals(3, capturing.uploads.size());
        final Set<String> keys = capturing.uploads.stream().map(UploadCall::objectKey).collect(Collectors.toSet());
        assertTrue(keys.contains("blocks/0000/0000/0000/0000/001.blk.zstd"));
        assertTrue(keys.contains("blocks/0000/0000/0010/8273/182.blk.zstd"));
        assertTrue(keys.contains("blocks/0000/0000/0000/1234/567.blk.zstd"));
    }

    @Test
    @DisplayName("Object key with empty prefix omits the prefix segment")
    void objectKeyNoPrefix() throws InterruptedException {
        final CapturingS3Client capturing = new CapturingS3Client();
        start(
                new ExpandedCloudStoragePlugin(capturing),
                new SimpleInMemoryHistoricalBlockFacility(),
                Map.of(
                        "expanded.cloud.storage.endpointUrl", "http://fake:9000",
                        "expanded.cloud.storage.objectKeyPrefix", "",
                        "expanded.cloud.storage.accessKey", MINIO_USER,
                        "expanded.cloud.storage.secretKey", MINIO_PASSWORD));

        plugin.handleVerification(verifiedNotification(1L, testBlock(1).blockUnparsed()));
        plugin.awaitAndDrain(DRAIN_TIMEOUT_MS);

        assertEquals(1, capturing.uploads.size());
        assertEquals("0000/0000/0000/0000/001.blk.zstd", capturing.uploads.getFirst().objectKey());
    }

    @Test
    @DisplayName("Successful upload publishes PersistedNotification with succeeded=true")
    void successPublishesPersistedNotification() throws InterruptedException {
        final CapturingS3Client capturing = new CapturingS3Client();
        start(
                new ExpandedCloudStoragePlugin(capturing),
                new SimpleInMemoryHistoricalBlockFacility(),
                Map.of(
                        "expanded.cloud.storage.endpointUrl", "http://fake:9000",
                        "expanded.cloud.storage.accessKey", MINIO_USER,
                        "expanded.cloud.storage.secretKey", MINIO_PASSWORD));

        plugin.handleVerification(verifiedNotification(42L, testBlock(42).blockUnparsed()));
        plugin.awaitAndDrain(DRAIN_TIMEOUT_MS);

        final List<PersistedNotification> notifications = blockMessaging.getSentPersistedNotifications();
        assertEquals(1, notifications.size(), "Exactly one PersistedNotification expected");
        assertEquals(42L, notifications.getFirst().blockNumber());
        assertTrue(notifications.getFirst().succeeded(), "PersistedNotification must report succeeded=true");
    }

    @Test
    @DisplayName("S3ResponseException (HTTP 503) produces PersistedNotification with succeeded=false")
    void responseExceptionProducesFailedNotification() throws InterruptedException {
        final int statusCode = 503;
        final byte[] body = "Service Unavailable".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        final S3Client throwingClient = new NoOpS3Client() {
            @Override
            public void uploadFile(
                    final String objectKey,
                    final String storageClass,
                    final Iterator<byte[]> contentIterable,
                    final String contentType)
                    throws S3ClientException, IOException {
                throw new S3ResponseException(statusCode, body, null, "S3 returned 503");
            }
        };
        start(
                new ExpandedCloudStoragePlugin(throwingClient),
                new SimpleInMemoryHistoricalBlockFacility(),
                Map.of(
                        "expanded.cloud.storage.endpointUrl", "http://fake:9000",
                        "expanded.cloud.storage.accessKey", MINIO_USER,
                        "expanded.cloud.storage.secretKey", MINIO_PASSWORD));

        plugin.handleVerification(verifiedNotification(7L, testBlock(7).blockUnparsed()));
        plugin.awaitAndDrain(DRAIN_TIMEOUT_MS);

        final List<PersistedNotification> notifications = blockMessaging.getSentPersistedNotifications();
        assertEquals(1, notifications.size(), "PersistedNotification must be sent even on S3ResponseException");
        assertEquals(7L, notifications.getFirst().blockNumber());
        assertFalse(notifications.getFirst().succeeded(), "PersistedNotification must report succeeded=false on 503");
    }

    @Test
    @DisplayName("S3ResponseException (HTTP 403 Forbidden) is not rethrown by handleVerification")
    void responseExceptionForbiddenNotRethrown() {
        final S3Client throwingClient = new NoOpS3Client() {
            @Override
            public void uploadFile(
                    final String objectKey,
                    final String storageClass,
                    final Iterator<byte[]> contentIterable,
                    final String contentType)
                    throws S3ClientException, IOException {
                throw new S3ResponseException(
                        403,
                        "Forbidden".getBytes(java.nio.charset.StandardCharsets.UTF_8),
                        null,
                        "Access denied");
            }
        };
        start(
                new ExpandedCloudStoragePlugin(throwingClient),
                new SimpleInMemoryHistoricalBlockFacility(),
                Map.of(
                        "expanded.cloud.storage.endpointUrl", "http://fake:9000",
                        "expanded.cloud.storage.accessKey", MINIO_USER,
                        "expanded.cloud.storage.secretKey", MINIO_PASSWORD));

        assertDoesNotThrow(
                () -> plugin.handleVerification(verifiedNotification(0L, testBlock(0).blockUnparsed())),
                "S3ResponseException (403) must never propagate out of handleVerification");
    }

    @Test
    @DisplayName("S3ClientException thrown by uploadFile is not rethrown by handleVerification")
    void s3ExceptionNotRethrown() {
        final S3Client throwingClient = new NoOpS3Client() {
            @Override
            public void uploadFile(
                    final String objectKey,
                    final String storageClass,
                    final Iterator<byte[]> contentIterable,
                    final String contentType)
                    throws S3ClientException, IOException {
                throw new S3ClientException("Simulated base S3 failure");
            }
        };
        start(
                new ExpandedCloudStoragePlugin(throwingClient),
                new SimpleInMemoryHistoricalBlockFacility(),
                Map.of(
                        "expanded.cloud.storage.endpointUrl", "http://fake:9000",
                        "expanded.cloud.storage.accessKey", MINIO_USER,
                        "expanded.cloud.storage.secretKey", MINIO_PASSWORD));

        assertDoesNotThrow(
                () -> plugin.handleVerification(verifiedNotification(0L, testBlock(0).blockUnparsed())),
                "S3ClientException must never propagate out of handleVerification");
    }

    @Test
    @DisplayName("S3ResponseException carries HTTP status code, body, and is not rethrown")
    void responseExceptionDetailsAreNotRethrown() {
        // Verifies the richer S3ResponseException (from bucky) is fully swallowed regardless
        // of which HTTP status code (4xx or 5xx) the server returned.
        final int[] statusCodes = {400, 403, 404, 409, 500, 503};
        for (final int code : statusCodes) {
            final byte[] responseBody = ("Error " + code).getBytes(java.nio.charset.StandardCharsets.UTF_8);
            final S3Client throwingClient = new NoOpS3Client() {
                @Override
                public void uploadFile(
                        final String objectKey,
                        final String storageClass,
                        final Iterator<byte[]> contentIterable,
                        final String contentType)
                        throws S3ClientException, IOException {
                    final S3ResponseException ex = new S3ResponseException(code, responseBody, null);
                    // Verify the exception carries the expected details before throwing
                    assertEquals(code, ex.getResponseStatusCode());
                    assertTrue(ex.getResponseBody().length > 0);
                    throw ex;
                }
            };
            start(
                    new ExpandedCloudStoragePlugin(throwingClient),
                    new SimpleInMemoryHistoricalBlockFacility(),
                    Map.of(
                            "expanded.cloud.storage.endpointUrl", "http://fake:9000",
                            "expanded.cloud.storage.accessKey", MINIO_USER,
                            "expanded.cloud.storage.secretKey", MINIO_PASSWORD));

            assertDoesNotThrow(
                    () -> plugin.handleVerification(verifiedNotification(0L, testBlock(0).blockUnparsed())),
                    "S3ResponseException (HTTP " + code + ") must not propagate from handleVerification");
        }
    }

    @Test
    @DisplayName("Invalid storageClass is rejected at config construction time")
    void invalidStorageClassRejected() {
        org.junit.jupiter.api.Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> new ExpandedCloudStorageConfig(
                        "http://fake:9000", "bucket", "blocks", "INVALID_CLASS",
                        "us-east-1", "", "", 60, 4),
                "Invalid storageClass must throw IllegalArgumentException");
    }

    // ---- Integration tests (MinIO via Testcontainers) -----------------------

    @Test
    @DisplayName("Integration: blocks are uploaded using folder-hierarchy keys in MinIO")
    void integrationUploadSingleBlocks() throws InterruptedException {
        start(
                new ExpandedCloudStoragePlugin(),
                new SimpleInMemoryHistoricalBlockFacility(),
                Map.of(
                        "expanded.cloud.storage.endpointUrl", minioEndpoint,
                        "expanded.cloud.storage.bucketName", BUCKET_NAME,
                        "expanded.cloud.storage.objectKeyPrefix", "blocks",
                        "expanded.cloud.storage.accessKey", MINIO_USER,
                        "expanded.cloud.storage.secretKey", MINIO_PASSWORD));

        final List<TestBlock> blocks =
                TestBlockBuilder.generateBlocksInRange(100L, 104L, START_TIME, ONE_DAY);
        for (final TestBlock block : blocks) {
            plugin.handleVerification(verifiedNotification(block.number(), block.blockUnparsed()));
        }
        plugin.awaitAndDrain(DRAIN_TIMEOUT_MS);

        final Set<String> objects = listAllObjects();
        for (long i = 100L; i <= 104L; i++) {
            final String padded = String.format("%019d", i);
            final String expectedKey = "blocks/"
                    + padded.substring(0, 4) + "/" + padded.substring(4, 8) + "/"
                    + padded.substring(8, 12) + "/" + padded.substring(12, 16) + "/"
                    + padded.substring(16) + ".blk.zstd";
            assertTrue(objects.contains(expectedKey), "Expected object not found in MinIO: " + expectedKey);
        }
    }

    @Test
    @DisplayName("Integration: downloaded .blk.zstd is non-empty and PersistedNotification is published")
    void integrationUploadedObjectsAreNonEmptyAndNotified() throws Exception {
        start(
                new ExpandedCloudStoragePlugin(),
                new SimpleInMemoryHistoricalBlockFacility(),
                Map.of(
                        "expanded.cloud.storage.endpointUrl", minioEndpoint,
                        "expanded.cloud.storage.bucketName", BUCKET_NAME,
                        "expanded.cloud.storage.objectKeyPrefix", "intblocks",
                        "expanded.cloud.storage.accessKey", MINIO_USER,
                        "expanded.cloud.storage.secretKey", MINIO_PASSWORD));

        final TestBlock block = testBlock(200L);
        plugin.handleVerification(verifiedNotification(200L, block.blockUnparsed()));
        plugin.awaitAndDrain(DRAIN_TIMEOUT_MS);

        // Verify content is non-empty
        final String padded = String.format("%019d", 200L);
        final String key = "intblocks/"
                + padded.substring(0, 4) + "/" + padded.substring(4, 8) + "/"
                + padded.substring(8, 12) + "/" + padded.substring(12, 16) + "/"
                + padded.substring(16) + ".blk.zstd";
        final byte[] downloaded = minioClient
                .getObject(GetObjectArgs.builder().bucket(BUCKET_NAME).object(key).build())
                .readAllBytes();
        assertTrue(downloaded.length > 0, "Downloaded .blk.zstd must not be empty");

        // Verify PersistedNotification was published
        final List<PersistedNotification> notifications = blockMessaging.getSentPersistedNotifications();
        assertEquals(1, notifications.size(), "One PersistedNotification expected after integration upload");
        assertEquals(200L, notifications.getFirst().blockNumber());
        assertTrue(notifications.getFirst().succeeded(), "PersistedNotification must report succeeded=true");
    }

    // ---- Private helpers ----------------------------------------------------

    private Set<String> listAllObjects() {
        try {
            return StreamSupport.stream(
                            minioClient
                                    .listObjects(ListObjectsArgs.builder()
                                            .bucket(BUCKET_NAME)
                                            .recursive(true)
                                            .build())
                                    .spliterator(),
                            false)
                    .map(result -> {
                        try {
                            return result.get().objectName();
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    })
                    .collect(Collectors.toSet());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
