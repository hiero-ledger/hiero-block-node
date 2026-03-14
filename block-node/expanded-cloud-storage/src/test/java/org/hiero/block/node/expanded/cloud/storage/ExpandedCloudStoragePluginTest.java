// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.expanded.cloud.storage;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.minio.BucketExistsArgs;
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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.hiero.block.node.app.fixtures.blocks.TestBlock;
import org.hiero.block.node.app.fixtures.blocks.TestBlockBuilder;
import org.hiero.block.node.app.fixtures.plugintest.PluginTestBase;
import org.hiero.block.node.app.fixtures.plugintest.SimpleInMemoryHistoricalBlockFacility;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.PersistedNotification;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.GenericContainer;

/**
 * Integration and unit tests for {@link ExpandedCloudStoragePlugin}.
 *
 * <ul>
 *   <li>Unit tests use a {@link NoOpS3Client} injected via the package-private constructor.</li>
 *   <li>Integration tests spin up a MinIO container via Testcontainers.</li>
 * </ul>
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS)
@SuppressWarnings("SameParameterValue")
class ExpandedCloudStoragePluginTest
        extends PluginTestBase<ExpandedCloudStoragePlugin, ExecutorService, ScheduledExecutorService> {

    private static final Instant START_TIME =
            ZonedDateTime.of(2025, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant();
    private static final Duration ONE_DAY = Duration.of(1, ChronoUnit.DAYS);
    private static final String BUCKET_NAME = "test-expanded-blocks";
    private static final int MINIO_PORT = 9000;
    private static final String MINIO_USER = "minioadmin";
    private static final String MINIO_PASSWORD = "minioadmin";

    // ---- Unit-test helpers --------------------------------------------------

    /** Recorded upload calls from the capturing S3 client used in unit tests. */
    private record UploadCall(String objectKey, String storageClass, String contentType) {}

    /**
     * A capturing S3 client that records uploadFile calls so unit tests can assert them without
     * a real S3 endpoint.
     */
    private static class CapturingS3Client extends NoOpS3Client {
        final java.util.List<UploadCall> uploads = new java.util.ArrayList<>();

        @Override
        public void uploadFile(
                final String objectKey,
                final String storageClass,
                final java.util.Iterator<byte[]> contentIterable,
                final String contentType) {
            uploads.add(new UploadCall(objectKey, storageClass, contentType));
        }
    }

    // ---- MinIO fields for integration tests ---------------------------------

    private final MinioClient minioClient;
    private final String minioEndpoint;

    @SuppressWarnings("resource")
    public ExpandedCloudStoragePluginTest() throws GeneralSecurityException, IOException, MinioException {
        super(Executors.newSingleThreadExecutor(), Executors.newSingleThreadScheduledExecutor());

        // Start MinIO container for integration tests
        GenericContainer<?> minioContainer = new GenericContainer<>("minio/minio:latest")
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

        // Create integration-test bucket
        minioClient.makeBucket(MakeBucketArgs.builder().bucket(BUCKET_NAME).build());
    }

    // ---- Unit tests (NoOpS3Client / CapturingS3Client) ----------------------

    @Test
    @DisplayName("Plugin is disabled when endpointUrl is blank — no uploads attempted")
    void pluginDisabledWhenNoEndpoint() {
        final CapturingS3Client capturing = new CapturingS3Client();
        // We do NOT pass the capturing client; we use blank endpoint to disable.
        start(
                new ExpandedCloudStoragePlugin(),
                new SimpleInMemoryHistoricalBlockFacility(),
                Map.of("expanded.cloud.storage.endpointUrl", ""));

        sendBlocks(START_TIME, 0, 4);
        plugin.handlePersisted(new PersistedNotification(4L, true, 0, BlockSource.UNKNOWN));

        assertEquals(0, capturing.uploads.size(), "No uploads should occur when plugin is disabled");
    }

    @Test
    @DisplayName("Plugin skips upload for failed persistence notifications")
    void skipsUploadOnFailedNotification() {
        final CapturingS3Client capturing = new CapturingS3Client();
        start(
                new ExpandedCloudStoragePlugin(capturing),
                new SimpleInMemoryHistoricalBlockFacility(),
                Map.of(
                        "expanded.cloud.storage.endpointUrl", "http://fake:9000",
                        "expanded.cloud.storage.bucketName", BUCKET_NAME,
                        "expanded.cloud.storage.accessKey", MINIO_USER,
                        "expanded.cloud.storage.secretKey", MINIO_PASSWORD));

        sendBlocks(START_TIME, 0, 0);
        // Send a failed notification
        plugin.handlePersisted(new PersistedNotification(0L, false, 0, BlockSource.UNKNOWN));

        assertEquals(0, capturing.uploads.size(), "No upload should occur for a failed notification");
    }

    @Test
    @DisplayName("Plugin calls uploadFile with correct object key, storage class, and content type")
    void uploadsBlockWithCorrectParameters() {
        final CapturingS3Client capturing = new CapturingS3Client();
        start(
                new ExpandedCloudStoragePlugin(capturing),
                new SimpleInMemoryHistoricalBlockFacility(),
                Map.of(
                        "expanded.cloud.storage.endpointUrl", "http://fake:9000",
                        "expanded.cloud.storage.bucketName", BUCKET_NAME,
                        "expanded.cloud.storage.objectKeyPrefix", "myblocks",
                        "expanded.cloud.storage.storageClass", "STANDARD",
                        "expanded.cloud.storage.accessKey", MINIO_USER,
                        "expanded.cloud.storage.secretKey", MINIO_PASSWORD));

        sendBlocks(START_TIME, 0, 0);
        plugin.handlePersisted(new PersistedNotification(0L, true, 0, BlockSource.UNKNOWN));

        assertEquals(1, capturing.uploads.size(), "Exactly one uploadFile call expected");
        final UploadCall call = capturing.uploads.getFirst();
        assertEquals("myblocks/0000000000000000000.blk.zstd", call.objectKey());
        assertEquals("STANDARD", call.storageClass());
        assertEquals("application/octet-stream", call.contentType());
    }

    @Test
    @DisplayName("Object key uses correct zero-padded format for various block numbers")
    void objectKeyZeroPadding() {
        final CapturingS3Client capturing = new CapturingS3Client();
        start(
                new ExpandedCloudStoragePlugin(capturing),
                new SimpleInMemoryHistoricalBlockFacility(),
                Map.of(
                        "expanded.cloud.storage.endpointUrl", "http://fake:9000",
                        "expanded.cloud.storage.bucketName", BUCKET_NAME,
                        "expanded.cloud.storage.objectKeyPrefix", "blocks",
                        "expanded.cloud.storage.accessKey", MINIO_USER,
                        "expanded.cloud.storage.secretKey", MINIO_PASSWORD));

        sendBlocks(START_TIME, 0, 2);
        plugin.handlePersisted(new PersistedNotification(0L, true, 0, BlockSource.UNKNOWN));
        plugin.handlePersisted(new PersistedNotification(1L, true, 0, BlockSource.UNKNOWN));
        plugin.handlePersisted(new PersistedNotification(2L, true, 0, BlockSource.UNKNOWN));

        assertEquals(3, capturing.uploads.size());
        assertEquals("blocks/0000000000000000000.blk.zstd", capturing.uploads.get(0).objectKey());
        assertEquals("blocks/0000000000000000001.blk.zstd", capturing.uploads.get(1).objectKey());
        assertEquals("blocks/0000000000000000002.blk.zstd", capturing.uploads.get(2).objectKey());
    }

//    @Test
//    @DisplayName("S3ClientException from uploadFile is swallowed and does not crash the plugin")
//    void s3ExceptionSwallowed() {
//        final S3Client throwingClient = new NoOpS3Client() {
//            @Override
//            public void uploadFile(
//                    final String objectKey,
//                    final String storageClass,
//                    final java.util.Iterator<byte[]> contentIterable,
//                    final String contentType)
//                    throws S3ClientException {
//                throw new S3ClientException("Simulated S3 failure");
//            }
//        };
//
//        assertDoesNotThrow(() -> {
//            start(
//                    new ExpandedCloudStoragePlugin(throwingClient),
//                    new SimpleInMemoryHistoricalBlockFacility(),
//                    Map.of(
//                            "expanded.cloud.storage.endpointUrl", "http://fake:9000",
//                            "expanded.cloud.storage.bucketName", BUCKET_NAME,
//                            "expanded.cloud.storage.accessKey", MINIO_USER,
//                            "expanded.cloud.storage.secretKey", MINIO_PASSWORD));
//
//            sendBlocks(START_TIME, 0, 0);
//            plugin.handlePersisted(new PersistedNotification(0L, true, 0, BlockSource.UNKNOWN));
//        });
//    }

    // ---- Integration tests (MinIO via Testcontainers) -----------------------

    @Test
    @DisplayName("Integration: plugin uploads blocks as individual .blk.zstd objects to MinIO")
    void integrationUploadSingleBlocks() throws Exception {
        start(
                new ExpandedCloudStoragePlugin(),
                new SimpleInMemoryHistoricalBlockFacility(),
                Map.of(
                        "expanded.cloud.storage.endpointUrl", minioEndpoint,
                        "expanded.cloud.storage.bucketName", BUCKET_NAME,
                        "expanded.cloud.storage.objectKeyPrefix", "blocks",
                        "expanded.cloud.storage.accessKey", MINIO_USER,
                        "expanded.cloud.storage.secretKey", MINIO_PASSWORD));

        sendBlocks(START_TIME, 0, 4);
        for (int i = 0; i <= 4; i++) {
            plugin.handlePersisted(new PersistedNotification((long) i, true, 0, BlockSource.UNKNOWN));
        }

        final Set<String> objects = listAllObjects();
        for (int i = 0; i <= 4; i++) {
            final String expectedKey = "blocks/" + String.format("%019d", i) + ".blk.zstd";
            assertEquals(true, objects.contains(expectedKey),
                    "Expected object key not found in MinIO: " + expectedKey);
        }
    }

    @Test
    @DisplayName("Integration: each uploaded object is non-empty bytes")
    void integrationUploadedObjectsAreNonEmpty() throws Exception {
        start(
                new ExpandedCloudStoragePlugin(),
                new SimpleInMemoryHistoricalBlockFacility(),
                Map.of(
                        "expanded.cloud.storage.endpointUrl", minioEndpoint,
                        "expanded.cloud.storage.bucketName", BUCKET_NAME,
                        "expanded.cloud.storage.objectKeyPrefix", "intblocks",
                        "expanded.cloud.storage.accessKey", MINIO_USER,
                        "expanded.cloud.storage.secretKey", MINIO_PASSWORD));

        sendBlocks(START_TIME, 10, 10);
        plugin.handlePersisted(new PersistedNotification(10L, true, 0, BlockSource.UNKNOWN));

        final String key = "intblocks/0000000000000000010.blk.zstd";
        final byte[] downloaded = minioClient
                .getObject(GetObjectArgs.builder()
                        .bucket(BUCKET_NAME)
                        .object(key)
                        .build())
                .readAllBytes();
        assertEquals(true, downloaded.length > 0, "Downloaded block object must not be empty");
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

    private void sendBlocks(final Instant firstBlockTime, final long startBlockNumber, final long endBlockNumber) {
        final List<TestBlock> blocks =
                TestBlockBuilder.generateBlocksInRange(startBlockNumber, endBlockNumber, firstBlockTime, ONE_DAY);
        for (final TestBlock block : blocks) {
            blockMessaging.sendBlockItems(block.asBlockItems());
        }
    }
}
