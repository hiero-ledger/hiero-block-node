// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.expanded.cloud.storage;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
 *       {@link BaseS3ClientAdapter} via the no-arg constructor.</li>
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

        // Start MinIO container — shared across integration tests in this class.
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

    /** Returns the first {@link TestBlock} for the given block number. */
    private TestBlock testBlock(final long blockNumber) {
        return TestBlockBuilder.generateBlocksInRange(blockNumber, blockNumber, START_TIME, ONE_DAY)
                .getFirst();
    }

    /** Builds a successful {@link VerificationNotification} carrying the given block. */
    private VerificationNotification verifiedNotification(final long blockNumber, final BlockUnparsed block) {
        return new VerificationNotification(true, blockNumber, Bytes.EMPTY, block, BlockSource.UNKNOWN);
    }

    /** Builds a failed {@link VerificationNotification} with no block payload. */
    private VerificationNotification failedNotification(final long blockNumber) {
        return new VerificationNotification(false, blockNumber, Bytes.EMPTY, null, BlockSource.UNKNOWN);
    }

    // ---- Unit tests ---------------------------------------------------------

    @Test
    @DisplayName("Plugin is disabled when endpointUrl is blank — handleVerification is a no-op")
    void pluginDisabledWhenNoEndpoint() {
        final CapturingS3Client capturing = new CapturingS3Client();
        start(
                new ExpandedCloudStoragePlugin(capturing),
                new SimpleInMemoryHistoricalBlockFacility(),
                Map.of("expanded.cloud.storage.endpointUrl", ""));

        plugin.handleVerification(verifiedNotification(0L, testBlock(0).blockUnparsed()));

        assertEquals(0, capturing.uploads.size(), "No uploads should occur when plugin is disabled");
    }

    @Test
    @DisplayName("Plugin skips upload for a failed VerificationNotification")
    void skipsUploadOnFailedVerification() {
        final CapturingS3Client capturing = new CapturingS3Client();
        start(
                new ExpandedCloudStoragePlugin(capturing),
                new SimpleInMemoryHistoricalBlockFacility(),
                Map.of(
                        "expanded.cloud.storage.endpointUrl", "http://fake:9000",
                        "expanded.cloud.storage.accessKey", MINIO_USER,
                        "expanded.cloud.storage.secretKey", MINIO_PASSWORD));

        plugin.handleVerification(failedNotification(0L));

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
                        "expanded.cloud.storage.objectKeyPrefix", "myblocks",
                        "expanded.cloud.storage.storageClass", "STANDARD",
                        "expanded.cloud.storage.accessKey", MINIO_USER,
                        "expanded.cloud.storage.secretKey", MINIO_PASSWORD));

        plugin.handleVerification(verifiedNotification(0L, testBlock(0).blockUnparsed()));

        assertEquals(1, capturing.uploads.size(), "Exactly one uploadFile call expected");
        final UploadCall call = capturing.uploads.getFirst();
        assertEquals("myblocks/0000000000000000000.blk.zstd", call.objectKey());
        assertEquals("STANDARD", call.storageClass());
        assertEquals("application/octet-stream", call.contentType());
    }

    @Test
    @DisplayName("Object key uses 19-digit zero-padded block number for various block numbers")
    void objectKeyZeroPadding() {
        final CapturingS3Client capturing = new CapturingS3Client();
        start(
                new ExpandedCloudStoragePlugin(capturing),
                new SimpleInMemoryHistoricalBlockFacility(),
                Map.of(
                        "expanded.cloud.storage.endpointUrl", "http://fake:9000",
                        "expanded.cloud.storage.objectKeyPrefix", "blocks",
                        "expanded.cloud.storage.accessKey", MINIO_USER,
                        "expanded.cloud.storage.secretKey", MINIO_PASSWORD));

        plugin.handleVerification(verifiedNotification(0L, testBlock(0).blockUnparsed()));
        plugin.handleVerification(verifiedNotification(1L, testBlock(1).blockUnparsed()));
        plugin.handleVerification(verifiedNotification(1234567L, testBlock(1234567L).blockUnparsed()));

        assertEquals(3, capturing.uploads.size());
        assertEquals("blocks/0000000000000000000.blk.zstd", capturing.uploads.get(0).objectKey());
        assertEquals("blocks/0000000000000000001.blk.zstd", capturing.uploads.get(1).objectKey());
        assertEquals("blocks/0000000000001234567.blk.zstd", capturing.uploads.get(2).objectKey());
    }

    @Test
    @DisplayName("S3ClientException thrown by uploadFile is swallowed — plugin does not rethrow")
    void s3ExceptionSwallowed() {
        final S3Client throwingClient = new NoOpS3Client() {
            @Override
            public void uploadFile(
                    final String objectKey,
                    final String storageClass,
                    final Iterator<byte[]> contentIterable,
                    final String contentType)
                    throws S3ClientException, IOException {
                throw new S3ClientException("Simulated S3 failure");
            }
        };

        start(
                new ExpandedCloudStoragePlugin(throwingClient),
                new SimpleInMemoryHistoricalBlockFacility(),
                Map.of(
                        "expanded.cloud.storage.endpointUrl", "http://fake:9000",
                        "expanded.cloud.storage.accessKey", MINIO_USER,
                        "expanded.cloud.storage.secretKey", MINIO_PASSWORD));

        final VerificationNotification notification = verifiedNotification(0L, testBlock(0).blockUnparsed());

        // start() is called before assertDoesNotThrow so only handleVerification is wrapped
        assertDoesNotThrow(
                () -> plugin.handleVerification(notification),
                "S3ClientException must be swallowed — plugin must never rethrow");
    }

    // ---- Integration tests (MinIO via Testcontainers) -----------------------

    @Test
    @DisplayName("Integration: blocks are uploaded as individual .blk.zstd objects in MinIO")
    void integrationUploadSingleBlocks() {
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

        final Set<String> objects = listAllObjects();
        for (long i = 100L; i <= 104L; i++) {
            final String expectedKey = "blocks/" + String.format("%019d", i) + ".blk.zstd";
            assertTrue(objects.contains(expectedKey), "Expected object not found in MinIO: " + expectedKey);
        }
    }

    @Test
    @DisplayName("Integration: each uploaded .blk.zstd object contains non-empty bytes")
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

        final TestBlock block = testBlock(200L);
        plugin.handleVerification(verifiedNotification(200L, block.blockUnparsed()));

        final String key = "intblocks/0000000000000000200.blk.zstd";
        final byte[] downloaded = minioClient
                .getObject(GetObjectArgs.builder()
                        .bucket(BUCKET_NAME)
                        .object(key)
                        .build())
                .readAllBytes();
        assertTrue(downloaded.length > 0, "Downloaded .blk.zstd must not be empty");
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
