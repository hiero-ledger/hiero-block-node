// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.expanded.cloud.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.GenericContainer;

/**
 * Integration tests for {@link ExpandedCloudStoragePlugin} using a real MinIO container.
 *
 * <p>The MinIO container is started once for the whole class via {@link BeforeAll}. If Docker
 * is not available the entire class is skipped via {@link org.junit.jupiter.api.Assumptions},
 * leaving the unit tests in {@link ExpandedCloudStoragePluginTest} unaffected.
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS)
class ExpandedCloudStoragePluginIntegrationTest
        extends PluginTestBase<ExpandedCloudStoragePlugin, ExecutorService, ScheduledExecutorService> {

    private static final Instant START_TIME =
            ZonedDateTime.of(2025, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant();
    private static final Duration ONE_DAY = Duration.of(1, ChronoUnit.DAYS);
    private static final String BUCKET_NAME = "test-expanded-blocks";
    private static final int MINIO_PORT = 9000;
    private static final String MINIO_USER = "minioadmin";
    private static final String MINIO_PASSWORD = "minioadmin";
    private static final long DRAIN_TIMEOUT_MS = 5_000L;

    // Shared across all tests in this class — started once in @BeforeAll.
    private static GenericContainer<?> minioContainer;
    private static MinioClient minioClient;
    private static String minioEndpoint;

    @BeforeAll
    @SuppressWarnings("resource")
    static void startMinio() throws GeneralSecurityException, IOException, MinioException {
        try {
            minioContainer = new GenericContainer<>("minio/minio:latest")
                    .withCommand("server /data")
                    .withExposedPorts(MINIO_PORT)
                    .withEnv("MINIO_ROOT_USER", MINIO_USER)
                    .withEnv("MINIO_ROOT_PASSWORD", MINIO_PASSWORD);
            minioContainer.start();
        } catch (final IllegalStateException e) {
            // Testcontainers throws IllegalStateException when Docker is unavailable.
            // Calling assumeTrue(false) marks all tests in this class as skipped rather than failed.
            assumeTrue(false, "Docker not available — skipping MinIO integration tests: " + e.getMessage());
            return;
        }
        minioEndpoint = "http://" + minioContainer.getHost() + ":" + minioContainer.getMappedPort(MINIO_PORT);
        minioClient = MinioClient.builder()
                .endpoint(minioEndpoint)
                .credentials(MINIO_USER, MINIO_PASSWORD)
                .build();
        minioClient.makeBucket(MakeBucketArgs.builder().bucket(BUCKET_NAME).build());
    }

    @AfterAll
    static void stopMinio() {
        if (minioContainer != null && minioContainer.isRunning()) {
            minioContainer.stop();
        }
    }

    public ExpandedCloudStoragePluginIntegrationTest() {
        super(Executors.newSingleThreadExecutor(), Executors.newSingleThreadScheduledExecutor());
    }

    // ---- Helpers ------------------------------------------------------------

    private TestBlock testBlock(final long blockNumber) {
        return TestBlockBuilder.generateBlocksInRange(blockNumber, blockNumber, START_TIME, ONE_DAY)
                .getFirst();
    }

    private VerificationNotification verifiedNotification(final long blockNumber, final BlockUnparsed block) {
        return new VerificationNotification(true, blockNumber, Bytes.EMPTY, block, BlockSource.UNKNOWN);
    }

    // ---- Tests --------------------------------------------------------------

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

        final String padded = String.format("%019d", 200L);
        final String key = "intblocks/"
                + padded.substring(0, 4) + "/" + padded.substring(4, 8) + "/"
                + padded.substring(8, 12) + "/" + padded.substring(12, 16) + "/"
                + padded.substring(16) + ".blk.zstd";
        final byte[] downloaded = minioClient
                .getObject(GetObjectArgs.builder().bucket(BUCKET_NAME).object(key).build())
                .readAllBytes();
        assertTrue(downloaded.length > 0, "Downloaded .blk.zstd must not be empty");

        final List<PersistedNotification> notifications = blockMessaging.getSentPersistedNotifications();
        assertEquals(1, notifications.size(), "One PersistedNotification expected after integration upload");
        assertEquals(200L, notifications.getFirst().blockNumber());
        assertTrue(notifications.getFirst().succeeded(), "PersistedNotification must report succeeded=true");
    }

    @Test
    @DisplayName("Integration: empty objectKeyPrefix produces bare hierarchy key in MinIO")
    void integrationUploadWithNoPrefix() throws InterruptedException {
        start(
                new ExpandedCloudStoragePlugin(),
                new SimpleInMemoryHistoricalBlockFacility(),
                Map.of(
                        "expanded.cloud.storage.endpointUrl", minioEndpoint,
                        "expanded.cloud.storage.bucketName", BUCKET_NAME,
                        "expanded.cloud.storage.objectKeyPrefix", "",
                        "expanded.cloud.storage.accessKey", MINIO_USER,
                        "expanded.cloud.storage.secretKey", MINIO_PASSWORD));

        final TestBlock block = testBlock(300L);
        plugin.handleVerification(verifiedNotification(300L, block.blockUnparsed()));
        plugin.awaitAndDrain(DRAIN_TIMEOUT_MS);

        final String padded = String.format("%019d", 300L);
        final String expectedKey = padded.substring(0, 4) + "/" + padded.substring(4, 8) + "/"
                + padded.substring(8, 12) + "/" + padded.substring(12, 16) + "/"
                + padded.substring(16) + ".blk.zstd";
        assertTrue(
                listAllObjects().contains(expectedKey),
                "Expected bare-hierarchy key (no prefix) not found in MinIO: " + expectedKey);
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
