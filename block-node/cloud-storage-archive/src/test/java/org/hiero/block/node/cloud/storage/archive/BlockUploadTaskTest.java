// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.cloud.storage.archive;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hedera.bucky.S3Client;
import com.hedera.pbj.runtime.OneOf;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.config.api.ConfigurationBuilder;
import io.minio.BucketExistsArgs;
import io.minio.ListObjectsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.RemoveObjectArgs;
import io.minio.Result;
import io.minio.errors.MinioException;
import io.minio.messages.Item;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.app.fixtures.TestMetricsExporter;
import org.hiero.block.node.app.fixtures.plugintest.TestBlockMessagingFacility;
import org.hiero.block.node.spi.blockmessaging.BlockMessagingFacility;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.PersistedNotification;
import org.hiero.metrics.core.MetricRegistry;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;

/// Direct unit tests for [BlockUploadTask] that bypass the plugin and call [BlockUploadTask#call]
/// directly against the shared MinIO container.  Failure injection is achieved by overriding
/// [BlockUploadTask#doUploadPart] in [FailingBlockUploadTask].
@DisplayName("BlockUploadTask Tests")
class BlockUploadTaskTest {

    private static final int MINIO_PORT = 9000;
    private static final String MINIO_USER = "minioadmin";
    private static final String MINIO_PASSWORD = "minioadmin";
    private static final String BUCKET_NAME = "test-bucket";

    // @todo(2013) we should remove that and use another approach
    /// Shared MinIO container — started once before all tests and stopped after the last test.
    private static final GenericContainer<?> MINIO_CONTAINER = new GenericContainer<>("minio/minio:latest")
            .withCommand("server /data")
            .withExposedPorts(MINIO_PORT)
            .withEnv("MINIO_ROOT_USER", MINIO_USER)
            .withEnv("MINIO_ROOT_PASSWORD", MINIO_PASSWORD);

    private static MinioClient minioClient;
    private static String minioEndpoint;

    /// 100 blocks per group (groupingLevel = 2).
    private static final int GROUPING_LEVEL = 2;
    /// 5 MB — the minimum non-final part size accepted by S3/MinIO.
    private static final int PART_SIZE_MB = 5;
    /// ~600 KB per block so the buffer overflows the 5 MB threshold after ~9 blocks.
    private static final int BLOCK_DATA_BYTES = 600 * 1024;

    @BeforeAll
    static void startMinio() throws GeneralSecurityException, IOException, MinioException {
        MINIO_CONTAINER.start();
        minioEndpoint = "http://" + MINIO_CONTAINER.getHost() + ":" + MINIO_CONTAINER.getMappedPort(MINIO_PORT);
        minioClient = MinioClient.builder()
                .endpoint(minioEndpoint)
                .credentials(MINIO_USER, MINIO_PASSWORD)
                .build();
        minioClient.makeBucket(MakeBucketArgs.builder().bucket(BUCKET_NAME).build());
        assertTrue(minioClient.bucketExists(
                BucketExistsArgs.builder().bucket(BUCKET_NAME).build()));
    }

    @AfterAll
    static void stopMinio() {
        MINIO_CONTAINER.stop();
    }

    /// Removes all objects from the test bucket before each test so tests do not see each other's uploads.
    @BeforeEach
    void clearBucket() throws Exception {
        for (final Result<Item> result : minioClient.listObjects(
                ListObjectsArgs.builder().bucket(BUCKET_NAME).recursive(true).build())) {
            minioClient.removeObject(RemoveObjectArgs.builder()
                    .bucket(BUCKET_NAME)
                    .object(result.get().objectName())
                    .build());
        }
    }

    /// Returns all object keys currently in the test bucket.
    private static Set<String> getAllObjects() throws Exception {
        final Iterable<Result<Item>> results = minioClient.listObjects(
                ListObjectsArgs.builder().bucket(BUCKET_NAME).recursive(true).build());
        final Set<String> keys = new HashSet<>();
        for (final Result<Item> result : results) {
            keys.add(result.get().objectName());
        }
        return keys;
    }

    private Map<String, String> pluginConfig(int groupingLevel, int partSizeMb) {
        return Map.of(
                "cloud.archive.groupingLevel", String.valueOf(groupingLevel),
                "cloud.archive.partSizeMb", String.valueOf(partSizeMb),
                "cloud.archive.endpointUrl", minioEndpoint,
                "cloud.archive.regionName", "us-east-1",
                "cloud.archive.bucketName", BUCKET_NAME,
                "cloud.archive.accessKey", MINIO_USER,
                "cloud.archive.secretKey", MINIO_PASSWORD);
    }

    private CloudStorageArchivePlugin.MetricsHolder createMetricsHolder(TestMetricsExporter exporter) {
        return CloudStorageArchivePlugin.MetricsHolder.createMetrics(
                MetricRegistry.builder().setMetricsExporter(exporter).build());
    }

    /// Verifies that when [BlockUploadTask#doUploadPart] throws during a mid-loop flush, all
    /// blocks whose tar bytes were in the buffer at the time of failure receive a failed
    /// [PersistedNotification], and [BlockUploadTask.UploadResult#FAILED] is returned.
    @Test
    @DisplayName("Failed part upload sends false persisted notifications for all buffered blocks")
    void testFailedfulUploadReturnsFailure() throws Exception {
        final int groupSize = (int) Math.pow(10, GROUPING_LEVEL);
        final TestBlockMessagingFacility messaging = new TestBlockMessagingFacility();
        final BlockingQueue<BlockWithSource> queue = new LinkedBlockingQueue<>();

        final ConfigurationBuilder builder =
                ConfigurationBuilder.create().withConfigDataType(CloudStorageArchiveConfig.class);
        pluginConfig(GROUPING_LEVEL, PART_SIZE_MB).forEach(builder::withValue);
        final BlockUploadTask task = new FailingBlockUploadTask(
                builder.build().getConfigData(CloudStorageArchiveConfig.class),
                messaging,
                0,
                groupSize,
                queue,
                createMetricsHolder(new TestMetricsExporter()));

        // Pre-fill the queue with enough large blocks to trigger at least one part flush
        final Random rng = new Random(0xDEADBEEFL);
        for (int i = 0; i < groupSize; i++) {
            final byte[] data = new byte[BLOCK_DATA_BYTES];
            rng.nextBytes(data);
            final BlockItemUnparsed item = new BlockItemUnparsed(
                    new OneOf<>(BlockItemUnparsed.ItemOneOfType.SIGNED_TRANSACTION, Bytes.wrap(data)));
            final BlockUnparsed block = BlockUnparsed.newBuilder()
                    .blockItems(new BlockItemUnparsed[] {item})
                    .build();
            queue.put(new BlockWithSource(block, BlockSource.PUBLISHER));
        }

        final BlockUploadTask.UploadResult result = task.call();
        assertThat(result).isEqualTo(BlockUploadTask.UploadResult.FAILED);

        final List<PersistedNotification> notifications = messaging.getSentPersistedNotifications();
        assertThat(notifications).isNotEmpty().allSatisfy(n -> {
            assertThat(n.succeeded()).isFalse();
            assertThat(n.blockNumber()).isZero();
            assertThat(n.blockSource()).isEqualTo(BlockSource.PUBLISHER);
        });
    }

    /// Verifies that when the final partial buffer upload (after the main loop) fails, all blocks
    /// whose tar bytes were accumulated in that buffer receive a failed [PersistedNotification].
    /// Uses small blocks so no mid-loop flush occurs and [doUploadPart] is called exactly once.
    @Test
    @DisplayName("Failed final part upload sends false persisted notification for the first remaining block")
    void testFinalPartFailureSendsFalseNotifications() throws Exception {
        final int groupingLevel = 1;
        final int groupSize = (int) Math.pow(10, groupingLevel);
        final TestBlockMessagingFacility messaging = new TestBlockMessagingFacility();
        final BlockingQueue<BlockWithSource> queue = new LinkedBlockingQueue<>();

        final ConfigurationBuilder builder =
                ConfigurationBuilder.create().withConfigDataType(CloudStorageArchiveConfig.class);
        pluginConfig(groupingLevel, PART_SIZE_MB).forEach(builder::withValue);
        final BlockUploadTask task = new FailingBlockUploadTask(
                builder.build().getConfigData(CloudStorageArchiveConfig.class),
                messaging,
                0,
                groupSize,
                queue,
                createMetricsHolder(new TestMetricsExporter()));

        // Small blocks (100 bytes each) so the total buffer stays well below PART_SIZE_MB,
        // meaning no mid-loop flush occurs and all blocks end up in the final partial buffer.
        final Random rng = new Random(0xDEADBEEFL);
        for (int i = 0; i < groupSize; i++) {
            final byte[] data = new byte[100];
            rng.nextBytes(data);
            final BlockItemUnparsed item = new BlockItemUnparsed(
                    new OneOf<>(BlockItemUnparsed.ItemOneOfType.SIGNED_TRANSACTION, Bytes.wrap(data)));
            final BlockUnparsed block = BlockUnparsed.newBuilder()
                    .blockItems(new BlockItemUnparsed[] {item})
                    .build();
            queue.put(new BlockWithSource(block, BlockSource.PUBLISHER));
        }

        assertThat(task.call()).isEqualTo(BlockUploadTask.UploadResult.FAILED);

        final List<PersistedNotification> notifications = messaging.getSentPersistedNotifications();
        assertThat(notifications).hasSize(1);
        final PersistedNotification notification = notifications.getFirst();
        assertThat(notification.succeeded()).isFalse();
        assertThat(notification.blockNumber()).isZero();
        assertThat(notification.blockSource()).isEqualTo(BlockSource.PUBLISHER);
    }

    /// Verifies that [BlockUploadTask#call] returns [BlockUploadTask.UploadResult#SUCCESS] when
    /// all blocks are uploaded successfully, and that every block receives a successful
    /// [PersistedNotification].
    @Test
    @DisplayName("Successful upload returns SUCCESS and sends true persisted notifications for all blocks")
    void testSuccessfulUploadReturnsSuccess() throws Exception {
        final int groupSize = (int) Math.pow(10, GROUPING_LEVEL);
        final TestBlockMessagingFacility messaging = new TestBlockMessagingFacility();
        final BlockingQueue<BlockWithSource> queue = new LinkedBlockingQueue<>();

        final ConfigurationBuilder builder =
                ConfigurationBuilder.create().withConfigDataType(CloudStorageArchiveConfig.class);
        pluginConfig(GROUPING_LEVEL, PART_SIZE_MB).forEach(builder::withValue);
        final BlockUploadTask task = new BlockUploadTask(
                builder.build().getConfigData(CloudStorageArchiveConfig.class),
                messaging,
                0,
                groupSize,
                queue,
                createMetricsHolder(new TestMetricsExporter()));

        final Random rng = new Random(0xDEADBEEFL);
        for (int i = 0; i < groupSize; i++) {
            final byte[] data = new byte[BLOCK_DATA_BYTES];
            rng.nextBytes(data);
            final BlockItemUnparsed item = new BlockItemUnparsed(
                    new OneOf<>(BlockItemUnparsed.ItemOneOfType.SIGNED_TRANSACTION, Bytes.wrap(data)));
            final BlockUnparsed block = BlockUnparsed.newBuilder()
                    .blockItems(new BlockItemUnparsed[] {item})
                    .build();
            queue.put(new BlockWithSource(block, BlockSource.PUBLISHER));
        }

        final BlockUploadTask.UploadResult result = task.call();
        assertThat(result).isEqualTo(BlockUploadTask.UploadResult.SUCCESS);

        assertThat(getAllObjects()).contains("0000/0000/0000/0000/0.tar");
        final List<PersistedNotification> notifications = messaging.getSentPersistedNotifications();
        assertThat(notifications).isNotEmpty().allSatisfy(n -> {
            assertThat(n.succeeded()).isTrue();
            assertThat(n.blockSource()).isEqualTo(BlockSource.PUBLISHER);
        });
    }

    /// Verifies that the last block's [BlockSource] is preserved in its [PersistedNotification].
    /// The last block (index 9) is odd, so its source is [BlockSource#BACKFILL].
    @Test
    @DisplayName("Last block source is preserved in persisted notification for mixed-source groups")
    void testMixedSourcesPreservedInPersistedNotifications() throws Exception {
        final int groupingLevel = 1;
        final int groupSize = (int) Math.pow(10, groupingLevel);
        final TestBlockMessagingFacility messaging = new TestBlockMessagingFacility();
        final BlockingQueue<BlockWithSource> queue = new LinkedBlockingQueue<>();

        final ConfigurationBuilder builder =
                ConfigurationBuilder.create().withConfigDataType(CloudStorageArchiveConfig.class);
        pluginConfig(groupingLevel, PART_SIZE_MB).forEach(builder::withValue);
        final BlockUploadTask task = new BlockUploadTask(
                builder.build().getConfigData(CloudStorageArchiveConfig.class),
                messaging,
                0,
                groupSize,
                queue,
                createMetricsHolder(new TestMetricsExporter()));

        // Even blocks → PUBLISHER, odd blocks → BACKFILL
        final Random rng = new Random(0xDEADBEEFL);
        for (int i = 0; i < groupSize; i++) {
            final byte[] data = new byte[100];
            rng.nextBytes(data);
            final BlockItemUnparsed item = new BlockItemUnparsed(
                    new OneOf<>(BlockItemUnparsed.ItemOneOfType.SIGNED_TRANSACTION, Bytes.wrap(data)));
            final BlockUnparsed block = BlockUnparsed.newBuilder()
                    .blockItems(new BlockItemUnparsed[] {item})
                    .build();
            final BlockSource source = (i % 2 == 0) ? BlockSource.PUBLISHER : BlockSource.BACKFILL;
            queue.put(new BlockWithSource(block, source));
        }

        assertThat(task.call()).isEqualTo(BlockUploadTask.UploadResult.SUCCESS);

        final List<PersistedNotification> notifications = messaging.getSentPersistedNotifications();
        assertThat(notifications).hasSize(1);
        assertThat(notifications.getFirst().blockNumber()).isEqualTo(groupSize - 1L);
        assertThat(notifications.getFirst().blockSource()).isEqualTo(BlockSource.BACKFILL);
    }

    /// Verifies that a null [BlockSource] in [BlockWithSource] results in
    /// [BlockSource#UNKNOWN] in the [PersistedNotification].
    @Test
    @DisplayName("Null source in verification notification defaults to UNKNOWN in persisted notification")
    void testNullSourceDefaultsToUnknown() throws Exception {
        final int groupingLevel = 1;
        final int groupSize = (int) Math.pow(10, groupingLevel);
        final TestBlockMessagingFacility messaging = new TestBlockMessagingFacility();
        final BlockingQueue<BlockWithSource> queue = new LinkedBlockingQueue<>();

        final ConfigurationBuilder builder =
                ConfigurationBuilder.create().withConfigDataType(CloudStorageArchiveConfig.class);
        pluginConfig(groupingLevel, PART_SIZE_MB).forEach(builder::withValue);
        final BlockUploadTask task = new BlockUploadTask(
                builder.build().getConfigData(CloudStorageArchiveConfig.class),
                messaging,
                0,
                groupSize,
                queue,
                createMetricsHolder(new TestMetricsExporter()));

        final Random rng = new Random(0xDEADBEEFL);
        for (int i = 0; i < groupSize; i++) {
            final byte[] data = new byte[100];
            rng.nextBytes(data);
            final BlockItemUnparsed item = new BlockItemUnparsed(
                    new OneOf<>(BlockItemUnparsed.ItemOneOfType.SIGNED_TRANSACTION, Bytes.wrap(data)));
            final BlockUnparsed block = BlockUnparsed.newBuilder()
                    .blockItems(new BlockItemUnparsed[] {item})
                    .build();
            queue.put(new BlockWithSource(block, null));
        }

        assertThat(task.call()).isEqualTo(BlockUploadTask.UploadResult.SUCCESS);

        final List<PersistedNotification> notifications = messaging.getSentPersistedNotifications();
        assertThat(notifications).hasSize(1);
        final PersistedNotification notification = notifications.getFirst();
        assertThat(notification.blockNumber()).isEqualTo(groupSize - 1L);
        assertThat(notification.blockSource()).isEqualTo(BlockSource.UNKNOWN);
    }

    /// Verifies that after a successful upload [blocksWritten] equals the group size and
    /// [storedBytes] is positive, confirming metrics reflect what was durably stored.
    @Test
    @DisplayName("Successful upload tracks correct blocksWritten and storedBytes")
    void testSuccessfulUploadTracksMetrics() throws Exception {
        final int groupSize = (int) Math.pow(10, GROUPING_LEVEL);
        final TestMetricsExporter exporter = new TestMetricsExporter();
        final TestBlockMessagingFacility messaging = new TestBlockMessagingFacility();
        final BlockingQueue<BlockWithSource> queue = new LinkedBlockingQueue<>();

        final ConfigurationBuilder builder =
                ConfigurationBuilder.create().withConfigDataType(CloudStorageArchiveConfig.class);
        pluginConfig(GROUPING_LEVEL, PART_SIZE_MB).forEach(builder::withValue);
        final BlockUploadTask task = new BlockUploadTask(
                builder.build().getConfigData(CloudStorageArchiveConfig.class),
                messaging,
                0,
                groupSize,
                queue,
                createMetricsHolder(exporter));

        final Random rng = new Random(0xDEADBEEFL);
        for (int i = 0; i < groupSize; i++) {
            final byte[] data = new byte[BLOCK_DATA_BYTES];
            rng.nextBytes(data);
            final BlockItemUnparsed item = new BlockItemUnparsed(
                    new OneOf<>(BlockItemUnparsed.ItemOneOfType.SIGNED_TRANSACTION, Bytes.wrap(data)));
            queue.put(new BlockWithSource(
                    BlockUnparsed.newBuilder()
                            .blockItems(new BlockItemUnparsed[] {item})
                            .build(),
                    BlockSource.PUBLISHER));
        }

        assertThat(task.call()).isEqualTo(BlockUploadTask.UploadResult.SUCCESS);

        assertThat(exporter.getMetricValue(CloudStorageArchivePlugin.METRIC_CLOUD_ARCHIVE_BLOCKS_WRITTEN.name()))
                .isEqualTo(groupSize);
        assertThat(exporter.getMetricValue(CloudStorageArchivePlugin.METRIC_CLOUD_ARCHIVE_STORED_BYTES.name()))
                .isGreaterThan(0L);
    }

    /// Verifies that a failed upload leaves both [blocksWritten] and [storedBytes] at zero,
    /// since no data was durably stored.
    @Test
    @DisplayName("Failed upload leaves blocksWritten and storedBytes at zero")
    void testFailedUploadDoesNotTrackMetrics() throws Exception {
        final int groupSize = (int) Math.pow(10, GROUPING_LEVEL);
        final TestMetricsExporter exporter = new TestMetricsExporter();
        final TestBlockMessagingFacility messaging = new TestBlockMessagingFacility();
        final BlockingQueue<BlockWithSource> queue = new LinkedBlockingQueue<>();

        final ConfigurationBuilder builder =
                ConfigurationBuilder.create().withConfigDataType(CloudStorageArchiveConfig.class);
        pluginConfig(GROUPING_LEVEL, PART_SIZE_MB).forEach(builder::withValue);
        final BlockUploadTask task = new FailingBlockUploadTask(
                builder.build().getConfigData(CloudStorageArchiveConfig.class),
                messaging,
                0,
                groupSize,
                queue,
                createMetricsHolder(exporter));

        final Random rng = new Random(0xDEADBEEFL);
        for (int i = 0; i < groupSize; i++) {
            final byte[] data = new byte[BLOCK_DATA_BYTES];
            rng.nextBytes(data);
            final BlockItemUnparsed item = new BlockItemUnparsed(
                    new OneOf<>(BlockItemUnparsed.ItemOneOfType.SIGNED_TRANSACTION, Bytes.wrap(data)));
            queue.put(new BlockWithSource(
                    BlockUnparsed.newBuilder()
                            .blockItems(new BlockItemUnparsed[] {item})
                            .build(),
                    BlockSource.PUBLISHER));
        }

        assertThat(task.call()).isEqualTo(BlockUploadTask.UploadResult.FAILED);

        assertThat(exporter.getMetricValue(CloudStorageArchivePlugin.METRIC_CLOUD_ARCHIVE_BLOCKS_WRITTEN.name()))
                .isZero();
        assertThat(exporter.getMetricValue(CloudStorageArchivePlugin.METRIC_CLOUD_ARCHIVE_STORED_BYTES.name()))
                .isZero();
    }

    /// [BlockUploadTask] subclass that always throws from [doUploadPart] to simulate S3 failures.
    private static final class FailingBlockUploadTask extends BlockUploadTask {

        FailingBlockUploadTask(
                CloudStorageArchiveConfig config,
                BlockMessagingFacility blockMessaging,
                long firstBlock,
                int groupSize,
                BlockingQueue<BlockWithSource> queue,
                CloudStorageArchivePlugin.MetricsHolder metricsHolder) {
            super(config, blockMessaging, firstBlock, groupSize, queue, metricsHolder);
        }

        @Override
        void doUploadPart(byte[] buffer, S3Client s3, String uploadId, List<String> etags) throws IOException {
            throw new IOException("Simulated S3 final part upload failure");
        }
    }
}
