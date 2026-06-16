// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.cloud.storage.archive;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hedera.bucky.S3Client;
import com.hedera.hapi.node.base.NodeAddressBook;
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
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.hiero.block.api.TssData;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.app.fixtures.TestMetricsExporter;
import org.hiero.block.node.app.fixtures.plugintest.TestBlockMessagingFacility;
import org.hiero.block.node.spi.ApplicationStateFacility;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.PersistedNotification;
import org.hiero.block.node.spi.historicalblocks.LongRange;
import org.hiero.metrics.core.MetricRegistry;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;

/// Direct unit tests for [TempArchiveUploadTask] that exercise the success path and the named
/// error paths against a real MinIO container.  Failure injection uses the package-private hooks
/// [TempArchiveUploadTask#doUploadPart], [TempArchiveUploadTask#doCompleteMultipartUpload], and
/// [TempArchiveUploadTask#doUploadTextFile], overridden in local subclasses following the same
/// pattern as [BlockUploadTaskTest].
@DisplayName("TempArchiveUploadTask Tests")
class TempArchiveUploadTaskTest {

    private static final int MINIO_PORT = 9000;
    private static final String MINIO_USER = "minioadmin";
    private static final String MINIO_PASSWORD = "minioadmin";
    private static final String BUCKET_NAME = "test-bucket";
    private static final int GROUPING_LEVEL = 1;
    private static final int PART_SIZE_MB = 5;
    /// ~600 KB per block; two blocks exceed the 5 MB part threshold, triggering a mid-loop flush.
    private static final int BLOCK_DATA_BYTES = 600 * 1024;

    // @todo(2013) we should remove that and use another approach
    private static final GenericContainer<?> MINIO_CONTAINER = new GenericContainer<>("minio/minio:latest")
            .withCommand("server /data")
            .withExposedPorts(MINIO_PORT)
            .withEnv("MINIO_ROOT_USER", MINIO_USER)
            .withEnv("MINIO_ROOT_PASSWORD", MINIO_PASSWORD);

    private static MinioClient minioClient;
    private static String minioEndpoint;

    private CloudStorageArchiveConfig config;

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

    @BeforeEach
    void setUp() throws Exception {
        final ConfigurationBuilder builder =
                ConfigurationBuilder.create().withConfigDataType(CloudStorageArchiveConfig.class);
        pluginConfig().forEach(builder::withValue);
        config = builder.build().getConfigData(CloudStorageArchiveConfig.class);
        clearBucket();
    }

    private void clearBucket() throws Exception {
        for (final Result<Item> result : minioClient.listObjects(
                ListObjectsArgs.builder().bucket(BUCKET_NAME).recursive(true).build())) {
            minioClient.removeObject(RemoveObjectArgs.builder()
                    .bucket(BUCKET_NAME)
                    .object(result.get().objectName())
                    .build());
        }
        try (S3Client s3 = openS3Client()) {
            for (final Map.Entry<String, List<String>> entry :
                    s3.listMultipartUploads().entrySet()) {
                for (final String uploadId : entry.getValue()) {
                    try {
                        s3.abortMultipartUpload(entry.getKey(), uploadId);
                    } catch (Exception ignored) {
                    }
                }
            }
        }
    }

    private Map<String, String> pluginConfig() {
        return Map.of(
                "cloud.storage.archive.groupingLevel", String.valueOf(GROUPING_LEVEL),
                "cloud.storage.archive.partSizeMb", String.valueOf(PART_SIZE_MB),
                "cloud.storage.archive.endpointUrl", minioEndpoint,
                "cloud.storage.archive.regionName", "us-east-1",
                "cloud.storage.archive.bucketName", BUCKET_NAME,
                "cloud.storage.archive.accessKey", MINIO_USER,
                "cloud.storage.archive.secretKey", MINIO_PASSWORD);
    }

    private S3Client openS3Client() throws Exception {
        return new S3Client(
                config.regionName(), config.endpointUrl(), config.bucketName(), config.accessKey(), config.secretKey());
    }

    private CloudStorageArchivePlugin.MetricsHolder createMetricsHolder() {
        return CloudStorageArchivePlugin.MetricsHolder.createMetrics(MetricRegistry.builder()
                .setMetricsExporter(new TestMetricsExporter())
                .build());
    }

    private static BlockWithSource makeBlock(int dataSize) {
        final byte[] data = new byte[dataSize];
        new Random(0).nextBytes(data);
        final BlockItemUnparsed item = new BlockItemUnparsed(
                new OneOf<>(BlockItemUnparsed.ItemOneOfType.SIGNED_TRANSACTION, Bytes.wrap(data)));
        return new BlockWithSource(
                BlockUnparsed.newBuilder()
                        .blockItems(new BlockItemUnparsed[] {item})
                        .build(),
                BlockSource.PUBLISHER);
    }

    private TempArchiveUploadTask buildTask(
            String s3Key, long firstBlock, BlockingQueue<BlockWithSource> queue, TestBlockMessagingFacility messaging) {
        return new TempArchiveUploadTask(
                config, messaging, new NoOpApplicationStateFacility(), createMetricsHolder(), s3Key, firstBlock, queue);
    }

    /// Verifies the happy path: blocks queued followed by SEGMENT_END produce a completed `.tmp`
    /// key and a `.meta` key whose content is the decimal last-block number.  The returned
    /// [TempArchiveEntry] has the correct [TempArchiveEntry#firstBlock()],
    /// [TempArchiveEntry#lastBlock()], and a null [TempArchiveEntry#uploadId()].
    @Test
    @DisplayName("Success: .tmp and .meta keys written; correct TempArchiveEntry returned")
    void successfulUploadWritesBothKeysAndReturnsEntry() throws Exception {
        final String s3Key = TempArchiveKey.formatTar(0, "");
        final String metaKey = TempArchiveKey.formatMeta(0, "");
        final BlockingQueue<BlockWithSource> queue = new LinkedBlockingQueue<>();
        final TestBlockMessagingFacility messaging = new TestBlockMessagingFacility();

        for (int i = 0; i < 3; i++) {
            queue.put(makeBlock(100));
        }
        queue.put(TempArchiveUploadTask.SEGMENT_END);

        final TempArchiveEntry entry = buildTask(s3Key, 0, queue, messaging).call();

        assertThat(entry.s3Key()).isEqualTo(s3Key);
        assertThat(entry.firstBlock()).isZero();
        assertThat(entry.lastBlock()).isEqualTo(2L);
        assertThat(entry.uploadId()).isNull();
        try (S3Client s3 = openS3Client()) {
            assertThat(s3.listObjects(s3Key, 1)).contains(s3Key);
            assertThat(s3.downloadTextFile(metaKey)).isEqualTo("2");
        }
        final List<PersistedNotification> notifications = messaging.getSentPersistedNotifications();
        assertThat(notifications).hasSize(1);
        assertThat(notifications.getFirst().blockNumber()).isEqualTo(2L);
        assertThat(notifications.getFirst().succeeded()).isTrue();
        assertThat(notifications.getFirst().blockSource()).isEqualTo(BlockSource.PUBLISHER);
    }

    /// Verifies that when [doCompleteMultipartUpload] throws, [call()] rethrows the exception and
    /// neither the `.tmp` key (not yet committed) nor the `.meta` key is present in S3.
    @Test
    @DisplayName("completeMultipartUpload failure: exception rethrown; no keys committed")
    void completeFailureThrowsAndLeavesNoKeys() throws Exception {
        final String s3Key = TempArchiveKey.formatTar(0, "");
        final String metaKey = TempArchiveKey.formatMeta(0, "");
        final BlockingQueue<BlockWithSource> queue = new LinkedBlockingQueue<>();
        final TestBlockMessagingFacility messaging = new TestBlockMessagingFacility();

        queue.put(makeBlock(100));
        queue.put(TempArchiveUploadTask.SEGMENT_END);

        final TempArchiveUploadTask task = new FailingCompleteTask(
                config, messaging, new NoOpApplicationStateFacility(), createMetricsHolder(), s3Key, 0, queue);

        assertThatThrownBy(task::call).isInstanceOf(IOException.class);
        try (S3Client s3 = openS3Client()) {
            // The multipart upload was never completed, so the .tmp object is not visible.
            assertThat(s3.listObjects(s3Key, 1)).doesNotContain(s3Key);
            assertThat(s3.listObjects(metaKey, 1)).doesNotContain(metaKey);
        }
    }

    /// Verifies that when [doUploadTextFile] (the meta companion write) throws, [call()] rethrows
    /// the exception.  The `.tmp` key was already committed by [doCompleteMultipartUpload], so it
    /// IS present; only the `.meta` key is absent.
    @Test
    @DisplayName("Meta file write failure: exception rethrown; .tmp exists but .meta absent")
    void metaWriteFailureThrowsAndLeavesTmpButNoMeta() throws Exception {
        final String s3Key = TempArchiveKey.formatTar(0, "");
        final String metaKey = TempArchiveKey.formatMeta(0, "");
        final BlockingQueue<BlockWithSource> queue = new LinkedBlockingQueue<>();
        final TestBlockMessagingFacility messaging = new TestBlockMessagingFacility();

        queue.put(makeBlock(100));
        queue.put(TempArchiveUploadTask.SEGMENT_END);

        final TempArchiveUploadTask task = new FailingMetaTask(
                config, messaging, new NoOpApplicationStateFacility(), createMetricsHolder(), s3Key, 0, queue);

        assertThatThrownBy(task::call).isInstanceOf(IOException.class);
        try (S3Client s3 = openS3Client()) {
            assertThat(s3.listObjects(s3Key, 1)).contains(s3Key);
            assertThat(s3.listObjects(metaKey, 1)).doesNotContain(metaKey);
        }
    }

    /// Verifies that interrupting the virtual thread while it is blocked on [BlockingQueue#take]
    /// causes [call()] to rethrow [InterruptedException] and abort the in-progress multipart upload.
    @Test
    @DisplayName("Thread interruption aborts upload and rethrows InterruptedException")
    void interruptionAbortsUploadAndRethrows() throws Exception {
        final String s3Key = TempArchiveKey.formatTar(0, "");
        final BlockingQueue<BlockWithSource> queue = new LinkedBlockingQueue<>();
        final TestBlockMessagingFacility messaging = new TestBlockMessagingFacility();
        final TempArchiveUploadTask task = buildTask(s3Key, 0, queue, messaging);

        final InterruptedException[] caught = {null};
        final Thread thread = new Thread(() -> {
            try {
                task.call();
            } catch (InterruptedException e) {
                caught[0] = e;
            } catch (Exception ignored) {
            }
        });
        thread.start();
        // Give the task time to create the multipart upload and block on take().
        Thread.sleep(200);
        thread.interrupt();
        thread.join(5_000);

        assertThat(caught[0]).isNotNull();
        try (S3Client s3 = openS3Client()) {
            assertThat(s3.listMultipartUploads()).doesNotContainKey(s3Key);
        }
    }

    /// Verifies that when [doUploadPart] throws during a mid-loop part flush, [call()] rethrows
    /// the exception, aborts the multipart upload, and sends no [PersistedNotification] (no data
    /// was durably confirmed before the failure).
    @Test
    @DisplayName("Part upload failure during loop: exception rethrown, no notifications sent, upload aborted")
    void partUploadFailureDuringLoopThrowsAndAbortsUpload() throws Exception {
        final String s3Key = TempArchiveKey.formatTar(0, "");
        final BlockingQueue<BlockWithSource> queue = new LinkedBlockingQueue<>();
        final TestBlockMessagingFacility messaging = new TestBlockMessagingFacility();

        // Large blocks so the cumulative buffer exceeds PART_SIZE_MB before SEGMENT_END,
        // triggering the mid-loop doUploadPart call.
        for (int i = 0; i < 10; i++) {
            queue.put(makeBlock(BLOCK_DATA_BYTES));
        }
        queue.put(TempArchiveUploadTask.SEGMENT_END);

        final TempArchiveUploadTask task = new FailingUploadPartTask(
                config, messaging, new NoOpApplicationStateFacility(), createMetricsHolder(), s3Key, 0, queue);

        assertThatThrownBy(task::call).isInstanceOf(IOException.class);
        assertThat(messaging.getSentPersistedNotifications()).isEmpty();
        try (S3Client s3 = openS3Client()) {
            assertThat(s3.listMultipartUploads()).doesNotContainKey(s3Key);
        }
    }

    /// Verifies that when [doUploadPart] throws while uploading the final partial buffer (after
    /// SEGMENT_END), [call()] sends one failed [PersistedNotification] for the first unseen block
    /// and rethrows the exception.
    @Test
    @DisplayName("Part upload failure for final part: failed notification sent, exception rethrown")
    void partUploadFailureForFinalPartSendsFailedNotificationAndThrows() throws Exception {
        final String s3Key = TempArchiveKey.formatTar(0, "");
        final BlockingQueue<BlockWithSource> queue = new LinkedBlockingQueue<>();
        final TestBlockMessagingFacility messaging = new TestBlockMessagingFacility();

        // Small blocks so the buffer stays below PART_SIZE_MB throughout the loop;
        // doUploadPart is only called for the final partial buffer.
        for (int i = 0; i < 3; i++) {
            queue.put(makeBlock(100));
        }
        queue.put(TempArchiveUploadTask.SEGMENT_END);

        final TempArchiveUploadTask task = new FailingUploadPartTask(
                config, messaging, new NoOpApplicationStateFacility(), createMetricsHolder(), s3Key, 0, queue);

        assertThatThrownBy(task::call).isInstanceOf(IOException.class);
        final List<PersistedNotification> notifications = messaging.getSentPersistedNotifications();
        assertThat(notifications).hasSize(1);
        assertThat(notifications.getFirst().succeeded()).isFalse();
        assertThat(notifications.getFirst().blockNumber()).isZero();
        assertThat(notifications.getFirst().blockSource()).isEqualTo(BlockSource.PUBLISHER);
        try (S3Client s3 = openS3Client()) {
            assertThat(s3.listMultipartUploads()).doesNotContainKey(s3Key);
        }
    }

    /// [TempArchiveUploadTask] subclass that always throws from [doUploadPart].
    private static final class FailingUploadPartTask extends TempArchiveUploadTask {

        FailingUploadPartTask(
                CloudStorageArchiveConfig config,
                TestBlockMessagingFacility messaging,
                ApplicationStateFacility asf,
                CloudStorageArchivePlugin.MetricsHolder metrics,
                String s3Key,
                long firstBlock,
                BlockingQueue<BlockWithSource> queue) {
            super(config, messaging, asf, metrics, s3Key, firstBlock, queue);
        }

        @Override
        void doUploadPart(byte[] buffer, S3Client s3, String uploadId, List<String> etags) throws IOException {
            throw new IOException("Simulated part upload failure");
        }
    }

    /// [TempArchiveUploadTask] subclass that throws from [doCompleteMultipartUpload].
    private static final class FailingCompleteTask extends TempArchiveUploadTask {

        FailingCompleteTask(
                CloudStorageArchiveConfig config,
                TestBlockMessagingFacility messaging,
                ApplicationStateFacility asf,
                CloudStorageArchivePlugin.MetricsHolder metrics,
                String s3Key,
                long firstBlock,
                BlockingQueue<BlockWithSource> queue) {
            super(config, messaging, asf, metrics, s3Key, firstBlock, queue);
        }

        @Override
        void doCompleteMultipartUpload(S3Client s3, String key, String uploadId, List<String> etags)
                throws IOException {
            throw new IOException("Simulated completeMultipartUpload failure");
        }
    }

    /// [TempArchiveUploadTask] subclass that throws from [doUploadTextFile].
    private static final class FailingMetaTask extends TempArchiveUploadTask {

        FailingMetaTask(
                CloudStorageArchiveConfig config,
                TestBlockMessagingFacility messaging,
                ApplicationStateFacility asf,
                CloudStorageArchivePlugin.MetricsHolder metrics,
                String s3Key,
                long firstBlock,
                BlockingQueue<BlockWithSource> queue) {
            super(config, messaging, asf, metrics, s3Key, firstBlock, queue);
        }

        @Override
        void doUploadTextFile(S3Client s3, String key, String storageClass, String content) throws IOException {
            throw new IOException("Simulated meta file write failure");
        }
    }

    private static final class NoOpApplicationStateFacility implements ApplicationStateFacility {

        @Override
        public void updateTssData(TssData tssData) {}

        @Override
        public boolean updateAddressBook(NodeAddressBook nodeAddressBook) {
            return true;
        }

        @Override
        public void addStoredBlockRange(LongRange blockRange) {}

        @Override
        public void addAvailableBlockRange(LongRange blockRange) {}
    }
}
