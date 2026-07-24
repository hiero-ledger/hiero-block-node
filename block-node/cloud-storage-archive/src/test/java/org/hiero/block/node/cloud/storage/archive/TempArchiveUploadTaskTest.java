// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.cloud.storage.archive;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.hiero.block.api.NetworkData;
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
    /// ~600 KiB per block; nine or more blocks exceed the 5 MiB part threshold, triggering a mid-loop flush.
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

    private TempArchiveUploadTask buildTask(
            String s3Key,
            long firstBlock,
            BlockingQueue<BlockWithSource> queue,
            TestBlockMessagingFacility messaging,
            TrackingApplicationStateFacility asf) {
        return new TempArchiveUploadTask(config, messaging, asf, createMetricsHolder(), s3Key, firstBlock, queue);
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
        final TrackingApplicationStateFacility asf = new TrackingApplicationStateFacility();

        for (int i = 0; i < 3; i++) {
            queue.put(makeBlock(100));
        }
        queue.put(TempArchiveUploadTask.SEGMENT_END);

        final TempArchiveEntry entry =
                buildTask(s3Key, 0, queue, messaging, asf).call();

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
        assertThat(asf.addedRanges).hasSize(1);
        assertThat(asf.addedRanges.getFirst()).isEqualTo(new LongRange(0, 2));
    }

    /// Verifies that when [doCompleteMultipartUpload] throws, [call()] rethrows the exception,
    /// neither the `.tmp` key (not yet committed) nor the `.meta` key is present in S3, a failed
    /// [PersistedNotification] is sent so downstream subscribers are not left dangling, and --
    /// unlike before the abort-removal -- the multipart upload is left hanging on S3 (with its
    /// already-flushed part intact) for [StartupRecoveryTask] to resume, instead of being aborted.
    @Test
    @DisplayName("completeMultipartUpload failure: exception rethrown; upload left hanging, not aborted")
    void completeFailureThrowsAndLeavesUploadHanging() throws Exception {
        final String s3Key = TempArchiveKey.formatTar(0, "");
        final String metaKey = TempArchiveKey.formatMeta(0, "");
        final BlockingQueue<BlockWithSource> queue = new LinkedBlockingQueue<>();
        final TestBlockMessagingFacility messaging = new TestBlockMessagingFacility();
        final TrackingApplicationStateFacility asf = new TrackingApplicationStateFacility();

        queue.put(makeBlock(100));
        queue.put(TempArchiveUploadTask.SEGMENT_END);

        final TempArchiveUploadTask task =
                new FailingCompleteTask(config, messaging, asf, createMetricsHolder(), s3Key, 0, queue);

        assertThatThrownBy(task::call).isInstanceOf(IOException.class);
        try (S3Client s3 = openS3Client()) {
            // The multipart upload was never completed, so the .tmp object is not visible.
            assertThat(s3.listObjects(s3Key, 1)).doesNotContain(s3Key);
            assertThat(s3.listObjects(metaKey, 1)).doesNotContain(metaKey);
            // The upload itself must still be hanging -- not aborted -- with its part intact.
            final Map<String, List<String>> hangingUploads = s3.listMultipartUploads();
            assertThat(hangingUploads).containsKey(s3Key);
            assertThat(s3.listParts(s3Key, hangingUploads.get(s3Key).getFirst()))
                    .isNotEmpty();
        }
        assertThat(asf.addedRanges).isEmpty();
        final List<PersistedNotification> notifications = messaging.getSentPersistedNotifications();
        assertThat(notifications).hasSize(1);
        assertThat(notifications.getFirst().succeeded()).isFalse();
        assertThat(notifications.getFirst().blockNumber()).isZero();
        assertThat(notifications.getFirst().blockSource()).isEqualTo(BlockSource.PUBLISHER);
    }

    /// Verifies that when [doUploadTextFile] (the meta companion write) throws, [call()] rethrows
    /// the exception.  The `.tmp` key was already committed by [doCompleteMultipartUpload], so it
    /// IS present; only the `.meta` key is absent.  Because the tar is durable, a success
    /// [PersistedNotification] and range registration are still sent before rethrowing.
    @Test
    @DisplayName("Meta file write failure: exception rethrown; .tmp exists but .meta absent; success notification sent")
    void metaWriteFailureThrowsAndLeavesTmpButNoMeta() throws Exception {
        final String s3Key = TempArchiveKey.formatTar(0, "");
        final String metaKey = TempArchiveKey.formatMeta(0, "");
        final BlockingQueue<BlockWithSource> queue = new LinkedBlockingQueue<>();
        final TestBlockMessagingFacility messaging = new TestBlockMessagingFacility();
        final TrackingApplicationStateFacility asf = new TrackingApplicationStateFacility();

        queue.put(makeBlock(100));
        queue.put(TempArchiveUploadTask.SEGMENT_END);

        final TempArchiveUploadTask task =
                new FailingMetaTask(config, messaging, asf, createMetricsHolder(), s3Key, 0, queue);

        assertThatThrownBy(task::call).isInstanceOf(IOException.class);
        try (S3Client s3 = openS3Client()) {
            assertThat(s3.listObjects(s3Key, 1)).contains(s3Key);
            assertThat(s3.listObjects(metaKey, 1)).doesNotContain(metaKey);
        }
        assertThat(asf.addedRanges).hasSize(1);
        assertThat(asf.addedRanges.getFirst()).isEqualTo(new LongRange(0, 0));
        final List<PersistedNotification> notifications = messaging.getSentPersistedNotifications();
        assertThat(notifications).hasSize(1);
        assertThat(notifications.getFirst().succeeded()).isTrue();
        assertThat(notifications.getFirst().blockNumber()).isZero();
        assertThat(notifications.getFirst().blockSource()).isEqualTo(BlockSource.PUBLISHER);
    }

    /// Verifies that interrupting the virtual thread while it is blocked on [BlockingQueue#take]
    /// with no blocks accumulated causes [call()] to rethrow [InterruptedException] and leave the
    /// in-progress multipart upload hanging on S3 (not aborted) for [StartupRecoveryTask].
    @Test
    @DisplayName("Thread interruption with no blocks accumulated leaves upload hanging, rethrows InterruptedException")
    void interruptionWithNoBlocksLeavesUploadHangingAndRethrows() throws Exception {
        final String s3Key = TempArchiveKey.formatTar(0, "");
        final BlockingQueue<BlockWithSource> queue = new LinkedBlockingQueue<>();
        final TestBlockMessagingFacility messaging = new TestBlockMessagingFacility();
        final TrackingApplicationStateFacility asf = new TrackingApplicationStateFacility();
        final TempArchiveUploadTask task = buildTask(s3Key, 0, queue, messaging, asf);

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
            assertThat(s3.listMultipartUploads()).containsKey(s3Key);
        }
    }

    /// Verifies that interrupting the virtual thread while it is blocked on [BlockingQueue#take]
    /// after at least one block has been accumulated still rethrows [InterruptedException] from
    /// [call()] (unlike the zero-blocks case, the interrupt does not abort the upload first): the
    /// segment is completed and committed to S3 like a normal [TempArchiveUploadTask#SEGMENT_END]
    /// before the exception is rethrown.
    @Test
    @DisplayName("Thread interruption with blocks accumulated completes upload early, then rethrows")
    void interruptionWithBlocksAccumulatedCompletesEarlyThenRethrows() throws Exception {
        final String s3Key = TempArchiveKey.formatTar(0, "");
        final String metaKey = TempArchiveKey.formatMeta(0, "");
        final BlockingQueue<BlockWithSource> queue = new LinkedBlockingQueue<>();
        final TestBlockMessagingFacility messaging = new TestBlockMessagingFacility();
        final TrackingApplicationStateFacility asf = new TrackingApplicationStateFacility();
        final TempArchiveUploadTask task = buildTask(s3Key, 0, queue, messaging, asf);

        for (int i = 0; i < 3; i++) {
            queue.put(makeBlock(100));
        }

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
        // Give the task time to consume the queued blocks and block on the next take().
        Thread.sleep(200);
        thread.interrupt();
        thread.join(5_000);

        assertThat(caught[0]).isNotNull();

        try (S3Client s3 = openS3Client()) {
            assertThat(s3.listObjects(s3Key, 1)).contains(s3Key);
            assertThat(s3.listMultipartUploads()).doesNotContainKey(s3Key);
            assertThat(s3.downloadTextFile(metaKey)).isEqualTo("2");
        }
        final List<PersistedNotification> notifications = messaging.getSentPersistedNotifications();
        assertThat(notifications).hasSize(1);
        assertThat(notifications.getFirst().blockNumber()).isEqualTo(2L);
        assertThat(notifications.getFirst().succeeded()).isTrue();
        assertThat(notifications.getFirst().blockSource()).isEqualTo(BlockSource.PUBLISHER);
        assertThat(asf.addedRanges).hasSize(1);
        assertThat(asf.addedRanges.getFirst()).isEqualTo(new LongRange(0, 2));
    }

    /// Verifies that when [doUploadPart] throws during a mid-loop part flush, [call()] rethrows
    /// the exception, sends one failed [PersistedNotification] for the first unseen block --
    /// consistent with the other two upload-failure paths in this class -- and leaves the
    /// multipart upload hanging on S3 rather than aborting it.
    @Test
    @DisplayName("Part upload failure during loop: failed notification sent, exception rethrown, upload left hanging")
    void partUploadFailureDuringLoopSendsFailedNotificationAndThrows() throws Exception {
        final String s3Key = TempArchiveKey.formatTar(0, "");
        final BlockingQueue<BlockWithSource> queue = new LinkedBlockingQueue<>();
        final TestBlockMessagingFacility messaging = new TestBlockMessagingFacility();
        final TrackingApplicationStateFacility asf = new TrackingApplicationStateFacility();

        // Large blocks so the cumulative buffer exceeds PART_SIZE_MB before SEGMENT_END,
        // triggering the mid-loop doUploadPart call.
        for (int i = 0; i < 10; i++) {
            queue.put(makeBlock(BLOCK_DATA_BYTES));
        }
        queue.put(TempArchiveUploadTask.SEGMENT_END);

        final TempArchiveUploadTask task =
                new FailingUploadPartTask(config, messaging, asf, createMetricsHolder(), s3Key, 0, queue);

        assertThatThrownBy(task::call).isInstanceOf(IOException.class);
        final List<PersistedNotification> notifications = messaging.getSentPersistedNotifications();
        assertThat(notifications).hasSize(1);
        assertThat(notifications.getFirst().succeeded()).isFalse();
        assertThat(notifications.getFirst().blockNumber()).isZero();
        assertThat(notifications.getFirst().blockSource()).isEqualTo(BlockSource.PUBLISHER);
        assertThat(asf.addedRanges).isEmpty();
        try (S3Client s3 = openS3Client()) {
            assertThat(s3.listMultipartUploads()).containsKey(s3Key);
        }
    }

    /// Verifies that when [doUploadPart] throws while uploading the final partial buffer (after
    /// SEGMENT_END), [call()] sends one failed [PersistedNotification] for the first unseen block,
    /// rethrows the exception, and leaves the multipart upload hanging on S3 rather than aborting it.
    @Test
    @DisplayName("Part upload failure for final part: failed notification sent, upload left hanging")
    void partUploadFailureForFinalPartSendsFailedNotificationAndLeavesUploadHanging() throws Exception {
        final String s3Key = TempArchiveKey.formatTar(0, "");
        final BlockingQueue<BlockWithSource> queue = new LinkedBlockingQueue<>();
        final TestBlockMessagingFacility messaging = new TestBlockMessagingFacility();
        final TrackingApplicationStateFacility asf = new TrackingApplicationStateFacility();

        // Small blocks so the buffer stays below PART_SIZE_MB throughout the loop;
        // doUploadPart is only called for the final partial buffer.
        for (int i = 0; i < 3; i++) {
            queue.put(makeBlock(100));
        }
        queue.put(TempArchiveUploadTask.SEGMENT_END);

        final TempArchiveUploadTask task =
                new FailingUploadPartTask(config, messaging, asf, createMetricsHolder(), s3Key, 0, queue);

        assertThatThrownBy(task::call).isInstanceOf(IOException.class);
        final List<PersistedNotification> notifications = messaging.getSentPersistedNotifications();
        assertThat(notifications).hasSize(1);
        assertThat(notifications.getFirst().succeeded()).isFalse();
        assertThat(notifications.getFirst().blockNumber()).isZero();
        assertThat(notifications.getFirst().blockSource()).isEqualTo(BlockSource.PUBLISHER);
        assertThat(asf.addedRanges).isEmpty();
        try (S3Client s3 = openS3Client()) {
            assertThat(s3.listMultipartUploads()).containsKey(s3Key);
        }
    }

    /// Verifies the multi-part success path: blocks large enough to trigger a mid-loop flush
    /// produce a single [applicationStateFacility.addStoredBlockRange] call covering the full
    /// range after both the multipart complete and the meta write succeed.
    @Test
    @DisplayName("Multi-part success: single addStoredBlockRange covering full range after completion")
    void multiPartSuccessRegistersFullRangeAfterCompletion() throws Exception {
        final String s3Key = TempArchiveKey.formatTar(0, "");
        final BlockingQueue<BlockWithSource> queue = new LinkedBlockingQueue<>();
        final TestBlockMessagingFacility messaging = new TestBlockMessagingFacility();
        final TrackingApplicationStateFacility asf = new TrackingApplicationStateFacility();

        // ~600 KB per block; 10 blocks = ~6 MB, exceeding the 5 MB part threshold so at least
        // one mid-loop flush occurs before SEGMENT_END.
        final int numBlocks = 10;
        for (int i = 0; i < numBlocks; i++) {
            queue.put(makeBlock(BLOCK_DATA_BYTES));
        }
        queue.put(TempArchiveUploadTask.SEGMENT_END);

        final TempArchiveEntry entry =
                buildTask(s3Key, 0, queue, messaging, asf).call();

        assertThat(entry.firstBlock()).isZero();
        assertThat(entry.lastBlock()).isEqualTo(numBlocks - 1L);
        assertThat(asf.addedRanges).hasSize(1);
        assertThat(asf.addedRanges.getFirst()).isEqualTo(new LongRange(0, numBlocks - 1L));
        final List<PersistedNotification> notifications = messaging.getSentPersistedNotifications();
        assertThat(notifications).hasSize(1);
        assertThat(notifications.getFirst().blockNumber()).isEqualTo(numBlocks - 1L);
        assertThat(notifications.getFirst().succeeded()).isTrue();
    }

    /// Verifies the resume constructor's key behaviors together: it reuses the given upload ID and
    /// ETags instead of starting a new multipart upload, prepends
    /// [TempArchiveResumeState#trailingBytes()] to the accumulation buffer, begins consuming the
    /// queue at [TempArchiveResumeState#nextBlockNumber()] rather than [firstBlock], and the
    /// returned [TempArchiveEntry#firstBlock()] still reflects the original segment identity
    /// passed to the constructor, not the resume point.
    @Test
    @DisplayName("Resume constructor: reuses uploadId/etags, prepends trailingBytes, resumes from nextBlockNumber")
    void resumeConstructorResumesFromRecoveredStateAndPreservesSegmentIdentity() throws Exception {
        final String s3Key = TempArchiveKey.formatTar(0, "");
        final long firstBlock = 0;

        // Manually build and upload a first part large enough to satisfy S3's non-final-part
        // minimum size, simulating parts already server-side-copied by StartupRecoveryTask.
        final List<BlockWithSource> preBlocks = new ArrayList<>();
        for (int i = 0; i < 9; i++) {
            preBlocks.add(makeBlock(BLOCK_DATA_BYTES));
        }
        byte[] part1Bytes = new byte[0];
        for (int i = 0; i < preBlocks.size(); i++) {
            part1Bytes = S3UploadUtils.concat(
                    part1Bytes, TarEntries.toTarEntry(preBlocks.get(i).block(), firstBlock + i));
        }
        assertThat(part1Bytes.length).isGreaterThanOrEqualTo(PART_SIZE_MB * 1024 * 1024);

        final String uploadId;
        final String etag1;
        try (S3Client s3 = openS3Client()) {
            uploadId = s3.createMultipartUpload(s3Key, config.storageClass().name(), S3UploadUtils.CONTENT_TYPE);
            etag1 = s3.multipartUploadPart(s3Key, uploadId, 1, part1Bytes);
        }

        final long nextBlockNumber = preBlocks.size();
        // Arbitrary carry-over bytes representing a boundary part's not-yet-full tail.
        final byte[] trailingBytes = TarEntries.toTarEntry(makeBlock(50).block(), 999);
        final TempArchiveResumeState resumeState =
                new TempArchiveResumeState(s3Key, firstBlock, nextBlockNumber, uploadId, List.of(etag1), trailingBytes);

        final BlockingQueue<BlockWithSource> queue = new LinkedBlockingQueue<>();
        final TestBlockMessagingFacility messaging = new TestBlockMessagingFacility();
        final TrackingApplicationStateFacility asf = new TrackingApplicationStateFacility();
        final BlockWithSource newBlock = makeBlock(100);
        queue.put(newBlock);
        queue.put(TempArchiveUploadTask.SEGMENT_END);

        final TempArchiveUploadTask task = new TempArchiveUploadTask(
                config, messaging, asf, createMetricsHolder(), s3Key, firstBlock, queue, resumeState);

        final TempArchiveEntry entry = task.call();

        // Original firstBlock identity preserved even though consumption started at nextBlockNumber.
        assertThat(entry.firstBlock()).isZero();
        // Consumption started at nextBlockNumber (9); the single new block became block 9.
        assertThat(entry.lastBlock()).isEqualTo(nextBlockNumber);

        try (S3Client s3 = openS3Client()) {
            // Reused the existing upload -- it completed and is no longer hanging.
            assertThat(s3.listMultipartUploads()).doesNotContainKey(s3Key);
            final byte[] newEntryBytes = TarEntries.toTarEntry(newBlock.block(), nextBlockNumber);
            final int expectedLength = part1Bytes.length + trailingBytes.length + newEntryBytes.length;
            final byte[] finalTar = s3.downloadObjectRange(s3Key, 0, expectedLength - 1);
            // The final tar begins with the manually-uploaded first part (proving the existing
            // uploadId/etags were reused rather than a new upload started), followed by the
            // prepended trailingBytes, followed by the new block's tar entry.
            assertThat(Arrays.copyOfRange(finalTar, 0, part1Bytes.length)).isEqualTo(part1Bytes);
            assertThat(Arrays.copyOfRange(finalTar, part1Bytes.length, part1Bytes.length + trailingBytes.length))
                    .isEqualTo(trailingBytes);
            assertThat(Arrays.copyOfRange(finalTar, part1Bytes.length + trailingBytes.length, expectedLength))
                    .isEqualTo(newEntryBytes);
        }
    }

    /// Verifies the loopStart fix for a resumed task: when a [TempArchiveUploadTask] resumes via
    /// [TempArchiveResumeState] and is interrupted before consuming any NEW block, it must not
    /// mistake the already-durable resumed part for data accumulated in this run and spuriously
    /// complete the segment -- it must rethrow [InterruptedException] and leave the upload
    /// hanging, exactly like a fresh task interrupted with zero blocks (mirrors
    /// [StartupRecoveryTaskTest]'s round trip for the regular path, adapted to the resume path
    /// since any real accumulated block always completes a temp archive on interrupt).
    @Test
    @DisplayName("Resumed task interrupted before any new block: upload stays hanging, not spuriously completed")
    void resumedTaskInterruptedBeforeNewBlockLeavesUploadHanging() throws Exception {
        final String s3Key = TempArchiveKey.formatTar(0, "");
        final long firstBlock = 0;

        // Manually build and upload a real first part, simulating parts a previous run already
        // flushed and StartupRecoveryTask server-side-copied into a fresh upload before resuming.
        final List<BlockWithSource> preBlocks = new ArrayList<>();
        for (int i = 0; i < 9; i++) {
            preBlocks.add(makeBlock(BLOCK_DATA_BYTES));
        }
        byte[] part1Bytes = new byte[0];
        for (int i = 0; i < preBlocks.size(); i++) {
            part1Bytes = S3UploadUtils.concat(
                    part1Bytes, TarEntries.toTarEntry(preBlocks.get(i).block(), firstBlock + i));
        }
        final String uploadId;
        final String etag1;
        try (S3Client s3 = openS3Client()) {
            uploadId = s3.createMultipartUpload(s3Key, config.storageClass().name(), S3UploadUtils.CONTENT_TYPE);
            etag1 = s3.multipartUploadPart(s3Key, uploadId, 1, part1Bytes);
        }
        final long nextBlockNumber = preBlocks.size();
        final TempArchiveResumeState resumeState =
                new TempArchiveResumeState(s3Key, firstBlock, nextBlockNumber, uploadId, List.of(etag1), new byte[0]);

        final BlockingQueue<BlockWithSource> queue = new LinkedBlockingQueue<>();
        final TestBlockMessagingFacility messaging = new TestBlockMessagingFacility();
        final TrackingApplicationStateFacility asf = new TrackingApplicationStateFacility();
        final TempArchiveUploadTask task = new TempArchiveUploadTask(
                config, messaging, asf, createMetricsHolder(), s3Key, firstBlock, queue, resumeState);

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
        // Give the resumed task time to seed its state and block on the next take() -- no new
        // block is ever offered.
        Thread.sleep(200);
        thread.interrupt();
        thread.join(5_000);

        assertThat(caught[0]).isNotNull();
        assertThat(messaging.getSentPersistedNotifications()).isEmpty();
        try (S3Client s3 = openS3Client()) {
            final Map<String, List<String>> hangingUploads = s3.listMultipartUploads();
            assertThat(hangingUploads).containsKey(s3Key);
            assertThat(s3.listParts(s3Key, hangingUploads.get(s3Key).getFirst()))
                    .hasSize(1);
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

    private static final class TrackingApplicationStateFacility implements ApplicationStateFacility {
        final List<LongRange> addedRanges = new ArrayList<>();

        @Override
        public void updateTssData(TssData tssData) {}

        @Override
        public void addStoredBlockRange(LongRange blockRange) {
            addedRanges.add(blockRange);
        }

        @Override
        public NetworkData knownPublishers() {
            return null;
        }

        @Override
        public NetworkData inboundPartners() {
            return null;
        }

        @Override
        public NetworkData outboundPartners() {
            return null;
        }

        @Override
        public NetworkData backfillSources() {
            return null;
        }

        @Override
        public void updateBackfillSources(final NetworkData sources) {}
    }
}
