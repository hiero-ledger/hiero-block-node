// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.cloud.storage.expanded;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.app.fixtures.blocks.TestBlock;
import org.hiero.block.node.app.fixtures.blocks.TestBlockBuilder;
import org.hiero.block.node.app.fixtures.plugintest.PluginTestBase;
import org.hiero.block.node.app.fixtures.plugintest.SimpleInMemoryHistoricalBlockFacility;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.PersistedNotification;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification.FailureType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/// Unit tests for {@link ExpandedCloudStoragePlugin}.
///
/// All tests here inject a `CapturingS3Client` or an anonymous {@link S3UploadClient}
/// subclass via the package-private constructor — no Docker or real S3 endpoint required.
///
/// @see ExpandedCloudStoragePluginIntegrationTest for S3Mock-backed integration tests
@Timeout(value = 30, unit = TimeUnit.SECONDS)
class ExpandedCloudStoragePluginTest
        extends PluginTestBase<ExpandedCloudStoragePlugin, ExecutorService, ScheduledExecutorService> {

    private static final Instant START_TIME =
            ZonedDateTime.of(2025, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant();
    private static final Duration ONE_DAY = Duration.of(1, ChronoUnit.DAYS);

    // ---- Capturing S3 client ------------------------------------------------

    private record UploadCall(String objectKey, String storageClass, String contentType) {}

    /// Records `uploadFile` calls so tests can assert the exact arguments passed to the
    /// upload client without hitting any real endpoint.
    ///
    /// Uses {@link CopyOnWriteArrayList} because {@code uploadFile} is called from virtual
    /// upload threads while the test thread reads {@code uploads} after awaiting notifications.
    private static class CapturingS3Client implements S3UploadClient {
        final List<UploadCall> uploads = new CopyOnWriteArrayList<>();

        @Override
        public void uploadFile(
                final String objectKey,
                final String storageClass,
                final Iterator<byte[]> contentIterable,
                final String contentType) {
            uploads.add(new UploadCall(objectKey, storageClass, contentType));
        }

        @Override
        public void close() {}
    }

    /// Provides the single-threaded executors required by {@link PluginTestBase}.
    /// A single thread is sufficient here because {@code handleVerification} is always
    /// called from the test thread; the virtual-thread executor is used internally by
    /// the plugin for async upload tasks.
    public ExpandedCloudStoragePluginTest() {
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

    /// Builds a {@link VerificationNotification} that reports {@code success=false} —
    /// the plugin must skip the upload for these notifications.
    private VerificationNotification failedNotification(final long blockNumber) {
        return new VerificationNotification(
                false, FailureType.BAD_BLOCK_PROOF, blockNumber, Bytes.EMPTY, null, BlockSource.UNKNOWN);
    }

    /// Drives the plugin's drain loop and polls until at least `expectedCount`
    /// {@link PersistedNotification}s have been dispatched, or the 5-second timeout
    /// elapses. Uses the package-private
    /// {@link ExpandedCloudStoragePlugin#drainCompletedTasks()} so tests do not need a
    /// second `handleVerification` call to flush results.
    private void awaitNotifications(final int expectedCount) throws InterruptedException {
        final long deadline = System.currentTimeMillis() + 5_000L;
        while (System.currentTimeMillis() < deadline) {
            plugin.drainCompletedTasks();
            if (blockMessaging.getSentPersistedNotifications().size() >= expectedCount) return;
            Thread.sleep(10);
        }
    }

    // ---- Tests --------------------------------------------------------------

    @Test
    @DisplayName("Plugin skips upload for a failed VerificationNotification")
    void skipsUploadOnFailedVerification() {
        final CapturingS3Client capturing = new CapturingS3Client();
        start(
                new ExpandedCloudStoragePlugin(capturing),
                new SimpleInMemoryHistoricalBlockFacility(),
                Map.of(
                        "cloud.storage.expanded.endpointUrl", "http://fake:9000",
                        "cloud.storage.expanded.bucketName", "test-bucket",
                        "cloud.storage.expanded.regionName", "us-east-1"));

        plugin.handleVerification(failedNotification(0L));
        // Failed verification is skipped synchronously before any task is submitted.

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
                        "cloud.storage.expanded.endpointUrl", "http://fake:9000",
                        "cloud.storage.expanded.bucketName", "test-bucket",
                        "cloud.storage.expanded.regionName", "us-east-1",
                        "cloud.storage.expanded.objectKeyPrefix", "myblocks",
                        "cloud.storage.expanded.storageClass", "STANDARD"));

        plugin.handleVerification(verifiedNotification(0L, testBlock(0).blockUnparsed()));
        awaitNotifications(1);

        assertEquals(1, capturing.uploads.size(), "Exactly one uploadFile call expected");
        final UploadCall call = capturing.uploads.getFirst();
        // Block 0: 0000000000000000000 → myblocks/0000/0000/0000/0000/000.blk.zstd
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
                        "cloud.storage.expanded.endpointUrl", "http://fake:9000",
                        "cloud.storage.expanded.bucketName", "test-bucket",
                        "cloud.storage.expanded.regionName", "us-east-1",
                        "cloud.storage.expanded.objectKeyPrefix", "blocks"));

        // Block 1:         0000000000000000001 → blocks/0000/0000/0000/0000/001.blk.zstd
        // Block 108273182: 0000000000108273182 → blocks/0000/0000/0010/8273/182.blk.zstd
        // Block 1234567:   0000000000001234567 → blocks/0000/0000/0000/1234/567.blk.zstd
        plugin.handleVerification(verifiedNotification(1L, testBlock(1).blockUnparsed()));
        plugin.handleVerification(
                verifiedNotification(108273182L, testBlock(108273182L).blockUnparsed()));
        plugin.handleVerification(
                verifiedNotification(1234567L, testBlock(1234567L).blockUnparsed()));
        awaitNotifications(3);

        assertEquals(3, capturing.uploads.size());
        final Set<String> keys =
                capturing.uploads.stream().map(UploadCall::objectKey).collect(Collectors.toSet());
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
                        "cloud.storage.expanded.endpointUrl", "http://fake:9000",
                        "cloud.storage.expanded.bucketName", "test-bucket",
                        "cloud.storage.expanded.regionName", "us-east-1",
                        "cloud.storage.expanded.objectKeyPrefix", ""));

        plugin.handleVerification(verifiedNotification(1L, testBlock(1).blockUnparsed()));
        awaitNotifications(1);

        assertEquals(1, capturing.uploads.size());
        assertEquals(
                "0000/0000/0000/0000/001.blk.zstd", capturing.uploads.getFirst().objectKey());
    }

    @Test
    @DisplayName("Successful upload publishes PersistedNotification with succeeded=true")
    void successPublishesPersistedNotification() throws InterruptedException {
        final CapturingS3Client capturing = new CapturingS3Client();
        start(
                new ExpandedCloudStoragePlugin(capturing),
                new SimpleInMemoryHistoricalBlockFacility(),
                Map.of(
                        "cloud.storage.expanded.endpointUrl", "http://fake:9000",
                        "cloud.storage.expanded.bucketName", "test-bucket",
                        "cloud.storage.expanded.regionName", "us-east-1"));

        plugin.handleVerification(verifiedNotification(42L, testBlock(42).blockUnparsed()));
        awaitNotifications(1);

        final List<PersistedNotification> notifications = blockMessaging.getSentPersistedNotifications();
        assertEquals(1, notifications.size(), "Exactly one PersistedNotification expected");
        assertEquals(42L, notifications.getFirst().blockNumber());
        assertTrue(notifications.getFirst().succeeded(), "PersistedNotification must report succeeded=true");
    }

    @Test
    @DisplayName("UploadException from uploadFile produces PersistedNotification with succeeded=false")
    void uploadExceptionProducesFailedNotification() throws InterruptedException {
        final S3UploadClient throwingClient = new S3UploadClient() {
            @Override
            public void uploadFile(
                    final String objectKey,
                    final String storageClass,
                    final Iterator<byte[]> contentIterable,
                    final String contentType)
                    throws UploadException {
                throw new UploadException("Simulated S3 service error", null);
            }

            @Override
            public void close() {}
        };
        start(
                new ExpandedCloudStoragePlugin(throwingClient),
                new SimpleInMemoryHistoricalBlockFacility(),
                Map.of(
                        "cloud.storage.expanded.endpointUrl", "http://fake:9000",
                        "cloud.storage.expanded.bucketName", "test-bucket",
                        "cloud.storage.expanded.regionName", "us-east-1"));

        plugin.handleVerification(verifiedNotification(7L, testBlock(7).blockUnparsed()));
        awaitNotifications(1);

        final List<PersistedNotification> notifications = blockMessaging.getSentPersistedNotifications();
        assertEquals(1, notifications.size(), "PersistedNotification must be sent even on UploadException");
        assertEquals(7L, notifications.getFirst().blockNumber());
        assertFalse(
                notifications.getFirst().succeeded(),
                "PersistedNotification must report succeeded=false on UploadException");
    }

    @Test
    @DisplayName("UploadException thrown by uploadFile is not rethrown by handleVerification")
    void uploadExceptionNotRethrown() {
        final S3UploadClient throwingClient = new S3UploadClient() {
            @Override
            public void uploadFile(
                    final String objectKey,
                    final String storageClass,
                    final Iterator<byte[]> contentIterable,
                    final String contentType)
                    throws UploadException {
                throw new UploadException("Simulated S3 failure", null);
            }

            @Override
            public void close() {}
        };
        start(
                new ExpandedCloudStoragePlugin(throwingClient),
                new SimpleInMemoryHistoricalBlockFacility(),
                Map.of(
                        "cloud.storage.expanded.endpointUrl", "http://fake:9000",
                        "cloud.storage.expanded.bucketName", "test-bucket",
                        "cloud.storage.expanded.regionName", "us-east-1"));

        assertDoesNotThrow(
                () -> plugin.handleVerification(
                        verifiedNotification(0L, testBlock(0).blockUnparsed())),
                "UploadException must never propagate out of handleVerification");
    }

    @Test
    @DisplayName("StorageClass enum only contains defined values")
    void storageClassEnumValues() {
        final ExpandedCloudStorageConfig.StorageClass[] values = ExpandedCloudStorageConfig.StorageClass.values();
        assertEquals(1, values.length, "Expected exactly one StorageClass value");
        assertEquals(ExpandedCloudStorageConfig.StorageClass.STANDARD, values[0]);
    }

    @Test
    @DisplayName("All defined StorageClass enum values are accepted by config")
    void allValidStorageClassesAccepted() {
        for (final ExpandedCloudStorageConfig.StorageClass sc : ExpandedCloudStorageConfig.StorageClass.values()) {
            assertDoesNotThrow(
                    () -> new ExpandedCloudStorageConfig(
                            "http://fake:9000", "bucket", "blocks", sc, "us-east-1", "", "", 60),
                    "StorageClass " + sc + " must not throw");
        }
    }

    @Test
    @DisplayName("Config accepts blank required fields — plugin logs WARNING instead of throwing")
    void configAcceptsBlankRequiredFields() {
        // blank bucketName, endpointUrl, and regionName must all be accepted at construction time;
        // the plugin reports misconfiguration via WARNING logs, not exceptions.
        assertDoesNotThrow(
                () -> new ExpandedCloudStorageConfig(
                        "http://fake:9000",
                        "",
                        "blocks",
                        ExpandedCloudStorageConfig.StorageClass.STANDARD,
                        "us-east-1",
                        "",
                        "",
                        60),
                "blank bucketName must not throw");
        assertDoesNotThrow(
                () -> new ExpandedCloudStorageConfig(
                        "",
                        "bucket",
                        "blocks",
                        ExpandedCloudStorageConfig.StorageClass.STANDARD,
                        "us-east-1",
                        "",
                        "",
                        60),
                "blank endpointUrl must not throw");
        assertDoesNotThrow(
                () -> new ExpandedCloudStorageConfig(
                        "http://fake:9000",
                        "bucket",
                        "blocks",
                        ExpandedCloudStorageConfig.StorageClass.STANDARD,
                        "",
                        "",
                        "",
                        60),
                "blank regionName must not throw");
    }

    @Test
    @DisplayName("handleVerification skips upload for null block body and negative block number")
    void handleVerificationGuardsSkipUpload() {
        final CapturingS3Client capturing = new CapturingS3Client();
        start(
                new ExpandedCloudStoragePlugin(capturing),
                new SimpleInMemoryHistoricalBlockFacility(),
                Map.of(
                        "cloud.storage.expanded.endpointUrl", "http://fake:9000",
                        "cloud.storage.expanded.bucketName", "test-bucket",
                        "cloud.storage.expanded.regionName", "us-east-1"));

        // verified=true but block payload is null
        plugin.handleVerification(new VerificationNotification(true, null, 1L, Bytes.EMPTY, null, BlockSource.UNKNOWN));
        // verified=true but block number is negative
        plugin.handleVerification(new VerificationNotification(
                true, null, -1L, Bytes.EMPTY, testBlock(0).blockUnparsed(), BlockSource.UNKNOWN));
        // Both cases are skipped synchronously before any task is submitted.

        assertEquals(0, capturing.uploads.size(), "No upload expected for null block or negative block number");
        assertTrue(
                blockMessaging.getSentPersistedNotifications().isEmpty(),
                "No PersistedNotification expected when upload was skipped");
    }

    @Test
    @DisplayName("IOException from uploadFile produces PersistedNotification with succeeded=false")
    void ioExceptionProducesFailedNotification() throws InterruptedException {
        final S3UploadClient throwingClient = new S3UploadClient() {
            @Override
            public void uploadFile(
                    final String objectKey,
                    final String storageClass,
                    final Iterator<byte[]> contentIterable,
                    final String contentType)
                    throws UploadException, IOException {
                throw new IOException("Simulated I/O failure");
            }

            @Override
            public void close() {}
        };
        start(
                new ExpandedCloudStoragePlugin(throwingClient),
                new SimpleInMemoryHistoricalBlockFacility(),
                Map.of(
                        "cloud.storage.expanded.endpointUrl", "http://fake:9000",
                        "cloud.storage.expanded.bucketName", "test-bucket",
                        "cloud.storage.expanded.regionName", "us-east-1"));

        plugin.handleVerification(verifiedNotification(5L, testBlock(5).blockUnparsed()));
        awaitNotifications(1);

        final List<PersistedNotification> notifications = blockMessaging.getSentPersistedNotifications();
        assertEquals(1, notifications.size(), "PersistedNotification must be sent even on IOException");
        assertEquals(5L, notifications.getFirst().blockNumber());
        assertFalse(
                notifications.getFirst().succeeded(),
                "PersistedNotification must report succeeded=false on IOException");
    }

    @Test
    @DisplayName("stop() closes the injected S3 client")
    void stopClosesS3Client() throws InterruptedException {
        final AtomicBoolean closed = new AtomicBoolean(false);
        final S3UploadClient trackingClient = new S3UploadClient() {
            @Override
            public void uploadFile(
                    final String objectKey,
                    final String storageClass,
                    final Iterator<byte[]> contentIterable,
                    final String contentType) {}

            @Override
            public void close() {
                closed.set(true);
            }
        };
        start(
                new ExpandedCloudStoragePlugin(trackingClient),
                new SimpleInMemoryHistoricalBlockFacility(),
                Map.of(
                        "cloud.storage.expanded.endpointUrl", "http://fake:9000",
                        "cloud.storage.expanded.bucketName", "test-bucket",
                        "cloud.storage.expanded.regionName", "us-east-1"));

        plugin.handleVerification(verifiedNotification(1L, testBlock(1).blockUnparsed()));
        awaitNotifications(1);

        plugin.stop();
        assertTrue(closed.get(), "plugin.stop() must call close() on the S3 client");
    }

    @Test
    @DisplayName("buildBlockObjectKey handles zero, leaf-to-seg4 rollover, and max 19-digit block number")
    void objectKeyEdgeCasesIncludingZeroAndMax() {
        final CapturingS3Client capturing = new CapturingS3Client();
        start(
                new ExpandedCloudStoragePlugin(capturing),
                new SimpleInMemoryHistoricalBlockFacility(),
                Map.of(
                        "cloud.storage.expanded.endpointUrl", "http://fake:9000",
                        "cloud.storage.expanded.bucketName", "test-bucket",
                        "cloud.storage.expanded.regionName", "us-east-1",
                        "cloud.storage.expanded.objectKeyPrefix", "blocks"));

        assertEquals(
                "blocks/0000/0000/0000/0000/000.blk.zstd",
                plugin.buildBlockObjectKey(0L),
                "Block 0 must produce all-zero segments");
        assertEquals(
                "blocks/0000/0000/0000/0000/999.blk.zstd",
                plugin.buildBlockObjectKey(999L),
                "Block 999 must be the max leaf value before rollover");
        assertEquals(
                "blocks/0000/0000/0000/0001/000.blk.zstd",
                plugin.buildBlockObjectKey(1_000L),
                "Block 1000 must roll leaf into seg4");
        assertEquals(
                "blocks/9223/3720/3685/4775/807.blk.zstd",
                plugin.buildBlockObjectKey(Long.MAX_VALUE),
                "Max 19-digit block number must saturate all segments");
    }

    @Test
    @DisplayName("Metrics: successful upload increments uploadsTotal, bytesTotal, and latencyNs")
    void successIncrementsUploadAndByteCounters() throws InterruptedException {
        final CapturingS3Client capturing = new CapturingS3Client();
        start(
                new ExpandedCloudStoragePlugin(capturing),
                new SimpleInMemoryHistoricalBlockFacility(),
                Map.of(
                        "cloud.storage.expanded.endpointUrl", "http://fake:9000",
                        "cloud.storage.expanded.bucketName", "test-bucket",
                        "cloud.storage.expanded.regionName", "us-east-1"));

        plugin.handleVerification(verifiedNotification(1L, testBlock(1).blockUnparsed()));
        awaitNotifications(1);

        assertEquals(
                1L,
                getMetricValue(ExpandedCloudStoragePlugin.METRIC_EXPANDED_CLOUD_STORAGE_TOTAL_UPLOADS),
                "uploadsTotal must be 1 after one successful upload");
        assertEquals(
                0L,
                getMetricValue(ExpandedCloudStoragePlugin.METRIC_EXPANDED_CLOUD_STORAGE_TOTAL_UPLOAD_FAILURES),
                "uploadFailuresTotal must be 0 after a successful upload");
        assertTrue(
                getMetricValue(ExpandedCloudStoragePlugin.METRIC_EXPANDED_CLOUD_STORAGE_TOTAL_UPLOADED_BYTES) > 0L,
                "uploadBytesTotal must be positive after a successful upload");
        assertTrue(
                getMetricValue(ExpandedCloudStoragePlugin.METRIC_EXPANDED_CLOUD_STORAGE_UPLOAD_LATENCY_NS) > 0L,
                "uploadLatencyNs must be positive after a successful upload");
    }

    @Test
    @DisplayName("Metrics: failed upload increments failuresTotal and latencyNs; bytesTotal stays zero")
    void failureIncrementsFailureCounterAndLatency() throws InterruptedException {
        final S3UploadClient throwingClient = new S3UploadClient() {
            @Override
            public void uploadFile(
                    final String objectKey,
                    final String storageClass,
                    final Iterator<byte[]> contentIterable,
                    final String contentType)
                    throws UploadException {
                throw new UploadException("Simulated failure for metrics test", null);
            }

            @Override
            public void close() {}
        };
        start(
                new ExpandedCloudStoragePlugin(throwingClient),
                new SimpleInMemoryHistoricalBlockFacility(),
                Map.of(
                        "cloud.storage.expanded.endpointUrl", "http://fake:9000",
                        "cloud.storage.expanded.bucketName", "test-bucket",
                        "cloud.storage.expanded.regionName", "us-east-1"));

        plugin.handleVerification(verifiedNotification(1L, testBlock(1).blockUnparsed()));
        awaitNotifications(1);

        assertEquals(
                0L,
                getMetricValue(ExpandedCloudStoragePlugin.METRIC_EXPANDED_CLOUD_STORAGE_TOTAL_UPLOADS),
                "uploadsTotal must be 0 after a failed upload");
        assertEquals(
                1L,
                getMetricValue(ExpandedCloudStoragePlugin.METRIC_EXPANDED_CLOUD_STORAGE_TOTAL_UPLOAD_FAILURES),
                "uploadFailuresTotal must be 1 after a failed upload");
        assertEquals(
                0L,
                getMetricValue(ExpandedCloudStoragePlugin.METRIC_EXPANDED_CLOUD_STORAGE_TOTAL_UPLOADED_BYTES),
                "uploadBytesTotal must be 0 after a failed upload");
        assertTrue(
                getMetricValue(ExpandedCloudStoragePlugin.METRIC_EXPANDED_CLOUD_STORAGE_UPLOAD_LATENCY_NS) > 0L,
                "uploadLatencyNs must be positive even after a failed upload");
    }

    @Test
    @DisplayName("Ten concurrent verified blocks each produce a PersistedNotification with succeeded=true")
    void tenBlocksAllProduceSuccessNotifications() throws InterruptedException {
        final CapturingS3Client capturing = new CapturingS3Client();
        start(
                new ExpandedCloudStoragePlugin(capturing),
                new SimpleInMemoryHistoricalBlockFacility(),
                Map.of(
                        "cloud.storage.expanded.endpointUrl", "http://fake:9000",
                        "cloud.storage.expanded.bucketName", "test-bucket",
                        "cloud.storage.expanded.regionName", "us-east-1"));

        for (int i = 0; i < 10; i++) {
            plugin.handleVerification(verifiedNotification(i, testBlock(i).blockUnparsed()));
        }
        awaitNotifications(10);

        final List<PersistedNotification> notifications = blockMessaging.getSentPersistedNotifications();
        assertEquals(10, notifications.size(), "Each of the ten blocks must produce exactly one PersistedNotification");
        assertTrue(
                notifications.stream().allMatch(PersistedNotification::succeeded),
                "All notifications must report succeeded=true");
    }

    @Test
    @DisplayName(
            "Unchecked exception escaping upload task increments failure counter and sends no PersistedNotification")
    void uncheckedExceptionInTaskIncrementsFailureCounter() throws InterruptedException {
        final CountDownLatch exceptionThrown = new CountDownLatch(1);
        final S3UploadClient throwingClient = new S3UploadClient() {
            @Override
            public void uploadFile(
                    final String objectKey,
                    final String storageClass,
                    final Iterator<byte[]> contentIterable,
                    final String contentType) {
                exceptionThrown.countDown();
                throw new RuntimeException("Simulated unexpected task failure");
            }

            @Override
            public void close() {}
        };
        start(
                new ExpandedCloudStoragePlugin(throwingClient),
                new SimpleInMemoryHistoricalBlockFacility(),
                Map.of(
                        "cloud.storage.expanded.endpointUrl", "http://fake:9000",
                        "cloud.storage.expanded.bucketName", "test-bucket",
                        "cloud.storage.expanded.regionName", "us-east-1"));

        plugin.handleVerification(verifiedNotification(1L, testBlock(1).blockUnparsed()));
        // Wait for the virtual thread to throw, then give it a moment to fully complete.
        assertTrue(exceptionThrown.await(5, TimeUnit.SECONDS), "Upload task must have thrown within 5s");
        Thread.sleep(20);
        plugin.drainCompletedTasks();

        assertTrue(
                blockMessaging.getSentPersistedNotifications().isEmpty(),
                "No PersistedNotification expected when an unchecked exception escapes the task");
        assertEquals(
                1L,
                getMetricValue(ExpandedCloudStoragePlugin.METRIC_EXPANDED_CLOUD_STORAGE_TOTAL_UPLOAD_FAILURES),
                "uploadFailuresTotal must be incremented for the escaped RuntimeException");
    }

    @Test
    @DisplayName(
            "PersistedNotifications are published in ascending block-number order when results are drained together")
    void notificationsPublishedInAscendingBlockOrder() throws InterruptedException {
        final CountDownLatch bothUploaded = new CountDownLatch(2);
        final S3UploadClient countingClient = new S3UploadClient() {
            @Override
            public void uploadFile(
                    final String objectKey,
                    final String storageClass,
                    final Iterator<byte[]> contentIterable,
                    final String contentType) {
                bothUploaded.countDown();
            }

            @Override
            public void close() {}
        };
        start(
                new ExpandedCloudStoragePlugin(countingClient),
                new SimpleInMemoryHistoricalBlockFacility(),
                Map.of(
                        "cloud.storage.expanded.endpointUrl", "http://fake:9000",
                        "cloud.storage.expanded.bucketName", "test-bucket",
                        "cloud.storage.expanded.regionName", "us-east-1"));

        // Submit out of numeric order: 7 first, then 2
        plugin.handleVerification(verifiedNotification(7L, testBlock(7).blockUnparsed()));
        plugin.handleVerification(verifiedNotification(2L, testBlock(2).blockUnparsed()));

        // Wait for both uploads to complete, then do a single drain pass so both results
        // are staged in the ConcurrentSkipListMap before any notification is published.
        assertTrue(bothUploaded.await(5, TimeUnit.SECONDS), "Both uploads must complete within 5s");
        Thread.sleep(50); // let virtual threads return UploadResult to CompletionService
        plugin.drainCompletedTasks();

        final List<PersistedNotification> notifications = blockMessaging.getSentPersistedNotifications();
        assertEquals(2, notifications.size(), "Both blocks must produce PersistedNotifications");
        assertEquals(
                2L,
                notifications.get(0).blockNumber(),
                "Block 2 must be published first even though it was submitted second");
        assertEquals(
                7L,
                notifications.get(1).blockNumber(),
                "Block 7 must be published second even though it was submitted first");
    }

    @Test
    @DisplayName(
            "Mixed success and failure in the same drain batch each produce the correct PersistedNotification and metrics")
    void mixedSuccessAndFailureProduceCorrectNotificationsAndMetrics() throws InterruptedException {
        final S3UploadClient mixedClient = new S3UploadClient() {
            @Override
            public void uploadFile(
                    final String objectKey,
                    final String storageClass,
                    final Iterator<byte[]> contentIterable,
                    final String contentType)
                    throws UploadException {
                if (objectKey.contains("/002.blk.zstd")) {
                    throw new UploadException("Simulated failure for block 2", null);
                }
            }

            @Override
            public void close() {}
        };
        start(
                new ExpandedCloudStoragePlugin(mixedClient),
                new SimpleInMemoryHistoricalBlockFacility(),
                Map.of(
                        "cloud.storage.expanded.endpointUrl", "http://fake:9000",
                        "cloud.storage.expanded.bucketName", "test-bucket",
                        "cloud.storage.expanded.regionName", "us-east-1",
                        "cloud.storage.expanded.objectKeyPrefix", "blocks"));

        plugin.handleVerification(verifiedNotification(1L, testBlock(1).blockUnparsed()));
        plugin.handleVerification(verifiedNotification(2L, testBlock(2).blockUnparsed()));
        awaitNotifications(2);

        final List<PersistedNotification> notifications = blockMessaging.getSentPersistedNotifications();
        assertEquals(2, notifications.size(), "Both blocks must produce a PersistedNotification");
        assertTrue(
                notifications.stream().anyMatch(n -> n.blockNumber() == 1L && n.succeeded()),
                "Block 1 must have succeeded=true");
        assertTrue(
                notifications.stream().anyMatch(n -> n.blockNumber() == 2L && !n.succeeded()),
                "Block 2 must have succeeded=false");
        assertEquals(
                1L,
                getMetricValue(ExpandedCloudStoragePlugin.METRIC_EXPANDED_CLOUD_STORAGE_TOTAL_UPLOADS),
                "uploadsTotal must be 1 — only block 1 succeeded");
        assertEquals(
                1L,
                getMetricValue(ExpandedCloudStoragePlugin.METRIC_EXPANDED_CLOUD_STORAGE_TOTAL_UPLOAD_FAILURES),
                "uploadFailuresTotal must be 1 — only block 2 failed");
    }

    @Test
    @DisplayName("handleVerification is a no-op after stop() has nulled the S3 client")
    void handleVerificationIsNoOpAfterStop() throws InterruptedException {
        final CapturingS3Client capturing = new CapturingS3Client();
        start(
                new ExpandedCloudStoragePlugin(capturing),
                new SimpleInMemoryHistoricalBlockFacility(),
                Map.of(
                        "cloud.storage.expanded.endpointUrl", "http://fake:9000",
                        "cloud.storage.expanded.bucketName", "test-bucket",
                        "cloud.storage.expanded.regionName", "us-east-1"));

        // First notification works normally
        plugin.handleVerification(verifiedNotification(1L, testBlock(1).blockUnparsed()));
        awaitNotifications(1);
        assertEquals(1, capturing.uploads.size(), "One upload expected before stop()");

        // stop() clears s3Client — subsequent handleVerification calls must be no-ops
        plugin.stop();
        plugin.handleVerification(verifiedNotification(2L, testBlock(2).blockUnparsed()));
        Thread.sleep(50);

        assertEquals(1, capturing.uploads.size(), "No further upload must occur after stop()");
        assertEquals(
                1,
                blockMessaging.getSentPersistedNotifications().size(),
                "No new PersistedNotification must be sent after stop()");
    }

    @Test
    @DisplayName("stop() publishes all pending PersistedNotifications before closing the S3 client")
    void stopDrainsNotificationsBeforeClose() throws InterruptedException {
        final CountDownLatch uploadStarted = new CountDownLatch(1);
        final CountDownLatch proceedWithUpload = new CountDownLatch(1);
        final AtomicBoolean closedAfterNotification = new AtomicBoolean(false);

        final S3UploadClient delayedClient = new S3UploadClient() {
            @Override
            public void uploadFile(
                    final String objectKey,
                    final String storageClass,
                    final Iterator<byte[]> contentIterable,
                    final String contentType)
                    throws java.io.IOException {
                uploadStarted.countDown();
                try {
                    proceedWithUpload.await(10, TimeUnit.SECONDS);
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }

            @Override
            public void close() {
                closedAfterNotification.set(
                        !blockMessaging.getSentPersistedNotifications().isEmpty());
            }
        };
        start(
                new ExpandedCloudStoragePlugin(delayedClient),
                new SimpleInMemoryHistoricalBlockFacility(),
                Map.of(
                        "cloud.storage.expanded.endpointUrl", "http://fake:9000",
                        "cloud.storage.expanded.bucketName", "test-bucket",
                        "cloud.storage.expanded.regionName", "us-east-1"));

        plugin.handleVerification(verifiedNotification(1L, testBlock(1).blockUnparsed()));
        assertTrue(uploadStarted.await(5, TimeUnit.SECONDS), "Upload must have started within 5s");

        // Call stop() on a separate thread — it will block in drainInFlightTasks waiting for the upload
        final Thread stopThread = new Thread(() -> plugin.stop());
        stopThread.start();

        // Give stop() time to enter the drain loop, then let the upload complete
        Thread.sleep(100);
        proceedWithUpload.countDown();

        stopThread.join(10_000);

        assertEquals(
                1,
                blockMessaging.getSentPersistedNotifications().size(),
                "PersistedNotification must be published before stop() completes");
        assertTrue(
                closedAfterNotification.get(), "S3 client must be closed only after notifications have been published");
    }
}
