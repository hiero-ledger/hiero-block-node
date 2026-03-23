// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.expanded.cloud.storage;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hedera.bucky.S3ClientException;
import com.hedera.bucky.S3ResponseException;
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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Unit tests for {@link ExpandedCloudStoragePlugin}.
 *
 * <p>All tests here inject a {@link CapturingS3Client} or an anonymous {@link S3UploadClient}
 * subclass via the package-private constructor — no Docker or real S3 endpoint required.
 *
 * @see ExpandedCloudStoragePluginIntegrationTest for MinIO-backed integration tests
 */
@Timeout(value = 30, unit = TimeUnit.SECONDS)
class ExpandedCloudStoragePluginTest
        extends PluginTestBase<ExpandedCloudStoragePlugin, ExecutorService, ScheduledExecutorService> {

    private static final Instant START_TIME =
            ZonedDateTime.of(2025, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant();
    private static final Duration ONE_DAY = Duration.of(1, ChronoUnit.DAYS);

    // ---- Capturing S3 client ------------------------------------------------

    private record UploadCall(String objectKey, String storageClass, String contentType) {}

    /**
     * Records {@code uploadFile} calls so tests can assert the exact arguments passed to the
     * upload client without hitting any real endpoint.
     */
    private static class CapturingS3Client extends S3UploadClient {
        final List<UploadCall> uploads = new java.util.ArrayList<>();

        @Override
        void uploadFile(
                final String objectKey,
                final String storageClass,
                final Iterator<byte[]> contentIterable,
                final String contentType)
                throws S3ClientException, IOException {
            uploads.add(new UploadCall(objectKey, storageClass, contentType));
        }

        @Override
        public void close() {}
    }

    public ExpandedCloudStoragePluginTest() {
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

    private VerificationNotification failedNotification(final long blockNumber) {
        return new VerificationNotification(false, blockNumber, Bytes.EMPTY, null, BlockSource.UNKNOWN);
    }

    /**
     * Drives the plugin's drain loop and polls until at least {@code expectedCount}
     * {@link PersistedNotification}s have been dispatched, or the 5-second timeout elapses.
     * Uses the package-private {@link ExpandedCloudStoragePlugin#drainCompletedTasks()} so
     * tests do not need a second {@code handleVerification} call to flush results.
     */
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
    @DisplayName("Plugin is disabled when endpointUrl is blank — handleVerification is a no-op")
    void pluginDisabledWhenNoEndpoint() throws InterruptedException {
        final CapturingS3Client capturing = new CapturingS3Client();
        start(
                new ExpandedCloudStoragePlugin(capturing),
                new SimpleInMemoryHistoricalBlockFacility(),
                Map.of("expanded.cloud.storage.endpointUrl", ""));

        plugin.handleVerification(verifiedNotification(0L, testBlock(0).blockUnparsed()));
        // Plugin is disabled — handleVerification is a synchronous no-op; no upload task submitted.

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
                Map.of("expanded.cloud.storage.endpointUrl", "http://fake:9000"));

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
                        "expanded.cloud.storage.endpointUrl", "http://fake:9000",
                        "expanded.cloud.storage.objectKeyPrefix", "myblocks",
                        "expanded.cloud.storage.storageClass", "STANDARD"));

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
                        "expanded.cloud.storage.endpointUrl", "http://fake:9000",
                        "expanded.cloud.storage.objectKeyPrefix", "blocks"));

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
                        "expanded.cloud.storage.endpointUrl", "http://fake:9000",
                        "expanded.cloud.storage.objectKeyPrefix", ""));

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
                Map.of("expanded.cloud.storage.endpointUrl", "http://fake:9000"));

        plugin.handleVerification(verifiedNotification(42L, testBlock(42).blockUnparsed()));
        awaitNotifications(1);

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
        final S3UploadClient throwingClient = new S3UploadClient() {
            @Override
            void uploadFile(final String objectKey, final String storageClass,
                    final Iterator<byte[]> contentIterable, final String contentType)
                    throws S3ClientException, IOException {
                throw new S3ResponseException(statusCode, body, null, "S3 returned 503");
            }

            @Override
            public void close() {}
        };
        start(
                new ExpandedCloudStoragePlugin(throwingClient),
                new SimpleInMemoryHistoricalBlockFacility(),
                Map.of("expanded.cloud.storage.endpointUrl", "http://fake:9000"));

        plugin.handleVerification(verifiedNotification(7L, testBlock(7).blockUnparsed()));
        awaitNotifications(1);

        final List<PersistedNotification> notifications = blockMessaging.getSentPersistedNotifications();
        assertEquals(1, notifications.size(), "PersistedNotification must be sent even on S3ResponseException");
        assertEquals(7L, notifications.getFirst().blockNumber());
        assertFalse(notifications.getFirst().succeeded(), "PersistedNotification must report succeeded=false on 503");
    }

    @Test
    @DisplayName("S3ResponseException (HTTP 403 Forbidden) is not rethrown by handleVerification")
    void responseExceptionForbiddenNotRethrown() {
        final S3UploadClient throwingClient = new S3UploadClient() {
            @Override
            void uploadFile(final String objectKey, final String storageClass,
                    final Iterator<byte[]> contentIterable, final String contentType)
                    throws S3ClientException, IOException {
                throw new S3ResponseException(
                        403, "Forbidden".getBytes(java.nio.charset.StandardCharsets.UTF_8), null, "Access denied");
            }

            @Override
            public void close() {}
        };
        start(
                new ExpandedCloudStoragePlugin(throwingClient),
                new SimpleInMemoryHistoricalBlockFacility(),
                Map.of("expanded.cloud.storage.endpointUrl", "http://fake:9000"));

        assertDoesNotThrow(
                () -> plugin.handleVerification(
                        verifiedNotification(0L, testBlock(0).blockUnparsed())),
                "S3ResponseException (403) must never propagate out of handleVerification");
    }

    @Test
    @DisplayName("S3ClientException thrown by uploadFile is not rethrown by handleVerification")
    void s3ExceptionNotRethrown() {
        final S3UploadClient throwingClient = new S3UploadClient() {
            @Override
            void uploadFile(final String objectKey, final String storageClass,
                    final Iterator<byte[]> contentIterable, final String contentType)
                    throws S3ClientException, IOException {
                throw new S3ClientException("Simulated base S3 failure");
            }

            @Override
            public void close() {}
        };
        start(
                new ExpandedCloudStoragePlugin(throwingClient),
                new SimpleInMemoryHistoricalBlockFacility(),
                Map.of("expanded.cloud.storage.endpointUrl", "http://fake:9000"));

        assertDoesNotThrow(
                () -> plugin.handleVerification(
                        verifiedNotification(0L, testBlock(0).blockUnparsed())),
                "S3ClientException must never propagate out of handleVerification");
    }

    @Test
    @DisplayName("S3ResponseException carries HTTP status code and body regardless of status code")
    void responseExceptionDetailsAreNotRethrown() {
        final int[] statusCodes = {400, 403, 404, 409, 500, 503};
        for (final int code : statusCodes) {
            final byte[] responseBody = ("Error " + code).getBytes(java.nio.charset.StandardCharsets.UTF_8);
            final S3UploadClient throwingClient = new S3UploadClient() {
                @Override
                void uploadFile(
                        final String objectKey,
                        final String storageClass,
                        final Iterator<byte[]> contentIterable,
                        final String contentType)
                        throws S3ClientException, IOException {
                    final S3ResponseException ex = new S3ResponseException(code, responseBody, null);
                    assertEquals(code, ex.getResponseStatusCode());
                    assertTrue(ex.getResponseBody().length > 0);
                    throw ex;
                }

                @Override
                public void close() {}
            };
            start(
                    new ExpandedCloudStoragePlugin(throwingClient),
                    new SimpleInMemoryHistoricalBlockFacility(),
                    Map.of("expanded.cloud.storage.endpointUrl", "http://fake:9000"));

            assertDoesNotThrow(
                    () -> plugin.handleVerification(
                            verifiedNotification(0L, testBlock(0).blockUnparsed())),
                    "S3ResponseException (HTTP " + code + ") must not propagate from handleVerification");
        }
    }

    @Test
    @DisplayName("Invalid storageClass is rejected at config construction time")
    void invalidStorageClassRejected() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new ExpandedCloudStorageConfig(
                        "http://fake:9000", "bucket", "blocks", "INVALID_CLASS", "us-east-1", "", "", 60, 4),
                "Invalid storageClass must throw IllegalArgumentException");
    }

    @Test
    @DisplayName("Config rejects uploadTimeoutSeconds < 1 and maxConcurrentUploads < 1")
    void configBoundsValidation() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new ExpandedCloudStorageConfig(
                        "http://fake:9000", "bucket", "blocks", "STANDARD", "us-east-1", "", "", 0, 4),
                "uploadTimeoutSeconds=0 must throw IllegalArgumentException");
        assertThrows(
                IllegalArgumentException.class,
                () -> new ExpandedCloudStorageConfig(
                        "http://fake:9000", "bucket", "blocks", "STANDARD", "us-east-1", "", "", 60, 0),
                "maxConcurrentUploads=0 must throw IllegalArgumentException");
    }

    @Test
    @DisplayName("All valid S3 storage classes are accepted by config")
    void allValidStorageClassesAccepted() {
        for (final String storageClass : ExpandedCloudStorageConfig.VALID_STORAGE_CLASSES) {
            assertDoesNotThrow(
                    () -> new ExpandedCloudStorageConfig(
                            "http://fake:9000", "bucket", "blocks", storageClass, "us-east-1", "", "", 60, 4),
                    "Valid storageClass '" + storageClass + "' must not throw");
        }
    }

    @Test
    @DisplayName("handleVerification skips upload for null block body and negative block number")
    void handleVerificationGuardsSkipUpload() throws InterruptedException {
        final CapturingS3Client capturing = new CapturingS3Client();
        start(
                new ExpandedCloudStoragePlugin(capturing),
                new SimpleInMemoryHistoricalBlockFacility(),
                Map.of("expanded.cloud.storage.endpointUrl", "http://fake:9000"));

        // verified=true but block payload is null
        plugin.handleVerification(new VerificationNotification(true, 1L, Bytes.EMPTY, null, BlockSource.UNKNOWN));
        // verified=true but block number is negative
        plugin.handleVerification(new VerificationNotification(
                true, -1L, Bytes.EMPTY, testBlock(0).blockUnparsed(), BlockSource.UNKNOWN));
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
            void uploadFile(
                    final String objectKey,
                    final String storageClass,
                    final Iterator<byte[]> contentIterable,
                    final String contentType)
                    throws S3ClientException, IOException {
                throw new IOException("Simulated I/O failure");
            }

            @Override
            public void close() {}
        };
        start(
                new ExpandedCloudStoragePlugin(throwingClient),
                new SimpleInMemoryHistoricalBlockFacility(),
                Map.of("expanded.cloud.storage.endpointUrl", "http://fake:9000"));

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
    @DisplayName("Base S3ClientException from uploadFile produces PersistedNotification with succeeded=false")
    void s3ClientExceptionProducesFailedNotification() throws InterruptedException {
        final S3UploadClient throwingClient = new S3UploadClient() {
            @Override
            void uploadFile(
                    final String objectKey,
                    final String storageClass,
                    final Iterator<byte[]> contentIterable,
                    final String contentType)
                    throws S3ClientException, IOException {
                throw new S3ClientException("Simulated base S3 failure");
            }

            @Override
            public void close() {}
        };
        start(
                new ExpandedCloudStoragePlugin(throwingClient),
                new SimpleInMemoryHistoricalBlockFacility(),
                Map.of("expanded.cloud.storage.endpointUrl", "http://fake:9000"));

        plugin.handleVerification(verifiedNotification(9L, testBlock(9).blockUnparsed()));
        awaitNotifications(1);

        final List<PersistedNotification> notifications = blockMessaging.getSentPersistedNotifications();
        assertEquals(1, notifications.size(), "PersistedNotification must be sent even on base S3ClientException");
        assertEquals(9L, notifications.getFirst().blockNumber());
        assertFalse(notifications.getFirst().succeeded(), "PersistedNotification must report succeeded=false");
    }

    @Test
    @DisplayName("stop() closes the injected S3 client")
    void stopClosesS3Client() throws InterruptedException {
        final AtomicBoolean closed = new AtomicBoolean(false);
        final S3UploadClient trackingClient = new S3UploadClient() {
            @Override
            void uploadFile(
                    final String objectKey,
                    final String storageClass,
                    final Iterator<byte[]> contentIterable,
                    final String contentType)
                    throws S3ClientException, IOException {}

            @Override
            public void close() {
                closed.set(true);
            }
        };
        start(
                new ExpandedCloudStoragePlugin(trackingClient),
                new SimpleInMemoryHistoricalBlockFacility(),
                Map.of("expanded.cloud.storage.endpointUrl", "http://fake:9000"));

        plugin.handleVerification(verifiedNotification(1L, testBlock(1).blockUnparsed()));
        awaitNotifications(1);

        plugin.stop();
        assertTrue(closed.get(), "plugin.stop() must call close() on the S3 client");
    }
}
