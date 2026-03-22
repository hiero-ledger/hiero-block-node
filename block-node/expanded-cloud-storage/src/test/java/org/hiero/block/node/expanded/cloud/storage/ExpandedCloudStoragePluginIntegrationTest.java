// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.expanded.cloud.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.adobe.testing.s3mock.testcontainers.S3MockContainer;
import com.hedera.pbj.runtime.io.buffer.Bytes;
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
import org.hiero.block.node.base.s3.S3Client;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.PersistedNotification;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Integration tests for {@link ExpandedCloudStoragePlugin} using a real S3Mock container.
 *
 * <p>The S3Mock container is started once for the whole class via {@link BeforeAll}. If Docker
 * is not available the entire class is skipped via {@link org.junit.jupiter.api.Assumptions},
 * leaving the unit tests in {@link ExpandedCloudStoragePluginTest} unaffected.
 *
 * <p>Test-side S3 verification uses {@link S3Client} from {@code org.hiero.block.node.base}
 * which is already on the module path (expanded-cloud-storage requires base). No additional
 * library dependency is needed.
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS)
class ExpandedCloudStoragePluginIntegrationTest
        extends PluginTestBase<ExpandedCloudStoragePlugin, ExecutorService, ScheduledExecutorService> {

    private static final Instant START_TIME =
            ZonedDateTime.of(2025, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant();
    private static final Duration ONE_DAY = Duration.of(1, ChronoUnit.DAYS);
    private static final String BUCKET_NAME = "test-expanded-blocks";
    private static final String S3MOCK_VERSION = "4.11.0";
    private static final String ACCESS_KEY = "test-access-key";
    private static final String SECRET_KEY = "test-secret-key";
    private static final long DRAIN_TIMEOUT_MS = 5_000L;

    private static S3MockContainer s3MockContainer;
    private static S3Client s3Client;
    private static String s3Endpoint;

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

    @AfterAll
    static void stopS3Mock() {
        if (s3Client != null) s3Client.close();
        if (s3MockContainer != null && s3MockContainer.isRunning()) s3MockContainer.stop();
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
    @DisplayName("Integration: blocks are uploaded using folder-hierarchy keys in S3Mock")
    void integrationUploadSingleBlocks() throws InterruptedException {
        start(
                new ExpandedCloudStoragePlugin(),
                new SimpleInMemoryHistoricalBlockFacility(),
                Map.of(
                        "expanded.cloud.storage.endpointUrl", s3Endpoint,
                        "expanded.cloud.storage.bucketName", BUCKET_NAME,
                        "expanded.cloud.storage.objectKeyPrefix", "blocks",
                        "expanded.cloud.storage.accessKey", ACCESS_KEY,
                        "expanded.cloud.storage.secretKey", SECRET_KEY));

        final List<TestBlock> blocks = TestBlockBuilder.generateBlocksInRange(100L, 104L, START_TIME, ONE_DAY);
        for (final TestBlock block : blocks) {
            plugin.handleVerification(verifiedNotification(block.number(), block.blockUnparsed()));
        }
        plugin.awaitAndDrain(DRAIN_TIMEOUT_MS);

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
                        "expanded.cloud.storage.endpointUrl", s3Endpoint,
                        "expanded.cloud.storage.bucketName", BUCKET_NAME,
                        "expanded.cloud.storage.objectKeyPrefix", "intblocks",
                        "expanded.cloud.storage.accessKey", ACCESS_KEY,
                        "expanded.cloud.storage.secretKey", SECRET_KEY));

        final TestBlock block = testBlock(200L);
        plugin.handleVerification(verifiedNotification(200L, block.blockUnparsed()));
        plugin.awaitAndDrain(DRAIN_TIMEOUT_MS);

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
                        "expanded.cloud.storage.endpointUrl", s3Endpoint,
                        "expanded.cloud.storage.bucketName", BUCKET_NAME,
                        "expanded.cloud.storage.objectKeyPrefix", "",
                        "expanded.cloud.storage.accessKey", ACCESS_KEY,
                        "expanded.cloud.storage.secretKey", SECRET_KEY));

        final TestBlock block = testBlock(300L);
        plugin.handleVerification(verifiedNotification(300L, block.blockUnparsed()));
        plugin.awaitAndDrain(DRAIN_TIMEOUT_MS);

        final String padded = String.format("%019d", 300L);
        final String expectedKey = padded.substring(0, 4) + "/" + padded.substring(4, 8) + "/"
                + padded.substring(8, 12) + "/" + padded.substring(12, 16) + "/"
                + padded.substring(16) + ".blk.zstd";
        assertTrue(
                listAllObjects("").contains(expectedKey),
                "Expected bare-hierarchy key (no prefix) not found in S3Mock: " + expectedKey);
    }

    // ---- Private helpers ----------------------------------------------------

    private Set<String> listAllObjects(final String prefix) {
        try {
            return Set.copyOf(s3Client.listObjects(prefix.isEmpty() ? "" : prefix, 1000));
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }
}
