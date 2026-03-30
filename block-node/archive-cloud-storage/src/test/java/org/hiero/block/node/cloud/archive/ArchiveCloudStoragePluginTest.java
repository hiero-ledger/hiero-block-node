// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.cloud.archive;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import com.hedera.pbj.runtime.OneOf;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.config.api.Configuration;
import com.swirlds.config.api.ConfigurationBuilder;
import com.swirlds.metrics.api.Metrics;
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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import org.hiero.block.api.BlockNodeVersions;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.app.fixtures.async.BlockingExecutor;
import org.hiero.block.node.app.fixtures.async.ScheduledBlockingExecutor;
import org.hiero.block.node.app.fixtures.blocks.TestBlock;
import org.hiero.block.node.app.fixtures.blocks.TestBlockBuilder;
import org.hiero.block.node.app.fixtures.plugintest.NoOpServiceBuilder;
import org.hiero.block.node.app.fixtures.plugintest.PluginTestBase;
import org.hiero.block.node.app.fixtures.plugintest.SimpleInMemoryHistoricalBlockFacility;
import org.hiero.block.node.app.fixtures.plugintest.TestBlockMessagingFacility;
import org.hiero.block.node.app.fixtures.plugintest.TestHealthFacility;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.PersistedNotification;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import org.hiero.block.node.spi.historicalblocks.HistoricalBlockFacility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;

/// Unit tests for [ArchiveCloudStoragePlugin].
@DisplayName("ArchiveCloudStoragePlugin Tests")
class ArchiveCloudStoragePluginTest {

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

    /// The MinIO client connected to [MINIO_CONTAINER].
    private static MinioClient minioClient;

    /// The HTTP endpoint of [MINIO_CONTAINER].
    private static String minioEndpoint;

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

    /// Returns the config map used to initialise the plugin under test with a 10 MB part size.
    private Map<String, String> pluginConfig() {
        return pluginConfig(PluginTests.GROUPING_LEVEL, 10);
    }

    /// Returns the config map used to initialise the plugin under test.
    private Map<String, String> pluginConfig(int groupingLevel, int partSizeMb) {
        return Map.of(
                "cloud-archive.groupingLevel", String.valueOf(groupingLevel),
                "cloud-archive.partSizeMb", String.valueOf(partSizeMb),
                "cloud-archive.endpointUrl", minioEndpoint,
                "cloud-archive.bucketName", BUCKET_NAME,
                "cloud-archive.accessKey", MINIO_USER,
                "cloud-archive.secretKey", MINIO_PASSWORD);
    }

    /// Constructor and init tests that do not require a running plugin.
    @Nested
    @DisplayName("Constructor & Init Tests")
    final class ConstructorAndInitTests {

        /// Verifies that the no-args constructor does not throw.
        @Test
        @DisplayName("No-args constructor does not throw")
        void testNoArgsConstructor() {
            assertThatNoException().isThrownBy(ArchiveCloudStoragePlugin::new);
        }

        /// Verifies that [ArchiveCloudStoragePlugin#init] throws [NullPointerException] when context is null.
        @Test
        @DisplayName("init throws NullPointerException when context is null")
        void testInitNullContext() {
            final ArchiveCloudStoragePlugin plugin = new ArchiveCloudStoragePlugin();
            assertThatNullPointerException().isThrownBy(() -> plugin.init(null, new NoOpServiceBuilder()));
        }

        /// Verifies that [ArchiveCloudStoragePlugin#init] does not throw when [ServiceBuilder] is null.
        @Test
        @DisplayName("init does not throw when ServiceBuilder is null")
        void testInitNullServiceBuilder() {
            final Configuration configuration = ConfigurationBuilder.create()
                    .withConfigDataType(ArchiveCloudStorageConfig.class)
                    .withValue("cloud-archive.endpointUrl", minioEndpoint)
                    .build();
            final Metrics metricsMock = mock(Metrics.class);
            final HistoricalBlockFacility historicalBlockFacility = new SimpleInMemoryHistoricalBlockFacility();
            final BlockNodeContext testContext = new BlockNodeContext(
                    configuration,
                    metricsMock,
                    new TestHealthFacility(),
                    new TestBlockMessagingFacility(),
                    historicalBlockFacility,
                    null,
                    null,
                    BlockNodeVersions.DEFAULT);
            final ArchiveCloudStoragePlugin plugin = new ArchiveCloudStoragePlugin();
            assertThatNoException().isThrownBy(() -> plugin.init(testContext, null));
        }
    }

    /// Integration tests that drive the plugin via [PluginTestBase].
    @Nested
    @DisplayName("Plugin Tests")
    final class PluginTests
            extends PluginTestBase<ArchiveCloudStoragePlugin, BlockingExecutor, ScheduledBlockingExecutor> {

        private static final int GROUPING_LEVEL = 1; // groupSize = 10^1 = 10 blocks per tar
        private final BlockingExecutor pluginExecutor;

        PluginTests() {
            super(
                    new BlockingExecutor(new LinkedBlockingQueue<>()),
                    new ScheduledBlockingExecutor(new LinkedBlockingQueue<>()));
            start(new ArchiveCloudStoragePlugin(), new SimpleInMemoryHistoricalBlockFacility(), pluginConfig());
            pluginExecutor = testThreadPoolManager.executor();
            // the plugin implements BlockNotificationHandler but does not self-register, so wire it up here
            blockMessaging.registerBlockNotificationHandler(plugin, false, "ArchiveCloudStoragePlugin");
        }

        @Test
        @DisplayName("Plugin should upload a tar file for a single batch of blocks and publish persisted notifications")
        void testSingleBatch() throws Exception {
            final int groupSize = (int) Math.pow(10, GROUPING_LEVEL); // 10
            final List<TestBlock> blocks = TestBlockBuilder.generateBlocksInRange(0, groupSize - 1);
            // send verification notifications — the last one returns FINISHED and sets liveBlockArchiveTask to null
            sendVerifications(blocks);
            // await the virtual-thread upload to complete
            pluginExecutor.executeSerially();
            // the tar key for startBlock=0, groupingLevel=1: "0000/0000/0000/0000/0.tar"
            assertTrue(getAllObjects().contains("0000/0000/0000/0000/0.tar"), "tar file should be uploaded to S3");
            // every block should have received a successful persisted notification
            final List<PersistedNotification> notifications = blockMessaging.getSentPersistedNotifications();
            assertEquals(groupSize, notifications.size(), "expected one persisted notification per block");
            notifications.forEach(n -> assertTrue(n.succeeded(), "all persisted notifications should be successful"));
        }

        @Test
        @DisplayName("Plugin should upload two tar files for two consecutive batches of blocks")
        void testTwoBatches() throws Exception {
            final int groupSize = (int) Math.pow(10, GROUPING_LEVEL); // 10
            // send first batch (0–9), wait for upload, then send second batch (10–19)
            sendVerifications(TestBlockBuilder.generateBlocksInRange(0, groupSize - 1));
            pluginExecutor.executeSerially();
            sendVerifications(TestBlockBuilder.generateBlocksInRange(groupSize, groupSize * 2 - 1));
            pluginExecutor.executeSerially();

            final Set<String> objects = getAllObjects();
            assertTrue(objects.contains("0000/0000/0000/0000/0.tar"), "first tar should be uploaded");
            assertTrue(objects.contains("0000/0000/0000/0000/1.tar"), "second tar should be uploaded");
            final List<PersistedNotification> notifications = blockMessaging.getSentPersistedNotifications();
            assertEquals(groupSize * 2, notifications.size(), "expected one persisted notification per block");
            notifications.forEach(n -> assertTrue(n.succeeded(), "all persisted notifications should be successful"));
        }

        @Test
        @DisplayName("Plugin should handle blocks arriving out of order within a batch")
        void testOutOfOrderWithinBatch() throws Exception {
            final int groupSize = (int) Math.pow(10, GROUPING_LEVEL); // 10
            final List<TestBlock> blocks = new ArrayList<>(TestBlockBuilder.generateBlocksInRange(0, groupSize - 1));
            Collections.shuffle(blocks);
            sendVerifications(blocks);
            pluginExecutor.executeSerially();

            assertTrue(getAllObjects().contains("0000/0000/0000/0000/0.tar"), "tar file should be uploaded");
            final List<PersistedNotification> notifications = blockMessaging.getSentPersistedNotifications();
            assertEquals(groupSize, notifications.size(), "expected one persisted notification per block");
            notifications.forEach(n -> assertTrue(n.succeeded(), "all persisted notifications should be successful"));
        }

        @Test
        @DisplayName("Block from next group is stashed and replayed when that group's task starts")
        void testNextGroupBlockStashedAndReplayed() throws Exception {
            final int groupSize = (int) Math.pow(10, GROUPING_LEVEL); // 10
            // send one block from group 0–9 to create the first task
            sendVerification(TestBlockBuilder.generateBlocksInRange(5, 5).getFirst());
            // send a block from group 10–19 — it should be stashed (BLOCK_OUT_OF_RANGE for task 0–9)
            sendVerification(
                    TestBlockBuilder.generateBlocksInRange(groupSize, groupSize).getFirst());
            assertThat(plugin.blocksStash).hasSize(1);
            // complete group 0–9 (all except block 5 which was already sent)
            sendVerifications(TestBlockBuilder.generateBlocksInRange(0, 4));
            sendVerifications(TestBlockBuilder.generateBlocksInRange(6, groupSize - 1));
            pluginExecutor.executeSerially();
            assertTrue(getAllObjects().contains("0000/0000/0000/0000/0.tar"), "first tar should be uploaded");
            assertThat(plugin.blocksStash).hasSize(1);

            // send block 11 — creates task for 10–19 and replays block 10 from stash
            sendVerification(TestBlockBuilder.generateBlocksInRange(groupSize + 1, groupSize + 1)
                    .getFirst());
            assertThat(plugin.blocksStash).isEmpty();
            // complete group 10–19 (blocks 10 and 11 already submitted)
            sendVerifications(TestBlockBuilder.generateBlocksInRange(groupSize + 2, groupSize * 2 - 1));
            pluginExecutor.executeSerially();
            assertTrue(getAllObjects().contains("0000/0000/0000/0000/1.tar"), "second tar should be uploaded");

            final List<PersistedNotification> notifications = blockMessaging.getSentPersistedNotifications();
            assertEquals(groupSize * 2, notifications.size(), "expected one persisted notification per block");
            notifications.forEach(n -> assertTrue(n.succeeded(), "all persisted notifications should be successful"));
        }

        @Test
        @DisplayName("Plugin aborts the active upload mid-batch when stop() is called, leaving no tar in S3")
        void testAbortMidBatch() throws Exception {
            // Send only block 0 — this creates a task for group 0-9 and enqueues the run() task.
            sendVerification(TestBlockBuilder.generateBlocksInRange(0, 0).getFirst());
            assertThat(plugin.liveBlockArchiveTask).isNotNull();

            // Stopping the plugin aborts the active task: sets aborted=true, cancels pending futures,
            // and calls abortMultipartUpload on the S3 client.
            plugin.stop();

            // Run the enqueued task — aborted flag causes the run() loop to exit immediately.
            pluginExecutor.executeSerially();

            assertThat(getAllObjects()).doesNotContain("0000/0000/0000/0000/0.tar");
        }

        /// Sends a [VerificationNotification] for each block via the messaging facility.
        private void sendVerifications(List<TestBlock> blocks) {
            for (final TestBlock block : blocks) {
                sendVerification(block);
            }
        }

        /// Sends a single [VerificationNotification] via the messaging facility.
        private void sendVerification(TestBlock block) {
            blockMessaging.sendBlockVerification(new VerificationNotification(
                    true, block.number(), Bytes.EMPTY, block.blockUnparsed(), BlockSource.PUBLISHER));
        }
    }

    /// Integration tests that verify the multipart upload code path is exercised
    /// when individual blocks are large enough for the buffer to exceed [ArchiveCloudStorageConfig#partSizeMb()].
    @Nested
    @DisplayName("Large Block Plugin Tests")
    final class LargeBlockPluginTests
            extends PluginTestBase<ArchiveCloudStoragePlugin, BlockingExecutor, ScheduledBlockingExecutor> {

        /// 5 MB — the minimum non-final part size that S3/MinIO accepts.
        private static final int PART_SIZE_MB = 5;
        /// 100 blocks per tar (groupingLevel = 2 → 10^2).
        private static final int GROUPING_LEVEL = 2;
        /// Each block carries ~600 KB of pseudo-random (incompressible) bytes.
        private static final int BLOCK_DATA_BYTES = 600 * 1024;

        private final BlockingExecutor pluginExecutor;

        LargeBlockPluginTests() {
            super(
                    new BlockingExecutor(new LinkedBlockingQueue<>()),
                    new ScheduledBlockingExecutor(new LinkedBlockingQueue<>()));
            start(
                    new ArchiveCloudStoragePlugin(),
                    new SimpleInMemoryHistoricalBlockFacility(),
                    pluginConfig(GROUPING_LEVEL, PART_SIZE_MB));
            pluginExecutor = testThreadPoolManager.executor();
            blockMessaging.registerBlockNotificationHandler(plugin, false, "ArchiveCloudStoragePlugin");
        }

        /// Verifies that when blocks are large enough to exceed [ArchiveCloudStorageConfig#partSizeMb()],
        /// the upload is split into multiple parts and the final tar is still committed correctly.
        ///
        /// With [PART_SIZE_MB] = 5 MB and [BLOCK_DATA_BYTES] = 600 KB per block, the buffer
        /// exceeds the threshold after ~9 blocks out of 100, so [LiveBlockArchiveTask] calls
        /// `uploadBlockChunk` multiple times before `completeUpload` flushes the remainder.
        @Test
        @DisplayName("Multipart upload splits data into multiple parts for large blocks")
        void testMultipartUploadWithLargeBlocks() throws Exception {
            final int groupSize = (int) Math.pow(10, GROUPING_LEVEL); // 100

            final Random rng = new Random(0xDEADBEEFL);
            for (int i = 0; i < groupSize; i++) {
                final byte[] data = new byte[BLOCK_DATA_BYTES];
                // We generate random bytes so that the compression outputs _almost_ the same number of bytes back
                rng.nextBytes(data);
                final BlockItemUnparsed item = new BlockItemUnparsed(
                        new OneOf<>(BlockItemUnparsed.ItemOneOfType.SIGNED_TRANSACTION, Bytes.wrap(data)));
                final BlockUnparsed block = BlockUnparsed.newBuilder()
                        .blockItems(new BlockItemUnparsed[] {item})
                        .build();
                blockMessaging.sendBlockVerification(
                        new VerificationNotification(true, i, Bytes.EMPTY, block, BlockSource.PUBLISHER));
            }

            pluginExecutor.executeSerially();

            assertTrue(
                    getAllObjects().contains("0000/0000/0000/0000/0.tar"), "tar file should be uploaded via multipart");
            final List<PersistedNotification> notifications = blockMessaging.getSentPersistedNotifications();
            assertEquals(groupSize, notifications.size(), "expected one persisted notification per block");
            notifications.forEach(n -> assertTrue(n.succeeded(), "all persisted notifications should be successful"));
        }

        /// Verifies the exact-fit path in [LiveBlockArchiveTask]: when the accumulated buffer
        /// is pre-filled to [partSizeBytes - entrySize] and then one block whose tar entry is
        /// exactly [entrySize] bytes is processed, `uploadBlockChunk` returns an empty remainder
        /// and the persisted notification for that block is sent immediately — not deferred to
        /// `completeUpload`.
        @Test
        @DisplayName("uploadBlockChunk returns empty remainder when accumulated buffer exactly equals partSizeBytes")
        void testExactFitPart() throws Exception {
            final int groupSize = (int) Math.pow(10, GROUPING_LEVEL); // 100
            final int partSizeBytes = PART_SIZE_MB * 1024 * 1024;

            // A small, incompressible block — exact payload size is irrelevant because we will
            // adjust the pre-filled buffer to compensate for whatever the tar entry turns out to be.
            final byte[] data = new byte[512];
            new Random(0xCAFEBABEL).nextBytes(data);
            final BlockItemUnparsed item = new BlockItemUnparsed(
                    new OneOf<>(BlockItemUnparsed.ItemOneOfType.SIGNED_TRANSACTION, Bytes.wrap(data)));
            final BlockUnparsed block = BlockUnparsed.newBuilder()
                    .blockItems(new BlockItemUnparsed[] {item})
                    .build();

            // Measure how many bytes this block's tar entry occupies.
            final int entrySize = BlockToTarEntry.toTarEntry(block, 0).length;

            // Send block 0: creates liveBlockArchiveTask and enqueues run() on the executor.
            // The executor hasn't been ticked yet, so run() hasn't started and we can still
            // mutate the task's buffer.
            blockMessaging.sendBlockVerification(
                    new VerificationNotification(true, 0, Bytes.EMPTY, block, BlockSource.PUBLISHER));

            // Pre-fill the buffer so that appending block 0's tar entry brings it to exactly
            // partSizeBytes, exercising the zero-remainder branch of uploadBlockChunk.
            ((LiveBlockArchiveTask) plugin.liveBlockArchiveTask).taredBlocksBuffer =
                    new byte[partSizeBytes - entrySize];

            // Send the remaining blocks in the group.
            for (int i = 1; i < groupSize; i++) {
                blockMessaging.sendBlockVerification(
                        new VerificationNotification(true, i, Bytes.EMPTY, block, BlockSource.PUBLISHER));
            }

            pluginExecutor.executeSerially();

            assertTrue(
                    getAllObjects().contains("0000/0000/0000/0000/0.tar"),
                    "tar file should be uploaded via multipart with exact-fit part size");
            final List<PersistedNotification> notifications = blockMessaging.getSentPersistedNotifications();
            assertEquals(groupSize, notifications.size(), "expected one persisted notification per block");
            notifications.forEach(n -> assertTrue(n.succeeded(), "all persisted notifications should be successful"));
        }
    }
}
