// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.cloud.storage.archive;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import com.hedera.bucky.S3Client;
import com.hedera.pbj.runtime.OneOf;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.config.api.Configuration;
import com.swirlds.config.api.ConfigurationBuilder;
import io.minio.BucketExistsArgs;
import io.minio.ListObjectsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.RemoveObjectArgs;
import io.minio.Result;
import io.minio.errors.MinioException;
import io.minio.messages.Item;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Stream;
import org.hiero.block.api.BlockNodeVersions;
import org.hiero.block.api.TssData;
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
import org.hiero.metrics.core.MetricRegistry;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.containers.GenericContainer;

/// Unit tests for [CloudStorageArchivePlugin].
@DisplayName("CloudStorageArchivePlugin Tests")
class CloudStorageArchivePluginTest {

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
                "cloud.archive.groupingLevel", String.valueOf(groupingLevel),
                "cloud.archive.partSizeMb", String.valueOf(partSizeMb),
                "cloud.archive.endpointUrl", minioEndpoint,
                "cloud.archive.regionName", "us-east-1",
                "cloud.archive.bucketName", BUCKET_NAME,
                "cloud.archive.accessKey", MINIO_USER,
                "cloud.archive.secretKey", MINIO_PASSWORD);
    }

    /// Constructor and init tests that do not require a running plugin.
    @Nested
    @DisplayName("Constructor & Init Tests")
    final class ConstructorAndInitTests {

        /// Verifies that the no-args constructor does not throw.
        @Test
        @DisplayName("No-args constructor does not throw")
        void testNoArgsConstructor() {
            assertThatNoException().isThrownBy(CloudStorageArchivePlugin::new);
        }

        /// Verifies that [CloudStorageArchivePlugin#init] throws [NullPointerException] when context is null.
        @Test
        @DisplayName("init throws NullPointerException when context is null")
        void testInitNullContext() {
            final CloudStorageArchivePlugin plugin = new CloudStorageArchivePlugin();
            assertThatNullPointerException().isThrownBy(() -> plugin.init(null, new NoOpServiceBuilder()));
        }

        /// Verifies that [CloudStorageArchivePlugin#init] does not throw when [ServiceBuilder] is null.
        @Test
        @DisplayName("init does not throw when ServiceBuilder is null")
        void testInitNullServiceBuilder() {
            final Configuration configuration = ConfigurationBuilder.create()
                    .withConfigDataType(CloudStorageArchiveConfig.class)
                    .withValue("cloud.archive.endpointUrl", minioEndpoint)
                    .build();
            final MetricRegistry metricsMock = mock(MetricRegistry.class);
            final HistoricalBlockFacility historicalBlockFacility = new SimpleInMemoryHistoricalBlockFacility();
            final BlockNodeContext testContext = new BlockNodeContext(
                    configuration,
                    metricsMock,
                    new TestHealthFacility(),
                    new TestBlockMessagingFacility(),
                    historicalBlockFacility,
                    null,
                    null,
                    null,
                    BlockNodeVersions.DEFAULT,
                    TssData.DEFAULT);
            final CloudStorageArchivePlugin plugin = new CloudStorageArchivePlugin();
            assertThatNoException().isThrownBy(() -> plugin.init(testContext, null));
        }

        /// Verifies that the plugin does NOT register as a block notification handler when a
        /// required configuration field is blank or empty.  Each case omits exactly one field, or
        /// sets it to whitespace only, so that [CloudStorageArchiveConfig#validate] returns a
        /// non-empty violation list.
        @ParameterizedTest(name = "plugin not registered when {0} is blank or empty")
        @MethodSource("blankOrEmptyFieldConfigs")
        @DisplayName("Plugin not registered when a required config field is blank or empty")
        void testPluginNotRegisteredForMissingField(String fieldName, Map<String, String> configValues) {
            final ConfigurationBuilder builder =
                    ConfigurationBuilder.create().withConfigDataType(CloudStorageArchiveConfig.class);
            configValues.forEach(builder::withValue);
            final TestBlockMessagingFacility messaging = new TestBlockMessagingFacility();
            final BlockNodeContext testContext = new BlockNodeContext(
                    builder.build(),
                    mock(MetricRegistry.class),
                    new TestHealthFacility(),
                    messaging,
                    new SimpleInMemoryHistoricalBlockFacility(),
                    null,
                    null,
                    null,
                    BlockNodeVersions.DEFAULT,
                    TssData.DEFAULT);
            new CloudStorageArchivePlugin().init(testContext, null);
            assertThat(messaging.getBlockNotificationHandlerCount()).isZero();
        }

        static Stream<Arguments> blankOrEmptyFieldConfigs() {
            final Map<String, String> full = new HashMap<>(Map.of(
                    "cloud.archive.endpointUrl", "http://localhost:9000",
                    "cloud.archive.regionName", "us-east-1",
                    "cloud.archive.accessKey", "minioadmin",
                    "cloud.archive.secretKey", "minioadmin",
                    "cloud.archive.bucketName", "test-bucket"));
            return Stream.of(
                    Arguments.of("endpointUrl (empty)", withoutKey(full, "cloud.archive.endpointUrl")),
                    Arguments.of("regionName (empty)", withoutKey(full, "cloud.archive.regionName")),
                    Arguments.of("accessKey (empty)", withoutKey(full, "cloud.archive.accessKey")),
                    Arguments.of("secretKey (empty)", withoutKey(full, "cloud.archive.secretKey")),
                    Arguments.of("bucketName (empty)", withoutKey(full, "cloud.archive.bucketName")),
                    Arguments.of("endpointUrl (blank)", withValue(full, "cloud.archive.endpointUrl", "   ")),
                    Arguments.of("regionName (blank)", withValue(full, "cloud.archive.regionName", "   ")),
                    Arguments.of("accessKey (blank)", withValue(full, "cloud.archive.accessKey", "   ")),
                    Arguments.of("secretKey (blank)", withValue(full, "cloud.archive.secretKey", "   ")),
                    Arguments.of("bucketName (blank)", withValue(full, "cloud.archive.bucketName", "   ")));
        }

        private static Map<String, String> withoutKey(Map<String, String> source, String key) {
            final Map<String, String> copy = new HashMap<>(source);
            copy.remove(key);
            return copy;
        }

        private static Map<String, String> withValue(Map<String, String> source, String key, String value) {
            final Map<String, String> copy = new HashMap<>(source);
            copy.put(key, value);
            return copy;
        }

        /// Verifies that the plugin DOES register as a block notification handler when all
        /// required configuration fields are present.
        @Test
        @DisplayName("Plugin registers as notification handler when all required config fields are present")
        void testPluginRegisteredWhenAllConfigFieldsPresent() {
            final Configuration configuration = ConfigurationBuilder.create()
                    .withConfigDataType(CloudStorageArchiveConfig.class)
                    .withValue("cloud.archive.endpointUrl", "http://localhost:9000")
                    .withValue("cloud.archive.regionName", "us-east-1")
                    .withValue("cloud.archive.accessKey", "minioadmin")
                    .withValue("cloud.archive.secretKey", "minioadmin")
                    .withValue("cloud.archive.bucketName", "test-bucket")
                    .build();
            final TestBlockMessagingFacility messaging = new TestBlockMessagingFacility();
            final BlockNodeContext testContext = new BlockNodeContext(
                    configuration,
                    mock(MetricRegistry.class),
                    new TestHealthFacility(),
                    messaging,
                    new SimpleInMemoryHistoricalBlockFacility(),
                    null,
                    null,
                    null,
                    BlockNodeVersions.DEFAULT,
                    TssData.DEFAULT);
            new CloudStorageArchivePlugin().init(testContext, null);
            assertThat(messaging.getBlockNotificationHandlerCount()).isOne();
        }
    }

    /// Integration tests that drive the plugin via [PluginTestBase].
    @Nested
    @DisplayName("Plugin Tests")
    final class PluginTests
            extends PluginTestBase<CloudStorageArchivePlugin, BlockingExecutor, ScheduledBlockingExecutor> {

        private static final int GROUPING_LEVEL = 1; // groupSize = 10^1 = 10 blocks per tar
        private final BlockingExecutor pluginExecutor;

        PluginTests() {
            super(
                    new BlockingExecutor(new LinkedBlockingQueue<>()),
                    new ScheduledBlockingExecutor(new LinkedBlockingQueue<>()));
            start(new CloudStorageArchivePlugin(), new SimpleInMemoryHistoricalBlockFacility(), pluginConfig());
            pluginExecutor = testThreadPoolManager.executor();
        }

        /// Drains the startup recovery task after [clearBucket] has cleaned S3.
        ///
        /// The constructor runs before `@BeforeEach`, so recovery must be deferred here to avoid
        /// seeing stale S3 objects left by a previous test.
        @BeforeEach
        void drainRecovery() {
            pluginExecutor.executeSerially();
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
            // only the last block receives a successful persisted notification
            final List<PersistedNotification> notifications = blockMessaging.getSentPersistedNotifications();
            assertEquals(1, notifications.size(), "expected one persisted notification for the last block");
            assertEquals(
                    groupSize - 1L,
                    notifications.getFirst().blockNumber(),
                    "notification should be for the last block");
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
            assertEquals(2, notifications.size(), "expected one persisted notification per batch");
            assertEquals(
                    groupSize - 1L,
                    notifications.get(0).blockNumber(),
                    "first notification should be for the last block of batch 0");
            assertEquals(
                    groupSize * 2 - 1L,
                    notifications.get(1).blockNumber(),
                    "second notification should be for the last block of batch 1");
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
            assertEquals(1, notifications.size(), "expected one persisted notification for the last block");
            assertEquals(
                    groupSize - 1L,
                    notifications.getFirst().blockNumber(),
                    "notification should be for the last block");
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
            assertEquals(2, notifications.size(), "expected one persisted notification per batch");
            assertEquals(
                    groupSize - 1L,
                    notifications.get(0).blockNumber(),
                    "first notification should be for the last block of batch 0");
            assertEquals(
                    groupSize * 2 - 1L,
                    notifications.get(1).blockNumber(),
                    "second notification should be for the last block of batch 1");
            notifications.forEach(n -> assertTrue(n.succeeded(), "all persisted notifications should be successful"));
        }

        /// Verifies that a null [VerificationNotification] is silently ignored: no task is created
        /// and no exception is thrown.
        @Test
        @DisplayName("Null verification notification is silently ignored")
        void testNullVerificationIgnored() {
            assertThatNoException().isThrownBy(() -> plugin.handleVerification(null));
            assertThat(plugin.currentUploadFuture).isNull();
        }

        /// Verifies that a failed verification notification (`success = false`) is silently ignored:
        /// no task is created and no exception is thrown.
        @Test
        @DisplayName("Failed verification notification is silently ignored")
        void testFailedVerificationIgnored() {
            final TestBlock block = TestBlockBuilder.generateBlocksInRange(0, 0).getFirst();
            assertThatNoException()
                    .isThrownBy(() -> plugin.handleVerification(new VerificationNotification(
                            false,
                            VerificationNotification.FailureType.BAD_BLOCK_PROOF,
                            block.number(),
                            Bytes.EMPTY,
                            block.blockUnparsed(),
                            BlockSource.PUBLISHER)));
            assertThat(plugin.currentUploadFuture).isNull();
        }

        /// Verifies the draining behaviour when block 0 arrives last: blocks 1–9 accumulate in
        /// [CloudStorageArchivePlugin#currentGroupPending] and only flush to the queue once block 0
        /// is received.
        @Test
        @DisplayName("Blocks 1–9 are held in pending and drained when the missing block 0 arrives last")
        void testPendingDrainedWhenFirstBlockArrivesLast() throws Exception {
            final int groupSize = (int) Math.pow(10, GROUPING_LEVEL); // 10
            // Send blocks 1-9 first — they all go into currentGroupPending, queue stays empty.
            sendVerifications(TestBlockBuilder.generateBlocksInRange(1, groupSize - 1));
            assertThat(plugin.currentBlockQueue).isEmpty();
            assertThat(plugin.currentGroupPending).hasSize(groupSize - 1);

            // Block 0 arrives last — triggers a full drain of all 10 blocks into the queue.
            sendVerification(TestBlockBuilder.generateBlocksInRange(0, 0).getFirst());
            assertThat(plugin.currentGroupPending).isEmpty();

            pluginExecutor.executeSerially();
            assertTrue(getAllObjects().contains("0000/0000/0000/0000/0.tar"), "tar file should be uploaded");
            final List<PersistedNotification> notifications = blockMessaging.getSentPersistedNotifications();
            assertEquals(1, notifications.size(), "expected one persisted notification for the last block");
            assertEquals(
                    groupSize - 1L,
                    notifications.getFirst().blockNumber(),
                    "notification should be for the last block");
            notifications.forEach(n -> assertTrue(n.succeeded(), "all persisted notifications should be successful"));
        }

        /// Verifies that a block two groups ahead (e.g. block 20 while the current task covers
        /// 0–9) is stashed, remains stashed while the intermediate group (10–19) is processed,
        /// and is finally replayed when the third group's task starts.
        @Test
        @DisplayName("Block two groups ahead is stashed and replayed for the correct group")
        void testBlockTwoGroupsAheadStashedAndReplayed() throws Exception {
            final int groupSize = (int) Math.pow(10, GROUPING_LEVEL); // 10
            // Trigger group 0-9 task with block 0
            sendVerification(TestBlockBuilder.generateBlocksInRange(0, 0).getFirst());
            // Block 20 is two groups ahead — should land in blocksStash
            sendVerification(TestBlockBuilder.generateBlocksInRange(groupSize * 2, groupSize * 2)
                    .getFirst());
            assertThat(plugin.blocksStash).containsKey((long) groupSize * 2);

            // Complete group 0-9
            sendVerifications(TestBlockBuilder.generateBlocksInRange(1, groupSize - 1));
            pluginExecutor.executeSerially();
            assertTrue(getAllObjects().contains("0000/0000/0000/0000/0.tar"), "first tar should be uploaded");
            // Block 20 must still be stashed — it does not belong to group 10-19
            assertThat(plugin.blocksStash).containsKey((long) groupSize * 2);

            // Complete group 10-19
            sendVerifications(TestBlockBuilder.generateBlocksInRange(groupSize, groupSize * 2 - 1));
            pluginExecutor.executeSerially();
            assertTrue(getAllObjects().contains("0000/0000/0000/0000/1.tar"), "second tar should be uploaded");
            // Block 20 still stashed — the trigger for group 20-29 hasn't arrived yet
            assertThat(plugin.blocksStash).containsKey((long) groupSize * 2);

            // Complete group 20-29: block 20 is replayed from stash, rest arrive normally
            sendVerifications(TestBlockBuilder.generateBlocksInRange(groupSize * 2 + 1, groupSize * 3 - 1));
            pluginExecutor.executeSerially();
            assertTrue(getAllObjects().contains("0000/0000/0000/0000/2.tar"), "third tar should be uploaded");
            assertThat(plugin.blocksStash).isEmpty();

            final List<PersistedNotification> notifications = blockMessaging.getSentPersistedNotifications();
            assertEquals(3, notifications.size(), "expected one persisted notification per batch");
            assertEquals(
                    groupSize - 1L,
                    notifications.get(0).blockNumber(),
                    "first notification should be for the last block of batch 0");
            assertEquals(
                    groupSize * 2 - 1L,
                    notifications.get(1).blockNumber(),
                    "second notification should be for the last block of batch 1");
            assertEquals(
                    groupSize * 3 - 1L,
                    notifications.get(2).blockNumber(),
                    "third notification should be for the last block of batch 2");
            notifications.forEach(n -> assertTrue(n.succeeded(), "all persisted notifications should be successful"));
        }

        @Test
        @DisplayName("Plugin aborts the active upload mid-batch when stop() is called, leaving no tar in S3")
        void testAbortMidBatch() throws Exception {
            // Send only block 0 — this creates a task for group 0-9 and submits a Future.
            sendVerification(TestBlockBuilder.generateBlocksInRange(0, 0).getFirst());
            assertThat(plugin.currentUploadFuture).isNotNull();

            // Cancelling the future interrupts the virtual thread; blockQueue.take() throws
            // InterruptedException, which triggers abortMultipartUpload inside BlockUploadTask.
            plugin.stop();

            // Drive the task wrapper on the blocking executor. The future was cancelled before the
            // task started running, so FutureTask.get() inside the BlockingExecutor wrapper throws
            // CancellationException — that is expected and can be ignored here.
            try {
                pluginExecutor.executeSerially();
            } catch (java.util.concurrent.CancellationException ignored) {
            }

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
                    true, null, block.number(), Bytes.EMPTY, block.blockUnparsed(), BlockSource.PUBLISHER));
        }
    }

    /// Integration tests that verify the multipart upload code path is exercised
    /// when individual blocks are large enough for the buffer to exceed [CloudStorageArchiveConfig#partSizeMb()].
    @Nested
    @DisplayName("Large Block Plugin Tests")
    final class LargeBlockPluginTests
            extends PluginTestBase<CloudStorageArchivePlugin, BlockingExecutor, ScheduledBlockingExecutor> {

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
                    new CloudStorageArchivePlugin(),
                    new SimpleInMemoryHistoricalBlockFacility(),
                    pluginConfig(GROUPING_LEVEL, PART_SIZE_MB));
            pluginExecutor = testThreadPoolManager.executor();
        }

        /// Drains the startup recovery task after [clearBucket] has cleaned S3.
        @BeforeEach
        void drainRecovery() {
            pluginExecutor.executeSerially();
        }

        /// Verifies that when blocks are large enough to exceed [CloudStorageArchiveConfig#partSizeMb()],
        /// the upload is split into multiple parts and the final tar is still committed correctly.
        /// With [PART_SIZE_MB] = 5 MB and [BLOCK_DATA_BYTES] = 600 KB per block, the buffer
        /// overflows after ~9 blocks, so [BlockUploadTask] flushes several parts before the final one.
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
                        new VerificationNotification(true, null, i, Bytes.EMPTY, block, BlockSource.PUBLISHER));
            }

            pluginExecutor.executeSerially();

            assertTrue(
                    getAllObjects().contains("0000/0000/0000/0000/0.tar"), "tar file should be uploaded via multipart");
            final List<PersistedNotification> notifications = blockMessaging.getSentPersistedNotifications();
            assertThat(notifications).isNotEmpty();
            notifications.forEach(n -> assertTrue(n.succeeded(), "all persisted notifications should be successful"));
        }
    }

    /// Tests for [CloudStorageArchivePlugin#stop].
    @Nested
    @DisplayName("Stop Tests")
    final class StopTests
            extends PluginTestBase<CloudStorageArchivePlugin, BlockingExecutor, ScheduledBlockingExecutor> {

        private final BlockingExecutor pluginExecutor;

        StopTests() {
            super(
                    new BlockingExecutor(new LinkedBlockingQueue<>()),
                    new ScheduledBlockingExecutor(new LinkedBlockingQueue<>()));
            start(new CloudStorageArchivePlugin(), new SimpleInMemoryHistoricalBlockFacility(), pluginConfig());
            pluginExecutor = testThreadPoolManager.executor();
        }

        /// Drains the startup recovery task after [clearBucket] has cleaned S3.
        @BeforeEach
        void drainRecovery() {
            pluginExecutor.executeSerially();
        }

        /// Verifies that [CloudStorageArchivePlugin#stop] does not throw and unregisters the plugin
        /// from the block messaging facility when there is no active archive task.
        @Test
        @DisplayName("stop() with no active task does not throw and unregisters the notification handler")
        void testStopWithNoActiveTask() {
            assertThat(plugin.currentUploadFuture).isNull();
            assertThat(blockMessaging.getBlockNotificationHandlerCount()).isOne();
            assertThatNoException().isThrownBy(plugin::stop);
            assertThat(blockMessaging.getBlockNotificationHandlerCount()).isZero();
        }

        /// Verifies that [CloudStorageArchivePlugin#stop] cancels the active [BlockUploadTask]
        /// future and unregisters the plugin.
        @Test
        @DisplayName("stop() with an active task aborts the upload and unregisters the notification handler")
        void testStopWithActiveTaskAbortsAndUnregisters() throws Exception {
            sendVerification(TestBlockBuilder.generateBlocksInRange(0, 0).getFirst());
            assertThat(plugin.currentUploadFuture).isNotNull();
            assertThat(blockMessaging.getBlockNotificationHandlerCount()).isOne();

            plugin.stop();
            // CancellationException is expected: the future was cancelled before the task ran
            try {
                pluginExecutor.executeSerially();
            } catch (java.util.concurrent.CancellationException ignored) {
            }

            assertThat(blockMessaging.getBlockNotificationHandlerCount()).isZero();
            assertThat(getAllObjects()).doesNotContain("0000/0000/0000/0000/0.tar");
        }

        /// Verifies that verification notifications sent after [CloudStorageArchivePlugin#stop] are
        /// not processed: the plugin is no longer registered so no new upload task is created.
        @Test
        @DisplayName("Verification notifications are not processed after stop()")
        void testNotificationsIgnoredAfterStop() {
            plugin.stop();
            sendVerification(TestBlockBuilder.generateBlocksInRange(0, 0).getFirst());
            assertThat(plugin.currentUploadFuture).isNull();
        }

        private void sendVerification(TestBlock block) {
            blockMessaging.sendBlockVerification(new VerificationNotification(
                    true, null, block.number(), Bytes.EMPTY, block.blockUnparsed(), BlockSource.PUBLISHER));
        }
    }

    /// End-to-end recovery integration tests that drive a real [CloudStorageArchivePlugin] against
    /// the shared MinIO container.
    ///
    /// Unlike [PluginTests], these tests intentionally omit a `@BeforeEach` that drains the startup
    /// recovery task.  Instead, each test arranges its desired S3 state after [clearBucket] has run
    /// and before calling [BlockingExecutor#executeSerially] for the first time, so that
    /// [StartupRecoveryTask] observes the intended initial condition when it is finally executed.
    @Nested
    @DisplayName("Recovery Integration Tests")
    final class RecoveryIntegrationTests
            extends PluginTestBase<CloudStorageArchivePlugin, BlockingExecutor, ScheduledBlockingExecutor> {

        private static final int GROUPING_LEVEL = 1;
        private static final String CONTENT_TYPE = "application/x-tar";

        private final BlockingExecutor pluginExecutor;

        RecoveryIntegrationTests() {
            super(
                    new BlockingExecutor(new LinkedBlockingQueue<>()),
                    new ScheduledBlockingExecutor(new LinkedBlockingQueue<>()));
            start(
                    new CloudStorageArchivePlugin(),
                    new SimpleInMemoryHistoricalBlockFacility(),
                    pluginConfig(GROUPING_LEVEL, 10));
            pluginExecutor = testThreadPoolManager.executor();
        }

        /// Verifies the full resume-from-completed-group path end-to-end: a completed tar is planted
        /// in S3 before recovery runs, and the plugin picks up exactly at the next group start.
        @Test
        @DisplayName("Plugin resumes from a completed tar and starts uploading the next group")
        void resumeAfterCompletedTarStartsNextGroup() throws Exception {
            final long groupSize = Math.powExact(10, GROUPING_LEVEL);
            final CloudStorageArchiveConfig config = makeConfig();
            final String firstKey = ArchiveKey.format(0, GROUPING_LEVEL);

            // Plant a completed tar for the first group (blocks 0–9) before recovery runs.
            final List<TestBlock> firstGroupBlocks = TestBlockBuilder.generateBlocksInRange(0, (int) groupSize - 1);
            try (S3Client s3 = openS3Client(config)) {
                final String uploadId =
                        s3.createMultipartUpload(firstKey, config.storageClass().name(), CONTENT_TYPE);
                final String etag = s3.multipartUploadPart(firstKey, uploadId, 1, buildTarBytes(firstGroupBlocks, 0));
                s3.completeMultipartUpload(firstKey, uploadId, List.of(etag));
            }

            pluginExecutor.executeSerially();

            final List<TestBlock> secondGroupBlocks =
                    TestBlockBuilder.generateBlocksInRange((int) groupSize, (int) (groupSize * 2) - 1);
            sendVerifications(secondGroupBlocks);

            pluginExecutor.executeSerially();

            final String secondKey = ArchiveKey.format(groupSize, GROUPING_LEVEL);
            assertThat(getAllObjects()).contains(secondKey);
            final List<PersistedNotification> notifications = blockMessaging.getSentPersistedNotifications();
            assertThat(notifications).isNotEmpty();
            assertThat(notifications.getLast().blockNumber()).isEqualTo(groupSize * 2 - 1);
            assertThat(notifications.getLast().succeeded()).isTrue();
        }

        /// Verifies the full resume-from-hanging-upload path end-to-end: a hanging multipart upload
        /// containing tar entries for blocks 0–4 is planted in S3, the plugin recovers it, and then
        /// completes the group with fresh blocks 4–9, resulting in a committed tar at the expected key.
        @Test
        @DisplayName("Plugin resumes from a hanging multipart upload and completes the group correctly")
        void resumeFromHangingUploadCompletesGroup() throws Exception {
            final long groupSize = Math.powExact(10, GROUPING_LEVEL);
            final String key = ArchiveKey.format(0, GROUPING_LEVEL);
            final List<TestBlock> blocks = TestBlockBuilder.generateBlocksInRange(0, (int) groupSize - 1);

            final CloudStorageArchiveConfig config = makeConfig();
            try (S3Client s3 = openS3Client(config)) {
                final String uploadId =
                        s3.createMultipartUpload(key, config.storageClass().name(), CONTENT_TYPE);
                s3.multipartUploadPart(key, uploadId, 1, buildTarBytes(blocks.subList(0, 5), 0));
                // deliberately NOT completing — simulate a crash mid-upload
            }

            pluginExecutor.executeSerially();

            sendVerifications(blocks.subList(4, (int) groupSize));
            pluginExecutor.executeSerially();

            assertThat(getAllObjects()).contains(key);
            final List<PersistedNotification> notifications = blockMessaging.getSentPersistedNotifications();
            assertThat(notifications).isNotEmpty();
            assertThat(notifications.getLast().blockNumber()).isEqualTo(groupSize - 1);
            assertThat(notifications.getLast().succeeded()).isTrue();
        }

        /// Verifies that when a hanging multipart upload has no parts (crash before any part was
        /// written), recovery aborts it, falls back to completed-objects discovery, and the plugin
        /// correctly starts a fresh upload for the next group.
        ///
        /// A completed tar for group 0 is planted before recovery runs, so the fall-back returns
        /// `currentGroupStart = groupSize`.  Blocks for group 1 are then sent and must produce a
        /// committed tar at the expected key.
        @Test
        @DisplayName("Hanging upload with no parts: aborted, plugin resumes from last completed tar")
        void hangingUploadWithNoPartsAbortsAndResumesFromLastCompletedTar() throws Exception {
            final long groupSize = Math.powExact(10, GROUPING_LEVEL);
            final CloudStorageArchiveConfig config = makeConfig();
            final String firstKey = ArchiveKey.format(0, GROUPING_LEVEL);
            final String secondKey = ArchiveKey.format(groupSize, GROUPING_LEVEL);

            try (S3Client s3 = openS3Client(config)) {
                // Complete group 0 (blocks 0–9).
                final String uploadId0 =
                        s3.createMultipartUpload(firstKey, config.storageClass().name(), CONTENT_TYPE);
                final String etag0 = s3.multipartUploadPart(
                        firstKey,
                        uploadId0,
                        1,
                        buildTarBytes(TestBlockBuilder.generateBlocksInRange(0, (int) groupSize - 1), 0));
                s3.completeMultipartUpload(firstKey, uploadId0, List.of(etag0));
                // Hanging upload for group 1 key with no parts — simulates a crash before any data was written.
                s3.createMultipartUpload(secondKey, config.storageClass().name(), CONTENT_TYPE);
            }

            // Recovery: parts list is empty → abort the upload → fall back to completed-objects
            // → currentGroupStart = groupSize.
            pluginExecutor.executeSerially();

            // Sending block groupSize triggers completeRecovery(), which submits a fresh
            // BlockUploadTask for group 1.  Remaining blocks follow into the queue.
            sendVerifications(TestBlockBuilder.generateBlocksInRange((int) groupSize, (int) (groupSize * 2) - 1));
            pluginExecutor.executeSerially();

            assertThat(getAllObjects()).contains(secondKey);
            final List<PersistedNotification> notifications = blockMessaging.getSentPersistedNotifications();
            assertThat(notifications).isNotEmpty();
            assertThat(notifications.getLast().blockNumber()).isEqualTo(groupSize * 2 - 1);
            assertThat(notifications.getLast().succeeded()).isTrue();
        }

        /// Verifies that when the hanging upload belongs to a non-first group (group 1, blocks
        /// 10–19), recovery correctly parses the group start from the S3 key, resumes the upload
        /// at the right block boundary, and the completed tar is committed at the expected key.
        ///
        /// The hanging upload contains complete tar entries for blocks 10–14.
        /// [TarEntries#findLastBlockStart] identifies block 14 as the last clean boundary, so
        /// recovery returns `nextBlockNumber = 14` and `trailingBytes = [entries for blocks 10–13]`.
        /// The plugin then receives blocks 14–19 and uses them to finish the group.
        @Test
        @DisplayName("Hanging upload for a non-first group: plugin resumes and completes the group")
        void resumeFromHangingUploadOnNonFirstGroupCompletesGroup() throws Exception {
            final long groupSize = Math.powExact(10, GROUPING_LEVEL);
            final CloudStorageArchiveConfig config = makeConfig();
            final String firstKey = ArchiveKey.format(0, GROUPING_LEVEL);
            final String secondKey = ArchiveKey.format(groupSize, GROUPING_LEVEL);
            final List<TestBlock> secondGroupBlocks =
                    TestBlockBuilder.generateBlocksInRange((int) groupSize, (int) (groupSize * 2) - 1);

            try (S3Client s3 = openS3Client(config)) {
                // Complete group 0 (blocks 0–9).
                final String uploadId0 =
                        s3.createMultipartUpload(firstKey, config.storageClass().name(), CONTENT_TYPE);
                final String etag0 = s3.multipartUploadPart(
                        firstKey,
                        uploadId0,
                        1,
                        buildTarBytes(TestBlockBuilder.generateBlocksInRange(0, (int) groupSize - 1), 0));
                s3.completeMultipartUpload(firstKey, uploadId0, List.of(etag0));
                // Hanging upload for group 1 (blocks 10–14) — simulates a crash mid-upload.
                final String uploadId1 = s3.createMultipartUpload(
                        secondKey, config.storageClass().name(), CONTENT_TYPE);
                s3.multipartUploadPart(
                        secondKey, uploadId1, 1, buildTarBytes(secondGroupBlocks.subList(0, 5), (int) groupSize));
                // deliberately NOT completing
            }

            // Recovery finds the group 1 hanging upload, locates block 14 as the last clean
            // boundary, and returns nextBlockNumber=14 with blocks 10–13 as trailingBytes.
            pluginExecutor.executeSerially();

            sendVerifications(secondGroupBlocks.subList(4, (int) groupSize));
            pluginExecutor.executeSerially();

            assertThat(getAllObjects()).contains(secondKey);
            final List<PersistedNotification> notifications = blockMessaging.getSentPersistedNotifications();
            assertThat(notifications).isNotEmpty();
            assertThat(notifications.getLast().blockNumber()).isEqualTo(groupSize * 2 - 1);
            assertThat(notifications.getLast().succeeded()).isTrue();
        }

        /// Verifies that blocks arriving before the resume point during a resumed upload are
        /// silently skipped and do not corrupt or stall the upload.
        ///
        /// Setup mirrors [resumeFromHangingUploadCompletesGroup]: a hanging upload with entries for
        /// blocks 0–4 produces `nextBlockNumber = 4` and `trailingBytes = [entries for blocks 0–3]`.
        /// Unlike that test, ALL blocks 0–9 are sent here.  Blocks 0–3 fall before
        /// `nextBlockToQueue = 4`, so [drainPendingToQueue] never reaches them; they accumulate in
        /// `currentGroupPending` and are silently dropped.  Blocks 4–9 drain normally, the
        /// [BlockUploadTask] combines them with `trailingBytes`, and the complete tar is committed.
        @Test
        @DisplayName("Blocks before the resume point are silently skipped and upload still completes")
        void blocksBeforeResumePointAreSkippedAndUploadCompletes() throws Exception {
            final long groupSize = Math.powExact(10, GROUPING_LEVEL);
            final String key = ArchiveKey.format(0, GROUPING_LEVEL);
            final List<TestBlock> blocks = TestBlockBuilder.generateBlocksInRange(0, (int) groupSize - 1);

            final CloudStorageArchiveConfig config = makeConfig();
            try (S3Client s3 = openS3Client(config)) {
                final String uploadId =
                        s3.createMultipartUpload(key, config.storageClass().name(), CONTENT_TYPE);
                s3.multipartUploadPart(key, uploadId, 1, buildTarBytes(blocks.subList(0, 5), 0));
                // deliberately NOT completing — simulate a crash mid-upload
            }

            pluginExecutor.executeSerially();

            sendVerifications(blocks);
            pluginExecutor.executeSerially();

            assertThat(getAllObjects()).contains(key);
            final List<PersistedNotification> notifications = blockMessaging.getSentPersistedNotifications();
            assertThat(notifications).isNotEmpty();
            assertThat(notifications.getLast().blockNumber()).isEqualTo(groupSize - 1);
            assertThat(notifications.getLast().succeeded()).isTrue();
            // Blocks 0–3 were silently dropped: still in pending, never queued.
            assertThat(plugin.currentGroupPending).containsOnlyKeys(0L, 1L, 2L, 3L);
        }

        private CloudStorageArchiveConfig makeConfig() {
            final ConfigurationBuilder builder =
                    ConfigurationBuilder.create().withConfigDataType(CloudStorageArchiveConfig.class);
            pluginConfig(GROUPING_LEVEL, 10).forEach(builder::withValue);
            return builder.build().getConfigData(CloudStorageArchiveConfig.class);
        }

        private static S3Client openS3Client(CloudStorageArchiveConfig config) throws Exception {
            return new S3Client(
                    config.regionName(),
                    config.endpointUrl(),
                    config.bucketName(),
                    config.accessKey(),
                    config.secretKey());
        }

        private static byte[] buildTarBytes(List<TestBlock> blocks, int startBlockNum) throws IOException {
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            for (int i = 0; i < blocks.size(); i++) {
                baos.write(TarEntries.toTarEntry(blocks.get(i).blockUnparsed(), startBlockNum + i));
            }
            return baos.toByteArray();
        }

        private void sendVerifications(List<TestBlock> blocks) {
            for (final TestBlock block : blocks) {
                sendVerification(block);
            }
        }

        private void sendVerification(TestBlock block) {
            blockMessaging.sendBlockVerification(new VerificationNotification(
                    true, null, block.number(), Bytes.EMPTY, block.blockUnparsed(), BlockSource.PUBLISHER));
        }
    }
}
