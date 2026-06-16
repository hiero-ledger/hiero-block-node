// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.cloud.storage.archive;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Stream;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.app.config.node.NodeConfig;
import org.hiero.block.node.app.fixtures.TestMetricsExporter;
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
import org.hiero.block.node.spi.blockmessaging.VerificationNotification.FailureInfo;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification.FailureType;
import org.hiero.block.node.spi.historicalblocks.HistoricalBlockFacility;
import org.hiero.block.node.spi.historicalblocks.LongRange;
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
                "cloud.storage.archive.groupingLevel", String.valueOf(groupingLevel),
                "cloud.storage.archive.partSizeMb", String.valueOf(partSizeMb),
                "cloud.storage.archive.endpointUrl", minioEndpoint,
                "cloud.storage.archive.regionName", "us-east-1",
                "cloud.storage.archive.bucketName", BUCKET_NAME,
                "cloud.storage.archive.accessKey", MINIO_USER,
                "cloud.storage.archive.secretKey", MINIO_PASSWORD);
    }

    /// A [CloudStorageArchivePlugin] subclass that replaces the real [BlockUploadTask] with a
    /// configurable [Callable].  Used by [MidRunRecoveryTests] and [MidRunExceptionTests] to drive
    /// the upload-failure code path without an active S3 upload.
    private static class FailingUploadPlugin extends CloudStorageArchivePlugin {
        private final Callable<UploadResult> uploadBehavior;

        FailingUploadPlugin(Callable<UploadResult> uploadBehavior) {
            this.uploadBehavior = uploadBehavior;
        }

        @Override
        Callable<UploadResult> newUploadTask(long firstBlock, long groupSize, BlockingQueue<BlockWithSource> queue) {
            return uploadBehavior;
        }
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
                    .withConfigDataType(NodeConfig.class)
                    .withValue("cloud.storage.archive.endpointUrl", minioEndpoint)
                    .build();
            final HistoricalBlockFacility historicalBlockFacility = new SimpleInMemoryHistoricalBlockFacility();
            final BlockNodeContext testContext = new BlockNodeContext(
                    configuration,
                    MetricRegistry.builder().build(),
                    new TestHealthFacility(),
                    new TestBlockMessagingFacility(),
                    historicalBlockFacility,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    new ArrayList<>(),
                    new ArrayList<>());
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
            final ConfigurationBuilder builder = ConfigurationBuilder.create()
                    .withConfigDataType(CloudStorageArchiveConfig.class)
                    .withConfigDataType(NodeConfig.class);
            configValues.forEach(builder::withValue);
            final TestBlockMessagingFacility messaging = new TestBlockMessagingFacility();
            final BlockNodeContext testContext = new BlockNodeContext(
                    builder.build(),
                    MetricRegistry.builder().build(),
                    new TestHealthFacility(),
                    messaging,
                    new SimpleInMemoryHistoricalBlockFacility(),
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    new ArrayList<>(),
                    new ArrayList<>());
            new CloudStorageArchivePlugin().init(testContext, null);
            assertThat(messaging.getBlockNotificationHandlerCount()).isZero();
        }

        static Stream<Arguments> blankOrEmptyFieldConfigs() {
            final Map<String, String> full = new HashMap<>(Map.of(
                    "cloud.storage.archive.endpointUrl", "http://localhost:9000",
                    "cloud.storage.archive.regionName", "us-east-1",
                    "cloud.storage.archive.accessKey", "minioadmin",
                    "cloud.storage.archive.secretKey", "minioadmin",
                    "cloud.storage.archive.bucketName", "test-bucket"));
            return Stream.of(
                    Arguments.of("endpointUrl (empty)", withoutKey(full, "cloud.storage.archive.endpointUrl")),
                    Arguments.of("regionName (empty)", withoutKey(full, "cloud.storage.archive.regionName")),
                    Arguments.of("accessKey (empty)", withoutKey(full, "cloud.storage.archive.accessKey")),
                    Arguments.of("secretKey (empty)", withoutKey(full, "cloud.storage.archive.secretKey")),
                    Arguments.of("bucketName (empty)", withoutKey(full, "cloud.storage.archive.bucketName")),
                    Arguments.of("endpointUrl (blank)", withValue(full, "cloud.storage.archive.endpointUrl", "   ")),
                    Arguments.of("regionName (blank)", withValue(full, "cloud.storage.archive.regionName", "   ")),
                    Arguments.of("accessKey (blank)", withValue(full, "cloud.storage.archive.accessKey", "   ")),
                    Arguments.of("secretKey (blank)", withValue(full, "cloud.storage.archive.secretKey", "   ")),
                    Arguments.of("bucketName (blank)", withValue(full, "cloud.storage.archive.bucketName", "   ")));
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
                    .withConfigDataType(NodeConfig.class)
                    .withValue("cloud.storage.archive.endpointUrl", "http://localhost:9000")
                    .withValue("cloud.storage.archive.regionName", "us-east-1")
                    .withValue("cloud.storage.archive.accessKey", "minioadmin")
                    .withValue("cloud.storage.archive.secretKey", "minioadmin")
                    .withValue("cloud.storage.archive.bucketName", "test-bucket")
                    .build();
            final TestBlockMessagingFacility messaging = new TestBlockMessagingFacility();
            final BlockNodeContext testContext = new BlockNodeContext(
                    configuration,
                    MetricRegistry.builder().build(),
                    new TestHealthFacility(),
                    messaging,
                    new SimpleInMemoryHistoricalBlockFacility(),
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    new ArrayList<>(),
                    new ArrayList<>());
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
        @DisplayName("Block from next group goes to temp archive and is consolidated when group is complete")
        void testNextGroupBlockStashedAndReplayed() throws Exception {
            final int groupSize = (int) Math.pow(10, GROUPING_LEVEL); // 10
            // send one block from group 0–9 to create the first task
            sendVerification(TestBlockBuilder.generateBlocksInRange(5, 5).getFirst());
            // send a block from group 10–19 while group 0–9 is active — it should land in the
            // streaming temp archive queue, since out-of-range blocks go to temp archives immediately
            sendVerification(
                    TestBlockBuilder.generateBlocksInRange(groupSize, groupSize).getFirst());
            assertThat(plugin.tempGroupActiveQueues).containsKey((long) groupSize);
            // complete group 0–9 (all except block 5 which was already sent)
            sendVerifications(TestBlockBuilder.generateBlocksInRange(0, 4));
            sendVerifications(TestBlockBuilder.generateBlocksInRange(6, groupSize - 1));
            // complete group 10–19: block 19 (last in group) triggers closeActiveTempSegment
            // and places SEGMENT_END in the queue so TempArchiveUploadTask[10,19] can finalise
            sendVerification(TestBlockBuilder.generateBlocksInRange(groupSize + 1, groupSize + 1)
                    .getFirst());
            sendVerifications(TestBlockBuilder.generateBlocksInRange(groupSize + 2, groupSize * 2 - 1));
            // segment is now closed — all 10 blocks + sentinel are in the queue
            assertThat(plugin.tempGroupActiveQueues).doesNotContainKey((long) groupSize);
            // runs BlockUploadTask for [0, 9] and TempArchiveUploadTask for [10, 19]
            pluginExecutor.executeSerially();
            assertTrue(getAllObjects().contains("0000/0000/0000/0000/0.tar"), "first tar should be uploaded");
            // block 20 drives the production handleVerification path (checkAndDrainTempUploadResults +
            // checkAndDrainConsolidations), queuing ConsolidationTask for [10,19].  It also starts a
            // regular BlockUploadTask for group [20,29], so complete that group to avoid a hang.
            sendVerification(TestBlockBuilder.generateBlocksInRange(groupSize * 2, groupSize * 2)
                    .getFirst());
            sendVerifications(TestBlockBuilder.generateBlocksInRange(groupSize * 2 + 1, groupSize * 3 - 1));
            pluginExecutor.executeSerially(); // runs ConsolidationTask for [10,19] then BlockUploadTask for [20,29]
            assertTrue(getAllObjects().contains("0000/0000/0000/0000/1.tar"), "second tar should be uploaded");
            assertTrue(getAllObjects().contains("0000/0000/0000/0000/2.tar"), "third tar should be uploaded");

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

        /// Verifies that a gap in the arrival stream for a group's temp archive closes the
        /// current segment (by placing [TempArchiveUploadTask.SEGMENT_END] in its queue) and
        /// immediately starts a fresh segment from the gap block.
        ///
        /// Scenario: group 10–19 receives blocks 10–12, then block 15 (gap at 13–14).
        ///   - The first segment [10, 12] receives SEGMENT_END; a second segment starts at 15.
        ///   - Blocks 16–19 complete the second segment; block 19 (last in group) closes it.
        ///   - Both tasks run in the same [executeSerially] call as [BlockUploadTask] for
        ///     group 0–9, since all queues are fully populated before execution begins.
        @Test
        @DisplayName("Gap in temp archive stream closes the current segment and starts a fresh one from the gap block")
        void testGapInTempArchiveStreamClosesSegmentAndStartsFresh() throws Exception {
            final int groupSize = (int) Math.pow(10, GROUPING_LEVEL); // 10
            // Start group 0–9 regular task
            sendVerification(TestBlockBuilder.generateBlocksInRange(5, 5).getFirst());
            // Blocks 10–12 start the first temp archive segment for group 10
            sendVerifications(TestBlockBuilder.generateBlocksInRange(groupSize, groupSize + 2));
            assertThat(plugin.tempGroupActiveQueues).containsKey((long) groupSize);
            final BlockingQueue<BlockWithSource> firstSegmentQueue = plugin.tempGroupActiveQueues.get((long) groupSize);

            // Block 15 arrives — gap at 13–14: SEGMENT_END is placed in the first segment's
            // queue and a new segment starts immediately at block 15
            sendVerification(TestBlockBuilder.generateBlocksInRange(groupSize + 5, groupSize + 5)
                    .getFirst());
            assertThat(plugin.tempGroupActiveQueues).containsKey((long) groupSize); // new segment active
            assertThat(plugin.tempGroupActiveQueues.get((long) groupSize)).isNotSameAs(firstSegmentQueue);
            assertThat(plugin.tempUploadFutures).containsKey((long) groupSize); // future for [10,12]
            assertThat(plugin.tempUploadFutures).containsKey((long) groupSize + 5); // future for [15,?]

            // Blocks 16–19: block 19 (last in group) closes the second segment
            sendVerifications(TestBlockBuilder.generateBlocksInRange(groupSize + 6, groupSize * 2 - 1));
            assertThat(plugin.tempGroupActiveQueues).doesNotContainKey((long) groupSize);

            // Complete group 0–9 and run all three tasks in one call
            sendVerifications(TestBlockBuilder.generateBlocksInRange(0, 4));
            sendVerifications(TestBlockBuilder.generateBlocksInRange(6, groupSize - 1));
            // runs BlockUploadTask[0,9], TempArchiveUploadTask[10,12], TempArchiveUploadTask[15,19]
            pluginExecutor.executeSerially();

            final Set<String> objects = getAllObjects();
            assertTrue(objects.contains("0000/0000/0000/0000/0.tar"), "group 0–9 tar should be uploaded");
            assertTrue(
                    objects.contains(TempArchiveKey.formatTar(groupSize, "")),
                    "temp archive for segment [10,12] should be uploaded");
            assertTrue(
                    objects.contains(TempArchiveKey.formatTar(groupSize + 5, "")),
                    "temp archive for segment [15,19] should be uploaded");

            // Notification order matches task submission order:
            // BlockUploadTask[0,9] → block 9, TempArchiveUploadTask[10,12] → block 12,
            // TempArchiveUploadTask[15,19] → block 19
            final List<PersistedNotification> notifications = blockMessaging.getSentPersistedNotifications();
            assertEquals(3, notifications.size(), "expected one notification per segment");
            assertEquals(groupSize - 1L, notifications.get(0).blockNumber(), "block 9 from group 0–9");
            assertEquals(groupSize + 2L, notifications.get(1).blockNumber(), "block 12 from segment [10,12]");
            assertEquals(groupSize * 2 - 1L, notifications.get(2).blockNumber(), "block 19 from segment [15,19]");
            notifications.forEach(n -> assertTrue(n.succeeded(), "all notifications should be successful"));
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
                            FailureInfo.standard(FailureType.BAD_BLOCK_PROOF),
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
        /// 0–9) starts a streaming temp archive immediately, is uploaded concurrently with group
        /// 0–9, and is eventually consolidated into the third group's final tar once block 30 drives
        /// the production drain path.  Group 10–19 still flows through the regular pipeline because
        /// no temp data exists for it when block 10 eventually arrives.
        ///
        /// Execution order (all blocks for group 20–29 are sent before the first executeSerially so
        /// that TempArchiveUploadTask can drain its complete queue without blocking):
        ///   executeSerially 1: BlockUploadTask[0,9] + TempArchiveUploadTask[20,29]
        ///   executeSerially 2: ConsolidationTask[20,29] + BlockUploadTask[10,19]
        ///   executeSerially 3: BlockUploadTask[30,39]
        ///
        /// Because TempArchiveUploadTask[20,29] runs in the first executeSerially (before
        /// BlockUploadTask[10,19]), the notification order is [9, 29, 19, 39].
        @Test
        @DisplayName("Block two groups ahead goes to temp buffer and is consolidated for the correct group")
        void testBlockTwoGroupsAheadStashedAndReplayed() throws Exception {
            final int groupSize = (int) Math.pow(10, GROUPING_LEVEL); // 10
            // Trigger group 0-9 task with block 0
            sendVerification(TestBlockBuilder.generateBlocksInRange(0, 0).getFirst());
            // Block 20 is two groups ahead — starts a TempArchiveUploadTask for group 20 immediately
            sendVerification(TestBlockBuilder.generateBlocksInRange(groupSize * 2, groupSize * 2)
                    .getFirst());
            assertThat(plugin.tempGroupActiveQueues).containsKey((long) groupSize * 2);

            // Complete group 0-9
            sendVerifications(TestBlockBuilder.generateBlocksInRange(1, groupSize - 1));
            // Complete group 20-29: blocks 21-28 stream into the live queue; block 29 (last in
            // group) triggers closeActiveTempSegment → SEGMENT_END so the task can finalise
            sendVerifications(TestBlockBuilder.generateBlocksInRange(groupSize * 2 + 1, groupSize * 3 - 2));
            assertThat(plugin.tempGroupActiveQueues).containsKey((long) groupSize * 2); // still streaming
            sendVerification(TestBlockBuilder.generateBlocksInRange(groupSize * 3 - 1, groupSize * 3 - 1)
                    .getFirst());
            // segment is now closed — all 10 blocks + sentinel are in the queue
            assertThat(plugin.tempGroupActiveQueues).doesNotContainKey((long) groupSize * 2);

            // runs BlockUploadTask for [0, 9] and TempArchiveUploadTask for [20, 29]
            pluginExecutor.executeSerially();
            assertTrue(getAllObjects().contains("0000/0000/0000/0000/0.tar"), "first tar should be uploaded");

            // Complete group 10-19 via the regular pipeline (no temp data for group 10-19):
            // block 10 drives checkAndDrainTempUploadResults → queues ConsolidationTask[20,29]
            // and starts BlockUploadTask[10,19]; blocks 11-19 follow into the queue.
            sendVerifications(TestBlockBuilder.generateBlocksInRange(groupSize, groupSize * 2 - 1));
            // runs ConsolidationTask for [20,29] then BlockUploadTask for [10,19]
            pluginExecutor.executeSerially();
            assertTrue(getAllObjects().contains("0000/0000/0000/0000/1.tar"), "second tar should be uploaded");
            assertTrue(getAllObjects().contains("0000/0000/0000/0000/2.tar"), "third tar should be uploaded");

            // block 30 drives checkAndDrainConsolidations and starts BlockUploadTask for [30,39]
            sendVerification(TestBlockBuilder.generateBlocksInRange(groupSize * 3, groupSize * 3)
                    .getFirst());
            sendVerifications(TestBlockBuilder.generateBlocksInRange(groupSize * 3 + 1, groupSize * 4 - 1));
            pluginExecutor.executeSerially(); // runs BlockUploadTask for [30,39]
            assertTrue(getAllObjects().contains("0000/0000/0000/0000/3.tar"), "fourth tar should be uploaded");
            assertThat(plugin.tempGroupActiveQueues).isEmpty();

            final List<PersistedNotification> notifications = blockMessaging.getSentPersistedNotifications();
            assertEquals(4, notifications.size(), "expected one persisted notification per batch");
            // executeSerially 1: BlockUploadTask[0,9] runs before TempArchiveUploadTask[20,29]
            assertEquals(
                    groupSize - 1L,
                    notifications.get(0).blockNumber(),
                    "first notification should be for block 9 (group 0-9)");
            assertEquals(
                    groupSize * 3 - 1L,
                    notifications.get(1).blockNumber(),
                    "second notification should be for block 29 (temp archive ran in first executeSerially)");
            // executeSerially 2: BlockUploadTask[10,19] (ConsolidationTask sends no notifications)
            assertEquals(
                    groupSize * 2 - 1L,
                    notifications.get(2).blockNumber(),
                    "third notification should be for block 19 (group 10-19)");
            // executeSerially 3: BlockUploadTask[30,39]
            assertEquals(
                    groupSize * 4 - 1L,
                    notifications.get(3).blockNumber(),
                    "fourth notification should be for block 39 (group 30-39)");
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
            final String firstKey = ArchiveKey.format(0, GROUPING_LEVEL, "");

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

            final String secondKey = ArchiveKey.format(groupSize, GROUPING_LEVEL, "");
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
            final String key = ArchiveKey.format(0, GROUPING_LEVEL, "");
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
            final String firstKey = ArchiveKey.format(0, GROUPING_LEVEL, "");
            final String secondKey = ArchiveKey.format(groupSize, GROUPING_LEVEL, "");

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
            final String firstKey = ArchiveKey.format(0, GROUPING_LEVEL, "");
            final String secondKey = ArchiveKey.format(groupSize, GROUPING_LEVEL, "");
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
            final String key = ArchiveKey.format(0, GROUPING_LEVEL, "");
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

        private final List<LongRange> capturedRanges = new ArrayList<>();

        @Override
        public void addStoredBlockRange(LongRange blockRange) {
            super.addStoredBlockRange(blockRange);
            capturedRanges.add(blockRange);
        }

        /// Verifies that `CloudStorageArchivePlugin.completeRecovery` does not call
        /// `ApplicationStateFacility.addStoredBlockRange` when S3 is empty (fresh start).
        @Test
        @DisplayName("Fresh start: addBlockRange is not called when the bucket is empty")
        void freshStartDoesNotCallAddBlockRange() {
            // Bucket is empty (cleared by @BeforeEach clearBucket). Drive the recovery task.
            pluginExecutor.executeSerially();
            // Trigger completeRecovery() via the first verification notification.
            sendVerification(TestBlockBuilder.generateBlocksInRange(0, 0).getFirst());
            assertThat(capturedRanges).isEmpty();
        }

        /// Verifies that `CloudStorageArchivePlugin.completeRecovery()` calls
        /// `ApplicationStateFacility.addStoredBlockRange` with the full range of the last completed group
        /// when recovery finds a completed tar in S3.
        @Test
        @DisplayName("Completed tar recovery: addBlockRange reports blocks 0 to groupSize-1 as STORED")
        void completedTarRecoveryCallsAddBlockRange() throws Exception {
            final long groupSize = Math.powExact(10, GROUPING_LEVEL);
            final CloudStorageArchiveConfig config = makeConfig();
            final String firstKey = ArchiveKey.format(0, GROUPING_LEVEL, "");

            try (S3Client s3 = openS3Client(config)) {
                final String uploadId =
                        s3.createMultipartUpload(firstKey, config.storageClass().name(), CONTENT_TYPE);
                final String etag = s3.multipartUploadPart(
                        firstKey,
                        uploadId,
                        1,
                        buildTarBytes(TestBlockBuilder.generateBlocksInRange(0, (int) groupSize - 1), 0));
                s3.completeMultipartUpload(firstKey, uploadId, List.of(etag));
            }

            pluginExecutor.executeSerially();

            // Trigger completeRecovery() — recovery result has currentGroupStart=groupSize, uploadId=null.
            sendVerification(TestBlockBuilder.generateBlocksInRange((int) groupSize, (int) groupSize)
                    .getFirst());

            assertThat(capturedRanges).containsExactly(new LongRange(0, groupSize - 1));
        }

        /// Verifies that `CloudStorageArchivePlugin.completeRecovery()` calls
        /// `ApplicationStateFacility.addStoredBlockRange` up to the last clean boundary found in the
        /// hanging upload when recovery resumes a multipart upload.
        ///
        /// A hanging upload with tar entries for blocks 0–4 is planted.  Recovery locates block 4
        /// as the last clean boundary, so `nextBlockNumber = 4` and `addBlockRange(0, 3, STORED)`
        /// is expected.
        @Test
        @DisplayName("Hanging upload recovery: addBlockRange reports blocks 0 to nextBlockNumber-1 as STORED")
        void hangingUploadRecoveryCallsAddBlockRange() throws Exception {
            final long groupSize = Math.powExact(10, GROUPING_LEVEL);
            final String key = ArchiveKey.format(0, GROUPING_LEVEL, "");
            final List<TestBlock> blocks = TestBlockBuilder.generateBlocksInRange(0, (int) groupSize - 1);

            final CloudStorageArchiveConfig config = makeConfig();
            try (S3Client s3 = openS3Client(config)) {
                final String uploadId =
                        s3.createMultipartUpload(key, config.storageClass().name(), CONTENT_TYPE);
                s3.multipartUploadPart(key, uploadId, 1, buildTarBytes(blocks.subList(0, 5), 0));
                // deliberately NOT completing — simulate a crash mid-upload
            }

            pluginExecutor.executeSerially();

            // Trigger completeRecovery() — recovery locates block 4 as the last clean boundary,
            // so nextBlockNumber=4, meaning blocks 0–3 were stored before the crash.
            sendVerification(TestBlockBuilder.generateBlocksInRange(4, 4).getFirst());

            assertThat(capturedRanges).containsExactly(new LongRange(0, 3));
        }

        /// Verifies that when `.meta` companion files for temp archives are present in S3, startup
        /// recovery rebuilds [CloudStorageArchivePlugin#tempArchiveTracker] from them and registers
        /// each recovered block range with [org.hiero.block.node.spi.ApplicationStateFacility].
        ///
        /// Two temp archive pairs (blocks 5–9 in group 0, blocks 20–29 in group 20) are planted
        /// before recovery runs.  After recovery, the first block notification triggers
        /// [CloudStorageArchivePlugin#completeRecoveryIfReady], which populates the tracker and
        /// calls `addStoredBlockRange` for each recovered range.
        @Test
        @DisplayName("Temp archive .meta files on S3 are rebuilt into tempArchiveTracker on recovery")
        void tempArchivesRebuiltFromMetaFilesOnRecovery() throws Exception {
            final CloudStorageArchiveConfig cfg = makeConfig();
            try (S3Client s3 = openS3Client(cfg)) {
                // Group 0: blocks 5–9
                s3.uploadTextFile(
                        TempArchiveKey.formatTar(5, cfg.objectKeyPrefix()),
                        cfg.storageClass().name(),
                        "dummy-tar");
                s3.uploadTextFile(
                        TempArchiveKey.formatMeta(5, cfg.objectKeyPrefix()),
                        cfg.storageClass().name(),
                        "9");
                // Group 20: blocks 20–29
                s3.uploadTextFile(
                        TempArchiveKey.formatTar(20, cfg.objectKeyPrefix()),
                        cfg.storageClass().name(),
                        "dummy-tar");
                s3.uploadTextFile(
                        TempArchiveKey.formatMeta(20, cfg.objectKeyPrefix()),
                        cfg.storageClass().name(),
                        "29");
            }

            // Run startup recovery.
            pluginExecutor.executeSerially();

            // Send block 30 to trigger completeRecoveryIfReady().
            sendVerification(TestBlockBuilder.generateBlocksInRange(30, 30).getFirst());

            assertThat(plugin.tempArchiveTracker).containsKey(5L);
            assertThat(plugin.tempArchiveTracker.get(5L).lastBlock()).isEqualTo(9L);
            assertThat(plugin.tempArchiveTracker).containsKey(20L);
            assertThat(plugin.tempArchiveTracker.get(20L).lastBlock()).isEqualTo(29L);
            assertThat(capturedRanges).contains(new LongRange(5, 9), new LongRange(20, 29));
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

    /// Tests that verify mid-run recovery is triggered when an upload task returns [UploadResult.FAILED],
    /// and that the plugin resumes correctly without losing blocks.
    @Nested
    @DisplayName("Mid-Run Recovery Tests")
    final class MidRunRecoveryTests
            extends PluginTestBase<CloudStorageArchivePlugin, BlockingExecutor, ScheduledBlockingExecutor> {

        private final BlockingExecutor pluginExecutor;

        MidRunRecoveryTests() {
            super(
                    new BlockingExecutor(new LinkedBlockingQueue<>()),
                    new ScheduledBlockingExecutor(new LinkedBlockingQueue<>()));
            start(
                    new FailingUploadPlugin(() -> UploadResult.FAILED),
                    new SimpleInMemoryHistoricalBlockFacility(),
                    pluginConfig());
            pluginExecutor = testThreadPoolManager.executor();
        }

        @BeforeEach
        void drainRecovery() {
            pluginExecutor.executeSerially();
        }

        /// Verifies that a task returning [UploadResult.FAILED] triggers recovery:
        /// the failed-tasks metric increments, blocks still in [currentGroupPending] are moved to
        /// [blocksStash], and the triggering block is also stashed so nothing is silently dropped.
        @Test
        @DisplayName("FAILED upload result stashes pending blocks and submits recovery")
        void failedResultTriggersMidRunRecovery() throws Exception {
            final List<TestBlock> blocks = TestBlockBuilder.generateBlocksInRange(0, 9);

            // Simulate blocks 5 and 6 arrived but were not yet drained to the task queue.
            sendVerification(blocks.get(5));
            sendVerification(blocks.get(6));

            // Run the fake upload task.  It returns FAILED immediately.
            pluginExecutor.executeSerially();

            // Block 7 triggers checkCompletedUpload() -> FAILED -> triggerMidRunRecovery().
            // triggerMidRunRecovery() moves currentGroupPending (blocks 5, 6) to blocksStash and
            // sets currentGroupStart to -1, so block 7 is stashed rather than added to pending.
            sendVerification(blocks.get(7));

            assertThat(plugin.currentUploadFuture).isNull();
            assertThat(plugin.currentGroupPending).isEmpty();
            assertThat(plugin.blocksStash).containsKeys(5L, 6L, 7L);
            assertThat(getMetricValue(CloudStorageArchivePlugin.METRIC_CLOUD_ARCHIVE_FAILED_TASKS))
                    .isEqualTo(1L);

            // Drive the recovery task - empty bucket results in a fresh start.
            pluginExecutor.executeSerially();
            assertThat(plugin.isRecoveryComplete()).isTrue();
            assertThat(plugin.recoveredNextBlockNumber()).isEqualTo(0L);
        }

        /// Verifies that the guard in [triggerMidRunRecovery] prevents a duplicate
        /// [StartupRecoveryTask] from being submitted when a second upload failure is detected
        /// while recovery is already running.
        @Test
        @DisplayName("Second upload failure while recovery is in progress does not spawn a second recovery task")
        void secondUploadFailureWhileRecoveryInProgressIsIgnored() {
            final List<TestBlock> blocks = TestBlockBuilder.generateBlocksInRange(0, 2);

            // First failure through the production path:
            // block 0 arrives -> startNewUploadTask(0) -> fake task T1 queued; block 0 -> pending.
            sendVerification(blocks.get(0));
            // Run T1 — instant FAILED, no exception.
            pluginExecutor.executeSerially();
            // block 1 arrives -> checkCompletedUpload() detects FAILED -> triggerMidRunRecovery()
            // -> block 0 stashed; recovery task T2 queued (queue size: 1, not yet run).
            sendVerification(blocks.get(1));

            assertThat(pluginExecutor.getQueue()).hasSize(1);
            assertThat(plugin.isRecoveryComplete()).isFalse();

            // Second failure while recovery is queued: inject done-FAILED future and send block 2.
            // checkCompletedUpload() calls triggerMidRunRecovery() via the production path, but the
            // guard (recoveryFuture != null && !isDone()) makes it a no-op — no second task queued.
            plugin.currentUploadFuture = CompletableFuture.completedFuture(UploadResult.FAILED);
            sendVerification(blocks.get(2));

            assertThat(pluginExecutor.getQueue()).hasSize(1);

            pluginExecutor.executeSerially();
            assertThat(plugin.isRecoveryComplete()).isTrue();
        }

        /// Verifies that blocks stashed during mid-run recovery are replayed to temp archives when
        /// recovery completes with no in-progress regular group (currentGroupStart == -1).
        ///
        /// Without the fix, [CloudStorageArchivePlugin#tryReplayStash] was only called inside the
        /// `if (currentGroupStart != -1)` branch of [CloudStorageArchivePlugin#completeRecoveryIfReady],
        /// so stash blocks were permanently stranded when recovery found no in-progress upload.
        @Test
        @DisplayName("Blocks stashed during recovery are replayed to temp archive when recovery finds no group")
        void blocksStashedDuringRecoveryReplayedToTempArchiveOnFreshStart() {
            final int groupSize = 10;

            // Block 0 starts the regular upload task for [0, 9].
            sendVerification(TestBlockBuilder.generateBlocksInRange(0, 0).getFirst());
            // Run the failing task — immediately returns FAILED.
            pluginExecutor.executeSerially();

            // Block 1 detects FAILED, triggers mid-run recovery, and is stashed.
            sendVerification(TestBlockBuilder.generateBlocksInRange(1, 1).getFirst());
            assertThat(plugin.blocksStash).containsKey(1L);

            // Block 10 arrives while recovery is in progress.  It must land in blocksStash, not
            // tempGroupActiveQueues, because all blocks are stashed while recoveryFuture != null.
            sendVerification(
                    TestBlockBuilder.generateBlocksInRange(groupSize, groupSize).getFirst());
            assertThat(plugin.blocksStash).containsKey((long) groupSize);
            assertThat(plugin.tempGroupActiveQueues).doesNotContainKey((long) groupSize);

            // Drive recovery — bucket is empty, so StartupRecoveryTask returns currentGroupStart=-1.
            pluginExecutor.executeSerially();

            // Block 11 triggers completeRecoveryIfReady().  tryReplayStash() must be called even
            // though currentGroupStart==-1; block 10 must be routed to routeToTempArchive rather
            // than remaining stranded in the stash.
            sendVerification(TestBlockBuilder.generateBlocksInRange(groupSize + 1, groupSize + 1)
                    .getFirst());

            assertThat(plugin.blocksStash).doesNotContainKey((long) groupSize);
            assertThat(plugin.tempGroupActiveQueues).containsKey((long) groupSize);
            assertThat(plugin.tempGroupActiveQueues.get((long) groupSize)).hasSize(2);
        }

        private void sendVerification(TestBlock block) {
            blockMessaging.sendBlockVerification(new VerificationNotification(
                    true, null, block.number(), Bytes.EMPTY, block.blockUnparsed(), BlockSource.PUBLISHER));
        }
    }

    /// Tests that verify recovery is triggered when the upload-task [Future] completes
    /// exceptionally (i.e. the task threw rather than returning [UploadResult.FAILED]).
    @Nested
    @DisplayName("Mid-Run Exception Recovery Tests")
    final class MidRunExceptionTests
            extends PluginTestBase<CloudStorageArchivePlugin, BlockingExecutor, ScheduledBlockingExecutor> {

        private final BlockingExecutor pluginExecutor;

        MidRunExceptionTests() {
            super(
                    new BlockingExecutor(new LinkedBlockingQueue<>()),
                    new ScheduledBlockingExecutor(new LinkedBlockingQueue<>()));
            start(
                    new FailingUploadPlugin(() -> {
                        throw new IOException("simulated S3 failure");
                    }),
                    new SimpleInMemoryHistoricalBlockFacility(),
                    pluginConfig());
            pluginExecutor = testThreadPoolManager.executor();
        }

        @BeforeEach
        void drainRecovery() {
            pluginExecutor.executeSerially();
        }

        /// Verifies that an exception thrown by the upload-task [Future] triggers recovery with the
        /// same state effects as the [UploadResult.FAILED] path.
        @Test
        @DisplayName("Exception from upload task stashes pending blocks and submits recovery")
        void exceptionFromUploadTaskTriggersMidRunRecovery() throws Exception {
            final List<TestBlock> blocks = TestBlockBuilder.generateBlocksInRange(0, 9);

            sendVerification(blocks.get(5));

            // Run the fake task — it throws IOException, so BlockingExecutor wraps it as
            // RuntimeException.
            assertThatThrownBy(pluginExecutor::executeSerially).isInstanceOf(RuntimeException.class);

            // Block 6 triggers handleVerification() -> checkCompletedUpload() -> resultNow() throws
            // IllegalStateException -> caught by handleVerification() -> triggerMidRunRecovery()
            // (stashes block 5) -> blocksStash.put(6) stashes the triggering block explicitly.
            sendVerification(blocks.get(6));

            assertThat(plugin.currentUploadFuture).isNull();
            assertThat(plugin.currentGroupPending).isEmpty();
            assertThat(plugin.blocksStash).containsKeys(5L, 6L);
            assertThat(getMetricValue(CloudStorageArchivePlugin.METRIC_CLOUD_ARCHIVE_FAILED_TASKS))
                    .isEqualTo(1L);

            pluginExecutor.executeSerially();
            assertThat(plugin.isRecoveryComplete()).isTrue();
            assertThat(plugin.recoveredNextBlockNumber()).isEqualTo(0L);
        }

        private void sendVerification(TestBlock block) {
            blockMessaging.sendBlockVerification(new VerificationNotification(
                    true, null, block.number(), Bytes.EMPTY, block.blockUnparsed(), BlockSource.PUBLISHER));
        }
    }

    /// Tests that verify plugin-level task metrics ([METRIC_CLOUD_ARCHIVE_SUCCESSFUL_TASKS] and
    /// [METRIC_CLOUD_ARCHIVE_FAILED_TASKS]) are correctly incremented as upload tasks complete.
    ///
    /// The success tests use the correct MinIO credentials via [PluginTestBase].  The failure test
    /// creates an independent [CloudStorageArchivePlugin] with wrong credentials that shares the
    /// same [BlockingExecutor], so execution is still controlled deterministically from the test body.
    @Nested
    @DisplayName("Task Metrics Tests")
    final class TaskMetricsTests
            extends PluginTestBase<CloudStorageArchivePlugin, BlockingExecutor, ScheduledBlockingExecutor> {

        private static final int GROUPING_LEVEL = 1;
        private final BlockingExecutor pluginExecutor;

        TaskMetricsTests() {
            super(
                    new BlockingExecutor(new LinkedBlockingQueue<>()),
                    new ScheduledBlockingExecutor(new LinkedBlockingQueue<>()));
            start(
                    new CloudStorageArchivePlugin(),
                    new SimpleInMemoryHistoricalBlockFacility(),
                    pluginConfig(GROUPING_LEVEL, 10));
            pluginExecutor = testThreadPoolManager.executor();
        }

        /// Drains the main plugin's startup recovery task before each test.
        @BeforeEach
        void drainRecovery() {
            pluginExecutor.executeSerially();
        }

        /// Verifies that a successful upload task increments [METRIC_CLOUD_ARCHIVE_SUCCESSFUL_TASKS]
        /// by one and leaves [METRIC_CLOUD_ARCHIVE_FAILED_TASKS] at zero.
        ///
        /// [checkCompletedUpload] is only called from [handleVerification], so the metric is not
        /// updated until the next notification arrives after the task finishes.  A single trigger
        /// block from the next group is sent after [executeSerially] to flush the counter.
        @Test
        @DisplayName("Successful upload task increments successfulTasks and leaves failedTasks at zero")
        void testSuccessfulUploadIncrementsSuccessfulTasks() throws Exception {
            final int groupSize = (int) Math.pow(10, GROUPING_LEVEL);
            sendVerifications(TestBlockBuilder.generateBlocksInRange(0, groupSize - 1));
            pluginExecutor.executeSerially();
            // Trigger checkCompletedUpload() for the just-finished task.
            sendVerification(
                    TestBlockBuilder.generateBlocksInRange(groupSize, groupSize).getFirst());
            assertThat(getMetricValue(CloudStorageArchivePlugin.METRIC_CLOUD_ARCHIVE_SUCCESSFUL_TASKS))
                    .isEqualTo(1L);
            assertThat(getMetricValue(CloudStorageArchivePlugin.METRIC_CLOUD_ARCHIVE_FAILED_TASKS))
                    .isZero();
        }

        /// Verifies that two consecutive successful upload tasks increment
        /// [METRIC_CLOUD_ARCHIVE_SUCCESSFUL_TASKS] by two.
        ///
        /// A trigger block is sent after each [executeSerially] so [checkCompletedUpload] detects
        /// each completed task before the assertion.
        @Test
        @DisplayName("Two consecutive successful tasks increment successfulTasks by two")
        void testTwoSuccessfulTasksIncrementSuccessfulTasksTwice() throws Exception {
            final int groupSize = (int) Math.pow(10, GROUPING_LEVEL);
            sendVerifications(TestBlockBuilder.generateBlocksInRange(0, groupSize - 1));
            pluginExecutor.executeSerially();
            sendVerifications(TestBlockBuilder.generateBlocksInRange(groupSize, groupSize * 2 - 1));
            pluginExecutor.executeSerially();
            // Trigger checkCompletedUpload() for the second completed task.
            sendVerification(TestBlockBuilder.generateBlocksInRange(groupSize * 2, groupSize * 2)
                    .getFirst());
            assertThat(getMetricValue(CloudStorageArchivePlugin.METRIC_CLOUD_ARCHIVE_SUCCESSFUL_TASKS))
                    .isEqualTo(2L);
            assertThat(getMetricValue(CloudStorageArchivePlugin.METRIC_CLOUD_ARCHIVE_FAILED_TASKS))
                    .isZero();
        }

        /// Verifies that a failed [StartupRecoveryTask] (due to invalid S3 credentials) causes
        /// [METRIC_CLOUD_ARCHIVE_FAILED_TASKS] to increment by one when the next
        /// [CloudStorageArchivePlugin#handleVerification] call detects the done-but-failed recovery
        /// future and catches the resulting [ExecutionException].
        ///
        /// An independent plugin instance is created with wrong credentials but shares the same
        /// [BlockingExecutor] as the main plugin, so recovery can be driven the same way.
        @Test
        @DisplayName("Failed recovery task increments failedTasks and leaves successfulTasks at zero")
        void testFailedRecoveryIncrementsFailedTasks() {
            final TestMetricsExporter exporter = new TestMetricsExporter();
            final ConfigurationBuilder builder = ConfigurationBuilder.create()
                    .withConfigDataType(CloudStorageArchiveConfig.class)
                    .withConfigDataType(NodeConfig.class);
            Map.of(
                            "cloud.storage.archive.groupingLevel", "1",
                            "cloud.storage.archive.partSizeMb", "10",
                            "cloud.storage.archive.endpointUrl", minioEndpoint,
                            "cloud.storage.archive.regionName", "us-east-1",
                            "cloud.storage.archive.bucketName", BUCKET_NAME,
                            "cloud.storage.archive.accessKey", "wronguser",
                            "cloud.storage.archive.secretKey", "wrongpassword")
                    .forEach(builder::withValue);
            final TestBlockMessagingFacility failingMessaging = new TestBlockMessagingFacility();
            final BlockNodeContext failingContext = new BlockNodeContext(
                    builder.build(),
                    MetricRegistry.builder().setMetricsExporter(exporter).build(),
                    new TestHealthFacility(),
                    failingMessaging,
                    new SimpleInMemoryHistoricalBlockFacility(),
                    null,
                    null,
                    testThreadPoolManager,
                    null,
                    null,
                    null,
                    new ArrayList<>(),
                    new ArrayList<>());
            final CloudStorageArchivePlugin failingPlugin = new CloudStorageArchivePlugin();
            failingPlugin.init(failingContext, null);
            // start() submits the recovery task to the shared BlockingExecutor.
            failingPlugin.start();
            // Drain the failing recovery task.  BlockingExecutor re-wraps the S3ResponseException
            // (403) as RuntimeException, so catch and ignore it here — the Future is still done.
            try {
                pluginExecutor.executeSerially();
            } catch (RuntimeException ignored) {
            }
            // handleVerification() detects the done-but-failed recovery future, catches the
            // resulting ExecutionException, and increments failedTasks.
            final TestBlock block = TestBlockBuilder.generateBlocksInRange(0, 0).getFirst();
            failingMessaging.sendBlockVerification(new VerificationNotification(
                    true, null, block.number(), Bytes.EMPTY, block.blockUnparsed(), BlockSource.PUBLISHER));
            assertThat(exporter.getMetricValue(CloudStorageArchivePlugin.METRIC_CLOUD_ARCHIVE_FAILED_TASKS.name()))
                    .isEqualTo(1L);
            assertThat(exporter.getMetricValue(CloudStorageArchivePlugin.METRIC_CLOUD_ARCHIVE_SUCCESSFUL_TASKS.name()))
                    .isZero();
            failingPlugin.stop();
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

    /// Tests for the EMB-aware startup behaviour: [CloudStorageArchivePlugin#firstRegularGroupStart]
    /// computation and routing of pre-EMB blocks to temporary S3 archives instead of the regular
    /// upload pipeline.
    @Nested
    @DisplayName("EMB-Aware Tests")
    final class EmbAwareTests
            extends PluginTestBase<CloudStorageArchivePlugin, BlockingExecutor, ScheduledBlockingExecutor> {

        private static final int GROUPING_LEVEL = 1; // groupSize = 10^1 = 10
        private final BlockingExecutor pluginExecutor;

        EmbAwareTests() {
            super(
                    new BlockingExecutor(new LinkedBlockingQueue<>()),
                    new ScheduledBlockingExecutor(new LinkedBlockingQueue<>()));
            final Map<String, String> config = new HashMap<>(pluginConfig(GROUPING_LEVEL, 10));
            config.put("block.node.earliestManagedBlock", "5");
            start(new CloudStorageArchivePlugin(), new SimpleInMemoryHistoricalBlockFacility(), config);
            pluginExecutor = testThreadPoolManager.executor();
        }

        void drainRecovery() {
            pluginExecutor.executeSerially();
        }

        /// Verifies that [CloudStorageArchivePlugin#firstRegularGroupStart] is computed as the
        /// first aligned group boundary strictly above EMB.  With EMB=5 and groupSize=10 the first
        /// aligned group at or above EMB is 10 (since group [0,9] partially precedes EMB=5).
        @Test
        @DisplayName("EMB=5 with groupSize=10 produces firstRegularGroupStart=10")
        void embFiveYieldsFirstRegularGroupStart10() {
            assertThat(plugin.firstRegularGroupStart).isEqualTo(10L);
        }

        /// Verifies the boundary-aligned case: when EMB falls exactly on a group boundary the
        /// group that starts at EMB belongs entirely to the managed range, so [firstRegularGroupStart]
        /// equals EMB itself.  With EMB=10 and groupSize=10 the ceiling-division formula
        /// `((emb + groupSize - 1) / groupSize) * groupSize` must produce 10, not 20.
        @Test
        @DisplayName("EMB=10 with groupSize=10 (aligned on boundary) produces firstRegularGroupStart=10")
        void embAlignedOnBoundaryYieldsSameBoundary() {
            drainRecovery();
            final Map<String, String> config = new HashMap<>(pluginConfig(GROUPING_LEVEL, 10));
            config.put("block.node.earliestManagedBlock", "10");
            start(new CloudStorageArchivePlugin(), new SimpleInMemoryHistoricalBlockFacility(), config);
            pluginExecutor.executeSerially(); // drain startup recovery for the new plugin instance
            assertThat(plugin.firstRegularGroupStart).isEqualTo(10L);
        }

        /// Verifies that blocks below [CloudStorageArchivePlugin#firstRegularGroupStart] are routed
        /// to the temp-archive buffer rather than starting a regular [BlockUploadTask].
        ///
        /// Sending blocks 5–8 (all in group [0,9], below firstRegularGroupStart=10) should
        /// populate [CloudStorageArchivePlugin#tempGroupActiveQueues] for group 0 and leave
        /// [CloudStorageArchivePlugin#currentUploadFuture] null.
        @Test
        @DisplayName("EMB=5: blocks below firstRegularGroupStart go to temp archive buffer, not regular pipeline")
        void blocksBelowFirstRegularGroupStartGoToTempBuffer() {
            drainRecovery();
            sendVerifications(TestBlockBuilder.generateBlocksInRange(5, 8));
            assertThat(plugin.currentUploadFuture).isNull();
            assertThat(plugin.tempGroupActiveQueues).containsKey(0L);
            assertThat(plugin.tempGroupActiveQueues.get(0L)).hasSize(4);
        }

        /// Verifies that a block exactly at [CloudStorageArchivePlugin#firstRegularGroupStart]
        /// starts the regular archive pipeline (not the temp archive path).
        @Test
        @DisplayName("EMB=5: block 10 (at firstRegularGroupStart) starts the regular upload pipeline")
        void blockAtFirstRegularGroupStartStartsRegularPipeline() {
            drainRecovery();
            sendVerification(TestBlockBuilder.generateBlocksInRange(10, 10).getFirst());
            // A regular BlockUploadTask was submitted.
            assertThat(plugin.currentUploadFuture).isNotNull();
            // Block 10 was drained from currentGroupPending into the upload queue.
            assertThat(plugin.currentBlockQueue).hasSize(1);
        }

        /// Verifies the full pre-EMB temporary-archive path end-to-end: blocks 0–9 (all below
        /// firstRegularGroupStart=10) are routed to a temp archive, which is then consolidated
        /// into the final `0.tar` by [ConsolidationTask].
        ///
        /// The last block of group 0 (block 9) triggers [CloudStorageArchivePlugin#closeActiveTempSegment],
        /// uploading a `.tmp` archive.  Block 10 (firstRegularGroupStart) then drives the production
        /// handleVerification path: it queues [ConsolidationTask] for [0,9] and starts a regular
        /// [BlockUploadTask] for [10,19].  Both tasks are run together in a single executeSerially call.
        @Test
        @DisplayName("EMB=5: pre-EMB blocks 0–9 are consolidated into the final 0.tar via temp archive path")
        void preEmbBlocksConsolidatedIntoFinalTar() throws Exception {
            drainRecovery();
            // Blocks 0–8 stream into the active temp archive queue for group 0.
            sendVerifications(TestBlockBuilder.generateBlocksInRange(0, 8));
            assertThat(plugin.tempGroupActiveQueues).containsKey(0L);

            // Block 9 (last in group [0,9]) triggers closeActiveTempSegment → SEGMENT_END in queue.
            sendVerification(TestBlockBuilder.generateBlocksInRange(9, 9).getFirst());
            assertThat(plugin.tempGroupActiveQueues).doesNotContainKey(0L);

            // Run TempArchiveUploadTask → uploads 0.tmp + 0.meta.
            pluginExecutor.executeSerially();

            // Block 10 (firstRegularGroupStart) drives the production handleVerification path,
            // queuing ConsolidationTask for [0,9] and starting a regular BlockUploadTask for [10,19].
            // Complete group [10,19] so the BlockUploadTask does not hang in executeSerially.
            sendVerification(TestBlockBuilder.generateBlocksInRange(10, 10).getFirst());
            sendVerifications(TestBlockBuilder.generateBlocksInRange(11, 19));
            pluginExecutor.executeSerially(); // runs ConsolidationTask for [0,9] then BlockUploadTask for [10,19]

            assertTrue(getAllObjects().contains("0000/0000/0000/0000/0.tar"), "consolidated tar should be uploaded");
            assertThat(getAllObjects()).noneMatch(k -> k.startsWith("tmp/"));
        }

        /// Verifies the dual-recovery path where [StartupRecoveryTask] returns both a non-empty
        /// [RecoveryResult#tempArchives] list (pre-EMB blocks stored as temp archives before the
        /// restart) and a non-negative [RecoveryResult#currentGroupStart] (a hanging multipart
        /// upload for the first EMB-aligned group).
        ///
        /// [completeRecoveryIfReady] must:
        ///   1. Rebuild [tempArchiveTracker] from the recovered temp archives and call
        ///      [checkGroupCoverage] so that a fully-covered group is queued for consolidation.
        ///   2. Resume the regular [BlockUploadTask] for the in-progress group.
        ///   3. Call [tryReplayStash] only after the tracker and group state are set up, so that
        ///      stashed blocks are routed to the correct destination without interleaving errors.
        ///
        /// S3 state planted before recovery:
        ///   - Temp archive covering all of group [0, 9] (pre-EMB; firstRegularGroupStart = 10).
        ///   - Hanging multipart upload for group [10, 19] with tar entries for blocks 10–12.
        ///
        /// Blocks 12 (boundary block of the resumed group) and 20 (future group) are stashed
        /// during recovery.  Block 13 is the trigger that fires completeRecoveryIfReady.
        ///
        /// Expected state after block 13 is processed:
        ///   - tempArchiveTracker holds the entry for blocks 0–9.
        ///   - pendingConsolidations holds group 0 (checkGroupCoverage detected full coverage).
        ///   - currentGroupStart = 10; currentUploadFuture is non-null (BlockUploadTask resumed).
        ///   - Block 12 was drained from stash → upload queue (not stranded in pending).
        ///   - Block 20 was routed to tempGroupActiveQueues for group 20 (not left in stash).
        @Test
        @DisplayName(
                "Recovery with temp archives + in-progress regular group: tracker rebuilt, consolidation queued, stash replayed without interleaving")
        void recoveryWithTempArchivesAndInProgressRegularGroupRebuildsTrackerAndReplaysStash() throws Exception {
            final long groupSize = Math.powExact(10, GROUPING_LEVEL); // 10
            final CloudStorageArchiveConfig cfg = makeConfig();

            try (S3Client s3 = openS3Client(cfg)) {
                // Temp archive covering all of group [0, 9] (pre-EMB).
                s3.uploadTextFile(
                        TempArchiveKey.formatTar(0, cfg.objectKeyPrefix()),
                        cfg.storageClass().name(),
                        "dummy-tar");
                s3.uploadTextFile(
                        TempArchiveKey.formatMeta(0, cfg.objectKeyPrefix()),
                        cfg.storageClass().name(),
                        String.valueOf(groupSize - 1)); // lastBlock = 9

                // Hanging multipart upload for group [10, 19] with tar entries for blocks 10–12.
                final String regularKey = ArchiveKey.format(groupSize, GROUPING_LEVEL, cfg.objectKeyPrefix());
                final String uploadId =
                        s3.createMultipartUpload(regularKey, cfg.storageClass().name(), "application/x-tar");
                final List<TestBlock> partialBlocks =
                        TestBlockBuilder.generateBlocksInRange((int) groupSize, (int) groupSize + 2);
                s3.multipartUploadPart(regularKey, uploadId, 1, buildTarBytes(partialBlocks, groupSize));
                // deliberately NOT completing — simulate crash mid-upload
            }

            // Blocks 12 and 20 arrive while the recovery task is queued; they are stashed.
            sendVerification(TestBlockBuilder.generateBlocksInRange(12, 12).getFirst());
            sendVerification(TestBlockBuilder.generateBlocksInRange(20, 20).getFirst());
            assertThat(plugin.blocksStash).containsKeys(12L, 20L);

            // Run the startup recovery task against the planted S3 state.
            pluginExecutor.executeSerially();

            // Block 13 triggers completeRecoveryIfReady().
            sendVerification(TestBlockBuilder.generateBlocksInRange(13, 13).getFirst());

            // 1. tempArchiveTracker rebuilt from the recovered temp archive for blocks 0–9.
            assertThat(plugin.tempArchiveTracker).containsKey(0L);
            assertThat(plugin.tempArchiveTracker.get(0L).lastBlock()).isEqualTo(groupSize - 1);

            // 2. checkGroupCoverage detected full coverage of group [0, 9]; queued for consolidation.
            assertThat(plugin.pendingConsolidations).containsKey(0L);

            // 3. Regular BlockUploadTask resumed for group [10, 19].
            assertThat(plugin.currentGroupStart).isEqualTo(groupSize);
            assertThat(plugin.currentUploadFuture).isNotNull();

            // 4. Stashed block 12 (boundary block, nextBlockToQueue=12 after resume) was replayed
            //    to currentGroupPending and drained to the upload queue; block 13 (the trigger)
            //    followed immediately — both are now in the queue and pending is empty.
            assertThat(plugin.currentGroupPending).isEmpty();
            assertThat(plugin.currentBlockQueue).hasSize(2);

            // 5. Stashed block 20 (future group) was routed to routeToTempArchive and is waiting
            //    in its temp archive queue — not stranded in blocksStash.
            assertThat(plugin.blocksStash).isEmpty();
            assertThat(plugin.tempGroupActiveQueues).containsKey(groupSize * 2);
            assertThat(plugin.tempGroupActiveQueues.get(groupSize * 2)).hasSize(1);
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

        private static byte[] buildTarBytes(List<TestBlock> blocks, long startBlockNum) throws IOException {
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

    /// Tests for the EMB == 0 path (the production default): [CloudStorageArchivePlugin#firstRegularGroupStart]
    /// must equal 0 and block 0 must enter the regular upload pipeline rather than the temp archive path.
    @Nested
    @DisplayName("EMB=0 Tests")
    final class EmbZeroTests
            extends PluginTestBase<CloudStorageArchivePlugin, BlockingExecutor, ScheduledBlockingExecutor> {

        private static final int GROUPING_LEVEL = 1; // groupSize = 10^1 = 10

        EmbZeroTests() {
            super(
                    new BlockingExecutor(new LinkedBlockingQueue<>()),
                    new ScheduledBlockingExecutor(new LinkedBlockingQueue<>()));
            final Map<String, String> config = new HashMap<>(pluginConfig(GROUPING_LEVEL, 10));
            config.put("block.node.earliestManagedBlock", "0");
            start(new CloudStorageArchivePlugin(), new SimpleInMemoryHistoricalBlockFacility(), config);
            testThreadPoolManager.executor().executeSerially();
        }

        /// Verifies that [CloudStorageArchivePlugin#firstRegularGroupStart] is 0 when EMB is 0,
        /// so all groups starting from block 0 use the regular upload pipeline.
        @Test
        @DisplayName("EMB=0 produces firstRegularGroupStart=0")
        void embZeroYieldsFirstRegularGroupStart0() {
            assertThat(plugin.firstRegularGroupStart).isEqualTo(0L);
        }

        /// Verifies that block 0 (at firstRegularGroupStart=0) starts the regular upload pipeline
        /// and is not routed to the temp archive path.
        @Test
        @DisplayName("EMB=0: block 0 starts the regular upload pipeline, not the temp archive path")
        void blockZeroStartsRegularPipeline() {
            blockMessaging.sendBlockVerification(new VerificationNotification(
                    true,
                    null,
                    0L,
                    Bytes.EMPTY,
                    TestBlockBuilder.generateBlocksInRange(0, 0).getFirst().blockUnparsed(),
                    BlockSource.PUBLISHER));
            assertThat(plugin.currentUploadFuture).isNotNull();
            assertThat(plugin.tempGroupActiveQueues).doesNotContainKey(0L);
        }
    }
}
