// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.cloud.storage.archive;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hedera.bucky.S3Client;
import com.hedera.bucky.S3ResponseException;
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
import java.util.logging.Logger;
import java.util.stream.Stream;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.app.fixtures.TestMetricsExporter;
import org.hiero.block.node.app.fixtures.async.BlockingExecutor;
import org.hiero.block.node.app.fixtures.async.ScheduledBlockingExecutor;
import org.hiero.block.node.app.fixtures.blocks.TestBlock;
import org.hiero.block.node.app.fixtures.blocks.TestBlockBuilder;
import org.hiero.block.node.app.fixtures.logging.TestLogHandler;
import org.hiero.block.node.app.fixtures.plugintest.PluginTestBase;
import org.hiero.block.node.app.fixtures.plugintest.RecordingServiceBuilder;
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
import org.junit.jupiter.api.AfterEach;
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

    /// Returns the config map with an explicit [maxConcurrentTempArchives] value.
    private Map<String, String> pluginConfig(int groupingLevel, int partSizeMb, int maxConcurrentTempArchives) {
        return Map.of(
                "cloud.storage.archive.groupingLevel",
                String.valueOf(groupingLevel),
                "cloud.storage.archive.partSizeMb",
                String.valueOf(partSizeMb),
                "cloud.storage.archive.maxConcurrentTempArchives",
                String.valueOf(maxConcurrentTempArchives),
                "cloud.storage.archive.endpointUrl",
                minioEndpoint,
                "cloud.storage.archive.regionName",
                "us-east-1",
                "cloud.storage.archive.bucketName",
                BUCKET_NAME,
                "cloud.storage.archive.accessKey",
                MINIO_USER,
                "cloud.storage.archive.secretKey",
                MINIO_PASSWORD);
    }

    /// A [CloudStorageArchivePlugin] subclass that overrides [newConsolidationTask] to return a
    /// failing [Callable] on the first call and delegates to the real [ConsolidationTask] on all
    /// subsequent calls.  Used by [ConsolidationRetryTests].
    private static class FailingFirstConsolidationPlugin extends CloudStorageArchivePlugin {
        private int consolidationCallCount = 0;

        @Override
        Callable<UploadResult> newConsolidationTask(List<TempArchiveEntry> entries, long groupStart, long groupSize) {
            if (consolidationCallCount++ == 0) {
                return () -> {
                    throw new IOException("simulated consolidation failure");
                };
            }
            return super.newConsolidationTask(entries, groupStart, groupSize);
        }
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

    /// A [CloudStorageArchivePlugin] subclass that replaces the real [TempArchiveUploadTask] with a
    /// configurable [Callable].  Used by [TempUploadFailureTests] to drive the temp-upload-failure
    /// code path without an active S3 upload.
    private static class FailingTempArchivePlugin extends CloudStorageArchivePlugin {
        private final Callable<TempArchiveEntry> tempBehavior;

        FailingTempArchivePlugin(Callable<TempArchiveEntry> tempBehavior) {
            this.tempBehavior = tempBehavior;
        }

        @Override
        Callable<TempArchiveEntry> newTempArchiveUploadTask(
                String s3Key, long firstBlock, BlockingQueue<BlockWithSource> queue) {
            return tempBehavior;
        }
    }

    /// A [CloudStorageArchivePlugin] subclass that replaces a resumed [TempArchiveUploadTask] with
    /// a configurable [Callable], mirroring [FailingTempArchivePlugin] for the resume path. Used to
    /// drive the resumed-temp-upload-failure code path without a live S3 upload actually running.
    private static class FailingResumedTempArchivePlugin extends CloudStorageArchivePlugin {
        private final Callable<TempArchiveEntry> tempBehavior;

        FailingResumedTempArchivePlugin(Callable<TempArchiveEntry> tempBehavior) {
            this.tempBehavior = tempBehavior;
        }

        @Override
        Callable<TempArchiveEntry> newResumedTempArchiveUploadTask(
                String s3Key,
                long firstBlock,
                BlockingQueue<BlockWithSource> queue,
                TempArchiveResumeState resumeState) {
            return tempBehavior;
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
            assertThatNullPointerException().isThrownBy(() -> plugin.init(null, new RecordingServiceBuilder()));
        }

        /// Verifies that [CloudStorageArchivePlugin#init] does not throw when [ServiceBuilder] is null.
        @Test
        @DisplayName("init does not throw when ServiceBuilder is null")
        void testInitNullServiceBuilder() {
            final Configuration configuration = ConfigurationBuilder.create()
                    .withConfigDataType(CloudStorageArchiveConfig.class)
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
            final ConfigurationBuilder builder =
                    ConfigurationBuilder.create().withConfigDataType(CloudStorageArchiveConfig.class);
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

        /// Verifies that [CloudStorageArchivePlugin#stop] does not throw and does not attempt to
        /// unregister a notification handler that was never registered (because `configValid` is
        /// `false` when a required configuration field is absent).
        @Test
        @DisplayName("stop() with invalid config does not throw and does not unregister handler")
        void testStopWithInvalidConfigDoesNotUnregister() {
            final ConfigurationBuilder builder =
                    ConfigurationBuilder.create().withConfigDataType(CloudStorageArchiveConfig.class);
            // endpointUrl present but other required fields absent — configValid stays false.
            builder.withValue("cloud.storage.archive.endpointUrl", "http://localhost:9000");
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
            final CloudStorageArchivePlugin plugin = new CloudStorageArchivePlugin();
            plugin.init(testContext, null);
            assertThat(messaging.getBlockNotificationHandlerCount()).isZero();
            assertThatNoException().isThrownBy(plugin::stop);
            assertThat(messaging.getBlockNotificationHandlerCount()).isZero();
        }

        /// Verifies that the plugin DOES register as a block notification handler when all
        /// required configuration fields are present.
        @Test
        @DisplayName("Plugin registers as notification handler when all required config fields are present")
        void testPluginRegisteredWhenAllConfigFieldsPresent() {
            final Configuration configuration = ConfigurationBuilder.create()
                    .withConfigDataType(CloudStorageArchiveConfig.class)
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
            // Block 0 is the only aligned-boundary block in this group, so it must arrive first to
            // start the regular task; only the remaining blocks are shuffled.
            final TestBlock firstBlock = blocks.remove(0);
            Collections.shuffle(blocks);
            sendVerification(firstBlock);
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
            // send block 0 (aligned) from group 0–9 to create the first task
            sendVerification(TestBlockBuilder.generateBlocksInRange(0, 0).getFirst());
            // Blocks 10-14 from group 10–19 arrive while group 0–9 is active. They aren't more
            // than one group ahead, so they fill gapBuffer (gapBufferSize=5) rather than bypassing
            // it directly; the fifth (14) triggers flushGapBufferToTemp, which starts the streaming
            // temp archive segment for group 10 since none of them fall in group 0's range.
            sendVerifications(TestBlockBuilder.generateBlocksInRange(groupSize, groupSize + 4));
            assertThat(plugin.tempGroupActiveQueues).containsKey((long) groupSize);
            // complete group 0–9 (blocks 1-9; block 0 was already sent)
            sendVerifications(TestBlockBuilder.generateBlocksInRange(1, groupSize - 1));
            // complete group 10–19: block 19 (last in group) triggers closeActiveTempSegment
            // and places SEGMENT_END in the queue so TempArchiveUploadTask[10,19] can finalise
            sendVerifications(TestBlockBuilder.generateBlocksInRange(groupSize + 5, groupSize * 2 - 1));
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
        /// Scenario: group 10–19 receives blocks 10–12 then 15–16 (5 total, skipping 13–14) as a
        /// single gapBuffer flush (gapBufferSize=5), then blocks 17–19 complete the group.
        ///   - Processed in ascending order, blocks 10-12 open the first segment [10, 12]; block 15
        ///     detects the forward gap (nextExpected=13), closes that segment with SEGMENT_END, and
        ///     starts a fresh one; block 16 continues the fresh segment.
        ///   - Blocks 17–19 complete the second segment; block 19 (last in group) closes it.
        ///   - Both tasks run in the same [executeSerially] call as [BlockUploadTask] for
        ///     group 0–9, since all queues are fully populated before execution begins.
        @Test
        @DisplayName("Gap in temp archive stream closes the current segment and starts a fresh one from the gap block")
        void testGapInTempArchiveStreamClosesSegmentAndStartsFresh() throws Exception {
            final int groupSize = (int) Math.pow(10, GROUPING_LEVEL); // 10
            // Start group 0–9 regular task with block 0 (aligned)
            sendVerification(TestBlockBuilder.generateBlocksInRange(0, 0).getFirst());

            // Blocks 10-12 then 15-16 (5 total, skipping 13-14) fill gapBuffer and flush together;
            // see the scenario note above for how this reproduces the gap-closes-and-reopens path.
            sendVerifications(TestBlockBuilder.generateBlocksInRange(groupSize, groupSize + 2));
            sendVerifications(TestBlockBuilder.generateBlocksInRange(groupSize + 5, groupSize + 6));

            assertThat(plugin.tempGroupActiveQueues).containsKey((long) groupSize); // second segment active
            assertThat(plugin.tempUploadFutures).containsKey((long) groupSize); // future for [10,12]
            assertThat(plugin.tempUploadFutures).containsKey((long) groupSize + 5); // future for [15,?]

            // Blocks 17–19: block 19 (last in group) closes the second segment
            sendVerifications(TestBlockBuilder.generateBlocksInRange(groupSize + 7, groupSize * 2 - 1));
            assertThat(plugin.tempGroupActiveQueues).doesNotContainKey((long) groupSize);

            // Complete group 0–9 (blocks 1-9; block 0 was already sent) and run all three tasks
            // in one call
            sendVerifications(TestBlockBuilder.generateBlocksInRange(1, groupSize - 1));
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

        /// Verifies that blocks arriving before the aligned first block of a group are treated as
        /// an ordinary gap: buffered in [CloudStorageArchivePlugin#gapBuffer] up to
        /// [CloudStorageArchiveConfig#gapBufferSize()], then flushed to a [TempArchiveUploadTask]
        /// once the buffer fills without the aligned block ever showing up -- instead of
        /// accumulating unbounded in [CloudStorageArchivePlugin#currentGroupPending] forever. No
        /// regular task is ever started here, since the aligned block (0) never arrives.
        @Test
        @DisplayName("Blocks before the aligned first block flush to temp archive once the gap buffer fills")
        void testBlocksBeforeAlignedFirstBlockFlushToTempArchiveWhenBufferFills() {
            // Blocks 1-5 arrive without block 0; gapBufferSize defaults to 5, so the fifth
            // buffered block (5) triggers the flush.
            sendVerifications(TestBlockBuilder.generateBlocksInRange(1, 5));

            assertThat(plugin.gapBuffer).isEmpty();
            assertThat(plugin.currentUploadFuture).isNull();
            assertThat(plugin.currentGroupPending).isEmpty();
            assertThat(plugin.tempGroupActiveQueues).containsKey(0L);
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

        /// Verifies the "before resume point" discard fix: a straggler for an earlier,
        /// already-passed group (temp-archived, missing 20-21) must fall through to
        /// [routeToTempArchive] instead of being discarded once group 30's regular task has moved
        /// `currentGroupStart` past it. Pre-fix, both stragglers would vanish silently and group
        /// 20-29 would never consolidate.
        @Test
        @DisplayName(
                "Straggler from an earlier, already-passed group is routed to temp archive instead of being discarded")
        void testStragglerFromEarlierGroupRoutesToTempArchive() throws Exception {
            final int groupSize = (int) Math.pow(10, GROUPING_LEVEL); // 10

            // Block 22 is two groups ahead of the (still-unstarted) regular pipeline -- starts a
            // temp archive for group 20 directly, missing blocks 20-21.
            sendVerification(TestBlockBuilder.generateBlocksInRange(groupSize * 2 + 2, groupSize * 2 + 2)
                    .getFirst());
            assertThat(plugin.tempGroupActiveQueues).containsKey((long) groupSize * 2);

            // Blocks 23-29 complete this segment; block 29 (last in group) closes it.
            sendVerifications(TestBlockBuilder.generateBlocksInRange(groupSize * 2 + 3, groupSize * 3 - 1));
            assertThat(plugin.tempGroupActiveQueues).doesNotContainKey((long) groupSize * 2);

            // Block 30 is the next "expected" block (lastHandedOffBlock was 29) and starts a
            // regular task for group 30-39, advancing currentGroupStart well past the stragglers.
            sendVerification(TestBlockBuilder.generateBlocksInRange(groupSize * 3, groupSize * 3)
                    .getFirst());
            assertThat(plugin.currentGroupStart).isEqualTo(groupSize * 3L);

            // Stragglers 20 and 21 arrive late (e.g. via backfill). Both are retrograde and below
            // nextBlockToQueue, but belong to a group entirely below currentGroupStart, so they
            // must fall through to routeToTempArchive rather than being discarded.
            sendVerifications(TestBlockBuilder.generateBlocksInRange(groupSize * 2, groupSize * 2 + 1));

            // Complete group 30-39 so its BlockUploadTask can finish without hanging executeSerially().
            sendVerifications(TestBlockBuilder.generateBlocksInRange(groupSize * 3 + 1, groupSize * 4 - 1));

            // Runs TempArchiveUploadTask[22,29], BlockUploadTask[30,39], TempArchiveUploadTask[20,21].
            pluginExecutor.executeSerially();

            // Drive the production drain path (checkAndDrainTempUploadResults + checkAndDrainConsolidations)
            // by sending the next group's first block; complete that group too to avoid a hang.
            sendVerification(TestBlockBuilder.generateBlocksInRange(groupSize * 4, groupSize * 4)
                    .getFirst());
            sendVerifications(TestBlockBuilder.generateBlocksInRange(groupSize * 4 + 1, groupSize * 5 - 1));
            pluginExecutor.executeSerially(); // runs ConsolidationTask[20,29] then BlockUploadTask[40,49]

            assertTrue(
                    getAllObjects().contains(ArchiveKey.format(groupSize * 2, GROUPING_LEVEL, "")),
                    "group 20-29 should be consolidated into a final tar despite the late stragglers");

            final List<PersistedNotification> notifications = blockMessaging.getSentPersistedNotifications();
            final List<Long> notifiedBlocks = notifications.stream()
                    .map(PersistedNotification::blockNumber)
                    .toList();
            assertThat(notifiedBlocks)
                    .as("straggler segment [20,21] must be persisted, not silently discarded")
                    .contains(groupSize * 2 + 1L);
            notifications.forEach(n -> assertTrue(n.succeeded(), "all persisted notifications should be successful"));
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

        /// Verifies that blocks arriving in a gap are buffered and drained into the regular task
        /// once the contiguous sequence catches up, leaving no temp archives created.
        @Test
        @DisplayName("Gap buffer drains to regular task when gap is filled")
        void testGapBufferDrainsToRegularTask() {
            // Blocks 0 and 1 are contiguous; regular task is started and both are queued.
            sendVerification(TestBlockBuilder.generateBlocksInRange(0, 0).getFirst());
            sendVerification(TestBlockBuilder.generateBlocksInRange(1, 1).getFirst());
            assertThat(plugin.currentUploadFuture).isNotNull();
            assertThat(plugin.currentBlockQueue).hasSize(2);

            // Block 5 arrives; gap at [2,3,4] is detected and block 5 enters gapBuffer.
            sendVerification(TestBlockBuilder.generateBlocksInRange(5, 5).getFirst());
            assertThat(plugin.gapBuffer).containsKey(5L);
            assertThat(plugin.tempGroupActiveQueues).isEmpty();

            // Filling the gap: blocks 2, 3, 4. Each triggers drainGapBufferToRegular();
            // block 4 finally lets block 5 drain too (gapBuffer.firstKey() == lastHandedOffBlock+1).
            sendVerifications(TestBlockBuilder.generateBlocksInRange(2, 4));
            assertThat(plugin.gapBuffer).isEmpty();
            assertThat(plugin.tempGroupActiveQueues).isEmpty();
            assertThat(plugin.currentBlockQueue).hasSize(6); // blocks 0-5
        }

        /// Verifies that when the gap buffer fills before the gap closes, in-group blocks are
        /// salvaged to [currentGroupPending] rather than being routed to a temp archive.
        /// This prevents the active [BlockUploadTask] from blocking on [BlockingQueue#take] for
        /// blocks that would otherwise be sent to a temp archive it doesn't read from.
        @Test
        @DisplayName("Gap buffer flush salvages in-group blocks to pending, not temp")
        void testGapBufferFlushSalvagesInGroupBlocksToPending() {
            // Block 0 anchors lastHandedOffBlock = 0 via the regular pipeline.
            sendVerification(TestBlockBuilder.generateBlocksInRange(0, 0).getFirst());
            assertThat(plugin.currentUploadFuture).isNotNull();

            // Blocks 5-9: gap at [1,4] is detected (gapBufferSize=5).
            // Block 9 (5th in buffer) triggers flushGapBufferToTemp().
            // All five blocks are within the active regular group [0,9] and >= nextBlockToQueue=1,
            // so they are salvaged to currentGroupPending instead of a temp archive.
            sendVerifications(TestBlockBuilder.generateBlocksInRange(5, 9));
            assertThat(plugin.gapBuffer).isEmpty();
            assertThat(plugin.tempGroupActiveQueues).isEmpty();
            assertThat(plugin.tempUploadFutures).isEmpty();
            assertThat(plugin.currentGroupPending).containsKeys(5L, 6L, 7L, 8L, 9L);
        }

        /// Verifies that when the gap buffer fills with blocks from a different group (outside the
        /// active regular task's range), those blocks are correctly flushed to a [TempArchiveUploadTask].
        @Test
        @DisplayName("Gap buffer flushes out-of-group blocks to temp archive when filled")
        void testGapBufferFlushesOutOfGroupBlocksToTemp() {
            final int groupSize = (int) Math.pow(10, GROUPING_LEVEL); // 10
            // Block 0 anchors lastHandedOffBlock = 0 via the regular pipeline.
            sendVerification(TestBlockBuilder.generateBlocksInRange(0, 0).getFirst());
            assertThat(plugin.currentUploadFuture).isNotNull();

            // Blocks 5 (in-group, goes to gapBuffer) + blocks 10-13 (next group, also to gapBuffer).
            // When block 13 (5th in buffer) arrives the buffer flushes:
            //   - block 5 is in [0,9] and >= nextBlockToQueue=1 → salvaged to currentGroupPending
            //   - blocks 10-13 are beyond currentGroupStart+groupSize=10 → routed to temp archive
            sendVerification(TestBlockBuilder.generateBlocksInRange(5, 5).getFirst());
            sendVerifications(TestBlockBuilder.generateBlocksInRange(groupSize, groupSize + 3));
            assertThat(plugin.gapBuffer).isEmpty();
            assertThat(plugin.currentGroupPending).containsKey(5L);
            assertThat(plugin.tempUploadFutures).containsKey((long) groupSize);
        }

        /// Verifies the aligned-boundary half of `freshStartSafeForRegular`: a fresh start whose
        /// first arriving block sits exactly on a group boundary goes directly to the regular
        /// pipeline without gap detection, even though the block is far outside group 0.  Block
        /// 500 is deliberately both aligned (500 % groupSize == 0) and beyond group 0
        /// (500 >= groupSize), so this is the only test that would catch a regression narrowing
        /// that clause to group 0 only.
        @Test
        @DisplayName("First arriving block on an aligned boundary starts regular task without gap buffering")
        void testFreshStartOnAlignedBoundaryStartsRegularTask() {
            // Block 500 is the first verified block; lastHandedOffBlock == -1 at this point.
            sendVerification(TestBlockBuilder.generateBlocksInRange(500, 500).getFirst());
            assertThat(plugin.currentUploadFuture).isNotNull();
            assertThat(plugin.currentGroupStart).isEqualTo(500L);
            assertThat(plugin.gapBuffer).isEmpty();
            assertThat(plugin.tempGroupActiveQueues).isEmpty();
        }

        /// Verifies that a non-aligned first arrival is treated like any other gap -- buffered in
        /// [gapBuffer] rather than immediately starting a regular task -- with no exception for the
        /// group containing block 0.  Whether this is an out-of-order genesis (predecessors coming)
        /// or a live deployment (predecessors gone forever) can't be known from the block number
        /// alone, so it can't be assumed safe.
        @Test
        @DisplayName("First arriving block that isn't aligned is buffered as a gap, not started as a regular task")
        void testFreshStartNonAlignedBuffersAsGap() {
            final int groupSize = (int) Math.pow(10, GROUPING_LEVEL); // 10
            // Block 15 is the first block ever seen: group 10-19, non-aligned.
            sendVerification(TestBlockBuilder.generateBlocksInRange(groupSize + 5, groupSize + 5)
                    .getFirst());

            assertThat(plugin.currentUploadFuture).isNull();
            assertThat(plugin.gapBuffer).containsKey((long) groupSize + 5);
            assertThat(plugin.currentGroupPending).isEmpty();
            assertThat(plugin.tempGroupActiveQueues).isEmpty();
        }

        /// Verifies the fresh-live-deployment scenario that motivated this behaviour: the very
        /// first block ever observed is deep inside a group whose earlier blocks will never
        /// arrive (they were produced before this node started archiving).  Because the block is
        /// more than one full group ahead of the placeholder group 0, the gap buffer is skipped
        /// entirely and the block is routed directly to a temp archive instead of stalling forever
        /// waiting for a regular task that can never drain.
        @Test
        @DisplayName("First arriving block far past group 0 routes directly to temp archive instead of stalling")
        void testFreshStartFarMidGroupRoutesToTempArchive() {
            final int groupSize = (int) Math.pow(10, GROUPING_LEVEL); // 10
            // Block 25 is the first block ever seen: group 20-29, more than one group ahead of
            // the placeholder group 0 -- the regular pipeline can never receive blocks 20-24.
            sendVerification(TestBlockBuilder.generateBlocksInRange(groupSize * 2 + 5, groupSize * 2 + 5)
                    .getFirst());

            assertThat(plugin.currentUploadFuture).isNull();
            assertThat(plugin.gapBuffer).isEmpty();
            assertThat(plugin.tempGroupActiveQueues).containsKey((long) groupSize * 2);
        }

        /// Verifies the fresh-live-deployment scenario when the first block is only a few
        /// positions into a group beyond group 0 (not far enough ahead to skip the buffer
        /// outright).  The block is buffered on the chance predecessors are merely out of order,
        /// but once the gap buffer fills without them ever arriving, [flushGapBufferToTemp]
        /// resolves it to a temp archive instead of leaving the regular pipeline stalled forever.
        @Test
        @DisplayName("First arriving block that never gets predecessors flushes to temp archive once buffer fills")
        void testFreshStartMidGroupFlushesToTempWhenPredecessorsNeverArrive() {
            final int groupSize = (int) Math.pow(10, GROUPING_LEVEL); // 10
            // Block 12 first, then 13-16 continue arriving contiguously but 10-11 never do.
            // gapBufferSize defaults to 5, so the fifth buffered block (16) triggers the flush.
            // (Stays short of block 19, the group's last block, so the segment remains open.)
            sendVerifications(TestBlockBuilder.generateBlocksInRange(groupSize + 2, groupSize + 6));

            assertThat(plugin.gapBuffer).isEmpty();
            assertThat(plugin.currentUploadFuture).isNull();
            assertThat(plugin.tempGroupActiveQueues).containsKey((long) groupSize);
        }

        /// Verifies that a [VerificationNotification] that is non-null and successful but carries
        /// a null block is silently ignored: the third branch of [logInvalidOrFailedNotification].
        @Test
        @DisplayName("Verification notification with null block is silently ignored")
        void testNullBlockVerificationIgnored() {
            assertThatNoException()
                    .isThrownBy(() -> plugin.handleVerification(
                            new VerificationNotification(true, null, 0, Bytes.EMPTY, null, BlockSource.PUBLISHER)));
            assertThat(plugin.currentUploadFuture).isNull();
        }

        /// Verifies that [tryStartNewUploadTask] skips creating a regular [BlockUploadTask] when
        /// [hasAnyTempDataForGroup] returns `true` for the target group, routing the block to a
        /// temp archive instead.
        ///
        /// A [TempArchiveEntry] is injected directly into [tempArchiveTracker] for group 10 to
        /// simulate data that arrived via a previous temp upload (without needing a real S3 upload).
        /// The entry covers blocks [10..14].
        ///
        /// Block 10 triggers `routeToCloudArchive(10)` → `tryStartNewUploadTask(10)` → skip (group
        /// has temp data) → `routeToTempArchive(10)`.  Because block 10 is already covered by the
        /// existing [10..14] archive entry, it is recognised as a duplicate and discarded -- no new
        /// streaming segment is opened and no regular [BlockUploadTask] is started.
        @Test
        @DisplayName("tryStartNewUploadTask skips when group already has temp archive data")
        void testNoRegularTaskWhenGroupHasTempData() throws Exception {
            final int groupSize = (int) Math.pow(10, GROUPING_LEVEL); // 10
            // Complete group 0–9 via the regular pipeline.
            sendVerifications(TestBlockBuilder.generateBlocksInRange(0, groupSize - 1));
            pluginExecutor.executeSerially(); // run BlockUploadTask[0,9]

            // Inject a finalised TempArchiveEntry for group 10 (uploadId=null means committed).
            plugin.tempArchiveTracker.put(
                    (long) groupSize, new TempArchiveEntry("tmp/00010.tar", groupSize, groupSize + 4, null));

            // Block 10: routeToCloudArchive -> tryStartNewUploadTask skips (temp data exists) ->
            // routeToTempArchive -> duplicate detected (covered by [10..14]) -> discarded.
            // No regular task and no new streaming segment are created.
            sendVerification(
                    TestBlockBuilder.generateBlocksInRange(groupSize, groupSize).getFirst());

            assertThat(plugin.currentUploadFuture).isNull();
            assertThat(plugin.tempGroupActiveQueues).doesNotContainKey((long) groupSize);
        }

        /// Verifies that when the gap buffer's contiguous drain reaches a block at the boundary
        /// of the next group, that block is routed to a [TempArchiveUploadTask] rather than the
        /// regular pending queue.
        ///
        /// Scenario: blocks 0–8 flow normally.  Block 10 (first of next group) triggers gap
        /// detection and enters [gapBuffer] because groupStart(10)==currentGroupStart+groupSize
        /// (exactly one group ahead, not two+).  Block 9 closes the gap: `drainGapBufferToRegular`
        /// drains block 9 to the regular task, then block 10 is contiguous at
        /// `lastHandedOffBlock+1=10` but outside `[0, 9]`, so it routes to temp archive.
        @Test
        @DisplayName("Gap buffer drains out-of-group block to temp archive when gap closes at group boundary")
        void testGapBufferDrainsOutOfGroupBlockToTempArchive() {
            final int groupSize = (int) Math.pow(10, GROUPING_LEVEL); // 10
            // Blocks 0–8 flow through the regular pipeline.
            sendVerifications(TestBlockBuilder.generateBlocksInRange(0, groupSize - 2));
            // Block 10: gap at [9]; groupStart(10)=10 is NOT more than one group ahead
            // (10 > 0+10=10 is false) so block 10 enters the gap buffer instead of going
            // directly to a temp archive.
            sendVerification(
                    TestBlockBuilder.generateBlocksInRange(groupSize, groupSize).getFirst());
            assertThat(plugin.gapBuffer).containsKey((long) groupSize);
            // Block 9 closes the gap: drainGapBufferToRegular drains block 9 to pending/queue,
            // then block 10 (now at lastHandedOffBlock+1=10) is outside [0,9] → temp archive.
            sendVerification(TestBlockBuilder.generateBlocksInRange(groupSize - 1, groupSize - 1)
                    .getFirst());
            assertThat(plugin.gapBuffer).isEmpty();
            assertThat(plugin.tempGroupActiveQueues).containsKey((long) groupSize);
            assertThat(plugin.tempGroupActiveQueues.get((long) groupSize)).hasSize(1);
        }

        /// Verifies that a block arriving in an abandoned gap -- a range that was skipped when a
        /// later block advanced [lastHandedOffBlock] past it -- is routed to a new temp segment
        /// rather than discarded.
        ///
        /// Scenario (groupSize = 10):
        ///   - Block 0 starts the regular task for group [0, 9].
        ///   - Blocks 10-14 fill [gapBuffer] (capacity 5) and are flushed to a temp segment
        ///     [10..14]; [lastHandedOffBlock] advances to 14.
        ///   - Block 19 (a gap in temp) closes segment [10..14] and immediately closes
        ///     a new one-block segment [19..19] (the group's last block); [lastHandedOffBlock] = 19.
        ///   - Blocks 15-18 now arrive retrograde.  Because none of them is covered by any
        ///     existing segment, each is treated as an abandoned gap block: a new segment [15..18]
        ///     is started at block 15 and closed via bridge detection when block 18 is queued
        ///     (because block 19 is already covered by segment [19..19]).
        @Test
        @DisplayName("Abandoned gap block starts a new temp segment instead of being discarded")
        void testAbandonedGapBlockStartsNewTempSegment() {
            final long gs = (long) Math.pow(10, GROUPING_LEVEL); // 10

            // Block 0 starts the regular task for group [0, 9].
            sendVerification(TestBlockBuilder.generateBlocksInRange(0, 0).getFirst());
            assertThat(plugin.currentUploadFuture).isNotNull();

            // Blocks 10-14 fill gapBuffer (capacity 5) and are flushed to temp segment [10..14].
            sendVerifications(TestBlockBuilder.generateBlocksInRange((int) gs, (int) gs + 4));
            assertThat(plugin.gapBuffer).isEmpty();
            assertThat(plugin.tempGroupActiveQueues).containsKey(gs);
            assertThat(plugin.tempUploadFutures).containsKey(gs);

            // Block 19: gap in temp. Closes [10..14], starts and immediately closes [19..19].
            sendVerification(TestBlockBuilder.generateBlocksInRange((int) gs * 2 - 1, (int) gs * 2 - 1)
                    .getFirst());
            assertThat(plugin.tempGroupActiveQueues).doesNotContainKey(gs);
            assertThat(plugin.tempUploadFutures).containsKeys(gs, gs + gs - 1);
            assertThat(plugin.lastHandedOffBlock).isEqualTo(gs * 2 - 1);

            // Block 15 arrives retrograde but uncovered -- must start a new segment, not be discarded.
            sendVerification(TestBlockBuilder.generateBlocksInRange((int) gs + 5, (int) gs + 5)
                    .getFirst());
            assertThat(plugin.tempGroupActiveQueues).containsKey(gs);

            // Blocks 16-17 continue the new segment.
            sendVerifications(TestBlockBuilder.generateBlocksInRange((int) gs + 6, (int) gs + 7));
            assertThat(plugin.tempGroupActiveQueues).containsKey(gs);

            // Block 18: bridge detection fires (block 19 already covered), closing segment [15..18].
            sendVerification(TestBlockBuilder.generateBlocksInRange((int) gs + 8, (int) gs + 8)
                    .getFirst());
            assertThat(plugin.tempGroupActiveQueues).doesNotContainKey(gs);
            // Three temp segments: [10..14] = gs, [19..19] = gs+9, [15..18] = gs+5.
            assertThat(plugin.tempUploadFutures).containsKeys(gs, gs + 5, gs + gs - 1);
        }

        /// End-to-end test that verifies blocks arriving severely out of order -- including several
        /// blocks that fill an abandoned gap -- are eventually consolidated into the group's final tar.
        ///
        /// The scenario is the same as [testAbandonedGapBlockStartsNewTempSegment] but continues
        /// through execution of all tasks and consolidation:
        ///   - Blocks 1-9 are reclaimed for the regular task [0, 9].
        ///   - [executeSerially] runs BlockUploadTask[0,9] and the three TempArchiveUploadTasks.
        ///   - Block 20 drives [checkAndDrainTempUploadResults] (all three futures done), detecting
        ///     full coverage of [10, 19] and queuing a [ConsolidationTask].  It also starts the
        ///     regular task for group [20, 29].
        ///   - Blocks 21-29 complete group [20, 29].
        ///   - [executeSerially] runs the [ConsolidationTask] and BlockUploadTask[20,29].
        ///   - Both "0000/0000/0000/0000/1.tar" and "0000/0000/0000/0000/2.tar" must exist in S3.
        @Test
        @DisplayName("Abandoned gap blocks are eventually consolidated into the group tar")
        void testAbandonedGapBlocksConsolidateAfterUpload() throws Exception {
            final int gs = (int) Math.pow(10, GROUPING_LEVEL); // 10

            // Block 0 starts the regular task for group [0, 9].
            sendVerification(TestBlockBuilder.generateBlocksInRange(0, 0).getFirst());
            // Blocks 10-14 fill gapBuffer and are flushed to temp segment [10..14].
            sendVerifications(TestBlockBuilder.generateBlocksInRange(gs, gs + 4));
            // Block 19: closes [10..14] and starts/closes [19..19].
            sendVerification(TestBlockBuilder.generateBlocksInRange(gs * 2 - 1, gs * 2 - 1)
                    .getFirst());
            // Blocks 15-18: fill abandoned gap; segment [15..18] is closed by bridge detection at 18.
            sendVerifications(TestBlockBuilder.generateBlocksInRange(gs + 5, gs + 8));
            assertThat(plugin.tempGroupActiveQueues).doesNotContainKey((long) gs);
            assertThat(plugin.tempUploadFutures).hasSize(3);
            // Blocks 1-9: reclaimed for the regular task.
            sendVerifications(TestBlockBuilder.generateBlocksInRange(1, gs - 1));

            // Run BlockUploadTask[0,9] + three TempArchiveUploadTasks.
            pluginExecutor.executeSerially();
            assertTrue(getAllObjects().contains("0000/0000/0000/0000/0.tar"), "group 0-9 tar should be uploaded");

            // Block 20 drives drain and consolidation check; starts regular task for [20, 29].
            sendVerification(
                    TestBlockBuilder.generateBlocksInRange(gs * 2, gs * 2).getFirst());
            sendVerifications(TestBlockBuilder.generateBlocksInRange(gs * 2 + 1, gs * 3 - 1));
            // Run ConsolidationTask[10,19] + BlockUploadTask[20,29].
            pluginExecutor.executeSerially();
            final Set<String> objects = getAllObjects();
            assertTrue(objects.contains("0000/0000/0000/0000/1.tar"), "consolidated group 10-19 tar should exist");
            assertTrue(objects.contains("0000/0000/0000/0000/2.tar"), "group 20-29 tar should be uploaded");
        }

        /// Verifies that a block that was already uploaded as part of a temp segment -- including
        /// after [tempGroupNextExpected] has been removed when the group's last block closed the
        /// segment -- is recognised as a true duplicate and discarded rather than starting a
        /// spurious new segment.
        @Test
        @DisplayName("Retrograde duplicate block in a completed temp segment is discarded")
        void testRetrogradeDuplicateInTempSegmentIsDiscarded() {
            final int gs = (int) Math.pow(10, GROUPING_LEVEL); // 10

            // Block 0 starts the regular task for group [0, 9].
            sendVerification(TestBlockBuilder.generateBlocksInRange(0, 0).getFirst());
            // Blocks 10-14 fill gapBuffer, creating segment [10..14].
            sendVerifications(TestBlockBuilder.generateBlocksInRange(gs, gs + 4));
            // Blocks 15-19 continue the same segment; block 19 (last in group) closes it.
            // tempGroupNextExpected[10] is removed after the close.
            sendVerifications(TestBlockBuilder.generateBlocksInRange(gs + 5, gs * 2 - 1));
            assertThat(plugin.tempGroupActiveQueues).doesNotContainKey((long) gs);
            assertThat(plugin.tempUploadFutures).containsOnlyKeys((long) gs);
            assertThat(plugin.tempSegmentLastBlock).containsEntry((long) gs, (long) (gs * 2 - 1));

            // Block 15 arrives again -- it is covered by the in-flight segment [10..19].
            // With tempGroupNextExpected[10] removed, the old code would default nextExpected to 15
            // (blockNumber) and bypass the duplicate check, creating a spurious second segment.
            // The fix checks coverage unconditionally, so the duplicate must be discarded.
            sendVerification(
                    TestBlockBuilder.generateBlocksInRange(gs + 5, gs + 5).getFirst());
            assertThat(plugin.tempUploadFutures).containsOnlyKeys((long) gs);
            assertThat(plugin.tempGroupActiveQueues).doesNotContainKey((long) gs);
            assertThat(getMetricValue(CloudStorageArchivePlugin.METRIC_CLOUD_ARCHIVE_DUPLICATE_BLOCKS_DISCARDED))
                    .isEqualTo(1L);
        }

        /// Verifies that a genuine in-group duplicate -- a block already drained to the active
        /// regular task's queue -- is discarded via [CloudStorageArchivePlugin]'s "before resume
        /// point" branch rather than being reclaimed or routed to temp archive, and that the
        /// duplicate-discard metric is incremented accordingly.
        @Test
        @DisplayName("Genuine in-group duplicate after drain increments the duplicate-discard metric")
        void testInGroupDuplicateAfterDrainIncrementsDuplicateMetric() {
            sendVerifications(TestBlockBuilder.generateBlocksInRange(0, 5));
            assertThat(plugin.currentUploadFuture).isNotNull();

            // Block 5 was already drained to the active task's queue; resending it is a genuine
            // duplicate, discarded because it is below nextBlockToQueue but still within the
            // active group.
            sendVerification(TestBlockBuilder.generateBlocksInRange(5, 5).getFirst());

            assertThat(getMetricValue(CloudStorageArchivePlugin.METRIC_CLOUD_ARCHIVE_DUPLICATE_BLOCKS_DISCARDED))
                    .isEqualTo(1L);
            assertThat(plugin.tempGroupActiveQueues).isEmpty();
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

        /// Verifies that [CloudStorageArchivePlugin#stop] cancels all in-flight
        /// [TempArchiveUploadTask] and [ConsolidationTask] futures and clears their tracking maps.
        ///
        /// Futures are injected directly into the plugin's maps to simulate active tasks without
        /// needing a real S3 upload.
        @Test
        @DisplayName("stop() cancels active temp archive and consolidation futures and empties their maps")
        void testStopCancelsTempAndConsolidationFutures() {
            final CompletableFuture<TempArchiveEntry> tempFuture = new CompletableFuture<>();
            final CompletableFuture<UploadResult> consolFuture = new CompletableFuture<>();
            plugin.tempUploadFutures.put(10L, tempFuture);
            plugin.consolidationFutures.put(0L, consolFuture);

            plugin.stop();

            assertThat(tempFuture.isCancelled()).isTrue();
            assertThat(consolFuture.isCancelled()).isTrue();
            assertThat(plugin.tempUploadFutures).isEmpty();
            assertThat(plugin.consolidationFutures).isEmpty();
            assertThat(blockMessaging.getBlockNotificationHandlerCount()).isZero();
        }

        private void sendVerification(TestBlock block) {
            blockMessaging.sendBlockVerification(new VerificationNotification(
                    true, null, block.number(), Bytes.EMPTY, block.blockUnparsed(), BlockSource.PUBLISHER));
        }
    }

    /// Tests that verify [CloudStorageArchivePlugin#stop] cancels a startup recovery task that has
    /// been submitted but not yet executed.  Unlike [StopTests], these tests do NOT drain the
    /// recovery task in a `@BeforeEach` hook so that `recoveryFuture` is still non-null when
    /// `stop()` is called.
    @Nested
    @DisplayName("Stop During Recovery Tests")
    final class StopDuringRecoveryTests
            extends PluginTestBase<CloudStorageArchivePlugin, BlockingExecutor, ScheduledBlockingExecutor> {

        private final BlockingExecutor pluginExecutor;

        StopDuringRecoveryTests() {
            super(
                    new BlockingExecutor(new LinkedBlockingQueue<>()),
                    new ScheduledBlockingExecutor(new LinkedBlockingQueue<>()));
            start(new CloudStorageArchivePlugin(), new SimpleInMemoryHistoricalBlockFacility(), pluginConfig());
            pluginExecutor = testThreadPoolManager.executor();
        }

        /// Verifies that [CloudStorageArchivePlugin#stop] cancels the queued startup recovery task
        /// and unregisters the notification handler before the task has had a chance to run.
        @Test
        @DisplayName("stop() cancels the in-flight startup recovery task and unregisters the handler")
        void testStopCancelsInFlightRecovery() {
            // Recovery task is queued in the BlockingExecutor but has not yet run.
            assertThat(pluginExecutor.getQueue()).hasSize(1);
            assertThat(blockMessaging.getBlockNotificationHandlerCount()).isOne();

            plugin.stop();

            assertThat(blockMessaging.getBlockNotificationHandlerCount()).isZero();
            // Drive the cancelled task to flush it from the executor queue.
            try {
                pluginExecutor.executeSerially();
            } catch (java.util.concurrent.CancellationException ignored) {
            }
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
            // Blocks 0–3 arrived as retrograde (blockNumber < nextBlockToQueue=4) and were
            // silently discarded before reaching currentGroupPending.
            assertThat(plugin.currentGroupPending).isEmpty();
            assertThat(getMetricValue(CloudStorageArchivePlugin.METRIC_CLOUD_ARCHIVE_DUPLICATE_BLOCKS_DISCARDED))
                    .isEqualTo(4L);
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
        /// `CloudStorageArchivePlugin.completeRecoveryIfReady()`, which populates the tracker and
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

        /// Verifies the full resume-from-hanging-temp-archive path end-to-end: a hanging temp
        /// archive multipart upload for a mid-group segment (blocks 5–9, the tail of group 0) with
        /// one durable part is planted before recovery runs; recovery must resume it (not abort it)
        /// into the same live-queue maps a fresh segment would use -- no redundant second segment
        /// for the group -- and the resumed task must complete the segment correctly once the
        /// remaining verified block arrives.
        @Test
        @DisplayName("Plugin resumes a hanging temp archive upload with durable parts and completes the segment")
        void resumeFromHangingTempArchiveCompletesSegment() throws Exception {
            final CloudStorageArchiveConfig config = makeConfig();
            final long segmentFirstBlock = 5L;
            final String tarKey = TempArchiveKey.formatTar(segmentFirstBlock, config.objectKeyPrefix());
            final List<TestBlock> segmentBlocks = TestBlockBuilder.generateBlocksInRange(5, 9);

            // Plant a hanging temp archive multipart upload with a durable part covering blocks
            // 5-9, simulating a prior run that crashed after flushing this part but before
            // SEGMENT_END arrived.
            try (S3Client s3 = openS3Client(config)) {
                final String uploadId =
                        s3.createMultipartUpload(tarKey, config.storageClass().name(), CONTENT_TYPE);
                s3.multipartUploadPart(tarKey, uploadId, 1, buildTarBytes(segmentBlocks, (int) segmentFirstBlock));
                // deliberately NOT completing — simulate a crash mid-upload
            }

            // Run startup recovery: finds the hanging temp upload, completes it to materialize a
            // readable object, and prepares a resumable state (nextBlockNumber=9, trailingBytes =
            // entries for blocks 5-8).
            pluginExecutor.executeSerially();

            // Block 9 is both the resume point and the last block of group 0 (groupSize=10); this
            // notification triggers completeRecoveryIfReady() (registering the resumed segment)
            // and then routeVerifiedBlock, which enqueues block 9 onto the resumed task's own
            // queue and immediately closes the segment.
            sendVerification(TestBlockBuilder.generateBlocksInRange(9, 9).getFirst());

            // The resumed segment is registered under its original firstBlock -- no redundant
            // second segment for the group.
            assertThat(plugin.tempUploadFutures).containsOnlyKeys(segmentFirstBlock);

            // Run the resumed TempArchiveUploadTask; its queue already has block 9 + SEGMENT_END.
            pluginExecutor.executeSerially();

            // Re-send block 9: recognized as a duplicate against the now-completed tracker entry,
            // which also drains the completed future into tempArchiveTracker as a side effect.
            sendVerification(TestBlockBuilder.generateBlocksInRange(9, 9).getFirst());

            assertThat(plugin.tempArchiveTracker).containsKey(segmentFirstBlock);
            final TempArchiveEntry entry = plugin.tempArchiveTracker.get(segmentFirstBlock);
            assertThat(entry.firstBlock()).isEqualTo(segmentFirstBlock);
            assertThat(entry.lastBlock()).isEqualTo(9L);
            assertThat(entry.uploadId()).isNull();

            try (S3Client s3 = openS3Client(config)) {
                assertThat(s3.listMultipartUploads()).doesNotContainKey(tarKey);
            }
            assertThat(getAllObjects())
                    .contains(tarKey, TempArchiveKey.formatMeta(segmentFirstBlock, config.objectKeyPrefix()));
        }

        /// Reproduces the race where a resumed temp-archive segment and a freshly-started regular
        /// [BlockUploadTask] both claim the same group. Group 0's tar completed cleanly before the
        /// crash, but the temp segment for group 1 (started while group 0 was still active, then
        /// left hanging by the crash) covers the very group that [StartupRecoveryTask] computes as
        /// `currentGroupStart` for Case 1 (`groupSize`). Pre-fix, `completeRecoveryIfReady()` started
        /// a second, conflicting [BlockUploadTask] for that group regardless of the resumed temp
        /// segment, so the resumed segment's queue never received another block or `SEGMENT_END` and
        /// its virtual thread blocked forever on `blockQueue.take()`.
        @Test
        @DisplayName("Resumed temp archive for the next group prevents a conflicting regular task from starting")
        void resumedTempArchiveForNextGroupPreventsConflictingRegularTask() throws Exception {
            final long groupSize = Math.powExact(10, GROUPING_LEVEL);
            final CloudStorageArchiveConfig config = makeConfig();

            // Group 0 completed cleanly before the crash.
            final String firstKey = ArchiveKey.format(0, GROUPING_LEVEL, "");
            final List<TestBlock> firstGroupBlocks = TestBlockBuilder.generateBlocksInRange(0, (int) groupSize - 1);
            try (S3Client s3 = openS3Client(config)) {
                final String uploadId =
                        s3.createMultipartUpload(firstKey, config.storageClass().name(), CONTENT_TYPE);
                final String etag = s3.multipartUploadPart(firstKey, uploadId, 1, buildTarBytes(firstGroupBlocks, 0));
                s3.completeMultipartUpload(firstKey, uploadId, List.of(etag));
            }

            // Group 1 (blocks 10-19) has a hanging temp archive segment for blocks 15-17, left
            // over from a gap-buffer flush while group 0 was still the active regular group.
            final long segmentFirstBlock = groupSize + 5;
            final String tarKey = TempArchiveKey.formatTar(segmentFirstBlock, config.objectKeyPrefix());
            final List<TestBlock> segmentBlocks =
                    TestBlockBuilder.generateBlocksInRange((int) segmentFirstBlock, (int) segmentFirstBlock + 2);
            try (S3Client s3 = openS3Client(config)) {
                final String uploadId =
                        s3.createMultipartUpload(tarKey, config.storageClass().name(), CONTENT_TYPE);
                s3.multipartUploadPart(tarKey, uploadId, 1, buildTarBytes(segmentBlocks, (int) segmentFirstBlock));
                // deliberately NOT completing -- simulate a crash mid-upload
            }

            pluginExecutor.executeSerially();

            // Trigger completeRecoveryIfReady() with the next expected block for the resumed segment.
            sendVerification(
                    TestBlockBuilder.generateBlocksInRange((int) segmentFirstBlock + 3, (int) segmentFirstBlock + 3)
                            .getFirst());

            // The resumed segment still owns group 1's active queue.
            assertThat(plugin.tempUploadFutures).containsKey(segmentFirstBlock);
            assertThat(plugin.tempGroupActiveQueues).containsKey(groupSize);

            // No conflicting regular BlockUploadTask was started for the same group -- the resumed
            // segment is the sole owner, so the plugin must fall through to the temp-archive path
            // for any further blocks in this group instead of starting a competing regular task.
            assertThat(plugin.currentUploadFuture).isNull();
        }

        /// Dedicated test protecting the "never abort" design: once a hanging temp segment is
        /// resumed, blocks that were already durable in the pre-crash upload (below the resume
        /// point) must be recognized as duplicates via
        /// [CloudStorageArchivePlugin#isBlockCoveredByAnyTempSegment] and discarded -- not
        /// re-queued to the resumed task (which only expects blocks from the resume point onward
        /// and would otherwise stall forever waiting for a block nobody will send) or
        /// double-counted -- even while the resumed task is still in flight, not yet completed.
        @Test
        @DisplayName(
                "Redelivered blocks below the resume point are discarded as duplicates while the segment is in flight")
        void redeliveredBlocksBelowResumePointAreDiscardedWhileSegmentInFlight() throws Exception {
            final CloudStorageArchiveConfig config = makeConfig();
            final long segmentFirstBlock = 5L;
            final String tarKey = TempArchiveKey.formatTar(segmentFirstBlock, config.objectKeyPrefix());
            final List<TestBlock> segmentBlocks = TestBlockBuilder.generateBlocksInRange(5, 9);

            try (S3Client s3 = openS3Client(config)) {
                final String uploadId =
                        s3.createMultipartUpload(tarKey, config.storageClass().name(), CONTENT_TYPE);
                s3.multipartUploadPart(tarKey, uploadId, 1, buildTarBytes(segmentBlocks, (int) segmentFirstBlock));
                // deliberately NOT completing — simulate a crash mid-upload
            }

            pluginExecutor.executeSerially();

            // Trigger completeRecoveryIfReady() with an unrelated, far-ahead block so the resumed
            // segment is registered without closing it (unlike the full round-trip test above).
            sendVerification(TestBlockBuilder.generateBlocksInRange(50, 50).getFirst());

            // The resumed segment is registered and its task is in flight (not yet run/completed).
            assertThat(plugin.tempUploadFutures).containsKey(segmentFirstBlock);
            assertThat(plugin.tempUploadFutures.get(segmentFirstBlock).isDone()).isFalse();

            final long duplicatesBefore =
                    getMetricValue(CloudStorageArchivePlugin.METRIC_CLOUD_ARCHIVE_DUPLICATE_BLOCKS_DISCARDED);

            // Redeliver block 6 — already durable in the old (pre-crash) part, below nextBlockNumber=9.
            sendVerification(TestBlockBuilder.generateBlocksInRange(6, 6).getFirst());

            // Discarded as a duplicate — not queued to the resumed task, not double-counted.
            assertThat(getMetricValue(CloudStorageArchivePlugin.METRIC_CLOUD_ARCHIVE_DUPLICATE_BLOCKS_DISCARDED))
                    .isEqualTo(duplicatesBefore + 1);
            assertThat(plugin.tempUploadFutures.get(segmentFirstBlock).isDone()).isFalse();
        }

        /// Verifies that two hanging temp archive uploads for the SAME group are both resumed and
        /// both complete, instead of the second [Map#put] in [CloudStorageArchivePlugin
        /// #completeRecoveryIfReady] silently overwriting the first segment's queue reference in
        /// `tempGroupActiveQueues` and orphaning its virtual thread on `queue.take()` forever.
        ///
        /// Plants two hanging `.tmp` multipart uploads in the same group [0,9]: firstBlock=2
        /// (blocks 2-3, recoverable nextBlockNumber=3) and firstBlock=6 (blocks 6-7-8, recoverable
        /// nextBlockNumber=8). Segment 6 has the higher nextBlockNumber, so it becomes the "active"
        /// continuation that receives blocks 8-9; segment 2 must be seeded with SEGMENT_END so it
        /// still finalises on its own.
        @Test
        @DisplayName("Two hanging temp archives for the same group are both resumed and both complete")
        void twoResumableTempArchivesForSameGroupBothComplete() throws Exception {
            final CloudStorageArchiveConfig config = makeConfig();
            final long supersededFirstBlock = 2L;
            final long activeFirstBlock = 6L;
            final String supersededTarKey = TempArchiveKey.formatTar(supersededFirstBlock, config.objectKeyPrefix());
            final String activeTarKey = TempArchiveKey.formatTar(activeFirstBlock, config.objectKeyPrefix());

            try (S3Client s3 = openS3Client(config)) {
                final String supersededUploadId = s3.createMultipartUpload(
                        supersededTarKey, config.storageClass().name(), CONTENT_TYPE);
                s3.multipartUploadPart(
                        supersededTarKey,
                        supersededUploadId,
                        1,
                        buildTarBytes(TestBlockBuilder.generateBlocksInRange(2, 3), (int) supersededFirstBlock));
                // deliberately NOT completing — simulate a crash mid-upload

                final String activeUploadId = s3.createMultipartUpload(
                        activeTarKey, config.storageClass().name(), CONTENT_TYPE);
                s3.multipartUploadPart(
                        activeTarKey,
                        activeUploadId,
                        1,
                        buildTarBytes(TestBlockBuilder.generateBlocksInRange(6, 8), (int) activeFirstBlock));
                // deliberately NOT completing — simulate a crash mid-upload
            }

            // Recovery resolves both hanging uploads independently into two resumable entries.
            pluginExecutor.executeSerially();

            // Block 8 is the resume point for the active segment; this notification triggers
            // completeRecoveryIfReady() (registering both resumed segments) and then routes block 8
            // into the active segment's queue.
            sendVerification(TestBlockBuilder.generateBlocksInRange(8, 8).getFirst());
            assertThat(plugin.tempUploadFutures).containsOnlyKeys(supersededFirstBlock, activeFirstBlock);

            // Block 9 is the last block of the group; it also routes to the active segment and
            // closes it with SEGMENT_END.
            sendVerification(TestBlockBuilder.generateBlocksInRange(9, 9).getFirst());

            // Run both resumed tasks. Without the fix, the superseded segment's task would block
            // forever on queue.take() (bounded only by BlockingExecutor's 60s per-task timeout).
            pluginExecutor.executeSerially();

            // An unrelated notification drains the now-completed futures into tempArchiveTracker.
            sendVerification(TestBlockBuilder.generateBlocksInRange(50, 50).getFirst());

            assertThat(plugin.tempUploadFutures).doesNotContainKeys(supersededFirstBlock, activeFirstBlock);
            assertThat(plugin.tempArchiveTracker).containsKeys(supersededFirstBlock, activeFirstBlock);
            assertThat(plugin.tempArchiveTracker.get(supersededFirstBlock).lastBlock())
                    .isEqualTo(2L);
            assertThat(plugin.tempArchiveTracker.get(activeFirstBlock).lastBlock())
                    .isEqualTo(9L);

            try (S3Client s3 = openS3Client(config)) {
                assertThat(s3.listMultipartUploads()).doesNotContainKeys(supersededTarKey, activeTarKey);
            }
            assertThat(getAllObjects()).contains(supersededTarKey, activeTarKey);
        }

        /// Five hanging temp archives for five distinct groups are found resumable at startup, but
        /// the default `maxConcurrentTempArchives=4`. Only the first four (in S3-key / ascending
        /// firstBlock order) should be started immediately; the fifth must be deferred to
        /// [CloudStorageArchivePlugin#pendingResumedArchives] rather than pushing `tempUploadFutures`
        /// past the configured limit.
        @Test
        @DisplayName("Resumable archives beyond the concurrency limit are deferred, not started immediately")
        void excessResumableArchivesAreDeferred() throws Exception {
            final CloudStorageArchiveConfig config = makeConfig();
            for (final long firstBlock : List.of(20L, 40L, 60L, 80L, 100L)) {
                plantHangingTempArchive(config, firstBlock);
            }

            // Recovery resolves all five hanging uploads into resumable entries.
            pluginExecutor.executeSerially();

            // Trigger completeRecoveryIfReady() with an unrelated, far-ahead block.
            sendVerification(TestBlockBuilder.generateBlocksInRange(200, 200).getFirst());

            // Only four of the five winning resumed archives fit under the default cap of 4.
            assertThat(plugin.tempUploadFutures).containsOnlyKeys(20L, 40L, 60L, 80L);
            assertThat(plugin.tempGroupActiveQueues).containsOnlyKeys(20L, 40L, 60L, 80L);
            assertThat(plugin.pendingResumedArchives).hasSize(1);
            assertThat(plugin.pendingResumedArchives.peek().firstBlock()).isEqualTo(100L);
        }

        /// Once a slot frees, the deferred resumable archive for group 100 must be started by
        /// `CloudStorageArchivePlugin.drainPendingResumedArchives`. [BlockingExecutor#executeSerially]
        /// runs every queued task, not just one, so all four active segments (20, 40, 60, 80) are
        /// closed here before running it -- otherwise the still-open ones would block on
        /// `queue.take()` for up to their 60s per-task timeout.
        @Test
        @DisplayName("Deferred resumable archive is started once a temp-upload slot frees")
        void deferredResumableArchiveStartsWhenSlotFrees() throws Exception {
            final int groupSize = Math.powExact(10, GROUPING_LEVEL);
            final CloudStorageArchiveConfig config = makeConfig();
            for (final long firstBlock : List.of(20L, 40L, 60L, 80L, 100L)) {
                plantHangingTempArchive(config, firstBlock);
            }

            pluginExecutor.executeSerially();
            sendVerification(TestBlockBuilder.generateBlocksInRange(200, 200).getFirst());
            assertThat(plugin.pendingResumedArchives).hasSize(1);

            // Complete every active segment's group. Each segment was planted with 2 blocks
            // (firstBlock, firstBlock+1); recovery conservatively treats the last written tar entry
            // as unconfirmed and resumes from it, so the resume point is firstBlock+1, not +2 (see
            // resumeFromHangingUploadCompletesGroup, which resends the last uploaded block the same way).
            for (final long groupStart : List.of(20L, 40L, 60L, 80L)) {
                sendVerifications(
                        TestBlockBuilder.generateBlocksInRange((int) groupStart + 1, (int) groupStart + groupSize - 1));
            }
            assertThat(plugin.tempGroupActiveQueues).isEmpty();

            // Run all four now-closed TempArchiveUploadTasks to completion.
            pluginExecutor.executeSerially();

            // Drives checkAndDrainTempUploadResults (frees all four slots), then
            // drainPendingResumedArchives (starts the deferred firstBlock=100 archive), then
            // drainOverflowStash (block 200, stashed earlier while slots were full, starts its own
            // new segment with a slot to spare).
            sendVerification(TestBlockBuilder.generateBlocksInRange(201, 201).getFirst());

            assertThat(plugin.pendingResumedArchives).isEmpty();
            assertThat(plugin.tempUploadFutures).containsOnlyKeys(100L, 200L);
            assertThat(plugin.tempGroupActiveQueues).containsKeys(100L, 200L);
            assertThat(plugin.tempArchiveTracker).containsKeys(20L, 40L, 60L, 80L);
        }

        /// Plants a hanging (never-completed) multipart upload for a 2-block temp archive segment
        /// starting at `firstBlock`, simulating a crash mid-upload.
        private void plantHangingTempArchive(CloudStorageArchiveConfig config, long firstBlock) throws Exception {
            final String tarKey = TempArchiveKey.formatTar(firstBlock, config.objectKeyPrefix());
            final List<TestBlock> blocks =
                    TestBlockBuilder.generateBlocksInRange((int) firstBlock, (int) firstBlock + 1);
            try (S3Client s3 = openS3Client(config)) {
                final String uploadId =
                        s3.createMultipartUpload(tarKey, config.storageClass().name(), CONTENT_TYPE);
                s3.multipartUploadPart(tarKey, uploadId, 1, buildTarBytes(blocks, (int) firstBlock));
                // deliberately NOT completing — simulate a crash mid-upload
            }
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
    /// Tests that verify mid-run recovery is triggered when any upload task fails — whether a
    /// [BlockUploadTask] returns [UploadResult.FAILED], throws an exception, or a
    /// [TempArchiveUploadTask] throws.  Each test calls [start] directly so it can supply its own
    /// plugin variant; JUnit creates a fresh instance per test method, so state never leaks.
    @Nested
    @DisplayName("Mid-Run Recovery Tests")
    final class MidRunRecoveryTests
            extends PluginTestBase<CloudStorageArchivePlugin, BlockingExecutor, ScheduledBlockingExecutor> {

        /// Captures log records emitted by [CloudStorageArchivePlugin] so failure tests can assert
        /// the real exception was logged, not a causeless [IllegalStateException].
        private final TestLogHandler logHandler = new TestLogHandler();
        private Logger pluginLogger;

        MidRunRecoveryTests() {
            super(
                    new BlockingExecutor(new LinkedBlockingQueue<>()),
                    new ScheduledBlockingExecutor(new LinkedBlockingQueue<>()));
        }

        @BeforeEach
        void attachLogHandler() {
            pluginLogger = Logger.getLogger(CloudStorageArchivePlugin.class.getName());
            pluginLogger.addHandler(logHandler);
        }

        @AfterEach
        void detachLogHandler() {
            pluginLogger.removeHandler(logHandler);
        }

        /// Verifies that a task returning [UploadResult.FAILED] triggers recovery:
        /// the failed-tasks metric increments, blocks still in [currentGroupPending] are moved to
        /// [blocksStash], and the triggering block is also stashed so nothing is silently dropped.
        @Test
        @DisplayName("FAILED upload result stashes pending blocks and submits recovery")
        void failedResultTriggersMidRunRecovery() throws Exception {
            start(
                    new FailingUploadPlugin(() -> UploadResult.FAILED),
                    new SimpleInMemoryHistoricalBlockFacility(),
                    pluginConfig());
            final BlockingExecutor executor = testThreadPoolManager.executor();
            executor.executeSerially(); // drain startup recovery

            final List<TestBlock> blocks = TestBlockBuilder.generateBlocksInRange(0, 10);

            // Block 0 (aligned) starts the regular task and drains immediately. This is required
            // to reach FailingUploadPlugin's mocked failure: it only overrides newUploadTask (the
            // regular task), not newTempArchiveUploadTask, so without a regular task started here,
            // blocks 5-9 below would instead flow to the real, unmocked TempArchiveUploadTask.
            sendVerification(blocks.get(0));

            // Blocks 5-9 fill the gap buffer (gapBufferSize=5) and get salvaged to
            // currentGroupPending -- they're in-group and >= nextBlockToQueue(1) -- rather than
            // being drained, since block 1 (needed to drain them) never arrives.
            sendVerifications(blocks.subList(5, 10));

            // Run the fake upload task.  It returns FAILED immediately.
            executor.executeSerially();

            // Block 10 triggers checkCompletedUpload() -> FAILED -> triggerMidRunRecovery().
            // triggerMidRunRecovery() moves currentGroupPending (blocks 5-9) to blocksStash and
            // sets currentGroupStart to -1, so block 10 is stashed rather than added to pending.
            sendVerification(blocks.get(10));

            assertThat(plugin.currentUploadFuture).isNull();
            assertThat(plugin.currentGroupPending).isEmpty();
            assertThat(plugin.blocksStash).containsKeys(5L, 6L, 7L, 8L, 9L, 10L);
            assertThat(getMetricValue(CloudStorageArchivePlugin.METRIC_CLOUD_ARCHIVE_FAILED_TASKS))
                    .isEqualTo(1L);

            // Drive the recovery task - empty bucket results in a fresh start.
            executor.executeSerially();
            assertThat(plugin.isRecoveryComplete()).isTrue();
            assertThat(plugin.recoveredNextBlockNumber()).isEqualTo(0L);
        }

        /// Verifies that the guard in [triggerMidRunRecovery] prevents a duplicate
        /// [StartupRecoveryTask] from being submitted when a second upload failure is detected
        /// while recovery is already running.
        @Test
        @DisplayName("Second upload failure while recovery is in progress does not spawn a second recovery task")
        void secondUploadFailureWhileRecoveryInProgressIsIgnored() {
            start(
                    new FailingUploadPlugin(() -> UploadResult.FAILED),
                    new SimpleInMemoryHistoricalBlockFacility(),
                    pluginConfig());
            final BlockingExecutor executor = testThreadPoolManager.executor();
            executor.executeSerially(); // drain startup recovery

            final List<TestBlock> blocks = TestBlockBuilder.generateBlocksInRange(0, 2);

            // First failure through the production path:
            // block 0 arrives -> startNewUploadTask(0) -> fake task T1 queued; block 0 -> pending.
            sendVerification(blocks.get(0));
            // Run T1 — instant FAILED, no exception.
            executor.executeSerially();
            // block 1 arrives -> checkCompletedUpload() detects FAILED -> triggerMidRunRecovery()
            // -> block 0 stashed; recovery task T2 queued (queue size: 1, not yet run).
            sendVerification(blocks.get(1));

            assertThat(executor.getQueue()).hasSize(1);
            assertThat(plugin.isRecoveryComplete()).isFalse();

            // Second failure while recovery is queued: inject done-FAILED future and send block 2.
            // checkCompletedUpload() calls triggerMidRunRecovery() via the production path, but the
            // guard (recoveryFuture != null && !isDone()) makes it a no-op — no second task queued.
            plugin.currentUploadFuture = CompletableFuture.completedFuture(UploadResult.FAILED);
            sendVerification(blocks.get(2));

            assertThat(executor.getQueue()).hasSize(1);

            executor.executeSerially();
            assertThat(plugin.isRecoveryComplete()).isTrue();
        }

        /// Verifies that blocks stashed during mid-run recovery are replayed to temp archives when
        /// recovery completes with no in-progress regular group (currentGroupStart == -1).
        ///
        /// Without the fix, `CloudStorageArchivePlugin.tryReplayStash()` was only called inside the
        /// `if (currentGroupStart != -1)` branch of `CloudStorageArchivePlugin.completeRecoveryIfReady()`,
        /// so stash blocks were permanently stranded when recovery found no in-progress upload.
        @Test
        @DisplayName("Blocks stashed during recovery are replayed to temp archive when recovery finds no group")
        void blocksStashedDuringRecoveryReplayedToTempArchiveOnFreshStart() {
            start(
                    new FailingUploadPlugin(() -> UploadResult.FAILED),
                    new SimpleInMemoryHistoricalBlockFacility(),
                    pluginConfig());
            final BlockingExecutor executor = testThreadPoolManager.executor();
            executor.executeSerially(); // drain startup recovery

            final int groupSize = 10;

            // Block 0 starts the regular upload task for [0, 9].
            sendVerification(TestBlockBuilder.generateBlocksInRange(0, 0).getFirst());
            // Run the failing task — immediately returns FAILED.
            executor.executeSerially();

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
            executor.executeSerially();

            // Block 11 triggers completeRecoveryIfReady().  tryReplayStash() must be called even
            // though currentGroupStart==-1; block 10 must be routed to routeToTempArchive rather
            // than remaining stranded in the stash.
            sendVerification(TestBlockBuilder.generateBlocksInRange(groupSize + 1, groupSize + 1)
                    .getFirst());

            assertThat(plugin.blocksStash).doesNotContainKey((long) groupSize);
            assertThat(plugin.tempGroupActiveQueues).containsKey((long) groupSize);
            assertThat(plugin.tempGroupActiveQueues.get((long) groupSize)).hasSize(2);
        }

        /// Verifies that an exception thrown by the upload-task [Future] triggers recovery with the
        /// same state effects as the [UploadResult.FAILED] path.
        @Test
        @DisplayName("Exception from upload task stashes pending blocks and submits recovery")
        void exceptionFromUploadTaskTriggersMidRunRecovery() throws Exception {
            start(
                    new FailingUploadPlugin(() -> {
                        throw new IOException("simulated S3 failure");
                    }),
                    new SimpleInMemoryHistoricalBlockFacility(),
                    pluginConfig());
            final BlockingExecutor executor = testThreadPoolManager.executor();
            executor.executeSerially(); // drain startup recovery

            final List<TestBlock> blocks = TestBlockBuilder.generateBlocksInRange(0, 10);

            // Block 0 (aligned) starts the regular task and drains immediately. This is required
            // to reach FailingUploadPlugin's mocked failure: it only overrides newUploadTask (the
            // regular task), not newTempArchiveUploadTask, so without a regular task started here,
            // block 5 below would instead flow to the real, unmocked TempArchiveUploadTask.
            sendVerification(blocks.get(0));

            // Blocks 5-9 fill the gap buffer (gapBufferSize=5) and get salvaged to
            // currentGroupPending -- they're in-group and >= nextBlockToQueue(1) -- rather than
            // being drained, since block 1 (needed to drain them) never arrives.
            sendVerifications(blocks.subList(5, 10));

            // Run the fake task — it throws IOException, so BlockingExecutor wraps it as
            // RuntimeException.
            assertThatThrownBy(executor::executeSerially).isInstanceOf(RuntimeException.class);

            // Block 10 triggers handleVerification() -> checkCompletedUpload() calls
            // currentUploadFuture.get(), which throws ExecutionException, propagating up to
            // handleVerification()'s catch block. It logs the real cause via e.getCause(), then
            // calls triggerMidRunRecovery() (moves currentGroupPending blocks 5-9 to stash) and
            // manually stashes block 10 itself, since routeVerifiedBlock(10) is never reached.
            sendVerification(blocks.get(10));

            assertThat(plugin.currentUploadFuture).isNull();
            assertThat(plugin.currentGroupPending).isEmpty();
            assertThat(plugin.blocksStash).containsKeys(5L, 6L, 7L, 8L, 9L, 10L);
            assertThat(getMetricValue(CloudStorageArchivePlugin.METRIC_CLOUD_ARCHIVE_FAILED_TASKS))
                    .isEqualTo(1L);
            assertThat(logHandler.thrownExceptions())
                    .anySatisfy(
                            t -> assertThat(t).isInstanceOf(IOException.class).hasMessage("simulated S3 failure"));

            executor.executeSerially();
            assertThat(plugin.isRecoveryComplete()).isTrue();
            assertThat(plugin.recoveredNextBlockNumber()).isEqualTo(0L);
        }

        /// Verifies that when a [TempArchiveUploadTask] fails, [checkAndDrainTempUploadResults]
        /// calls [triggerMidRunRecovery] so that [tempArchiveTracker] is rebuilt from S3 and
        /// group coverage can be re-evaluated once recovery completes.
        ///
        /// Without the fix, the failed temp entry was silently dropped from [tempUploadFutures]
        /// and its blocks were permanently absent from [tempArchiveTracker], leaving the group
        /// coverage gap unresolvable until the next node restart.
        ///
        /// Scenario: blocks 0-9 fully populate the regular BlockUploadTask queue; blocks 10-19
        /// are contiguous but outside the active group so they go directly to a FailingTempArchive.
        /// Block 19 (last in group [10,19]) closes the segment.  Both tasks are in the queue;
        /// BlockUploadTask[0,9] runs first and completes, then FailingTempArchive[10,19] throws.
        @Test
        @DisplayName("Temp archive upload failure triggers mid-run recovery")
        void failedTempUploadTriggersMidRunRecovery() throws Exception {
            start(
                    new FailingTempArchivePlugin(() -> {
                        throw new IOException("simulated temp upload failure");
                    }),
                    new SimpleInMemoryHistoricalBlockFacility(),
                    pluginConfig(1, 10));
            final BlockingExecutor executor = testThreadPoolManager.executor();
            executor.executeSerially(); // drain startup recovery

            // Blocks 0-9 fill the regular BlockUploadTask queue for group [0,9].
            sendVerifications(TestBlockBuilder.generateBlocksInRange(0, 9));
            // Blocks 10-19 are contiguous (expected=10 after lhob=9) but outside group [0,9],
            // so each is routed directly via routeToTempArchive.
            // Block 19 (last in group [10,19]) closes the segment with SEGMENT_END.
            sendVerifications(TestBlockBuilder.generateBlocksInRange(10, 19));
            assertThat(plugin.tempUploadFutures).containsKey(10L);

            // executeSerially runs BlockUploadTask[0,9] (success) then FailingTempArchive[10,19]
            // (throws) — the exception from the failing temp task propagates as RuntimeException.
            assertThatThrownBy(executor::executeSerially).isInstanceOf(RuntimeException.class);

            // Block 20 triggers checkCompletedUpload() (success for [0,9]) and then
            // checkAndDrainTempUploadResults() -> failure -> triggerMidRunRecovery().
            sendVerification(TestBlockBuilder.generateBlocksInRange(20, 20).getFirst());

            assertThat(plugin.tempUploadFutures).isEmpty();
            assertThat(plugin.currentUploadFuture).isNull();
            // Block 20 was stashed because recovery is now active.
            assertThat(plugin.blocksStash).containsKey(20L);
            assertThat(getMetricValue(CloudStorageArchivePlugin.METRIC_CLOUD_ARCHIVE_FAILED_TASKS))
                    .isEqualTo(1L);
            assertThat(logHandler.thrownExceptions())
                    .anySatisfy(t ->
                            assertThat(t).isInstanceOf(IOException.class).hasMessage("simulated temp upload failure"));

            // Drive recovery and verify it completes.
            executor.executeSerially();
            assertThat(plugin.isRecoveryComplete()).isTrue();
        }

        /// Verifies that a failing RESUMED [TempArchiveUploadTask] triggers mid-run recovery via
        /// the same glue code as a fresh one, proving `newResumedTempArchiveUploadTask` is wired
        /// into [CloudStorageArchivePlugin#checkAndDrainTempUploadResults] identically to
        /// [CloudStorageArchivePlugin#newTempArchiveUploadTask].
        ///
        /// A hanging temp archive multipart upload for a mid-group segment (blocks 5-9) with one
        /// durable part is planted in S3 before the plugin starts, so recovery resumes it into
        /// [FailingResumedTempArchivePlugin]'s mocked failing behavior instead of a real upload.
        @Test
        @DisplayName("Resumed temp archive upload failure triggers mid-run recovery")
        void failedResumedTempUploadTriggersMidRunRecovery() throws Exception {
            final ConfigurationBuilder builder =
                    ConfigurationBuilder.create().withConfigDataType(CloudStorageArchiveConfig.class);
            pluginConfig(1, 10).forEach(builder::withValue);
            final CloudStorageArchiveConfig cfg = builder.build().getConfigData(CloudStorageArchiveConfig.class);

            final long segmentFirstBlock = 5L;
            final String tarKey = TempArchiveKey.formatTar(segmentFirstBlock, cfg.objectKeyPrefix());
            final List<TestBlock> segmentBlocks = TestBlockBuilder.generateBlocksInRange(5, 9);
            try (S3Client s3 = new S3Client(
                    cfg.regionName(), cfg.endpointUrl(), cfg.bucketName(), cfg.accessKey(), cfg.secretKey())) {
                final String uploadId =
                        s3.createMultipartUpload(tarKey, cfg.storageClass().name(), "application/x-tar");
                final byte[] tarBytes = RecoveryIntegrationTests.buildTarBytes(segmentBlocks, (int) segmentFirstBlock);
                s3.multipartUploadPart(tarKey, uploadId, 1, tarBytes);
                // deliberately NOT completing — simulate a crash mid-upload
            }

            start(
                    new FailingResumedTempArchivePlugin(() -> {
                        throw new IOException("simulated resumed temp upload failure");
                    }),
                    new SimpleInMemoryHistoricalBlockFacility(),
                    pluginConfig(1, 10));
            final BlockingExecutor executor = testThreadPoolManager.executor();
            executor.executeSerially(); // drain startup recovery -- prepares the resumable segment

            // Trigger completeRecoveryIfReady(), which registers the resumed segment via
            // newResumedTempArchiveUploadTask (the mocked failing behavior).
            sendVerification(TestBlockBuilder.generateBlocksInRange(50, 50).getFirst());
            assertThat(plugin.tempUploadFutures).containsKey(segmentFirstBlock);

            // Running the mocked task throws — the exception propagates as RuntimeException.
            assertThatThrownBy(executor::executeSerially).isInstanceOf(RuntimeException.class);

            // The next notification drains the failed future and triggers mid-run recovery.
            sendVerification(TestBlockBuilder.generateBlocksInRange(51, 51).getFirst());

            assertThat(plugin.tempUploadFutures).doesNotContainKey(segmentFirstBlock);
            assertThat(getMetricValue(CloudStorageArchivePlugin.METRIC_CLOUD_ARCHIVE_FAILED_TASKS))
                    .isEqualTo(1L);
        }

        /// Verifies that [triggerMidRunRecovery] clears [gapBuffer] and moves its entries to
        /// [blocksStash], so they are not stranded above the recovered [lastHandedOffBlock].
        ///
        /// Scenario:
        ///   - Block 0 starts the regular upload task (task queued, not yet run); lastHandedOffBlock = 0.
        ///   - Block 5 arrives while the task is still in-flight: expected=1, gap detected,
        ///     block 5 enters gapBuffer (checkCompletedUpload sees the task not yet done).
        ///   - The upload task runs and returns FAILED.
        ///   - Block 6 arrives: checkCompletedUpload() sees FAILED, calls triggerMidRunRecovery(),
        ///     which must move gapBuffer (block 5) to blocksStash and clear gapBuffer.
        ///     Block 6 is then stashed by routeVerifiedBlock() because recovery is active.
        ///
        /// Without the fix, gapBuffer would still hold block 5 after recovery.  After recovery
        /// resets lastHandedOffBlock (potentially much lower), block 5 would sit at a position
        /// far above lastHandedOffBlock+1, permanently interfering with gap detection.
        @Test
        @DisplayName("triggerMidRunRecovery moves gap-buffered blocks to stash and clears gapBuffer")
        void triggerMidRunRecoveryMovesGapBufferToStash() {
            start(
                    new FailingUploadPlugin(() -> UploadResult.FAILED),
                    new SimpleInMemoryHistoricalBlockFacility(),
                    pluginConfig());
            final BlockingExecutor executor = testThreadPoolManager.executor();
            executor.executeSerially(); // drain startup recovery

            // Block 0 starts the regular upload task; lastHandedOffBlock becomes 0.
            // The task is queued but has NOT run yet.
            sendVerification(TestBlockBuilder.generateBlocksInRange(0, 0).getFirst());
            assertThat(plugin.currentUploadFuture).isNotNull();

            // Block 5 arrives while the task is still in-flight: expected=1, gap detected.
            // checkCompletedUpload() finds the task not done yet, so block 5 enters gapBuffer.
            sendVerification(TestBlockBuilder.generateBlocksInRange(5, 5).getFirst());
            assertThat(plugin.gapBuffer).containsKey(5L);

            // Now run the failing task -- it returns FAILED.
            executor.executeSerially();

            // Block 6 arrives: checkCompletedUpload() sees FAILED -> triggerMidRunRecovery() is called.
            // The fix must move gapBuffer (block 5) to blocksStash and clear gapBuffer before recovery runs.
            // Block 6 is then stashed by routeVerifiedBlock() because recoveryFuture is now set.
            sendVerification(TestBlockBuilder.generateBlocksInRange(6, 6).getFirst());

            assertThat(plugin.gapBuffer).isEmpty();
            assertThat(plugin.blocksStash).containsKeys(5L, 6L);
        }

        /// Verifies that [triggerMidRunRecovery] stashes blocks from [currentGroupPending] when
        /// they were placed there by [flushGapBufferToTemp] but could not drain because the gap
        /// below [nextBlockToQueue] was never filled.
        ///
        /// Scenario:
        ///   - Block 0 starts the regular upload task; block 0 is drained immediately
        ///     ([nextBlockToQueue] advances to 1).
        ///   - Blocks 2-6 arrive while the task is in-flight: all are above [expected]=1,
        ///     so they accumulate in [gapBuffer].  When the fifth block (6) brings the buffer
        ///     to [CloudStorageArchiveConfig#gapBufferSize()] (default 5),
        ///     [flushGapBufferToTemp] fires and moves all five blocks into [currentGroupPending]
        ///     (each is within the active group's remaining range [1, 10)).  Block 1 never
        ///     arrives, so [drainPendingToQueue] cannot advance past the gap and
        ///     [currentGroupPending] retains all five blocks.
        ///   - The upload task runs and returns [UploadResult.FAILED].
        ///   - Block 7 triggers [checkCompletedUpload] -> FAILED -> [triggerMidRunRecovery],
        ///     which must move [currentGroupPending] (blocks 2-6) to [blocksStash] and clear it.
        ///     Block 7 is stashed by [routeVerifiedBlock] because recovery is now active.
        @Test
        @DisplayName("triggerMidRunRecovery moves currentGroupPending blocks to stash after gapBuffer flush")
        void triggerMidRunRecoveryMovesCurrentGroupPendingToStash() throws Exception {
            start(
                    new FailingUploadPlugin(() -> UploadResult.FAILED),
                    new SimpleInMemoryHistoricalBlockFacility(),
                    pluginConfig());
            final BlockingExecutor executor = testThreadPoolManager.executor();
            executor.executeSerially(); // drain startup recovery

            // Block 0 is aligned; starts the regular upload task and is drained immediately.
            sendVerification(TestBlockBuilder.generateBlocksInRange(0, 0).getFirst());
            assertThat(plugin.currentUploadFuture).isNotNull();

            // Blocks 2-6 create a gap (expected=1) and fill gapBuffer to capacity.
            // flushGapBufferToTemp() fires on block 6: all five blocks are within
            // [nextBlockToQueue=1, groupEnd=10) so they go to currentGroupPending.
            // Block 1 never arrives, so drainPendingToQueue() cannot drain any of them.
            sendVerifications(TestBlockBuilder.generateBlocksInRange(2, 6));
            assertThat(plugin.gapBuffer).isEmpty();
            assertThat(plugin.currentGroupPending).containsKeys(2L, 3L, 4L, 5L, 6L);

            // Run the failing task -- it returns FAILED immediately.
            executor.executeSerially();

            // Block 7 triggers checkCompletedUpload() -> FAILED -> triggerMidRunRecovery().
            // currentGroupPending (blocks 2-6) must be moved to blocksStash.
            // Block 7 is stashed because recoveryFuture is now set.
            sendVerification(TestBlockBuilder.generateBlocksInRange(7, 7).getFirst());

            assertThat(plugin.currentGroupPending).isEmpty();
            assertThat(plugin.gapBuffer).isEmpty();
            assertThat(plugin.blocksStash).containsKeys(2L, 3L, 4L, 5L, 6L, 7L);
            assertThat(getMetricValue(CloudStorageArchivePlugin.METRIC_CLOUD_ARCHIVE_FAILED_TASKS))
                    .isEqualTo(1L);

            // Drive recovery -- empty bucket results in a fresh start.
            executor.executeSerially();
            assertThat(plugin.isRecoveryComplete()).isTrue();
            assertThat(plugin.recoveredNextBlockNumber()).isEqualTo(0L);
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

        /// Captures log records emitted by [CloudStorageArchivePlugin] so the failed-recovery test
        /// can assert the real exception was logged, not a causeless [IllegalStateException].
        private final TestLogHandler logHandler = new TestLogHandler();
        private Logger pluginLogger;

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

        @BeforeEach
        void attachLogHandler() {
            pluginLogger = Logger.getLogger(CloudStorageArchivePlugin.class.getName());
            pluginLogger.addHandler(logHandler);
        }

        @AfterEach
        void detachLogHandler() {
            pluginLogger.removeHandler(logHandler);
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
            final ConfigurationBuilder builder =
                    ConfigurationBuilder.create().withConfigDataType(CloudStorageArchiveConfig.class);
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
            final TestBlock block = TestBlockBuilder.generateBlocksInRange(0, 0).getFirst();
            failingMessaging.sendBlockVerification(new VerificationNotification(
                    true, null, block.number(), Bytes.EMPTY, block.blockUnparsed(), BlockSource.PUBLISHER));
            assertThat(exporter.getMetricValue(CloudStorageArchivePlugin.METRIC_CLOUD_ARCHIVE_FAILED_TASKS.name()))
                    .isEqualTo(1L);
            assertThat(exporter.getMetricValue(CloudStorageArchivePlugin.METRIC_CLOUD_ARCHIVE_SUCCESSFUL_TASKS.name()))
                    .isZero();
            assertThat(logHandler.thrownExceptions()).anySatisfy(t -> {
                assertThat(t).isInstanceOf(S3ResponseException.class);
                assertThat(((S3ResponseException) t).getResponseStatusCode()).isEqualTo(403);
            });
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

    /// Integration tests that verify the plugin-level consolidation retry: when a [ConsolidationTask]
    /// fails, [checkAndDrainConsolidations] calls `CloudStorageArchivePlugin.checkGroupCoverage()` which
    /// detects that the group is still fully covered in [tempArchiveTracker], re-queues it in
    /// [pendingConsolidations], and immediately re-submits a new [ConsolidationTask].  On the retry
    /// the task succeeds, the final tar is committed, and the temporary objects are deleted.
    @Nested
    @DisplayName("Consolidation Retry Tests")
    final class ConsolidationRetryTests
            extends PluginTestBase<CloudStorageArchivePlugin, BlockingExecutor, ScheduledBlockingExecutor> {

        private static final int GROUPING_LEVEL = 1; // groupSize = 10^1 = 10
        private final BlockingExecutor pluginExecutor;

        /// Captures log records emitted by [CloudStorageArchivePlugin] so the retry test can assert
        /// the real exception was logged, not a causeless [IllegalStateException].
        private final TestLogHandler logHandler = new TestLogHandler();
        private Logger pluginLogger;

        ConsolidationRetryTests() {
            super(
                    new BlockingExecutor(new LinkedBlockingQueue<>()),
                    new ScheduledBlockingExecutor(new LinkedBlockingQueue<>()));
            start(
                    new FailingFirstConsolidationPlugin(),
                    new SimpleInMemoryHistoricalBlockFacility(),
                    pluginConfig(GROUPING_LEVEL, 10));
            pluginExecutor = testThreadPoolManager.executor();
        }

        @BeforeEach
        void attachLogHandler() {
            pluginLogger = Logger.getLogger(CloudStorageArchivePlugin.class.getName());
            pluginLogger.addHandler(logHandler);
        }

        @AfterEach
        void detachLogHandler() {
            pluginLogger.removeHandler(logHandler);
        }

        @BeforeEach
        void drainRecovery() {
            pluginExecutor.executeSerially();
        }

        @Test
        @DisplayName("Failed ConsolidationTask is detected, group is re-queued, and the retry produces the final tar")
        void failedConsolidationRetrySucceeds() throws Exception {
            final int groupSize = (int) Math.pow(10, GROUPING_LEVEL); // 10

            // Block 0 starts the regular BlockUploadTask for group [0,9].
            sendVerification(TestBlockBuilder.generateBlocksInRange(0, 0).getFirst());
            // Complete group [0,9] first so lastHandedOffBlock reaches 9 before out-of-group
            // blocks arrive; otherwise blocks 1–9 would be retrograde and discarded.
            sendVerifications(TestBlockBuilder.generateBlocksInRange(1, groupSize - 1));
            // Blocks 10–19 go to a TempArchiveUploadTask because they are outside the active group [0,9].
            // Block 19 (last in group [10,19]) closes the active temp segment.
            sendVerifications(TestBlockBuilder.generateBlocksInRange(groupSize, groupSize * 2 - 1));
            assertThat(plugin.tempGroupActiveQueues).doesNotContainKey((long) groupSize);

            // Run BlockUploadTask[0,9] and TempArchiveUploadTask[10,19].
            pluginExecutor.executeSerially();
            assertThat(getAllObjects()).contains("0000/0000/0000/0000/0.tar");

            // Block 20: checkAndDrainTempUploadResults picks up the TempArchiveEntry[10,19] future,
            // checkGroupCoverage detects full coverage, and checkAndDrainConsolidations immediately
            // submits the FIRST (failing) ConsolidationTask[10,19].  Block 20 also starts
            // BlockUploadTask[20,29]; blocks 21–29 are drained to its queue.
            sendVerification(TestBlockBuilder.generateBlocksInRange(groupSize * 2, groupSize * 2)
                    .getFirst());
            sendVerifications(TestBlockBuilder.generateBlocksInRange(groupSize * 2 + 1, groupSize * 3 - 1));

            // Run the failing ConsolidationTask[10,19]: it throws, so executeSerially re-wraps
            // the exception as RuntimeException and stops.  BlockUploadTask[20,29] remains queued.
            try {
                pluginExecutor.executeSerially();
            } catch (RuntimeException ignored) {
                // Expected: the first ConsolidationTask[10,19] threw.
            }
            assertThat(getAllObjects()).doesNotContain("0000/0000/0000/0000/1.tar");

            // Block 39 triggers checkAndDrainConsolidations: it detects the failed
            // ConsolidationTask[10,19] future, calls checkGroupCoverage (tempArchiveTracker still
            // holds the [10,19] entry), re-queues group 10 in pendingConsolidations, and immediately
            // re-submits the SECOND (real, successful) ConsolidationTask[10,19].
            // Block 39 itself (blockNumber=39 > expected=30) lands in the gap buffer (incidental
            // side effect of choosing block 39 as the trigger; not the behaviour under test).
            sendVerification(TestBlockBuilder.generateBlocksInRange(groupSize * 4 - 1, groupSize * 4 - 1)
                    .getFirst());
            assertThat(plugin.consolidationFutures).containsKey((long) groupSize);
            assertThat(logHandler.thrownExceptions()).anySatisfy(t -> assertThat(t)
                    .isInstanceOf(IOException.class)
                    .hasMessage("simulated consolidation failure"));

            // Run the remaining queued tasks: BlockUploadTask[20,29] (still queued from before)
            // and the retry ConsolidationTask[10,19].
            pluginExecutor.executeSerially();

            // The retry succeeded: the final tar for group [10,19] is now in S3.
            assertThat(getAllObjects()).contains("0000/0000/0000/0000/1.tar");
            // ConsolidationTask cleaned up the temporary objects for group [10,19].
            assertThat(getAllObjects()).doesNotContain(TempArchiveKey.formatTar(groupSize, ""));
            assertThat(getAllObjects()).doesNotContain(TempArchiveKey.formatMeta(groupSize, ""));
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

    /// Tests that verify the overflow stash ([CloudStorageArchivePlugin#tempOverflowStash]) used
    /// when `CloudStorageArchiveConfig.maxConcurrentTempArchives()` is reached.  Blocks that cannot
    /// start a new [TempArchiveUploadTask] because the concurrent limit is exhausted are queued
    /// in the overflow stash and drained by `CloudStorageArchivePlugin.drainOverflowStash()` as
    /// soon as an in-flight temp upload future completes.
    @Nested
    @DisplayName("Overflow Stash Tests")
    final class OverflowStashTests
            extends PluginTestBase<CloudStorageArchivePlugin, BlockingExecutor, ScheduledBlockingExecutor> {

        private static final int GROUPING_LEVEL = 1; // groupSize = 10

        private final BlockingExecutor pluginExecutor;

        OverflowStashTests() {
            super(
                    new BlockingExecutor(new LinkedBlockingQueue<>()),
                    new ScheduledBlockingExecutor(new LinkedBlockingQueue<>()));
            start(
                    new CloudStorageArchivePlugin(),
                    new SimpleInMemoryHistoricalBlockFacility(),
                    pluginConfig(GROUPING_LEVEL, 10, 1)); // maxConcurrentTempArchives=1
            pluginExecutor = testThreadPoolManager.executor();
        }

        @BeforeEach
        void drainRecovery() {
            pluginExecutor.executeSerially();
        }

        /// Verifies the full overflow-stash cycle end-to-end:
        ///
        ///  1. Block 20 (two groups ahead) starts [TempArchiveUploadTask] for group [20, 29];
        ///     `tempUploadFutures.size()` hits the limit of 1.
        ///  2. Block 30 (three groups ahead): no active queue for group 30 and limit already
        ///     reached → block 30 enters [CloudStorageArchivePlugin#tempOverflowStash].
        ///  3. After [TempArchiveUploadTask] for [20, 29] finishes and its future is drained from
        ///     `tempUploadFutures`, `CloudStorageArchivePlugin.drainOverflowStash() sees a free
        ///     slot and routes block 30 to a new [TempArchiveUploadTask] for group [30, 39].
        @Test
        @DisplayName("Overflow block is stashed when temp-archive limit is hit and drained when a slot frees")
        void testOverflowStashDrainsWhenSlotFrees() throws Exception {
            final int groupSize = (int) Math.pow(10, GROUPING_LEVEL); // 10

            // Block 0 starts the regular BlockUploadTask for group [0, 9].
            sendVerification(TestBlockBuilder.generateBlocksInRange(0, 0).getFirst());
            assertThat(plugin.currentUploadFuture).isNotNull();

            // Block 20 (two groups ahead) bypasses the gap buffer because
            // groupStart(20)=20 > currentGroupStart(0)+groupSize(10)=10.
            // A TempArchiveUploadTask for group [20, 29] is started immediately.
            // tempUploadFutures.size()=1 == maxConcurrentTempArchives(1): limit reached.
            sendVerification(TestBlockBuilder.generateBlocksInRange(groupSize * 2, groupSize * 2)
                    .getFirst());
            assertThat(plugin.tempUploadFutures).hasSize(1);

            // Block 30 (three groups ahead): no active queue for group 30 and limit exhausted
            // → block 30 goes to tempOverflowStash instead of starting a new temp task.
            sendVerification(TestBlockBuilder.generateBlocksInRange(groupSize * 3, groupSize * 3)
                    .getFirst());
            assertThat(plugin.tempOverflowStash).containsKey((long) groupSize * 3);
            assertThat(plugin.tempGroupActiveQueues).doesNotContainKey((long) groupSize * 3);

            // Blocks 21–29 complete the group [20, 29] temp segment.
            // Block 29 (last in group) triggers closeActiveTempSegment → SEGMENT_END in queue.
            sendVerifications(TestBlockBuilder.generateBlocksInRange(groupSize * 2 + 1, groupSize * 3 - 1));
            assertThat(plugin.tempGroupActiveQueues).doesNotContainKey((long) groupSize * 2);

            // Blocks 1–9 are retrograde (lastHandedOffBlock=29) but within the active regular
            // group [0, 9] and >= nextBlockToQueue(1), so they are reclaimed for BlockUploadTask.
            sendVerifications(TestBlockBuilder.generateBlocksInRange(1, groupSize - 1));

            // Run BlockUploadTask[0, 9] and TempArchiveUploadTask[20, 29].
            pluginExecutor.executeSerially();

            // Block 31 drives the drain cycle inside handleVerification:
            //   1. checkAndDrainTempUploadResults removes the completed future for [20, 29],
            //      freeing one slot.
            //   2. drainOverflowStash routes block 30 to a new TempArchiveUploadTask for
            //      group [30, 39].
            sendVerification(TestBlockBuilder.generateBlocksInRange(groupSize * 3 + 1, groupSize * 3 + 1)
                    .getFirst());
            assertThat(plugin.tempOverflowStash).isEmpty();
            assertThat(plugin.tempGroupActiveQueues).containsKey((long) groupSize * 3);
        }

        /// Verifies that [drainOverflowStash] drains all stashed blocks for a group even when the
        /// concurrent-archive limit is 1 and multiple consecutive blocks for the same group are waiting.
        ///
        /// With maxConcurrentTempArchives=1, group [20,29]'s task occupies the single slot.  All of
        /// blocks 30-39 enter [CloudStorageArchivePlugin#tempOverflowStash] while the slot is full.
        /// Once that slot is freed, the drain must continue past the block that starts the group
        /// [30,39] segment and deliver all remaining stash blocks to the already-active queue --
        /// without requiring an additional slot for each continuation block.
        @Test
        @DisplayName(
                "maxConcurrentTempArchives=1: all overflow blocks for a group drain including continuations into an active queue")
        void overflowStashDrainsCompletelyWhenGroupHasActiveQueue() throws Exception {
            final int groupSize = (int) Math.pow(10, GROUPING_LEVEL); // 10

            // Block 0 starts the regular BlockUploadTask for group [0, 9].
            sendVerification(TestBlockBuilder.generateBlocksInRange(0, 0).getFirst());
            assertThat(plugin.currentUploadFuture).isNotNull();

            // Block 20 (two groups ahead) starts TempArchiveUploadTask for group [20, 29].
            // tempUploadFutures.size()=1 == maxConcurrentTempArchives(1): limit reached.
            sendVerification(TestBlockBuilder.generateBlocksInRange(groupSize * 2, groupSize * 2)
                    .getFirst());
            assertThat(plugin.tempUploadFutures).hasSize(1);

            // Blocks 30-39: no active queue for group 30 and limit exhausted -> all 10 go to overflow.
            sendVerifications(TestBlockBuilder.generateBlocksInRange(groupSize * 3, groupSize * 4 - 1));
            assertThat(plugin.tempOverflowStash).hasSize(10);

            // Blocks 21-29 complete the group [20, 29] temp segment.
            // Block 29 (last in group) triggers closeActiveTempSegment -> SEGMENT_END in queue.
            sendVerifications(TestBlockBuilder.generateBlocksInRange(groupSize * 2 + 1, groupSize * 3 - 1));
            assertThat(plugin.tempGroupActiveQueues).doesNotContainKey((long) groupSize * 2);

            // Blocks 1-9 are retrograde (lastHandedOffBlock=29) but within the active regular
            // group [0, 9] and >= nextBlockToQueue(1), so they are reclaimed for BlockUploadTask.
            sendVerifications(TestBlockBuilder.generateBlocksInRange(1, groupSize - 1));

            // Run BlockUploadTask[0, 9] and TempArchiveUploadTask[20, 29].
            pluginExecutor.executeSerially();

            // Block 40 drives the drain cycle:
            //   1. checkAndDrainTempUploadResults frees the slot used by [20, 29].
            //   2. drainOverflowStash: block 30 starts a new TempArchiveUploadTask for [30, 39]
            //      (consuming the freed slot); blocks 31-39 continue into the same active queue
            //      without needing an additional slot; block 39 (last in group) closes the segment.
            sendVerification(TestBlockBuilder.generateBlocksInRange(groupSize * 4, groupSize * 4)
                    .getFirst());
            assertThat(plugin.tempOverflowStash).isEmpty();
            // Segment for [30, 39] was closed by block 39; queue removed.
            assertThat(plugin.tempGroupActiveQueues).doesNotContainKey((long) groupSize * 3);
            // The upload future for [30, 39] is in flight.
            assertThat(plugin.tempUploadFutures).containsKey((long) groupSize * 3);
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
