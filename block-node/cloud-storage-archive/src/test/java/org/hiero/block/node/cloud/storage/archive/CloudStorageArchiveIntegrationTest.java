// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.cloud.storage.archive;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.hedera.bucky.S3Client;
import com.hedera.pbj.runtime.OneOf;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.config.api.ConfigurationBuilder;
import java.io.BufferedReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.locks.LockSupport;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.app.fixtures.async.TestThreadPoolManager;
import org.hiero.block.node.app.fixtures.blocks.TestBlock;
import org.hiero.block.node.app.fixtures.blocks.TestBlockBuilder;
import org.hiero.block.node.app.fixtures.plugintest.SimpleInMemoryHistoricalBlockFacility;
import org.hiero.block.node.app.fixtures.plugintest.TestBlockMessagingFacility;
import org.hiero.block.node.app.fixtures.plugintest.TestHealthFacility;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.ServiceLoaderFunction;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import org.hiero.metrics.core.MetricRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// One-shot integration test for [CloudStorageArchivePlugin] against real S3-compatible cloud storage.
///
/// In order to run it, create a properties file at `~/.hedera/cloud-archive-integration-test.properties`:
///
/// ```properties
/// cloud.storage.archive.endpointUrl  = https://s3.eu-north-1.amazonaws.com
/// cloud.storage.archive.regionName   = eu-north-1
/// cloud.storage.archive.accessKey    = ...
/// cloud.storage.archive.secretKey    = ...
/// cloud.storage.archive.bucketName   = my-integration-test-bucket
/// ```
///
/// The test overrides `groupingLevel` (set to 1, i.e. 10 blocks per group) and `partSizeMb`
/// (set to 5, the minimum non-final S3 multipart part size) for speed.
///
/// If the properties file is absent the test is **skipped** (not failed).
@Disabled // remove this if you want to run the test
@DisplayName("Cloud Storage Archive – One-shot Integration Test")
class CloudStorageArchiveIntegrationTest {

    private static final String DEFAULT_PROPS_PATH =
            System.getProperty("user.home") + "/.hedera/cloud-archive-integration-test.properties";
    private static final int GROUPING_LEVEL = 1; // 10^1 = 10 blocks per group
    private static final int PART_SIZE_MB = 5; // minimum non-final S3 multipart part size
    private static final int AWAIT_TIMEOUT_SECONDS = 60;

    private Map<String, String> cfg;

    // All plugin contexts started during the test — stopped in @AfterEach even on failure.
    private final List<PluginContext> activeContexts = new ArrayList<>();

    /// Bundles everything created for one plugin instance.
    private record PluginContext(
            CloudStorageArchivePlugin plugin,
            TestBlockMessagingFacility messaging,
            ExecutorService executor,
            ScheduledExecutorService scheduled) {}

    @BeforeEach
    void setUp() throws Exception {
        final Path propsPath = Path.of(DEFAULT_PROPS_PATH);
        assumeTrue(
                Files.exists(propsPath),
                "Integration-test config not found at '"
                        + DEFAULT_PROPS_PATH
                        + "'. Please create it before running the test.");

        final Properties props = new Properties();
        try (final BufferedReader reader = Files.newBufferedReader(propsPath)) {
            props.load(reader);
        }

        cfg = new HashMap<>();
        for (final String name : props.stringPropertyNames()) {
            cfg.put(name, props.getProperty(name));
        }
        // Override for speed: small groups and minimum part size
        cfg.put("cloud.storage.archive.groupingLevel", String.valueOf(GROUPING_LEVEL));
        cfg.put("cloud.storage.archive.partSizeMb", String.valueOf(PART_SIZE_MB));

        assertThat(cfg.get("cloud.storage.archive.bucketName"))
                .as("bucketName must be set in properties file")
                .isNotBlank();

        clearBucket();
    }

    @AfterEach
    void tearDown() {
        for (final PluginContext pc : activeContexts) {
            try (ExecutorService executorService = pc.executor();
                    ScheduledExecutorService scheduledExecutorService = pc.scheduled()) {
                pc.plugin().stop();
                executorService.shutdown();
                scheduledExecutorService.shutdown();
            } catch (final Exception ignored) {
            }
        }
        activeContexts.clear();
        try {
            clearBucket();
        } catch (final Exception ignored) {
        }
    }

    @Test
    @DisplayName("One-shot: out-of-order blocks, cross-group stash, stop/restart, and crash recovery")
    void oneShotIntegrationScenario() throws Exception {
        final int groupSize = Math.powExact(10, GROUPING_LEVEL); // 10 blocks

        // Phase 1: Blocks 0–9 arrive shuffled → group 0 tar uploaded

        final PluginContext p1 = startPlugin();
        awaitRecovery(p1.plugin()); // empty bucket -> fresh start (currentGroupStart = -1)

        final List<TestBlock> group0 = new ArrayList<>(TestBlockBuilder.generateBlocksInRange(0, groupSize - 1));
        Collections.shuffle(group0);
        sendVerifications(p1, group0);

        awaitTarInS3("0000/0000/0000/0000/0.tar");
        assertThat(p1.messaging().getSentPersistedNotifications().stream()
                        .anyMatch(n -> n.blockNumber() == groupSize - 1L && n.succeeded()))
                .as("persisted notification for last block of group 0")
                .isTrue();

        // Phase 2: Block 20 arrives early; cross-group stash + replay

        // Block 10 triggers a fresh upload task for group 1 [10, 20).
        sendVerification(
                p1, TestBlockBuilder.generateBlocksInRange(groupSize, groupSize).getFirst());
        // Block 20 is a group ahead; it lands in blocksStash and shouldn't corrupt group 1.
        sendVerification(
                p1,
                TestBlockBuilder.generateBlocksInRange((long) groupSize * 2, (long) groupSize * 2)
                        .getFirst());
        assertThat(p1.plugin().blocksStash).as("block 20 must be stashed").containsKey((long) groupSize * 2);

        // Blocks 11–19 complete group 1.
        sendVerifications(p1, TestBlockBuilder.generateBlocksInRange(groupSize + 1, (long) groupSize * 2 - 1));

        awaitTarInS3("0000/0000/0000/0000/1.tar");
        // Block 20 is still stashed — it will be lost when P1 stops.
        assertThat(p1.plugin().blocksStash)
                .as("block 20 remains stashed after group 1 upload")
                .isNotEmpty();

        // Phase 3: Normal stop and restart

        p1.plugin().stop();

        // Block 20 was stashed in P1's memory and is gone; it must be re-sent to P2.

        final PluginContext p2 = startPlugin();
        awaitRecovery(p2.plugin()); // finds groups 0+1 done -> recovery result: currentGroupStart=20

        // Send blocks 20–29
        sendVerifications(p2, TestBlockBuilder.generateBlocksInRange((long) groupSize * 2, (long) groupSize * 3 - 1));

        awaitTarInS3("0000/0000/0000/0000/2.tar");

        // Phase 4: Crash recovery — stop mid-group leaves a hanging multipart upload
        final long group3Start = (long) groupSize * 3; // 30

        // Blocks 30–34 start group 3's upload task but since test blocks are far smaller than the
        // 5 MB part-size threshold, no part has been flushed to S3 when P2 is stopped. The
        // multipart upload for group 3 is therefore left with 0 parts.
        sendVerifications(p2, TestBlockBuilder.generateBlocksInRange(group3Start, group3Start + 4));
        p2.plugin().stop();

        // P3 recovery: any 0-part hanging upload is aborted.
        // Last completed tar is group 2 → currentGroupStart=30.  Block 30 triggers
        // completeRecovery(); all 10 blocks 30–39 complete group 3.
        final PluginContext p3 = startPlugin();
        awaitRecovery(p3.plugin());
        sendVerifications(p3, TestBlockBuilder.generateBlocksInRange(group3Start, (long) groupSize * 4 - 1));

        awaitTarInS3("0000/0000/0000/0000/3.tar");

        // Phase 5: Crash recovery with actual uploaded parts
        // Large blocks (600 KB each): 9 blocks are around 5.4 MB, they exceed the 5 MB part threshold,
        // so BlockUploadTask flushes one part to S3 before we stop the plugin.
        final long group4Start = (long) groupSize * 4; // 40

        final PluginContext p4 = startPlugin();
        awaitRecovery(p4.plugin()); // finds group 3 done -> fresh start at group 4

        // Block 40 triggers completeRecovery(); blocks 40–48 accumulate until the 5 MB threshold
        // is crossed and one part is flushed to S3.
        for (long bn = group4Start; bn < group4Start + 9; bn++) {
            sendVerification(p4, generateLargeBlock(bn, 600 * 1024));
        }

        // Wait until S3 has at least one flushed part for group 4's key, then stop P4.
        // This leaves a 1-part hanging multipart upload in S3.
        awaitPartUploaded(ArchiveKey.format(group4Start, GROUPING_LEVEL, ""));
        p4.plugin().stop();

        // P5 recovers: finds the 1-part upload, completes it, creates a new upload
        final PluginContext p5 = startPlugin();
        awaitRecovery(p5.plugin());

        // Read nextBlockNumber BEFORE the first sendVerification triggers completeRecovery()
        // (which consumes and nulls out recoveryFuture).
        final long resumeFrom = p5.plugin().recoveredNextBlockNumber();

        // Send blocks from the resume point to the end of group 4.
        // Blocks before resumeFrom are already covered by the recovered part's trailingBytes.
        for (long bn = resumeFrom; bn < group4Start + groupSize; bn++) {
            sendVerification(p5, generateLargeBlock(bn, 600 * 1024));
        }

        awaitTarInS3(ArchiveKey.format(group4Start, GROUPING_LEVEL, ""));

        // Final assertion: all five groups are archived

        assertThat(getAllObjects())
                .as("all five group tars must be present in S3")
                .containsExactlyInAnyOrder(
                        "0000/0000/0000/0000/0.tar",
                        "0000/0000/0000/0000/1.tar",
                        "0000/0000/0000/0000/2.tar",
                        "0000/0000/0000/0000/3.tar",
                        "0000/0000/0000/0000/4.tar");
    }

    /// Creates, initializes, and starts a fresh plugin instance backed by a real virtual-thread
    /// executor.  The context is registered in [activeContexts] so [tearDown] can stop it even on
    /// test failure.
    private PluginContext startPlugin() {
        final ConfigurationBuilder builder =
                ConfigurationBuilder.create().withConfigDataType(CloudStorageArchiveConfig.class);
        cfg.forEach(builder::withValue);

        final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        final ScheduledExecutorService scheduled = Executors.newSingleThreadScheduledExecutor();
        final TestThreadPoolManager<ExecutorService, ScheduledExecutorService> tpm =
                new TestThreadPoolManager<>(executor, scheduled);
        final TestBlockMessagingFacility messaging = new TestBlockMessagingFacility();
        final BlockNodeContext ctx = new BlockNodeContext(
                builder.build(),
                MetricRegistry.builder().build(),
                new TestHealthFacility(),
                messaging,
                new SimpleInMemoryHistoricalBlockFacility(),
                null,
                new ServiceLoaderFunction(),
                tpm,
                null,
                null,
                null,
                new ArrayList<>(),
                new ArrayList<>());

        final CloudStorageArchivePlugin plugin = new CloudStorageArchivePlugin();
        plugin.init(ctx, null);
        plugin.start();

        final PluginContext pc = new PluginContext(plugin, messaging, executor, scheduled);
        activeContexts.add(pc);
        return pc;
    }

    /// Blocks until [CloudStorageArchivePlugin.isRecoveryComplete()] returns `true`, or fails
    /// after [AWAIT_TIMEOUT_SECONDS].
    private static void awaitRecovery(CloudStorageArchivePlugin plugin) {
        final Instant deadline = Instant.now().plusSeconds(AWAIT_TIMEOUT_SECONDS);
        while (Instant.now().isBefore(deadline)) {
            if (plugin.isRecoveryComplete()) {
                return;
            }
            LockSupport.parkNanos(500_000_000);
        }
        fail("Recovery task did not complete within " + AWAIT_TIMEOUT_SECONDS + " seconds");
    }

    /// Polls S3 until [key] appears as a committed object, or fails after [AWAIT_TIMEOUT_SECONDS].
    private void awaitTarInS3(final String key) throws Exception {
        final Instant deadline = Instant.now().plusSeconds(AWAIT_TIMEOUT_SECONDS);
        while (Instant.now().isBefore(deadline)) {
            if (getAllObjects().contains(key)) return;
            LockSupport.parkNanos(500_000_000);
        }
        fail("Tar not uploaded to S3 within " + AWAIT_TIMEOUT_SECONDS + "s: " + key);
    }

    private void sendVerification(final PluginContext pc, final TestBlock block) {
        pc.messaging()
                .sendBlockVerification(new VerificationNotification(
                        true, null, block.number(), Bytes.EMPTY, block.blockUnparsed(), BlockSource.PUBLISHER));
    }

    private void sendVerifications(final PluginContext pc, final List<TestBlock> blocks) {
        for (final TestBlock block : blocks) {
            sendVerification(pc, block);
        }
    }

    /// Lists all committed object keys in the test bucket.
    private Set<String> getAllObjects() throws Exception {
        final CloudStorageArchiveConfig config = buildConfig();
        final Set<String> keys = new HashSet<>();
        try (final S3Client s3 = openS3Client(config)) {
            String token = null;
            do {
                final S3Client.ListPage page = s3.listObjectsPage("", token, null, S3Client.LIST_OBJECTS_MAX);
                keys.addAll(page.keys());
                token = page.continuationToken();
            } while (token != null);
        }
        return keys;
    }

    /// Creates a single test block padded to [paddingBytes] using a [SIGNED_TRANSACTION] item.
    /// The block number seeds the RNG so the content is reproducible but distinct per block.
    private static TestBlock generateLargeBlock(long blockNumber, int paddingBytes) {
        final byte[] data = new byte[paddingBytes];
        new Random(blockNumber).nextBytes(data);
        final BlockItemUnparsed item = new BlockItemUnparsed(
                new OneOf<>(BlockItemUnparsed.ItemOneOfType.SIGNED_TRANSACTION, Bytes.wrap(data)));
        final BlockUnparsed block = BlockUnparsed.newBuilder()
                .blockItems(new BlockItemUnparsed[] {item})
                .build();
        return new TestBlock(blockNumber, block);
    }

    /// Polls S3 until at least one part has been uploaded to the hanging multipart upload for [key].
    /// Fails after [AWAIT_TIMEOUT_SECONDS] if no part appears.
    private void awaitPartUploaded(String key) throws Exception {
        final CloudStorageArchiveConfig config = buildConfig();
        final Instant deadline = Instant.now().plusSeconds(AWAIT_TIMEOUT_SECONDS);
        try (final S3Client s3 = openS3Client(config)) {
            while (Instant.now().isBefore(deadline)) {
                for (final Map.Entry<String, List<String>> entry :
                        s3.listMultipartUploads().entrySet()) {
                    if (entry.getKey().equals(key)) {
                        for (final String uploadId : entry.getValue()) {
                            if (!s3.listParts(key, uploadId).isEmpty()) {
                                return;
                            }
                        }
                    }
                }
                LockSupport.parkNanos(500_000_000);
            }
        }
        fail("No part uploaded for key " + key + " within " + AWAIT_TIMEOUT_SECONDS + "s");
    }

    /// Removes all objects and aborts all hanging multipart uploads from the test bucket.
    private void clearBucket() throws Exception {
        final CloudStorageArchiveConfig config = buildConfig();
        try (final S3Client s3 = openS3Client(config)) {
            String token = null;
            do {
                final S3Client.ListPage page = s3.listObjectsPage("", token, null, S3Client.LIST_OBJECTS_MAX);
                for (final String key : page.keys()) {
                    s3.deleteObject(key);
                }
                token = page.continuationToken();
            } while (token != null);
            for (final Map.Entry<String, List<String>> entry :
                    s3.listMultipartUploads().entrySet()) {
                for (final String uploadId : entry.getValue()) {
                    s3.abortMultipartUpload(entry.getKey(), uploadId);
                }
            }
        }
    }

    private CloudStorageArchiveConfig buildConfig() {
        final ConfigurationBuilder builder =
                ConfigurationBuilder.create().withConfigDataType(CloudStorageArchiveConfig.class);
        cfg.forEach(builder::withValue);
        return builder.build().getConfigData(CloudStorageArchiveConfig.class);
    }

    private static S3Client openS3Client(final CloudStorageArchiveConfig config) throws Exception {
        return new S3Client(
                config.regionName(), config.endpointUrl(), config.bucketName(), config.accessKey(), config.secretKey());
    }
}
