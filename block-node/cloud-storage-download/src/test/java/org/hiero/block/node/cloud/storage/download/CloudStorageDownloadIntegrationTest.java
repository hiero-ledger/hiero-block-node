// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.cloud.storage.download;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.hedera.bucky.S3Client;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.hapi.node.base.SemanticVersion;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.hiero.block.api.BlockRange;
import org.hiero.block.api.RosterEntry;
import org.hiero.block.api.TssData;
import org.hiero.block.api.TssRoster;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.app.fixtures.plugintest.PluginTestBase;
import org.hiero.block.node.app.fixtures.plugintest.SimpleInMemoryHistoricalBlockFacility;
import org.hiero.block.node.base.CompressionType;
import org.hiero.block.node.spi.blockmessaging.BackfilledBlockNotification;
import org.hiero.block.node.spi.blockmessaging.BlockNotificationHandler;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.PersistedNotification;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

/// Runs the plugin with its real bucket client against a RustFS container. The class is
/// skipped when Docker is not available. Each test uses its own bucket. The poll interval is
/// an hour, so the scheduled run never comes and each test drives the poll itself.
@Timeout(value = 120, unit = TimeUnit.SECONDS)
class CloudStorageDownloadIntegrationTest
        extends PluginTestBase<CloudStorageDownloadPlugin, ExecutorService, ScheduledThreadPoolExecutor> {

    private static final String RUSTFS_IMAGE = "rustfs/rustfs:1.0.4";
    private static final int S3_PORT = 9000;
    private static final String ACCESS_KEY = "download-test-access";
    private static final String SECRET_KEY = "download-test-secret";
    private static final String REGION = "us-east-1";
    private static final long THRESHOLD = 10L;
    private static final List<Long> NODES = List.of(3L, 4L, 5L);

    private static GenericContainer<?> rustfs;
    private static String endpoint;
    private static MinioClient minio;
    private static int bucketCount;

    /// Seeds the bucket of the current test.
    private S3Client seeder;

    private String bucket;
    private final Persistence persistence = new Persistence();

    @BeforeAll
    @SuppressWarnings({"resource", "HttpUrlsUsage"})
    static void startRustfs() {
        try {
            rustfs = new GenericContainer<>(RUSTFS_IMAGE)
                    .withEnv("RUSTFS_ACCESS_KEY", ACCESS_KEY)
                    .withEnv("RUSTFS_SECRET_KEY", SECRET_KEY)
                    .withExposedPorts(S3_PORT)
                    .waitingFor(Wait.forListeningPort());
            rustfs.start();
        } catch (final IllegalStateException e) {
            assumeTrue(false, "Docker not available, skipping the RustFS tests: " + e.getMessage());
        }
        endpoint = "http://" + rustfs.getHost() + ":" + rustfs.getMappedPort(S3_PORT);
        minio = MinioClient.builder()
                .endpoint(endpoint)
                .credentials(ACCESS_KEY, SECRET_KEY)
                .build();
    }

    @AfterAll
    static void stopRustfs() {
        if (rustfs != null) {
            rustfs.stop();
        }
    }

    CloudStorageDownloadIntegrationTest() {
        super(Executors.newSingleThreadExecutor(), new ScheduledThreadPoolExecutor(1));
    }

    @AfterEach
    void closeSeeder() {
        if (seeder != null) {
            seeder.close();
        }
    }

    /// Creates a new bucket for the current test, and the client that seeds it.
    private void newBucket() throws Exception {
        bucketCount++;
        bucket = "download-test-" + bucketCount;
        minio.makeBucket(MakeBucketArgs.builder().bucket(bucket).build());
        seeder = new S3Client(REGION, endpoint, bucket, ACCESS_KEY, SECRET_KEY);
    }

    /// Starts the plugin on the current bucket, with the three nodes in the roster and the given
    /// last stored block.
    private void startPlugin(final String prefix, final long lastStored) {
        final Map<String, String> config = new HashMap<>();
        config.put("cloud.storage.download.enabled", "true");
        config.put("cloud.storage.download.endpointUrl", endpoint);
        config.put("cloud.storage.download.regionName", REGION);
        config.put("cloud.storage.download.bucketName", bucket);
        config.put("cloud.storage.download.accessKey", ACCESS_KEY);
        config.put("cloud.storage.download.secretKey", SECRET_KEY);
        config.put("cloud.storage.download.objectKeyPrefix", prefix);
        config.put("cloud.storage.download.aheadThresholdBlocks", String.valueOf(THRESHOLD));
        config.put("cloud.storage.download.downloadWindow", "4");
        config.put("cloud.storage.download.pollIntervalMs", "3600000");
        start(new CloudStorageDownloadPlugin(), new SimpleInMemoryHistoricalBlockFacility(), config);
        blockMessaging.registerBlockNotificationHandler(persistence, false, "persistence");
        updateTssData(roster());
        if (lastStored >= 0) {
            replaceStoredBlocks(List.of(
                    BlockRange.newBuilder().rangeStart(0).rangeEnd(lastStored).build()));
        }
    }

    private static TssData roster() {
        final List<RosterEntry> entries = new ArrayList<>();
        for (final long nodeId : NODES) {
            entries.add(RosterEntry.newBuilder().nodeId(nodeId).build());
        }
        return TssData.newBuilder()
                .currentRoster(TssRoster.newBuilder().rosterEntries(entries).build())
                .build();
    }

    /// Uploads blocks `first` to `last` to the folder of every node. Each node's copy carries
    /// the node ID as the major software version, so a test can tell which folder a block came
    /// from.
    private void seedBlocks(final String prefix, final long first, final long last) throws Exception {
        for (final long nodeId : NODES) {
            for (long blockNumber = first; blockNumber <= last; blockNumber++) {
                upload(BlockKeys.blockKey(prefix, nodeId, blockNumber), blockFile(nodeId, blockNumber));
            }
        }
    }

    /// Uploads the marker of a block to the folder of every node.
    private void seedMarker(final String prefix, final long blockNumber) throws Exception {
        for (final long nodeId : NODES) {
            upload(BlockKeys.markerKey(prefix, nodeId, blockNumber), new byte[] {1});
        }
    }

    private void upload(final String key, final byte[] bytes) throws Exception {
        seeder.uploadFile(key, "STANDARD", List.of(bytes).iterator(), "application/octet-stream");
    }

    private static byte[] blockFile(final long nodeId, final long blockNumber) {
        final BlockHeader header = BlockHeader.newBuilder()
                .number(blockNumber)
                .softwareVersion(
                        SemanticVersion.newBuilder().major((int) nodeId).build())
                .build();
        final BlockUnparsed block = BlockUnparsed.newBuilder()
                .blockItems(BlockItemUnparsed.newBuilder()
                        .blockHeader(BlockHeader.PROTOBUF.toBytes(header))
                        .build())
                .build();
        return CompressionType.ZSTD.compress(
                BlockUnparsed.PROTOBUF.toBytes(block).toByteArray());
    }

    private static List<Long> blocks(final long first, final long last) {
        final List<Long> numbers = new ArrayList<>();
        for (long blockNumber = first; blockNumber <= last; blockNumber++) {
            numbers.add(blockNumber);
        }
        return numbers;
    }

    @Test
    @DisplayName("An existing object opens as a stream and a missing key gives null, with no exception")
    void existingAndMissingObjects() throws Exception {
        newBucket();
        upload("blocks/present", new byte[] {7, 8, 9});
        try (S3Client client = new S3Client(REGION, endpoint, bucket, ACCESS_KEY, SECRET_KEY)) {
            try (InputStream present = client.openObjectStream("blocks/present")) {
                assertThat(present).isNotNull();
                assertThat(present.readAllBytes()).containsExactly(7, 8, 9);
            }
            try (InputStream missing = client.openObjectStream("blocks/missing")) {
                assertThat(missing).isNull();
            }
        }
    }

    @Test
    @DisplayName("A bucket ahead by less than the threshold: nothing is downloaded or submitted after several polls")
    void bucketNotFarEnoughAhead() throws Exception {
        newBucket();
        seedBlocks("blocks", 5, 4 + THRESHOLD - 1);
        seedMarker("blocks", 4 + THRESHOLD - 1);
        startPlugin("blocks", 4);
        for (int i = 0; i < 3; i++) {
            plugin.poller().poll();
        }
        assertThat(persistence.submitted).isEmpty();
        assertThat(plugin.metricsHolder().blocksPulled().get()).isZero();
    }

    @Test
    @DisplayName("All three folders hold the range: every block comes from the folder the probe hit, in order")
    void rangeFromOneFolderInOrder() throws Exception {
        newBucket();
        seedBlocks("blocks", 5, 4 + THRESHOLD);
        seedMarker("blocks", 4 + THRESHOLD);
        startPlugin("blocks", 4);
        plugin.poller().poll();
        assertThat(persistence.submitted).containsExactlyElementsOf(blocks(5, 4 + THRESHOLD));
        assertThat(persistence.sourceNodes).hasSize(1);
        assertThat(NODES).containsAll(persistence.sourceNodes);
        assertThat(plugin.metricsHolder().blocksPulled().get()).isEqualTo(THRESHOLD);
    }

    @Test
    @DisplayName("An empty object key prefix puts the node folders at the bucket root")
    void emptyPrefix() throws Exception {
        newBucket();
        seedBlocks("", 0, THRESHOLD - 1);
        seedMarker("", THRESHOLD - 1);
        startPlugin("", -1);
        plugin.poller().poll();
        assertThat(persistence.submitted).containsExactlyElementsOf(blocks(0, THRESHOLD - 1));
    }

    @Test
    @DisplayName("A probe block without its marker is absent although its block file exists")
    void blockWithoutMarkerIsAbsent() throws Exception {
        newBucket();
        seedBlocks("blocks", 5, 4 + THRESHOLD);
        startPlugin("blocks", 4);
        plugin.poller().poll();
        assertThat(persistence.submitted).isEmpty();
    }

    /// Stands in for verification and persistence: records each submitted block and the node
    /// whose copy it is, and reports it persisted.
    private final class Persistence implements BlockNotificationHandler {
        private final List<Long> submitted = new CopyOnWriteArrayList<>();
        private final List<Long> sourceNodes = new CopyOnWriteArrayList<>();

        @Override
        public void handleBackfilled(final BackfilledBlockNotification notification) {
            submitted.add(notification.blockNumber());
            try {
                final BlockHeader header = BlockHeader.PROTOBUF.parse(
                        notification.block().blockItems().getFirst().blockHeaderOrThrow());
                final long nodeId = header.softwareVersionOrThrow().major();
                if (!sourceNodes.contains(nodeId)) {
                    sourceNodes.add(nodeId);
                }
            } catch (final Exception e) {
                throw new IllegalStateException(e);
            }
            blockMessaging.sendBlockPersisted(
                    new PersistedNotification(notification.blockNumber(), true, 0, BlockSource.CLOUD_STORAGE));
        }
    }
}
