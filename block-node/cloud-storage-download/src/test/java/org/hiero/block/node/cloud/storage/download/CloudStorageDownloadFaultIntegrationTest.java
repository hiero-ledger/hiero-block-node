// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.cloud.storage.download;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.hedera.bucky.S3Client;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.hapi.node.base.SemanticVersion;
import io.minio.BucketExistsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import org.hiero.block.api.BlockRange;
import org.hiero.block.api.RosterEntry;
import org.hiero.block.api.TssData;
import org.hiero.block.api.TssRoster;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.app.fixtures.logging.TestLogHandler;
import org.hiero.block.node.app.fixtures.plugintest.PluginTestBase;
import org.hiero.block.node.app.fixtures.plugintest.SimpleInMemoryHistoricalBlockFacility;
import org.hiero.block.node.base.CompressionType;
import org.hiero.block.node.cloud.storage.download.CloudStorageDownloadPlugin.MetricsHolder;
import org.hiero.block.node.spi.blockmessaging.BackfilledBlockNotification;
import org.hiero.block.node.spi.blockmessaging.BlockNotificationHandler;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.PersistedNotification;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;

/// Runs the plugin with its real bucket client against RustFS behind Toxiproxy, to stage bad
/// copies and storage faults. The plugin and the seeding clients reach RustFS only through the
/// proxy, whose port stays the same when RustFS restarts. The class is skipped when Docker is
/// not available. Each test uses its own bucket and drives the poll itself.
///
/// The roster has two nodes, 3 and 4, so the probe always looks in both folders; a test makes
/// node 3 the source by putting the probe block only in its folder.
@Timeout(value = 180, unit = TimeUnit.SECONDS)
class CloudStorageDownloadFaultIntegrationTest
        extends PluginTestBase<CloudStorageDownloadPlugin, ExecutorService, ScheduledThreadPoolExecutor> {

    private static final String RUSTFS_IMAGE = "rustfs/rustfs:1.0.4";
    private static final String TOXIPROXY_IMAGE = "ghcr.io/shopify/toxiproxy:2.12.0";
    private static final String RUSTFS_ALIAS = "rustfs";
    private static final int S3_PORT = 9000;
    private static final int TOXIPROXY_API_PORT = 8474;
    private static final int PROXY_PORT = 8666;
    private static final String PROXY = "rustfs";
    private static final String ACCESS_KEY = "download-test-access";
    private static final String SECRET_KEY = "download-test-secret";
    private static final String REGION = "us-east-1";
    private static final String PREFIX = "blocks";
    private static final long THRESHOLD = 10L;
    private static final long REQUEST_TIMEOUT_MS = 2_000L;
    private static final long SOURCE_NODE = 3L;
    private static final long OTHER_NODE = 4L;
    private static final String PROBE_FAILED = "Bucket probe for block";

    static {
        // the JDK reads this once, when the first HttpClient is built; bucky's S3Client sets it
        // too, but only when its class loads, which is after the Toxiproxy client below
        System.setProperty("jdk.httpclient.allowRestrictedHeaders", "Host,Content-Length");
    }

    private static final HttpClient HTTP = HttpClient.newHttpClient();

    private static Network network;
    private static GenericContainer<?> rustfs;
    private static GenericContainer<?> toxiproxy;
    private static String endpoint;
    private static String toxiproxyApi;
    private static MinioClient minio;
    private static int bucketCount;

    /// Seeds the bucket of the current test.
    private S3Client seeder;

    private String bucket;
    private final Persistence persistence = new Persistence();
    private final TestLogHandler logHandler = new TestLogHandler();
    private final Logger pollerLogger = Logger.getLogger(BucketPoller.class.getName());

    @BeforeAll
    @SuppressWarnings({"resource", "HttpUrlsUsage"})
    static void startContainers() throws Exception {
        try {
            network = Network.newNetwork();
            rustfs = new GenericContainer<>(RUSTFS_IMAGE)
                    .withNetwork(network)
                    .withNetworkAliases(RUSTFS_ALIAS)
                    .withEnv("RUSTFS_ACCESS_KEY", ACCESS_KEY)
                    .withEnv("RUSTFS_SECRET_KEY", SECRET_KEY)
                    .withExposedPorts(S3_PORT)
                    .waitingFor(Wait.forListeningPort());
            rustfs.start();
            toxiproxy = new GenericContainer<>(TOXIPROXY_IMAGE)
                    .withNetwork(network)
                    .withExposedPorts(TOXIPROXY_API_PORT, PROXY_PORT)
                    .waitingFor(Wait.forHttp("/version").forPort(TOXIPROXY_API_PORT));
            toxiproxy.start();
        } catch (final IllegalStateException e) {
            assumeTrue(false, "Docker not available, skipping the RustFS fault tests: " + e.getMessage());
        }
        toxiproxyApi = "http://" + toxiproxy.getHost() + ":" + toxiproxy.getMappedPort(TOXIPROXY_API_PORT);
        toxiproxy(
                "POST",
                "/proxies",
                "{\"name\":\"" + PROXY + "\",\"listen\":\"0.0.0.0:" + PROXY_PORT + "\",\"upstream\":\"" + RUSTFS_ALIAS
                        + ":" + S3_PORT + "\",\"enabled\":true}");
        endpoint = "http://" + toxiproxy.getHost() + ":" + toxiproxy.getMappedPort(PROXY_PORT);
        minio = MinioClient.builder()
                .endpoint(endpoint)
                .credentials(ACCESS_KEY, SECRET_KEY)
                .build();
    }

    @AfterAll
    static void stopContainers() {
        if (toxiproxy != null) {
            toxiproxy.stop();
        }
        if (rustfs != null) {
            rustfs.stop();
        }
        if (network != null) {
            network.close();
        }
    }

    CloudStorageDownloadFaultIntegrationTest() {
        super(Executors.newVirtualThreadPerTaskExecutor(), new ScheduledThreadPoolExecutor(1));
    }

    @BeforeEach
    void resetProxyAndWatchLogs() throws Exception {
        // enables the proxy and removes every toxic a failed test may have left behind
        toxiproxy("POST", "/reset", null);
        pollerLogger.addHandler(logHandler);
    }

    @AfterEach
    void closeSeederAndLogs() {
        pollerLogger.removeHandler(logHandler);
        if (seeder != null) {
            seeder.close();
        }
    }

    /// Sends one request to the Toxiproxy API.
    ///
    /// @param method the HTTP method
    /// @param path the API path
    /// @param json the request body, `null` for none
    private static void toxiproxy(final String method, final String path, final String json) throws Exception {
        final HttpRequest request = HttpRequest.newBuilder(URI.create(toxiproxyApi + path))
                .header("Content-Type", "application/json")
                .method(method, json == null ? BodyPublishers.noBody() : BodyPublishers.ofString(json))
                .build();
        final HttpResponse<String> response = HTTP.send(request, BodyHandlers.ofString());
        if (response.statusCode() >= 300) {
            throw new IllegalStateException(
                    "Toxiproxy " + method + " " + path + " answered " + response.statusCode() + ": " + response.body());
        }
    }

    /// Names a new bucket for the current test, and creates it unless `create` is false.
    private void newBucket(final boolean create) throws Exception {
        bucketCount++;
        bucket = "download-fault-" + bucketCount;
        if (create) {
            createBucket();
        }
    }

    private void createBucket() throws Exception {
        minio.makeBucket(MakeBucketArgs.builder().bucket(bucket).build());
        seeder = new S3Client(REGION, endpoint, bucket, ACCESS_KEY, SECRET_KEY);
    }

    /// Starts the plugin on the current bucket with roster nodes 3 and 4 and the given last
    /// stored block.
    private void startPlugin(final String secretKey, final long lastStored) {
        final Map<String, String> config = new HashMap<>();
        config.put("cloud.storage.download.endpointUrl", endpoint);
        config.put("cloud.storage.download.regionName", REGION);
        config.put("cloud.storage.download.bucketName", bucket);
        config.put("cloud.storage.download.accessKey", ACCESS_KEY);
        config.put("cloud.storage.download.secretKey", secretKey);
        config.put("cloud.storage.download.objectKeyPrefix", PREFIX);
        config.put("cloud.storage.download.aheadThresholdBlocks", String.valueOf(THRESHOLD));
        config.put("cloud.storage.download.downloadWindow", "4");
        config.put("cloud.storage.download.pollIntervalMs", "3600000");
        config.put("cloud.storage.download.requestTimeoutMs", String.valueOf(REQUEST_TIMEOUT_MS));
        start(new CloudStorageDownloadPlugin(), new SimpleInMemoryHistoricalBlockFacility(), config);
        blockMessaging.registerUnvalidatedBlockHandler(persistence, false, "persistence");
        updateTssData(roster());
        storedUpTo(lastStored);
    }

    /// Reports blocks 0 to `lastStored` as stored, as persistence would.
    private void storedUpTo(final long lastStored) {
        replaceStoredBlocks(List.of(
                BlockRange.newBuilder().rangeStart(0).rangeEnd(lastStored).build()));
    }

    /// A second plugin instance on the same bucket, as after a restart of the block node. It has
    /// its own thread pools, since stopping the first instance shuts down the ones it was given.
    private CloudStorageDownloadFaultIntegrationTest restartedPlugin(final String secretKey, final long lastStored)
            throws Exception {
        final CloudStorageDownloadFaultIntegrationTest restarted = new CloudStorageDownloadFaultIntegrationTest();
        restarted.bucket = bucket;
        restarted.resetProxyAndWatchLogs();
        restarted.startPlugin(secretKey, lastStored);
        return restarted;
    }

    private static void stopRestarted(final CloudStorageDownloadFaultIntegrationTest restarted) throws Exception {
        restarted.plugin.stop();
        restarted.closeSeederAndLogs();
        restarted.tearDown();
    }

    private static TssData roster() {
        final List<RosterEntry> entries = new ArrayList<>();
        for (final long nodeId : List.of(SOURCE_NODE, OTHER_NODE)) {
            entries.add(RosterEntry.newBuilder().nodeId(nodeId).build());
        }
        return TssData.newBuilder()
                .currentRoster(TssRoster.newBuilder().rosterEntries(entries).build())
                .build();
    }

    /// Uploads good copies of blocks `first` to `last` to the folder of a node.
    private void seed(final long nodeId, final long first, final long last) throws Exception {
        for (long blockNumber = first; blockNumber <= last; blockNumber++) {
            upload(nodeId, blockNumber, blockFile(nodeId, blockNumber));
        }
    }

    private void upload(final long nodeId, final long blockNumber, final byte[] bytes) throws Exception {
        seeder.uploadFile(
                BlockKeys.blockKey(PREFIX, nodeId, blockNumber),
                "STANDARD",
                List.of(bytes).iterator(),
                "application/octet-stream");
    }

    /// A block file whose header carries the node ID as the major software version, so a test
    /// can tell which folder a block came from.
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

    /// Bytes that are not zstd, so the copy cannot be decompressed.
    private static byte[] corruptFile() {
        return new byte[] {1, 2, 3, 4};
    }

    private static List<Long> blocks(final long first, final long last) {
        final List<Long> numbers = new ArrayList<>();
        for (long blockNumber = first; blockNumber <= last; blockNumber++) {
            numbers.add(blockNumber);
        }
        return numbers;
    }

    /// Runs one poll on its own thread, so the test can act while the pull is under way. An
    /// exception from the poll comes out of [FutureTask#get].
    private FutureTask<Void> pollInBackground() {
        final FutureTask<Void> polling = new FutureTask<>(new Poll(plugin.poller()));
        Thread.ofPlatform().name("test-poll").start(polling);
        return polling;
    }

    /// Waits until the stand-in persistence has been handed the given block.
    private void awaitSubmitted(final long blockNumber) throws InterruptedException {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        while (!persistence.submitted.contains(blockNumber) && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        assertThat(persistence.submitted).contains(blockNumber);
    }

    /// Waits until RustFS answers through the proxy again.
    private void awaitRustfs() throws InterruptedException {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        boolean ready = false;
        while (!ready && System.nanoTime() < deadline) {
            try {
                ready = minio.bucketExists(
                        BucketExistsArgs.builder().bucket(bucket).build());
            } catch (final Exception e) {
                Thread.sleep(200);
            }
        }
        assertThat(ready).as("RustFS answers again").isTrue();
    }

    private MetricsHolder metrics() {
        return plugin.metricsHolder();
    }

    @Test
    @DisplayName("A corrupt and a missing copy in the source folder come from the other folder")
    void badCopiesInSourceFolderComeFromTheOtherFolder() throws Exception {
        newBucket(true);
        // block 6 corrupt and block 8 missing in the source folder
        seed(SOURCE_NODE, 5, 7);
        seed(SOURCE_NODE, 9, 4 + THRESHOLD);
        upload(SOURCE_NODE, 6, corruptFile());
        seed(OTHER_NODE, 6, 6);
        seed(OTHER_NODE, 8, 8);
        startPlugin(SECRET_KEY, 4);
        plugin.poller().poll();
        assertThat(persistence.submitted).containsExactlyElementsOf(blocks(5, 4 + THRESHOLD));
        for (final long blockNumber : blocks(5, 4 + THRESHOLD)) {
            final long expected = blockNumber == 6 || blockNumber == 8 ? OTHER_NODE : SOURCE_NODE;
            assertThat(persistence.sourceOf.get(blockNumber))
                    .as("folder of block %d", blockNumber)
                    .isEqualTo(expected);
        }
        assertThat(metrics().blocksPulled().get()).isEqualTo(THRESHOLD);
        assertThat(metrics().copiesRejected().get()).isOne();
        assertThat(metrics().requestFailures().get()).isZero();
    }

    @Test
    @DisplayName("A block with no usable copy ends the pull; the next poll starts again from it")
    void blockWithNoUsableCopyEndsThePull() throws Exception {
        newBucket(true);
        seed(SOURCE_NODE, 5, 6 + THRESHOLD);
        upload(SOURCE_NODE, 7, corruptFile());
        startPlugin(SECRET_KEY, 4);
        plugin.poller().poll();
        assertThat(persistence.submitted).containsExactly(5L, 6L);
        assertThat(metrics().blocksDownloadFailed().get()).isOne();
        assertThat(metrics().blocksNotPresent().get()).isZero();
        assertThat(metrics().copiesRejected().get()).isOne();
        // persistence stores what was submitted, and a good copy of block 7 shows up
        storedUpTo(6);
        seed(OTHER_NODE, 7, 7);
        plugin.poller().poll();
        assertThat(persistence.submitted).containsExactlyElementsOf(blocks(5, 6 + THRESHOLD));
        assertThat(persistence.sourceOf.get(7L)).isEqualTo(OTHER_NODE);
        assertThat(metrics().blocksDownloadFailed().get()).isOne();
    }

    @Test
    @DisplayName("Latency above the request timeout is a request failure; without it the next poll pulls")
    void latencyAboveRequestTimeout() throws Exception {
        newBucket(true);
        seed(SOURCE_NODE, 5, 4 + THRESHOLD);
        startPlugin(SECRET_KEY, 4);
        toxiproxy(
                "POST",
                "/proxies/" + PROXY + "/toxics",
                "{\"name\":\"slow\",\"type\":\"latency\",\"stream\":\"downstream\",\"attributes\":{\"latency\":"
                        + (REQUEST_TIMEOUT_MS * 3) + "}}");
        plugin.poller().poll();
        assertThat(persistence.submitted).isEmpty();
        assertThat(metrics().requestFailures().get()).isOne();
        assertThat(logHandler.countContaining(PROBE_FAILED)).isOne();
        toxiproxy("DELETE", "/proxies/" + PROXY + "/toxics/slow", null);
        plugin.poller().poll();
        assertThat(persistence.submitted).containsExactlyElementsOf(blocks(5, 4 + THRESHOLD));
        assertThat(metrics().requestFailures().get()).isOne();
    }

    @Test
    @DisplayName("RustFS stopped during a pull ends it as a request failure; after a restart the pull resumes")
    void storageStoppedDuringPull() throws Exception {
        newBucket(true);
        seed(SOURCE_NODE, 5, 8 + THRESHOLD);
        startPlugin(SECRET_KEY, 4);
        // block 6 is in the first window of four, so its submission comes after the window's downloads
        persistence.hold = 6L;
        final FutureTask<Void> polling = pollInBackground();
        try {
            awaitSubmitted(6L);
            rustfs.getDockerClient()
                    .stopContainerCmd(rustfs.getContainerId())
                    .withTimeout(1)
                    .exec();
            persistence.release(6L);
            polling.get(120, TimeUnit.SECONDS);
        } finally {
            rustfs.getDockerClient().startContainerCmd(rustfs.getContainerId()).exec();
        }
        // the first window is submitted, the download of the second fails
        assertThat(persistence.submitted).containsExactly(5L, 6L, 7L, 8L);
        assertThat(metrics().requestFailures().get()).isOne();
        awaitRustfs();
        storedUpTo(8);
        plugin.poller().poll();
        assertThat(persistence.submitted).containsExactlyElementsOf(blocks(5, 8 + THRESHOLD));
    }

    @Test
    @DisplayName("Wrong credentials are a request failure on every poll, and a restart with the right ones pulls")
    void wrongCredentials() throws Exception {
        newBucket(true);
        seed(SOURCE_NODE, 5, 4 + THRESHOLD);
        startPlugin("not-the-secret", 4);
        plugin.poller().poll();
        plugin.poller().poll();
        assertThat(persistence.submitted).isEmpty();
        assertThat(metrics().requestFailures().get()).isEqualTo(2L);
        assertThat(logHandler.countContaining(PROBE_FAILED)).isEqualTo(2);
        plugin.stop();
        final CloudStorageDownloadFaultIntegrationTest restarted = restartedPlugin(SECRET_KEY, 4);
        try {
            restarted.plugin.poller().poll();
            assertThat(restarted.persistence.submitted).containsExactlyElementsOf(blocks(5, 4 + THRESHOLD));
            assertThat(restarted.metrics().requestFailures().get()).isZero();
        } finally {
            stopRestarted(restarted);
        }
    }

    @Test
    @DisplayName("A missing bucket is a request failure; once the bucket exists the next poll pulls")
    void missingBucket() throws Exception {
        newBucket(false);
        startPlugin(SECRET_KEY, 4);
        plugin.poller().poll();
        assertThat(persistence.submitted).isEmpty();
        assertThat(metrics().requestFailures().get()).isOne();
        assertThat(logHandler.countContaining(PROBE_FAILED)).isOne();
        createBucket();
        seed(SOURCE_NODE, 5, 4 + THRESHOLD);
        plugin.poller().poll();
        assertThat(persistence.submitted).containsExactlyElementsOf(blocks(5, 4 + THRESHOLD));
        assertThat(metrics().requestFailures().get()).isOne();
    }

    @Test
    @DisplayName("An unreachable endpoint is a request failure; once it is reachable the next poll pulls")
    void unreachableEndpoint() throws Exception {
        newBucket(true);
        seed(SOURCE_NODE, 5, 4 + THRESHOLD);
        toxiproxy("POST", "/proxies/" + PROXY, "{\"enabled\":false}");
        startPlugin(SECRET_KEY, 4);
        plugin.poller().poll();
        assertThat(persistence.submitted).isEmpty();
        assertThat(metrics().requestFailures().get()).isOne();
        assertThat(logHandler.countContaining(PROBE_FAILED)).isOne();
        toxiproxy("POST", "/proxies/" + PROXY, "{\"enabled\":true}");
        plugin.poller().poll();
        assertThat(persistence.submitted).containsExactlyElementsOf(blocks(5, 4 + THRESHOLD));
    }

    @Test
    @DisplayName("A plugin stopped during a pull; a new instance submits only the blocks not yet persisted")
    void stopDuringPullAndRestart() throws Exception {
        newBucket(true);
        seed(SOURCE_NODE, 5, 6 + THRESHOLD);
        startPlugin(SECRET_KEY, 4);
        persistence.hold = 7L;
        final FutureTask<Void> polling = pollInBackground();
        awaitSubmitted(7L);
        plugin.stop();
        polling.get(60, TimeUnit.SECONDS);
        assertThat(persistence.persisted).containsExactly(5L, 6L);
        final CloudStorageDownloadFaultIntegrationTest restarted = restartedPlugin(SECRET_KEY, 6);
        try {
            restarted.plugin.poller().poll();
            assertThat(restarted.persistence.submitted).containsExactlyElementsOf(blocks(7, 6 + THRESHOLD));
        } finally {
            stopRestarted(restarted);
        }
    }

    /// One poll, as a task whose exceptions reach the caller.
    private record Poll(BucketPoller poller) implements Callable<Void> {
        @Override
        public Void call() {
            poller.poll();
            return null;
        }
    }

    /// Stands in for verification and persistence: records each submitted block and the node
    /// whose copy it is, and reports it persisted, except a held block, which waits for
    /// [#release(long)].
    private final class Persistence implements BlockNotificationHandler {
        private final List<Long> submitted = new CopyOnWriteArrayList<>();
        private final List<Long> persisted = new CopyOnWriteArrayList<>();
        private final Map<Long, Long> sourceOf = new ConcurrentHashMap<>();
        private volatile long hold = -1L;

        @Override
        public void handleBackfilled(final BackfilledBlockNotification notification) {
            final long blockNumber = notification.blockNumber();
            try {
                final BlockHeader header = BlockHeader.PROTOBUF.parse(
                        notification.block().blockItems().getFirst().blockHeaderOrThrow());
                sourceOf.put(blockNumber, (long) header.softwareVersionOrThrow().major());
            } catch (final Exception e) {
                throw new IllegalStateException(e);
            }
            submitted.add(blockNumber);
            if (blockNumber != hold) {
                release(blockNumber);
            }
        }

        private void release(final long blockNumber) {
            persisted.add(blockNumber);
            blockMessaging.sendBlockPersisted(
                    new PersistedNotification(blockNumber, true, 0, BlockSource.CLOUD_STORAGE));
        }
    }
}
