// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.state.management;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.hedera.hapi.block.stream.input.RoundHeader;
import com.hedera.hapi.block.stream.output.BlockFooter;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.hapi.block.stream.output.MapChangeKey;
import com.hedera.hapi.block.stream.output.MapChangeValue;
import com.hedera.hapi.block.stream.output.MapUpdateChange;
import com.hedera.hapi.block.stream.output.SingletonUpdateChange;
import com.hedera.hapi.block.stream.output.StateChange;
import com.hedera.hapi.block.stream.output.StateChanges;
import com.hedera.hapi.block.stream.output.StateIdentifier;
import com.hedera.hapi.node.state.blockstream.BlockStreamInfo;
import com.hedera.pbj.runtime.io.ReadableSequentialData;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.config.api.ConfigurationBuilder;
import com.swirlds.config.api.validation.ConfigViolationException;
import com.swirlds.merkledb.config.MerkleDbConfig;
import com.swirlds.virtualmap.config.VirtualMapConfig;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;
import java.util.stream.Stream;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.app.fixtures.plugintest.RecordingServiceBuilder;
import org.hiero.block.node.app.fixtures.plugintest.TestBlockMessagingFacility;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import org.hiero.metrics.core.MetricRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/// Periodic state snapshot creation in `StateManagementPlugin`: the write itself, retention,
/// restore round trip, failure handling, the final snapshot on `stop()`, the scheduler, and
/// concurrency with apply. Uses a real `VirtualMapStateLifecycleManager` on a temp dir.
///
/// Every block chains to the previous one through its footer and sets the `BlockStreamInfo`
/// singleton to its own number (restore's self-check reads it), plus one extra singleton and
/// one KV entry whose value changes per block, so a restored state can be told apart from its
/// neighbours.
class StateManagementSnapshotTest {

    /// Long enough that the scheduled task never fires during a test; tests call
    /// `saveSnapshot()` directly instead.
    private static final String NO_SCHEDULED_SNAPSHOT_MS = "3600000";

    private static final int EXTRA_SINGLETON_ID = 1;
    private static final int EXTRA_KV_ID = 2;
    private static final MapChangeKey EXTRA_KV_KEY =
            MapChangeKey.newBuilder().protoBytesKey(Bytes.fromHex("01")).build();

    private static final RecordingServiceBuilder NOOP_SERVICE_BUILDER = new RecordingServiceBuilder();

    @Test
    void snapshotRoundTripRestoresExactlyTheAttestedState(@TempDir final Path tmp) throws Exception {
        final Path root = tmp.resolve("recent");
        final BlockUnparsed block2;
        final Bytes post2Hash;
        try (Node node = Node.start(root)) {
            node.applyRange(0L, 1L);
            block2 = node.nextBlock(2L);
            node.deliver(2L, block2);
            assertThat(node.plugin.attested().blockNumber()).isEqualTo(1L);
            post2Hash = node.plugin.stagedStateRootHash();

            assertThat(node.plugin.saveSnapshot()).isTrue();
            assertThat(node.plugin.lastSnapshottedBlock()).isEqualTo(1L);
            assertThat(numericDirs(root))
                    .as("the attested block (1) is written, not the staged one (2)")
                    .containsExactly(1L);
            assertThat(stagingDirs(root)).isEmpty();
            assertThat(root.resolve("tmp"))
                    .as("the live MerkleDb dir is left alone")
                    .isDirectory();
            assertThat(node.plugin.snapshotFailureTotal()).isZero();
        }

        try (Node restored = Node.start(root)) {
            final StateManagementPlugin plugin = restored.plugin;
            assertThat(plugin.isApplyHalted()).isFalse();
            assertThat(plugin.attested().blockNumber()).isEqualTo(1L);
            assertThat(plugin.lastSnapshottedBlock()).isEqualTo(1L);

            // Read the restored content directly: it must be post-1, not post-2.
            final var state = plugin.attested().state();
            assertThat(state).isNotNull();
            assertThat(state.getSingleton(EXTRA_SINGLETON_ID))
                    .as("extra singleton holds block 1's value")
                    .isEqualTo(storedSingletonValue(1L));
            assertThat(state.getKv(EXTRA_KV_ID, storedKvKey()))
                    .as("KV entry holds block 1's value")
                    .isEqualTo(storedKvValue(1L));
            assertThat(BlockStreamInfo.PROTOBUF
                            .parse(state.getSingleton(StateIdentifier.STATE_ID_BLOCK_STREAM_INFO.protoOrdinal())
                                    .toReadableSequentialData())
                            .blockNumber())
                    .isEqualTo(1L);

            // Block 2's footer carries post-1's root hash as computed by the first instance, so
            // it only validates if the restored state hashes identically.
            restored.deliver(2L, block2);
            assertThat(plugin.isApplyHalted())
                    .as("re-delivering the original block 2 applies on the restored state")
                    .isFalse();
            assertThat(plugin.hashMismatchTotal()).isZero();
            assertThat(plugin.stagedStateRootHash())
                    .as("applying block 2 again reproduces the original post-2 root hash")
                    .isEqualTo(post2Hash);
        }
    }

    @Test
    void saveSnapshotSkipsWhenThereIsNothingNewToWrite(@TempDir final Path tmp) throws Exception {
        final Path root = tmp.resolve("recent");
        try (Node node = Node.start(root)) {
            assertThat(node.plugin.saveSnapshot())
                    .as("nothing attested at boot")
                    .isFalse();
            node.applyRange(0L, 0L);
            assertThat(node.plugin.saveSnapshot())
                    .as("block 0 is only staged, not attested")
                    .isFalse();
            assertThat(numericDirs(root)).isEmpty();
            assertThat(stagingDirs(root)).isEmpty();

            node.applyRange(1L, 2L);
            assertThat(node.plugin.saveSnapshot()).isTrue();
            assertThat(node.plugin.saveSnapshot())
                    .as("no new attestation since the last snapshot")
                    .isFalse();
            assertThat(numericDirs(root)).containsExactly(1L);
            assertThat(node.plugin.snapshotFailureTotal()).isZero();
        }

        final Map<String, String> before = fingerprint(root.resolve("1"));
        try (Node restored = Node.start(root)) {
            assertThat(restored.plugin.attested().blockNumber()).isEqualTo(1L);
            assertThat(restored.plugin.saveSnapshot())
                    .as("a restored snapshot is never rewritten")
                    .isFalse();
        }
        assertThat(fingerprint(root.resolve("1")))
                .as("the restored dir is untouched by restore, saveSnapshot and stop")
                .isEqualTo(before);
        assertThat(numericDirs(root)).containsExactly(1L);
    }

    @Test
    void retentionKeepsNewestSnapshotsAndOnlyDeletesSnapshotEntries(@TempDir final Path tmp) throws Exception {
        final Path root = tmp.resolve("recent");
        // Left over from a crash before start: removed by the start-up cleanup.
        writeFile(root.resolve(".tmp-2").resolve("junk"));
        // Not snapshot entries: must survive every prune.
        writeFile(root.resolve("notes").resolve("readme.txt"));
        writeFile(root.resolve(".tmp-keep").resolve("file"));

        try (Node node = Node.start(root, "state.management.snapshotRecentRetentionCount", "3")) {
            assertThat(root.resolve(".tmp-2"))
                    .as("stale staging dir removed on start")
                    .doesNotExist();

            node.applyRange(0L, 1L);
            for (long attestedBlock = 1L; attestedBlock <= 5L; attestedBlock++) {
                node.applyRange(attestedBlock + 1L, attestedBlock + 1L);
                if (attestedBlock == 4L) {
                    // Appears mid-run: removed by the prune that follows the next write.
                    writeFile(root.resolve(".tmp-77").resolve("junk"));
                }
                assertThat(node.plugin.saveSnapshot())
                        .as("snapshot of block %d", attestedBlock)
                        .isTrue();
            }

            assertThat(numericDirs(root)).containsExactly(3L, 4L, 5L);
            assertThat(stagingDirs(root)).isEmpty();
            assertThat(root.resolve("notes").resolve("readme.txt")).exists();
            assertThat(root.resolve(".tmp-keep").resolve("file"))
                    .as("only .tmp-<digits> counts as staging")
                    .exists();
            assertThat(root.resolve("tmp"))
                    .as("the live MerkleDb dir is never pruned")
                    .isDirectory();

            // The live state still works after all that pruning.
            node.applyRange(7L, 7L);
            assertThat(node.plugin.attested().blockNumber()).isEqualTo(6L);
            assertThat(node.plugin.snapshotFailureTotal()).isZero();
        }
    }

    @Test
    void existingTargetDirIsKeptAndStaleStagingDirIsReplaced(@TempDir final Path tmp) throws Exception {
        final Path root = tmp.resolve("recent");
        try (Node node = Node.start(root)) {
            node.applyRange(0L, 2L);
            // Created after start so restore does not try to load it.
            final Path marker = writeFile(root.resolve("1").resolve("marker"));

            assertThat(node.plugin.saveSnapshot())
                    .as("an existing <N> dir is treated as complete")
                    .isFalse();
            assertThat(node.plugin.lastSnapshottedBlock()).isEqualTo(1L);
            assertThat(listNames(root.resolve("1")))
                    .as("the existing dir is not overwritten")
                    .containsExactly(marker.getFileName().toString());

            node.applyRange(3L, 3L);
            writeFile(root.resolve(".tmp-2").resolve("junk"));
            assertThat(node.plugin.saveSnapshot()).isTrue();
            assertThat(root.resolve("2").resolve("junk"))
                    .as("the stale staging content was cleared before the write")
                    .doesNotExist();
            assertThat(stagingDirs(root)).isEmpty();
            assertThat(numericDirs(root)).containsExactly(1L, 2L);
            assertThat(node.plugin.snapshotFailureTotal()).isZero();
        }

        try (Node restored = Node.start(root)) {
            assertThat(restored.plugin.isApplyHalted()).isFalse();
            assertThat(restored.plugin.attested().blockNumber())
                    .as("the snapshot written over a stale staging dir is loadable")
                    .isEqualTo(2L);
        }
    }

    @Test
    void writeFailureIsCountedAndRetriedOnTheNextRun(@TempDir final Path tmp) throws Exception {
        final Path root = tmp.resolve("recent");
        try (Node node = Node.start(root)) {
            node.applyRange(0L, 2L);
            try {
                // Root is listable but nothing can be created in it. The live MerkleDb under
                // tmp keeps its own permissions. Skipped where permissions are not enforced.
                Files.setPosixFilePermissions(root, PosixFilePermissions.fromString("r-x------"));
                assumeTrue(!Files.isWritable(root), "directory permissions are not enforced here");

                assertThat(node.plugin.saveSnapshot()).isFalse();
                assertThat(node.plugin.snapshotFailureTotal()).isEqualTo(1L);
                assertThat(node.plugin.lastSnapshottedBlock())
                        .as("a failed write does not advance the snapshot marker")
                        .isEqualTo(-1L);
                assertThat(numericDirs(root)).isEmpty();
                assertThat(stagingDirs(root)).isEmpty();
            } finally {
                Files.setPosixFilePermissions(root, PosixFilePermissions.fromString("rwx------"));
            }

            assertThat(node.plugin.saveSnapshot())
                    .as("the same block is retried once the root is writable again")
                    .isTrue();
            assertThat(numericDirs(root)).containsExactly(1L);
            assertThat(node.plugin.lastSnapshottedBlock()).isEqualTo(1L);
            assertThat(node.plugin.snapshotFailureTotal()).isEqualTo(1L);
        }
    }

    @Test
    void haltedNodeStillSnapshotsTheLastAttestedBlock(@TempDir final Path tmp) throws Exception {
        final Path root = tmp.resolve("recent");
        try (Node node = Node.start(root)) {
            node.applyRange(0L, 1L);
            node.deliver(2L, block(2L, Bytes.fromHex("deadbeef".repeat(12))));
            assertThat(node.plugin.isApplyHalted()).isTrue();
            assertThat(node.plugin.attested().blockNumber()).isZero();

            assertThat(node.plugin.saveSnapshot())
                    .as("block 0 was attested before the halt and is a valid recovery point")
                    .isTrue();
            assertThat(numericDirs(root)).containsExactly(0L);
        }
    }

    @Test
    void stopWritesAFinalSnapshotOfTheLatestAttestedBlock(@TempDir final Path tmp) throws Exception {
        final Path root = tmp.resolve("recent");
        try (Node node = Node.start(root)) {
            node.applyRange(0L, 3L);
            assertThat(numericDirs(root)).isEmpty();

            node.close();
            assertThat(node.plugin.lastSnapshottedBlock()).isEqualTo(2L);
            assertThat(numericDirs(root)).containsExactly(2L);
        }

        try (Node restored = Node.start(root)) {
            assertThat(restored.plugin.isApplyHalted()).isFalse();
            assertThat(restored.plugin.attested().blockNumber()).isEqualTo(2L);
        }
    }

    @Test
    void scheduledTaskWritesSnapshotWithoutExplicitCall(@TempDir final Path tmp) throws Exception {
        final Path root = tmp.resolve("recent");
        try (Node node = Node.start(root, "state.management.snapshotIntervalMillis", "1000")) {
            node.applyRange(0L, 2L);

            assertThat(awaitCondition(() -> Files.isDirectory(root.resolve("1")), 10_000L))
                    .as("scheduled snapshot of block 1 appears within 10s at a 1s interval")
                    .isTrue();
            assertThat(awaitCondition(() -> node.plugin.lastSnapshottedBlock() == 1L, 5_000L))
                    .isTrue();
            assertThat(node.plugin.snapshotFailureTotal()).isZero();
        }
    }

    @Test
    void concurrentSnapshotsDuringApplyStayConsistent(@TempDir final Path tmp) throws Exception {
        final Path root = tmp.resolve("recent");
        final long lastBlock = 20L;
        final AtomicInteger written = new AtomicInteger();
        final ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
        try (Node node = Node.start(root)) {
            final AtomicBoolean done = new AtomicBoolean(false);
            final Thread snapshotter = new Thread(
                    () -> {
                        while (!done.get()) {
                            try {
                                if (node.plugin.saveSnapshot()) {
                                    written.incrementAndGet();
                                }
                            } catch (final Throwable t) {
                                errors.add(t);
                            }
                            // Brief pause so the loop does not monopolise a core; not a wait.
                            LockSupport.parkNanos(TimeUnit.MICROSECONDS.toNanos(200L));
                        }
                    },
                    "snapshot-test-loop");
            snapshotter.setDaemon(true);
            snapshotter.start();
            try {
                node.applyRange(0L, lastBlock);
            } finally {
                done.set(true);
                snapshotter.join(30_000L);
            }
            assertThat(snapshotter.isAlive()).as("snapshot loop finished").isFalse();
            assertThat(errors).as("saveSnapshot never throws").isEmpty();
            assertThat(node.plugin.isApplyHalted()).isFalse();
            assertThat(node.plugin.snapshotFailureTotal()).isZero();
            assertThat(written.get()).as("the loop wrote at least one snapshot").isPositive();
        }

        // stop() took a final snapshot of the last attested block.
        assertThat(numericDirs(root)).hasSizeLessThanOrEqualTo(3).contains(lastBlock - 1L);
        assertThat(stagingDirs(root)).isEmpty();
        try (Node restored = Node.start(root)) {
            assertThat(restored.plugin.isApplyHalted()).isFalse();
            assertThat(restored.plugin.attested().blockNumber()).isEqualTo(lastBlock - 1L);
        }
    }

    @Test
    void snapshotConfigDefaultsAndBounds() {
        final StateManagementConfig defaults = snapshotConfig(Map.of());
        assertThat(defaults.snapshotIntervalMillis()).isEqualTo(900_000L);
        assertThat(defaults.snapshotRecentRetentionCount()).isEqualTo(3);

        assertThat(snapshotConfig(Map.of("state.management.snapshotIntervalMillis", "1000"))
                        .snapshotIntervalMillis())
                .isEqualTo(1_000L);
        assertThat(snapshotConfig(Map.of("state.management.snapshotIntervalMillis", "86400000"))
                        .snapshotIntervalMillis())
                .isEqualTo(86_400_000L);
        assertThat(snapshotConfig(Map.of("state.management.snapshotRecentRetentionCount", "1"))
                        .snapshotRecentRetentionCount())
                .isEqualTo(1);
        assertThat(snapshotConfig(Map.of("state.management.snapshotRecentRetentionCount", "1000"))
                        .snapshotRecentRetentionCount())
                .isEqualTo(1000);

        final List<Map<String, String>> outOfRange = List.of(
                Map.of("state.management.snapshotIntervalMillis", "999"),
                Map.of("state.management.snapshotIntervalMillis", "86400001"),
                Map.of("state.management.snapshotRecentRetentionCount", "0"),
                Map.of("state.management.snapshotRecentRetentionCount", "1001"));
        for (final Map<String, String> values : outOfRange) {
            assertThatThrownBy(() -> snapshotConfig(values))
                    .as("rejects %s", values)
                    .isInstanceOf(ConfigViolationException.class);
        }
    }

    // ── Fixtures ───────────────────────────────────────────────────────────

    /// A started plugin plus the messaging facility that feeds it. `close()` stops the plugin
    /// once, so tests can stop explicitly and still use try-with-resources.
    private static final class Node implements AutoCloseable {
        private final TestBlockMessagingFacility facility = new TestBlockMessagingFacility();
        private final StateManagementPlugin plugin = new StateManagementPlugin();
        private boolean stopped;

        /// @param overrides alternating config key/value pairs
        static Node start(final Path root, final String... overrides) throws InterruptedException {
            final var builder = ConfigurationBuilder.create()
                    .withConfigDataType(StateManagementConfig.class)
                    .withConfigDataType(MerkleDbConfig.class)
                    .withConfigDataType(VirtualMapConfig.class)
                    .withValue("state.management.snapshotRecentPath", root.toString())
                    .withValue("state.management.snapshotIntervalMillis", NO_SCHEDULED_SNAPSHOT_MS);
            for (int i = 0; i < overrides.length; i += 2) {
                builder.withValue(overrides[i], overrides[i + 1]);
            }
            final Node node = new Node();
            final BlockNodeContext context = new BlockNodeContext(
                    builder.build(),
                    MetricRegistry.builder().build(),
                    null,
                    node.facility,
                    null,
                    null,
                    null,
                    StateManagementPluginTestSupport.newThreadPoolManager(),
                    null);
            node.plugin.init(context, NOOP_SERVICE_BUILDER);
            node.plugin.start();
            StateManagementPluginTestSupport.awaitReady(node.plugin, 5_000L);
            return node;
        }

        /// The next block, chained to the currently staged state.
        BlockUnparsed nextBlock(final long blockNumber) {
            return block(blockNumber, plugin.stagedStateRootHash());
        }

        void deliver(final long blockNumber, final BlockUnparsed block) {
            facility.sendBlockVerification(new VerificationNotification(
                    true, null, blockNumber, Bytes.fromHex("aabb"), block, BlockSource.PUBLISHER));
        }

        /// Apply chained blocks `first..last` inclusive, failing fast if any halts apply.
        void applyRange(final long first, final long last) {
            for (long n = first; n <= last; n++) {
                deliver(n, nextBlock(n));
                assertThat(plugin.isApplyHalted())
                        .as("apply halted at block %d", n)
                        .isFalse();
            }
        }

        @Override
        public void close() {
            if (!stopped) {
                stopped = true;
                plugin.stop();
            }
        }
    }

    /// A block that sets `BlockStreamInfo.block_number`, the extra singleton and the extra KV
    /// entry to values derived from `blockNumber`.
    private static BlockUnparsed block(final long blockNumber, final Bytes startOfBlockStateRootHash) {
        final List<StateChange> changes = List.of(
                StateChange.newBuilder()
                        .stateId(StateIdentifier.STATE_ID_BLOCK_STREAM_INFO.protoOrdinal())
                        .singletonUpdate(SingletonUpdateChange.newBuilder()
                                .blockStreamInfoValue(BlockStreamInfo.newBuilder()
                                        .blockNumber(blockNumber)
                                        .build())
                                .build())
                        .build(),
                StateChange.newBuilder()
                        .stateId(EXTRA_SINGLETON_ID)
                        .singletonUpdate(extraSingletonChange(blockNumber))
                        .build(),
                StateChange.newBuilder()
                        .stateId(EXTRA_KV_ID)
                        .mapUpdate(MapUpdateChange.newBuilder()
                                .key(EXTRA_KV_KEY)
                                .value(extraKvValue(blockNumber))
                                .build())
                        .build());
        return BlockUnparsed.newBuilder()
                .blockItems(
                        BlockItemUnparsed.newBuilder()
                                .blockHeader(BlockHeader.PROTOBUF.toBytes(BlockHeader.newBuilder()
                                        .number(blockNumber)
                                        .build()))
                                .build(),
                        BlockItemUnparsed.newBuilder()
                                .roundHeader(RoundHeader.PROTOBUF.toBytes(RoundHeader.newBuilder()
                                        .roundNumber(blockNumber * 10L)
                                        .build()))
                                .build(),
                        BlockItemUnparsed.newBuilder()
                                .stateChanges(StateChanges.PROTOBUF.toBytes(StateChanges.newBuilder()
                                        .stateChanges(new ArrayList<>(changes))
                                        .build()))
                                .build(),
                        BlockItemUnparsed.newBuilder()
                                .blockFooter(BlockFooter.PROTOBUF.toBytes(BlockFooter.newBuilder()
                                        .startOfBlockStateRootHash(startOfBlockStateRootHash)
                                        .build()))
                                .build())
                .build();
    }

    private static SingletonUpdateChange extraSingletonChange(final long blockNumber) {
        return SingletonUpdateChange.newBuilder()
                .bytesValue(Bytes.wrap(("value-" + blockNumber).getBytes(StandardCharsets.UTF_8)))
                .build();
    }

    private static MapChangeValue extraKvValue(final long blockNumber) {
        return MapChangeValue.newBuilder().protoStringValue("kv-" + blockNumber).build();
    }

    // `StateChangeApplier` stores the oneof-selected payload, not the wrapper message.

    private static Bytes storedSingletonValue(final long blockNumber) {
        return firstDelimitedFieldPayload(SingletonUpdateChange.PROTOBUF.toBytes(extraSingletonChange(blockNumber)));
    }

    private static Bytes storedKvKey() {
        return firstDelimitedFieldPayload(MapChangeKey.PROTOBUF.toBytes(EXTRA_KV_KEY));
    }

    private static Bytes storedKvValue(final long blockNumber) {
        return firstDelimitedFieldPayload(MapChangeValue.PROTOBUF.toBytes(extraKvValue(blockNumber)));
    }

    /// Independent extraction of a wrapper message's first length-delimited field (same as
    /// `StateChangeApplierTest`), so expected stored bytes are not hardcoded.
    private static Bytes firstDelimitedFieldPayload(final Bytes wrapperMessageBytes) {
        final ReadableSequentialData input = wrapperMessageBytes.toReadableSequentialData();
        final int tag = input.readVarInt(false);
        if ((tag & 0x7) != 2) {
            throw new IllegalStateException("Expected a length-delimited field, got wire type " + (tag & 0x7));
        }
        final int length = input.readVarInt(false);
        return input.readBytes(length);
    }

    private static StateManagementConfig snapshotConfig(final Map<String, String> values) {
        final var builder = ConfigurationBuilder.create().withConfigDataType(StateManagementConfig.class);
        values.forEach(builder::withValue);
        return builder.build().getConfigData(StateManagementConfig.class);
    }

    /// Numerically named (snapshot) dirs directly under `root`, ascending.
    private static List<Long> numericDirs(final Path root) throws IOException {
        return listNames(root).stream()
                .filter(name -> !name.isEmpty() && name.chars().allMatch(Character::isDigit))
                .filter(name -> Files.isDirectory(root.resolve(name)))
                .map(Long::parseLong)
                .sorted()
                .toList();
    }

    /// `.tmp-<digits>` staging dirs directly under `root`.
    private static List<String> stagingDirs(final Path root) throws IOException {
        return listNames(root).stream()
                .filter(name -> name.matches("\\.tmp-\\d+"))
                .toList();
    }

    private static List<String> listNames(final Path dir) throws IOException {
        if (!Files.isDirectory(dir)) {
            return List.of();
        }
        try (Stream<Path> entries = Files.list(dir)) {
            return entries.map(p -> p.getFileName().toString()).sorted().toList();
        }
    }

    /// Relative path -> size and modification time of every file under `dir`, to show a
    /// directory was not rewritten.
    private static Map<String, String> fingerprint(final Path dir) throws IOException {
        final Map<String, String> result = new TreeMap<>();
        try (Stream<Path> walk = Files.walk(dir)) {
            for (final Path p : walk.filter(Files::isRegularFile).toList()) {
                result.put(dir.relativize(p).toString(), Files.size(p) + "@" + Files.getLastModifiedTime(p));
            }
        }
        return result;
    }

    private static Path writeFile(final Path file) throws IOException {
        Files.createDirectories(file.getParent());
        return Files.writeString(file, "x");
    }

    private static boolean awaitCondition(final BooleanSupplier condition, final long timeoutMillis)
            throws InterruptedException {
        final long deadline = System.currentTimeMillis() + timeoutMillis;
        while (!condition.getAsBoolean() && System.currentTimeMillis() < deadline) {
            Thread.sleep(20L);
        }
        return condition.getAsBoolean();
    }
}
