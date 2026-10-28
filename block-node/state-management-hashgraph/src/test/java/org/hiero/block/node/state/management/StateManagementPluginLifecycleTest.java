// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.state.management;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.hedera.hapi.block.stream.input.RoundHeader;
import com.hedera.hapi.block.stream.output.BlockFooter;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.hapi.block.stream.output.StateIdentifier;
import com.hedera.hapi.node.state.blockstream.BlockStreamInfo;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.base.time.Time;
import com.swirlds.config.api.ConfigurationBuilder;
import com.swirlds.merkledb.config.MerkleDbConfig;
import com.swirlds.state.merkle.VirtualMapState;
import com.swirlds.state.merkle.VirtualMapStateLifecycleManager;
import com.swirlds.virtualmap.config.VirtualMapConfig;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.logging.Logger;
import org.hiero.base.file.FileSystemManager;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.app.fixtures.logging.TestLogHandler;
import org.hiero.block.node.app.fixtures.plugintest.RecordingServiceBuilder;
import org.hiero.block.node.app.fixtures.plugintest.TestBlockMessagingFacility;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import org.hiero.block.node.spi.health.HealthFacility;
import org.hiero.metrics.core.MetricRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/// Exercises the `StateManagementPlugin` lifecycle end-to-end against an in-tree fixture (no
/// real VirtualMap-backed state). Covers startup, the lag-1 apply pipeline, apply-halt behavior,
/// and snapshot restore.
class StateManagementPluginLifecycleTest {

    @Test
    void rejectsBlockWithUnparseableHeader(@TempDir final Path tmp) throws Exception {
        final TestBlockMessagingFacility facility = new TestBlockMessagingFacility();
        final StateManagementPlugin plugin = startPlugin(tmp.resolve("recent"), facility);

        final BlockUnparsed corrupt = BlockUnparsed.newBuilder()
                .blockItems(BlockItemUnparsed.newBuilder()
                        .blockHeader(Bytes.fromHex("ffffffff")) // not a valid BlockHeader proto
                        .build())
                .build();
        facility.sendBlockVerification(
                new VerificationNotification(true, null, 1L, Bytes.fromHex("aabb"), corrupt, BlockSource.PUBLISHER));
        plugin.applyPending();

        assertThat(plugin.attested().state()).isNull();
        plugin.stop();
    }

    @Test
    void corruptSnapshotDirectoryHaltsRatherThanFallingBackToGenesis(@TempDir final Path tmp) throws Exception {
        final Path recentRoot = tmp.resolve("recent");
        Files.createDirectories(recentRoot);

        // A snapshot directory that exists (so restore attempts to load it, same as operator
        // intent being present) but holds none of the expected snapshot content — the plugin
        // should halt rather than silently fall back to genesis.
        Files.createDirectories(recentRoot.resolve("5"));

        final TestBlockMessagingFacility facility = new TestBlockMessagingFacility();
        final var configuration = ConfigurationBuilder.create()
                .withConfigDataType(StateManagementConfig.class)
                .withConfigDataType(MerkleDbConfig.class)
                .withConfigDataType(VirtualMapConfig.class)
                .withValue("state.management.snapshotRecentPath", recentRoot.toString())
                .build();
        final BlockNodeContext context = new BlockNodeContext(
                configuration,
                MetricRegistry.builder().build(),
                null,
                facility,
                null,
                null,
                null,
                StateManagementPluginTestSupport.newThreadPoolManager(),
                null);
        final StateManagementPlugin plugin = new StateManagementPlugin();
        plugin.init(context, NOOP_SERVICE_BUILDER);
        plugin.start();

        assertThat(plugin.isApplyHalted()).isTrue();
        assertThat(plugin.attested().state()).isNull();
        plugin.stop();
    }

    @Test
    void unlistableSnapshotRootHaltsRatherThanFallingBackToGenesis(@TempDir final Path tmp) throws Exception {
        final Path recentRoot = tmp.resolve("recent");
        final var configuration = ConfigurationBuilder.create()
                .withConfigDataType(StateManagementConfig.class)
                .withConfigDataType(MerkleDbConfig.class)
                .withConfigDataType(VirtualMapConfig.class)
                .withValue("state.management.snapshotRecentPath", recentRoot.toString())
                .build();
        final BlockNodeContext context = new BlockNodeContext(
                configuration,
                MetricRegistry.builder().build(),
                null,
                new TestBlockMessagingFacility(),
                null,
                null,
                null,
                StateManagementPluginTestSupport.newThreadPoolManager(),
                null);
        final StateManagementPlugin plugin = new StateManagementPlugin();
        plugin.init(context, NOOP_SERVICE_BUILDER);

        // The root exists (snapshots may be inside) but cannot be listed. Skipped where
        // permissions are not enforced (e.g. running as root, non-POSIX filesystems).
        Files.createDirectories(recentRoot);
        try {
            Files.setPosixFilePermissions(recentRoot, PosixFilePermissions.fromString("-wx------"));
            assumeTrue(!Files.isReadable(recentRoot), "directory permissions are not enforced here");
            plugin.start();

            assertThat(plugin.isApplyHalted()).isTrue();
            assertThat(plugin.attested().state()).isNull();
            assertThat(plugin.stagedStateRootHash())
                    .as("genesis state must not be seeded after a failed snapshot scan")
                    .isEqualTo(Bytes.EMPTY);
        } finally {
            Files.setPosixFilePermissions(recentRoot, PosixFilePermissions.fromString("rwx------"));
            plugin.stop();
        }
    }

    @Test
    void startupRestoresFromExistingSnapshot(@TempDir final Path tmp) throws Exception {
        final Path recentRoot = tmp.resolve("recent");
        Files.createDirectories(recentRoot);

        // Manufacture a snapshot the same way a future snapshot-creation plugin slice would —
        // directly via VirtualMapStateLifecycleManager's own public API, independent of this
        // plugin (which does not create snapshots yet).
        final var seedConfig = ConfigurationBuilder.create()
                .withConfigDataType(MerkleDbConfig.class)
                .withConfigDataType(VirtualMapConfig.class)
                .build();
        final var seedManager = new VirtualMapStateLifecycleManager(
                new NoOpSwirldsMetrics(), Time.getCurrent(), seedConfig, new FileSystemManager(recentRoot));
        seedManager.copyMutableState();
        seedManager.getMutableState().updateSingleton(1, Bytes.fromHex("c0ffee"));
        // Restore identifies a snapshot by reading BlockStreamInfo.block_number back out of the
        // loaded state and checking it against the directory name (§5.1/§6.2) — seed it here to
        // match the "5" directory below.
        seedBlockStreamInfo(seedManager.getMutableState(), 5L);
        seedManager.copyMutableState();
        final VirtualMapState sealed = seedManager.getLatestImmutableState();
        final Bytes rootHash = Bytes.wrap(sealed.getRoot().getHash().copyToByteArray());
        seedManager.createSnapshot(sealed, recentRoot.resolve("5"));

        final TestBlockMessagingFacility facility = new TestBlockMessagingFacility();
        final StateManagementPlugin plugin = startPlugin(recentRoot, facility);

        assertThat(plugin.attested().blockNumber()).isEqualTo(5L);
        assertThat(plugin.attested().roundNumber())
                .as("round number isn't persisted; it's unknown until the next applied block's RoundHeader")
                .isEqualTo(-1L);

        // The restored state becomes state 2 (hashingImmutable) too — block 6's footer must
        // carry the same root hash to be accepted, proving restore seeded real state content,
        // not just the block number.
        facility.sendBlockVerification(new VerificationNotification(
                true, null, 6L, Bytes.fromHex("aabb"), buildBlock(6L, 60L, rootHash), BlockSource.PUBLISHER));
        plugin.applyPending();
        assertThat(plugin.isApplyHalted()).isFalse();

        plugin.stop();
    }

    @Test
    void snapshotSelfCheckMismatchHaltsRatherThanServingWrongState(@TempDir final Path tmp) throws Exception {
        final Path recentRoot = tmp.resolve("recent");
        Files.createDirectories(recentRoot);

        final var seedConfig = ConfigurationBuilder.create()
                .withConfigDataType(MerkleDbConfig.class)
                .withConfigDataType(VirtualMapConfig.class)
                .build();
        final var seedManager = new VirtualMapStateLifecycleManager(
                new NoOpSwirldsMetrics(), Time.getCurrent(), seedConfig, new FileSystemManager(recentRoot));
        seedManager.copyMutableState();
        // Deliberately record block 7 inside the state, but store it under directory "5" — the
        // self-consistency check must catch this disagreement and halt.
        seedBlockStreamInfo(seedManager.getMutableState(), 7L);
        seedManager.copyMutableState();
        final VirtualMapState sealed = seedManager.getLatestImmutableState();
        sealed.getRoot().getHash(); // block until hashed; createSnapshot requires a hashed copy
        seedManager.createSnapshot(sealed, recentRoot.resolve("5"));

        final TestBlockMessagingFacility facility = new TestBlockMessagingFacility();
        final var configuration = ConfigurationBuilder.create()
                .withConfigDataType(StateManagementConfig.class)
                .withConfigDataType(MerkleDbConfig.class)
                .withConfigDataType(VirtualMapConfig.class)
                .withValue("state.management.snapshotRecentPath", recentRoot.toString())
                .build();
        final BlockNodeContext context = new BlockNodeContext(
                configuration,
                MetricRegistry.builder().build(),
                null,
                facility,
                null,
                null,
                null,
                StateManagementPluginTestSupport.newThreadPoolManager(),
                null);
        final StateManagementPlugin plugin = new StateManagementPlugin();
        plugin.init(context, NOOP_SERVICE_BUILDER);
        plugin.start();

        assertThat(plugin.isApplyHalted()).isTrue();
        assertThat(plugin.hashMismatchTotal()).isEqualTo(1L);
        assertThat(plugin.attested().state()).isNull();
        plugin.stop();
    }

    @Test
    void malformedStateChangesHaltsApplyWithoutApplying(@TempDir final Path tmp) throws Exception {
        final TestBlockMessagingFacility facility = new TestBlockMessagingFacility();
        final StateManagementPlugin plugin = startPlugin(tmp.resolve("recent"), facility);

        // Genesis block 0 with a valid header/footer (real empty-state hash passes genesis
        // validation) but a state_changes item carrying invalid protobuf bytes. The applier
        // throws; applyPending must halt apply and leave the block unapplied.
        final BlockUnparsed badBlock = BlockUnparsed.newBuilder()
                .blockItems(
                        BlockItemUnparsed.newBuilder()
                                .blockHeader(BlockHeader.PROTOBUF.toBytes(
                                        BlockHeader.newBuilder().number(0L).build()))
                                .build(),
                        BlockItemUnparsed.newBuilder()
                                .stateChanges(Bytes.fromHex("ffffffff"))
                                .build(),
                        BlockItemUnparsed.newBuilder()
                                .blockFooter(BlockFooter.PROTOBUF.toBytes(BlockFooter.newBuilder()
                                        .startOfBlockStateRootHash(plugin.stagedStateRootHash())
                                        .build()))
                                .build())
                .build();
        facility.sendBlockVerification(
                new VerificationNotification(true, null, 0L, Bytes.fromHex("aabb"), badBlock, BlockSource.PUBLISHER));
        plugin.applyPending();

        assertThat(plugin.isApplyHalted()).isTrue();
        assertThat(plugin.attested().state()).isNull();
        plugin.stop();
    }

    @Test
    void genesisBlockWithWrongStartHashIsRejectedNotAcceptedUnconditionally(@TempDir final Path tmp) throws Exception {
        final TestBlockMessagingFacility facility = new TestBlockMessagingFacility();
        final StateManagementPlugin plugin = startPlugin(tmp.resolve("recent"), facility);

        // Block 0's footer must carry the real (computed) empty-state hash, not just any
        // empty/zero placeholder — an arbitrary wrong hash must still be rejected at genesis.
        facility.sendBlockVerification(new VerificationNotification(
                true, null, 0L, Bytes.fromHex("aabb"), buildBlock(0L, 0L, Bytes.EMPTY), BlockSource.PUBLISHER));
        plugin.applyPending();

        assertThat(plugin.isApplyHalted()).isTrue();
        assertThat(plugin.hashMismatchTotal()).isEqualTo(1L);
        plugin.stop();
    }

    @Test
    void digestLengthMismatchLogsAlgorithmMismatchNotDivergence(@TempDir final Path tmp) throws Exception {
        final TestBlockMessagingFacility facility = new TestBlockMessagingFacility();
        final StateManagementPlugin plugin = startPlugin(tmp.resolve("recent"), facility);

        final TestLogHandler logHandler = new TestLogHandler();
        final Logger julLogger = Logger.getLogger(StateManagementPlugin.class.getName());
        julLogger.addHandler(logHandler);
        try {
            // A 48-byte (SHA-384-sized) start hash against this build's 32-byte (SHA-256)
            // genesis hash: a digest algorithm mismatch, not state divergence — the log must
            // say so rather than reading as corruption.
            facility.sendBlockVerification(new VerificationNotification(
                    true,
                    null,
                    0L,
                    Bytes.fromHex("aabb"),
                    buildBlock(0L, 0L, Bytes.fromHex("ab".repeat(48))),
                    BlockSource.PUBLISHER));
            plugin.applyPending();
        } finally {
            julLogger.removeHandler(logHandler);
        }

        assertThat(plugin.isApplyHalted()).isTrue();
        assertThat(plugin.hashMismatchTotal()).isEqualTo(1L);
        assertThat(logHandler.countContaining("digest algorithm mismatch")).isEqualTo(1);
        plugin.stop();
    }

    @Test
    void restartAfterApplyHaltStartsClean(@TempDir final Path tmp) throws Exception {
        final Path recentRoot = tmp.resolve("recent");
        final TestBlockMessagingFacility facility = new TestBlockMessagingFacility();
        final StateManagementPlugin plugin = startPlugin(recentRoot, facility);

        // Apply genesis block 0 (staged), then deliver block 1 whose footer start hash does
        // not match post-0 — a hash mismatch that halts apply.
        facility.sendBlockVerification(new VerificationNotification(
                true,
                null,
                0L,
                Bytes.fromHex("aabb"),
                buildBlock(0L, 0L, plugin.stagedStateRootHash()),
                BlockSource.PUBLISHER));
        plugin.applyPending();
        facility.sendBlockVerification(new VerificationNotification(
                true,
                null,
                1L,
                Bytes.fromHex("aabb"),
                buildBlock(1L, 10L, Bytes.fromHex("deadbeef".repeat(12))),
                BlockSource.PUBLISHER));
        plugin.applyPending();
        assertThat(plugin.isApplyHalted()).isTrue();
        plugin.stop();

        // Apply-halted state is in-memory only (the documented recovery is a restart). A fresh
        // instance must start clean and reach readiness.
        final TestBlockMessagingFacility facility2 = new TestBlockMessagingFacility();
        final StateManagementPlugin plugin2 = startPlugin(recentRoot, facility2);
        assertThat(plugin2.isApplyHalted()).isFalse();
        assertThat(StateManagementPluginTestSupport.awaitReady(plugin2, 5_000L)).isTrue();
        plugin2.stop();
    }

    @Test
    void pendingBlocksStopsGrowingOnceApplyHalted(@TempDir final Path tmp) throws Exception {
        // handleVerification must stop staging new blocks once apply halts, since applyPending()
        // never drains pendingBlocks again until a restart.
        final var configuration = ConfigurationBuilder.create()
                .withConfigDataType(StateManagementConfig.class)
                .withConfigDataType(MerkleDbConfig.class)
                .withConfigDataType(VirtualMapConfig.class)
                .withValue(
                        "state.management.snapshotRecentPath",
                        tmp.resolve("recent").toString())
                .build();
        final TestBlockMessagingFacility facility = new TestBlockMessagingFacility();
        final BlockNodeContext context = new BlockNodeContext(
                configuration,
                MetricRegistry.builder().build(),
                null,
                facility,
                null,
                null,
                null,
                StateManagementPluginTestSupport.newThreadPoolManager(),
                null);
        final StateManagementPlugin plugin = new StateManagementPlugin();
        plugin.init(context, NOOP_SERVICE_BUILDER);
        plugin.start();
        assertThat(StateManagementPluginTestSupport.awaitReady(plugin, 5_000L)).isTrue();

        // Halt apply via the same hash-mismatch recipe as restartAfterApplyHaltStartsClean:
        // apply genesis block 0, then deliver block 1 with a footer start hash that doesn't
        // match.
        facility.sendBlockVerification(new VerificationNotification(
                true,
                null,
                0L,
                Bytes.fromHex("aabb"),
                buildBlock(0L, 0L, plugin.stagedStateRootHash()),
                BlockSource.PUBLISHER));
        plugin.applyPending();
        facility.sendBlockVerification(new VerificationNotification(
                true,
                null,
                1L,
                Bytes.fromHex("aabb"),
                buildBlock(1L, 10L, Bytes.fromHex("deadbeef".repeat(12))),
                BlockSource.PUBLISHER));
        plugin.applyPending();
        assertThat(plugin.isApplyHalted()).isTrue();

        final long pendingAtApplyHalted = plugin.pendingBlocksSize();

        // Deliver 100 more "verified" blocks after halting apply — none of them must be staged.
        for (long blockNumber = 2L; blockNumber <= 101L; blockNumber++) {
            facility.sendBlockVerification(new VerificationNotification(
                    true,
                    null,
                    blockNumber,
                    Bytes.fromHex("aabb"),
                    buildBlock(blockNumber, blockNumber * 10L),
                    BlockSource.PUBLISHER));
        }

        assertThat(plugin.pendingBlocksSize())
                .as("pendingBlocks must not grow once apply is halted")
                .isEqualTo(pendingAtApplyHalted);
        plugin.stop();
    }

    @Test
    void pendingBlocksCapDropsExcessLiveBlocksDuringAGap(@TempDir final Path tmp) throws Exception {
        final var configuration = ConfigurationBuilder.create()
                .withConfigDataType(StateManagementConfig.class)
                .withConfigDataType(MerkleDbConfig.class)
                .withConfigDataType(VirtualMapConfig.class)
                .withValue(
                        "state.management.snapshotRecentPath",
                        tmp.resolve("recent").toString())
                .withValue("state.management.pendingBlocksMax", "3")
                .build();
        final TestBlockMessagingFacility facility = new TestBlockMessagingFacility();
        final BlockNodeContext context = new BlockNodeContext(
                configuration,
                MetricRegistry.builder().build(),
                null,
                facility,
                null,
                null,
                null,
                StateManagementPluginTestSupport.newThreadPoolManager(),
                null);
        final StateManagementPlugin plugin = new StateManagementPlugin();
        plugin.init(context, NOOP_SERVICE_BUILDER);
        plugin.start();

        // Apply genesis so `expectedNext` is a fixed block number, then leave block 1 missing —
        // an unresolved gap — while delivering more live blocks than the cap allows.
        facility.sendBlockVerification(new VerificationNotification(
                true,
                null,
                0L,
                Bytes.fromHex("aabb"),
                buildBlock(0L, 0L, plugin.stagedStateRootHash()),
                BlockSource.PUBLISHER));
        plugin.applyPending();
        assertThat(plugin.isApplyHalted()).isFalse();

        for (long blockNumber = 2L; blockNumber <= 9L; blockNumber++) {
            facility.sendBlockVerification(new VerificationNotification(
                    true,
                    null,
                    blockNumber,
                    Bytes.fromHex("aabb"),
                    buildBlock(blockNumber, blockNumber * 10L),
                    BlockSource.PUBLISHER));
        }

        assertThat(plugin.pendingBlocksSize())
                .as("pendingBlocks must stay at or below the configured cap")
                .isLessThanOrEqualTo(3);
        plugin.stop();
    }

    @Test
    void unwritableStateDirStaysInertWithoutThrowing(@TempDir final Path tmp) throws Exception {
        // Make the configured recent-snapshot path impossible to create (parent is a regular
        // file). init() must log and leave the plugin inert rather than throw — an exception
        // here would abort BlockNodeApp construction and take down every other plugin.
        final Path blocker = tmp.resolve("blocker");
        Files.writeString(blocker, "not a directory");
        final Path badRecent = blocker.resolve("state/recent");

        final var configuration = ConfigurationBuilder.create()
                .withConfigDataType(StateManagementConfig.class)
                .withConfigDataType(MerkleDbConfig.class)
                .withConfigDataType(VirtualMapConfig.class)
                .withValue("state.management.snapshotRecentPath", badRecent.toString())
                .build();
        final RecordingHealthFacility health = new RecordingHealthFacility();
        final BlockNodeContext context = new BlockNodeContext(
                configuration,
                MetricRegistry.builder().build(),
                health,
                new TestBlockMessagingFacility(),
                null,
                null,
                null,
                null,
                null);
        final StateManagementPlugin plugin = new StateManagementPlugin();

        // Neither init() nor start() may throw despite the unusable state directory.
        plugin.init(context, NOOP_SERVICE_BUILDER);
        plugin.start();

        assertThat(health.shutdownRequested)
                .as("a storage failure in this plugin must not shut down the whole Block Node")
                .isFalse();
        assertThat(plugin.isReady()).isFalse();
        plugin.stop();
    }

    /// Minimal `HealthFacility` that records whether a shutdown was requested.
    private static final class RecordingHealthFacility implements HealthFacility {
        private volatile boolean shutdownRequested = false;

        @Override
        public State blockNodeState() {
            return shutdownRequested ? State.SHUTTING_DOWN : State.RUNNING;
        }

        @Override
        public void shutdown(final String className, final String reason) {
            shutdownRequested = true;
        }
    }

    // ── Fixtures ───────────────────────────────────────────────────────────

    /// Write a `BlockStreamInfo` singleton directly via the raw `BinaryState` API, matching what
    /// `StateChangeApplier` would store from a real `SingletonUpdateChange` for this state ID —
    /// used to manufacture a seed snapshot that restore's self-consistency check (§5.1/§6.2)
    /// will accept.
    private static void seedBlockStreamInfo(final VirtualMapState state, final long blockNumber) {
        state.updateSingleton(
                StateIdentifier.STATE_ID_BLOCK_STREAM_INFO.protoOrdinal(),
                BlockStreamInfo.PROTOBUF.toBytes(
                        BlockStreamInfo.newBuilder().blockNumber(blockNumber).build()));
    }

    private static StateManagementPlugin startPlugin(final Path recentRoot, final TestBlockMessagingFacility facility) {
        final var configuration = ConfigurationBuilder.create()
                .withConfigDataType(StateManagementConfig.class)
                .withConfigDataType(MerkleDbConfig.class)
                .withConfigDataType(VirtualMapConfig.class)
                .withValue("state.management.snapshotRecentPath", recentRoot.toString())
                .build();
        final BlockNodeContext context = new BlockNodeContext(
                configuration,
                MetricRegistry.builder().build(),
                null,
                facility,
                null,
                null,
                null,
                StateManagementPluginTestSupport.newThreadPoolManager(),
                null);
        final StateManagementPlugin plugin = new StateManagementPlugin();
        plugin.init(context, NOOP_SERVICE_BUILDER);
        plugin.start();
        try {
            StateManagementPluginTestSupport.awaitReady(plugin, 5_000L);
        } catch (final InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
        return plugin;
    }

    private static final RecordingServiceBuilder NOOP_SERVICE_BUILDER = new RecordingServiceBuilder();

    private static BlockUnparsed buildBlock(final long blockNumber, final long roundNumber) {
        return buildBlock(blockNumber, roundNumber, Bytes.EMPTY);
    }

    private static BlockUnparsed buildBlock(
            final long blockNumber, final long roundNumber, final Bytes startOfBlockStateRootHash) {
        return BlockUnparsed.newBuilder()
                .blockItems(
                        BlockItemUnparsed.newBuilder()
                                .blockHeader(BlockHeader.PROTOBUF.toBytes(BlockHeader.newBuilder()
                                        .number(blockNumber)
                                        .build()))
                                .build(),
                        BlockItemUnparsed.newBuilder()
                                .roundHeader(RoundHeader.PROTOBUF.toBytes(RoundHeader.newBuilder()
                                        .roundNumber(roundNumber)
                                        .build()))
                                .build(),
                        BlockItemUnparsed.newBuilder()
                                .blockFooter(BlockFooter.PROTOBUF.toBytes(BlockFooter.newBuilder()
                                        .startOfBlockStateRootHash(startOfBlockStateRootHash)
                                        .build()))
                                .build())
                .build();
    }
}
