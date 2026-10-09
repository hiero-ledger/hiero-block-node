// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.state.management;

import static org.assertj.core.api.Assertions.assertThat;

import com.hedera.hapi.block.stream.input.RoundHeader;
import com.hedera.hapi.block.stream.output.BlockFooter;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.base.time.Time;
import com.swirlds.config.api.ConfigurationBuilder;
import com.swirlds.merkledb.config.MerkleDbConfig;
import com.swirlds.state.merkle.VirtualMapState;
import com.swirlds.state.merkle.VirtualMapStateLifecycleManager;
import com.swirlds.virtualmap.config.VirtualMapConfig;
import java.nio.file.Files;
import java.nio.file.Path;
import org.hiero.base.file.FileSystemManager;
import org.hiero.block.api.StateMetadata;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.app.fixtures.TestMetricsExporter;
import org.hiero.block.node.app.fixtures.plugintest.RecordingServiceBuilder;
import org.hiero.block.node.app.fixtures.plugintest.TestBlockMessagingFacility;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import org.hiero.block.node.spi.health.HealthFacility;
import org.hiero.consensus.config.PathsConfig;
import org.hiero.consensus.metrics.noop.NoOpMetrics;
import org.hiero.metrics.core.MetricRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/// Exercises the `StateManagementPlugin` lifecycle end-to-end against an in-tree fixture (no
/// real VirtualMap-backed state). Covers startup, the lag-1 apply pipeline, apply-halt behavior,
/// and snapshot restore. Snapshot creation and gRPC query are not yet implemented; see follow-up
/// PRs.
class StateManagementPluginLifecycleTest {

    @Test
    void rejectsBlockWithUnparseableHeader(@TempDir final Path tmp) throws Exception {
        final TestBlockMessagingFacility facility = new TestBlockMessagingFacility();
        final StateManagementPlugin plugin = startPlugin(tmp.resolve("md.json"), tmp.resolve("recent"), facility);

        final BlockUnparsed corrupt = BlockUnparsed.newBuilder()
                .blockItems(BlockItemUnparsed.newBuilder()
                        .blockHeader(Bytes.fromHex("ffffffff")) // not a valid BlockHeader proto
                        .build())
                .build();
        facility.sendBlockVerification(
                new VerificationNotification(true, null, 1L, Bytes.fromHex("aabb"), corrupt, BlockSource.PUBLISHER));
        plugin.applyPending();

        assertThat(plugin.metadata()).isEqualTo(StateMetadata.DEFAULT);
        plugin.stop();
    }

    @Test
    void missingSnapshotForPersistedMetadataFallsBackToGenesis(@TempDir final Path tmp) throws Exception {
        final Path metadataPath = tmp.resolve("stateMetadata.json");
        final Path recentRoot = tmp.resolve("recent");
        // Persist metadata pointing at block 5 but never create its recent/5 snapshot dir.
        // On start the plugin cannot load state for block 5, so it must reset to genesis
        // rather than claim state it does not actually hold.
        new StateMetadataStore(metadataPath)
                .save(StateMetadata.newBuilder()
                        .blockNumber(5L)
                        .roundNumber(50L)
                        .stateRootHash(Bytes.fromHex("abcd"))
                        .stateSize(7L)
                        .build());

        final TestBlockMessagingFacility facility = new TestBlockMessagingFacility();
        final StateManagementPlugin plugin = startPlugin(metadataPath, recentRoot, facility);

        assertThat(plugin.metadata()).isEqualTo(StateMetadata.DEFAULT);
        assertThat(plugin.isApplyHalted()).isFalse();
        plugin.stop();
    }

    @Test
    void startupRestoresFromExistingSnapshot(@TempDir final Path tmp) throws Exception {
        final Path metadataPath = tmp.resolve("stateMetadata.json");
        final Path recentRoot = tmp.resolve("recent");
        Files.createDirectories(recentRoot);

        // Manufacture a snapshot the same way a future snapshot-creation plugin slice would —
        // directly via VirtualMapStateLifecycleManager's own public API, independent of this
        // plugin (which does not create snapshots yet).
        final var seedConfig = ConfigurationBuilder.create()
                .withConfigDataType(MerkleDbConfig.class)
                .withConfigDataType(VirtualMapConfig.class)
                .withConfigDataType(PathsConfig.class)
                .build();
        final var seedManager = new VirtualMapStateLifecycleManager(
                new NoOpMetrics(), Time.getCurrent(), seedConfig, new FileSystemManager(recentRoot));
        seedManager.copyMutableState();
        seedManager.getMutableState().updateSingleton(1, Bytes.fromHex("c0ffee"));
        seedManager.copyMutableState();
        final VirtualMapState sealed = seedManager.getLatestImmutableState();
        final Bytes rootHash = Bytes.wrap(sealed.getRoot().getHash().copyToByteArray());
        seedManager.createSnapshot(sealed, recentRoot.resolve("5"));
        new StateMetadataStore(metadataPath)
                .save(StateMetadata.newBuilder()
                        .blockNumber(5L)
                        .roundNumber(50L)
                        .stateRootHash(rootHash)
                        .stateSize(1L)
                        .build());

        final TestBlockMessagingFacility facility = new TestBlockMessagingFacility();
        final StateManagementPlugin plugin = startPlugin(metadataPath, recentRoot, facility);

        assertThat(plugin.metadata().blockNumber()).isEqualTo(5L);
        assertThat(plugin.metadata().roundNumber()).isEqualTo(50L);

        // The restored state becomes state 2 (hashingImmutable) too — block 6's footer must
        // carry the same root hash to be accepted, proving restore seeded real state content,
        // not just the metadata pointer.
        facility.sendBlockVerification(new VerificationNotification(
                true, null, 6L, Bytes.fromHex("aabb"), buildBlock(6L, 60L, rootHash), BlockSource.PUBLISHER));
        plugin.applyPending();
        assertThat(plugin.isApplyHalted()).isFalse();

        plugin.stop();
    }

    @Test
    void malformedStateChangesHaltsApplyWithoutApplying(@TempDir final Path tmp) throws Exception {
        final TestBlockMessagingFacility facility = new TestBlockMessagingFacility();
        final StateManagementPlugin plugin = startPlugin(tmp.resolve("md.json"), tmp.resolve("recent"), facility);

        // Genesis block 0 with a valid header/footer (empty start hash passes genesis
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
                                        .startOfBlockStateRootHash(Bytes.EMPTY)
                                        .build()))
                                .build())
                .build();
        facility.sendBlockVerification(
                new VerificationNotification(true, null, 0L, Bytes.fromHex("aabb"), badBlock, BlockSource.PUBLISHER));
        plugin.applyPending();

        assertThat(plugin.isApplyHalted()).isTrue();
        assertThat(plugin.metadata()).isEqualTo(StateMetadata.DEFAULT);
        plugin.stop();
    }

    @Test
    void restartAfterApplyHaltStartsClean(@TempDir final Path tmp) throws Exception {
        final Path metadataPath = tmp.resolve("stateMetadata.json");
        final Path recentRoot = tmp.resolve("recent");
        final TestBlockMessagingFacility facility = new TestBlockMessagingFacility();
        final StateManagementPlugin plugin = startPlugin(metadataPath, recentRoot, facility);

        // Apply genesis block 0 (staged), then deliver block 1 whose footer start hash does
        // not match post-0 — a hash mismatch that halts apply.
        facility.sendBlockVerification(new VerificationNotification(
                true, null, 0L, Bytes.fromHex("aabb"), buildBlock(0L, 0L), BlockSource.PUBLISHER));
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
        final StateManagementPlugin plugin2 = startPlugin(metadataPath, recentRoot, facility2);
        assertThat(plugin2.isApplyHalted()).isFalse();
        assertThat(StateManagementPluginTestSupport.awaitReady(plugin2, 5_000L)).isTrue();
        plugin2.stop();
    }

    @Test
    void pendingBlocksStopsGrowingOnceApplyHalted(@TempDir final Path tmp) throws Exception {
        // handleVerification must stop staging new blocks once apply is halted, since
        // applyPending() never drains pendingBlocks again until a restart — otherwise every
        // subsequent verified block accumulates in memory without bound. Reads the real
        // state_pending_blocks gauge via the established TestMetricsExporter pattern.
        final TestMetricsExporter metricsExporter = new TestMetricsExporter();
        final var configuration = ConfigurationBuilder.create()
                .withConfigDataType(StateManagementConfig.class)
                .withConfigDataType(MerkleDbConfig.class)
                .withConfigDataType(VirtualMapConfig.class)
                .withConfigDataType(PathsConfig.class)
                .withValue(
                        "state.management.stateMetadataPath",
                        tmp.resolve("md.json").toString())
                .withValue(
                        "state.management.stateSnapshotRecentPath",
                        tmp.resolve("recent").toString())
                .build();
        final TestBlockMessagingFacility facility = new TestBlockMessagingFacility();
        final BlockNodeContext context = new BlockNodeContext(
                configuration,
                MetricRegistry.builder().setMetricsExporter(metricsExporter).build(),
                null,
                facility,
                null,
                null,
                null,
                null,
                null);
        final StateManagementPlugin plugin = new StateManagementPlugin();
        plugin.init(context, NOOP_SERVICE_BUILDER);
        plugin.start();
        assertThat(StateManagementPluginTestSupport.awaitReady(plugin, 5_000L)).isTrue();

        // Halt apply via the same hash-mismatch recipe as restartAfterApplyHaltStartsClean:
        // apply genesis block 0, then deliver block 1 with a footer start hash that doesn't
        // match.
        facility.sendBlockVerification(new VerificationNotification(
                true, null, 0L, Bytes.fromHex("aabb"), buildBlock(0L, 0L), BlockSource.PUBLISHER));
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

        final long pendingAtApplyHalted =
                metricsExporter.getMetricValue(StateManagementPlugin.METRIC_PENDING_BLOCKS.name());

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

        assertThat(metricsExporter.getMetricValue(StateManagementPlugin.METRIC_PENDING_BLOCKS.name()))
                .as("pendingBlocks must not grow once apply is halted")
                .isEqualTo(pendingAtApplyHalted);
        plugin.stop();
    }

    @Test
    void unwritableStateDirRequestsShutdownWithoutThrowing(@TempDir final Path tmp) throws Exception {
        // Make the configured recent-snapshot path impossible to create: its parent is a
        // regular file, so directory creation fails (mirrors a non-writable /opt/hiero in a
        // real deployment). init() must log and request a graceful node shutdown via the health
        // facility rather than throw — an unchecked exception out of init() would abort
        // BlockNodeApp construction and take down every other plugin.
        final Path blocker = tmp.resolve("blocker");
        Files.writeString(blocker, "not a directory");
        final Path badRecent = blocker.resolve("state/recent");

        final var configuration = ConfigurationBuilder.create()
                .withConfigDataType(StateManagementConfig.class)
                .withConfigDataType(MerkleDbConfig.class)
                .withConfigDataType(VirtualMapConfig.class)
                .withConfigDataType(PathsConfig.class)
                .withValue(
                        "state.management.stateMetadataPath",
                        tmp.resolve("md.json").toString())
                .withValue("state.management.stateSnapshotRecentPath", badRecent.toString())
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
                .as("init() requests a graceful node shutdown on an unusable state dir")
                .isTrue();
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

    private static StateManagementPlugin startPlugin(
            final Path metadataPath, final Path recentRoot, final TestBlockMessagingFacility facility) {
        final var configuration = ConfigurationBuilder.create()
                .withConfigDataType(StateManagementConfig.class)
                .withConfigDataType(MerkleDbConfig.class)
                .withConfigDataType(VirtualMapConfig.class)
                .withConfigDataType(PathsConfig.class)
                .withValue("state.management.stateMetadataPath", metadataPath.toString())
                .withValue("state.management.stateSnapshotRecentPath", recentRoot.toString())
                .build();
        final BlockNodeContext context = new BlockNodeContext(
                configuration, MetricRegistry.builder().build(), null, facility, null, null, null, null, null);
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
