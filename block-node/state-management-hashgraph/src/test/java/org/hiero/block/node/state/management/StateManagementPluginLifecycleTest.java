// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.state.management;

import static org.assertj.core.api.Assertions.assertThat;

import com.hedera.hapi.block.stream.input.RoundHeader;
import com.hedera.hapi.block.stream.output.BlockFooter;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.pbj.runtime.grpc.ServiceInterface;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.config.api.ConfigurationBuilder;
import com.swirlds.merkledb.config.MerkleDbConfig;
import com.swirlds.virtualmap.config.VirtualMapConfig;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.helidon.webserver.http.HttpService;
import java.nio.file.Files;
import java.nio.file.Path;
import org.hiero.block.api.StateMetadata;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.app.fixtures.plugintest.TestBlockMessagingFacility;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.ServiceBuilder;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import org.hiero.consensus.config.PathsConfig;
import org.hiero.metrics.core.MetricRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Exercises the {@link StateManagementPlugin} lifecycle end-to-end against an in-tree fixture
 * (no swirlds-state-impl, no VirtualMap). Covers:
 *
 * <ul>
 *   <li>start writes no metadata for an empty filesystem (genesis stays implicit);</li>
 *   <li>a verified block applied via the synchronous apply hook, confirmed by the next
 *       block, advances the exposed metadata (lag-1);</li>
 *   <li>{@code saveSnapshot()} writes the snapshot binary and the metadata JSON;</li>
 *   <li>a fresh plugin pointing at the same directories restores the saved state.</li>
 * </ul>
 */
class StateManagementPluginLifecycleTest {

    @Test
    void applyAndSnapshotAndReloadEndToEnd(@TempDir final Path tmp) throws Exception {
        final Path metadataPath = tmp.resolve("stateMetadata.json");
        final Path recentRoot = tmp.resolve("snapshot/recent");

        // First plugin lifecycle — apply a synthetic block 1, snapshot, stop.
        final TestBlockMessagingFacility facility = new TestBlockMessagingFacility();
        final StateManagementPlugin plugin = startPlugin(metadataPath, recentRoot, facility);

        assertThat(StateManagementPluginTestSupport.awaitReady(plugin, 5_000L)).isTrue();
        assertThat(plugin.metadata()).isEqualTo(StateMetadata.DEFAULT);

        final BlockUnparsed block = buildBlock(1L, 11L);
        facility.sendBlockVerification(
                new VerificationNotification(true, null, 1L, Bytes.fromHex("aabb"), block, BlockSource.PUBLISHER));

        plugin.applyPending(); // block 1 applied (staged); under lag-1 not yet exposed

        // Confirm block 1 with an empty block 2 (footer chained to the staged hash) so
        // block 1 is attested and exposed to readers / snapshots.
        facility.sendBlockVerification(new VerificationNotification(
                true,
                null,
                2L,
                Bytes.fromHex("aabb"),
                buildBlock(2L, 22L, plugin.stagedStateRootHash()),
                BlockSource.PUBLISHER));
        plugin.applyPending();
        assertThat(plugin.metadata().blockNumber()).isEqualTo(1L);
        assertThat(plugin.metadata().roundNumber()).isEqualTo(11L);

        plugin.saveSnapshot();
        assertThat(Files.exists(metadataPath)).isTrue();
        // VirtualMapStateLifecycleManager.createSnapshot writes a directory tree (per the
        // consensus-node state-snapshot spec). Assert the directory exists and is non-empty.
        final Path snapshotDir = recentRoot.resolve("1");
        assertThat(Files.isDirectory(snapshotDir)).isTrue();
        try (var stream = Files.list(snapshotDir)) {
            assertThat(stream.findAny())
                    .as("snapshot dir must contain at least one file")
                    .isPresent();
        }

        plugin.stop();

        // Second plugin lifecycle — fresh instance, same paths, must load metadata + snapshot.
        final TestBlockMessagingFacility facility2 = new TestBlockMessagingFacility();
        final StateManagementPlugin plugin2 = startPlugin(metadataPath, recentRoot, facility2);

        assertThat(plugin2.metadata().blockNumber()).isEqualTo(1L);
        assertThat(plugin2.metadata().roundNumber()).isEqualTo(11L);
        plugin2.stop();
    }

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
        assertThat(plugin.isDegraded()).isFalse();
        plugin.stop();
    }

    @Test
    void malformedStateChangesDegradesWithoutApplying(@TempDir final Path tmp) throws Exception {
        final TestBlockMessagingFacility facility = new TestBlockMessagingFacility();
        final StateManagementPlugin plugin = startPlugin(tmp.resolve("md.json"), tmp.resolve("recent"), facility);

        // Genesis block 0 with a valid header/footer (empty start hash passes genesis
        // validation) but a state_changes item carrying invalid protobuf bytes. The
        // applier throws; applyPending must degrade and leave the block unapplied.
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

        assertThat(plugin.isDegraded()).isTrue();
        assertThat(plugin.metadata()).isEqualTo(StateMetadata.DEFAULT);
        plugin.stop();
    }

    @Test
    void stopWritesFinalSnapshotForExposedBlock(@TempDir final Path tmp) throws Exception {
        final Path metadataPath = tmp.resolve("stateMetadata.json");
        final Path recentRoot = tmp.resolve("recent");
        final TestBlockMessagingFacility facility = new TestBlockMessagingFacility();
        final StateManagementPlugin plugin = startPlugin(metadataPath, recentRoot, facility);

        // Apply block 1 and confirm it with block 2 so block 1 is exposed (metadata.block=1).
        facility.sendBlockVerification(new VerificationNotification(
                true, null, 1L, Bytes.fromHex("aabb"), buildBlock(1L, 11L), BlockSource.PUBLISHER));
        plugin.applyPending();
        facility.sendBlockVerification(new VerificationNotification(
                true,
                null,
                2L,
                Bytes.fromHex("aabb"),
                buildBlock(2L, 22L, plugin.stagedStateRootHash()),
                BlockSource.PUBLISHER));
        plugin.applyPending();
        assertThat(plugin.metadata().blockNumber()).isEqualTo(1L);

        // No explicit saveSnapshot() here — stop() must write the final snapshot for the
        // exposed block since it was never snapshotted during the run.
        plugin.stop();
        assertThat(Files.isDirectory(recentRoot.resolve("1"))).isTrue();
    }

    @Test
    void restartAfterDegradeStartsClean(@TempDir final Path tmp) throws Exception {
        final Path metadataPath = tmp.resolve("stateMetadata.json");
        final Path recentRoot = tmp.resolve("recent");
        final TestBlockMessagingFacility facility = new TestBlockMessagingFacility();
        final StateManagementPlugin plugin = startPlugin(metadataPath, recentRoot, facility);

        // Apply genesis block 0 (staged), then deliver block 1 whose footer start hash does
        // not match post-0 — a hash mismatch that degrades the plugin.
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
        assertThat(plugin.isDegraded()).isTrue();
        plugin.stop();

        // Degraded state is in-memory only (the documented recovery is a restart). A fresh
        // instance must start clean and reach readiness.
        final TestBlockMessagingFacility facility2 = new TestBlockMessagingFacility();
        final StateManagementPlugin plugin2 = startPlugin(metadataPath, recentRoot, facility2);
        assertThat(plugin2.isDegraded()).isFalse();
        assertThat(StateManagementPluginTestSupport.awaitReady(plugin2, 5_000L)).isTrue();
        plugin2.stop();
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
                .withValue("state.management.snapshotIntervalMillis", "3600000") // suppress automatic snapshot
                .withValue("state.management.stateChangesApplyIntervalMillis", "3600000") // suppress automatic apply
                .build();
        final BlockNodeContext context = new BlockNodeContext(
                configuration,
                MetricRegistry.builder().build(),
                null,
                facility,
                null,
                null,
                null,
                null,
                null,
                null,
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

    private static final ServiceBuilder NOOP_SERVICE_BUILDER = new ServiceBuilder() {
        @Override
        public void registerHttpService(@NonNull final String path, final HttpService... service) {}

        @Override
        public void registerGrpcService(@NonNull final ServiceInterface service) {}
    };

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
