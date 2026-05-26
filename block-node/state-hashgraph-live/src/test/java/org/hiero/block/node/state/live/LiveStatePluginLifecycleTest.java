// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.state.live;

import static org.assertj.core.api.Assertions.assertThat;

import com.hedera.hapi.block.stream.input.RoundHeader;
import com.hedera.hapi.block.stream.output.BlockFooter;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.config.api.ConfigurationBuilder;
import com.swirlds.merkledb.config.MerkleDbConfig;
import com.swirlds.virtualmap.config.VirtualMapConfig;
import org.hiero.consensus.config.PathsConfig;
import java.nio.file.Files;
import java.nio.file.Path;
import com.hedera.pbj.runtime.grpc.ServiceInterface;
import edu.umd.cs.findbugs.annotations.NonNull;
import org.hiero.block.api.StateMetadata;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.app.fixtures.plugintest.TestBlockMessagingFacility;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.ServiceBuilder;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.StateUpdateNotification;
import org.hiero.block.node.spi.blockmessaging.StateUpdateNotification.StateUpdateType;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import io.helidon.webserver.http.HttpService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Exercises the {@link LiveStatePlugin} lifecycle end-to-end against an in-tree fixture
 * (no swirlds-state-impl, no VirtualMap). Covers:
 *
 * <ul>
 *   <li>start writes no metadata for an empty filesystem (genesis stays implicit);</li>
 *   <li>a verified block applied via the synchronous apply hook advances metadata and
 *       emits a {@code VERIFIED} {@link StateUpdateNotification};</li>
 *   <li>{@code saveSnapshotNow} writes the snapshot binary, the metadata JSON, and emits
 *       a {@code SNAPSHOT} notification;</li>
 *   <li>a fresh plugin pointing at the same directories restores the saved state.</li>
 * </ul>
 */
class LiveStatePluginLifecycleTest {

    @Test
    void applyAndSnapshotAndReloadEndToEnd(@TempDir final Path tmp) throws Exception {
        final Path metadataPath = tmp.resolve("stateMetadata.json");
        final Path recentRoot = tmp.resolve("snapshot/recent");
        final Path historicRoot = tmp.resolve("snapshot/historic");

        // First plugin lifecycle — apply a synthetic block 1, snapshot, stop.
        final TestBlockMessagingFacility facility = new TestBlockMessagingFacility();
        final LiveStatePlugin plugin = startPlugin(metadataPath, recentRoot, historicRoot, facility);

        assertThat(plugin.awaitReady(5_000L)).isTrue();
        assertThat(plugin.metadata()).isEqualTo(StateMetadata.DEFAULT);

        final BlockUnparsed block = buildBlock(1L, 11L);
        facility.sendBlockVerification(new VerificationNotification(
                true, null, 1L, Bytes.fromHex("aabb"), block, BlockSource.PUBLISHER));

        plugin.applyPendingNow();
        assertThat(plugin.metadata().blockNumber()).isEqualTo(1L);
        assertThat(plugin.metadata().roundNumber()).isEqualTo(11L);
        assertThat(facility.getSentStateUpdateNotifications())
                .anyMatch(n -> n.type() == StateUpdateType.VERIFIED && n.blockNumber() == 1L);

        plugin.saveSnapshotNow();
        assertThat(Files.exists(metadataPath)).isTrue();
        // VirtualMapStateLifecycleManager.createSnapshot writes a directory tree (per the
        // consensus-node state-snapshot spec). Assert the directory exists and is non-empty.
        final Path snapshotDir = recentRoot.resolve("1");
        assertThat(Files.isDirectory(snapshotDir)).isTrue();
        try (var stream = Files.list(snapshotDir)) {
            assertThat(stream.findAny()).as("snapshot dir must contain at least one file").isPresent();
        }
        assertThat(facility.getSentStateUpdateNotifications())
                .anyMatch(n -> n.type() == StateUpdateType.SNAPSHOT && n.blockNumber() == 1L);

        plugin.stop();

        // Second plugin lifecycle — fresh instance, same paths, must load metadata + snapshot.
        final TestBlockMessagingFacility facility2 = new TestBlockMessagingFacility();
        final LiveStatePlugin plugin2 = startPlugin(metadataPath, recentRoot, historicRoot, facility2);

        assertThat(plugin2.metadata().blockNumber()).isEqualTo(1L);
        assertThat(plugin2.metadata().roundNumber()).isEqualTo(11L);
        plugin2.stop();
    }

    @Test
    void rejectsBlockWithUnparseableHeader(@TempDir final Path tmp) throws Exception {
        final TestBlockMessagingFacility facility = new TestBlockMessagingFacility();
        final LiveStatePlugin plugin = startPlugin(
                tmp.resolve("md.json"), tmp.resolve("recent"), tmp.resolve("historic"), facility);

        final BlockUnparsed corrupt = BlockUnparsed.newBuilder()
                .blockItems(BlockItemUnparsed.newBuilder()
                        .blockHeader(Bytes.fromHex("ffffffff")) // not a valid BlockHeader proto
                        .build())
                .build();
        facility.sendBlockVerification(new VerificationNotification(
                true, null, 1L, Bytes.fromHex("aabb"), corrupt, BlockSource.PUBLISHER));
        plugin.applyPendingNow();

        assertThat(plugin.metadata()).isEqualTo(StateMetadata.DEFAULT);
        assertThat(facility.getSentStateUpdateNotifications()).isEmpty();
        plugin.stop();
    }

    // ── Fixtures ───────────────────────────────────────────────────────────

    private static LiveStatePlugin startPlugin(
            final Path metadataPath,
            final Path recentRoot,
            final Path historicRoot,
            final TestBlockMessagingFacility facility) {
        final var configuration = ConfigurationBuilder.create()
                .withConfigDataType(LiveStateConfig.class)
                .withConfigDataType(MerkleDbConfig.class)
                .withConfigDataType(VirtualMapConfig.class)
                .withConfigDataType(PathsConfig.class)
                .withValue("state.live.stateMetadataPath", metadataPath.toString())
                .withValue("state.live.stateSnapshotRecentPath", recentRoot.toString())
                .withValue("state.live.stateSnapshotHistoricPath", historicRoot.toString())
                .withValue("state.live.snapshotIntervalMillis", "3600000") // suppress automatic snapshot
                .withValue("state.live.stateChangesApplyIntervalMillis", "3600000") // suppress automatic apply
                .build();
        final BlockNodeContext context = new BlockNodeContext(
                configuration, null, null, facility, null, null, null, null, null, null, null);
        final LiveStatePlugin plugin = new LiveStatePlugin();
        plugin.init(context, NOOP_SERVICE_BUILDER);
        plugin.start();
        try {
            plugin.awaitReady(5_000L);
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
