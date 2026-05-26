// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.state.live;

import static org.assertj.core.api.Assertions.assertThat;

import com.hedera.hapi.block.stream.input.RoundHeader;
import com.hedera.hapi.block.stream.output.BlockFooter;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.pbj.runtime.grpc.ServiceInterface;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.config.api.ConfigurationBuilder;
import com.swirlds.merkledb.config.MerkleDbConfig;
import com.swirlds.virtualmap.config.VirtualMapConfig;
import org.hiero.consensus.config.PathsConfig;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.helidon.webserver.http.HttpService;
import java.nio.file.Path;
import org.hiero.block.api.StateMetadata;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.app.fixtures.plugintest.TestBlockMessagingFacility;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.ServiceBuilder;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.StateUpdateNotification.StateUpdateType;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Acceptance scenarios from {@code docs/design/state/live-state.md §11}.
 *
 * <ol>
 *   <li>Genesis load → first verified block (any number) applies and metadata advances.</li>
 *   <li>After metadata.block=n, verification of n+1 applies cleanly and emits a VERIFIED
 *       StateUpdateNotification carrying n+1.</li>
 *   <li>After metadata.block=n, verification of n+2 (gap) does not apply.</li>
 *   <li>Chained n+1 then n+2 both apply in order; the final metadata is at n+2.</li>
 *   <li>Snapshot persists metadata.json + a snapshot file under recent/&lt;blockNumber&gt;.</li>
 * </ol>
 *
 * The plugin owns its own apply scheduler; tests drive {@code applyPendingNow} to
 * remove timing flakiness.
 */
class LiveStateAcceptanceTest {

    @Test
    void scenario1_genesisAppliesFirstBlock(@TempDir final Path tmp) {
        final Fixture f = startPlugin(tmp);

        // At genesis the footer's startOfBlockStateRootHash must be empty / all-zeros.
        f.deliverBlock(0L, 0L, Bytes.EMPTY);
        f.plugin.applyPendingNow();

        assertThat(f.plugin.metadata().blockNumber()).isZero();
        assertThat(f.facility.getSentStateUpdateNotifications())
                .anyMatch(n -> n.type() == StateUpdateType.VERIFIED && n.blockNumber() == 0L);
        assertThat(f.plugin.isDegraded()).isFalse();
        f.plugin.stop();
    }

    @Test
    void scenario2_nextBlockOnTopOfMetadataApplies(@TempDir final Path tmp) {
        final Fixture f = startPlugin(tmp);

        f.deliverBlock(5L, 50L, Bytes.EMPTY); // genesis lane accepts arbitrary first block number
        f.plugin.applyPendingNow();
        assertThat(f.plugin.metadata().blockNumber()).isEqualTo(5L);

        final Bytes liveAfter5 = f.plugin.metadata().stateRootHash();
        f.deliverBlock(6L, 60L, liveAfter5);
        f.plugin.applyPendingNow();
        assertThat(f.plugin.metadata().blockNumber()).isEqualTo(6L);
        assertThat(f.plugin.metadata().roundNumber()).isEqualTo(60L);

        f.plugin.stop();
    }

    @Test
    void scenario3_gapBlockIsNotApplied(@TempDir final Path tmp) {
        final Fixture f = startPlugin(tmp);

        f.deliverBlock(5L, 50L, Bytes.EMPTY);
        f.plugin.applyPendingNow();
        assertThat(f.plugin.metadata().blockNumber()).isEqualTo(5L);
        final Bytes liveAfter5 = f.plugin.metadata().stateRootHash();

        // Block 7 — skips 6. Footer hash is fine (no state changes in between) but
        // the apply loop will not advance past the gap.
        f.deliverBlock(7L, 70L, liveAfter5);
        f.plugin.applyPendingNow();
        assertThat(f.plugin.metadata().blockNumber()).isEqualTo(5L);

        f.deliverBlock(6L, 60L, liveAfter5);
        f.plugin.applyPendingNow();
        assertThat(f.plugin.metadata().blockNumber()).isEqualTo(7L);

        f.plugin.stop();
    }

    @Test
    void scenario4_chainedBlocksApplyInOrder(@TempDir final Path tmp) {
        final Fixture f = startPlugin(tmp);
        Bytes startHash = Bytes.EMPTY;
        for (long i = 0; i < 3; i++) {
            f.deliverBlock(i, i, startHash);
            f.plugin.applyPendingNow();
            startHash = f.plugin.metadata().stateRootHash();
        }

        assertThat(f.plugin.metadata().blockNumber()).isEqualTo(2L);
        assertThat(f.plugin.metadata().roundNumber()).isEqualTo(2L);
        assertThat(f.facility.getSentStateUpdateNotifications()
                        .stream()
                        .filter(n -> n.type() == StateUpdateType.VERIFIED)
                        .count())
                .isEqualTo(3L);
        f.plugin.stop();
    }

    @Test
    void scenario6_refusesApplyOnHashMismatch(@TempDir final Path tmp) {
        final Fixture f = startPlugin(tmp);

        f.deliverBlock(5L, 50L, Bytes.EMPTY); // genesis lane
        f.plugin.applyPendingNow();
        assertThat(f.plugin.metadata().blockNumber()).isEqualTo(5L);

        // Block 6 with a deliberately bogus startOfBlockStateRootHash.
        final Bytes bogus = Bytes.fromHex("deadbeef".repeat(12)); // 48 bytes != live hash
        f.deliverBlock(6L, 60L, bogus);
        f.plugin.applyPendingNow();

        assertThat(f.plugin.metadata().blockNumber()).isEqualTo(5L);
        assertThat(f.plugin.isDegraded()).isTrue();
        assertThat(f.plugin.hashMismatchTotal()).isEqualTo(1L);
        f.plugin.stop();
    }

    @Test
    void scenario5_snapshotPersistsMetadataAndSnapshotFile(@TempDir final Path tmp) throws java.io.IOException {
        final Fixture f = startPlugin(tmp);
        f.deliverBlock(1L, 10L, Bytes.EMPTY);
        f.plugin.applyPendingNow();
        f.plugin.saveSnapshotNow();

        assertThat(tmp.resolve("md.json").toFile()).exists();
        // VirtualMapStateLifecycleManager.createSnapshot writes a directory tree, not a single file.
        final java.nio.file.Path snapshotDir = tmp.resolve("recent").resolve("1");
        assertThat(java.nio.file.Files.isDirectory(snapshotDir)).isTrue();
        try (var stream = java.nio.file.Files.list(snapshotDir)) {
            assertThat(stream.findAny()).isPresent();
        }
        assertThat(f.facility.getSentStateUpdateNotifications())
                .anyMatch(n -> n.type() == StateUpdateType.SNAPSHOT && n.blockNumber() == 1L);

        f.plugin.stop();

        // A second plugin pointing at the same dirs picks up the persisted metadata.
        final Fixture g = startPlugin(tmp);
        assertThat(g.plugin.metadata()).isNotEqualTo(StateMetadata.DEFAULT);
        assertThat(g.plugin.metadata().blockNumber()).isEqualTo(1L);
        g.plugin.stop();
    }

    // ── Fixtures ───────────────────────────────────────────────────────────

    private record Fixture(LiveStatePlugin plugin, TestBlockMessagingFacility facility) {
        /** Deliver a block whose footer carries the supplied {@code startOfBlockStateRootHash}. */
        void deliverBlock(final long blockNumber, final long roundNumber, @NonNull final Bytes startHash) {
            final BlockUnparsed block = BlockUnparsed.newBuilder()
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
                                            .startOfBlockStateRootHash(startHash)
                                            .build()))
                                    .build())
                    .build();
            facility.sendBlockVerification(new VerificationNotification(
                    true, null, blockNumber, Bytes.fromHex("00"), block, BlockSource.PUBLISHER));
        }

        /** Current mutable state root hash — used by tests to chain block footers correctly. */
        @NonNull
        Bytes currentMutableHash() {
            try {
                return Bytes.wrap(plugin.lifecycleManager()
                        .getMutableState()
                        .getRoot()
                        .getHash()
                        .copyToByteArray());
            } catch (final RuntimeException e) {
                return Bytes.EMPTY;
            }
        }
    }

    private static Fixture startPlugin(final Path tmp) {
        final TestBlockMessagingFacility facility = new TestBlockMessagingFacility();
        final var configuration = ConfigurationBuilder.create()
                .withConfigDataType(LiveStateConfig.class)
                .withConfigDataType(MerkleDbConfig.class)
                .withConfigDataType(VirtualMapConfig.class)
                .withConfigDataType(PathsConfig.class)
                .withValue("state.live.stateMetadataPath", tmp.resolve("md.json").toString())
                .withValue("state.live.stateSnapshotRecentPath", tmp.resolve("recent").toString())
                .withValue("state.live.stateSnapshotHistoricPath", tmp.resolve("historic").toString())
                .withValue("state.live.snapshotIntervalMillis", "3600000")
                .withValue("state.live.stateChangesApplyIntervalMillis", "3600000")
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
        return new Fixture(plugin, facility);
    }

    private static final ServiceBuilder NOOP_SERVICE_BUILDER = new ServiceBuilder() {
        @Override
        public void registerHttpService(@NonNull final String path, final HttpService... service) {}

        @Override
        public void registerGrpcService(@NonNull final ServiceInterface service) {}
    };
}
