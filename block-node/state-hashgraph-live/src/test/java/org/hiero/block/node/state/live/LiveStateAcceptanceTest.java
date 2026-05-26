// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.state.live;

import static org.assertj.core.api.Assertions.assertThat;

import com.hedera.hapi.block.stream.input.RoundHeader;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.pbj.runtime.grpc.ServiceInterface;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.config.api.ConfigurationBuilder;
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

        f.deliverBlock(0L, 0L);
        f.plugin.applyPendingNow();

        assertThat(f.plugin.metadata().blockNumber()).isZero();
        assertThat(f.facility.getSentStateUpdateNotifications())
                .anyMatch(n -> n.type() == StateUpdateType.VERIFIED && n.blockNumber() == 0L);
        f.plugin.stop();
    }

    @Test
    void scenario2_nextBlockOnTopOfMetadataApplies(@TempDir final Path tmp) {
        final Fixture f = startPlugin(tmp);

        f.deliverBlock(5L, 50L);
        f.plugin.applyPendingNow();
        assertThat(f.plugin.metadata().blockNumber()).isEqualTo(5L);

        f.deliverBlock(6L, 60L);
        f.plugin.applyPendingNow();
        assertThat(f.plugin.metadata().blockNumber()).isEqualTo(6L);
        assertThat(f.plugin.metadata().roundNumber()).isEqualTo(60L);

        f.plugin.stop();
    }

    @Test
    void scenario3_gapBlockIsNotApplied(@TempDir final Path tmp) {
        final Fixture f = startPlugin(tmp);

        f.deliverBlock(5L, 50L);
        f.plugin.applyPendingNow();
        assertThat(f.plugin.metadata().blockNumber()).isEqualTo(5L);

        // Block 7 — skips 6. Must not apply.
        f.deliverBlock(7L, 70L);
        f.plugin.applyPendingNow();
        assertThat(f.plugin.metadata().blockNumber()).isEqualTo(5L);

        // Once 6 arrives, the apply loop drains 6 then 7.
        f.deliverBlock(6L, 60L);
        f.plugin.applyPendingNow();
        assertThat(f.plugin.metadata().blockNumber()).isEqualTo(7L);

        f.plugin.stop();
    }

    @Test
    void scenario4_chainedBlocksApplyInOrder(@TempDir final Path tmp) {
        final Fixture f = startPlugin(tmp);
        f.deliverBlock(0L, 0L);
        f.deliverBlock(1L, 1L);
        f.deliverBlock(2L, 2L);
        f.plugin.applyPendingNow();

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
    void scenario5_snapshotPersistsMetadataAndSnapshotFile(@TempDir final Path tmp) {
        final Fixture f = startPlugin(tmp);
        f.deliverBlock(1L, 10L);
        f.plugin.applyPendingNow();
        f.plugin.saveSnapshotNow();

        assertThat(tmp.resolve("md.json").toFile()).exists();
        assertThat(tmp.resolve("recent").resolve("1").resolve("live-state.bin").toFile())
                .exists();
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
        void deliverBlock(final long blockNumber, final long roundNumber) {
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
                                    .build())
                    .build();
            facility.sendBlockVerification(new VerificationNotification(
                    true, null, blockNumber, Bytes.fromHex("00"), block, BlockSource.PUBLISHER));
        }
    }

    private static Fixture startPlugin(final Path tmp) {
        final TestBlockMessagingFacility facility = new TestBlockMessagingFacility();
        final var configuration = ConfigurationBuilder.create()
                .withConfigDataType(LiveStateConfig.class)
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
        return new Fixture(plugin, facility);
    }

    private static final ServiceBuilder NOOP_SERVICE_BUILDER = new ServiceBuilder() {
        @Override
        public void registerHttpService(@NonNull final String path, final HttpService... service) {}

        @Override
        public void registerGrpcService(@NonNull final ServiceInterface service) {}
    };
}
