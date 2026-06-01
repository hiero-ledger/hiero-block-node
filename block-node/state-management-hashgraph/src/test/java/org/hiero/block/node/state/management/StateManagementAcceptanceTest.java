// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.state.management;

import static org.assertj.core.api.Assertions.assertThat;

import com.hedera.hapi.block.stream.input.RoundHeader;
import com.hedera.hapi.block.stream.output.BlockFooter;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.hapi.block.stream.output.MapChangeKey;
import com.hedera.hapi.block.stream.output.MapChangeValue;
import com.hedera.hapi.block.stream.output.MapUpdateChange;
import com.hedera.hapi.block.stream.output.QueuePushChange;
import com.hedera.hapi.block.stream.output.SingletonUpdateChange;
import com.hedera.hapi.block.stream.output.StateChange;
import com.hedera.hapi.block.stream.output.StateChanges;
import com.hedera.pbj.runtime.grpc.ServiceInterface;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.config.api.ConfigurationBuilder;
import com.swirlds.merkledb.config.MerkleDbConfig;
import com.swirlds.virtualmap.config.VirtualMapConfig;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.helidon.webserver.http.HttpService;
import java.nio.file.Path;
import org.hiero.block.api.BinaryStateQuery;
import org.hiero.block.api.BinaryStateQueryResponse;
import org.hiero.block.api.BinaryStateQueryResponse.Code;
import org.hiero.block.api.StateMetadata;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.app.fixtures.plugintest.TestBlockMessagingFacility;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.ServiceBuilder;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.StateUpdateNotification.StateUpdateType;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import org.hiero.consensus.config.PathsConfig;
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
 * The plugin owns its own apply scheduler; tests drive the package-private
 * {@code applyPending()} directly to remove timing flakiness.
 */
class StateManagementAcceptanceTest {

    @Test
    void scenario1_genesisAppliesFirstBlock(@TempDir final Path tmp) {
        final Fixture f = startPlugin(tmp);

        // At genesis the footer's startOfBlockStateRootHash must be empty / all-zeros.
        // Under lag-1 block 0 is applied (staged) but NOT yet exposed: nothing has
        // attested it, so metadata is still DEFAULT.
        f.deliverAndApply(0L, 0L);
        assertThat(f.plugin.metadata()).isEqualTo(StateMetadata.DEFAULT);

        // Block 1's footer attests post-0, exposing block 0 to readers.
        f.confirm(1L, 10L);
        assertThat(f.plugin.metadata().blockNumber()).isZero();
        assertThat(f.facility.getSentStateUpdateNotifications())
                .anyMatch(n -> n.type() == StateUpdateType.VERIFIED && n.blockNumber() == 0L);
        assertThat(f.plugin.isDegraded()).isFalse();
        f.plugin.stop();
    }

    @Test
    void scenario2_nextBlockOnTopOfMetadataApplies(@TempDir final Path tmp) {
        final Fixture f = startPlugin(tmp);

        f.deliverAndApply(5L, 50L); // genesis lane accepts arbitrary first block number
        f.confirm(6L, 60L); // attests block 5 → exposes it
        assertThat(f.plugin.metadata().blockNumber()).isEqualTo(5L);

        f.confirm(7L, 70L); // attests block 6 → exposes it
        assertThat(f.plugin.metadata().blockNumber()).isEqualTo(6L);
        assertThat(f.plugin.metadata().roundNumber()).isEqualTo(60L);

        f.plugin.stop();
    }

    @Test
    void scenario3_gapBlockIsNotApplied(@TempDir final Path tmp) {
        final Fixture f = startPlugin(tmp);

        f.deliverAndApply(5L, 50L); // genesis lane: block 5 applied/staged
        f.confirm(6L, 60L); // exposes block 5 (block 6 now staged)
        assertThat(f.plugin.metadata().blockNumber()).isEqualTo(5L);

        // Empty blocks don't change state, so every footer carries the same staged hash.
        final Bytes staged = f.plugin.stagedStateRootHash();

        // Block 8 — skips 7. The apply loop will not advance past the gap (expectedNext=7).
        f.deliverBlockWithChanges(8L, 80L, staged, java.util.List.of());
        f.plugin.applyPending();
        assertThat(f.plugin.metadata().blockNumber()).isEqualTo(5L);

        // Fill the gap with 7: applies 7 (exposes 6), then 8 (exposes 7).
        f.deliverBlockWithChanges(7L, 70L, staged, java.util.List.of());
        f.plugin.applyPending();
        assertThat(f.plugin.metadata().blockNumber()).isEqualTo(7L);

        f.plugin.stop();
    }

    @Test
    void scenario4_chainedBlocksApplyInOrder(@TempDir final Path tmp) {
        final Fixture f = startPlugin(tmp);
        // Apply blocks 0..3 chained. Under lag-1 each block attests its predecessor,
        // so after block 3 the exposed block is 2, and VERIFIED has fired for 0,1,2.
        for (long i = 0; i <= 3; i++) {
            f.deliverAndApply(i, i);
        }

        assertThat(f.plugin.metadata().blockNumber()).isEqualTo(2L);
        assertThat(f.plugin.metadata().roundNumber()).isEqualTo(2L);
        assertThat(f.facility.getSentStateUpdateNotifications().stream()
                        .filter(n -> n.type() == StateUpdateType.VERIFIED)
                        .count())
                .isEqualTo(3L);
        f.plugin.stop();
    }

    @Test
    void recentSnapshotRetentionKeepsConfiguredCount(@TempDir final Path tmp) throws java.io.IOException {
        // Retention=3 keeps the current + 2 older recent dirs. Apply + snapshot
        // 4 successive blocks; expect recent/1 to be deleted and recent/{2,3,4}
        // to remain on disk. Snapshots live only under recent/ — the plugin does
        // no historic/tar archival.
        final Fixture f = startPluginWithRecentRetention(tmp, 3);
        // Snapshots capture the exposed (attested) block. Applying blocks 0..5
        // exposes blocks 0..4, so snapshots recent/0..recent/4 are written; with
        // retention 3 the oldest (0, 1) are pruned, leaving {2,3,4}.
        for (long block = 0L; block <= 5L; block++) {
            f.deliverAndApply(block, block * 10L);
            f.plugin.saveSnapshot();
        }

        final java.nio.file.Path recent = tmp.resolve("recent");
        try (var stream = java.nio.file.Files.list(recent)) {
            final java.util.Set<String> names =
                    stream.map(p -> p.getFileName().toString()).collect(java.util.stream.Collectors.toSet());
            assertThat(names).containsExactlyInAnyOrder("2", "3", "4");
        }
        // No historic archival — the plugin never creates a historic/ directory.
        assertThat(tmp.resolve("historic").toFile()).doesNotExist();
        f.plugin.stop();
    }

    @Test
    void scenario6_refusesApplyOnHashMismatch(@TempDir final Path tmp) {
        final Fixture f = startPlugin(tmp);

        f.deliverAndApply(5L, 50L); // genesis lane: block 5 applied/staged
        f.confirm(6L, 60L); // exposes block 5 (block 6 staged)
        assertThat(f.plugin.metadata().blockNumber()).isEqualTo(5L);

        // Block 7 with a deliberately bogus startOfBlockStateRootHash — does not match
        // the staged hash (post-6), so it is rejected and nothing new is exposed.
        final Bytes bogus = Bytes.fromHex("deadbeef".repeat(12)); // 48 bytes != staged hash
        f.deliverBlockWithChanges(7L, 70L, bogus, java.util.List.of());
        f.plugin.applyPending();

        assertThat(f.plugin.metadata().blockNumber()).isEqualTo(5L);
        assertThat(f.plugin.isDegraded()).isTrue();
        assertThat(f.plugin.hashMismatchTotal()).isEqualTo(1L);
        f.plugin.stop();
    }

    @Test
    void scenario5_snapshotPersistsMetadataAndSnapshotFile(@TempDir final Path tmp) throws java.io.IOException {
        final Fixture f = startPlugin(tmp);
        f.deliverAndApply(1L, 10L); // genesis lane: block 1 applied/staged
        f.confirm(2L, 20L); // exposes block 1 so it can be snapshotted
        f.plugin.saveSnapshot();

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

    @Test
    void scenario9_streamingBlocksApplyStateChangesAndAreQueryable(@TempDir final Path tmp) {
        // Self-review-1 item #8: stream blocks with state_changes from genesis and
        // verify all three query shapes (KV, Singleton, Queue) return the applied
        // values. Plugin-level — exercises the same apply + query code path the
        // gRPC E2E flows would, without the cost of building blocks with valid
        // block proofs and routing through the publish gRPC.
        final Fixture f = startPlugin(tmp);

        // Block 0 (genesis-lane): singleton stateId=1 ← bytes "aabbcc"
        //                          KV       stateId=2, key="01" ← "hello"
        final StateChange singletonAa = StateChange.newBuilder()
                .stateId(1)
                .singletonUpdate(SingletonUpdateChange.newBuilder()
                        .bytesValue(Bytes.fromHex("aabbcc"))
                        .build())
                .build();
        final MapChangeKey mapKey =
                MapChangeKey.newBuilder().protoBytesKey(Bytes.fromHex("01")).build();
        final MapChangeValue mapValue =
                MapChangeValue.newBuilder().protoStringValue("hello").build();
        final StateChange kvUpdate = StateChange.newBuilder()
                .stateId(2)
                .mapUpdate(
                        MapUpdateChange.newBuilder().key(mapKey).value(mapValue).build())
                .build();
        f.deliverAndApply(0L, 0L, java.util.List.of(singletonAa, kvUpdate)); // staged

        // Block 1: queue stateId=3 receives an element 0a.
        final StateChange queuePush = StateChange.newBuilder()
                .stateId(3)
                .queuePush(QueuePushChange.newBuilder()
                        .protoBytesElement(Bytes.fromHex("0a"))
                        .build())
                .build();
        f.deliverAndApply(1L, 10L, java.util.List.of(queuePush)); // exposes block 0, stages block 1
        // Confirm block 1 so its state (singleton + kv from block 0, plus the queue) is exposed.
        f.confirm(2L, 20L);

        // Plugin must now serve all three query shapes successfully off block 1.
        final BinaryStateQueryResponse singleton = f.plugin.getBinarySingleton(
                BinaryStateQuery.newBuilder().retrieveLatest(true).stateId(1L).build());
        assertThat(singleton.status()).isEqualTo(Code.SUCCESS);
        assertThat(singleton.singletonBytes()).isNotNull();
        assertThat(singleton.stateMetadata().blockNumber()).isEqualTo(1L);

        final BinaryStateQueryResponse queue = f.plugin.getBinaryQueue(
                BinaryStateQuery.newBuilder().retrieveLatest(true).stateId(3L).build());
        assertThat(queue.status()).isEqualTo(Code.SUCCESS);
        assertThat(queue.queueBytes()).isNotEmpty();

        final BinaryStateQueryResponse kv = f.plugin.getBinaryKV(BinaryStateQuery.newBuilder()
                .retrieveLatest(true)
                .stateId(2L)
                .keyBytes(MapChangeKey.PROTOBUF.toBytes(mapKey))
                .build());
        assertThat(kv.status()).isEqualTo(Code.SUCCESS);
        assertThat(kv.kvBytes()).isNotNull();

        f.plugin.stop();
    }

    @Test
    void scenario10_restoreSnapshotServesPersistedStateThroughQueries(@TempDir final Path tmp) {
        // Self-review-1 item #8: first run applies blocks with state changes and snapshots;
        // second run loads the snapshot and verifies the same values come back through
        // all three query shapes. Exercises persist → restart → read.
        final Fixture first = startPlugin(tmp);
        final StateChange singletonSeed = StateChange.newBuilder()
                .stateId(7)
                .singletonUpdate(SingletonUpdateChange.newBuilder()
                        .bytesValue(Bytes.fromHex("c0ffee"))
                        .build())
                .build();
        final MapChangeKey mapKey =
                MapChangeKey.newBuilder().protoBytesKey(Bytes.fromHex("ab")).build();
        final MapChangeValue mapValue =
                MapChangeValue.newBuilder().protoStringValue("persisted").build();
        final StateChange kvSeed = StateChange.newBuilder()
                .stateId(8)
                .mapUpdate(
                        MapUpdateChange.newBuilder().key(mapKey).value(mapValue).build())
                .build();
        final StateChange queueSeed = StateChange.newBuilder()
                .stateId(9)
                .queuePush(QueuePushChange.newBuilder()
                        .protoBytesElement(Bytes.fromHex("dd"))
                        .build())
                .build();
        first.deliverAndApply(0L, 0L, java.util.List.of(singletonSeed, kvSeed, queueSeed)); // staged
        first.confirm(1L, 10L); // exposes block 0 so it can be snapshotted
        first.plugin.saveSnapshot();
        first.plugin.stop();

        // Second plugin instance — same paths, must reload metadata + snapshot dir.
        final Fixture second = startPlugin(tmp);
        assertThat(second.plugin.metadata()).isNotEqualTo(StateMetadata.DEFAULT);
        assertThat(second.plugin.metadata().blockNumber()).isEqualTo(0L);

        final BinaryStateQueryResponse singleton = second.plugin.getBinarySingleton(
                BinaryStateQuery.newBuilder().retrieveLatest(true).stateId(7L).build());
        assertThat(singleton.status()).isEqualTo(Code.SUCCESS);
        assertThat(singleton.singletonBytes()).isNotNull();

        final BinaryStateQueryResponse kv = second.plugin.getBinaryKV(BinaryStateQuery.newBuilder()
                .retrieveLatest(true)
                .stateId(8L)
                .keyBytes(MapChangeKey.PROTOBUF.toBytes(mapKey))
                .build());
        assertThat(kv.status()).isEqualTo(Code.SUCCESS);

        final BinaryStateQueryResponse queue = second.plugin.getBinaryQueue(
                BinaryStateQuery.newBuilder().retrieveLatest(true).stateId(9L).build());
        assertThat(queue.status()).isEqualTo(Code.SUCCESS);
        assertThat(queue.queueBytes()).isNotEmpty();

        second.plugin.stop();
    }

    // ── Fixtures ───────────────────────────────────────────────────────────

    private record Fixture(StateManagementPlugin plugin, TestBlockMessagingFacility facility) {
        /** Deliver a block whose footer carries the supplied {@code startOfBlockStateRootHash}. */
        void deliverBlock(final long blockNumber, final long roundNumber, @NonNull final Bytes startHash) {
            deliverBlockWithChanges(blockNumber, roundNumber, startHash, java.util.List.of());
        }

        /**
         * Deliver a block carrying the supplied {@code state_changes}. The footer's
         * {@code startOfBlockStateRootHash} is set verbatim. Used by scenarios
         * 9/10 to exercise the full apply pipeline including state mutation.
         */
        void deliverBlockWithChanges(
                final long blockNumber,
                final long roundNumber,
                @NonNull final Bytes startHash,
                @NonNull final java.util.List<StateChange> changes) {
            final java.util.List<BlockItemUnparsed> items = new java.util.ArrayList<>();
            items.add(BlockItemUnparsed.newBuilder()
                    .blockHeader(BlockHeader.PROTOBUF.toBytes(
                            BlockHeader.newBuilder().number(blockNumber).build()))
                    .build());
            items.add(BlockItemUnparsed.newBuilder()
                    .roundHeader(RoundHeader.PROTOBUF.toBytes(
                            RoundHeader.newBuilder().roundNumber(roundNumber).build()))
                    .build());
            if (!changes.isEmpty()) {
                items.add(BlockItemUnparsed.newBuilder()
                        .stateChanges(StateChanges.PROTOBUF.toBytes(
                                StateChanges.newBuilder().stateChanges(changes).build()))
                        .build());
            }
            items.add(BlockItemUnparsed.newBuilder()
                    .blockFooter(BlockFooter.PROTOBUF.toBytes(BlockFooter.newBuilder()
                            .startOfBlockStateRootHash(startHash)
                            .build()))
                    .build());
            final BlockUnparsed block =
                    BlockUnparsed.newBuilder().blockItems(items).build();
            facility.sendBlockVerification(new VerificationNotification(
                    true, null, blockNumber, Bytes.fromHex("00"), block, BlockSource.PUBLISHER));
        }

        /**
         * Deliver block {@code blockNumber} with its footer chained to the current
         * staged hash (so it validates under lag-1), then drain {@code applyPending}.
         * The block applies but — per lag-1 — is not exposed until a following block
         * confirms it; use {@link #confirm} for that.
         */
        void deliverAndApply(
                final long blockNumber, final long roundNumber, @NonNull final java.util.List<StateChange> changes) {
            deliverBlockWithChanges(blockNumber, roundNumber, plugin.stagedStateRootHash(), changes);
            plugin.applyPending();
        }

        void deliverAndApply(final long blockNumber, final long roundNumber) {
            deliverAndApply(blockNumber, roundNumber, java.util.List.of());
        }

        /**
         * Deliver an empty confirming block {@code blockNumber} (no state changes) to
         * attest the previously applied block under lag-1, exposing it to queries.
         */
        void confirm(final long blockNumber, final long roundNumber) {
            deliverAndApply(blockNumber, roundNumber, java.util.List.of());
        }
    }

    private static Fixture startPluginWithRecentRetention(final Path tmp, final int recentRetention) {
        return startPlugin(tmp, recentRetention);
    }

    private static Fixture startPlugin(final Path tmp) {
        return startPlugin(tmp, 3);
    }

    private static Fixture startPlugin(final Path tmp, final int recentRetention) {
        final TestBlockMessagingFacility facility = new TestBlockMessagingFacility();
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
                .withValue("state.management.snapshotIntervalMillis", "3600000")
                .withValue("state.management.stateChangesApplyIntervalMillis", "3600000")
                .withValue("state.management.stateSnapshotRecentRetentionCount", Integer.toString(recentRetention))
                .build();
        final BlockNodeContext context =
                new BlockNodeContext(configuration, null, null, facility, null, null, null, null, null, null, null);
        final StateManagementPlugin plugin = new StateManagementPlugin();
        plugin.init(context, NOOP_SERVICE_BUILDER);
        plugin.start();
        try {
            StateManagementPluginTestSupport.awaitReady(plugin, 5_000L);
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
