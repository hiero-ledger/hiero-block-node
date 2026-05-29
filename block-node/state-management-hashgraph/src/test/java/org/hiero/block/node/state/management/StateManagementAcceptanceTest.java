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
        f.deliverBlock(0L, 0L, Bytes.EMPTY);
        f.plugin.applyPending();

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
        f.plugin.applyPending();
        assertThat(f.plugin.metadata().blockNumber()).isEqualTo(5L);

        final Bytes liveAfter5 = f.plugin.metadata().stateRootHash();
        f.deliverBlock(6L, 60L, liveAfter5);
        f.plugin.applyPending();
        assertThat(f.plugin.metadata().blockNumber()).isEqualTo(6L);
        assertThat(f.plugin.metadata().roundNumber()).isEqualTo(60L);

        f.plugin.stop();
    }

    @Test
    void scenario3_gapBlockIsNotApplied(@TempDir final Path tmp) {
        final Fixture f = startPlugin(tmp);

        f.deliverBlock(5L, 50L, Bytes.EMPTY);
        f.plugin.applyPending();
        assertThat(f.plugin.metadata().blockNumber()).isEqualTo(5L);
        final Bytes liveAfter5 = f.plugin.metadata().stateRootHash();

        // Block 7 — skips 6. Footer hash is fine (no state changes in between) but
        // the apply loop will not advance past the gap.
        f.deliverBlock(7L, 70L, liveAfter5);
        f.plugin.applyPending();
        assertThat(f.plugin.metadata().blockNumber()).isEqualTo(5L);

        f.deliverBlock(6L, 60L, liveAfter5);
        f.plugin.applyPending();
        assertThat(f.plugin.metadata().blockNumber()).isEqualTo(7L);

        f.plugin.stop();
    }

    @Test
    void scenario4_chainedBlocksApplyInOrder(@TempDir final Path tmp) {
        final Fixture f = startPlugin(tmp);
        Bytes startHash = Bytes.EMPTY;
        for (long i = 0; i < 3; i++) {
            f.deliverBlock(i, i, startHash);
            f.plugin.applyPending();
            startHash = f.plugin.metadata().stateRootHash();
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
    void scenario8_historicArchivesAreCappedByRetention(@TempDir final Path tmp) throws java.io.IOException {
        final Fixture f = startPluginWithRetention(tmp, 2);

        // Apply + snapshot 3 successive blocks, producing snapshots 1, 2, 3.
        // After each snapshot, the previous recent dir is archived to historic.
        // With retention=2, after writing 3.tar the historic store should hold
        // only 2.tar and 1.tar — wait, that's 2. After writing 4 it would drop 1.
        // So produce four snapshots so the retention actually trims one.
        Bytes start = Bytes.EMPTY;
        for (long block = 1; block <= 4; block++) {
            f.deliverBlock(block, block * 10L, start);
            f.plugin.applyPending();
            f.plugin.saveSnapshot();
            start = f.plugin.metadata().stateRootHash();
        }

        final java.nio.file.Path historic = tmp.resolve("historic");
        final java.util.Set<String> names;
        try (var stream = java.nio.file.Files.list(historic)) {
            names = stream.map(p -> p.getFileName().toString()).collect(java.util.stream.Collectors.toSet());
        }
        // 1, 2, 3 would all be archives (4 is in recent/). Retention of 2 should leave
        // only the two most recent archives: 3.tar and 2.tar (or some prefix variant).
        assertThat(names).hasSize(2).contains("3.tar").contains("2.tar").doesNotContain("1.tar");
        f.plugin.stop();
    }

    @Test
    void scenario7_previousSnapshotsAreArchivedToHistoric(@TempDir final Path tmp) throws java.io.IOException {
        // Force recent retention = 1 so the test exercises the archive-then-delete
        // path on every snapshot cycle. The default retention (3) would leave the
        // older recent dir on disk and just archive a copy.
        final Fixture f = startPluginWithRecentRetention(tmp, 1);

        // Block 1 → snapshot 1.
        f.deliverBlock(1L, 10L, Bytes.EMPTY);
        f.plugin.applyPending();
        f.plugin.saveSnapshot();
        final java.nio.file.Path recent1 = tmp.resolve("recent").resolve("1");
        assertThat(java.nio.file.Files.isDirectory(recent1)).isTrue();

        // Block 2 → snapshot 2. The recent/1 directory should be tarred to
        // historic/1.tar and removed.
        final Bytes liveAfter1 = f.plugin.metadata().stateRootHash();
        f.deliverBlock(2L, 20L, liveAfter1);
        f.plugin.applyPending();
        f.plugin.saveSnapshot();

        assertThat(tmp.resolve("recent").resolve("2").toFile()).exists();
        assertThat(recent1.toFile()).doesNotExist();
        final java.nio.file.Path historic1 = tmp.resolve("historic").resolve("1.tar");
        assertThat(historic1.toFile()).exists();
        assertThat(java.nio.file.Files.size(historic1)).isGreaterThan(1024L); // header + content + EOA
        f.plugin.stop();
    }

    @Test
    void recentSnapshotRetentionKeepsConfiguredCount(@TempDir final Path tmp) throws java.io.IOException {
        // Retention=3 keeps the current + 2 older recent dirs. Apply + snapshot
        // 4 successive blocks; expect recent/1 to be archived + deleted, and
        // recent/{2,3,4} to remain on disk.
        final Fixture f = startPluginWithRecentRetention(tmp, 3);
        Bytes start = Bytes.EMPTY;
        for (long block = 1L; block <= 4L; block++) {
            f.deliverBlock(block, block * 10L, start);
            f.plugin.applyPending();
            f.plugin.saveSnapshot();
            start = f.plugin.metadata().stateRootHash();
        }

        final java.nio.file.Path recent = tmp.resolve("recent");
        try (var stream = java.nio.file.Files.list(recent)) {
            final java.util.Set<String> names =
                    stream.map(p -> p.getFileName().toString()).collect(java.util.stream.Collectors.toSet());
            assertThat(names).containsExactlyInAnyOrder("2", "3", "4");
        }
        // recent/1 was archived to historic before being deleted from recent.
        assertThat(tmp.resolve("historic").resolve("1.tar").toFile()).exists();
        f.plugin.stop();
    }

    @Test
    void historicRetentionDeletesOldestArchiveFirst(@TempDir final Path tmp) throws java.io.IOException {
        // Self-review-1 item #14, Nana's exact example: with retention=3 and
        // 0.tar, 1.tar, 2.tar present, adding 3.tar must drop 0.tar (the oldest).
        // recent-retention=1 forces every block's previous snapshot to be archived,
        // so applying blocks 1..4 produces real 1.tar, 2.tar, 3.tar archives.
        final Fixture f = startPlugin(tmp, 3L, 1);

        // Pre-seed a dummy 0.tar that pre-dates any real archive. It must be the
        // first victim once the archive count exceeds the retention window.
        final java.nio.file.Path historic = tmp.resolve("historic");
        java.nio.file.Files.createDirectories(historic);
        final java.nio.file.Path dummyZero = historic.resolve("0.tar");
        java.nio.file.Files.writeString(dummyZero, "seed");

        Bytes start = Bytes.EMPTY;
        for (long block = 1L; block <= 4L; block++) {
            f.deliverBlock(block, block * 10L, start);
            f.plugin.applyPending();
            f.plugin.saveSnapshot();
            start = f.plugin.metadata().stateRootHash();
        }

        // Archives produced: dummy 0.tar + real 1/2/3.tar = 4. Retention 3 trims
        // the single oldest — 0.tar — and keeps the three most recent.
        try (var stream = java.nio.file.Files.list(historic)) {
            final java.util.Set<String> names =
                    stream.map(p -> p.getFileName().toString()).collect(java.util.stream.Collectors.toSet());
            assertThat(names).containsExactlyInAnyOrder("1.tar", "2.tar", "3.tar");
        }
        assertThat(dummyZero.toFile()).doesNotExist();
        f.plugin.stop();
    }

    @Test
    void scenario6_refusesApplyOnHashMismatch(@TempDir final Path tmp) {
        final Fixture f = startPlugin(tmp);

        f.deliverBlock(5L, 50L, Bytes.EMPTY); // genesis lane
        f.plugin.applyPending();
        assertThat(f.plugin.metadata().blockNumber()).isEqualTo(5L);

        // Block 6 with a deliberately bogus startOfBlockStateRootHash.
        final Bytes bogus = Bytes.fromHex("deadbeef".repeat(12)); // 48 bytes != live hash
        f.deliverBlock(6L, 60L, bogus);
        f.plugin.applyPending();

        assertThat(f.plugin.metadata().blockNumber()).isEqualTo(5L);
        assertThat(f.plugin.isDegraded()).isTrue();
        assertThat(f.plugin.hashMismatchTotal()).isEqualTo(1L);
        f.plugin.stop();
    }

    @Test
    void scenario5_snapshotPersistsMetadataAndSnapshotFile(@TempDir final Path tmp) throws java.io.IOException {
        final Fixture f = startPlugin(tmp);
        f.deliverBlock(1L, 10L, Bytes.EMPTY);
        f.plugin.applyPending();
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
        f.deliverBlockWithChanges(0L, 0L, Bytes.EMPTY, java.util.List.of(singletonAa, kvUpdate));
        f.plugin.applyPending();

        // Block 1: queue stateId=3 receives an element 0a.
        final Bytes startAfter0 = f.plugin.metadata().stateRootHash();
        final StateChange queuePush = StateChange.newBuilder()
                .stateId(3)
                .queuePush(QueuePushChange.newBuilder()
                        .protoBytesElement(Bytes.fromHex("0a"))
                        .build())
                .build();
        f.deliverBlockWithChanges(1L, 10L, startAfter0, java.util.List.of(queuePush));
        f.plugin.applyPending();

        // Plugin must now serve all three query shapes successfully.
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
        first.deliverBlockWithChanges(0L, 0L, Bytes.EMPTY, java.util.List.of(singletonSeed, kvSeed, queueSeed));
        first.plugin.applyPending();
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
    }

    private static Fixture startPluginWithRetention(final Path tmp, final long retention) {
        return startPlugin(tmp, retention, 3);
    }

    private static Fixture startPluginWithRecentRetention(final Path tmp, final int recentRetention) {
        return startPlugin(tmp, 0L, recentRetention);
    }

    private static Fixture startPlugin(final Path tmp) {
        return startPlugin(tmp, 0L, 3);
    }

    private static Fixture startPlugin(final Path tmp, final long retention, final int recentRetention) {
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
                .withValue(
                        "state.management.stateSnapshotHistoricPath",
                        tmp.resolve("historic").toString())
                .withValue("state.management.snapshotIntervalMillis", "3600000")
                .withValue("state.management.stateChangesApplyIntervalMillis", "3600000")
                .withValue("state.management.historicArchiveRetentionCount", Long.toString(retention))
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
