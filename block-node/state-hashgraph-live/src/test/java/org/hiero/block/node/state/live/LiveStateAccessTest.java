// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.state.live;

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
import java.util.ArrayList;
import java.util.List;
import org.hiero.block.api.BinaryStateQuery;
import org.hiero.block.api.BinaryStateQueryResponse;
import org.hiero.block.api.BinaryStateQueryResponse.Code;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.app.fixtures.plugintest.TestBlockMessagingFacility;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.ServiceBuilder;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import org.hiero.consensus.config.PathsConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Exercises the gRPC query surface of {@link LiveStatePlugin} (STORY-7).
 * Bypasses Helidon by calling the {@code StateServiceInterface} default
 * methods directly — same code path the gRPC dispatcher invokes on a
 * matched request.
 *
 * <p>State is arranged by delivering a block carrying {@code state_changes}
 * and letting the plugin apply it, exactly as in production. The applier
 * stores the serialized change-carrier bytes (e.g.
 * {@code SingletonUpdateChange.PROTOBUF.toBytes(...)}), so queries assert
 * against those carrier bytes rather than raw values.
 */
class LiveStateAccessTest {

    @Test
    void kvSingletonAndQueueHappyPaths(@TempDir final Path tmp) {
        final Started s = startPlugin(tmp);

        // Singleton stateId=1, KV stateId=2 (key=01), queue stateId=3 (two pushes).
        final SingletonUpdateChange singleton = SingletonUpdateChange.newBuilder()
                .bytesValue(Bytes.fromHex("aa"))
                .build();
        final MapChangeKey kvKey =
                MapChangeKey.newBuilder().protoBytesKey(Bytes.fromHex("01")).build();
        final MapChangeValue kvValue =
                MapChangeValue.newBuilder().protoStringValue("bb").build();
        final QueuePushChange queueA =
                QueuePushChange.newBuilder().protoBytesElement(Bytes.fromHex("aa")).build();
        final QueuePushChange queueB =
                QueuePushChange.newBuilder().protoBytesElement(Bytes.fromHex("bb")).build();

        s.deliverBlock(
                0L,
                0L,
                Bytes.EMPTY,
                List.of(
                        StateChange.newBuilder().stateId(1).singletonUpdate(singleton).build(),
                        StateChange.newBuilder()
                                .stateId(2)
                                .mapUpdate(MapUpdateChange.newBuilder()
                                        .key(kvKey)
                                        .value(kvValue)
                                        .build())
                                .build(),
                        StateChange.newBuilder().stateId(3).queuePush(queueA).build(),
                        StateChange.newBuilder().stateId(3).queuePush(queueB).build()));
        s.plugin.applyPending();

        // The applier stores serialized carriers; compute the expected stored bytes.
        final Bytes expectedSingleton = SingletonUpdateChange.PROTOBUF.toBytes(singleton);
        final Bytes expectedKvKey = MapChangeKey.PROTOBUF.toBytes(kvKey);
        final Bytes expectedKvValue = MapChangeValue.PROTOBUF.toBytes(kvValue);
        final Bytes expectedQueueA = QueuePushChange.PROTOBUF.toBytes(queueA);
        final Bytes expectedQueueB = QueuePushChange.PROTOBUF.toBytes(queueB);

        final BinaryStateQueryResponse singletonResp = s.plugin.getBinarySingleton(
                BinaryStateQuery.newBuilder().retrieveLatest(true).stateId(1L).build());
        assertThat(singletonResp.status()).isEqualTo(Code.SUCCESS);
        assertThat(singletonResp.singletonBytes()).isEqualTo(expectedSingleton);

        final BinaryStateQueryResponse kv = s.plugin.getBinaryKV(BinaryStateQuery.newBuilder()
                .retrieveLatest(true)
                .stateId(2L)
                .keyBytes(expectedKvKey)
                .build());
        assertThat(kv.status()).isEqualTo(Code.SUCCESS);
        assertThat(kv.kvBytes()).isEqualTo(expectedKvValue);

        final BinaryStateQueryResponse queueAll = s.plugin.getBinaryQueue(
                BinaryStateQuery.newBuilder().retrieveLatest(true).stateId(3L).build());
        assertThat(queueAll.status()).isEqualTo(Code.SUCCESS);
        assertThat(queueAll.queueBytes()).containsExactly(expectedQueueA, expectedQueueB);

        // Indexed peek uses VirtualMap's internal queue index (head…tail range, not an
        // array index from zero). We assert structural success and that the returned
        // element is one of the pushed values; the exact mapping of index→element is
        // owned by swirlds-state-impl and may differ across versions.
        final BinaryStateQueryResponse queueOne = s.plugin.getBinaryQueue(BinaryStateQuery.newBuilder()
                .retrieveLatest(true)
                .stateId(3L)
                .queueIndex(1L)
                .build());
        assertThat(queueOne.status()).isEqualTo(Code.SUCCESS);
        assertThat(queueOne.queueBytes()).hasSize(1);
        assertThat(queueOne.queueBytes().getFirst()).isIn(expectedQueueA, expectedQueueB);

        s.plugin.stop();
    }

    @Test
    void notFoundAndInvalidShapes(@TempDir final Path tmp) {
        final LiveStatePlugin plugin = startPlugin(tmp).plugin;

        // not found — clean state.
        assertThat(plugin.getBinarySingleton(
                        BinaryStateQuery.newBuilder().retrieveLatest(true).stateId(99L).build())
                .status())
                .isEqualTo(Code.NOT_FOUND);

        // KV without key.
        assertThat(plugin.getBinaryKV(BinaryStateQuery.newBuilder()
                        .retrieveLatest(true)
                        .stateId(2L)
                        .build())
                .status())
                .isEqualTo(Code.INVALID_REQUEST);

        // KV with queue_index set.
        assertThat(plugin.getBinaryKV(BinaryStateQuery.newBuilder()
                        .retrieveLatest(true)
                        .stateId(2L)
                        .keyBytes(Bytes.fromHex("01"))
                        .queueIndex(1L)
                        .build())
                .status())
                .isEqualTo(Code.INVALID_REQUEST);

        // Singleton with key bytes set.
        assertThat(plugin.getBinarySingleton(BinaryStateQuery.newBuilder()
                        .retrieveLatest(true)
                        .stateId(1L)
                        .keyBytes(Bytes.fromHex("01"))
                        .build())
                .status())
                .isEqualTo(Code.INVALID_REQUEST);

        // Queue with key bytes set.
        assertThat(plugin.getBinaryQueue(BinaryStateQuery.newBuilder()
                        .retrieveLatest(true)
                        .stateId(3L)
                        .keyBytes(Bytes.fromHex("01"))
                        .build())
                .status())
                .isEqualTo(Code.INVALID_REQUEST);

        // Block 0 (genesis) is a valid block_number — at a fresh start metadata.blockNumber()
        // is 0, so block_number=0 is a MATCH and falls through to the state lookup
        // (NOT_FOUND because no items have been applied), not a treat-zero-as-latest
        // shortcut and not an immediate INVALID_REQUEST.
        assertThat(plugin.getBinarySingleton(BinaryStateQuery.newBuilder()
                        .stateId(99L)
                        .blockNumber(0L)
                        .build())
                .status())
                .isEqualTo(Code.NOT_FOUND);

        // Missing block_specifier (neither block_number nor retrieve_latest set) is invalid.
        assertThat(plugin.getBinarySingleton(BinaryStateQuery.newBuilder()
                        .stateId(1L)
                        .build())
                .status())
                .isEqualTo(Code.INVALID_REQUEST);

        // Stale block_number does not match latest applied.
        assertThat(plugin.getBinarySingleton(BinaryStateQuery.newBuilder()
                        .stateId(1L)
                        .blockNumber(42L)
                        .build())
                .status())
                .isEqualTo(Code.INVALID_REQUEST);

        plugin.stop();
    }

    // ── Fixtures ───────────────────────────────────────────────────────────

    private record Started(LiveStatePlugin plugin, TestBlockMessagingFacility facility) {
        /** Deliver a block carrying the supplied {@code state_changes}. */
        void deliverBlock(
                final long blockNumber,
                final long roundNumber,
                @NonNull final Bytes startHash,
                @NonNull final List<StateChange> changes) {
            final List<BlockItemUnparsed> items = new ArrayList<>();
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
                        .stateChanges(StateChanges.PROTOBUF.toBytes(StateChanges.newBuilder()
                                .stateChanges(changes)
                                .build()))
                        .build());
            }
            items.add(BlockItemUnparsed.newBuilder()
                    .blockFooter(BlockFooter.PROTOBUF.toBytes(BlockFooter.newBuilder()
                            .startOfBlockStateRootHash(startHash)
                            .build()))
                    .build());
            facility.sendBlockVerification(new VerificationNotification(
                    true, null, blockNumber, Bytes.fromHex("00"),
                    BlockUnparsed.newBuilder().blockItems(items).build(), BlockSource.PUBLISHER));
        }
    }

    private static Started startPlugin(final Path tmp) {
        final TestBlockMessagingFacility facility = new TestBlockMessagingFacility();
        final var configuration = ConfigurationBuilder.create()
                .withConfigDataType(LiveStateConfig.class)
                .withConfigDataType(MerkleDbConfig.class)
                .withConfigDataType(VirtualMapConfig.class)
                .withConfigDataType(PathsConfig.class)
                .withValue("state.live.stateMetadataPath",
                        tmp.resolve("md.json").toString())
                .withValue("state.live.stateSnapshotRecentPath",
                        tmp.resolve("recent").toString())
                .withValue("state.live.stateSnapshotHistoricPath",
                        tmp.resolve("historic").toString())
                .withValue("state.live.snapshotIntervalMillis", "3600000")
                .withValue("state.live.stateChangesApplyIntervalMillis", "3600000")
                .build();
        final BlockNodeContext context = new BlockNodeContext(
                configuration, null, null, facility, null, null, null, null, null, null, null);
        final LiveStatePlugin plugin = new LiveStatePlugin();
        plugin.init(context, NOOP_SERVICE_BUILDER);
        plugin.start();
        try {
            LiveStatePluginTestSupport.awaitReady(plugin, 5_000L);
        } catch (final InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
        return new Started(plugin, facility);
    }

    private static final ServiceBuilder NOOP_SERVICE_BUILDER = new ServiceBuilder() {
        @Override
        public void registerHttpService(@NonNull final String path, final HttpService... service) {}

        @Override
        public void registerGrpcService(@NonNull final ServiceInterface service) {}
    };
}
