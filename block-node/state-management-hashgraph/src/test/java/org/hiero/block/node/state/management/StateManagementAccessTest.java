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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
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
import org.hiero.metrics.core.MetricRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Exercises the gRPC query surface of {@link StateManagementPlugin}.
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
class StateManagementAccessTest {

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
        final QueuePushChange queueA = QueuePushChange.newBuilder()
                .protoBytesElement(Bytes.fromHex("aa"))
                .build();
        final QueuePushChange queueB = QueuePushChange.newBuilder()
                .protoBytesElement(Bytes.fromHex("bb"))
                .build();

        s.deliverBlock(
                0L,
                0L,
                Bytes.EMPTY,
                List.of(
                        StateChange.newBuilder()
                                .stateId(1)
                                .singletonUpdate(singleton)
                                .build(),
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
        // lag-1: block 0 is applied but not exposed until the next block attests it.
        // Deliver an empty confirming block 1 (footer chained to the staged hash).
        s.deliverBlock(1L, 10L, s.plugin.stagedStateRootHash(), List.of());
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
        final Started s = startPlugin(tmp);
        // lag-1: queries report NOT_READY until at least one block is attested. Apply
        // an empty genesis block 0 and confirm it with block 1 so an (empty) attested
        // state exists; the shape/NOT_FOUND checks below run against it.
        s.deliverBlock(0L, 0L, Bytes.EMPTY, List.of());
        s.plugin.applyPending();
        s.deliverBlock(1L, 10L, s.plugin.stagedStateRootHash(), List.of());
        s.plugin.applyPending();
        final StateManagementPlugin plugin = s.plugin;

        // not found — clean state.
        assertThat(plugin.getBinarySingleton(BinaryStateQuery.newBuilder()
                                .retrieveLatest(true)
                                .stateId(99L)
                                .build())
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

        // No block_specifier set (neither block_number nor retrieve_latest) is a
        // malformed request → INVALID_REQUEST (distinct from a well-formed request
        // for state we do not hold, which is NOT_FOUND).
        assertThat(plugin.getBinarySingleton(
                                BinaryStateQuery.newBuilder().stateId(1L).build())
                        .status())
                .isEqualTo(Code.INVALID_REQUEST);

        // A non-latest block_number is a valid shape, but the plugin only serves
        // latest state — so it returns NOT_FOUND (no historical state held),
        // affirming the latest-only API rather than rejecting the request shape.
        assertThat(plugin.getBinarySingleton(BinaryStateQuery.newBuilder()
                                .stateId(1L)
                                .blockNumber(42L)
                                .build())
                        .status())
                .isEqualTo(Code.NOT_FOUND);

        plugin.stop();
    }

    @Test
    void concurrentReadsDuringAppliesDoNotThrow(@TempDir final Path tmp) throws Exception {
        final Started s = startPlugin(tmp);

        // Seed a singleton at state 1 in genesis block 0; confirm with block 1 so it is exposed.
        s.deliverBlock(
                0L,
                0L,
                Bytes.EMPTY,
                List.of(StateChange.newBuilder()
                        .stateId(1)
                        .singletonUpdate(SingletonUpdateChange.newBuilder()
                                .bytesValue(Bytes.fromHex("aa"))
                                .build())
                        .build()));
        s.plugin.applyPending();

        final AtomicBoolean stop = new AtomicBoolean(false);
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final Thread reader = new Thread(() -> {
            while (!stop.get() && failure.get() == null) {
                try {
                    final BinaryStateQueryResponse r = s.plugin.getBinarySingleton(BinaryStateQuery.newBuilder()
                            .retrieveLatest(true)
                            .stateId(1L)
                            .build());
                    assertThat(r.status()).isIn(Code.SUCCESS, Code.NOT_FOUND, Code.NOT_READY);
                } catch (final Throwable t) {
                    failure.set(t);
                }
            }
        });
        reader.start();

        // Rotate attestedImmutable repeatedly by applying a chain of confirming blocks while
        // the reader hammers queries. Pins that a read overlapping setAttested rotation never
        // throws (e.g. ReferenceCountException) and always yields a structured status.
        for (long b = 1L; b <= 50L; b++) {
            s.deliverBlock(b, b * 10L, s.plugin.stagedStateRootHash(), List.of());
            s.plugin.applyPending();
        }
        stop.set(true);
        reader.join(5_000L);

        assertThat(failure.get())
                .as("no exception from concurrent reads during state rotation")
                .isNull();
        s.plugin.stop();
    }

    // ── Fixtures ───────────────────────────────────────────────────────────

    private record Started(StateManagementPlugin plugin, TestBlockMessagingFacility facility) {
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
                        .stateChanges(StateChanges.PROTOBUF.toBytes(
                                StateChanges.newBuilder().stateChanges(changes).build()))
                        .build());
            }
            items.add(BlockItemUnparsed.newBuilder()
                    .blockFooter(BlockFooter.PROTOBUF.toBytes(BlockFooter.newBuilder()
                            .startOfBlockStateRootHash(startHash)
                            .build()))
                    .build());
            facility.sendBlockVerification(new VerificationNotification(
                    true,
                    null,
                    blockNumber,
                    Bytes.fromHex("00"),
                    BlockUnparsed.newBuilder().blockItems(items).build(),
                    BlockSource.PUBLISHER));
        }
    }

    private static Started startPlugin(final Path tmp) {
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
        return new Started(plugin, facility);
    }

    private static final ServiceBuilder NOOP_SERVICE_BUILDER = new ServiceBuilder() {
        @Override
        public void registerHttpService(@NonNull final String path, final HttpService... service) {}

        @Override
        public void registerGrpcService(@NonNull final ServiceInterface service) {}
    };
}
