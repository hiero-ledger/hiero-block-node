// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.suites.utils;

import com.hedera.hapi.block.stream.output.MapChangeKey;
import com.hedera.hapi.block.stream.output.MapChangeValue;
import com.hedera.hapi.block.stream.output.MapUpdateChange;
import com.hedera.hapi.block.stream.output.QueuePushChange;
import com.hedera.hapi.block.stream.output.SingletonUpdateChange;
import com.hedera.hapi.block.stream.output.StateChange;
import com.hedera.hapi.block.stream.output.StateChanges;
import com.hedera.hapi.block.stream.output.StateIdentifier;
import com.hedera.hapi.node.base.Timestamp;
import com.hedera.hapi.node.state.blockstream.BlockStreamInfo;
import com.hedera.pbj.runtime.io.ReadableSequentialData;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.base.time.Time;
import com.swirlds.config.api.ConfigurationBuilder;
import com.swirlds.merkledb.config.MerkleDbConfig;
import com.swirlds.metrics.api.Metric;
import com.swirlds.metrics.api.MetricConfig;
import com.swirlds.metrics.api.Metrics;
import com.swirlds.state.BinaryState;
import com.swirlds.state.merkle.VirtualMapState;
import com.swirlds.state.merkle.VirtualMapStateLifecycleManager;
import com.swirlds.virtualmap.config.VirtualMapConfig;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import org.hiero.base.file.FileSystemManager;

/// Writes a state snapshot the `state-management-hashgraph` plugin restores on boot, so E2E tests
/// can read real state over gRPC instead of only `NOT_READY`.
///
/// The snapshot lands in `<recentRoot>/<SEED_BLOCK_NUMBER>` and holds a `BlockStreamInfo` singleton
/// with that block number, which restore's self-check requires. Values are written with the raw
/// `BinaryState` API, so the seeded bytes are exactly what queries return. The generator also
/// applies block 1's changes the way the plugin's applier stores them (the oneof payload of each
/// `state_changes` wrapper) and returns the footer hashes blocks 1 and 2 must carry, so a test can
/// stream blocks the running plugin applies and attests.
public final class StateSeedSnapshotGenerator {

    /// Block the seed snapshot is attested at, and its directory name.
    public static final long SEED_BLOCK_NUMBER = 0L;

    /// Seeded state ids. KV and queue ids must be >= 2 (field 1 tags singleton keys).
    public static final int SINGLETON_STATE_ID = 7;

    public static final int KV_STATE_ID = 8;
    public static final int QUEUE_STATE_ID = 10;

    /// Seeded values, stored and returned as-is.
    public static final Bytes SINGLETON_VALUE = Bytes.fromHex("c0ffee");

    public static final Bytes KV_KEY = Bytes.fromHex("ab");
    public static final Bytes KV_VALUE = Bytes.fromHex("0102");
    public static final List<Bytes> QUEUE_ELEMENTS = List.of(Bytes.fromHex("dd"), Bytes.fromHex("de"));

    /// Raw values block 1 carries in its `state_changes`.
    private static final Bytes BLOCK1_SINGLETON_RAW = Bytes.fromHex("beef");

    private static final Bytes BLOCK1_KV_KEY_RAW = Bytes.fromHex("cd");
    private static final String BLOCK1_KV_VALUE_RAW = "applied";
    private static final Bytes BLOCK1_QUEUE_ELEMENT_RAW = Bytes.fromHex("ee");

    /// Block 1's changes as `state_changes` wrappers.
    private static final SingletonUpdateChange BLOCK1_SINGLETON =
            SingletonUpdateChange.newBuilder().bytesValue(BLOCK1_SINGLETON_RAW).build();

    private static final MapChangeKey BLOCK1_KV_KEY =
            MapChangeKey.newBuilder().protoBytesKey(BLOCK1_KV_KEY_RAW).build();
    private static final MapChangeValue BLOCK1_KV_VALUE =
            MapChangeValue.newBuilder().protoStringValue(BLOCK1_KV_VALUE_RAW).build();
    private static final QueuePushChange BLOCK1_QUEUE_PUSH = QueuePushChange.newBuilder()
            .protoBytesElement(BLOCK1_QUEUE_ELEMENT_RAW)
            .build();

    /// Block 1's values as the plugin stores them: the payload inside each wrapper.
    public static final Bytes BLOCK1_SINGLETON_STORED =
            payloadOf(SingletonUpdateChange.PROTOBUF.toBytes(BLOCK1_SINGLETON));

    public static final Bytes BLOCK1_KV_KEY_STORED = payloadOf(MapChangeKey.PROTOBUF.toBytes(BLOCK1_KV_KEY));
    public static final Bytes BLOCK1_KV_VALUE_STORED = payloadOf(MapChangeValue.PROTOBUF.toBytes(BLOCK1_KV_VALUE));
    public static final Bytes BLOCK1_QUEUE_ELEMENT_STORED =
            payloadOf(QueuePushChange.PROTOBUF.toBytes(BLOCK1_QUEUE_PUSH));

    /// Footer hashes and block 1's changes for streaming on top of the seed.
    ///
    /// @param block1StartHash `startOfBlockStateRootHash` for block 1: the seed's root hash
    /// @param block2StartHash `startOfBlockStateRootHash` for block 2: the root hash after block 1
    /// @param block1StateChanges the changes block 1 must carry to produce `block2StartHash`
    public record SeededChain(Bytes block1StartHash, Bytes block2StartHash, StateChanges block1StateChanges) {}

    private StateSeedSnapshotGenerator() {}

    /// Write the seed snapshot to `<recentRoot>/<SEED_BLOCK_NUMBER>` and compute the chain.
    ///
    /// @param recentRoot the directory `state.management.snapshotRecentPath` will point at
    /// @return the footer hashes and block 1 changes for streaming on top of the seed
    /// @throws Exception if the state cannot be built or written
    public static SeededChain generate(final Path recentRoot) throws Exception {
        Files.createDirectories(recentRoot);
        final var configuration = ConfigurationBuilder.create()
                .withConfigDataType(MerkleDbConfig.class)
                .withConfigDataType(VirtualMapConfig.class)
                .build();
        final var manager = new VirtualMapStateLifecycleManager(
                noOpMetrics(), Time.getCurrent(), configuration, new FileSystemManager(recentRoot));
        manager.copyMutableState();

        final BinaryState seed = manager.getMutableState();
        seed.updateSingleton(SINGLETON_STATE_ID, SINGLETON_VALUE);
        seed.updateKv(KV_STATE_ID, KV_KEY, KV_VALUE);
        QUEUE_ELEMENTS.forEach(element -> seed.pushQueue(QUEUE_STATE_ID, element));
        // block_time keeps the message non-empty: an all-default BlockStreamInfo (block 0 and
        // nothing else) encodes to zero bytes, which restore reads as "no block number".
        seed.updateSingleton(
                StateIdentifier.STATE_ID_BLOCK_STREAM_INFO.protoOrdinal(),
                BlockStreamInfo.PROTOBUF.toBytes(BlockStreamInfo.newBuilder()
                        .blockNumber(SEED_BLOCK_NUMBER)
                        .blockTime(new Timestamp(123L, 456))
                        .build()));
        manager.copyMutableState();
        final VirtualMapState sealedSeed = manager.getLatestImmutableState();
        final Bytes block1StartHash = rootHashOf(sealedSeed);
        manager.createSnapshot(sealedSeed, recentRoot.resolve(Long.toString(SEED_BLOCK_NUMBER)));

        final BinaryState block1 = manager.getMutableState();
        block1.updateSingleton(SINGLETON_STATE_ID, BLOCK1_SINGLETON_STORED);
        block1.updateKv(KV_STATE_ID, BLOCK1_KV_KEY_STORED, BLOCK1_KV_VALUE_STORED);
        block1.pushQueue(QUEUE_STATE_ID, BLOCK1_QUEUE_ELEMENT_STORED);
        manager.copyMutableState();
        final Bytes block2StartHash = rootHashOf(manager.getLatestImmutableState());

        final StateChanges block1StateChanges = StateChanges.newBuilder()
                .stateChanges(
                        StateChange.newBuilder()
                                .stateId(SINGLETON_STATE_ID)
                                .singletonUpdate(BLOCK1_SINGLETON)
                                .build(),
                        StateChange.newBuilder()
                                .stateId(KV_STATE_ID)
                                .mapUpdate(MapUpdateChange.newBuilder()
                                        .key(BLOCK1_KV_KEY)
                                        .value(BLOCK1_KV_VALUE)
                                        .build())
                                .build(),
                        StateChange.newBuilder()
                                .stateId(QUEUE_STATE_ID)
                                .queuePush(BLOCK1_QUEUE_PUSH)
                                .build())
                .build();
        return new SeededChain(block1StartHash, block2StartHash, block1StateChanges);
    }

    private static Bytes rootHashOf(final VirtualMapState state) {
        return Bytes.wrap(state.getRoot().getHash().copyToByteArray());
    }

    /// The first length-delimited field's payload of a single-field wrapper message, as the
    /// plugin's applier extracts it.
    private static Bytes payloadOf(final Bytes wrapper) {
        final ReadableSequentialData input = wrapper.toReadableSequentialData();
        final int tag = input.readVarInt(false);
        if ((tag & 0x7) != 2) {
            throw new IllegalStateException("Expected a length-delimited field, got wire type " + (tag & 0x7));
        }
        return input.readBytes(input.readVarInt(false));
    }

    /// A `Metrics` that hands out no-op metrics. The lifecycle manager only creates metrics and
    /// updates them; the plugin's own no-op implementation is not exported.
    private static Metrics noOpMetrics() {
        return (Metrics) Proxy.newProxyInstance(
                Metrics.class.getClassLoader(), new Class<?>[] {Metrics.class}, (proxy, method, args) -> {
                    if (method.getName().equals("getOrCreate")) {
                        return noOp(((MetricConfig<?, ?>) args[0]).getResultClass());
                    }
                    return defaultFor(proxy, method, args);
                });
    }

    private static Metric noOp(final Class<? extends Metric> type) {
        return (Metric) Proxy.newProxyInstance(
                type.getClassLoader(), new Class<?>[] {type}, StateSeedSnapshotGenerator::defaultFor);
    }

    private static Object defaultFor(final Object proxy, final Method method, final Object[] args) {
        return switch (method.getName()) {
            case "equals" -> proxy == args[0];
            case "hashCode" -> System.identityHashCode(proxy);
            case "toString" -> "no-op " + method.getDeclaringClass().getSimpleName();
            default -> {
                final Class<?> type = method.getReturnType();
                if (type == boolean.class) {
                    yield false;
                } else if (type == double.class) {
                    yield 0d;
                } else if (type == float.class) {
                    yield 0f;
                } else if (type == long.class) {
                    yield 0L;
                } else if (type == int.class) {
                    yield 0;
                } else if (type == short.class) {
                    yield (short) 0;
                } else if (type == byte.class) {
                    yield (byte) 0;
                } else if (type == char.class) {
                    yield '\0';
                } else if (Collection.class.isAssignableFrom(type)) {
                    yield List.of();
                }
                yield null;
            }
        };
    }
}
