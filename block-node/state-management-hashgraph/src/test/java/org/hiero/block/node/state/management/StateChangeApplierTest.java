// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.state.management;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.hedera.hapi.block.stream.output.MapChangeKey;
import com.hedera.hapi.block.stream.output.MapChangeValue;
import com.hedera.hapi.block.stream.output.MapDeleteChange;
import com.hedera.hapi.block.stream.output.MapUpdateChange;
import com.hedera.hapi.block.stream.output.QueuePopChange;
import com.hedera.hapi.block.stream.output.QueuePushChange;
import com.hedera.hapi.block.stream.output.SingletonUpdateChange;
import com.hedera.hapi.block.stream.output.StateChange;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.List;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockUnparsed;
import org.junit.jupiter.api.Test;

/**
 * Direct unit tests for {@link StateChangeApplier}. The applier is the
 * load-bearing piece of STORY-5 — every block applied to the live state goes
 * through it. Fixtures use typed PBJ builder setters (e.g. {@code protoBytesKey},
 * {@code protoStringValue}, {@code bytesValue}, {@code protoBytesElement}) to
 * select a oneof variant.
 */
class StateChangeApplierTest {

    @Test
    void appliesSingletonAndKvAndQueueOperations() {
        final InMemoryBinaryState state = new InMemoryBinaryState();
        final StateChangeApplier applier = new StateChangeApplier();

        final SingletonUpdateChange singleton =
                SingletonUpdateChange.newBuilder().bytesValue(Bytes.fromHex("aabbcc")).build();
        final MapChangeKey kvKey =
                MapChangeKey.newBuilder().protoBytesKey(Bytes.fromHex("01")).build();
        final MapChangeValue kvValue =
                MapChangeValue.newBuilder().protoStringValue("hello").build();

        final BlockUnparsed block = buildBlockWithChanges(List.of(
                singletonChange(1, singleton),
                mapUpdateChange(2, kvKey, kvValue),
                queuePushChange(3, Bytes.fromHex("0a")),
                queuePushChange(3, Bytes.fromHex("0b"))));

        final var result = applier.applyBlock(state, block);

        assertThat(result.blockNumber()).isEqualTo(7L);
        assertThat(result.appliedChanges()).isEqualTo(4);
        assertThat(state.getSingleton(1)).isEqualTo(SingletonUpdateChange.PROTOBUF.toBytes(singleton));
        assertThat(state.getKv(2, MapChangeKey.PROTOBUF.toBytes(kvKey)))
                .isEqualTo(MapChangeValue.PROTOBUF.toBytes(kvValue));
        assertThat(state.getQueueAsList(3)).hasSize(2);
    }

    @Test
    void mapDeleteAndQueuePopRemoveEntries() {
        final InMemoryBinaryState state = new InMemoryBinaryState();
        final StateChangeApplier applier = new StateChangeApplier();

        final MapChangeKey key =
                MapChangeKey.newBuilder().protoBytesKey(Bytes.fromHex("01")).build();
        final MapChangeValue value =
                MapChangeValue.newBuilder().protoStringValue("v").build();
        state.updateKv(2, MapChangeKey.PROTOBUF.toBytes(key), MapChangeValue.PROTOBUF.toBytes(value));
        state.pushQueue(3, Bytes.fromHex("0a"));
        state.pushQueue(3, Bytes.fromHex("0b"));
        assertThat(state.size()).isEqualTo(3L);

        final BlockUnparsed block = buildBlockWithChanges(List.of(
                mapDeleteChange(2, key),
                queuePopChange(3)));

        applier.applyBlock(state, block);

        assertThat(state.getKv(2, MapChangeKey.PROTOBUF.toBytes(key))).isNull();
        assertThat(state.getQueueAsList(3)).hasSize(1);
    }

    @Test
    void corruptStateChangesItemAborts() {
        final InMemoryBinaryState state = new InMemoryBinaryState();
        final StateChangeApplier applier = new StateChangeApplier();

        final BlockUnparsed block = BlockUnparsed.newBuilder()
                .blockItems(
                        blockHeaderItem(7L),
                        BlockItemUnparsed.newBuilder()
                                // not a parseable StateChanges payload — varint length > available bytes
                                .stateChanges(Bytes.fromHex("ffffffffff"))
                                .build())
                .build();

        assertThatThrownBy(() -> applier.applyBlock(state, block))
                .isInstanceOf(IllegalStateException.class);
        assertThat(state.size()).isZero();
    }

    // ── Fixtures ───────────────────────────────────────────────────────────

    private static BlockUnparsed buildBlockWithChanges(final List<StateChange> changes) {
        return BlockUnparsed.newBuilder()
                .blockItems(
                        blockHeaderItem(7L),
                        BlockItemUnparsed.newBuilder()
                                .stateChanges(StateChangeApplier.encodeStateChanges(changes))
                                .build())
                .build();
    }

    private static BlockItemUnparsed blockHeaderItem(final long blockNumber) {
        return BlockItemUnparsed.newBuilder()
                .blockHeader(com.hedera.hapi.block.stream.output.BlockHeader.PROTOBUF.toBytes(
                        com.hedera.hapi.block.stream.output.BlockHeader.newBuilder()
                                .number(blockNumber)
                                .build()))
                .build();
    }

    private static StateChange singletonChange(final int stateId, final SingletonUpdateChange change) {
        return StateChange.newBuilder()
                .stateId(stateId)
                .singletonUpdate(change)
                .build();
    }

    private static StateChange mapUpdateChange(
            final int stateId, final MapChangeKey key, final MapChangeValue value) {
        return StateChange.newBuilder()
                .stateId(stateId)
                .mapUpdate(MapUpdateChange.newBuilder().key(key).value(value).build())
                .build();
    }

    private static StateChange mapDeleteChange(final int stateId, final MapChangeKey key) {
        return StateChange.newBuilder()
                .stateId(stateId)
                .mapDelete(MapDeleteChange.newBuilder().key(key).build())
                .build();
    }

    private static StateChange queuePushChange(final int stateId, final Bytes payload) {
        return StateChange.newBuilder()
                .stateId(stateId)
                .queuePush(QueuePushChange.newBuilder().protoBytesElement(payload).build())
                .build();
    }

    private static StateChange queuePopChange(final int stateId) {
        return StateChange.newBuilder()
                .stateId(stateId)
                .queuePop(QueuePopChange.newBuilder().build())
                .build();
    }
}
