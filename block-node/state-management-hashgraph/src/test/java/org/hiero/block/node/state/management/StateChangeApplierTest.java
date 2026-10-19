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
import com.hedera.hapi.block.stream.output.StateChanges;
import com.hedera.hapi.node.base.AccountID;
import com.hedera.hapi.node.base.AccountTokenAssociation;
import com.hedera.hapi.node.base.TokenID;
import com.hedera.hapi.node.state.common.EntityIDPair;
import com.hedera.pbj.runtime.io.ReadableSequentialData;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.ArrayList;
import java.util.List;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockUnparsed;
import org.junit.jupiter.api.Test;

/**
 * Direct unit tests for {@link StateChangeApplier}. The applier is the
 * load-bearing piece of the apply path — every block applied to the live state
 * goes through it. Fixtures use typed PBJ builder setters (e.g. {@code protoBytesKey},
 * {@code protoStringValue}, {@code bytesValue}, {@code protoBytesElement}) to
 * select a oneof variant.
 */
class StateChangeApplierTest {

    @Test
    void appliesSingletonAndKvAndQueueOperations() {
        final InMemoryBinaryState state = new InMemoryBinaryState();
        final StateChangeApplier applier = new StateChangeApplier();

        final SingletonUpdateChange singleton = SingletonUpdateChange.newBuilder()
                .bytesValue(Bytes.fromHex("aabbcc"))
                .build();
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
        // BinaryState stores the oneof-selected payload, not the whole wrapper message — see
        // StateChangeApplier's class doc.
        assertThat(state.getSingleton(1))
                .isEqualTo(firstDelimitedFieldPayload(SingletonUpdateChange.PROTOBUF.toBytes(singleton)));
        assertThat(state.getKv(2, firstDelimitedFieldPayload(MapChangeKey.PROTOBUF.toBytes(kvKey))))
                .isEqualTo(firstDelimitedFieldPayload(MapChangeValue.PROTOBUF.toBytes(kvValue)));
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
        final Bytes storedKey = firstDelimitedFieldPayload(MapChangeKey.PROTOBUF.toBytes(key));
        state.updateKv(2, storedKey, firstDelimitedFieldPayload(MapChangeValue.PROTOBUF.toBytes(value)));
        state.pushQueue(3, Bytes.fromHex("0a"));
        state.pushQueue(3, Bytes.fromHex("0b"));
        assertThat(state.size()).isEqualTo(3L);

        final BlockUnparsed block = buildBlockWithChanges(List.of(mapDeleteChange(2, key), queuePopChange(3)));

        applier.applyBlock(state, block);

        assertThat(state.getKv(2, storedKey)).isNull();
        assertThat(state.getQueueAsList(3)).hasSize(1);
    }

    @Test
    void tokenRelationshipKeyNeedsNoFieldReorder() {
        // Guards against AccountTokenAssociation's field order drifting from EntityIDPair's
        // again upstream — no remap needed today, unlike CN's older reference applier.
        final InMemoryBinaryState state = new InMemoryBinaryState();
        final StateChangeApplier applier = new StateChangeApplier();

        final AccountID accountId = AccountID.newBuilder().accountNum(1001).build();
        final TokenID tokenId = TokenID.newBuilder().tokenNum(2002).build();
        final MapChangeKey key = MapChangeKey.newBuilder()
                .tokenRelationshipKey(new AccountTokenAssociation(accountId, tokenId))
                .build();
        final MapChangeValue value =
                MapChangeValue.newBuilder().protoStringValue("associated").build();

        final BlockUnparsed block = buildBlockWithChanges(List.of(mapUpdateChange(9, key, value)));
        applier.applyBlock(state, block);

        final Bytes expectedKey = EntityIDPair.PROTOBUF.toBytes(new EntityIDPair(accountId, tokenId));
        assertThat(state.getKv(9, expectedKey))
                .isEqualTo(firstDelimitedFieldPayload(MapChangeValue.PROTOBUF.toBytes(value)));
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

        assertThatThrownBy(() -> applier.applyBlock(state, block)).isInstanceOf(IllegalStateException.class);
        assertThat(state.size()).isZero();
    }

    // ── Fixtures ───────────────────────────────────────────────────────────

    /// Independent re-implementation of the "first delimited field" extraction a oneof wrapper
    /// message undergoes in `StateChangeApplier` (not a call into production code), so tests can
    /// compute the expected stored bytes without hardcoding wire-format byte literals.
    private static Bytes firstDelimitedFieldPayload(final Bytes wrapperMessageBytes) {
        final ReadableSequentialData input = wrapperMessageBytes.toReadableSequentialData();
        final int tag = input.readVarInt(false);
        if ((tag & 0x7) != 2) {
            throw new IllegalStateException("Expected a length-delimited field, got wire type " + (tag & 0x7));
        }
        final int length = input.readVarInt(false);
        return input.readBytes(length);
    }

    private static BlockUnparsed buildBlockWithChanges(final List<StateChange> changes) {
        return BlockUnparsed.newBuilder()
                .blockItems(
                        blockHeaderItem(7L),
                        BlockItemUnparsed.newBuilder()
                                .stateChanges(encodeStateChanges(changes))
                                .build())
                .build();
    }

    /// Build the PBJ-encoded `state_changes` carrier bytes from a list of changes.
    private static Bytes encodeStateChanges(final List<StateChange> changes) {
        final StateChanges sc =
                StateChanges.newBuilder().stateChanges(new ArrayList<>(changes)).build();
        return StateChanges.PROTOBUF.toBytes(sc);
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
        return StateChange.newBuilder().stateId(stateId).singletonUpdate(change).build();
    }

    private static StateChange mapUpdateChange(final int stateId, final MapChangeKey key, final MapChangeValue value) {
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
                .queuePush(
                        QueuePushChange.newBuilder().protoBytesElement(payload).build())
                .build();
    }

    private static StateChange queuePopChange(final int stateId) {
        return StateChange.newBuilder()
                .stateId(stateId)
                .queuePop(QueuePopChange.newBuilder().build())
                .build();
    }
}
