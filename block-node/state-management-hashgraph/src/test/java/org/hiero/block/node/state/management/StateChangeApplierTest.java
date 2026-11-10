// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.state.management;

import static org.assertj.core.api.Assertions.assertThat;

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
    void legacyFieldTwoTokenAssociationEncodingHaltsWithDistinctError() {
        // MapChangeKey's old, reserved field-2 (TokenAssociation) encoding has no PBJ builder
        // accessor (reserved fields get none), so this constructs the StateChanges payload by
        // hand at the wire level — the same level StateChangeApplier itself parses at — rather
        // than through typed builders.
        final InMemoryBinaryState state = new InMemoryBinaryState();
        final StateChangeApplier applier = new StateChangeApplier();

        final Bytes value = MapChangeValue.PROTOBUF.toBytes(
                MapChangeValue.newBuilder().protoStringValue("associated").build());
        // Field 2 (reserved TokenAssociation), length-delimited. Content is irrelevant — only
        // the field number matters to trigger the legacy-encoding check.
        final byte[] legacyKey =
                lengthDelimitedField(2, Bytes.fromHex("0a03666f6f").toByteArray());
        final byte[] mapUpdateChange = concat(
                lengthDelimitedField(1, legacyKey), // MapUpdateChange.key: field 1
                lengthDelimitedField(2, value.toByteArray())); // MapUpdateChange.value: field 2
        final byte[] stateChange = concat(
                varintField(1, TOKEN_ASSOCIATION_STATE_ID), // StateChange.state_id: field 1
                lengthDelimitedField(5, mapUpdateChange)); // StateChange.map_update: field 5
        final Bytes stateChangesBytes =
                Bytes.wrap(lengthDelimitedField(2, stateChange)); // StateChanges.state_changes: field 2

        final BlockUnparsed block = BlockUnparsed.newBuilder()
                .blockItems(
                        blockHeaderItem(7L),
                        BlockItemUnparsed.newBuilder()
                                .stateChanges(stateChangesBytes)
                                .build())
                .build();

        final var result = applier.applyBlock(state, block);

        assertThat(result.status()).isEqualTo(StateChangeApplier.Status.LEGACY_TOKEN_ASSOCIATION);
        assertThat(result.detail()).contains("legacy").contains("stateId=" + TOKEN_ASSOCIATION_STATE_ID);
        assertThat(state.size()).isZero();
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

        final var result = applier.applyBlock(state, block);

        assertThat(result.status()).isEqualTo(StateChangeApplier.Status.MALFORMED_STATE_CHANGES);
        assertThat(state.size()).isZero();
    }

    @Test
    void incompleteChangesAreReportedAsMalformedWithoutMutatingState() {
        final StateChangeApplier applier = new StateChangeApplier();
        // map_update with a key but no value, a state change with no state_id, and a queue push
        // whose wrapper carries no payload: each is malformed, none may touch the state.
        final byte[] key = lengthDelimitedField(1, Bytes.fromHex("0a03666f6f").toByteArray());
        final byte[] keyOnlyUpdate = lengthDelimitedField(1, key); // MapUpdateChange.key only
        final byte[] noValue = concat(varintField(1, 4), lengthDelimitedField(5, keyOnlyUpdate));
        final byte[] noStateId =
                lengthDelimitedField(5, concat(lengthDelimitedField(1, key), lengthDelimitedField(2, key)));
        final byte[] emptyPush = concat(varintField(1, 5), lengthDelimitedField(7, new byte[] {0x08, 0x01}));

        for (final byte[] change : List.of(noValue, noStateId, emptyPush)) {
            final InMemoryBinaryState state = new InMemoryBinaryState();
            final BlockUnparsed block = BlockUnparsed.newBuilder()
                    .blockItems(
                            blockHeaderItem(7L),
                            BlockItemUnparsed.newBuilder()
                                    .stateChanges(Bytes.wrap(lengthDelimitedField(2, change)))
                                    .build())
                    .build();

            final var result = applier.applyBlock(state, block);

            assertThat(result.status()).isEqualTo(StateChangeApplier.Status.MALFORMED_STATE_CHANGES);
            assertThat(result.detail()).isNotBlank();
            assertThat(state.size()).isZero();
        }
    }

    @Test
    void blockWithoutParseableHeaderIsReportedAsUnparseableHeader() {
        final InMemoryBinaryState state = new InMemoryBinaryState();
        final StateChangeApplier applier = new StateChangeApplier();
        final BlockUnparsed block = BlockUnparsed.newBuilder()
                .blockItems(BlockItemUnparsed.newBuilder()
                        .blockHeader(Bytes.fromHex("ffffffff"))
                        .build())
                .build();

        final var result = applier.applyBlock(state, block);

        assertThat(result.status()).isEqualTo(StateChangeApplier.Status.UNPARSEABLE_HEADER);
        assertThat(result.blockNumber()).isEqualTo(-1L);
    }

    @Test
    void partialUpdatesSpliceChangedFieldsAndClearedFieldsOntoThePriorValue() {
        // The prior value has fields 1..3. The partial replaces field 2 and clears field 3, so
        // the result is field 1 unchanged plus the new field 2. Cleared fields are checked in
        // both the packed and the unpacked encoding, for a map entry and for a singleton.
        final byte[] prior = concat(
                lengthDelimitedField(1, "aaa".getBytes()),
                lengthDelimitedField(2, "bbb".getBytes()),
                lengthDelimitedField(3, "ccc".getBytes()));
        final byte[] changed = lengthDelimitedField(2, "XX".getBytes());
        final byte[] expected = concat(lengthDelimitedField(1, "aaa".getBytes()), changed);
        final byte[] packedCleared = lengthDelimitedField(5, varint(3));
        final byte[] unpackedCleared = varintField(5, 3);

        for (final byte[] cleared : List.of(packedCleared, unpackedCleared)) {
            final InMemoryBinaryState state = new InMemoryBinaryState();
            final StateChangeApplier applier = new StateChangeApplier();
            applyRaw(applier, state, mapUpdate(4, KEY, prior));
            final var result = applyRaw(applier, state, mapUpdate(4, KEY, changed, varintField(4, 1), cleared));

            assertThat(result.status()).isEqualTo(StateChangeApplier.Status.APPLIED);
            assertThat(state.getKv(4, Bytes.wrap(KEY))).isEqualTo(Bytes.wrap(expected));
        }

        final byte[] singletonCleared = lengthDelimitedField(10002, varint(3));
        final byte[] singletonUnpackedCleared = varintField(10002, 3);
        for (final byte[] cleared : List.of(singletonCleared, singletonUnpackedCleared)) {
            final InMemoryBinaryState state = new InMemoryBinaryState();
            final StateChangeApplier applier = new StateChangeApplier();
            applyRaw(applier, state, singletonUpdate(5, prior));
            final var result = applyRaw(applier, state, singletonUpdate(5, changed, varintField(10001, 1), cleared));

            assertThat(result.status()).isEqualTo(StateChangeApplier.Status.APPLIED);
            assertThat(state.getSingleton(5)).isEqualTo(Bytes.wrap(expected));
        }
    }

    @Test
    void fullUpdateWithIdenticalFlagIsStoredAsTheValue() {
        final InMemoryBinaryState state = new InMemoryBinaryState();
        final byte[] value = lengthDelimitedField(1, "aaa".getBytes());

        final var result = applyRaw(
                new StateChangeApplier(), state, mapUpdate(4, KEY, value, varintField(3, 1), varintField(4, 0)));

        assertThat(result.status()).isEqualTo(StateChangeApplier.Status.APPLIED);
        assertThat(state.getKv(4, Bytes.wrap(KEY))).isEqualTo(Bytes.wrap(value));
    }

    @Test
    void partialUpdateWithoutAPriorValueIsReportedAndLeavesStateUnchanged() {
        final byte[] changed = lengthDelimitedField(2, "XX".getBytes());

        for (final byte[] change : List.of(
                mapUpdate(4, KEY, changed, varintField(4, 1)), singletonUpdate(5, changed, varintField(10001, 1)))) {
            final InMemoryBinaryState state = new InMemoryBinaryState();

            final var result = applyRaw(new StateChangeApplier(), state, change);

            assertThat(result.status()).isEqualTo(StateChangeApplier.Status.PARTIAL_WITHOUT_BASELINE);
            assertThat(result.detail()).contains("no prior value");
            assertThat(state.size()).isZero();
        }
    }

    @Test
    void invalidPartialUpdatesAreReportedAsMalformedAndLeaveTheValueUnchanged() {
        final byte[] prior =
                concat(lengthDelimitedField(1, "aaa".getBytes()), lengthDelimitedField(2, "bbb".getBytes()));
        final byte[] changed = lengthDelimitedField(2, "XX".getBytes());
        final byte[] partial = varintField(4, 1);
        final List<byte[]> invalid = List.of(
                mapUpdate(4, KEY, changed, varintField(4, 2)), // flag is not 0 or 1
                mapUpdate(4, KEY, changed, lengthDelimitedField(5, varint(3))), // cleared but not partial
                mapUpdate(4, KEY, changed, partial, varintField(9, 1)), // unknown metadata on a partial
                mapUpdate(4, KEY, changed, partial, lengthDelimitedField(5, concat(varint(3), varint(2)))), // unsorted
                mapUpdate(4, KEY, changed, partial, lengthDelimitedField(5, varint(2))), // clears a replaced field
                mapUpdate(4, KEY, new byte[] {0x7f}, partial)); // truncated partial bytes

        for (final byte[] change : invalid) {
            final InMemoryBinaryState state = new InMemoryBinaryState();
            final StateChangeApplier applier = new StateChangeApplier();
            applyRaw(applier, state, mapUpdate(4, KEY, prior));

            final var result = applyRaw(applier, state, change);

            assertThat(result.status()).isEqualTo(StateChangeApplier.Status.MALFORMED_STATE_CHANGES);
            assertThat(state.getKv(4, Bytes.wrap(KEY))).isEqualTo(Bytes.wrap(prior));
        }
    }

    // ── Fixtures ───────────────────────────────────────────────────────────

    private static final int TOKEN_ASSOCIATION_STATE_ID = 9;

    private static final byte[] KEY = "key".getBytes();

    /// A `StateChange` for `stateId` carrying a `MapUpdateChange` with the given key and value
    /// payloads, plus any extra raw `MapUpdateChange` fields (for example `partial`).
    private static byte[] mapUpdate(
            final int stateId, final byte[] keyPayload, final byte[] valuePayload, final byte[]... extra) {
        final byte[] update = concat(
                lengthDelimitedField(1, lengthDelimitedField(1, keyPayload)), // key: MapChangeKey
                lengthDelimitedField(2, lengthDelimitedField(1, valuePayload)), // value: MapChangeValue
                concat(extra));
        return concat(varintField(1, stateId), lengthDelimitedField(5, update));
    }

    /// A `StateChange` for `stateId` carrying a `SingletonUpdateChange` whose oneof payload is
    /// field 3, plus any extra raw fields (for example `partial`).
    private static byte[] singletonUpdate(final int stateId, final byte[] payload, final byte[]... extra) {
        final byte[] update = concat(lengthDelimitedField(3, payload), concat(extra));
        return concat(varintField(1, stateId), lengthDelimitedField(4, update));
    }

    /// Apply one block holding the given raw `StateChange` messages, each as its own
    /// `StateChanges.state_changes` entry.
    private static StateChangeApplier.ApplyResult applyRaw(
            final StateChangeApplier applier, final InMemoryBinaryState state, final byte[]... stateChanges) {
        final byte[][] entries = new byte[stateChanges.length][];
        for (int i = 0; i < stateChanges.length; i++) {
            entries[i] = lengthDelimitedField(2, stateChanges[i]);
        }
        return applier.applyBlock(
                state,
                BlockUnparsed.newBuilder()
                        .blockItems(
                                blockHeaderItem(7L),
                                BlockItemUnparsed.newBuilder()
                                        .stateChanges(Bytes.wrap(concat(entries)))
                                        .build())
                        .build());
    }

    /// Minimal hand-rolled protobuf wire encoding, used for shapes PBJ builders cannot produce (the
    /// legacy field-2 `MapChangeKey`, and partial-update metadata not in the pinned proto).

    private static byte[] varint(long value) {
        final var out = new java.io.ByteArrayOutputStream();
        while (true) {
            if ((value & ~0x7FL) == 0) {
                out.write((int) value);
                return out.toByteArray();
            }
            out.write((int) ((value & 0x7F) | 0x80));
            value >>>= 7;
        }
    }

    private static byte[] lengthDelimitedField(final int fieldNumber, final byte[] payload) {
        return concat(varint(((long) fieldNumber << 3) | 2L), varint(payload.length), payload);
    }

    private static byte[] varintField(final int fieldNumber, final long value) {
        return concat(varint(((long) fieldNumber << 3)), varint(value));
    }

    private static byte[] concat(final byte[]... parts) {
        final var out = new java.io.ByteArrayOutputStream();
        for (final byte[] part : parts) {
            out.writeBytes(part);
        }
        return out.toByteArray();
    }

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
