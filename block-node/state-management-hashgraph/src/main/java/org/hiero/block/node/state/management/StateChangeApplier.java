// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.state.management;

import com.hedera.hapi.block.stream.input.RoundHeader;
import com.hedera.hapi.block.stream.output.BlockFooter;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.ProtoConstants;
import com.hedera.pbj.runtime.ProtoParserTools;
import com.hedera.pbj.runtime.io.ReadableSequentialData;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.state.BinaryState;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.io.IOException;
import java.util.List;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockUnparsed;

/// Walks a verified block once, applying every `state_changes` mutation to a `BinaryState`
/// and returning the per-block metadata (`blockNumber`, `roundNumber`, applied-change count).
///
/// ## Storage encoding
/// `BinaryState` (`VirtualMapStateImpl`) wraps whatever bytes it's given with its own
/// `(stateId << 3 | WIRE_TYPE_DELIMITED)` tag — it expects the **raw inner entity payload**,
/// not an already-tagged oneof wrapper message. `state_changes` items carry the wrapper
/// (`MapChangeKey`/`MapChangeValue`/`SingletonUpdateChange`/`QueuePushChange`), so each one is
/// walked at the protobuf wire level (mirroring CN's reference
/// `BlockStreamRecoveryWorkflow.BinaryStateChangeApplier`) to extract just the first delimited
/// field's payload — the oneof-selected value — rather than re-serializing the whole wrapper.
///
/// Token-relationship keys (`stateId == 9`, `MapChangeKey.token_relationship_key`, field 25) do
/// **not** need the `TokenAssociation` → `EntityIDPair` field-reorder CN's older reference
/// applier performs: that reorder targeted the now-reserved field 2 (`TokenAssociation`,
/// token_id then account_id). The current proto (confirmed against this repo's pinned
/// `state_changes.proto`/`basic_types.proto`, v0.76.1) carries this key as
/// `AccountTokenAssociation` instead — introduced upstream specifically to fix this mismatch —
/// with the same field order and types as `EntityIDPair` (account_id=1, token_id=2), so the
/// extracted payload is already state-storage-compatible with no remap.
///
/// PBJ's generated oneof accessors hand back a decoded Java object for whichever variant is
/// set, not that variant's raw bytes — extracting "just the payload" from an already-parsed
/// `StateChange` would mean re-encoding by hand through a switch over every oneof variant,
/// which silently goes stale as HAPI adds new ones. Walking the wire format directly needs no
/// such switch.
///
/// ## Why no separate `inspectBlock`
/// The plugin needs `BlockFooter.startOfBlockStateRootHash` before applying, to validate the
/// incoming block against our live state. `extractStartOfBlockStateRootHash` gets it without
/// a full walk by scanning from the end until it finds the footer, with no position assumed.
final class StateChangeApplier {

    private static final System.Logger LOGGER = System.getLogger(StateChangeApplier.class.getName());

    /// Outcome of a single block apply.
    ///
    /// @param blockNumber the block number, or `-1` if unparseable
    /// @param roundNumber the round number, or `-1` if absent
    /// @param appliedChanges the number of `state_changes` mutations applied
    record ApplyResult(long blockNumber, long roundNumber, int appliedChanges) {}

    /// The raw bytes of a single oneof-selected field, plus the field number it was tagged
    /// with. The field number is unused today but must survive the extraction step: CN's
    /// `state-changes-diffing` branch selects a schema by this same field number to support
    /// partial `state_changes` updates, and this is the one place that number is available.
    ///
    /// @param fieldNumber the protobuf field number the payload was tagged with
    /// @param bytes the raw bytes of that field's value
    private record OneOfPayload(int fieldNumber, @NonNull Bytes bytes) {}

    /// Apply every `state_changes` item in `block` to `binaryState`.
    ///
    /// @param binaryState the state to mutate
    /// @param block the verified block whose items are walked once
    /// @return the per-block metadata
    /// @throws IllegalStateException if a `state_changes` item is malformed.
    @NonNull
    ApplyResult applyBlock(@NonNull final BinaryState binaryState, @NonNull final BlockUnparsed block) {
        long blockNumber = -1L;
        long roundNumber = -1L;
        int applied = 0;

        for (final var item : block.blockItems()) {
            if (item.hasBlockHeader() && blockNumber < 0L) {
                try {
                    blockNumber = BlockHeader.PROTOBUF
                            .parse(item.blockHeaderOrThrow())
                            .number();
                } catch (final ParseException ignored) {
                    // Leaves blockNumber = -1; caller treats that as a failed apply.
                }
            } else if (item.hasRoundHeader()) {
                try {
                    roundNumber = RoundHeader.PROTOBUF
                            .parse(item.roundHeaderOrThrow())
                            .roundNumber();
                } catch (final ParseException ignored) {
                    // Keep previous round number on parse failure.
                }
            } else if (item.hasStateChanges()) {
                try {
                    applied += applyStateChangesBytes(binaryState, item.stateChangesOrThrow());
                } catch (final RuntimeException e) {
                    throw new IllegalStateException("Failed to parse state_changes item", e);
                }
            }
            // BlockFooter is read by extractStartOfBlockStateRootHash before this
            // method runs; we don't need to re-parse it here.
        }
        return new ApplyResult(blockNumber, roundNumber, applied);
    }

    /// Pull the `BlockFooter.startOfBlockStateRootHash` from a block without walking the
    /// whole item list: scans backward from the end until it finds the footer, with no
    /// position assumed.
    ///
    /// @param block the block to scan
    /// @return the hash, or `null` if the block has no parseable footer.
    @Nullable
    static Bytes extractStartOfBlockStateRootHash(@NonNull final BlockUnparsed block) {
        final List<BlockItemUnparsed> items = block.blockItems();
        for (int i = items.size() - 1; i >= 0; i--) {
            final BlockItemUnparsed item = items.get(i);
            if (item.hasBlockFooter()) {
                try {
                    final BlockFooter footer = BlockFooter.PROTOBUF.parse(item.blockFooterOrThrow());
                    return footer.startOfBlockStateRootHash() == null
                            ? Bytes.EMPTY
                            : footer.startOfBlockStateRootHash();
                } catch (final ParseException ignored) {
                    return null;
                }
            }
        }
        return null;
    }

    // ── Wire-level StateChanges walk ──────────────────────────────────────────
    // Mirrors CN's BlockStreamRecoveryWorkflow.BinaryStateChangeApplier. Field numbers below are
    // from block/stream/output/state_changes.proto (StateChanges, StateChange, MapUpdateChange,
    // MapDeleteChange) and don't change independently of that schema.

    /// Walk one `StateChanges` message's raw bytes (field 1 = consensus_timestamp, skipped;
    /// field 2 = repeated `StateChange`) and apply each `StateChange` found.
    ///
    /// @param binaryState the state to mutate
    /// @param stateChangesBytes the raw `StateChanges` protobuf bytes
    /// @return the number of changes actually applied (skipped schema events excluded)
    private static int applyStateChangesBytes(
            @NonNull final BinaryState binaryState, @NonNull final Bytes stateChangesBytes) {
        int applied = 0;
        final ReadableSequentialData input = stateChangesBytes.toReadableSequentialData();
        while (input.hasRemaining()) {
            final int tag = input.readVarInt(false);
            switch (tag) {
                case 10 -> skipMessage(input); // consensus_timestamp: field 1, message
                case 18 -> { // state_changes: field 2, message
                    final int messageLength = input.readVarInt(false);
                    if (messageLength > 0) {
                        final long endPosition = input.position() + messageLength;
                        if (applyStateChange(binaryState, input, endPosition)) {
                            applied++;
                        }
                    }
                }
                default -> skipField(input, tag);
            }
        }
        return applied;
    }

    /// Walk one `StateChange` message and dispatch its oneof-selected mutation.
    ///
    /// @param binaryState the state to mutate
    /// @param input positioned at the start of the `StateChange` message
    /// @param endPosition the byte offset the message ends at
    /// @return `true` if a mutation was applied (schema-level events are skipped, not applied)
    private static boolean applyStateChange(
            @NonNull final BinaryState binaryState,
            @NonNull final ReadableSequentialData input,
            final long endPosition) {
        int stateId = -1;
        boolean applied = false;
        while (input.position() < endPosition) {
            final int tag = input.readVarInt(false);
            switch (tag) {
                case 8 -> stateId = ProtoParserTools.readUint32(input); // state_id: field 1, uint32
                case 18, 26 -> { // state_add: field 2, state_remove: field 3 — out of scope for v1
                    LOGGER.log(System.Logger.Level.DEBUG, "Skipping unsupported schema change (stateId={0})", stateId);
                    skipMessage(input);
                }
                case 34 -> { // singleton_update: field 4, message
                    final int messageLength = input.readVarInt(false);
                    if (messageLength > 0) {
                        final Bytes rawValue = readOneOfPayload(
                                        input, input.position() + messageLength, "SingletonUpdateChange")
                                .bytes();
                        binaryState.updateSingleton(requireStateId(stateId), rawValue);
                        applied = true;
                    }
                }
                case 42 -> { // map_update: field 5, message
                    final int messageLength = input.readVarInt(false);
                    if (messageLength > 0) {
                        applyMapUpdate(binaryState, requireStateId(stateId), input, input.position() + messageLength);
                        applied = true;
                    }
                }
                case 50 -> { // map_delete: field 6, message
                    final int messageLength = input.readVarInt(false);
                    if (messageLength > 0) {
                        applyMapDelete(binaryState, requireStateId(stateId), input, input.position() + messageLength);
                        applied = true;
                    }
                }
                case 58 -> { // queue_push: field 7, message
                    final int messageLength = input.readVarInt(false);
                    if (messageLength > 0) {
                        final Bytes rawElement = readOneOfPayload(
                                        input, input.position() + messageLength, "QueuePushChange")
                                .bytes();
                        binaryState.pushQueue(requireStateId(stateId), rawElement);
                        applied = true;
                    }
                }
                case 66 -> { // queue_pop: field 8, message (empty)
                    skipMessage(input);
                    binaryState.popQueue(requireStateId(stateId));
                    applied = true;
                }
                default -> skipField(input, tag);
            }
        }
        return applied;
    }

    /// Walk a `MapUpdateChange` message (`key` field 1, `value` field 2) and apply it.
    private static void applyMapUpdate(
            @NonNull final BinaryState binaryState,
            final int stateId,
            @NonNull final ReadableSequentialData input,
            final long endPosition) {
        Bytes rawKey = null;
        Bytes rawValue = null;
        while (input.position() < endPosition) {
            final int tag = input.readVarInt(false);
            switch (tag) {
                case 10 -> { // key: field 1, message
                    final int messageLength = input.readVarInt(false);
                    if (messageLength > 0) {
                        rawKey = readOneOfPayload(input, input.position() + messageLength, "MapChangeKey")
                                .bytes();
                    }
                }
                case 18 -> { // value: field 2, message
                    final int messageLength = input.readVarInt(false);
                    if (messageLength > 0) {
                        rawValue = readOneOfPayload(input, input.position() + messageLength, "MapChangeValue")
                                .bytes();
                    }
                }
                default -> skipField(input, tag);
            }
        }
        if (rawKey == null || rawValue == null) {
            throw new IllegalStateException("MapChangeKey or MapChangeValue missing from MapUpdateChange");
        }
        binaryState.updateKv(stateId, rawKey, rawValue);
    }

    /// Walk a `MapDeleteChange` message (`key` field 1) and apply it.
    private static void applyMapDelete(
            @NonNull final BinaryState binaryState,
            final int stateId,
            @NonNull final ReadableSequentialData input,
            final long endPosition) {
        Bytes rawKey = null;
        while (input.position() < endPosition) {
            final int tag = input.readVarInt(false);
            if (tag == 10) { // key: field 1, message
                final int messageLength = input.readVarInt(false);
                if (messageLength > 0) {
                    rawKey = readOneOfPayload(input, input.position() + messageLength, "MapChangeKey")
                            .bytes();
                }
            } else {
                skipField(input, tag);
            }
        }
        if (rawKey == null) {
            throw new IllegalStateException("MapChangeKey missing from MapDeleteChange");
        }
        binaryState.removeKv(stateId, rawKey);
    }

    /// Read the first delimited (length-prefixed) field within a message — the oneof-selected
    /// payload — and the field number it was tagged with.
    ///
    /// @param input positioned at the start of the oneof wrapper message
    /// @param endPosition the byte offset the message ends at
    /// @param description the wrapper type name, for the error message if nothing is set
    /// @return the payload and its field number
    @NonNull
    private static OneOfPayload readOneOfPayload(
            @NonNull final ReadableSequentialData input, final long endPosition, @NonNull final String description) {
        Bytes payload = null;
        int fieldNumber = -1;
        while (input.position() < endPosition) {
            final int tag = input.readVarInt(false);
            final ProtoConstants wireType = ProtoConstants.get(tag & ProtoConstants.TAG_WIRE_TYPE_MASK);
            if (payload == null && wireType == ProtoConstants.WIRE_TYPE_DELIMITED) {
                fieldNumber = tag >>> ProtoParserTools.TAG_FIELD_OFFSET;
                final int length = input.readVarInt(false);
                payload = input.readBytes(length);
            } else {
                skipField(input, wireType);
            }
        }
        if (payload == null) {
            throw new IllegalStateException(description + " payload missing");
        }
        return new OneOfPayload(fieldNumber, payload);
    }

    private static int requireStateId(final int stateId) {
        if (stateId < 0) {
            throw new IllegalStateException("StateChange missing state_id");
        }
        return stateId;
    }

    private static void skipField(@NonNull final ReadableSequentialData input, final int tag) {
        skipField(input, ProtoConstants.get(tag & ProtoConstants.TAG_WIRE_TYPE_MASK));
    }

    private static void skipField(@NonNull final ReadableSequentialData input, @NonNull final ProtoConstants wireType) {
        try {
            ProtoParserTools.skipField(input, wireType);
        } catch (final IOException e) {
            throw new IllegalStateException("Failed to skip protobuf field with wire type " + wireType, e);
        }
    }

    private static void skipMessage(@NonNull final ReadableSequentialData input) {
        final int messageLength = input.readVarInt(false);
        input.skip(messageLength);
    }
}
