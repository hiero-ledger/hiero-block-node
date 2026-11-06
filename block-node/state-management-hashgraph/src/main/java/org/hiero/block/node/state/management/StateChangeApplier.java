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
/// `BinaryState` expects each entity's raw payload, not the `state_changes` oneof wrapper
/// (`MapChangeKey`/`MapChangeValue`/`SingletonUpdateChange`/`QueuePushChange`) around it, so
/// this walks the protobuf wire format directly to extract just the oneof-selected field,
/// rather than re-encoding an already-parsed `StateChange` by hand (which would need a switch
/// over every oneof variant and go stale as HAPI adds more).
///
/// Token-relationship keys (`stateId == 9`, field 25) need no field-reorder despite CN's older
/// reference applier performing one: the current proto carries this key as
/// `AccountTokenAssociation`, already in `EntityIDPair` order (account_id=1, token_id=2).
final class StateChangeApplier {

    private static final System.Logger LOGGER = System.getLogger(StateChangeApplier.class.getName());

    /// `stateId` for token-relationship (`TokenAssociation`/`AccountTokenAssociation`) keys.
    private static final int TOKEN_ASSOCIATION_STATE_ID = 9;

    /// `MapChangeKey` oneof field number for the old, reserved `TokenAssociation` encoding
    /// (`token_id` first) that consensus-node's own reference applier still remaps for older
    /// captured chains. This plugin does not remap it, see `Status.LEGACY_TOKEN_ASSOCIATION`.
    private static final int LEGACY_TOKEN_ASSOCIATION_FIELD = 2;

    /// How a block apply ended. Expected data conditions are reported here instead of thrown, so
    /// the caller switches on the status rather than catching exceptions as flow control.
    enum Status {
        /// Every `state_changes` item was applied.
        APPLIED,
        /// A `state_changes` item was malformed (a missing key, value, payload or state id, or
        /// truncated protobuf). The mutable state may be partly changed.
        MALFORMED_STATE_CHANGES,
        /// A `MapChangeKey` for `stateId == 9` carried the legacy, reserved field-2
        /// (`TokenAssociation`) encoding instead of the current field-25
        /// (`AccountTokenAssociation`) encoding. `reserved` in protobuf is a compile-time guard,
        /// not a runtime wire-format check, so these bytes would otherwise be stored unrecognized
        /// and only surface later as a generic, misleading hash mismatch.
        LEGACY_TOKEN_ASSOCIATION,
        /// The block carries no parseable `BlockHeader`.
        UNPARSEABLE_HEADER
    }

    /// Outcome of a single block apply.
    ///
    /// @param status how the apply ended
    /// @param detail a human-readable reason when `status` is not `APPLIED`, else `null`
    /// @param blockNumber the block number, or `-1` if unparseable
    /// @param roundNumber the round number, or `-1` if absent
    /// @param appliedChanges the number of `state_changes` mutations applied
    record ApplyResult(
            @NonNull Status status, @Nullable String detail, long blockNumber, long roundNumber, int appliedChanges) {

        /// @return `true` if the block was fully applied
        boolean applied() {
            return status == Status.APPLIED;
        }
    }

    /// The raw bytes of a single oneof-selected field, plus the field number it was tagged
    /// with. The field number is only used to detect the legacy token-association encoding.
    ///
    /// @param fieldNumber the protobuf field number the payload was tagged with
    /// @param bytes the raw bytes of that field's value
    private record OneOfPayload(int fieldNumber, @NonNull Bytes bytes) {}

    /// Mutable progress of one block's `state_changes` walk: how many mutations were applied and
    /// the first failure, if any. A failure stops the walk.
    private static final class Walk {
        private int applied;
        private Status failureStatus;
        private String failureDetail;

        boolean failed() {
            return failureStatus != null;
        }

        void fail(@NonNull final Status status, @NonNull final String detail) {
            if (failureStatus == null) {
                failureStatus = status;
                failureDetail = detail;
            }
        }
    }

    /// Apply every `state_changes` item in `block` to `binaryState`. Data problems come back in
    /// the result's status. Only an unexpected failure inside `binaryState` itself propagates.
    ///
    /// @param binaryState the state to mutate
    /// @param block the verified block whose items are walked once
    /// @return the per-block outcome
    @NonNull
    ApplyResult applyBlock(@NonNull final BinaryState binaryState, @NonNull final BlockUnparsed block) {
        long blockNumber = -1L;
        long roundNumber = -1L;
        final Walk walk = new Walk();

        for (final var item : block.blockItems()) {
            if (item.hasBlockHeader() && blockNumber < 0L) {
                try {
                    blockNumber = BlockHeader.PROTOBUF
                            .parse(item.blockHeaderOrThrow())
                            .number();
                } catch (final ParseException ignored) {
                    // Leaves blockNumber = -1, reported below as an unparseable header.
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
                applyStateChangesBytes(binaryState, item.stateChangesOrThrow(), walk);
                if (walk.failed()) {
                    return new ApplyResult(
                            walk.failureStatus, walk.failureDetail, blockNumber, roundNumber, walk.applied);
                }
            }
            // BlockFooter is read by extractStartOfBlockStateRootHash before this
            // method runs; we don't need to re-parse it here.
        }
        if (blockNumber < 0L) {
            return new ApplyResult(
                    Status.UNPARSEABLE_HEADER, "block has no parseable BlockHeader", -1L, roundNumber, walk.applied);
        }
        return new ApplyResult(Status.APPLIED, null, blockNumber, roundNumber, walk.applied);
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
    // MapDeleteChange) and don't change independently of that schema. Helpers record a failure
    // on the `Walk` and return; a malformed wire encoding that the protobuf reader itself
    // rejects (truncation) is caught once, in `applyStateChangesBytes`.

    /// Walk one `StateChanges` message's raw bytes (field 1 = consensus_timestamp, skipped;
    /// field 2 = repeated `StateChange`) and apply each `StateChange` found.
    ///
    /// @param binaryState the state to mutate
    /// @param stateChangesBytes the raw `StateChanges` protobuf bytes
    /// @param walk records the applied count and the first failure
    private static void applyStateChangesBytes(
            @NonNull final BinaryState binaryState, @NonNull final Bytes stateChangesBytes, @NonNull final Walk walk) {
        final ReadableSequentialData input = stateChangesBytes.toReadableSequentialData();
        try {
            while (input.hasRemaining() && !walk.failed()) {
                final int tag = input.readVarInt(false);
                switch (tag) {
                    case 10 -> skipMessage(input); // consensus_timestamp: field 1, message
                    case 18 -> { // state_changes: field 2, message
                        final int messageLength = input.readVarInt(false);
                        if (messageLength > 0) {
                            applyStateChange(binaryState, input, input.position() + messageLength, walk);
                        }
                    }
                    default -> skipField(input, tag);
                }
            }
        } catch (final RuntimeException | IOException e) {
            // The protobuf reader rejects truncated or invalid bytes by throwing; this is the one
            // place that turns that into a status.
            walk.fail(Status.MALFORMED_STATE_CHANGES, "Failed to parse state_changes item: " + e);
        }
    }

    /// Walk one `StateChange` message and dispatch its oneof-selected mutation.
    ///
    /// @param binaryState the state to mutate
    /// @param input positioned at the start of the `StateChange` message
    /// @param endPosition the byte offset the message ends at
    /// @param walk records the applied count and the first failure
    private static void applyStateChange(
            @NonNull final BinaryState binaryState,
            @NonNull final ReadableSequentialData input,
            final long endPosition,
            @NonNull final Walk walk)
            throws IOException {
        int stateId = -1;
        boolean applied = false;
        while (input.position() < endPosition && !walk.failed()) {
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
                        final OneOfPayload payload = readOneOfPayload(
                                input, input.position() + messageLength, "SingletonUpdateChange", walk);
                        if (payload != null && requireStateId(stateId, walk)) {
                            binaryState.updateSingleton(stateId, payload.bytes());
                            applied = true;
                        }
                    }
                }
                case 42 -> { // map_update: field 5, message
                    final int messageLength = input.readVarInt(false);
                    if (messageLength > 0) {
                        final boolean done = requireStateId(stateId, walk)
                                && applyMapUpdate(binaryState, stateId, input, input.position() + messageLength, walk);
                        applied |= done;
                    }
                }
                case 50 -> { // map_delete: field 6, message
                    final int messageLength = input.readVarInt(false);
                    if (messageLength > 0) {
                        final boolean done = requireStateId(stateId, walk)
                                && applyMapDelete(binaryState, stateId, input, input.position() + messageLength, walk);
                        applied |= done;
                    }
                }
                case 58 -> { // queue_push: field 7, message
                    final int messageLength = input.readVarInt(false);
                    if (messageLength > 0) {
                        final OneOfPayload payload =
                                readOneOfPayload(input, input.position() + messageLength, "QueuePushChange", walk);
                        if (payload != null && requireStateId(stateId, walk)) {
                            binaryState.pushQueue(stateId, payload.bytes());
                            applied = true;
                        }
                    }
                }
                case 66 -> { // queue_pop: field 8, message (empty)
                    skipMessage(input);
                    if (requireStateId(stateId, walk)) {
                        binaryState.popQueue(stateId);
                        applied = true;
                    }
                }
                default -> skipField(input, tag);
            }
        }
        if (applied && !walk.failed()) {
            walk.applied++;
        }
    }

    /// Walk a `MapUpdateChange` message (`key` field 1, `value` field 2) and apply it.
    ///
    /// @return `true` if the update was applied, `false` if a failure was recorded
    private static boolean applyMapUpdate(
            @NonNull final BinaryState binaryState,
            final int stateId,
            @NonNull final ReadableSequentialData input,
            final long endPosition,
            @NonNull final Walk walk)
            throws IOException {
        Bytes rawKey = null;
        Bytes rawValue = null;
        while (input.position() < endPosition && !walk.failed()) {
            final int tag = input.readVarInt(false);
            switch (tag) {
                case 10 -> { // key: field 1, message
                    final int messageLength = input.readVarInt(false);
                    if (messageLength > 0) {
                        final OneOfPayload keyPayload =
                                readOneOfPayload(input, input.position() + messageLength, "MapChangeKey", walk);
                        if (keyPayload != null && acceptKey(stateId, keyPayload, walk)) {
                            rawKey = keyPayload.bytes();
                        }
                    }
                }
                case 18 -> { // value: field 2, message
                    final int messageLength = input.readVarInt(false);
                    if (messageLength > 0) {
                        final OneOfPayload valuePayload =
                                readOneOfPayload(input, input.position() + messageLength, "MapChangeValue", walk);
                        if (valuePayload != null) {
                            rawValue = valuePayload.bytes();
                        }
                    }
                }
                default -> skipField(input, tag);
            }
        }
        if (walk.failed()) {
            return false;
        }
        if (rawKey == null || rawValue == null) {
            walk.fail(Status.MALFORMED_STATE_CHANGES, "MapChangeKey or MapChangeValue missing from MapUpdateChange");
            return false;
        }
        binaryState.updateKv(stateId, rawKey, rawValue);
        return true;
    }

    /// Walk a `MapDeleteChange` message (`key` field 1) and apply it.
    ///
    /// @return `true` if the delete was applied, `false` if a failure was recorded
    private static boolean applyMapDelete(
            @NonNull final BinaryState binaryState,
            final int stateId,
            @NonNull final ReadableSequentialData input,
            final long endPosition,
            @NonNull final Walk walk)
            throws IOException {
        Bytes rawKey = null;
        while (input.position() < endPosition && !walk.failed()) {
            final int tag = input.readVarInt(false);
            if (tag == 10) { // key: field 1, message
                final int messageLength = input.readVarInt(false);
                if (messageLength > 0) {
                    final OneOfPayload keyPayload =
                            readOneOfPayload(input, input.position() + messageLength, "MapChangeKey", walk);
                    if (keyPayload != null && acceptKey(stateId, keyPayload, walk)) {
                        rawKey = keyPayload.bytes();
                    }
                }
            } else {
                skipField(input, tag);
            }
        }
        if (walk.failed()) {
            return false;
        }
        if (rawKey == null) {
            walk.fail(Status.MALFORMED_STATE_CHANGES, "MapChangeKey missing from MapDeleteChange");
            return false;
        }
        binaryState.removeKv(stateId, rawKey);
        return true;
    }

    /// Read the first delimited (length-prefixed) field within a message — the oneof-selected
    /// payload — and the field number it was tagged with.
    ///
    /// @param input positioned at the start of the oneof wrapper message
    /// @param endPosition the byte offset the message ends at
    /// @param description the wrapper type name, for the failure detail if nothing is set
    /// @param walk records a failure if the payload is missing
    /// @return the payload and its field number, or `null` if a failure was recorded
    @Nullable
    private static OneOfPayload readOneOfPayload(
            @NonNull final ReadableSequentialData input,
            final long endPosition,
            @NonNull final String description,
            @NonNull final Walk walk)
            throws IOException {
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
                ProtoParserTools.skipField(input, wireType);
            }
        }
        if (payload == null) {
            walk.fail(Status.MALFORMED_STATE_CHANGES, description + " payload missing");
            return null;
        }
        return new OneOfPayload(fieldNumber, payload);
    }

    /// Accept a `MapChangeKey` payload, unless it is for `stateId == 9` tagged with the legacy,
    /// reserved field-2 (`TokenAssociation`) encoding instead of the current field-25
    /// (`AccountTokenAssociation`) encoding. This plugin does not remap it (unlike
    /// consensus-node's reference applier), so it records a distinct failure.
    ///
    /// @param stateId the state ID the key belongs to
    /// @param keyPayload the decoded `MapChangeKey` oneof payload
    /// @param walk records the failure if the legacy encoding is detected
    /// @return `true` if the key is usable
    private static boolean acceptKey(
            final int stateId, @NonNull final OneOfPayload keyPayload, @NonNull final Walk walk) {
        if (stateId == TOKEN_ASSOCIATION_STATE_ID && keyPayload.fieldNumber() == LEGACY_TOKEN_ASSOCIATION_FIELD) {
            walk.fail(
                    Status.LEGACY_TOKEN_ASSOCIATION,
                    "MapChangeKey for stateId=" + TOKEN_ASSOCIATION_STATE_ID + " carries the legacy, reserved "
                            + "field-2 TokenAssociation encoding, which this plugin does not remap");
            return false;
        }
        return true;
    }

    private static boolean requireStateId(final int stateId, @NonNull final Walk walk) {
        if (stateId < 0) {
            walk.fail(Status.MALFORMED_STATE_CHANGES, "StateChange missing state_id");
            return false;
        }
        return true;
    }

    private static void skipField(@NonNull final ReadableSequentialData input, final int tag) throws IOException {
        ProtoParserTools.skipField(input, ProtoConstants.get(tag & ProtoConstants.TAG_WIRE_TYPE_MASK));
    }

    private static void skipMessage(@NonNull final ReadableSequentialData input) {
        final int messageLength = input.readVarInt(false);
        input.skip(messageLength);
    }
}
