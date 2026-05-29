// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.state.live;

import com.hedera.hapi.block.stream.output.BlockFooter;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.hapi.block.stream.input.RoundHeader;
import com.hedera.hapi.block.stream.output.MapChangeKey;
import com.hedera.hapi.block.stream.output.MapChangeValue;
import com.hedera.hapi.block.stream.output.QueuePushChange;
import com.hedera.hapi.block.stream.output.SingletonUpdateChange;
import com.hedera.hapi.block.stream.output.StateChange;
import com.hedera.hapi.block.stream.output.StateChanges;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.state.BinaryState;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.ArrayList;
import java.util.List;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockUnparsed;

/**
 * Walks the items of a verified block once, applying every `state_changes`
 * mutation to a {@link BinaryState} owned by the state-management plugin and
 * returning the per-block metadata ({@code blockNumber}, {@code roundNumber},
 * applied-change count) on the way out.
 *
 * <h2>Storage encoding</h2>
 * Each variant of {@link StateChange} is translated to its corresponding
 * {@link BinaryState} write call. The bytes handed off are the PBJ-encoded
 * carrier message ({@link MapChangeKey}, {@link MapChangeValue},
 * {@link SingletonUpdateChange}, {@link QueuePushChange}) so the wire form is
 * consistent on read-back through the `getBinary*` RPCs.
 *
 * <h2>Why no separate `inspectBlock`</h2>
 * The plugin needs `BlockFooter.startOfBlockStateRootHash` BEFORE applying so
 * it can validate the incoming block lines up with our live state. We don't
 * re-walk the whole block for that — instead {@link #extractStartOfBlockStateRootHash}
 * scans the items list from the end (the footer is always the second-to-last
 * item in the stream, so this is O(1) in practice) and pulls the hash without
 * touching state.
 */
final class StateChangeApplier {

    private static final System.Logger LOGGER = System.getLogger(StateChangeApplier.class.getName());

    /** Outcome of a single block apply. */
    record ApplyResult(long blockNumber, long roundNumber, int appliedChanges) {}

    /**
     * Apply every `state_changes` item in `block` to `binaryState` and return
     * the metadata extracted along the way.
     *
     * @throws IllegalStateException if a `state_changes` item is malformed.
     */
    @NonNull
    ApplyResult applyBlock(@NonNull final BinaryState binaryState, @NonNull final BlockUnparsed block) {
        long blockNumber = -1L;
        long roundNumber = -1L;
        int applied = 0;

        for (final var item : block.blockItems()) {
            if (item.hasBlockHeader() && blockNumber < 0L) {
                try {
                    blockNumber =
                            BlockHeader.PROTOBUF.parse(item.blockHeaderOrThrow()).number();
                } catch (final Exception ignored) {
                    // Leaves blockNumber = -1; caller treats that as a failed apply.
                }
            } else if (item.hasRoundHeader()) {
                try {
                    roundNumber = RoundHeader.PROTOBUF
                            .parse(item.roundHeaderOrThrow())
                            .roundNumber();
                } catch (final Exception ignored) {
                    // Keep previous round number on parse failure.
                }
            } else if (item.hasStateChanges()) {
                try {
                    final StateChanges changes = StateChanges.PROTOBUF.parse(item.stateChangesOrThrow());
                    applied += applyChanges(binaryState, changes.stateChanges());
                } catch (final Exception e) {
                    throw new IllegalStateException("Failed to parse state_changes item", e);
                }
            }
            // BlockFooter is read by extractStartOfBlockStateRootHash before this
            // method runs; we don't need to re-parse it here.
        }
        return new ApplyResult(blockNumber, roundNumber, applied);
    }

    /**
     * Pull the `BlockFooter.startOfBlockStateRootHash` from a block without
     * walking the whole item list. The footer is always near the end of the
     * stream (BlockProof is last, BlockFooter is immediately before it), so a
     * reverse scan is O(1) in practice.
     *
     * @return the hash, or `null` if the block has no parseable footer.
     */
    @Nullable
    static Bytes extractStartOfBlockStateRootHash(@NonNull final BlockUnparsed block) {
        final List<BlockItemUnparsed> items = block.blockItems();
        for (int i = items.size() - 1; i >= 0; i--) {
            final BlockItemUnparsed item = items.get(i);
            if (item.hasBlockFooter()) {
                try {
                    final BlockFooter footer = BlockFooter.PROTOBUF.parse(item.blockFooterOrThrow());
                    return footer.startOfBlockStateRootHash() == null ? Bytes.EMPTY : footer.startOfBlockStateRootHash();
                } catch (final Exception ignored) {
                    return null;
                }
            }
        }
        return null;
    }

    private static int applyChanges(
            @NonNull final BinaryState binaryState, @NonNull final List<StateChange> changes) {
        int count = 0;
        for (final StateChange change : changes) {
            final int stateId = change.stateId();
            switch (change.changeOperation().kind()) {
                case STATE_ADD, STATE_REMOVE, UNSET -> {
                    // Schema-level events — out of scope for live-state v1. Log at TRACE
                    // so a stream that suddenly starts carrying these is at least visible
                    // when debug logging is enabled.
                    LOGGER.log(
                            System.Logger.Level.TRACE,
                            "Skipping unsupported schema change (kind={0}, stateId={1})",
                            change.changeOperation().kind(),
                            stateId);
                }
                case SINGLETON_UPDATE -> {
                    final SingletonUpdateChange su = change.singletonUpdate();
                    if (su != null) {
                        binaryState.updateSingleton(stateId, SingletonUpdateChange.PROTOBUF.toBytes(su));
                        count++;
                    }
                }
                case MAP_UPDATE -> {
                    final var mu = change.mapUpdate();
                    if (mu != null && mu.hasKey() && mu.hasValue()) {
                        final Bytes key = MapChangeKey.PROTOBUF.toBytes(mu.keyOrThrow());
                        final Bytes value = MapChangeValue.PROTOBUF.toBytes(mu.valueOrThrow());
                        binaryState.updateKv(stateId, key, value);
                        count++;
                    }
                }
                case MAP_DELETE -> {
                    final var md = change.mapDelete();
                    if (md != null && md.hasKey()) {
                        final Bytes key = MapChangeKey.PROTOBUF.toBytes(md.keyOrThrow());
                        binaryState.removeKv(stateId, key);
                        count++;
                    }
                }
                case QUEUE_PUSH -> {
                    final QueuePushChange qp = change.queuePush();
                    if (qp != null) {
                        binaryState.pushQueue(stateId, QueuePushChange.PROTOBUF.toBytes(qp));
                        count++;
                    }
                }
                case QUEUE_POP -> {
                    binaryState.popQueue(stateId);
                    count++;
                }
            }
        }
        return count;
    }

    /** Convenience used by tests: build a {@code state_changes} block item from a list. */
    @NonNull
    static Bytes encodeStateChanges(@NonNull final List<StateChange> changes) {
        final StateChanges sc = StateChanges.newBuilder()
                .stateChanges(new ArrayList<>(changes))
                .build();
        return StateChanges.PROTOBUF.toBytes(sc);
    }
}
