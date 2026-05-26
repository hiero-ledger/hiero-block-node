// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.state.live;

import com.hedera.hapi.block.stream.output.MapChangeKey;
import com.hedera.hapi.block.stream.output.MapChangeValue;
import com.hedera.hapi.block.stream.output.QueuePushChange;
import com.hedera.hapi.block.stream.output.SingletonUpdateChange;
import com.hedera.hapi.block.stream.output.StateChange;
import com.hedera.hapi.block.stream.output.StateChanges;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.state.BinaryState;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.ArrayList;
import java.util.List;
import org.hiero.block.internal.BlockUnparsed;

/**
 * Walks the items of a verified block, extracts metadata, and applies every
 * {@code state_changes} mutation to a {@link BinaryState} owned by
 * {@link LiveStatePlugin}.
 *
 * <h2>Storage encoding</h2>
 * Each variant of {@link StateChange} is translated to its corresponding
 * {@link BinaryState} write call. The bytes handed off are the PBJ-encoded
 * carrier message ({@link MapChangeKey}, {@link MapChangeValue},
 * {@link SingletonUpdateChange}, {@link QueuePushChange}) so the wire form is
 * consistent on read-back through the {@code getBinary*} RPCs.
 */
final class StateChangeApplier {

    /** Outcome of a single block apply. */
    record ApplyResult(long blockNumber, long roundNumber, int appliedChanges, @NonNull Bytes startOfBlockStateRootHash) {}

    @NonNull
    ApplyResult applyBlock(@NonNull final BinaryState binaryState, @NonNull final BlockUnparsed block) {
        return applyBlock(binaryState, block, true);
    }

    /**
     * Variant used by the plugin's pre-apply validation path: walk the block items to extract
     * metadata (block number, round number, footer hash) without mutating the state.
     */
    @NonNull
    ApplyResult inspectBlock(@NonNull final BlockUnparsed block) {
        return applyBlock(null, block, false);
    }

    @NonNull
    private ApplyResult applyBlock(
            final BinaryState binaryState, @NonNull final BlockUnparsed block, final boolean mutate) {
        long blockNumber = -1L;
        long roundNumber = -1L;
        int applied = 0;
        Bytes startOfBlockStateRootHash = Bytes.EMPTY;

        for (final var item : block.blockItems()) {
            if (item.hasBlockHeader() && blockNumber < 0L) {
                try {
                    final var header =
                            com.hedera.hapi.block.stream.output.BlockHeader.PROTOBUF.parse(item.blockHeaderOrThrow());
                    blockNumber = header.number();
                } catch (final Exception ignored) {
                    // Leaves blockNumber = -1; caller treats that as a failed apply.
                }
            } else if (item.hasRoundHeader()) {
                try {
                    final var roundHeader =
                            com.hedera.hapi.block.stream.input.RoundHeader.PROTOBUF.parse(item.roundHeaderOrThrow());
                    roundNumber = roundHeader.roundNumber();
                } catch (final Exception ignored) {
                    // Keep previous round number on parse failure.
                }
            } else if (item.hasStateChanges() && mutate) {
                try {
                    final StateChanges changes =
                            StateChanges.PROTOBUF.parse(item.stateChangesOrThrow());
                    applied += applyChanges(binaryState, changes.stateChanges());
                } catch (final Exception e) {
                    throw new IllegalStateException("Failed to parse state_changes item", e);
                }
            } else if (item.hasBlockFooter()) {
                try {
                    final var footer = com.hedera.hapi.block.stream.output.BlockFooter.PROTOBUF.parse(
                            item.blockFooterOrThrow());
                    if (footer.startOfBlockStateRootHash() != null) {
                        startOfBlockStateRootHash = footer.startOfBlockStateRootHash();
                    }
                } catch (final Exception ignored) {
                    // Leaves startOfBlockStateRootHash = empty; the plugin treats that as a
                    // validation failure unless we are at genesis.
                }
            }
        }
        return new ApplyResult(blockNumber, roundNumber, applied, startOfBlockStateRootHash);
    }

    private static int applyChanges(
            @NonNull final BinaryState binaryState, @NonNull final List<StateChange> changes) {
        int count = 0;
        for (final StateChange change : changes) {
            final int stateId = change.stateId();
            switch (change.changeOperation().kind()) {
                case STATE_ADD, STATE_REMOVE, UNSET -> {
                    // Schema-level events — out of scope for live-state v1.
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
