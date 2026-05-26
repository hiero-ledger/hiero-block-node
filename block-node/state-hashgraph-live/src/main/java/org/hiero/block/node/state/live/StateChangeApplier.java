// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.state.live;

import com.hedera.hapi.block.stream.output.MapChangeKey;
import com.hedera.hapi.block.stream.output.MapChangeValue;
import com.hedera.hapi.block.stream.output.QueuePushChange;
import com.hedera.hapi.block.stream.output.SingletonUpdateChange;
import com.hedera.hapi.block.stream.output.StateChange;
import com.hedera.hapi.block.stream.output.StateChanges;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.ArrayList;
import java.util.List;
import org.hiero.block.internal.BlockUnparsed;

/**
 * Walks the items of a verified block, extracts metadata, and applies every
 * {@code state_changes} mutation to a {@link LiveState}.
 *
 * <h2>Storage encoding</h2>
 * To stay independent of {@code swirlds-state-impl}, this applier owns its own
 * canonical encoding of state contents. The convention is straightforward:
 *
 * <ul>
 *   <li><b>Singleton</b>: the value bytes are
 *       {@code SingletonUpdateChange.PROTOBUF.toBytes(change)} — i.e. the full
 *       PBJ-encoded oneof carrier. Clients that issue {@code getBinarySingleton}
 *       receive these exact bytes and parse them back via
 *       {@link SingletonUpdateChange#PROTOBUF}.</li>
 *   <li><b>KV</b>: key bytes are {@code MapChangeKey.PROTOBUF.toBytes(key)};
 *       value bytes are {@code MapChangeValue.PROTOBUF.toBytes(value)}.</li>
 *   <li><b>Queue</b>: element bytes are
 *       {@code QueuePushChange.PROTOBUF.toBytes(push)}.</li>
 * </ul>
 *
 * <p>Using the wrapper-level encoding sidesteps having to switch on every
 * {@code oneof} variant — the same form survives a snapshot reload and hashes
 * deterministically.
 */
final class StateChangeApplier {

    /** Outcome of a single block apply. */
    record ApplyResult(long blockNumber, long roundNumber, int appliedChanges) {}

    @NonNull
    ApplyResult applyBlock(@NonNull final LiveState state, @NonNull final BlockUnparsed block) {
        long blockNumber = -1L;
        long roundNumber = -1L;
        int applied = 0;

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
            } else if (item.hasStateChanges()) {
                try {
                    final StateChanges changes =
                            StateChanges.PROTOBUF.parse(item.stateChangesOrThrow());
                    applied += applyChanges(state, changes.stateChanges());
                } catch (final Exception e) {
                    // A malformed state_changes item aborts apply of this block — bubble up so
                    // the plugin can refuse to advance metadata.
                    throw new IllegalStateException("Failed to parse state_changes item", e);
                }
            }
        }
        return new ApplyResult(blockNumber, roundNumber, applied);
    }

    private static int applyChanges(@NonNull final LiveState state, @NonNull final List<StateChange> changes) {
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
                        state.updateSingleton(stateId, SingletonUpdateChange.PROTOBUF.toBytes(su));
                        count++;
                    }
                }
                case MAP_UPDATE -> {
                    final var mu = change.mapUpdate();
                    if (mu != null && mu.hasKey() && mu.hasValue()) {
                        final Bytes key = MapChangeKey.PROTOBUF.toBytes(mu.keyOrThrow());
                        final Bytes value = MapChangeValue.PROTOBUF.toBytes(mu.valueOrThrow());
                        state.updateKv(stateId, key, value);
                        count++;
                    }
                }
                case MAP_DELETE -> {
                    final var md = change.mapDelete();
                    if (md != null && md.hasKey()) {
                        final Bytes key = MapChangeKey.PROTOBUF.toBytes(md.keyOrThrow());
                        state.removeKv(stateId, key);
                        count++;
                    }
                }
                case QUEUE_PUSH -> {
                    final QueuePushChange qp = change.queuePush();
                    if (qp != null) {
                        state.pushQueue(stateId, QueuePushChange.PROTOBUF.toBytes(qp));
                        count++;
                    }
                }
                case QUEUE_POP -> {
                    state.popQueue(stateId);
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
