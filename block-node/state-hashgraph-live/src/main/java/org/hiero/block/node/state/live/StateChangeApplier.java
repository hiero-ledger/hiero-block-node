// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.state.live;

import edu.umd.cs.findbugs.annotations.NonNull;
import org.hiero.block.internal.BlockUnparsed;

/**
 * Walks block items and feeds {@code state_changes} into a {@link LiveState}.
 *
 * <p>v1 / STORY-4 only reads the block header for the block number and tracks the
 * last {@code RoundHeader.roundNumber()} seen. STORY-5 ports
 * {@code BinaryStateChangeApplier} from
 * {@code hedera-state-validator/.../BlockStreamRecoveryWorkflow.java} to mutate
 * the {@link LiveState} maps from each state-change item.
 */
final class StateChangeApplier {

    /** Outcome of a single block apply. */
    record ApplyResult(long blockNumber, long roundNumber) {}

    @NonNull
    ApplyResult applyBlock(@NonNull final LiveState state, @NonNull final BlockUnparsed block) {
        long blockNumber = -1L;
        long roundNumber = -1L;

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
            }
            // STORY-5: handle item.hasStateChanges() — iterate state_changes and apply via state.update*.
        }
        return new ApplyResult(blockNumber, roundNumber);
    }
}
