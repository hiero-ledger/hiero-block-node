package org.hiero.block.node.state.live;

import java.io.Serializable;

/**
 * Metadata about the current state, this is what is persisted to disk for LiveStatePlugin.
 *
 * @param lastAppliedBlockNumber the last block number that was applied to state. Will be -1 if no blocks have been
 *                               applied yet.
 * @param nextBlockStateRootHash the root hash of the state at the start of the next block state. For comparing when we
 *                               get data for the next block(lastAppliedBlockNumber+1). It will be null if no blocks
 *                               have been applied yet.
 */
public record StateMetadata(
    long lastAppliedBlockNumber,
    byte[] nextBlockStateRootHash
) implements Serializable {
}
