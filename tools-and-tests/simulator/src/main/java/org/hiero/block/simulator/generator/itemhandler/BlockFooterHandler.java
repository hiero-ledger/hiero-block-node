// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.generator.itemhandler;

import static java.util.Objects.requireNonNull;
import static org.hiero.block.simulator.Constants.BLOCK_HASH_ALGORITHM;

import com.google.protobuf.ByteString;
import com.hedera.hapi.block.stream.output.protoc.BlockFooter;
import com.hedera.hapi.block.stream.protoc.BlockItem;

/**
 * Handler for block footers in the block stream.
 * Creates and manages block footer items carrying the previous block root hash, the start of block state root
 * hash and the root of the all previous block hashes tree.
 */
public class BlockFooterHandler extends AbstractBlockItemHandler {
    private final byte[] previousBlockHash;
    private final byte[] previousStateRootHash;
    private final byte[] hashOfAllBlockHashesTree;

    /**
     * Constructs a new BlockFooterHandler.
     *
     * @param previousBlockHash Hash of the previous block
     * @param hashOfAllBlockHashesTree Root hash of the all previous block hashes tree
     * @throws NullPointerException if previousBlockHash or hashOfAllBlockHashesTree is null
     */
    public BlockFooterHandler(final byte[] previousBlockHash, final byte[] hashOfAllBlockHashesTree) {
        this.previousBlockHash = requireNonNull(previousBlockHash);
        // the crafted blocks carry no state, so the state root placeholder is one zero filled digest
        this.previousStateRootHash = new byte[BLOCK_HASH_ALGORITHM.hashSize()];
        this.hashOfAllBlockHashesTree = requireNonNull(hashOfAllBlockHashesTree);
    }

    @Override
    public BlockItem getItem() {
        if (blockItem == null) {
            blockItem =
                    BlockItem.newBuilder().setBlockFooter(createBlockFooter()).build();
        }
        return blockItem;
    }

    private BlockFooter createBlockFooter() {
        return BlockFooter.newBuilder()
                .setPreviousBlockRootHash(ByteString.copyFrom(previousBlockHash))
                .setStartOfBlockStateRootHash(ByteString.copyFrom(previousStateRootHash))
                .setRootHashOfAllBlockHashesTree(ByteString.copyFrom(hashOfAllBlockHashesTree))
                .build();
    }
}
