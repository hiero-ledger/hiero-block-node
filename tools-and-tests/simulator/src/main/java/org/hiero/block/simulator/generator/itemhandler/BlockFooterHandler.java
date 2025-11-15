// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.generator.itemhandler;

import static java.util.Objects.requireNonNull;

import com.google.protobuf.ByteString;
import com.hedera.hapi.block.stream.output.protoc.BlockFooter;
import com.hedera.hapi.block.stream.protoc.BlockItem;
import edu.umd.cs.findbugs.annotations.NonNull;
import org.hiero.block.common.hasher.StreamingTreeHasher;

/**
 * Handler for block proofs in the block stream.
 * Creates and manages block proof items containing cryptographic proof of block validity.
 */
public class BlockFooterHandler extends AbstractBlockItemHandler {
    private final byte[] previousBlockHash;
    private final byte[] previousStateRootHash;
    private final byte[] hashOfAllBlockHashesTree;

    /**
     * Constructs a new BlockProofHandler.
     *
     * @param previousBlockHash Hash of the previous block
     * @throws NullPointerException if previousBlockHash or currentBlockHash is null
     */
    public BlockFooterHandler(@NonNull final byte[] previousBlockHash) {
        this.previousBlockHash = requireNonNull(previousBlockHash);
        this.previousStateRootHash = new byte[StreamingTreeHasher.HASH_LENGTH];
        this.hashOfAllBlockHashesTree = new byte[StreamingTreeHasher.HASH_LENGTH];
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
