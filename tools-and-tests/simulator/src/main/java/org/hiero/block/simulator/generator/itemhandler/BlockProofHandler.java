// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.generator.itemhandler;

import static java.util.Objects.requireNonNull;

import com.google.protobuf.ByteString;
import com.hedera.hapi.block.stream.protoc.BlockItem;
import com.hedera.hapi.block.stream.protoc.BlockProof;
import com.hedera.hapi.block.stream.protoc.TssSignedBlockProof;
import edu.umd.cs.findbugs.annotations.NonNull;
import org.hiero.block.common.hasher.HashingUtilities;

/**
 * Handler for block proofs in the block stream.
 * Creates and manages block proof items containing cryptographic proof of block validity.
 */
public class BlockProofHandler extends AbstractBlockItemHandler {
    private final byte[] currentBlockHash;
    private final long currentBlockNumber;

    /**
     * Constructs a new BlockProofHandler.
     *
     * @param currentBlockHash Hash of the current block
     * @param currentBlockNumber Number of the current block
     * @throws NullPointerException if previousBlockHash or currentBlockHash is null
     */
    public BlockProofHandler(@NonNull final byte[] currentBlockHash, final long currentBlockNumber) {
        this.currentBlockHash = requireNonNull(currentBlockHash);
        this.currentBlockNumber = currentBlockNumber;
    }

    @Override
    public BlockItem getItem() {
        if (blockItem == null) {
            blockItem = BlockItem.newBuilder().setBlockProof(createBlockProof()).build();
        }
        return blockItem;
    }

    private BlockProof createBlockProof() {
        return BlockProof.newBuilder()
                .setBlock(currentBlockNumber)
                .setSignedBlockProof(TssSignedBlockProof.newBuilder().setBlockSignature(produceSignature()))
                .build();
    }

    private ByteString produceSignature() {
        return ByteString.copyFrom(HashingUtilities.noThrowSha384HashOf(currentBlockHash));
    }
}
