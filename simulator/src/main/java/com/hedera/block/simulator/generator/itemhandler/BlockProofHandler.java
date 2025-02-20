// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.simulator.generator.itemhandler;

import static java.util.Objects.requireNonNull;

import com.google.protobuf.ByteString;
import com.hedera.block.common.hasher.HashingUtilities;
import com.hedera.block.common.hasher.StreamingTreeHasher;
import com.hedera.hapi.block.stream.protoc.BlockItem;
import com.hedera.hapi.block.stream.protoc.BlockProof;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Handler for block proofs in the block stream.
 * Creates and manages block proof items containing cryptographic proof of block validity.
 */
public class BlockProofHandler extends AbstractBlockItemHandler {
    private final byte[] previousBlockHash;
    private final byte[] currentBlockHash;
    private final byte[] previousStateRootHash;
    private final long currentBlockNumber;

    /**
     * Constructs a new BlockProofHandler.
     *
     * @param previousBlockHash Hash of the previous block
     * @param currentBlockHash Hash of the current block
     * @param currentBlockNumber Number of the current block
     * @throws NullPointerException if previousBlockHash or currentBlockHash is null
     */
    public BlockProofHandler(
            @NonNull final byte[] previousBlockHash,
            @NonNull final byte[] currentBlockHash,
            final long currentBlockNumber) {
        this.previousBlockHash = requireNonNull(previousBlockHash);
        this.currentBlockHash = requireNonNull(currentBlockHash);
        this.currentBlockNumber = currentBlockNumber;
        this.previousStateRootHash = new byte[StreamingTreeHasher.HASH_LENGTH];
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
                .setPreviousBlockRootHash(ByteString.copyFrom(previousBlockHash))
                .setStartOfBlockStateRootHash(ByteString.copyFrom(previousStateRootHash))
                .setBlockSignature(produceSignature())
                .build();
    }

    private ByteString produceSignature() {
        return ByteString.copyFrom(HashingUtilities.noThrowSha384HashOf(currentBlockHash));
    }
}
