// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.generator.itemhandler;

import static java.util.Objects.requireNonNull;

import com.google.protobuf.ByteString;
import com.hedera.hapi.block.stream.protoc.BlockItem;
import com.hedera.hapi.block.stream.protoc.BlockProof;
import com.hedera.hapi.block.stream.protoc.TssSignedBlockProof;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import org.hiero.block.signing.TssBlockSigner;

/**
 * Handler for block proofs in the block stream.
 * Creates a real, verifiable TSS block signature over the block root hash using a {@link TssBlockSigner}.
 */
public class BlockProofHandler extends AbstractBlockItemHandler {
    private final byte[] currentBlockHash;
    private final long currentBlockNumber;
    private final TssBlockSigner signer;

    /**
     * Constructs a new BlockProofHandler.
     *
     * @param currentBlockHash Hash of the current block
     * @param currentBlockNumber Number of the current block
     * @param signer the TSS signer producing the block signature
     * @throws NullPointerException if currentBlockHash or signer is null
     */
    public BlockProofHandler(
            @NonNull final byte[] currentBlockHash,
            final long currentBlockNumber,
            @NonNull final TssBlockSigner signer) {
        this.currentBlockHash = requireNonNull(currentBlockHash);
        this.currentBlockNumber = currentBlockNumber;
        this.signer = requireNonNull(signer);
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
        final com.hedera.hapi.block.stream.BlockProof proof =
                signer.signBlockProof(currentBlockNumber, Bytes.wrap(currentBlockHash));
        return ByteString.copyFrom(proof.signedBlockProof().blockSignature().toByteArray());
    }
}
