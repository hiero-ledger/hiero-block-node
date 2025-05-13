// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.generator.itemhandler;

import static java.util.Objects.requireNonNull;

import com.hedera.hapi.block.stream.output.protoc.BlockHeader;
import com.hedera.hapi.block.stream.protoc.BlockItem;
import com.hederahashgraph.api.proto.java.BlockHashAlgorithm;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Handler for block headers in the block stream.
 * Creates and manages block header items containing metadata about the block.
 */
public class BlockHeaderHandler extends AbstractBlockItemHandler {
    private final byte[] previousBlockHash;
    private final long currentBlockNumber;

    /**
     * Constructs a new BlockHeaderHandler.
     *
     * @param previousBlockHash Hash of the previous block in the chain
     * @param currentBlockNumber Number of the current block
     * @throws NullPointerException if previousBlockHash is null
     */
    public BlockHeaderHandler(@NonNull final byte[] previousBlockHash, final long currentBlockNumber) {
        this.previousBlockHash = requireNonNull(previousBlockHash);
        this.currentBlockNumber = currentBlockNumber;
    }

    @Override
    public BlockItem getItem() {
        if (blockItem == null) {
            blockItem =
                    BlockItem.newBuilder().setBlockHeader(createBlockHeader()).build();
        }
        return blockItem;
    }

    private BlockHeader createBlockHeader() {
        return BlockHeader.newBuilder()
                .setHapiProtoVersion(getSemanticVersion())
                .setSoftwareVersion(getSemanticVersion())
                .setHashAlgorithm(BlockHashAlgorithm.SHA2_384)
                .setBlockTimestamp(getTimestamp())
                .setNumber(currentBlockNumber)
                .build();
    }
}
