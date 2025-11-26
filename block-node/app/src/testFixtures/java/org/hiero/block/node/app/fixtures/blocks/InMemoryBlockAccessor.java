// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.fixtures.blocks;

import com.github.luben.zstd.Zstd;
import com.hedera.hapi.block.stream.Block;
import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.List;
import org.hiero.block.node.spi.historicalblocks.BlockAccessor;

/**
 * A simple in-memory block accessor class. This class is intended to be used
 * for testing purposes only!
 */
public final class InMemoryBlockAccessor implements BlockAccessor {
    /** Internal in-memory reference of the accessible block */
    private final Block block;
    /** The block number of the accessible block */
    private final long blockNumber;
    /** simple flag to track calls to close */
    private boolean isClosed = false;

    /**
     * Constructor. Enforces preconditions on the input block items.
     *
     * @param blockItems of the block, if no block header is present, the
     * block number will be set to
     * {@value org.hiero.block.node.spi.BlockNodePlugin#UNKNOWN_BLOCK_NUMBER}
     */
    public InMemoryBlockAccessor(final List<BlockItem> blockItems) {
        this(blockItems, true);
    }

    /**
     * Constructor.
     *
     * @param blockItems of the block, if no block header is present, the
     * block number will be set to
     * {@value org.hiero.block.node.spi.BlockNodePlugin#UNKNOWN_BLOCK_NUMBER}
     * @param enforcePreconditions if true, will enforce preconditions on the
     * input block items
     * @throws IllegalArgumentException if preconditions are not met but
     * enforced
     */
    public InMemoryBlockAccessor(final List<BlockItem> blockItems, final boolean enforcePreconditions) {
        if (enforcePreconditions) {
            if (blockItems.isEmpty()) {
                throw new IllegalArgumentException("Block items list cannot be empty");
            }
            // Ensure the first item is a BlockHeader
            if (!blockItems.getFirst().hasBlockHeader()) {
                throw new IllegalArgumentException("First block item must be a BlockHeader");
            }
            // Ensure the last item is a BlockProof
            if (!blockItems.getLast().hasBlockProof()) {
                throw new IllegalArgumentException("Last block item must be a BlockProof");
            }
        }
        // Create a Block from the provided block items
        block = Block.newBuilder().items(blockItems).build();
        final BlockHeader header = block.items().getFirst().blockHeader();
        blockNumber = header != null ? header.number() : -1;
    }

    /**
     * {@inheritDoc}
     * <p>
     * This method returns the block number of the accessible block or
     * {@value org.hiero.block.node.spi.BlockNodePlugin#UNKNOWN_BLOCK_NUMBER}
     * if the block header is not present as the first item in the block.
     */
    @Override
    public long blockNumber() {
        return blockNumber;
    }

    @Override
    public Bytes blockBytes(final Format format) {
        return switch (format) {
            case JSON -> Block.JSON.toBytes(block);
            case PROTOBUF -> Block.PROTOBUF.toBytes(block);
            case ZSTD_PROTOBUF -> zstdCompressBytes(Block.PROTOBUF.toBytes(block));
        };
    }

    private Bytes zstdCompressBytes(final Bytes bytes) {
        return Bytes.wrap(Zstd.compress(bytes.toByteArray()));
    }

    @Override
    public void close() {
        isClosed = true;
    }

    @Override
    public boolean isClosed() {
        return isClosed;
    }
}
