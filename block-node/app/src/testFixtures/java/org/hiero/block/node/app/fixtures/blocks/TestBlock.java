// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.fixtures.blocks;

import com.hedera.hapi.block.stream.Block;
import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.List;
import java.util.Objects;
import org.hiero.block.internal.BlockItemSetUnparsed;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.historicalblocks.BlockAccessor;

/**
 * A simple wrapper for blocks, with convenience methods for testing.
 */
public final class TestBlock {
    private final long number;
    private final int blockSize;
    private final Block block;
    private final BlockUnparsed blockUnparsed;

    public TestBlock(final long number, final Block block) {
        this.number = number;
        this.block = Objects.requireNonNull(block);
        final List<BlockItemUnparsed> converted = TestBlockBuilder.convertToUnparsedItems(block.items());
        this.blockUnparsed = BlockUnparsed.newBuilder().blockItems(converted).build();
        this.blockSize = this.block.items().size();
    }

    public TestBlock(final long number, final BlockUnparsed blockUnparsed) {
        this.number = number;
        this.blockUnparsed = blockUnparsed;
        final List<BlockItem> converted = TestBlockBuilder.convertToItems(blockUnparsed.blockItems());
        this.block = Block.newBuilder().items(converted).build();
        this.blockSize = this.block.items().size();
    }

    public long number() {
        return number;
    }

    public int blockSize() {
        return blockSize;
    }

    public Block block() {
        return block;
    }

    public BlockUnparsed blockUnparsed() {
        return blockUnparsed;
    }

    public Bytes bytes() {
        return Block.PROTOBUF.toBytes(block);
    }

    public BlockItemSetUnparsed asItemSetUnparsed() {
        return BlockItemSetUnparsed.newBuilder()
                .blockItems(blockUnparsed.blockItems())
                .build();
    }

    public BlockItems asBlockItems() {
        return new BlockItems(blockUnparsed.blockItems(), number, true, true);
    }

    public BlockItem[] asBlockItemArray() {
        return block.items().toArray(new BlockItem[0]);
    }

    public BlockItemUnparsed[] asBlockItemUnparsedArray() {
        return blockUnparsed.blockItems().toArray(new BlockItemUnparsed[0]);
    }

    public BlockAccessor asBlockAccessor() {
        return new MinimalBlockAccessor(number, block);
    }
}
