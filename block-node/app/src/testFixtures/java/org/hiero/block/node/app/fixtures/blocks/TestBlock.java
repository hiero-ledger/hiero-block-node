// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.fixtures.blocks;

import com.hedera.hapi.block.stream.Block;
import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.hapi.block.stream.BlockProof;
import com.hedera.hapi.block.stream.output.BlockFooter;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.hapi.node.base.SemanticVersion;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;
import org.hiero.block.internal.BlockItemSetUnparsed;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.internal.PublishStreamRequestUnparsed;
import org.hiero.block.node.spi.blockmessaging.BackfilledBlockNotification;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.historicalblocks.BlockAccessor;

/**
 * A simple wrapper for blocks, with convenience methods for testing.
 */
public class TestBlock {
    /// so size/8 bounds the deepest a non-degenerate message can nest.
    public static final int MAX_BLOCK_MESSAGE_DEPTH = Integer.MAX_VALUE / 8;
    private final long number;
    private final int blockSize;
    private final Block block;
    private final BlockUnparsed blockUnparsed;
    private final BlockHeader header;
    private final BlockFooter footer;
    private final List<BlockProof> proofs;
    private final SemanticVersion hapiVersion;

    public TestBlock(final long number, final Block block) {
        this(number, block, TestBlockBuilder.convertToUnparsed(block));
    }

    public TestBlock(final long number, final BlockUnparsed blockUnparsed) {
        this(number, TestBlockBuilder.convertToBlock(blockUnparsed), blockUnparsed);
    }

    private TestBlock(final long number, final Block block, final BlockUnparsed blockUnparsed) {
        this.block = Objects.requireNonNull(block);
        this.blockUnparsed = Objects.requireNonNull(blockUnparsed);
        this.blockSize = this.block.items().size();
        this.header = block.items().getFirst().blockHeader();
        this.footer = block.items().stream()
                .filter(BlockItem::hasBlockFooter)
                .findFirst()
                .map(BlockItem::blockFooter)
                .orElse(null);
        this.proofs = block.items().stream()
                .filter(BlockItem::hasBlockProof)
                .map(BlockItem::blockProof)
                .toList();
        if (header != null) {
            this.hapiVersion = header.hapiProtoVersion();
            this.number = header.number();
        } else {
            this.hapiVersion = null;
            this.number = number;
        }
    }

    ///  Returns the block number from the header, if it is present, else what was passed to the constructor.
    public long number() {
        return number;
    }

    public int blockSize() {
        return blockSize;
    }

    public Block block() {
        return block;
    }

    public BlockItem getHeader() {
        return block.items().getFirst();
    }

    public BlockItemUnparsed getHeaderUnparsed() {
        return blockUnparsed.blockItems().getFirst();
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

    public PublishStreamRequestUnparsed asPublishStreamRequestUnparsed() {
        return PublishStreamRequestUnparsed.newBuilder()
                .blockItems(asItemSetUnparsed())
                .build();
    }

    public BackfilledBlockNotification asBackfilledNotification() {
        return new BackfilledBlockNotification(number, blockUnparsed);
    }

    public BlockHeader header() {
        return header;
    }

    public BlockFooter footer() {
        return footer;
    }

    public List<BlockProof> proofs() {
        return proofs;
    }

    public SemanticVersion hapiVersion() {
        return hapiVersion;
    }

    public List<BlockItem> asBlockItemFiltered(final Predicate<BlockItem> filter) {
        return block.items().stream().filter(Objects.requireNonNull(filter)).toList();
    }

    public List<BlockItemUnparsed> asBlockItemUnparsedFiltered(final Predicate<BlockItemUnparsed> filter) {
        return blockUnparsed.blockItems().stream()
                .filter(Objects.requireNonNull(filter))
                .toList();
    }

    public TestBlock replace(final Predicate<BlockItemUnparsed> filter, final BlockItemUnparsed replacement) {
        final List<BlockItemUnparsed> items = new ArrayList<>();
        for (final BlockItemUnparsed currentItem : blockUnparsed.blockItems()) {
            if (filter.test(currentItem)) {
                items.add(replacement);
            } else {
                items.add(currentItem);
            }
        }
        return new TestBlock(
                number, BlockUnparsed.newBuilder().blockItems(items).build());
    }

    public TestBlock append(final BlockItem toAppend) {
        final List<BlockItem> items = new ArrayList<>(block.items());
        items.add(toAppend);
        return new TestBlock(number, Block.newBuilder().items(items).build());
    }
}
