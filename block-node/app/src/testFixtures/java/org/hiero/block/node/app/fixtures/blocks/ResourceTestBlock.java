// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.fixtures.blocks;

import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.function.Predicate;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockUnparsed;

/// todo add documentation
public class ResourceTestBlock extends TestBlock {
    private Bytes blockRootHash;

    public ResourceTestBlock(final long number, final BlockUnparsed blockUnparsed, final Bytes blockRootHash) {
        super(number, blockUnparsed);
        this.blockRootHash = blockRootHash;
    }

    public Bytes blockRootHash() {
        return blockRootHash;
    }

    @Override
    public ResourceTestBlock append(final BlockItem toAppend) {
        final TestBlock superAppend = super.append(toAppend);
        return new ResourceTestBlock(superAppend.number(), superAppend.blockUnparsed(), blockRootHash());
    }

    @Override
    public ResourceTestBlock replace(final Predicate<BlockItemUnparsed> filter, final BlockItemUnparsed replacement) {
        final TestBlock superReplace = super.replace(filter, replacement);
        return new ResourceTestBlock(superReplace.number(), superReplace.blockUnparsed(), blockRootHash());
    }
}
