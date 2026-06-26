// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.fixtures.blocks;

import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.hapi.node.base.NodeAddressBook;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.function.Predicate;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockUnparsed;

/// todo add documentation
public final class ResourceTestWRBBlock extends ResourceTestBlock {
    private final NodeAddressBook nodeAddressBook;

    public ResourceTestWRBBlock(
            final long number,
            final BlockUnparsed blockUnparsed,
            final Bytes blockRootHash,
            final NodeAddressBook nodeAddressBook) {
        this.nodeAddressBook = nodeAddressBook;
        super(number, blockUnparsed, blockRootHash);
    }

    public NodeAddressBook nodeAddressBook() {
        return nodeAddressBook;
    }

    @Override
    public ResourceTestWRBBlock append(final BlockItem toAppend) {
        final ResourceTestBlock superAppend = super.append(toAppend);
        return new ResourceTestWRBBlock(
                superAppend.number(), superAppend.blockUnparsed(), blockRootHash(), nodeAddressBook);
    }

    @Override
    public ResourceTestWRBBlock replace(
            final Predicate<BlockItemUnparsed> filter, final BlockItemUnparsed replacement) {
        final ResourceTestBlock superReplace = super.replace(filter, replacement);
        return new ResourceTestWRBBlock(
                superReplace.number(), superReplace.blockUnparsed(), blockRootHash(), nodeAddressBook);
    }
}
