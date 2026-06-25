// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.fixtures.blocks;

import com.hedera.hapi.node.base.NodeAddressBook;
import com.hedera.pbj.runtime.io.buffer.Bytes;
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
}
