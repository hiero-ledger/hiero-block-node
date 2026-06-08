// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.fixtures.blocks;

import com.hedera.pbj.runtime.io.buffer.Bytes;
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
}
