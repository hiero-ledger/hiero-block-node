// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.blockmessaging;

import org.hiero.block.internal.BlockUnparsed;

public record BackfilledBlockNotification(long blockNumber, BlockUnparsed block, BlockSource source) {
    public BackfilledBlockNotification(final long blockNumber, final BlockUnparsed block) {
        this(blockNumber, block, BlockSource.BACKFILL);
    }
}
