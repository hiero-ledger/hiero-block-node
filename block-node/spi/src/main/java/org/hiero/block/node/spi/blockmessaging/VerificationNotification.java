// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.blockmessaging;

import com.hedera.pbj.runtime.io.buffer.Bytes;

/**
 * Simple record for block verification notifications.
 *
 * @param success     true if the block was verified successfully, false otherwise
 * @param blockNumber the block number this notification is for
 * @param blockHash   the hash of the block, if the type is BLOCK_VERIFIED
 * @param block       the block, if the type is BLOCK_VERIFIED
 */
public record VerificationNotification(
        boolean success,
        long blockNumber,
        Bytes blockHash,
        org.hiero.block.internal.BlockUnparsed block,
        BlockSource source) {}
