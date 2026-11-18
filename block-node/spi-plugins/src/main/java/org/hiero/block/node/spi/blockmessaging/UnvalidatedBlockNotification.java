// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.blockmessaging;

import org.hiero.block.internal.BlockUnparsed;

/**
 * A whole block that is not verified yet and does not come from the live publisher, for example a block fetched by
 * backfill or downloaded from cloud storage. Sent on the unvalidated block ring.
 *
 * @param blockNumber the number of the block
 * @param block the whole block
 * @param source where the block came from
 */
public record UnvalidatedBlockNotification(long blockNumber, BlockUnparsed block, BlockSource source) {}
