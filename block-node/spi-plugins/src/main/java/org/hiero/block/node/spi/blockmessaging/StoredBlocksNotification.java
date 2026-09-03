// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.blockmessaging;

import java.util.List;
import org.hiero.block.api.BlockRange;

/**
 * Notification sent when the set of stored blocks changes. The list represents the merged view of
 * cloud-archived stored blocks and locally available blocks.
 *
 * @param storedBlocks the current merged stored block ranges
 */
public record StoredBlocksNotification(List<BlockRange> storedBlocks) {}
