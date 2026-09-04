// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.blockmessaging;

import java.util.List;
import org.hiero.block.api.BlockRange;

/**
 * Notification sent when the set of blocks available for client retrieval changes.
 *
 * @param availableBlocks the current available block ranges
 */
public record AvailableBlocksNotification(List<BlockRange> availableBlocks) {}
