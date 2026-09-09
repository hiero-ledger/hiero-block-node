// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.blockmessaging;

import org.hiero.block.api.TssData;

/**
 * Notification sent when the node's TSS data is updated.
 *
 * @param tssData the updated TSS data
 */
public record TssDataNotification(TssData tssData) {}
