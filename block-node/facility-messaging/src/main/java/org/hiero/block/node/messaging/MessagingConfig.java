// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.messaging;

import com.swirlds.config.api.ConfigData;
import com.swirlds.config.api.ConfigProperty;
import org.hiero.block.common.utils.Preconditions;
import org.hiero.block.node.base.Loggable;

/**
 * Configuration for the messaging system.
 *
 * @param blockItemQueueSize The maximum number of messages that can be queued
 *     for processing. This is used to limit the number of block item batches
 *     that can be in messing queue at any time. Each batch is expected to be
 *     approximately 100 block items. The bigger the queue, the more memory is
 *     used but also the more flexibility of subscribers to be out of sync with
 *     each other. The queue size must be a power of two.
 * @param blockNotificationQueueSize The maximum number of block notifications
 *     that can be queued for processing. This is used to limit the number of
 *     block notifications that can be in the queue at any time. Notifications
 *     are small and never carry a whole block. There should be no reason to
 *     have a big queue. The queue size must be a power of two.
 * @param unvalidatedBlockQueueSize The maximum number of unvalidated blocks
 *     that can be queued for processing. Unvalidated blocks are whole blocks
 *     that are not verified yet and do not come from the live publisher, for
 *     now the blocks fetched by backfill. Each queued entry may keep a whole
 *     block in memory. When the queue is full, the senders of unvalidated
 *     blocks wait until there is room; the live publisher never waits on this
 *     queue. The queue size must be a power of two.
 * @param verifiedBlockQueueSize The maximum number of verified blocks that can
 *     be queued for processing. Verified blocks are the results of block
 *     verification, and a successful result carries the whole block, so each
 *     queued entry may keep a whole block in memory. When the queue is full,
 *     verification waits until there is room, which in turn slows down the
 *     block item and unvalidated block senders. The default matches the size
 *     of the block notification queue that carried these results before. The
 *     queue size must be a power of two.
 */
@ConfigData("messaging")
public record MessagingConfig(
        @Loggable @ConfigProperty(defaultValue = "512") int blockItemQueueSize,
        @Loggable @ConfigProperty(defaultValue = "32") int blockNotificationQueueSize,
        @Loggable @ConfigProperty(defaultValue = "32") int unvalidatedBlockQueueSize,
        @Loggable @ConfigProperty(defaultValue = "32") int verifiedBlockQueueSize) {
    /**
     * Constructor.
     */
    public MessagingConfig {
        Preconditions.requirePositive(blockItemQueueSize);
        Preconditions.requirePowerOfTwo(blockItemQueueSize);
        Preconditions.requirePositive(blockNotificationQueueSize);
        Preconditions.requirePowerOfTwo(blockNotificationQueueSize);
        Preconditions.requirePositive(unvalidatedBlockQueueSize);
        Preconditions.requirePowerOfTwo(unvalidatedBlockQueueSize);
        Preconditions.requirePositive(verifiedBlockQueueSize);
        Preconditions.requirePowerOfTwo(verifiedBlockQueueSize);
    }
}
