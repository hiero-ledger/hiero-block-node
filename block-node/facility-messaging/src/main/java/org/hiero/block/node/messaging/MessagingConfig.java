// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.messaging;

import com.swirlds.config.api.ConfigData;
import com.swirlds.config.api.ConfigProperty;
import java.util.Objects;
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
 *     can contain a reference to a whole block of data and hence keep a whole
 *     block in memory: unless the verified block ring is enabled, see
 *     {@code verifiedBlockRingEnabled}, block verification results travel on
 *     this queue, and a successful one carries the whole block. The bigger the
 *     queue, the more memory is used. There should be no reason to have a big
 *     queue. The queue size must be a power of two.
 * @param unvalidatedBlockQueueSize The maximum number of unvalidated blocks
 *     that can be queued for processing. Unvalidated blocks are whole blocks
 *     that are not verified yet and do not come from the live publisher, for
 *     now the blocks fetched by backfill. Each queued entry may keep a whole
 *     block in memory. When the queue is full, the senders of unvalidated
 *     blocks wait until there is room; the live publisher never waits on this
 *     queue. The queue size must be a power of two.
 * @param verifiedBlockQueueSize The maximum number of verified blocks that can
 *     be queued for processing on the verified block ring. Only used when
 *     {@code verifiedBlockRingEnabled} is true. Verified blocks are the results
 *     of block verification, and a successful result carries the whole block,
 *     so each queued entry may keep a whole block in memory. When the queue is
 *     full, the senders of verification results wait until there is room. The
 *     default matches the size of the block notification queue. The queue size
 *     must be a power of two.
 * @param verifiedBlockRingEnabled Whether block verification results travel on
 *     a ring of their own, the verified block ring. THE VERIFIED BLOCK RING IS
 *     NOT READY FOR PRODUCTION: it is for testing only and must not be enabled
 *     in production. False by default: verification results travel on the
 *     block notification ring, as they did before the verified block ring
 *     existed, so a verified block handler receives them on the same thread as
 *     its other block notifications, in the order they were sent. Either way
 *     only the verified block handlers receive them.
 * @param waitStrategy How ring handlers wait for the next message once they
 *     have caught up. {@code BLOCKING} (the default) blocks on a lock, so idle
 *     handlers cost no CPU. {@code PHASED_BACKOFF} yields briefly first.
 *     {@code SLEEPING} is the old behavior and only meant for rollback.
 * @param waitStrategyYieldTimeoutMicros How long a {@code PHASED_BACKOFF}
 *     handler keeps yielding after its last message before it blocks, in
 *     microseconds. Must not be negative.
 */
@ConfigData("messaging")
public record MessagingConfig(
        @Loggable @ConfigProperty(defaultValue = "512") int blockItemQueueSize,
        @Loggable @ConfigProperty(defaultValue = "32") int blockNotificationQueueSize,
        @Loggable @ConfigProperty(defaultValue = "32") int unvalidatedBlockQueueSize,
        @Loggable @ConfigProperty(defaultValue = "32") int verifiedBlockQueueSize,
        @Loggable @ConfigProperty(defaultValue = "false") boolean verifiedBlockRingEnabled,
        @Loggable @ConfigProperty(defaultValue = "BLOCKING") MessagingWaitStrategy waitStrategy,
        @Loggable @ConfigProperty(defaultValue = "1000") long waitStrategyYieldTimeoutMicros) {
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
        Objects.requireNonNull(waitStrategy);
        Preconditions.requireGreaterOrEqual(waitStrategyYieldTimeoutMicros, 0L);
    }
}
