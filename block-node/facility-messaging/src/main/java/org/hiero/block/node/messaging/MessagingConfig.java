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
 *     block in memory. The bigger the queue, the more memory is used. There
 *     should be no reason to have a big queue. The queue size must be a power
 *     of two.
 * @param waitStrategy How ring handlers wait for the next message once they
 *     have caught up. {@code PHASED_BACKOFF} yields briefly and then blocks,
 *     so idle handlers cost no CPU. {@code SLEEPING} is the old behavior and
 *     only meant for rollback.
 * @param waitStrategyYieldTimeoutMicros How long a {@code PHASED_BACKOFF}
 *     handler keeps yielding after its last message before it blocks, in
 *     microseconds. Must not be negative.
 */
@ConfigData("messaging")
public record MessagingConfig(
        @Loggable @ConfigProperty(defaultValue = "512") int blockItemQueueSize,
        @Loggable @ConfigProperty(defaultValue = "32") int blockNotificationQueueSize,

        @Loggable @ConfigProperty(defaultValue = "PHASED_BACKOFF")
        MessagingWaitStrategy waitStrategy,

        @Loggable @ConfigProperty(defaultValue = "1000") long waitStrategyYieldTimeoutMicros) {
    /**
     * Constructor.
     */
    public MessagingConfig {
        Preconditions.requirePositive(blockItemQueueSize);
        Preconditions.requirePowerOfTwo(blockItemQueueSize);
        Preconditions.requirePositive(blockNotificationQueueSize);
        Preconditions.requirePowerOfTwo(blockNotificationQueueSize);
        Objects.requireNonNull(waitStrategy);
        Preconditions.requireGreaterOrEqual(waitStrategyYieldTimeoutMicros, 0L);
    }
}
