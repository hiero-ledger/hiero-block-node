// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.stream.subscriber;

import com.swirlds.config.api.ConfigData;
import com.swirlds.config.api.ConfigProperty;
import com.swirlds.config.api.validation.annotation.Min;
import org.hiero.block.node.base.Loggable;

/**
 * Use this configuration across the stream subscriber plugin.
 *
 * @param liveQueueSize The size of the queue used to transfer live batches
 *     between the messaging and the client thread.<br/>
 *     This value is a number of _batches_, not blocks, so generally this should
 *     be around 100 times the number of blocks that should be pending at any
 *     moment (i.e. a typical block is 100 batches, so to support 50 blocks this
 *     value would be 5000).
 * @param maximumFutureRequest The furthest in the future a request can set the
 *     start block for a stream.  If a request specifies a start block further
 *     than this many blocks above the latest known "live" block, the request will
 *     be rejected.
 * @param minimumLiveQueueCapacity The minimum available capacity in the live queue
 *     that the session will try to maintain.  If there is less than this much
 *     capacity available, the session will drop the oldest full blocks (at the queue head)
 *     in the queue until at least this many batches can be added without blocking.<br/>
 *     This value should typically be around 10% of the live queue size.
 */
@ConfigData("subscriber")
public record SubscriberConfig(
        @Loggable @ConfigProperty(defaultValue = "4000") @Min(100) int liveQueueSize,
        @Loggable @ConfigProperty(defaultValue = "4000") @Min(10) long maximumFutureRequest,
        @Loggable @ConfigProperty(defaultValue = "400") @Min(10) int minimumLiveQueueCapacity) {}
