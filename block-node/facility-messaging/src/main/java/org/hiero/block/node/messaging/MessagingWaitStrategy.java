// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.messaging;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.PhasedBackoffWaitStrategy;
import com.lmax.disruptor.SleepingWaitStrategy;
import com.lmax.disruptor.WaitStrategy;
import java.util.concurrent.TimeUnit;

/**
 * How a messaging ring handler waits for the next message once it has caught up with the publisher.
 */
public enum MessagingWaitStrategy {
    /**
     * Yield for up to the configured yield timeout after the last message, then block on a lock. A busy stream does not
     * pay a wake-up per batch, and an idle handler costs no CPU. There is no busy-spin phase, as a spinning handler on a
     * virtual thread would hold a carrier thread.
     */
    PHASED_BACKOFF,
    /** Always block on a lock and wait for the publisher to signal. Lowest CPU, one wake-up per batch. */
    BLOCKING,
    /**
     * Spin, yield, then sleep 100 ns in a loop, never blocking. Idle handlers keep the virtual thread carriers busy, so
     * an idle node burns several cores. Kept only as a rollback option.
     */
    SLEEPING;

    /**
     * Create a new wait strategy instance. Each ring needs its own instance, as the blocking strategies hold the lock the
     * ring's publisher signals.
     *
     * @param yieldTimeoutMicros how long a {@link #PHASED_BACKOFF} handler yields before blocking, in microseconds
     * @return a new wait strategy
     */
    WaitStrategy create(final long yieldTimeoutMicros) {
        return switch (this) {
            case PHASED_BACKOFF ->
                new PhasedBackoffWaitStrategy(0, yieldTimeoutMicros, TimeUnit.MICROSECONDS, new BlockingWaitStrategy());
            case BLOCKING -> new BlockingWaitStrategy();
            case SLEEPING -> new SleepingWaitStrategy();
        };
    }
}
