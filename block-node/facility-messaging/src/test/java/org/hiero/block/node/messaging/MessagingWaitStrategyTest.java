// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.messaging;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.PhasedBackoffWaitStrategy;
import com.lmax.disruptor.SleepingWaitStrategy;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Tests that each {@link MessagingWaitStrategy} creates the matching Disruptor wait strategy.
 */
class MessagingWaitStrategyTest {

    @Test
    void phasedBackoffCreatesPhasedBackoffWaitStrategy() {
        assertInstanceOf(PhasedBackoffWaitStrategy.class, MessagingWaitStrategy.PHASED_BACKOFF.create(1_000L));
    }

    @Test
    void blockingCreatesBlockingWaitStrategy() {
        assertInstanceOf(BlockingWaitStrategy.class, MessagingWaitStrategy.BLOCKING.create(1_000L));
    }

    @Test
    void sleepingCreatesSleepingWaitStrategy() {
        assertInstanceOf(SleepingWaitStrategy.class, MessagingWaitStrategy.SLEEPING.create(1_000L));
    }

    @ParameterizedTest
    @EnumSource(MessagingWaitStrategy.class)
    void eachCallCreatesANewInstance(final MessagingWaitStrategy waitStrategy) {
        assertNotSame(waitStrategy.create(1_000L), waitStrategy.create(1_000L));
    }
}
