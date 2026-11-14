// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.messaging;

import static org.hiero.block.server.messaging.DynamicBlockItemTest.intToBytes;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hedera.pbj.runtime.OneOf;
import com.swirlds.config.api.Configuration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockItemUnparsed.ItemOneOfType;
import org.hiero.block.node.messaging.BlockMessagingFacilityImpl;
import org.hiero.block.node.messaging.MessagingConfig;
import org.hiero.block.node.messaging.MessagingWaitStrategy;
import org.hiero.block.node.spi.blockmessaging.BlockItemHandler;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.BlockMessagingFacility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Tests for the wait strategy configuration and for message delivery under each wait strategy.
 */
class MessagingWaitStrategyConfigTest {

    @Test
    void defaultWaitStrategyIsBlocking() {
        final MessagingConfig config = TestConfig.getConfig().getConfigData(MessagingConfig.class);
        assertEquals(MessagingWaitStrategy.BLOCKING, config.waitStrategy());
    }

    @Test
    void defaultYieldTimeoutIsOneMillisecond() {
        final MessagingConfig config = TestConfig.getConfig().getConfigData(MessagingConfig.class);
        assertEquals(1_000L, config.waitStrategyYieldTimeoutMicros());
    }

    @Test
    void negativeYieldTimeoutIsRejected() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new MessagingConfig(512, 32, MessagingWaitStrategy.PHASED_BACKOFF, -1L));
    }

    @Test
    void waitStrategyIsReadFromConfiguration() {
        final Configuration configuration = TestConfig.getConfig(Map.of("messaging.waitStrategy", "PHASED_BACKOFF"));
        final MessagingConfig config = configuration.getConfigData(MessagingConfig.class);
        assertEquals(MessagingWaitStrategy.PHASED_BACKOFF, config.waitStrategy());
    }

    /**
     * A handler that has caught up with the publisher, and so has gone into the waiting phase of its strategy, must still
     * receive the next batch. The yield timeout is zero, so a phased backoff handler goes straight to blocking.
     */
    @ParameterizedTest
    @EnumSource(MessagingWaitStrategy.class)
    void handlerReceivesBatchPublishedAfterItCaughtUp(final MessagingWaitStrategy waitStrategy)
            throws InterruptedException {
        final Configuration configuration = TestConfig.getConfig(
                Map.of("messaging.waitStrategy", waitStrategy.name(), "messaging.waitStrategyYieldTimeoutMicros", "0"));
        final CountDownLatch firstBatchReceived = new CountDownLatch(1);
        final CountDownLatch secondBatchReceived = new CountDownLatch(2);
        final BlockItemHandler handler = new CountingHandler(firstBatchReceived, secondBatchReceived);
        final BlockMessagingFacility messagingService = new BlockMessagingFacilityImpl();
        messagingService.init(TestConfig.getBlockNodeContext(configuration), null);
        messagingService.registerBlockItemHandler(handler, false, "waitStrategyTestHandler");
        messagingService.start();
        try {
            messagingService.sendBlockItems(blockItems(0));
            assertTrue(firstBatchReceived.await(10, TimeUnit.SECONDS), "First batch was not delivered");
            messagingService.sendBlockItems(blockItems(1));
            assertTrue(
                    secondBatchReceived.await(10, TimeUnit.SECONDS), "Batch sent after catching up was not delivered");
        } finally {
            messagingService.stop();
        }
    }

    private static BlockItems blockItems(final int blockNumber) {
        return new BlockItems(
                List.of(new BlockItemUnparsed(new OneOf<>(ItemOneOfType.BLOCK_HEADER, intToBytes(blockNumber)))),
                blockNumber,
                true,
                true);
    }

    /**
     * Counts down both latches on every batch, so the second latch only reaches zero on the second batch.
     */
    private record CountingHandler(CountDownLatch firstBatchReceived, CountDownLatch secondBatchReceived)
            implements BlockItemHandler {
        @Override
        public void handleBlockItemsReceived(final BlockItems blockItems) {
            firstBatchReceived.countDown();
            secondBatchReceived.countDown();
        }
    }
}
