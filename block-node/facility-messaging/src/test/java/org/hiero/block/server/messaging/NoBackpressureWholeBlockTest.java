// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.messaging;

import static org.hiero.block.server.messaging.DynamicBlockItemTest.intToBytes;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hedera.pbj.runtime.OneOf;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockItemUnparsed.ItemOneOfType;
import org.hiero.block.node.messaging.BlockMessagingFacilityImpl;
import org.hiero.block.node.messaging.MessagingConfig;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.BlockMessagingFacility;
import org.hiero.block.node.spi.blockmessaging.NoBackPressureBlockItemHandler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests that a no-backpressure block item handler only ever receives whole blocks, no matter where in a block the
 * publisher is when the handler is registered.
 */
class NoBackpressureWholeBlockTest {
    private static final int TIMEOUT_SECONDS = 20;

    private BlockMessagingFacility facility;
    private int nextItemId;

    @BeforeEach
    void setUp() {
        facility = new BlockMessagingFacilityImpl();
        facility.init(TestConfig.getBlockNodeContext(), null);
        facility.start();
        nextItemId = 0;
    }

    @AfterEach
    void tearDown() {
        facility.stop();
    }

    /** Create a distinct batch so that received batches can be compared with the ones sent. */
    private BlockItems batch(final long blockNumber, final boolean start, final boolean end) {
        return new BlockItems(
                List.of(new BlockItemUnparsed(new OneOf<>(ItemOneOfType.BLOCK_HEADER, intToBytes(nextItemId++)))),
                blockNumber,
                start,
                end);
    }

    /** Records every delivered batch and counts down once the expected number has arrived. */
    private static final class RecordingHandler implements NoBackPressureBlockItemHandler {
        final List<BlockItems> received = new CopyOnWriteArrayList<>();
        final CountDownLatch expected;

        RecordingHandler(final int expectedCount) {
            this.expected = new CountDownLatch(expectedCount);
        }

        @Override
        public void handleBlockItemsReceived(final BlockItems blockItems) {
            received.add(blockItems);
            expected.countDown();
        }

        @Override
        public void onTooFarBehindError() {
            throw new AssertionError("should not be evicted");
        }
    }

    @Test
    void handlerRegisteredMidBlockSeesNothingUntilNextBlockStart() throws InterruptedException {
        // publisher is mid block 0 when the handler registers
        facility.sendBlockItems(batch(0, true, false));
        facility.sendBlockItems(batch(0, false, false));
        final RecordingHandler handler = new RecordingHandler(3);
        facility.registerNoBackpressureBlockItemHandler(handler, false, "midBlock");

        final BlockItems tail = batch(0, false, false);
        final BlockItems tailEnd = batch(0, false, true);
        final BlockItems start1 = batch(1, true, false);
        final BlockItems mid1 = batch(1, false, false);
        final BlockItems end1 = batch(1, false, true);
        for (final BlockItems items : List.of(tail, tailEnd, start1, mid1, end1)) {
            facility.sendBlockItems(items);
        }

        assertTrue(handler.expected.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        assertEquals(List.of(start1, mid1, end1), handler.received);
    }

    @Test
    void handlerRegisteredAtBlockBoundarySeesEveryBatch() throws InterruptedException {
        final RecordingHandler handler = new RecordingHandler(4);
        facility.registerNoBackpressureBlockItemHandler(handler, false, "boundary");

        final BlockItems start0 = batch(0, true, false);
        final BlockItems end0 = batch(0, false, true);
        final BlockItems start1 = batch(1, true, false);
        final BlockItems end1 = batch(1, false, true);
        for (final BlockItems items : List.of(start0, end0, start1, end1)) {
            facility.sendBlockItems(items);
        }

        assertTrue(handler.expected.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        assertEquals(List.of(start0, end0, start1, end1), handler.received);
    }

    @Test
    void handlerReRegisteredMidBlockAfterEvictionSeesOnlyWholeBlocks() throws InterruptedException {
        final int ringSize =
                TestConfig.getConfig().getConfigData(MessagingConfig.class).blockItemQueueSize();
        final CountDownLatch firstBatchSeen = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final CountDownLatch evicted = new CountDownLatch(1);
        final NoBackPressureBlockItemHandler slowHandler = new NoBackPressureBlockItemHandler() {
            @Override
            public void handleBlockItemsReceived(final BlockItems blockItems) {
                firstBatchSeen.countDown();
                try {
                    release.await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }

            @Override
            public void onTooFarBehindError() {
                evicted.countDown();
            }
        };
        facility.registerNoBackpressureBlockItemHandler(slowHandler, false, "slow");
        // handler blocks on the first batch, then the ring is filled well past the 80% eviction threshold
        facility.sendBlockItems(batch(0, true, false));
        assertTrue(firstBatchSeen.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        for (int i = 0; i < ringSize * 9 / 10; i++) {
            facility.sendBlockItems(batch(0, false, false));
        }
        release.countDown();
        assertTrue(evicted.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));

        // the publisher is mid block 0; re-register the handler
        final RecordingHandler handler = new RecordingHandler(2);
        facility.registerNoBackpressureBlockItemHandler(handler, false, "reRegistered");
        final BlockItems tail = batch(0, false, true);
        final BlockItems start1 = batch(1, true, false);
        final BlockItems end1 = batch(1, false, true);
        for (final BlockItems items : List.of(tail, start1, end1)) {
            facility.sendBlockItems(items);
        }

        assertTrue(handler.expected.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        assertEquals(List.of(start1, end1), handler.received);
    }
}
