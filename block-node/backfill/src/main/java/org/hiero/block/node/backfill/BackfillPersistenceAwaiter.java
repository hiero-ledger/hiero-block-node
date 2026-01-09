// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.backfill;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.TRACE;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.hiero.block.node.spi.blockmessaging.BlockNotificationHandler;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.PersistedNotification;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;

/**
 * Handles backpressure by tracking blocks sent for persistence and waiting for
 * confirmation that they have been persisted. This ensures the backfill process
 * does not overwhelm the persistence layer by fetching blocks faster than they
 * can be stored.
 *
 * <p>Usage pattern:
 * <ol>
 *   <li>Call {@link #trackBlock(long)} before sending a block for persistence</li>
 *   <li>Send the block via the messaging facility</li>
 *   <li>Call {@link #awaitPersistence(long, long)} to block until persistence is confirmed</li>
 * </ol>
 */
public class BackfillPersistenceAwaiter implements BlockNotificationHandler {
    private static final System.Logger LOGGER = System.getLogger(BackfillPersistenceAwaiter.class.getName());

    /**
     * Map of block numbers to latches that are released when persistence is confirmed.
     * The latch is created when a block is tracked and counted down when persistence
     * notification is received.
     */
    private final ConcurrentHashMap<Long, CountDownLatch> pendingBlocks = new ConcurrentHashMap<>();

    /**
     * Tracks a block that will be sent for persistence. Must be called before
     * sending the block to the messaging facility to avoid race conditions
     * where the persistence notification arrives before we start waiting.
     *
     * @param blockNumber the block number to track
     */
    public void trackBlock(long blockNumber) {
        pendingBlocks.computeIfAbsent(blockNumber, k -> {
            LOGGER.log(TRACE, "Tracking block [%s] for persistence".formatted(blockNumber));
            return new CountDownLatch(1);
        });
    }

    /**
     * Waits for persistence confirmation for a specific block.
     *
     * @param blockNumber the block number to wait for
     * @param timeoutMs maximum time to wait in milliseconds
     * @return true if persistence was confirmed or block was not being tracked, false if timed out or interrupted
     */
    public boolean awaitPersistence(long blockNumber, long timeoutMs) {
        CountDownLatch latch = pendingBlocks.get(blockNumber);
        if (latch == null) {
            LOGGER.log(DEBUG, "Block [%s] already persisted or not tracked".formatted(blockNumber));
            return true;
        }

        LOGGER.log(TRACE, "Waiting for block [%s] persistence (timeout=[%s]ms)".formatted(blockNumber, timeoutMs));
        try {
            boolean completed = latch.await(timeoutMs, TimeUnit.MILLISECONDS);
            if (completed) {
                LOGGER.log(TRACE, "Block [%s] persistence confirmed".formatted(blockNumber));
            } else {
                LOGGER.log(DEBUG, "Block [%s] persistence timed out after [%s]ms".formatted(blockNumber, timeoutMs));
            }
            return completed;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.log(DEBUG, "Block [%s] persistence wait interrupted".formatted(blockNumber));
            return false;
        } finally {
            pendingBlocks.remove(blockNumber);
        }
    }

    /**
     * Handles persistence notifications from the messaging facility. When a block
     * from the BACKFILL source is persisted, the corresponding latch is released.
     *
     * @param notification the persistence notification
     */
    @Override
    public void handlePersisted(@NonNull PersistedNotification notification) {
        // we only care for backfilled blocks
        if (notification.blockSource() != BlockSource.BACKFILL) {
            return;
        }

        long blockNumber = notification.blockNumber();
        CountDownLatch latch = pendingBlocks.get(blockNumber);
        if (latch != null) {
            if (notification.succeeded()) {
                LOGGER.log(TRACE, "Received persistence confirmation for block [%s]".formatted(blockNumber));
            } else {
                LOGGER.log(INFO, "Block [%s] persistence failed".formatted(blockNumber));
            }
            latch.countDown();
        }
    }

    /**
     * Handles verification notifications from the messaging facility. If verification
     * fails for a backfill block, the latch is released immediately to fail fast
     * rather than waiting for a persistence notification that will never arrive.
     *
     * @param notification the verification notification
     */
    @Override
    public void handleVerification(@NonNull VerificationNotification notification) {
        // we only care for backfilled blocks
        if (notification.source() != BlockSource.BACKFILL) {
            return;
        }

        // Only release latch on verification failure - success means we still wait for persistence
        if (!notification.success()) {
            long blockNumber = notification.blockNumber();
            CountDownLatch latch = pendingBlocks.get(blockNumber);
            if (latch != null) {
                LOGGER.log(INFO, "Block [%s] verification failed, releasing latch".formatted(blockNumber));
                latch.countDown();
            }
        }
    }

    /**
     * Returns the number of blocks currently being tracked for persistence.
     * Currently only used for testing.
     * @return the number of pending blocks
     */
    int getPendingCount() {
        return pendingBlocks.size();
    }

    /**
     * Clears all pending blocks. Should be called during shutdown or when
     * resetting state.
     */
    public void clear() {
        // Release all waiting threads before clearing
        for (CountDownLatch latch : pendingBlocks.values()) {
            latch.countDown();
        }
        pendingBlocks.clear();
    }
}
