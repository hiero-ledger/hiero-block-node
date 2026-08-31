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
 *
 * <p>A released latch is <b>not</b> equivalent to a successfully persisted block: latches are also
 * released on persistence failure and on verification failure so the waiter can fail fast. The
 * outcome is therefore tracked alongside the latch, and {@link #awaitPersistence(long, long)}
 * reports it.
 */
public class BackfillPersistenceAwaiter implements BlockNotificationHandler {
    private static final System.Logger LOGGER = System.getLogger(BackfillPersistenceAwaiter.class.getName());

    /**
     * A tracked block: the latch released once its fate is known, plus whether that fate was success.
     * The flag is last-write-wins - several persistence plugins can report on the same block, so a
     * late failure can overwrite an earlier success. The worst case is one extra fetch of a block
     * that is idempotent to re-persist, which is cheaper than sticky-success bookkeeping.
     */
    private static final class Pending {
        private final CountDownLatch latch = new CountDownLatch(1);
        private volatile boolean succeeded = false;

        /**
         * Records the outcome and releases the latch. The outcome is written first, so a waiter woken
         * by the latch always sees it.
         *
         * @param successful whether the block was reported as successfully persisted
         */
        void complete(final boolean successful) {
            succeeded = successful;
            latch.countDown();
        }

        /**
         * @return whether the block was reported as successfully persisted
         */
        boolean succeeded() {
            return succeeded;
        }

        /**
         * Waits until the block's fate is known, or the timeout elapses.
         *
         * @param timeoutMs maximum time to wait in milliseconds
         * @return true if the fate became known before the timeout elapsed
         * @throws InterruptedException if the wait is interrupted
         */
        boolean await(final long timeoutMs) throws InterruptedException {
            return latch.await(timeoutMs, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Map of block numbers to the latch/outcome pair released when the block's fate is known.
     * The entry is created when a block is tracked and counted down when a persistence or
     * verification notification is received.
     */
    private final ConcurrentHashMap<Long, Pending> pendingBlocks = new ConcurrentHashMap<>();

    /**
     * Tracks a block that will be sent for persistence. Must be called before
     * sending the block to the messaging facility to avoid race conditions
     * where the persistence notification arrives before we start waiting.
     *
     * @param blockNumber the block number to track
     */
    public void trackBlock(long blockNumber) {
        pendingBlocks.computeIfAbsent(blockNumber, k -> {
            final String trackingBlockMsg = "Tracking block [{0}] for persistence";
            LOGGER.log(TRACE, trackingBlockMsg, blockNumber);
            return new Pending();
        });
    }

    /**
     * Waits for persistence confirmation for a specific block.
     * <p>
     * A {@code false} result means "not persisted", covering a timeout, an interrupt, and a reported
     * persistence or verification failure alike. All of those leave the block still missing, which is
     * the only distinction the caller acts on.
     *
     * @param blockNumber the block number to wait for
     * @param timeoutMs maximum time to wait in milliseconds
     * @return true if persistence was confirmed or block was not being tracked, false if the block
     *         failed to persist, timed out, or the wait was interrupted
     */
    public boolean awaitPersistence(long blockNumber, long timeoutMs) {
        Pending pending = pendingBlocks.get(blockNumber);
        if (pending == null) {
            final String alreadyPersistedMsg = "Block [{0}] already persisted or not tracked";
            LOGGER.log(DEBUG, alreadyPersistedMsg, blockNumber);
            return true;
        }

        final String waitingForBlockMsg = "Waiting for block [{0}] persistence (timeout=[{1}]ms)";
        LOGGER.log(TRACE, waitingForBlockMsg, blockNumber, timeoutMs);
        try {
            boolean completed = pending.await(timeoutMs);
            if (completed) {
                final String persistenceConfirmedMsg = "Block [{0}] persistence resolved, succeeded=[{1}]";
                LOGGER.log(TRACE, persistenceConfirmedMsg, blockNumber, pending.succeeded());
            } else {
                final String persistenceTimedOutMsg = "Block [{0}] persistence timed out after [{1}]ms";
                LOGGER.log(DEBUG, persistenceTimedOutMsg, blockNumber, timeoutMs);
            }
            return completed && pending.succeeded();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            final String waitInterruptedMsg = "Block [%s] persistence wait interrupted".formatted(blockNumber);
            LOGGER.log(DEBUG, waitInterruptedMsg, e);
            return false;
        } finally {
            pendingBlocks.remove(blockNumber);
        }
    }

    /**
     * Handles persistence notifications from the messaging facility. When a block
     * from the BACKFILL source is reported on, its outcome is recorded and the
     * corresponding latch is released.
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
        Pending pending = pendingBlocks.get(blockNumber);
        if (pending != null) {
            if (notification.succeeded()) {
                final String receivedConfirmationMsg = "Received persistence confirmation for block [{0}]";
                LOGGER.log(TRACE, receivedConfirmationMsg, blockNumber);
            } else {
                final String persistenceFailedMsg = "Block [{0}] persistence failed";
                LOGGER.log(INFO, persistenceFailedMsg, blockNumber);
            }
            pending.complete(notification.succeeded());
        }
    }

    /**
     * Handles verification notifications from the messaging facility. If verification
     * fails for a backfill block, the block is marked as not persisted and the latch is
     * released immediately to fail fast rather than waiting for a persistence notification
     * that will never arrive.
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
            Pending pending = pendingBlocks.get(blockNumber);
            if (pending != null) {
                final String verificationFailedMsg = "Block [{0}] verification failed, marking block as not persisted";
                LOGGER.log(INFO, verificationFailedMsg, blockNumber);
                pending.complete(false);
            }
        }
    }

    /**
     * Clears all pending blocks. Should be called during shutdown or when
     * resetting state.
     */
    public void clear() {
        // Release all waiting threads before clearing. A block abandoned by a shutdown is reported as
        // not persisted, so it is re-detected and re-fetched on the next run.
        for (Pending pending : pendingBlocks.values()) {
            pending.complete(false);
        }
        pendingBlocks.clear();
    }
}
