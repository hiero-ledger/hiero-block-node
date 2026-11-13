// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.cloud.storage.download;

import static java.lang.System.Logger.Level.DEBUG;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.hiero.block.node.spi.blockmessaging.BlockNotificationHandler;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.PersistedNotification;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;

/// Waits for the fate of each block the poller submits: track the block, submit it, then await
/// it. A block persisted from another source, for example the publisher, also ends the wait.
class PersistenceAwaiter implements BlockNotificationHandler {
    private static final System.Logger LOGGER = System.getLogger(PersistenceAwaiter.class.getName());

    /// The fate of a submitted block.
    enum Outcome {
        /// Persisted from the block this plugin submitted.
        PERSISTED_BY_PULL,
        /// Persisted from another source, such as the publisher or backfill.
        PERSISTED_BY_OTHER,
        /// Failed to verify or to persist, or the wait was abandoned.
        FAILED,
        /// No outcome before the timeout.
        TIMED_OUT
    }

    /// Tracked blocks by number.
    private final Map<Long, Pending> pending = new ConcurrentHashMap<>();

    /// Starts tracking a block. Call before submitting it, so an outcome that arrives at once is
    /// not missed.
    ///
    /// @param blockNumber the block about to be submitted
    void track(final long blockNumber) {
        pending.putIfAbsent(blockNumber, new Pending());
    }

    /// Waits for the outcome of a tracked block and stops tracking it.
    ///
    /// @param blockNumber the tracked block
    /// @param timeoutMs how long to wait, in milliseconds
    /// @return the outcome; [Outcome#FAILED] when the block is not tracked or the wait is interrupted
    Outcome await(final long blockNumber, final long timeoutMs) {
        Outcome outcome = Outcome.FAILED;
        final Pending tracked = pending.get(blockNumber);
        if (tracked != null) {
            try {
                if (tracked.latch.await(timeoutMs, TimeUnit.MILLISECONDS)) {
                    outcome = tracked.outcome.get();
                } else {
                    outcome = Outcome.TIMED_OUT;
                }
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                LOGGER.log(DEBUG, "Wait for block {0} interrupted", blockNumber);
            } finally {
                pending.remove(blockNumber);
            }
        }
        return outcome;
    }

    /// {@inheritDoc}
    @Override
    public void handlePersisted(@NonNull final PersistedNotification notification) {
        final Pending tracked = pending.get(notification.blockNumber());
        final boolean ours = notification.blockSource() == BlockSource.CLOUD_STORAGE;
        // a failure of another source's copy leaves ours in flight, so keep waiting
        if (tracked != null && (notification.succeeded() || ours)) {
            final Outcome outcome;
            if (!notification.succeeded()) {
                outcome = Outcome.FAILED;
            } else if (ours) {
                outcome = Outcome.PERSISTED_BY_PULL;
            } else {
                outcome = Outcome.PERSISTED_BY_OTHER;
            }
            tracked.complete(outcome);
        }
    }

    /// {@inheritDoc}
    @Override
    public void handleVerification(@NonNull final VerificationNotification notification) {
        // success still waits for persistence; only a failure of our own copy ends the wait
        if (!notification.success() && notification.source() == BlockSource.CLOUD_STORAGE) {
            final Pending tracked = pending.get(notification.blockNumber());
            if (tracked != null) {
                tracked.complete(Outcome.FAILED);
            }
        }
    }

    /// Ends every wait as failed and stops tracking. Called on stop.
    void releaseAll() {
        for (final Pending tracked : pending.values()) {
            tracked.complete(Outcome.FAILED);
        }
        pending.clear();
    }

    /// A tracked block: the first outcome reported wins, later ones are ignored.
    private static final class Pending {
        private final CountDownLatch latch = new CountDownLatch(1);
        private final AtomicReference<Outcome> outcome = new AtomicReference<>();

        private void complete(final Outcome reported) {
            if (outcome.compareAndSet(null, reported)) {
                latch.countDown();
            }
        }
    }
}
