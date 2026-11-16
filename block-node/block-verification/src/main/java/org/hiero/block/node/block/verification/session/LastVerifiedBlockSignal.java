// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.session;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/// Lets a verification session whose result waits for an earlier block block until the last verified block moves,
/// instead of polling it. Code that advances the last verified block calls [#signalChanged] after a successful update.
/// A writer that doesn't signal only delays a waiter until its timeout ends.
public final class LastVerifiedBlockSignal {
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition changed = lock.newCondition();

    /// Wake every waiter. Call after the last verified block has changed.
    public void signalChanged() {
        lock.lock();
        try {
            changed.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /// Wait until the last verified block is no longer `seenValue` or the timeout passes.
    /// Returns at once if it has already changed. The value is checked while holding the lock,
    /// and [#signalChanged] takes the lock, so an update between the caller's read and this
    /// wait still wakes the caller. An interrupt ends the wait and keeps the interrupt flag set.
    ///
    /// @param lastVerifiedBlock the last verified block
    /// @param seenValue the value the caller last read
    /// @param timeout the longest time to wait
    /// @param unit the unit of the timeout
    public void awaitChangeFrom(
            final AtomicLong lastVerifiedBlock, final long seenValue, final long timeout, final TimeUnit unit) {
        lock.lock();
        try {
            long remainingNanos = unit.toNanos(timeout);
            while (lastVerifiedBlock.get() == seenValue && remainingNanos > 0) {
                remainingNanos = changed.awaitNanos(remainingNanos);
            }
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            lock.unlock();
        }
    }
}
