// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.backfill;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Minimal scheduler wrapper to keep executor wiring and shutdown logic in one place.
 */
final class BackfillScheduler implements AutoCloseable {

    private final ScheduledExecutorService executor;

    BackfillScheduler(ScheduledExecutorService executor) {
        this.executor = executor;
    }

    void schedulePeriodic(long initialDelayMs, long intervalMs, Runnable task) {
        executor.scheduleAtFixedRate(task, initialDelayMs, intervalMs, TimeUnit.MILLISECONDS);
    }

    void submit(Runnable task) {
        executor.submit(task);
    }

    @Override
    public void close() {
        executor.shutdownNow();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdown();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
