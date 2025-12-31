// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.backfill;

import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/**
 * Simple single-worker scheduler: always prefers live-tail tasks over historical.
 */
final class BackfillTaskScheduler {
    private final ConcurrentLinkedQueue<BackfillTask> liveQueue = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<BackfillTask> historicalQueue = new ConcurrentLinkedQueue<>();
    private final AtomicBoolean workerRunning = new AtomicBoolean(false);
    private final ExecutorService executor;
    private final Function<BackfillTask, LongRangeResult> taskRunner;

    BackfillTaskScheduler(ExecutorService executor, Function<BackfillTask, LongRangeResult> taskRunner) {
        this.executor = Objects.requireNonNull(executor);
        this.taskRunner = Objects.requireNonNull(taskRunner);
    }

    void submit(BackfillTask task) {
        if (task.gap().type() == GapType.LIVE_TAIL) {
            liveQueue.add(task);
        } else {
            historicalQueue.add(task);
        }
        tryStartWorker();
    }

    private void tryStartWorker() {
        if (!workerRunning.compareAndSet(false, true)) {
            return;
        }
        executor.submit(this::drain);
    }

    private void drain() {
        try {
            while (true) {
                BackfillTask task = pollNext();
                if (task == null) {
                    return;
                }
                LongRangeResult remaining = taskRunner.apply(task);
                if (remaining != null && remaining.remainingRange() != null) {
                    // Requeue leftover historical portion
                    submit(new BackfillTask(task.id(), new TypedGap(remaining.remainingRange(), GapType.HISTORICAL), task.origin()));
                }
            }
        } finally {
            workerRunning.set(false);
            if (!liveQueue.isEmpty() || !historicalQueue.isEmpty()) {
                tryStartWorker();
            }
        }
    }

    private BackfillTask pollNext() {
        BackfillTask t = liveQueue.poll();
        if (t != null) {
            return t;
        }
        return historicalQueue.poll();
    }

    boolean hasPendingLiveTail() {
        return !liveQueue.isEmpty();
    }

    /** Result of executing a task, optionally carrying a remaining range if preempted. */
    static final class LongRangeResult {
        private final org.hiero.block.node.spi.historicalblocks.LongRange remainingRange;

        LongRangeResult(org.hiero.block.node.spi.historicalblocks.LongRange remainingRange) {
            this.remainingRange = remainingRange;
        }

        org.hiero.block.node.spi.historicalblocks.LongRange remainingRange() {
            return remainingRange;
        }
    }
}
