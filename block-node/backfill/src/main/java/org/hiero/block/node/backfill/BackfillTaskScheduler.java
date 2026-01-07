// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.backfill;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * Single-worker scheduler with a bounded FIFO queue for processing backfill gaps.
 * <p>
 * Discards new gaps when the queue is full to prevent memory overload.
 * The executor lifecycle is managed by the caller (not shut down on close).
 * Designed to be instantiated multiple times (e.g., one for historical, one for live-tail).
 */
final class BackfillTaskScheduler implements AutoCloseable {
    private static final System.Logger LOGGER = System.getLogger(BackfillTaskScheduler.class.getName());

    private final ArrayBlockingQueue<GapDetector.Gap> queue;
    private final AtomicBoolean workerRunning = new AtomicBoolean(false);
    private final AtomicBoolean shutdown = new AtomicBoolean(false);
    private final ExecutorService executor;
    private final Consumer<GapDetector.Gap> gapProcessor;
    private final BackfillFetcher fetcher;
    private final BackfillPersistenceAwaiter persistenceAwaiter;

    /**
     * Creates a new scheduler with the specified queue capacity.
     *
     * @param executor the executor for running the worker thread (lifecycle managed by caller)
     * @param gapProcessor the consumer that processes each gap
     * @param queueCapacity maximum number of pending gaps (new gaps discarded when full)
     * @param fetcher the fetcher used by this scheduler (for availability queries)
     * @param persistenceAwaiter the awaiter for backpressure control (cleared on close)
     */
    BackfillTaskScheduler(
            @NonNull ExecutorService executor,
            @NonNull Consumer<GapDetector.Gap> gapProcessor,
            int queueCapacity,
            @NonNull BackfillFetcher fetcher,
            @NonNull BackfillPersistenceAwaiter persistenceAwaiter) {
        this.executor = Objects.requireNonNull(executor);
        this.gapProcessor = Objects.requireNonNull(gapProcessor);
        this.queue = new ArrayBlockingQueue<>(queueCapacity);
        this.fetcher = Objects.requireNonNull(fetcher);
        this.persistenceAwaiter = Objects.requireNonNull(persistenceAwaiter);
    }

    /**
     * Returns the fetcher associated with this scheduler.
     *
     * @return the fetcher for availability queries
     */
    @NonNull
    BackfillFetcher getFetcher() {
        return fetcher;
    }

    /**
     * Submits a gap to the queue for processing.
     *
     * @param gap the gap to process
     * @return true if the gap was accepted, false if the queue was full (gap discarded)
     */
    boolean submit(@NonNull GapDetector.Gap gap) {
        if (shutdown.get()) {
            return false;
        }
        boolean accepted = queue.offer(gap);
        if (accepted) {
            ensureWorkerRunning();
        } else {
            LOGGER.log(System.Logger.Level.TRACE, "Queue full, discarding gap: [%s]".formatted(gap));
        }
        return accepted;
    }

    /**
     * Returns the current number of tasks in the queue.
     */
    int queueSize() {
        return queue.size();
    }

    /**
     * Returns whether the worker is currently processing tasks.
     */
    boolean isRunning() {
        return workerRunning.get();
    }

    @Override
    public void close() {
        shutdown.set(true);
        queue.clear();
        persistenceAwaiter.clear();
        // Note: executor lifecycle is managed by the creator (BackfillPlugin)
    }

    private void ensureWorkerRunning() {
        if (!workerRunning.compareAndSet(false, true)) {
            return;
        }
        executor.submit(this::drain);
    }

    private void drain() {
        try {
            while (!shutdown.get() && !Thread.currentThread().isInterrupted()) {
                GapDetector.Gap gap = queue.poll();
                if (gap == null) {
                    return;
                }
                gapProcessor.accept(gap);
            }
        } finally {
            workerRunning.set(false);
            if (!queue.isEmpty() && !shutdown.get()) {
                ensureWorkerRunning();
            } else if (!shutdown.get()) {
                // Reset health when scheduler becomes idle (queue drained)
                fetcher.resetHealth();
            }
        }
    }
}
