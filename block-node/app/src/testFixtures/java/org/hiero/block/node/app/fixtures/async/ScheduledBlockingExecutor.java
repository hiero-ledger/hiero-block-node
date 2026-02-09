// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.fixtures.async;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;

public class ScheduledBlockingExecutor extends ScheduledThreadPoolExecutor {
    /** The work queue that will be used to hold the tasks. */
    private final BlockingQueue<Runnable> workQueue;

    public ScheduledBlockingExecutor(@NonNull final BlockingQueue<Runnable> workQueue) {
        super(1, Thread.ofVirtual().factory(), new AbortPolicy());

        this.workQueue = workQueue; // actual work queue
    }

    public ScheduledBlockingExecutor(int corePoolSize, @NonNull final BlockingQueue<Runnable> workQueue) {
        super(corePoolSize, Thread.ofVirtual().factory(), new AbortPolicy());

        this.workQueue = workQueue; // actual work queue
    }

    /**
     * Captures the task into the work queue without executing it.
     * <p>
     * We intentionally do NOT call {@code super.execute(command)} here. The
     * purpose of this executor is to hold tasks until tests explicitly drain
     * the queue, giving tests full control over execution ordering and timing.
     * Calling the parent's execute would defeat that contract. Tests that need
     * tasks to actually run should use a real {@link java.util.concurrent.ScheduledThreadPoolExecutor}.
     */
    @Override
    @SuppressWarnings("all")
    public void execute(@NonNull final Runnable command) {
        try {
            workQueue.put(command);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Thread was interrupted while trying to put a task into the work queue", e);
        }
    }
}
