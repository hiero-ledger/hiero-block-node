// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.fixtures.async;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;

public class ScheduledBlockingExecutor extends ScheduledThreadPoolExecutor {
    /** The work queue that will be used to hold the tasks. */
    private final BlockingQueue<Runnable> workQueue;

    public ScheduledBlockingExecutor(@NonNull final BlockingQueue<Runnable> workQueue) {
        super(1, Executors.defaultThreadFactory(), new AbortPolicy());

        this.workQueue = workQueue; // actual work queue
    }

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
