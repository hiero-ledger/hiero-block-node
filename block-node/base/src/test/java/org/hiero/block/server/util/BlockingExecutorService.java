// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.util;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class BlockingExecutorService extends ThreadPoolExecutor {
    // Useful for testing async logic. Blocks the test thread when waitTasksToComplete() is called.
    // Takes as param the expected number of tasks to finish before continuing the test thread.
    // (Additional tasks, if any, are executed before releasing, the queue should be empty)
    // Takes the pool size as a second parameter
    private final int expectedTasks;
    private int completedTasks = 0;
    private final CountDownLatch countDownLatch;

    public BlockingExecutorService(int expectedTasks, int poolSize) {
        super(poolSize, poolSize, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
        this.expectedTasks = expectedTasks;
        this.countDownLatch = new CountDownLatch(1);
    }

    @Override
    protected void afterExecute(Runnable r, Throwable t) {
        completedTasks++;
        if (getQueue().isEmpty() && completedTasks >= expectedTasks) {
            countDownLatch.countDown();
        }
    }

    public void waitTasksToComplete() throws InterruptedException {
        countDownLatch.await();
    }
}
