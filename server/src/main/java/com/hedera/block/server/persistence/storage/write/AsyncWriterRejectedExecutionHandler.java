// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.server.persistence.storage.write;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.ERROR;
import static java.lang.System.Logger.Level.WARNING;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * A custom handler for rejected execution of async writer tasks.
 * <p>
 * This handler is invoked when a task cannot be accepted for execution because the
 * queue has reached its capacity limit. It provides a graceful fallback mechanism
 * to ensure that block persistence tasks are not lost when the system is under high load.
 * <p>
 * The handler implements a "caller runs" policy, which means that when the queue is full,
 * the task will be executed in the thread that attempted to submit it. This provides
 * backpressure to the caller, slowing down the rate of incoming tasks.
 */
public class AsyncWriterRejectedExecutionHandler implements RejectedExecutionHandler {
    private static final System.Logger LOGGER = System.getLogger(AsyncWriterExecutorFactory.class.getName());

    /**
     * Handles a task that was rejected from the thread pool executor.
     * <p>
     * When a task is rejected because the queue is full, this method:
     * <ol>
     *   <li>Logs a warning message with details about the rejection
     *   <li>Executes the task in the caller's thread if the executor is not shutdown
     * </ol>
     * <p>
     * This creates backpressure on the caller, naturally throttling the submission rate
     * when the system is under high load.
     *
     * @param r the runnable task that was rejected
     * @param executor the executor that rejected the task
     */
    @Override
    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
        LOGGER.log(
                WARNING,
                "Task rejected from async writer executor. Queue capacity reached: queue size=%d, active threads=%d/%d, completed tasks=%d"
                        .formatted(
                                executor.getQueue().size(),
                                executor.getActiveCount(),
                                executor.getMaximumPoolSize(),
                                executor.getCompletedTaskCount()));
        // If the executor is not in the process of shutting down, run the task in the caller's thread
        if (!executor.isShutdown()) {
            LOGGER.log(DEBUG, "Executing rejected task in the caller's thread");
            r.run();
        } else {
            LOGGER.log(ERROR, "Rejected task discarded, because executor is shutting down");
            throw new RejectedExecutionException("Executor shutting down");
        }
    }
}
