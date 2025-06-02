// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.suites;

import static java.lang.System.Logger.Level.ERROR;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * An executor service that extends {@link ThreadPoolExecutor} to provide custom configurations
 * and enhanced error logging for tasks executed within the thread pool.
 *
 * <p>This executor is configured with:
 * <ul>
 *   <li>Core pool size: 8 threads
 *   <li>Maximum pool size: 8 threads
 *   <li>Keep-alive time: 10 seconds
 *   <li>Work queue: {@link LinkedBlockingQueue} with default capacity
 * </ul>
 *
 * <p>The executor overrides the {@link #afterExecute(Runnable, Throwable)} method to capture and log
 * any exceptions thrown during task execution, including exceptions thrown from {@code Runnable} tasks
 * and uncaught exceptions from {@code Future} tasks. This aids in debugging and error tracking
 * by ensuring that all exceptions are properly logged.
 */
public class ErrorLoggingExecutor extends ThreadPoolExecutor {
    private final System.Logger LOGGER = System.getLogger(getClass().getName());

    /**
     * Constructs a new {@code ErrorLoggingExecutor} with a fixed thread pool configuration.
     *
     * <p>The executor is set up with:
     * <ul>
     *   <li>Core pool size of 8 threads: the minimum number of threads to keep in the pool.
     *   <li>Maximum pool size of 8 threads: the maximum number of threads allowed in the pool.
     *   <li>Keep-alive time of 10 seconds: the maximum time that excess idle threads will wait for new tasks before terminating.
     *   <li>Work queue: a {@link LinkedBlockingQueue} to hold tasks before they are executed.
     * </ul>
     *
     * <p>This configuration ensures that the thread pool maintains 8 threads and does not create additional threads beyond that number.
     */
    public ErrorLoggingExecutor() {
        super(8, 8, 10, TimeUnit.SECONDS, new LinkedBlockingQueue<>());
    }

    /**
     * Invoked after the execution of the given {@code Runnable} task. This method provides an opportunity to perform
     * actions after each task execution, such as logging exceptions thrown by the task.
     *
     * <p>This implementation enhances error logging by capturing exceptions that may not have been
     * detected during the execution of the task. It handles both uncaught exceptions from {@code Runnable} tasks
     * and exceptions thrown during the computation of {@code Future} tasks.
     *
     * <p>Specifically, if the {@code Throwable} parameter {@code t} is {@code null}, indicating that no exception
     * was thrown during execution, but the task is an instance of {@code Future} and is done, it attempts to retrieve
     * the result or exception from the {@code Future}. Any exceptions encountered are then logged.
     *
     * <p>If the task execution resulted in an exception, this method logs the error message and stack trace using the {@code System.Logger}.
     *
     * @param r the runnable task that has completed execution
     * @param t the exception that caused termination, or {@code null} if execution completed normally
     */
    @Override
    protected void afterExecute(Runnable r, Throwable t) {
        Throwable localThrowable = t;
        if (localThrowable == null && r instanceof Future<?> && ((Future<?>) r).isDone()) {
            try {
                final Object result = ((Future<?>) r).get();
            } catch (final CancellationException ce) {
                localThrowable = ce;
            } catch (final ExecutionException ee) {
                localThrowable = ee.getCause();
            } catch (final InterruptedException ie) { // ignore/reset
                Thread.currentThread().interrupt();
            }
        }
        if (localThrowable != null) {
            LOGGER.log(ERROR, "Task encountered an error: ", localThrowable);
        }
    }
}
