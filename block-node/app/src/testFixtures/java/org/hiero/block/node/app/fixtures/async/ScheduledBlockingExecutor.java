// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.fixtures.async;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.lang.System.Logger;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

public class ScheduledBlockingExecutor extends ScheduledThreadPoolExecutor {
    private final Logger LOGGER = System.getLogger(getClass().getCanonicalName());
    private static final long TASK_TIMEOUT_MILLIS = 5_000L;
    private static final long RUNNABLE_FUTURE_MAX_TIMEOUT_SECONDS = 60L;
    /// The Map that will be used to hold the tasks.
    private final ConcurrentLinkedQueue<Schedule> scheduleQueue;
    /// Queue used to execute tasks
    private final BlockingQueue<Runnable> workQueue;
    /// Counter to indicate total submitted tasks.
    private int tasksSubmitted;
    /// All async executors created when running tasks async
    private final Queue<ExecutorService> asyncExecutors;

    public ScheduledBlockingExecutor(@NonNull final BlockingQueue<Runnable> workQueue) {
        super(1, Thread.ofVirtual().factory(), new AbortPolicy());
        this.workQueue = workQueue; // actual work queue
        asyncExecutors = new ConcurrentLinkedQueue<>();
        scheduleQueue = new ConcurrentLinkedQueue<>();
    }

    public ScheduledBlockingExecutor(int corePoolSize, @NonNull final BlockingQueue<Runnable> workQueue) {
        super(corePoolSize, Thread.ofVirtual().factory(), new AbortPolicy());
        this.workQueue = workQueue; // actual work queue
        asyncExecutors = new ConcurrentLinkedQueue<>();
        scheduleQueue = new ConcurrentLinkedQueue<>();
    }

    @Override
    public long getTaskCount() {
        return workQueue.size() + scheduleQueue.size();
    }

    /// Overriding this method in order to make sure this logic will be called
    /// every time we will submit a task to the pool.
    @Override
    @SuppressWarnings("all")
    public void execute(@NonNull final Runnable command) {
        if (command instanceof RunnableFuture<?> ft) {
            // Wrap the FutureTask in a Runnable that will call get() on it
            // otherwise when executing async below, we will not get an exception
            // since the FutureTask will capture it. Super's submit() methods
            // wrap the task as a FutureTask.
            workQueue.offer(new FutureRunnable(ft));
        } else {
            workQueue.offer(command);
        }
        tasksSubmitted++;
    }

    /// Invoke this method once you have submitted all the tasks that need to be
    /// run by this executor. This method will then proceed to poll each task
    /// (in the same order that they have been submitted) and will execute the
    /// tasks serially on the same thread that called this method. Once this
    /// method returns (or throws), we are certain that all tasks have run and
    /// can safely assert based on that. This method will throw an
    /// [IllegalStateException] to indicate a broken state, when the queue
    /// is empty.
    public void executeSerially() {
        moveScheduledToWorkQueue();
        if (!workQueue.isEmpty()) {
            while (!workQueue.isEmpty()) {
                workQueue.poll().run();
            }
        }
    }

    /// This method executes all tasks that were submitted to this executor
    /// asynchronously.
    ///
    /// Timeout is {@value TASK_TIMEOUT_MILLIS} milliseconds, blocking is enabled,
    /// and exceptions will be thrown on exceptional completion.
    /// Logging on exceptional completion is disabled.
    /// @see #executeAsync(boolean, long, boolean, boolean, Supplier)
    public List<CompletableFuture<Void>> executeAsync() {
        return executeAsync(true);
    }

    /// This method executes all tasks that were submitted to this executor
    /// asynchronously.
    ///
    /// Timeout is {@value TASK_TIMEOUT_MILLIS} milliseconds and blocking is
    /// enabled. Logging on exceptional completion is disabled.
    /// @see #executeAsync(boolean, long, boolean, boolean, Supplier)
    public List<CompletableFuture<Void>> executeAsync(final boolean throwOnExceptionalCompletion) {
        return executeAsync(TASK_TIMEOUT_MILLIS, throwOnExceptionalCompletion);
    }

    /// This method executes all tasks that were submitted to this executor
    /// asynchronously.
    ///
    /// Blocking is enabled and exceptions will be thrown on exceptional
    /// completion. Logging on exceptional completion is disabled.
    /// @see #executeAsync(boolean, long, boolean, boolean, Supplier)
    public List<CompletableFuture<Void>> executeAsync(final long timeoutMillis) {
        return executeAsync(timeoutMillis, true);
    }

    /// This method executes all tasks that were submitted to this executor
    /// asynchronously.
    ///
    /// Blocking is enabled. Logging on exceptional completion is disabled.
    /// @see #executeAsync(boolean, long, boolean, boolean, Supplier)
    public List<CompletableFuture<Void>> executeAsync(
            final long blockTimeoutMillis, final boolean throwOnExceptionalCompletion) {
        return executeAsync(blockTimeoutMillis, throwOnExceptionalCompletion, false);
    }

    /// This method executes all tasks that were submitted to this executor
    /// asynchronously.
    ///
    /// Blocking is enabled.
    /// @see #executeAsync(boolean, long, boolean, boolean, Supplier)
    public List<CompletableFuture<Void>> executeAsync(
            final long blockTimeoutMillis,
            final boolean throwOnExceptionalCompletion,
            final boolean logOnExceptionalCompletion) {
        return executeAsync(
                true,
                blockTimeoutMillis,
                throwOnExceptionalCompletion,
                logOnExceptionalCompletion,
                () -> Executors.newThreadPerTaskExecutor(Executors.defaultThreadFactory()));
    }

    /// This method executes all tasks that were submitted to this executor
    /// asynchronously.
    ///
    /// Default pool implementation used.
    /// @see #executeAsync(boolean, long, boolean, boolean, Supplier)
    public List<CompletableFuture<Void>> executeAsync(
            final long blockTimeoutMillis,
            final boolean blockUntilDone,
            final boolean throwOnExceptionalCompletion,
            final boolean logOnExceptionalCompletion) {
        return executeAsync(
                blockUntilDone,
                blockTimeoutMillis,
                throwOnExceptionalCompletion,
                logOnExceptionalCompletion,
                () -> Executors.newThreadPerTaskExecutor(Executors.defaultThreadFactory()));
    }

    /// This method executes all tasks that were submitted to this executor
    /// asynchronously.
    ///
    /// Internally, a [java.util.concurrent.ThreadPerTaskExecutor] is used
    /// to submit each task. All tasks are started in the order they were
    /// submitted to the executor. The way tasks are submitted is by
    /// calling [CompletableFuture#runAsync(Runnable, java.util.concurrent.Executor)]
    /// on each task in the internal worker queue. Depending on the parameters
    /// described below, this method will either block or not, throw an
    /// exception or not, log an exceptional execution or not, and will use a
    /// timeout for each task or not.
    ///
    /// @param blockUntilDone boolean value indicating if the method should
    /// block until all tasks are done or not. If true, the method will
    /// call [CompletableFuture#join()] on each submitted task.
    /// @param blockTimeoutMillis long value indicating the timeout in
    /// milliseconds for each task to complete. Internally,
    /// [CompletableFuture#orTimeout(long, TimeUnit)] will be used on each
    /// task with the supplied timeout value, unit is always
    /// [TimeUnit#MILLISECONDS].
    /// @param throwOnExceptionalCompletion boolean value indicating if the
    /// method should throw an exception if any of the tasks completes
    /// exceptionally. This includes timeouts as well. If true, the method
    /// will throw a [RuntimeException] with the cause. This will be done
    /// for the first encounter of an exceptional completion, given that tasks
    /// are started in the order they were submitted and joined in the same\
    /// order.
    /// @return List of [CompletableFuture] objects, each representing a
    /// submitted task. The list will contain the same number of futures as the
    /// number of tasks submitted to the executor.
    public List<CompletableFuture<Void>> executeAsync(
            final boolean blockUntilDone,
            final long blockTimeoutMillis,
            final boolean throwOnExceptionalCompletion,
            final boolean logOnExceptionalCompletion,
            final Supplier<ExecutorService> executorServiceSupplier) {
        if (blockTimeoutMillis <= 0) {
            throw new IllegalArgumentException("Timeout per task must be greater than 0");
        } else {
            final List<CompletableFuture<Void>> futures = new ArrayList<>();
            final ExecutorService pool = executorServiceSupplier.get();
            asyncExecutors.add(pool);
            moveScheduledToWorkQueue();
            if (!workQueue.isEmpty()) {
                while (!workQueue.isEmpty()) {
                    futures.add(CompletableFuture.runAsync(workQueue.poll(), pool)
                            .orTimeout(blockTimeoutMillis, TimeUnit.MILLISECONDS));
                }
            }
            if (blockUntilDone) {
                try {
                    // If we block until done, we will wait for all tasks to complete.
                    // If any timeout, an exception will be thrown, which will either be propagated or logged
                    // depending on the throwOnExceptionalCompletion parameter.
                    final CompletableFuture<?>[] arr = futures.toArray(new CompletableFuture[0]);
                    CompletableFuture.allOf(arr).join();
                } catch (final Exception e) {
                    // if any task completed exceptionally, we will either throw an exception or log it
                    final String message = "Exception occurred while running task in [%s]: %s"
                            .formatted(getClass().getCanonicalName(), e.getMessage());
                    if (throwOnExceptionalCompletion) {
                        // Throw an exception if configured to do so
                        throw new RuntimeException(message, e);
                    } else if (logOnExceptionalCompletion) {
                        // Log if configured to do so
                        LOGGER.log(Logger.Level.ERROR, message, e);
                    }
                }
            }
            return futures;
        }
    }

    /// Move all current scheduled tasks to the work queue for execution.
    /// Any non-recurring tasks are then removed from the work map, but
    /// recurring tasks remain to be executed again on the next cycle.
    ///
    /// For repeating schedules the raw Runnable is enqueued directly rather than
    /// the ScheduledTask (FutureTask) wrapper, because FutureTask.run() is a
    /// one-shot operation and silently becomes a no-op after its first execution.
    private void moveScheduledToWorkQueue() {
        List<Schedule> toRemove = new LinkedList<>();
        for (Schedule entry : scheduleQueue) {
            if (entry.repeat()) {
                // Guard against cancelled futures (e.g. plugin called scheduledFuture.cancel())
                if (!entry.task().isCancelled()) {
                    workQueue.add(entry.command());
                }
            } else {
                workQueue.add(entry.task());
                toRemove.add(entry);
            }
        }
        for (Schedule next : toRemove) {
            scheduleQueue.remove(next);
        }
    }

    // Override the schedule methods to ensure that we aren't waiting for schedules
    // We will just run the scheduled tasks whenever the test is ready to do
    // so using executeSerially or executeAsync.

    @Override
    public ScheduledFuture<?> schedule(final Runnable command, final long delay, final TimeUnit unit) {
        ScheduledTask<Void> item = new ScheduledTask<>(command, null, delay, unit);
        final Schedule schedule = new Schedule(item, null, unit, delay, 0, false);
        scheduleQueue.add(schedule);
        return item;
    }

    @Override
    public <V> ScheduledFuture<V> schedule(final Callable<V> callable, final long delay, final TimeUnit unit) {
        ScheduledTask<V> item = new ScheduledTask<>(callable, delay, unit);
        final Schedule schedule = new Schedule(item, null, unit, delay, 0, false);
        scheduleQueue.add(schedule);
        return item;
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(
            final Runnable command, final long initialDelay, final long period, final TimeUnit unit) {
        ScheduledTask<Void> item = new ScheduledTask<>(command, null, initialDelay, unit);
        final Schedule schedule = new Schedule(item, command, unit, initialDelay, period, true);
        scheduleQueue.add(schedule);
        return item;
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(
            final Runnable command, final long initialDelay, final long delay, final TimeUnit unit) {
        ScheduledTask<Void> item = new ScheduledTask<>(command, null, delay, unit);
        final Schedule schedule = new Schedule(item, command, unit, initialDelay, delay, true);
        scheduleQueue.add(schedule);
        return item;
    }

    private static record Schedule(
            ScheduledTask<?> task, Runnable command, TimeUnit unit, long delay, long period, boolean repeat) {}

    private static final class FutureRunnable implements Runnable {
        private final RunnableFuture<?> innerRunnable;

        FutureRunnable(final RunnableFuture<?> futureToRun) {
            innerRunnable = futureToRun;
        }

        public void run() {
            try {
                // run the task
                innerRunnable.run();
                // get, but with a timeout to ensure we do not block forever
                innerRunnable.get(RUNNABLE_FUTURE_MAX_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            } catch (final ExecutionException e) {
                // this is expected to happen, if we throw here, this means
                // that the task has thrown an exception during execution.
                throw new RuntimeException(e);
            } catch (final TimeoutException e) {
                // this is not expected to happen, if we throw here, this means
                // that either the timeout is too low, or the task is
                // potentially blocking forever.
                final String message =
                        "A task that executed in the %s took more than the allowed time of %d seconds. It is possible that the task is blocking forever."
                                .formatted(getClass().getName(), RUNNABLE_FUTURE_MAX_TIMEOUT_SECONDS);
                throw new RuntimeException(message, e);
            }
        }
    }

    private static final class ScheduledTask<V> extends FutureTask<V> implements ScheduledFuture<V> {
        private final long delay;
        private final TimeUnit unit;

        private ScheduledTask(final Callable<V> callable, long delay, TimeUnit unit) {
            super(callable);
            this.delay = delay;
            this.unit = unit;
        }

        private ScheduledTask(final Runnable runnable, final V result, long delay, TimeUnit unit) {
            super(runnable, result);
            this.delay = delay;
            this.unit = unit;
        }

        @Override
        public long getDelay(final TimeUnit unit) {
            return unit.convert(delay, this.unit);
        }

        @Override
        public int compareTo(final Delayed o) {
            long otherDelay = o.getDelay(this.unit);
            if (delay != otherDelay) {
                return delay > otherDelay ? 1 : -1;
            } else {
                return 0;
            }
        }
    }
}
