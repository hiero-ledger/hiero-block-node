// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.fixtures.async;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.lang.System.Logger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

/**
 * A very simple executor to be used only for testing! This executor has the
 * ability to block the caller and execute tasks until completion either
 * on the caller thread, or on a single thread per task. It will
 * collect all submitted tasks to it and will hold them in order. After that
 * when the {@link #executeSerially()} method is called, all tasks will be
 * executed in order serially on the same thread that called the method. This
 * will ensure that all tasks will complete before doing any asserts. This pool
 * can also execute tasks asynchronously using any of the
 * {@link #executeAsync()} methods. The tasks will be executed in the
 * order they were submitted to the pool, and will be executed using a
 * {@link java.util.concurrent.ThreadPerTaskExecutor} internally. There are
 * options to join the tasks, effectively blocking until all tasks are completed.
 */
public class BlockingExecutor extends ThreadPoolExecutor {
    private static final Logger LOGGER = System.getLogger(BlockingExecutor.class.getName());
    private static final long TASK_TIMEOUT_MILLIS = 5_000L;
    private static final long RUNNABLE_FUTURE_MAX_TIMEOUT_SECONDS = 60L;
    /** The work queue that will be used to hold the tasks. */
    private final BlockingQueue<Runnable> workQueue;
    /** Counter to indicate total submitted tasks. */
    private int tasksSubmitted;
    /// All async executors created when running tasks async
    private final Queue<ExecutorService> asyncExecutors;

    /**
     * Constructor.
     */
    public BlockingExecutor(@NonNull final BlockingQueue<Runnable> workQueue) {
        // supply super with arbitrary values, they will not be used
        super(
                1,
                1,
                0L,
                TimeUnit.MILLISECONDS,
                Objects.requireNonNull(workQueue), // super will not use this queue
                Executors.defaultThreadFactory(),
                new AbortPolicy());
        this.workQueue = workQueue; // actual work queue
        this.asyncExecutors = new ConcurrentLinkedQueue<>();
    }

    /**
     * Overriding this method in order to make sure this logic will be called
     * every time we will submit a task to the pool.
     */
    @Override
    @SuppressWarnings("all")
    public void execute(@NonNull final Runnable command) {
        if (command instanceof RunnableFuture<?> ft) {
            // Wrap the FutureTask in a Runnable that will call get() on it
            // otherwise when executing async below, we will not get an exception
            // since the FutureTask will capture it. Super's submit() methods
            // wrap the task as a FutureTask.
            workQueue.offer(() -> {
                try {
                    // run the task
                    ft.run();
                    // get, but with a timeout to ensure we do not block forever
                    ft.get(RUNNABLE_FUTURE_MAX_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                } catch (final ExecutionException e) {
                    // this is expected to happen, if we throw here, this means
                    // that the task has throw an exception during execution.
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
            });
        } else {
            workQueue.offer(command);
        }
        tasksSubmitted++;
    }

    /**
     * Invoke this method once you have submitted all the tasks that need to be
     * run by this executor. This method will then proceed to poll each task
     * (in the same order that they have been submitted) and will execute the
     * tasks serially on the same thread that called this method. Once this
     * method returns (or throws), we are certain that all tasks have run and
     * can safely assert based on that. This method will throw an
     * {@link IllegalStateException} to indicate a broken state, when the queue
     * is empty.
     */
    public void executeSerially() {
        if (workQueue.isEmpty()) {
            throw new IllegalStateException("Queue is empty");
        } else {
            while (!workQueue.isEmpty()) {
                workQueue.poll().run();
            }
        }
    }

    /**
     * This method executes all tasks that were submitted to this executor
     * asynchronously.
     * <p>
     * Timeout is {@value TASK_TIMEOUT_MILLIS} milliseconds, blocking is enabled,
     * and exceptions will be thrown on exceptional completion.
     * Logging on exceptional completion is disabled.
     * @see #executeAsync(boolean, long, boolean, boolean, Supplier)
     */
    public List<CompletableFuture<Void>> executeAsync() {
        return executeAsync(true);
    }

    /**
     * This method executes all tasks that were submitted to this executor
     * asynchronously.
     * <p>
     * Timeout is {@value TASK_TIMEOUT_MILLIS} milliseconds and blocking is
     * enabled. Logging on exceptional completion is disabled.
     * @see #executeAsync(boolean, long, boolean, boolean, Supplier)
     */
    public List<CompletableFuture<Void>> executeAsync(final boolean throwOnExceptionalCompletion) {
        return executeAsync(TASK_TIMEOUT_MILLIS, throwOnExceptionalCompletion);
    }

    /**
     * This method executes all tasks that were submitted to this executor
     * asynchronously.
     * <p>
     * Blocking is enabled and exceptions will be thrown on exceptional
     * completion. Logging on exceptional completion is disabled.
     * @see #executeAsync(boolean, long, boolean, boolean, Supplier)
     */
    public List<CompletableFuture<Void>> executeAsync(final long timeoutMillis) {
        return executeAsync(timeoutMillis, true);
    }

    /**
     * This method executes all tasks that were submitted to this executor
     * asynchronously.
     * <p>
     * Blocking is enabled. Logging on exceptional completion is disabled.
     * @see #executeAsync(boolean, long, boolean, boolean, Supplier)
     */
    public List<CompletableFuture<Void>> executeAsync(
            final long blockTimeoutMillis, final boolean throwOnExceptionalCompletion) {
        return executeAsync(blockTimeoutMillis, throwOnExceptionalCompletion, false);
    }

    /**
     * This method executes all tasks that were submitted to this executor
     * asynchronously.
     * <p>
     * Blocking is enabled.
     * @see #executeAsync(boolean, long, boolean, boolean, Supplier)
     */
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

    /**
     * This method executes all tasks that were submitted to this executor
     * asynchronously.
     * <p>
     * Default pool implementation used.
     * @see #executeAsync(boolean, long, boolean, boolean, Supplier)
     */
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

    /**
     * This method executes all tasks that were submitted to this executor
     * asynchronously.
     * <p>
     * Internally, a {@link java.util.concurrent.ThreadPerTaskExecutor} is used
     * to submit each task. All tasks are started in the order they were
     * submitted to the executor. The way tasks are submitted is by
     * calling {@link CompletableFuture#runAsync(Runnable, java.util.concurrent.Executor)}
     * on each task in the internal worker queue. Depending on the parameters
     * described below, this method will either block or not, throw an
     * exception or not, log an exceptional execution or not, and will use a
     * timeout for each task or not.
     *
     * @param blockUntilDone boolean value indicating if the method should
     * block until all tasks are done or not. If true, the method will
     * call {@link CompletableFuture#join()} on each submitted task.
     * @param blockTimeoutMillis long value indicating the timeout in
     * milliseconds for each task to complete. Internally,
     * {@link CompletableFuture#orTimeout(long, TimeUnit)} will be used on each
     * task with the supplied timeout value, unit is always
     * {@link TimeUnit#MILLISECONDS}.
     * @param throwOnExceptionalCompletion boolean value indicating if the
     * method should throw an exception if any of the tasks completes
     * exceptionally. This includes timeouts as well. If true, the method
     * will throw a {@link RuntimeException} with the cause. This will be done
     * for the first encounter of an exceptional completion, given that tasks
     * are started in the order they were submitted and joined in the same\
     * order.
     * @return List of {@link CompletableFuture} objects, each representing a
     * submitted task. The list will contain the same number of futures as the
     * number of tasks submitted to the executor.
     */
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
            if (workQueue.isEmpty()) {
                throw new IllegalStateException("Queue is empty");
            } else {
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

    /**
     * This method indicates if any task was ever submitted to this executor.
     * This is useful during tests in order to assert that the pool was
     * essentially not interacted with. An example would be if we have a test
     * where we want to assert that some production logic will never submit a
     * task to the executor given some condition, then we can use this method
     * to assert that. This method does not reflect the current state of the
     * queue, meaning the queue might be empty due to a call to the
     * {@link #executeSerially()} method, but this method will still return
     * true if any task was submitted before that.
     *
     * @return boolean value, true if any task was ever submitted, false
     * otherwise
     */
    public boolean wasAnyTaskSubmitted() {
        return tasksSubmitted > 0;
    }

    @Override
    public void close() {
        shutdownNow();
    }

    @Override
    public void shutdown() {
        shutdownNow();
    }

    @Override
    public boolean awaitTermination(final long timeout, final TimeUnit unit) {
        shutdownNow();
        return true;
    }

    @Override
    public List<Runnable> shutdownNow() {
        // Shutdown now all active running executors for async executions
        asyncExecutors.parallelStream().forEach(ExecutorService::shutdownNow);
        return super.shutdownNow();
    }

    /**
     * Operation currently not supported!
     */
    @Override
    @NonNull
    public <T> T invokeAny(@NonNull final Collection<? extends Callable<T>> tasks) {
        throw new UnsupportedOperationException("Operation not supported, to be extended as needed");
    }

    /**
     * Operation currently not supported!
     */
    @Override
    @NonNull
    public <T> T invokeAny(
            @NonNull final Collection<? extends Callable<T>> tasks, final long timeout, @NonNull final TimeUnit unit) {
        throw new UnsupportedOperationException("Operation not supported, to be extended as needed");
    }

    /**
     * Operation currently not supported!
     */
    @Override
    @NonNull
    public <T> List<Future<T>> invokeAll(@NonNull final Collection<? extends Callable<T>> tasks) {
        throw new UnsupportedOperationException("Operation not supported, to be extended as needed");
    }

    /**
     * Operation currently not supported!
     */
    @Override
    @NonNull
    public <T> List<Future<T>> invokeAll(
            @NonNull final Collection<? extends Callable<T>> tasks, final long timeout, @NonNull final TimeUnit unit) {
        throw new UnsupportedOperationException("Operation not supported, to be extended as needed");
    }
}
