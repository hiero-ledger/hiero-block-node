// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.fixtures.async;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * A very simple executor to be used only for testing! This executor will
 * collect all submitted tasks to it and will hold them in order. After that
 * when the {@link #executeSerially()} method is called, all tasks will be
 * executed in order serially on the same thread that called the method. This
 * will ensure that all tasks will complete before doing any asserts
 */
public class BlockingSerialExecutor extends ThreadPoolExecutor {
    /** The work queue that will be used to hold the tasks. */
    private final BlockingQueue<Runnable> workQueue;
    /** Counter to indicate total submitted tasks. */
    private int tasksSubmitted;

    /**
     * Constructor.
     */
    public BlockingSerialExecutor(@NonNull final BlockingQueue<Runnable> workQueue) {
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
    }

    /**
     * Overriding this method in order to make sure this logic will be called
     * every time we will submit a task to the pool.
     */
    @Override
    @SuppressWarnings("all")
    public void execute(@NonNull final Runnable command) {
        workQueue.offer(command);
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
