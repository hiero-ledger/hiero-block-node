// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.lang.Thread.Builder.OfPlatform;
import java.lang.Thread.UncaughtExceptionHandler;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.hiero.block.common.utils.Preconditions;
import org.hiero.block.node.spi.threading.ThreadPoolManager;

/**
 * The default implementation of the {@link ThreadPoolManager} interface. This
 * implementation is used systemwide to manage the thread pools.
 */
final class DefaultThreadPoolManager implements ThreadPoolManager {
    /** Name prefix for threads of executors created without a thread name. */
    private static final String UNNAMED_THREAD_PREFIX = "block-node-task-";

    private static final ExecutorService DEFAULT_THREAD_PER_TASK_EXECUTOR =
            createThreadPerTaskExecutor(UNNAMED_THREAD_PREFIX, null);
    private static final Map<UncaughtExceptionHandler, ExecutorService> HANDLER_TO_EXECUTOR = new ConcurrentHashMap<>();

    /**
     * {@inheritDoc}
     * <p>
     * This implementation caches executors based on the exception handler, and
     * tries to return the same thread-per-task executor for each exception
     * handler instance. If threadName is not null, however, a unique executor
     * service is returned.
     *
     * If no exception handler or name is provided, this returns the same static
     * executor as {@link #getThreadPerTaskExecutor()}.
     */
    @NonNull
    @Override
    public ExecutorService getThreadPerTaskExecutor(
            @Nullable final String threadName, @Nullable final UncaughtExceptionHandler uncaughtExceptionHandler) {
        if (threadName != null) {
            Preconditions.requireNotBlank(threadName);
            return createThreadPerTaskExecutor(threadName, uncaughtExceptionHandler);
        } else if (uncaughtExceptionHandler != null) {
            return HANDLER_TO_EXECUTOR.computeIfAbsent(
                    uncaughtExceptionHandler, handler -> createThreadPerTaskExecutor(UNNAMED_THREAD_PREFIX, handler));
        } else {
            return DEFAULT_THREAD_PER_TASK_EXECUTOR;
        }
    }

    /**
     * Creates a new thread-per-task executor that runs each task on a new daemon platform thread.
     *
     * @param threadNamePrefix the thread name prefix, a counter is appended
     * @param uncaughtExceptionHandler the uncaught exception handler, may be null
     * @return a new thread-per-task executor service
     */
    private static ExecutorService createThreadPerTaskExecutor(
            @NonNull final String threadNamePrefix, @Nullable final UncaughtExceptionHandler uncaughtExceptionHandler) {
        return Executors.newThreadPerTaskExecutor(daemonThreadFactory(threadNamePrefix, uncaughtExceptionHandler));
    }

    /**
     * Creates a factory for daemon platform threads, so that they never keep the JVM alive on shutdown.
     *
     * @param threadNamePrefix the thread name prefix, a counter is appended
     * @param uncaughtExceptionHandler the uncaught exception handler, may be null
     * @return a new thread factory
     */
    private static ThreadFactory daemonThreadFactory(
            @NonNull final String threadNamePrefix, @Nullable final UncaughtExceptionHandler uncaughtExceptionHandler) {
        final OfPlatform factoryBuilder = Thread.ofPlatform().daemon(true).name(threadNamePrefix, 0);
        if (uncaughtExceptionHandler != null) {
            factoryBuilder.uncaughtExceptionHandler(uncaughtExceptionHandler);
        }
        return factoryBuilder.factory();
    }

    /**
     * {@inheritDoc}
     */
    @NonNull
    @Override
    public ExecutorService createSingleThreadExecutor(
            @Nullable final String threadName,
            @Nullable final Thread.UncaughtExceptionHandler uncaughtExceptionHandler) {
        final OfPlatform factoryBuilder = Thread.ofPlatform();
        if (threadName != null) {
            Preconditions.requireNotBlank(threadName);
            factoryBuilder.name(threadName);
        }
        if (uncaughtExceptionHandler != null) {
            factoryBuilder.uncaughtExceptionHandler(uncaughtExceptionHandler);
        }
        final ThreadFactory factory = factoryBuilder.factory();
        return new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(), factory);
    }

    /**
     * {@inheritDoc}
     */
    @NonNull
    @Override
    public ScheduledExecutorService createScheduledExecutor(
            int corePoolSize,
            @Nullable final String threadName,
            @Nullable final Thread.UncaughtExceptionHandler uncaughtExceptionHandler) {
        if (threadName != null) {
            Preconditions.requireNotBlank(threadName);
        }
        final String threadNamePrefix = threadName == null ? UNNAMED_THREAD_PREFIX : threadName;
        return Executors.newScheduledThreadPool(
                corePoolSize, daemonThreadFactory(threadNamePrefix, uncaughtExceptionHandler));
    }

    /**
     * {@inheritDoc}
     */
    @NonNull
    @Override
    public ScheduledExecutorService createSingleThreadScheduledExecutor(
            @NonNull final String threadName, @NonNull final Thread.UncaughtExceptionHandler uncaughtExceptionHandler) {
        Preconditions.requireNotBlank(threadName);
        final OfPlatform factoryBuilder = Thread.ofPlatform();
        factoryBuilder.name(threadName);
        factoryBuilder.uncaughtExceptionHandler(uncaughtExceptionHandler);
        return Executors.newSingleThreadScheduledExecutor(factoryBuilder.factory());
    }
}
