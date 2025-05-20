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
    private static final ExecutorService DEFAULT_VIRTUAL_THREAD_EXECUTOR = Executors.newVirtualThreadPerTaskExecutor();
    private static final Map<UncaughtExceptionHandler, ExecutorService> VIRTUAL_THREAD_MAP = new ConcurrentHashMap<>();

    /**
     * {@inheritDoc}
     * <p>
     * This implementation caches executors based on the exception handler, and
     * tries to return the same thread-per-task executor specialized to use virtual
     * threads for each exception handler instance. If threadName is not null,
     * however, a unique executor service is returned.
     *
     * If no exception handler or name is provided, this returns the same static
     * executor as {@link #getVirtualThreadExecutor()}.
     */
    @NonNull
    @Override
    public ExecutorService getVirtualThreadExecutor(
            @Nullable final String threadName, @Nullable final UncaughtExceptionHandler uncaughtExceptionHandler) {
        if (threadName != null) {
            Preconditions.requireNotBlank(threadName);
            return createVirtualThreadExecutor(threadName, uncaughtExceptionHandler);
        } else if (uncaughtExceptionHandler != null) {
            if (!VIRTUAL_THREAD_MAP.containsKey(uncaughtExceptionHandler)) {
                final ExecutorService newExecutor = createVirtualThreadExecutor(threadName, uncaughtExceptionHandler);
                VIRTUAL_THREAD_MAP.put(uncaughtExceptionHandler, newExecutor);
            }
            return VIRTUAL_THREAD_MAP.get(uncaughtExceptionHandler);
        } else {
            return DEFAULT_VIRTUAL_THREAD_EXECUTOR;
        }
    }

    /**
     * Creates a new virtual thread-per-task executor specialized to use virtual threads.
     *
     * @param threadName the thread name prefix, may be null
     * @param uncaughtExceptionHandler the uncaught exception handler, may be null
     * @return a new virtual thread-per-task executor service if threadName is not null,
     *         or a cached (based on exception handler instance) executor service otherwise.
     */
    private static ExecutorService createVirtualThreadExecutor(
            @Nullable final String threadName, @Nullable final UncaughtExceptionHandler uncaughtExceptionHandler) {
        Thread.Builder factoryBuilder = Thread.ofVirtual();
        if (uncaughtExceptionHandler != null) {
            factoryBuilder.uncaughtExceptionHandler(uncaughtExceptionHandler);
        }
        if (threadName != null) {
            factoryBuilder.name(threadName, 0);
            return Executors.newThreadPerTaskExecutor(factoryBuilder.factory());
        } else {
            final ThreadFactory factory = factoryBuilder.factory();
            return Executors.newThreadPerTaskExecutor(factory);
        }
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
}
