// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.threading;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

/**
 * An interface that defines a manager for creating and managing thread pools
 * across the system. This should be the preferred way to create thread pools
 * for any given plugin. This allows for the block node to control the threading
 * across the entire system. It also allows for easy testing of the system where
 * thread pools are utilized, because the manager can be mocked or stubbed. In
 * this way, the system can be tested with less special "test-only" logic in
 * the plugins themselves.
 */
public interface ThreadPoolManager {
    /**
     * Factory method. Creates a new virtual thread-per-task {@link ExecutorService}.
     *
     * @return a new virtual thread-per-task executor service
     */
    @NonNull
    default ExecutorService getVirtualThreadExecutor() {
        return getVirtualThreadExecutor(null, null);
    }

    /**
     * Factory method. Creates a new virtual thread-per-task {@link ExecutorService}.
     *
     * Note, the thread name is a debugging aid. Naming threads may prevent
     * caching or reusing executor services, and may result in more overhead.
     *
     * @param threadName the thread name prefix, must not be blank
     * @return a new virtual thread-per-task executor service
     */
    @NonNull
    default ExecutorService getVirtualThreadExecutor(@NonNull final String threadName) {
        return getVirtualThreadExecutor(Objects.requireNonNull(threadName), null);
    }

    /**
     * Factory method. Creates a new virtual thread-per-task {@link ExecutorService} using
     * the specified (nullable) {@link Thread.UncaughtExceptionHandler}.
     *
     * Note, the thread name is a debugging aid, and optional. Naming threads
     * may prevent caching or reusing executor services, and may result in more
     * overhead.<br/>
     * Some implementations may reuse the same executor service for each
     * uncaught exception handler, but this is not guaranteed.<br/>
     * If both the thread name and the uncaught exception handler are null,
     * the executor service returned will be generic, and <strong>may</strong>
     * be shared among multiple callers.
     *
     * @param threadName the thread name prefix, must not be blank
     * @param uncaughtExceptionHandler the uncaught exception handler, nullable
     * @return a new virtual thread-per-task executor service
     */
    @NonNull
    ExecutorService getVirtualThreadExecutor(
            @Nullable final String threadName,
            @Nullable final Thread.UncaughtExceptionHandler uncaughtExceptionHandler);

    /**
     * Factory method for a single thread executor.
     * Creates a new, unnamed, single thread {@link ExecutorService}.
     *
     * @return a new single thread executor service
     */
    @NonNull
    default ExecutorService createSingleThreadExecutor() {
        return createSingleThreadExecutor(null, null);
    }

    /**
     * Factory method for a single thread executor.
     * Creates a new, named, single thread {@link ExecutorService}.
     *
     * Note, the thread name is a debugging aid. Naming threads may prevent
     * caching or reusing executor services, and may result in more overhead.
     *
     * @param threadName the thread's name, must not be blank
     * @return a new single thread executor service
     */
    @NonNull
    default ExecutorService createSingleThreadExecutor(@NonNull final String threadName) {
        return createSingleThreadExecutor(Objects.requireNonNull(threadName), null);
    }

    /**
     * Factory method for a single thread executor.
     * Creates a new single thread {@link ExecutorService} using the name
     * provided, if not null, and the specified
     * {@link Thread.UncaughtExceptionHandler}, if not null.
     *
     * Note, the thread name is a debugging aid, and optional. Naming threads
     * may prevent caching or reusing executor services, and may result in more
     * overhead.<br/>
     * Some implementations may reuse the same executor service for each
     * uncaught exception handler, but this is not guaranteed.<br/>
     * If both the thread name and the uncaught exception handler are null,
     * the executor service returned will be generic, and <strong>may</strong>
     * be shared among multiple callers.
     *
     * @param threadName the thread's name, must not be blank if filled in.
     * @param uncaughtExceptionHandler the uncaught exception handler, nullable
     * @return a new single thread executor service
     */
    @NonNull
    ExecutorService createSingleThreadExecutor(
            @Nullable final String threadName,
            @Nullable final Thread.UncaughtExceptionHandler uncaughtExceptionHandler);

    @NonNull
    default ScheduledExecutorService createSingleThreadScheduledExecutor() {
        return createSingleThreadScheduledExecutor(null,  null);
    }

    @NonNull
    default ScheduledExecutorService createSingleThreadScheduledExecutor(@NonNull final String threadName) {
        return createSingleThreadScheduledExecutor(Objects.requireNonNull(threadName),  null);
    }

    @NonNull
    ScheduledExecutorService createSingleThreadScheduledExecutor(
            @Nullable final String threadName,
            @Nullable final Thread.UncaughtExceptionHandler uncaughtExceptionHandler);
}
