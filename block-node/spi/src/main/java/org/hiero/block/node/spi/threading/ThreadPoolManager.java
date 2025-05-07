// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.threading;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.concurrent.ExecutorService;

/**
 * An interface that defines a manager for creating and managing thread pools
 * across the system. This should be the preferred way to create thread pools
 * for any given plugin. This allows for the block node to control the threading
 * across the entire system. It also allows for easy testing of the system where
 * thread pools are utilized, because the manager can be mocked or stubbed. In
 * this way, the system can be tested without facilitating any special
 * "test-only" logic in the plugins themselves.
 */
public interface ThreadPoolManager {
    /**
     * Factory method. Creates a new single thread {@link ExecutorService}.
     *
     * @param threadNamePrefix the prefix, must not be blank
     * @return a new single thread executor service
     */
    @NonNull
    ExecutorService createSingleThreadExecutor(@NonNull final String threadNamePrefix);

    /**
     * Factory method. Creates a new single thread {@link ExecutorService} using
     * the specified (nullable) {@link Thread.UncaughtExceptionHandler}.
     *
     * @param threadNamePrefix the prefix, must not be blank
     * @param uncaughtExceptionHandler the uncaught exception handler, nullable
     * @return a new single thread executor service
     */
    @NonNull
    ExecutorService createSingleThreadExecutor(
            @NonNull final String threadNamePrefix,
            @Nullable final Thread.UncaughtExceptionHandler uncaughtExceptionHandler);
}
