// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.fixtures.async;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.lang.Thread.UncaughtExceptionHandler;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import org.hiero.block.node.spi.threading.ThreadPoolManager;

/**
 * A very simplified version of the {@link ThreadPoolManager} that is used only
 * for testing. This class will return the same executor service that was passed
 * to it in the constructor. This is useful for testing purposes where we want
 * to control the executor service that is used in the tests.
 *
 * @param <T> the type of executor service
 */
public class TestThreadPoolManager<T extends ExecutorService> implements ThreadPoolManager {
    private final T executor;

    public TestThreadPoolManager(@NonNull T executor) {
        this.executor = Objects.requireNonNull(executor);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Test implementation, always returns the same executor service that was
     * passed to the constructor of this class.
     *
     * @return the executor service that was passed to the constructor
     */
    @NonNull
    @Override
    public T getVirtualThreadExecutor(
            @Nullable final String threadName, @Nullable final UncaughtExceptionHandler uncaughtExceptionHandler) {
        return executor;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Test implementation, always returns the same executor service that was
     * passed to the constructor of this class.
     *
     * @return the executor service that was passed to the constructor
     */
    @NonNull
    @Override
    public T createSingleThreadExecutor(
            @Nullable final String threadName,
            @Nullable final Thread.UncaughtExceptionHandler uncaughtExceptionHandler) {
        return executor;
    }

    @NonNull
    public final T executor() {
        return executor;
    }

    public void shutdownNow() {
        executor.shutdownNow();
    }
}
