// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.fixtures.async;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import org.hiero.block.node.spi.threading.ThreadPoolManager;

/**
 * A very simplified version of the {@link ThreadPoolManager} that is used only for testing. This class will return the
 * same executor service that was passed to it in the constructor. This is useful for testing purposes where we want to
 * control the executor service that is used in the tests.
 *
 * @param <T> the type of executor service
 */
public record TestThreadPoolManager<T extends ExecutorService>(@NonNull T executor) implements ThreadPoolManager {
    public TestThreadPoolManager {
        Objects.requireNonNull(executor);
    }

    /**
     * Test implementation, always returns the same executor service that was passed to the constructor of this class.
     *
     * @return the executor service that was passed to the constructor
     */
    @NonNull
    @Override
    public ExecutorService createExecutorService(
            final int corePoolSize,
            final int maxPoolSize,
            final long keepAliveTime,
            @NonNull final TimeUnit timeUnit,
            @NonNull final BlockingQueue<Runnable> workQueue,
            @NonNull final ThreadFactory threadFactory) {
        return executor;
    }
}
