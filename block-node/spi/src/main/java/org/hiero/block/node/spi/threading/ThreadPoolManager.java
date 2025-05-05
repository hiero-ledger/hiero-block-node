// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.threading;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

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
     * Factory method. Creates a new {@link ExecutorService}.
     *
     * @param corePoolSize the core pool size of the thread pool, must be
     * greater than 0 and less than or equal to maxPoolSize
     * @param maxPoolSize the maximum pool size of the thread pool, must be
     * greater 0 and greater than or equal to corePoolSize
     * @param keepAliveTime the time to keep the threads alive when idle, must
     * be positive or 0 (zero)
     * @param timeUnit the time unit of the keep alive time, must not be null
     * @param workQueue the work queue to use for the thread pool, must not be
     * null
     * @param threadFactory the thread factory to use for the thread pool, must
     * not be null
     * @return a new instance of the BlockNodeContext record
     */
    @NonNull
    ExecutorService createExecutorService(
            int corePoolSize,
            int maxPoolSize,
            long keepAliveTime,
            @NonNull final TimeUnit timeUnit,
            @NonNull final BlockingQueue<Runnable> workQueue,
            @NonNull final ThreadFactory threadFactory);
}
