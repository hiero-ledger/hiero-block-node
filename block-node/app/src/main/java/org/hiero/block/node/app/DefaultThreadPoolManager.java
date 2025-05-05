// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.hiero.block.node.spi.threading.ThreadPoolManager;

public final class DefaultThreadPoolManager implements ThreadPoolManager {
    /**
     * {@inheritDoc}
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
        return new ThreadPoolExecutor(corePoolSize, maxPoolSize, keepAliveTime, timeUnit, workQueue, threadFactory);
    }
}
