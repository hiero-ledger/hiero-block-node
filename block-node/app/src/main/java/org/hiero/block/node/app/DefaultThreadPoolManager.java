// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.lang.Thread.Builder.OfPlatform;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.hiero.block.common.utils.Preconditions;
import org.hiero.block.node.spi.threading.ThreadPoolManager;

public final class DefaultThreadPoolManager implements ThreadPoolManager {
    /**
     * {@inheritDoc}
     */
    @NonNull
    @Override
    public ExecutorService createSingleThreadExecutor(@NonNull final String threadName) {
        return createSingleThreadExecutor(threadName, null);
    }

    /**
     * {@inheritDoc}
     */
    @NonNull
    @Override
    public ExecutorService createSingleThreadExecutor(
            @NonNull final String threadName,
            @Nullable final Thread.UncaughtExceptionHandler uncaughtExceptionHandler) {
        Preconditions.requireNotBlank(threadName);
        final OfPlatform factoryBuilder = Thread.ofPlatform().name(threadName);
        if (uncaughtExceptionHandler != null) {
            factoryBuilder.uncaughtExceptionHandler(uncaughtExceptionHandler);
        }
        final ThreadFactory factory = factoryBuilder.factory();
        return new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(), factory);
    }
}
