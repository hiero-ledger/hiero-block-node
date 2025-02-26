// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.server.persistence.storage.write;

import static java.lang.System.Logger.Level.TRACE;
import static java.util.Objects.requireNonNull;

import com.hedera.block.server.persistence.storage.PersistenceStorageConfig;
import com.hedera.block.server.persistence.storage.PersistenceStorageConfig.ExecutorType;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Factory for creating executor instances for async writers based on configuration.
 * <p>
 * This factory creates different types of executors with specific configurations
 * to optimize performance for different workloads and deployment environments.
 * <p>
 * The factory supports creating:
 * <ul>
 *   <li>Thread pool executors with configurable thread count and queue size
 *   <li>Single-threaded executors for sequential processing
 *   <li>Fork-join pool executors for work-stealing parallelism
 *   <li>Direct executors that run tasks in the calling thread
 * </ul>
 */
public final class AsyncWriterExecutorFactory {
    private static final System.Logger LOGGER = System.getLogger(AsyncWriterExecutorFactory.class.getName());

    // Private constructor to prevent instantiation
    private AsyncWriterExecutorFactory() {}

    /**
     * Creates an executor based on the provided configuration.
     * <p>
     * This method creates the appropriate executor type with the specified parameters
     * depending on the configuration.
     *
     * @param config the persistence storage configuration containing executor settings
     * @return an executor configured according to the settings
     * @throws NullPointerException if config is null
     */
    @NonNull
    public static Executor createExecutor(@NonNull final PersistenceStorageConfig config) {
        requireNonNull(config);

        LOGGER.log(TRACE, "Creating async writer executor of type: %s".formatted(config.executorType()));
        final ExecutorType executorType = config.executorType();
        return switch (executorType) {
            case THREAD_POOL -> createThreadPoolExecutor(config);
            case SINGLE_THREAD -> createSingleThreadExecutor();
            case FORK_JOIN -> createForkJoinExecutor();
            case CALLING_THREAD -> createDirectExecutor();
        };
    }

    /**
     * Creates a thread pool executor with the specified configuration.
     * <p>
     * The thread pool uses:
     * <ul>
     *   <li>Fixed number of threads as specified in the configuration
     *   <li>Custom thread factory that creates named daemon threads
     *   <li>Configurable keep alive time for threads
     *   <li>Bounded queue with the specified capacity
     *   <li>Custom rejection handler that implements a caller-runs policy
     * </ul>
     * <p>
     * If virtual threads are enabled it creates a virtual
     * thread per task executor instead.
     *
     * @param config the configuration containing thread pool settings
     * @return a configured thread pool executor
     */
    @NonNull
    private static Executor createThreadPoolExecutor(@NonNull final PersistenceStorageConfig config) {
        if (config.useVirtualThreads()) {
            LOGGER.log(TRACE, "Creating virtual thread per task executor");
            return Executors.newVirtualThreadPerTaskExecutor();
        }

        final int threadCount = config.threadCount();
        final long threadKeepAliveTime = config.threadKeepAliveTime();
        final int queueLimit = config.executionQueueLimit();

        LOGGER.log(
                TRACE,
                "Creating thread pool executor with %d threads and queue limit of %d"
                        .formatted(threadCount, queueLimit));

        return new ThreadPoolExecutor(
                threadCount,
                threadCount,
                threadKeepAliveTime,
                TimeUnit.SECONDS,
                new LinkedBlockingDeque<>(queueLimit),
                new AsyncWriterThreadFactory(),
                new AsyncWriterRejectedExecutionHandler());
    }

    /**
     * Creates a single-threaded executor.
     * <p>
     * This executor processes tasks sequentially using a single worker thread.
     *
     * @return a single-threaded executor
     */
    @NonNull
    private static ExecutorService createSingleThreadExecutor() {
        LOGGER.log(TRACE, "Creating single thread executor");
        return Executors.newSingleThreadExecutor(new AsyncWriterThreadFactory());
    }

    /**
     * Creates a fork-join pool executor.
     * <p>
     *     This method returns the common ForkJoinPool, which uses a work-stealing
     *     algorithm for efficient load balancing across available processors.
     *
     * @return the common fork-join pool
     */
    @NonNull
    private static ForkJoinPool createForkJoinExecutor() {
        final ForkJoinPool forkJoinPool = ForkJoinPool.commonPool();
        LOGGER.log(
                TRACE,
                "Using common fork-join pool with parallelism level: %d".formatted(forkJoinPool.getParallelism()));
        return forkJoinPool;
    }

    /**
     * Creates a direct executor that runs tasks in the calling thread.
     * <p>
     *     This executor does not create any additional threads and executes
     *     tasks synchronously in the thread that submits them.
     * @return a direct executor
     */
    @NonNull
    private static Executor createDirectExecutor() {
        LOGGER.log(TRACE, "Creating a direct executor (tasks run in the calling thread)");
        return Runnable::run;
    }

    private static class AsyncWriterThreadFactory implements ThreadFactory {
        private final AtomicInteger threadNumber = new AtomicInteger(1);

        @Override
        public Thread newThread(@NonNull final Runnable r) {
            return new Thread(r, "async-writer-" + threadNumber.getAndIncrement());
        }
    }
}
