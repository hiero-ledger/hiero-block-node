// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.persistence.storage.write;

import static java.lang.System.Logger.Level.TRACE;
import static java.util.Objects.requireNonNull;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.hiero.block.server.persistence.storage.PersistenceStorageConfig;
import org.hiero.block.server.persistence.storage.PersistenceStorageConfig.ExecutorType;

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
 * </ul>
 */
public final class AsyncWriterExecutorFactory {
    private static final System.Logger LOGGER = System.getLogger(AsyncWriterExecutorFactory.class.getName());
    private static final int BASE_THREADS = 4;

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

        LOGGER.log(TRACE, "Creating async writer executor of type: {0}", config.executorType());
        final ExecutorType executorType = config.executorType();
        return switch (executorType) {
            case THREAD_POOL -> createThreadPoolExecutor(config);
            case SINGLE_THREAD -> createSingleThreadExecutor();
            case FORK_JOIN -> createForkJoinExecutor(config);
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
     *
     * @param config the configuration containing thread pool settings
     * @return a configured thread pool executor
     */
    @NonNull
    private static Executor createThreadPoolExecutor(@NonNull final PersistenceStorageConfig config) {
        final int threadCount = config.threadCount();
        final long threadKeepAliveTime = config.threadKeepAliveTime();
        final int queueLimit = config.executionQueueLimit();
        final boolean useVirtualThreads = config.useVirtualThreads();

        LOGGER.log(
                TRACE,
                "Creating thread pool executor with {0} threads and queue limit of {1}",
                threadCount,
                queueLimit);

        return new ThreadPoolExecutor(
                threadCount,
                threadCount,
                threadKeepAliveTime,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingDeque<>(queueLimit),
                new AsyncWriterThreadFactory(useVirtualThreads),
                new CallerRunsPolicy());
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
        return Executors.newSingleThreadExecutor(new AsyncWriterThreadFactory(false));
    }

    /**
     * Creates a fork-join pool executor.
     * <p>
     * This method creates a dedicated ForkJoinPool with configurable parallelism and thread limits.
     * The maximum number of threads is calculated based on the thread count configuration plus a base value.
     *
     * @param config the configuration containing executor settings
     * @return a configured fork-join pool
     */
    @NonNull
    private static ForkJoinPool createForkJoinExecutor(@NonNull final PersistenceStorageConfig config) {
        final int threadCount = config.threadCount();
        final long threadKeepAliveTime = config.threadKeepAliveTime();
        int maxThreads = threadCount + BASE_THREADS;

        LOGGER.log(TRACE, "Creating fork-join pool with parallelism: {0}, max threads: {1}", threadCount, maxThreads);

        return new ForkJoinPool(
                threadCount,
                ForkJoinPool.defaultForkJoinWorkerThreadFactory,
                null,
                true,
                maxThreads,
                maxThreads,
                0,
                null,
                threadKeepAliveTime,
                TimeUnit.MILLISECONDS);
    }

    private static class AsyncWriterThreadFactory implements ThreadFactory {
        private final AtomicInteger threadNumber = new AtomicInteger(1);
        private final boolean useVirtualThreads;

        public AsyncWriterThreadFactory(final boolean useVirtualThreads) {
            this.useVirtualThreads = useVirtualThreads;
        }

        @Override
        public Thread newThread(@NonNull final Runnable r) {
            if (useVirtualThreads) {
                return Thread.ofVirtual()
                        .name("async-writer-" + threadNumber.getAndIncrement())
                        .unstarted(r);
            }
            return new Thread(r, "async-writer-" + threadNumber.getAndIncrement());
        }
    }
}
