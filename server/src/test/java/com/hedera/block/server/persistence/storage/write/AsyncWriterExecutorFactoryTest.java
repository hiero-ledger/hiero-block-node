// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.server.persistence.storage.write;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hedera.block.server.persistence.storage.PersistenceStorageConfig;
import com.hedera.block.server.persistence.storage.PersistenceStorageConfig.ExecutorType;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ThreadPoolExecutor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Test class that tests the functionality of the
 * {@link AsyncWriterExecutorFactory}
 */
class AsyncWriterExecutorFactoryTest {

    /**
     * This test verifies that the factory throws a NullPointerException when
     * null config is provided.
     */
    @Test
    void testCreateExecutorWithNullConfig() {
        assertThrows(NullPointerException.class, () -> AsyncWriterExecutorFactory.createExecutor(null));
    }

    /**
     * This test verifies that the factory creates the correct executor type
     * based on the configuration.
     *
     * @param executorType the executor type to test
     */
    @ParameterizedTest
    @EnumSource(ExecutorType.class)
    void testCreateExecutorWithDifferentTypes(final ExecutorType executorType) {
        // Given
        final PersistenceStorageConfig config = createConfig(executorType, false, 4, 60L, 100);

        // When
        final Executor executor = AsyncWriterExecutorFactory.createExecutor(config);

        // Then
        assertNotNull(executor);

        switch (executorType) {
            case THREAD_POOL -> assertTrue(executor instanceof ThreadPoolExecutor);
            case SINGLE_THREAD -> assertTrue(executor instanceof ExecutorService);
            case FORK_JOIN -> assertTrue(executor instanceof ForkJoinPool);
        }
    }

    /**
     * This test verifies that the factory creates a virtual thread executor
     * when virtual threads are enabled.
     */
    @Test
    void testCreateThreadPoolExecutorWithVirtualThreads() {
        // Given
        final PersistenceStorageConfig config = createConfig(ExecutorType.THREAD_POOL, true, 4, 60L, 100);

        // When
        final Executor executor = AsyncWriterExecutorFactory.createExecutor(config);

        // Then
        assertNotNull(executor);
        System.out.println(executor.getClass().getName());
        // Virtual thread executor invokes underneath ThreadPerTaskExecutor with specific factory for virtual
        assertTrue(executor.getClass().getName().contains("ThreadPerTaskExecutor"));
    }

    /**
     * This test verifies that the factory creates a thread pool executor with
     * the correct configuration when virtual threads are disabled.
     */
    @Test
    void testCreateThreadPoolExecutorWithPlatformThreads() {
        // Given
        final int threadCount = 6;
        final long keepAliveTime = 120L;
        final int queueLimit = 200;
        final PersistenceStorageConfig config =
                createConfig(ExecutorType.THREAD_POOL, false, threadCount, keepAliveTime, queueLimit);

        // When
        final Executor executor = AsyncWriterExecutorFactory.createExecutor(config);

        // Then
        assertNotNull(executor);
        assertTrue(executor instanceof ThreadPoolExecutor);

        final ThreadPoolExecutor threadPoolExecutor = (ThreadPoolExecutor) executor;
        assertTrue(threadPoolExecutor.getCorePoolSize() == threadCount);
        assertTrue(threadPoolExecutor.getMaximumPoolSize() == threadCount);
        assertTrue(threadPoolExecutor.getKeepAliveTime(java.util.concurrent.TimeUnit.SECONDS) == keepAliveTime);
        assertTrue(threadPoolExecutor.getQueue().remainingCapacity() == queueLimit);
        assertTrue(threadPoolExecutor.getRejectedExecutionHandler() instanceof ThreadPoolExecutor.CallerRunsPolicy);
    }

    /**
     * Creates a test configuration with the specified parameters.
     */
    private PersistenceStorageConfig createConfig(
            final ExecutorType executorType,
            final boolean useVirtualThreads,
            final int threadCount,
            final long threadKeepAliveTime,
            final int executionQueueLimit) {
        return new PersistenceStorageConfig(
                java.nio.file.Path.of(""),
                java.nio.file.Path.of(""),
                PersistenceStorageConfig.StorageType.BLOCK_AS_LOCAL_FILE,
                PersistenceStorageConfig.CompressionType.NONE,
                3, // Default compression level
                1000, // Default archive batch size
                executionQueueLimit,
                executorType,
                threadCount,
                threadKeepAliveTime,
                useVirtualThreads);
    }
}
