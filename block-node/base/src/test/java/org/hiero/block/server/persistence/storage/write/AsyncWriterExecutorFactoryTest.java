// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.persistence.storage.write;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.swirlds.config.api.ConfigurationBuilder;
import com.swirlds.config.extensions.sources.SimpleConfigSource;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ThreadPoolExecutor;
import org.hiero.block.server.persistence.storage.PersistenceStorageConfig;
import org.hiero.block.server.persistence.storage.PersistenceStorageConfig.ExecutorType;
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
        final PersistenceStorageConfig config =
                createConfig(Map.of("persistence.storage.executorType", executorType.toString()));

        // When
        final Executor executor = AsyncWriterExecutorFactory.createExecutor(config);

        // Then
        assertNotNull(executor);

        switch (executorType) {
            case THREAD_POOL -> assertThat(executor).isExactlyInstanceOf(ThreadPoolExecutor.class);
            case SINGLE_THREAD -> assertThat(executor).isInstanceOf(ExecutorService.class);
            case FORK_JOIN -> assertThat(executor).isExactlyInstanceOf(ForkJoinPool.class);
        }
    }

    /**
     * This test verifies that the factory creates a virtual thread executor
     * when virtual threads are enabled.
     */
    @Test
    void testCreateThreadPoolExecutorWithVirtualThreads() {
        // Given
        final PersistenceStorageConfig config = createConfig(Map.of(
                "persistence.storage.executorType", "THREAD_POOL", "persistence.storage.useVirtualThreads", "true"));

        // When
        final Executor executor = AsyncWriterExecutorFactory.createExecutor(config);

        // Then
        assertNotNull(executor);
        assertThat(executor).isExactlyInstanceOf(ThreadPoolExecutor.class);

        final ThreadPoolExecutor threadPoolExecutor = (ThreadPoolExecutor) executor;
        assertTrue(threadPoolExecutor.getThreadFactory().newThread(() -> {}).isVirtual());
    }

    /**
     * This test verifies that the factory creates a thread pool executor with
     * the correct configuration when platform threads are used.
     */
    @Test
    void testCreateThreadPoolExecutorWithPlatformThreads() {
        // Given
        final int threadCount = 6;
        final int queueLimit = 200;
        final PersistenceStorageConfig config = createConfig(Map.of(
                "persistence.storage.executorType",
                "THREAD_POOL",
                "persistence.storage.useVirtualThreads",
                "false",
                "persistence.storage.threadCount",
                "6",
                "persistence.storage.executionQueueLimit",
                "200"));

        // When
        final Executor executor = AsyncWriterExecutorFactory.createExecutor(config);

        // Then
        assertNotNull(executor);
        assertThat(executor).isExactlyInstanceOf(ThreadPoolExecutor.class);

        final ThreadPoolExecutor threadPoolExecutor = (ThreadPoolExecutor) executor;
        assertEquals(threadCount, threadPoolExecutor.getCorePoolSize());
        assertEquals(threadCount, threadPoolExecutor.getMaximumPoolSize());
        assertEquals(queueLimit, threadPoolExecutor.getQueue().remainingCapacity());
        assertInstanceOf(ThreadPoolExecutor.CallerRunsPolicy.class, threadPoolExecutor.getRejectedExecutionHandler());

        // Verify thread naming
        final Thread thread = threadPoolExecutor.getThreadFactory().newThread(() -> {});
        assertTrue(thread.getName().startsWith("async-writer-"));
    }

    /**
     * This test verifies that the factory creates a fork-join pool executor with
     * the correct configuration.
     */
    @Test
    void testCreateForkJoinExecutor() {
        // Given
        final PersistenceStorageConfig config = createConfig(Map.of("persistence.storage.executorType", "FORK_JOIN"));

        // When
        final Executor executor = AsyncWriterExecutorFactory.createExecutor(config);

        // Then
        assertNotNull(executor);
        assertThat(executor).isExactlyInstanceOf(ForkJoinPool.class);

        final ForkJoinPool forkJoinPool = (ForkJoinPool) executor;
        assertTrue(forkJoinPool.getAsyncMode());
    }

    /**
     * Creates a test configuration with the specified parameters.
     */
    private PersistenceStorageConfig createConfig(@NonNull Map<String, String> customProperties) {
        ConfigurationBuilder configurationBuilder =
                ConfigurationBuilder.create().autoDiscoverExtensions();

        for (Map.Entry<String, String> entry : customProperties.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            configurationBuilder.withSource(new SimpleConfigSource(key, value).withOrdinal(500));
        }
        return configurationBuilder.build().getConfigData(PersistenceStorageConfig.class);
    }
}
