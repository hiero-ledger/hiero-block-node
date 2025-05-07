// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Test class for {@link DefaultThreadPoolManager}.
 */
@DisplayName("DefaultThreadPoolManager Tests")
class DefaultThreadPoolManagerTest {
    /** The instance under test. */
    private DefaultThreadPoolManager toTest;

    /**
     * Setup before each test.
     */
    @BeforeEach
    void setUp() {
        toTest = new DefaultThreadPoolManager();
    }

    /**
     * Functionality tests for {@link DefaultThreadPoolManager}.
     */
    @Nested
    @DisplayName("Functionality Tests")
    final class FunctionalityTests {
        /**
         * This test aims to verify that the
         * {@link DefaultThreadPoolManager#createSingleThreadExecutor(String)}
         * creates a single thread executor correctly, each invocation creates
         * a new instance with the proper setup.
         */
        @Test
        @DisplayName(
                "Test createSingleThreadExecutor(String) will correctly create a single thread executor, new instance on every invocation")
        void testCreateSingleThreadExecutor() {
            final ExecutorService actual = toTest.createSingleThreadExecutor("testThreadName");
            assertThat(actual)
                    .isNotNull()
                    .isExactlyInstanceOf(ThreadPoolExecutor.class)
                    .asInstanceOf(InstanceOfAssertFactories.type(ThreadPoolExecutor.class))
                    .returns(0L, executor -> executor.getKeepAliveTime(TimeUnit.MILLISECONDS))
                    .returns(1, ThreadPoolExecutor::getCorePoolSize)
                    .returns(1, ThreadPoolExecutor::getMaximumPoolSize);
            final ExecutorService actual2 = toTest.createSingleThreadExecutor("testThreadName2");
            assertThat(actual2)
                    .isNotNull()
                    .isExactlyInstanceOf(ThreadPoolExecutor.class)
                    .asInstanceOf(InstanceOfAssertFactories.type(ThreadPoolExecutor.class))
                    .returns(0L, executor -> executor.getKeepAliveTime(TimeUnit.MILLISECONDS))
                    .returns(1, ThreadPoolExecutor::getCorePoolSize)
                    .returns(1, ThreadPoolExecutor::getMaximumPoolSize)
                    .isNotSameAs(actual);
        }

        /**
         * This test aims to verify that the
         * {@link DefaultThreadPoolManager#createSingleThreadExecutor(String, UncaughtExceptionHandler)}
         * creates a single thread executor correctly, each invocation creates
         * a new instance with the proper setup.
         */
        @Test
        @DisplayName(
                "Test createSingleThreadExecutor(String, UncaughtExceptionHandler) will correctly create a single thread executor, new instance on every invocation")
        void testCreateSingleThreadExecutorWithUncaughtExceptionHandler() {
            final UncaughtExceptionHandler expectedHandler = (t, e) -> {
                // Handle the exception
            };
            final ExecutorService actual = toTest.createSingleThreadExecutor("testThreadName", expectedHandler);
            assertThat(actual)
                    .isNotNull()
                    .isExactlyInstanceOf(ThreadPoolExecutor.class)
                    .asInstanceOf(InstanceOfAssertFactories.type(ThreadPoolExecutor.class))
                    .returns(0L, executor -> executor.getKeepAliveTime(TimeUnit.MILLISECONDS))
                    .returns(1, ThreadPoolExecutor::getCorePoolSize)
                    .returns(1, ThreadPoolExecutor::getMaximumPoolSize)
                    .returns(expectedHandler, executor -> executor.getThreadFactory()
                            .newThread(() -> {})
                            .getUncaughtExceptionHandler());
            final ExecutorService actual2 = toTest.createSingleThreadExecutor("testThreadName2", expectedHandler);
            assertThat(actual2)
                    .isNotNull()
                    .isExactlyInstanceOf(ThreadPoolExecutor.class)
                    .asInstanceOf(InstanceOfAssertFactories.type(ThreadPoolExecutor.class))
                    .returns(0L, executor -> executor.getKeepAliveTime(TimeUnit.MILLISECONDS))
                    .returns(1, ThreadPoolExecutor::getCorePoolSize)
                    .returns(1, ThreadPoolExecutor::getMaximumPoolSize)
                    .returns(expectedHandler, executor -> executor.getThreadFactory()
                            .newThread(() -> {})
                            .getUncaughtExceptionHandler())
                    .isNotSameAs(actual);
        }
    }
}
