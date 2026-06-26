// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
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

        /**
         * This test aims to verify that the
         * {@link DefaultThreadPoolManager#createSingleThreadScheduledExecutor(String, UncaughtExceptionHandler)}
         * creates a single thread scheduled executor correctly using platform threads.
         */
        @Test
        @DisplayName(
                "Test createSingleThreadScheduledExecutor creates platform thread scheduled executor with correct configuration")
        void testCreateSingleThreadScheduledExecutor() throws Exception {
            final UncaughtExceptionHandler expectedHandler = (t, e) -> {
                // Handle the exception
            };
            final ScheduledExecutorService actual =
                    toTest.createSingleThreadScheduledExecutor("testScheduledThread", expectedHandler);
            assertThat(actual).isNotNull().isInstanceOf(ScheduledExecutorService.class);

            // Submit a task to verify the thread configuration
            actual.submit(() -> {
                        Thread currentThread = Thread.currentThread();
                        assertThat(currentThread.getName()).isEqualTo("testScheduledThread");
                        assertThat(currentThread.getUncaughtExceptionHandler()).isSameAs(expectedHandler);
                        assertThat(currentThread.isVirtual()).isFalse();
                    })
                    .get(5, TimeUnit.SECONDS);

            final ScheduledExecutorService actual2 =
                    toTest.createSingleThreadScheduledExecutor("testScheduledThread2", expectedHandler);
            assertThat(actual2)
                    .isNotNull()
                    .isInstanceOf(ScheduledExecutorService.class)
                    .isNotSameAs(actual);

            actual.shutdownNow();
            actual2.shutdownNow();
        }

        /**
         * This test aims to verify that
         * {@link DefaultThreadPoolManager#getVirtualThreadExecutor(String, UncaughtExceptionHandler)}
         * returns a non-null executor when both arguments are null, and returns
         * the same (default static) instance on repeated calls.
         */
        @Test
        @DisplayName(
                "Test getVirtualThreadExecutor(null, null) returns a non-null executor and same instance on repeated calls")
        void testGetVirtualThreadExecutorBothNull() {
            final ExecutorService actual = toTest.getVirtualThreadExecutor(null, null);
            assertThat(actual).isNotNull();
            final ExecutorService actual2 = toTest.getVirtualThreadExecutor(null, null);
            assertThat(actual2).isNotNull().isSameAs(actual);
        }

        /**
         * This test aims to verify that
         * {@link DefaultThreadPoolManager#getVirtualThreadExecutor(String, UncaughtExceptionHandler)}
         * returns a non-null executor when a threadName is provided and the handler is null,
         * and that tasks submitted to it run on virtual threads named with the given prefix.
         */
        @Test
        @DisplayName(
                "Test getVirtualThreadExecutor(threadName, null) returns a non-null executor running virtual threads named with the given prefix")
        void testGetVirtualThreadExecutorWithThreadName() throws Exception {
            final ExecutorService actual = toTest.getVirtualThreadExecutor("myThread", null);
            assertThat(actual).isNotNull();
            final String threadName =
                    actual.submit(() -> Thread.currentThread().getName()).get(5, TimeUnit.SECONDS);
            assertThat(threadName).isEqualTo("myThread0");
            final Thread isVirtual = actual.submit(Thread::currentThread).get(5, TimeUnit.SECONDS);
            assertThat(isVirtual.isVirtual()).isTrue();
            actual.shutdownNow();
        }

        /**
         * This test aims to verify that
         * {@link DefaultThreadPoolManager#getVirtualThreadExecutor(String, UncaughtExceptionHandler)}
         * returns a non-null executor when a handler is provided and the name is null,
         * and that repeated calls with the same handler return the same cached instance.
         */
        @Test
        @DisplayName(
                "Test getVirtualThreadExecutor(null, handler) returns a non-null executor and same instance on repeated calls with same handler")
        void testGetVirtualThreadExecutorWithHandler() {
            final UncaughtExceptionHandler handler = (t, e) -> {
                // handle exception
            };
            final ExecutorService actual = toTest.getVirtualThreadExecutor(null, handler);
            assertThat(actual).isNotNull();
            final ExecutorService actual2 = toTest.getVirtualThreadExecutor(null, handler);
            assertThat(actual2).isNotNull().isSameAs(actual);
            actual.shutdownNow();
        }

        /**
         * This test aims to verify that
         * {@link DefaultThreadPoolManager#getVirtualThreadExecutor(String, UncaughtExceptionHandler)}
         * returns different executor instances when called with two distinct handler instances.
         */
        @Test
        @DisplayName(
                "Test getVirtualThreadExecutor(null, handler) returns different instances for different handler instances")
        void testGetVirtualThreadExecutorDifferentHandlers() {
            final UncaughtExceptionHandler handler1 = (t, e) -> {
                // handle exception 1
            };
            final UncaughtExceptionHandler handler2 = (t, e) -> {
                // handle exception 2
            };
            final ExecutorService actual1 = toTest.getVirtualThreadExecutor(null, handler1);
            final ExecutorService actual2 = toTest.getVirtualThreadExecutor(null, handler2);
            assertThat(actual1).isNotNull();
            assertThat(actual2).isNotNull().isNotSameAs(actual1);
            actual1.shutdownNow();
            actual2.shutdownNow();
        }
    }
}
