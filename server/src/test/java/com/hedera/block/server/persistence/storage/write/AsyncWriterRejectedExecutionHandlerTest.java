// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.server.persistence.storage.write;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test class that tests the functionality of the
 * {@link AsyncWriterRejectedExecutionHandler}
 */
class AsyncWriterRejectedExecutionHandlerTest {

    private AsyncWriterRejectedExecutionHandler handler;

    @BeforeEach
    void setUp() {
        handler = new AsyncWriterRejectedExecutionHandler();
    }

    /**
     * This test verifies that the handler executes the rejected task in the caller's
     * thread when the executor is not shutting down.
     */
    @Test
    void testRejectedExecutionRunsTaskWhenExecutorNotShutdown() {
        // Given
        final AtomicBoolean taskExecuted = new AtomicBoolean(false);
        final Runnable task = () -> taskExecuted.set(true);
        
        // Create a test executor that is not shutting down
        final ThreadPoolExecutor executor = new ThreadPoolExecutor(
                1, 1, 60, TimeUnit.SECONDS, new LinkedBlockingQueue<>(1));
        
        // When
        handler.rejectedExecution(task, executor);
        
        // Then
        assertTrue(taskExecuted.get(), "Task should have been executed in the caller's thread");
    }

    /**
     * This test verifies that the handler throws a RejectedExecutionException
     * when the executor is shutting down.
     */
    @Test
    void testRejectedExecutionThrowsExceptionWhenExecutorShutdown() {
        // Given
        final Runnable task = () -> {};
        
        // Create a test executor that is shutting down
        final ThreadPoolExecutor executor = new ThreadPoolExecutor(
                1, 1, 60, TimeUnit.SECONDS, new LinkedBlockingQueue<>(1));
        executor.shutdown(); // Mark as shutting down
        
        // Then
        assertThrows(RejectedExecutionException.class, () -> {
            // When
            handler.rejectedExecution(task, executor);
        }, "Should throw RejectedExecutionException when executor is shutting down");
    }
} 