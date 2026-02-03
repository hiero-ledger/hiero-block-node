// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.hiero.block.node.app.fixtures.async.ScheduledBlockingExecutor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link ScheduledBlockingExecutor}.
 */
class ScheduledBlockingExecutorTest {

    private ScheduledBlockingExecutor executor;

    @BeforeEach
    void setUp() {
        executor = new ScheduledBlockingExecutor(2, new LinkedBlockingQueue<>());
    }

    @AfterEach
    void tearDown() {
        executor.shutdownNow();
    }

    /**
     * Verifies that tasks submitted via execute() actually run.
     * This test fails if execute() doesn't call super.execute(),
     * because the task just sits in the work queue forever.
     *
     * Note: ScheduledThreadPoolExecutor.submit() calls schedule() internally,
     * so it bypasses execute(). This test uses execute() directly to verify
     * the fix works for code that calls execute() (like ThreadPoolExecutor.submit()).
     */
    @Test
    @DisplayName("Tasks submitted via execute() should actually run")
    void executedTasksShouldRun() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);

        executor.execute(latch::countDown);

        boolean completed = latch.await(5, TimeUnit.SECONDS);
        assertTrue(completed, "Task submitted via execute() should have run within timeout");
    }
}
