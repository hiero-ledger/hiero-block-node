// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.state.management;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.hiero.block.node.app.fixtures.async.TestThreadPoolManager;

/**
 * Test-only helpers for {@link StateManagementPlugin}. Keeps busy-wait / polling
 * scaffolding out of production code — the plugin exposes only the read-only
 * {@code isReady()} observer, and tests poll it through here.
 */
final class StateManagementPluginTestSupport {

    private StateManagementPluginTestSupport() {}

    /**
     * Block the calling thread until the plugin reports caught-up, or the timeout
     * expires. Returns the final readiness state.
     */
    static boolean awaitReady(final StateManagementPlugin plugin, final long timeoutMillis)
            throws InterruptedException {
        final long deadline = System.currentTimeMillis() + timeoutMillis;
        while (!plugin.isReady() && System.currentTimeMillis() < deadline) {
            Thread.sleep(10L);
        }
        return plugin.isReady();
    }

    /**
     * A {@code ThreadPoolManager} backed by real (daemon) executors, for tests that drive the
     * plugin through {@code start()}/{@code stop()} and need its apply worker and background
     * executor to actually run.
     */
    static TestThreadPoolManager<ExecutorService, ScheduledExecutorService> newThreadPoolManager() {
        return new TestThreadPoolManager<>(
                Executors.newSingleThreadExecutor(), Executors.newSingleThreadScheduledExecutor());
    }
}
