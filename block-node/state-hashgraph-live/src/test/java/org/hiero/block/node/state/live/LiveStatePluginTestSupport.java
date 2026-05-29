// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.state.live;

/**
 * Test-only helpers for {@link LiveStatePlugin}. Keeps busy-wait / polling
 * scaffolding out of production code — the plugin exposes only the read-only
 * {@code isReady()} observer, and tests poll it through here.
 */
final class LiveStatePluginTestSupport {

    private LiveStatePluginTestSupport() {}

    /**
     * Block the calling thread until the plugin reports caught-up, or the timeout
     * expires. Returns the final readiness state.
     */
    static boolean awaitReady(final LiveStatePlugin plugin, final long timeoutMillis) throws InterruptedException {
        final long deadline = System.currentTimeMillis() + timeoutMillis;
        while (!plugin.isReady() && System.currentTimeMillis() < deadline) {
            Thread.sleep(10L);
        }
        return plugin.isReady();
    }
}
