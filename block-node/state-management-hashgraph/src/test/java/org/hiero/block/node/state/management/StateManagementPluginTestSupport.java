// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.state.management;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Stream;
import org.hiero.block.node.app.fixtures.async.TestThreadPoolManager;

/**
 * Test-only helpers for {@link StateManagementPlugin}. Keeps busy-wait / polling
 * scaffolding out of production code — the plugin exposes only the read-only
 * {@code isReady()} observer, and tests poll it through here. Also holds the snapshot-root
 * listing helpers shared by the plugin and {@link StateSnapshotManager} tests.
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
     * plugin through {@code start()}/{@code stop()} and need its catch-up and snapshot executors
     * to actually run.
     */
    static TestThreadPoolManager<ExecutorService, ScheduledExecutorService> newThreadPoolManager() {
        return new TestThreadPoolManager<>(
                Executors.newSingleThreadExecutor(), Executors.newSingleThreadScheduledExecutor());
    }

    /** Numerically named (snapshot) dirs directly under {@code root}, ascending. */
    static List<Long> numericDirs(final Path root) throws IOException {
        return listNames(root).stream()
                .filter(name -> StateSnapshotManager.parseBlockNumber(name) >= 0L)
                .filter(name -> Files.isDirectory(root.resolve(name)))
                .map(StateSnapshotManager::parseBlockNumber)
                .sorted()
                .toList();
    }

    /** {@code .tmp-<digits>} staging dirs directly under {@code root}. */
    static List<String> stagingDirs(final Path root) throws IOException {
        return listNames(root).stream()
                .filter(name -> name.matches("\\.tmp-\\d+"))
                .toList();
    }

    /** Names of the entries directly under {@code dir}, sorted; empty if it is not a directory. */
    static List<String> listNames(final Path dir) throws IOException {
        if (!Files.isDirectory(dir)) {
            return List.of();
        }
        try (Stream<Path> entries = Files.list(dir)) {
            return entries.map(p -> p.getFileName().toString()).sorted().toList();
        }
    }

    /** Create {@code file} (and its parent dirs) with a one-character body. */
    static Path writeFile(final Path file) throws IOException {
        Files.createDirectories(file.getParent());
        return Files.writeString(file, "x");
    }
}
