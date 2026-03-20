// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.archive.cloud.storage;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import org.hiero.block.node.app.fixtures.async.BlockingExecutor;
import org.hiero.block.node.app.fixtures.async.ScheduledBlockingExecutor;
import org.hiero.block.node.app.fixtures.plugintest.PluginTestBase;
import org.hiero.block.node.app.fixtures.plugintest.SimpleInMemoryHistoricalBlockFacility;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/// Unit tests for [ArchiveCloudStoragePlugin].
@DisplayName("ArchiveCloudStoragePluginTest Tests")
public class ArchiveCloudStoragePluginTest {

    private final Path rootDir;
    private final SimpleInMemoryHistoricalBlockFacility testHistoricalBlockFacility;
    private final ArchiveCloudStoragePlugin toTest;

    public ArchiveCloudStoragePluginTest(@TempDir Path rootDir) {
        this.rootDir = rootDir;
        this.testHistoricalBlockFacility = new SimpleInMemoryHistoricalBlockFacility();
        this.toTest = new ArchiveCloudStoragePlugin();
    }

    /// Tests for plugin startup behaviour.
    @Nested
    @DisplayName("Startup Tests")
    final class StartupTest
            extends PluginTestBase<ArchiveCloudStoragePlugin, BlockingExecutor, ScheduledExecutorService> {

        StartupTest() {
            super(
                    new BlockingExecutor(new LinkedBlockingQueue<>()),
                    new ScheduledBlockingExecutor(new LinkedBlockingQueue<>()));
        }

        @Test
        @DisplayName("Root directory is empty")
        void testRootDirectoryIsEmpty() {
            start(toTest, testHistoricalBlockFacility);
            assertThat(rootDir.toFile()).isEmptyDirectory();
        }
    }
}
