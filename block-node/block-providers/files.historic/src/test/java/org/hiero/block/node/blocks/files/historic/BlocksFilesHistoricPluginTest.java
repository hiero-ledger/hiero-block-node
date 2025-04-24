// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.blocks.files.historic;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.LinkedBlockingQueue;
import org.hiero.block.node.app.fixtures.async.BlockingSerialExecutor;
import org.hiero.block.node.app.fixtures.blocks.SimpleTestBlockItemBuilder;
import org.hiero.block.node.app.fixtures.plugintest.PluginTestBase;
import org.hiero.block.node.app.fixtures.plugintest.SimpleInMemoryHistoricalBlockFacility;
import org.hiero.block.node.base.CompressionType;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.PersistedNotification;
import org.hiero.hapi.block.node.BlockItemUnparsed;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for {@link BlocksFilesHistoricPlugin}.
 */
@DisplayName("BlocksFilesHistoricPlugin Tests")
class BlocksFilesHistoricPluginTest {
    /** The test block messaging facility to use for testing. */
    private final SimpleInMemoryHistoricalBlockFacility testHistoricalBlockFacility;
    /** The test block serial executor service to use for the plugin. */
    private final BlockingSerialExecutor pluginExecutor;
    /** The test config to use for the plugin. */
    private final FilesHistoricConfig testConfig;
    /** The instance under test. */
    private final BlocksFilesHistoricPlugin toTest;

    /**
     * Construct test environment.
     */
    BlocksFilesHistoricPluginTest(@TempDir final Path tempDir) {
        Objects.requireNonNull(tempDir);
        // create a blocking serial executor to run the plugin tasks that use
        // an executor service internally
        pluginExecutor = new BlockingSerialExecutor(new LinkedBlockingQueue<>());
        // generate test config, for the purposes of this test, we will always
        // use 10 blocks per zip, assuming that the first zip file will contain
        // for example blocks 0-9, the second zip file will contain blocks 10-19
        // also we will not use compression, and we will use the jUnit temp dir
        testConfig = new FilesHistoricConfig(tempDir, CompressionType.NONE, 1);
        // build the plugin using the test environment
        toTest = new BlocksFilesHistoricPlugin(testConfig, pluginExecutor);
        // initialize an in memory historical block facility to use for testing
        testHistoricalBlockFacility = new SimpleInMemoryHistoricalBlockFacility();
    }

    /**
     * Teardown logic to run after each test.
     */
    @AfterEach
    void tearDown() {
        pluginExecutor.shutdownNow();
    }

    /**
     * Plugin tests.
     */
    @Nested
    @DisplayName("Plugin Tests")
    final class PluginTests extends PluginTestBase<BlocksFilesHistoricPlugin> {
        /**
         * Construct plugin base.
         */
        PluginTests() {
            super(toTest, testHistoricalBlockFacility);
        }

        /**
         * This test aims to verify that the plugin can handle a simple range of
         * blocks that have been persisted and a notification is sent to the
         * messaging facility. The block provider that has persisted the blocks
         * must have a higher priority than the plugin we are testing. We expect
         * that the plugin we test will create a zip file with all the blocks in
         * the notification range (we set the range to 0-9 which fits the config
         * of 10 blocks per zip), i.e. this is the happy path test.
         */
        @Test
        @DisplayName("Test happy path zip range successful archival")
        void testZipRangeHappyPathArchival() throws IOException {
            // generate first 10 blocks from numbers 0-9 and add them to the
            // test historical block facility
            for (int i = 0; i < 10; i++) {
                final BlockItemUnparsed[] block = SimpleTestBlockItemBuilder.createSimpleBlockUnparsedWithNumber(i);
                testHistoricalBlockFacility.handleBlockItemsReceived(new BlockItems(List.of(block), i));
            }
            // assert that none of the first 10 blocks exist yet
            for (int i = 0; i < 10; i++) {
                assertThat(BlockPath.computeExistingBlockPath(testConfig, i)).isNull();
            }
            // send a block persisted notification for the range we just created
            blockMessaging.sendBlockPersisted(new PersistedNotification(0, 9, toTest.defaultPriority() + 1));
            // execute serially to ensure all tasks are completed
            pluginExecutor.executeSerially();
            // assert that the first 10 blocks exist now
            for (int i = 0; i < 10; i++) {
                assertThat(BlockPath.computeExistingBlockPath(testConfig, i)).isNotNull();
            }
        }
    }
}
