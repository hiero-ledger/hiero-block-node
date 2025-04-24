// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.blocks.files.historic;

import static org.assertj.core.api.Assertions.assertThat;

import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import org.hiero.block.node.app.fixtures.async.BlockingSerialExecutor;
import org.hiero.block.node.app.fixtures.blocks.SimpleTestBlockItemBuilder;
import org.hiero.block.node.app.fixtures.plugintest.PluginTestBase;
import org.hiero.block.node.app.fixtures.plugintest.SimpleInMemoryHistoricalBlockFacility;
import org.hiero.block.node.base.CompressionType;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.PersistedNotification;
import org.hiero.hapi.block.node.BlockItemUnparsed;
import org.hiero.hapi.block.node.BlockUnparsed;
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
            // assert that none of the first 10 blocks are zipped yet
            for (int i = 0; i < 10; i++) {
                assertThat(BlockPath.computeExistingBlockPath(testConfig, i)).isNull();
            }
            // send a block persisted notification for the range we just created
            blockMessaging.sendBlockPersisted(new PersistedNotification(0, 9, toTest.defaultPriority() + 1));
            // execute serially to ensure all tasks are completed
            pluginExecutor.executeSerially();
            // assert that the first 10 blocks are zipped now
            for (int i = 0; i < 10; i++) {
                assertThat(BlockPath.computeExistingBlockPath(testConfig, i)).isNotNull();
            }
        }

        /**
         * This test aims to verify that the plugin can handle a simple range of
         * blocks that have been persisted and a notification is sent to the
         * messaging facility. The block provider that has persisted the blocks
         * must have a higher priority than the plugin we are testing. We expect
         * that the plugin we test will create a zip file with all the blocks in
         * the notification range (we set the range to 0-9 which fits the config
         * of 10 blocks per zip), i.e. this is the happy path test. We assert
         * here the contents of each entry produce the same blocks as before
         * archival.
         */
        @Test
        @DisplayName("Test happy path zip range archive contents")
        void testZipRangeHappyPathArchiveContents() throws IOException, ParseException {
            // generate first 10 blocks from numbers 0-9 and add them to the
            // test historical block facility
            final List<BlockUnparsed> expectedBlocks = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                final BlockItemUnparsed[] block = SimpleTestBlockItemBuilder.createSimpleBlockUnparsedWithNumber(i);
                testHistoricalBlockFacility.handleBlockItemsReceived(new BlockItems(List.of(block), i));
                expectedBlocks.add(new BlockUnparsed(List.of(block)));
            }
            // assert that none of the first 10 blocks are zipped yet
            for (int i = 0; i < 10; i++) {
                assertThat(BlockPath.computeExistingBlockPath(testConfig, i)).isNull();
            }
            // send a block persisted notification for the range we just created
            blockMessaging.sendBlockPersisted(new PersistedNotification(0, 9, toTest.defaultPriority() + 1));
            // execute serially to ensure all tasks are completed
            pluginExecutor.executeSerially();
            // assert the contents of the zip file
            for (int i = 0; i < 10; i++) {
                final BlockPath blockPath = BlockPath.computeExistingBlockPath(testConfig, i);
                try (final ZipFile zipFile = new ZipFile(blockPath.zipFilePath().toFile())) {
                    // assert that the zip file contains the block file
                    final ZipEntry zipEntry = zipFile.getEntry(blockPath.blockFileName());
                    assertThat(zipEntry).isNotNull();
                    final byte[] zipEntryBytes =
                            zipFile.getInputStream(zipEntry).readAllBytes();
                    final BlockUnparsed actual = BlockUnparsed.PROTOBUF.parse(Bytes.wrap(zipEntryBytes));
                    assertThat(actual).isEqualTo(expectedBlocks.get(i));
                }
            }
        }

        /**
         * This test aims to verify that the plugin can handle a simple range of
         * blocks that have been persisted and a notification is sent to the
         * messaging facility. The block provider that has persisted the blocks
         * must have a higher priority than the plugin we are testing. We expect
         * that the plugin we test will create a zip file with all the blocks in
         * the notification range (we set the range to 0-4 which does not fit the
         * config of 10 blocks per zip), i.e. this is the happy path test.
         * This test will assert the behavior of the plugin when we send
         * multiple notifications, mimicking the scenario where the provider
         * will receive and make available some of the blocks in the range
         * we expect and at a later time it will make available more blocks in
         * the range we expect. The plugin under test should be sure that the
         * range it is following is covered before it will zip the blocks.
         */
        @Test
        @DisplayName("Test happy path zip range successful archival on multiple notifications")
        void testZipRangeWaitForEnoughAvailable() throws IOException {
            // generate first 5 blocks from numbers 0-4 and add them to the
            // test historical block facility
            for (int i = 0; i < 5; i++) {
                final BlockItemUnparsed[] block = SimpleTestBlockItemBuilder.createSimpleBlockUnparsedWithNumber(i);
                testHistoricalBlockFacility.handleBlockItemsReceived(new BlockItems(List.of(block), i));
            }
            // assert that none of the first 5 blocks are zipped yet
            for (int i = 0; i < 5; i++) {
                assertThat(BlockPath.computeExistingBlockPath(testConfig, i)).isNull();
            }
            // send a block persisted notification for the range we just created
            blockMessaging.sendBlockPersisted(new PersistedNotification(0, 4, toTest.defaultPriority() + 1));
            // assert that no task has been submitted to the pool because we have
            // not yet reached the desired amount of blocks we want to archive
            final boolean anyTaskSubmitted = pluginExecutor.wasAnyTaskSubmitted();
            assertThat(anyTaskSubmitted).isFalse();
            // assert that the first 5 blocks do not exist
            for (int i = 0; i < 5; i++) {
                assertThat(BlockPath.computeExistingBlockPath(testConfig, i)).isNull();
            }
            // generate the next 5 blocks from numbers 5-9 and add them to the
            // test historical block facility
            for (int i = 5; i < 10; i++) {
                final BlockItemUnparsed[] block = SimpleTestBlockItemBuilder.createSimpleBlockUnparsedWithNumber(i);
                testHistoricalBlockFacility.handleBlockItemsReceived(new BlockItems(List.of(block), i));
            }
            // assert that none of the first 10 blocks are zipped yet
            for (int i = 0; i < 10; i++) {
                assertThat(BlockPath.computeExistingBlockPath(testConfig, i)).isNull();
            }
            // send a block persisted notification for the range we just created
            blockMessaging.sendBlockPersisted(new PersistedNotification(5, 9, toTest.defaultPriority() + 1));
            // execute serially to ensure all tasks are completed
            pluginExecutor.executeSerially();
            // assert that the first 10 blocks are zipped now
            for (int i = 0; i < 10; i++) {
                assertThat(BlockPath.computeExistingBlockPath(testConfig, i)).isNotNull();
            }
        }

        /**
         * This test aims to verify that the plugin will not zip anything when
         * a notification that covers the range of persisted blocks is sent
         * from a provider that has a lower priority than the plugin we are
         * testing. We expect that the plugin we test will not create a zip in
         * those cases.
         */
        @Test
        @DisplayName("Test no zip will be created when notification is from lower priority provider")
        void testNoZipForLowerPriorityNotifications() throws IOException {
            // generate first 10 blocks from numbers 0-9 and add them to the
            // test historical block facility
            for (int i = 0; i < 10; i++) {
                final BlockItemUnparsed[] block = SimpleTestBlockItemBuilder.createSimpleBlockUnparsedWithNumber(i);
                testHistoricalBlockFacility.handleBlockItemsReceived(new BlockItems(List.of(block), i));
            }
            // assert that none of the first 10 blocks are zipped yet
            for (int i = 0; i < 10; i++) {
                assertThat(BlockPath.computeExistingBlockPath(testConfig, i)).isNull();
            }
            // send a block persisted notification for the range we just created
            blockMessaging.sendBlockPersisted(new PersistedNotification(0, 9, toTest.defaultPriority() - 1));
            // assert that no zipping task was submitted
            final boolean anyTaskSubmitted = pluginExecutor.wasAnyTaskSubmitted();
            assertThat(anyTaskSubmitted).isFalse();
            // assert that the first 10 blocks are not zipped
            for (int i = 0; i < 10; i++) {
                assertThat(BlockPath.computeExistingBlockPath(testConfig, i)).isNull();
            }
        }

        /**
         * This test aims to verify that the plugin will not zip anything when
         * a notification that covers the range of persisted blocks is sent
         * from a provider that has the same priority as the plugin we are
         * testing. We expect that the plugin we test will not create a zip in
         * those cases.
         */
        @Test
        @DisplayName("Test no zip will be created when notification is from same priority provider")
        void testNoZipForSamePriorityNotifications() throws IOException {
            // generate first 10 blocks from numbers 0-9 and add them to the
            // test historical block facility
            for (int i = 0; i < 10; i++) {
                final BlockItemUnparsed[] block = SimpleTestBlockItemBuilder.createSimpleBlockUnparsedWithNumber(i);
                testHistoricalBlockFacility.handleBlockItemsReceived(new BlockItems(List.of(block), i));
            }
            // assert that none of the first 10 blocks are zipped yet
            for (int i = 0; i < 10; i++) {
                assertThat(BlockPath.computeExistingBlockPath(testConfig, i)).isNull();
            }
            // send a block persisted notification for the range we just created
            blockMessaging.sendBlockPersisted(new PersistedNotification(0, 9, toTest.defaultPriority()));
            // assert that no zipping task was submitted
            final boolean anyTaskSubmitted = pluginExecutor.wasAnyTaskSubmitted();
            assertThat(anyTaskSubmitted).isFalse();
            // assert that the first 10 blocks are not zipped
            for (int i = 0; i < 10; i++) {
                assertThat(BlockPath.computeExistingBlockPath(testConfig, i)).isNull();
            }
        }
    }
}
