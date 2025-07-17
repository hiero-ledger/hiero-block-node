// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.blocks.files.historic;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.mockito.Mockito.mock;

import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.config.api.Configuration;
import com.swirlds.config.api.ConfigurationBuilder;
import com.swirlds.metrics.api.Metrics;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.LinkedBlockingQueue;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.app.fixtures.async.BlockingSerialExecutor;
import org.hiero.block.node.app.fixtures.async.TestThreadPoolManager;
import org.hiero.block.node.app.fixtures.blocks.SimpleTestBlockItemBuilder;
import org.hiero.block.node.app.fixtures.plugintest.NoOpServiceBuilder;
import org.hiero.block.node.app.fixtures.plugintest.PluginTestBase;
import org.hiero.block.node.app.fixtures.plugintest.SimpleBlockRangeSet;
import org.hiero.block.node.app.fixtures.plugintest.SimpleInMemoryHistoricalBlockFacility;
import org.hiero.block.node.app.fixtures.plugintest.TestBlockMessagingFacility;
import org.hiero.block.node.app.fixtures.plugintest.TestHealthFacility;
import org.hiero.block.node.base.CompressionType;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.ServiceBuilder;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.PersistedNotification;
import org.hiero.block.node.spi.historicalblocks.BlockAccessor;
import org.hiero.block.node.spi.historicalblocks.BlockRangeSet;
import org.hiero.block.node.spi.historicalblocks.HistoricalBlockFacility;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for {@link BlocksFilesHistoricPlugin}.
 */
@DisplayName("BlocksFilesHistoricPlugin Tests")
class BlocksFilesHistoricPluginTest {
    /** TempDir for the current test */
    private final Path testTempDir;
    /** The test block messaging facility to use for testing. */
    private final SimpleInMemoryHistoricalBlockFacility testHistoricalBlockFacility;
    /** The test config to use for the plugin, overridable. */
    private FilesHistoricConfig testConfig;
    /** The instance under test. */
    private final BlocksFilesHistoricPlugin toTest;

    /**
     * Construct test environment.
     */
    BlocksFilesHistoricPluginTest(@TempDir final Path tempDir) {
        this.testTempDir = Objects.requireNonNull(tempDir);
        // generate test config, for the purposes of this test, we will always
        // use 10 blocks per zip, assuming that the first zip file will contain
        // for example blocks 0-9, the second zip file will contain blocks 10-19
        // also we will not use compression, and we will use the jUnit temp dir
        testConfig = new FilesHistoricConfig(this.testTempDir, CompressionType.NONE, 1, 10L);
        // build the plugin using the test environment
        toTest = new BlocksFilesHistoricPlugin();
        // initialize an in memory historical block facility to use for testing
        testHistoricalBlockFacility = new SimpleInMemoryHistoricalBlockFacility();
    }

    /**
     * Constructor and Init tests.
     */
    @Nested
    @DisplayName("Constructor & Init Tests")
    final class ConstructorAndInitTests {
        /**
         * This test aims to verify that the no args constructor of
         * {@link BlocksFilesHistoricPlugin} does not throw any exceptions.
         */
        @Test
        @DisplayName("Test no args constructor does not throw any exceptions")
        void testNoArgsConstructor() {
            assertThatNoException().isThrownBy(BlocksFilesHistoricPlugin::new);
        }

        /**
         * This test aims to verify that the
         * {@link BlocksFilesHistoricPlugin#init(BlockNodeContext, ServiceBuilder)}
         * method throws a {@link NullPointerException} if the context is null.
         */
        @Test
        @DisplayName("Test init throws null pointer when supplied with null context")
        void testInitNullContext() {
            final BlocksFilesHistoricPlugin toTest = new BlocksFilesHistoricPlugin();
            assertThatNullPointerException().isThrownBy(() -> toTest.init(null, new NoOpServiceBuilder()));
        }

        /**
         * This test aims to verify that the
         * {@link BlocksFilesHistoricPlugin#init(BlockNodeContext, ServiceBuilder)}
         * method throws a {@link NullPointerException} if the context is null.
         */
        @Test
        @DisplayName("Test init does not throw when ServiceBuilder is null (currently unused)")
        void testInitNullServiceBuilder(@TempDir final Path tempDir) {
            // setup a local valid context
            final Configuration configuration = ConfigurationBuilder.create()
                    .withConfigDataType(FilesHistoricConfig.class)
                    .withValue("files.historic.rootPath", tempDir.toString())
                    .build();
            final Metrics metricsMock = mock(Metrics.class);
            final HistoricalBlockFacility historicalBlockProvider = new SimpleInMemoryHistoricalBlockFacility();
            final BlockNodeContext testContext = new BlockNodeContext(
                    configuration,
                    metricsMock,
                    new TestHealthFacility(),
                    new TestBlockMessagingFacility(),
                    historicalBlockProvider,
                    null,
                    new TestThreadPoolManager<>(new BlockingSerialExecutor(new LinkedBlockingQueue<>())));
            // call
            final BlocksFilesHistoricPlugin toTest = new BlocksFilesHistoricPlugin();
            assertThatNoException().isThrownBy(() -> toTest.init(testContext, null));
        }
    }

    /**
     * Plugin tests.
     */
    @Nested
    @DisplayName("Plugin Tests")
    final class PluginTests extends PluginTestBase<BlocksFilesHistoricPlugin> {
        /** The test block serial executor service to use for the plugin. */
        private final BlockingSerialExecutor pluginExecutor;

        /**
         * Construct plugin base.
         */
        PluginTests() {
            // match overrides to the test config
            final Map<String, String> configOverrides = getConfigOverrides();
            pluginExecutor = testThreadPoolManager.executor();
            // initialize and start the test plugin using the config overrides
            start(toTest, testHistoricalBlockFacility, configOverrides);
        }

        private Map<String, String> getConfigOverrides() {
            final Entry<String, String> rootPath =
                    Map.entry("files.historic.rootPath", testConfig.rootPath().toString());
            final Entry<String, String> compression = Map.entry(
                    "files.historic.compression", testConfig.compression().name());
            final Entry<String, String> powersOfTenPerZipFileContents = Map.entry(
                    "files.historic.powersOfTenPerZipFileContents",
                    String.valueOf(testConfig.powersOfTenPerZipFileContents()));
            final Entry<String, String> blockRetentionThreshold = Map.entry(
                    "files.historic.blockRetentionThreshold", String.valueOf(testConfig.blockRetentionThreshold()));
            return Map.ofEntries(rootPath, compression, powersOfTenPerZipFileContents, blockRetentionThreshold);
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
        @DisplayName("Test happy path zip successful archival")
        void testZipRangeHappyPathArchival() throws IOException {
            // generate first 10 blocks from numbers 0-9 and add them to the
            // test historical block facility
            for (int i = 0; i < 10; i++) {
                final BlockItemUnparsed[] block = SimpleTestBlockItemBuilder.createSimpleBlockUnparsedWithNumber(i);
                testHistoricalBlockFacility.handleBlockItemsReceived(new BlockItems(List.of(block), i), false);
            }
            // assert that none of the first 10 blocks are zipped yet
            for (int i = 0; i < 10; i++) {
                assertThat(BlockPath.computeExistingBlockPath(testConfig, i)).isNull();
            }
            // send a block persisted notification for the range we just created
            blockMessaging.sendBlockPersisted(
                    new PersistedNotification(0, 9, toTest.defaultPriority() + 1, BlockSource.PUBLISHER));
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
         * the notification range (we set the range to 0-19 which fits the config
         * of 10 blocks per zip), i.e. this is the happy path test for two full
         * consecutive batches to be archived in a single notification. We expect
         * all blocks to be archived.
         */
        @Test
        @DisplayName("Test happy path zip successful archival two full consecutive batches")
        void testZipRangeHappyPathArchivalTwoFullBatches() throws IOException {
            // generate first 20 blocks from numbers 0-19 and add them to the
            // test historical block facility
            for (int i = 0; i < 20; i++) {
                final BlockItemUnparsed[] block = SimpleTestBlockItemBuilder.createSimpleBlockUnparsedWithNumber(i);
                testHistoricalBlockFacility.handleBlockItemsReceived(new BlockItems(List.of(block), i), false);
            }
            // assert that none of the first 20 blocks are zipped yet
            for (int i = 0; i < 20; i++) {
                assertThat(BlockPath.computeExistingBlockPath(testConfig, i)).isNull();
            }
            // send a block persisted notification for the range we just created
            blockMessaging.sendBlockPersisted(
                    new PersistedNotification(0, 19, toTest.defaultPriority() + 1, BlockSource.PUBLISHER));
            // execute serially to ensure all tasks are completed
            pluginExecutor.executeSerially();
            // assert that the first 20 blocks are zipped now
            for (int i = 0; i < 20; i++) {
                assertThat(BlockPath.computeExistingBlockPath(testConfig, i)).isNotNull();
            }
        }

        /**
         * This test aims to verify that the plugin can handle a simple range of
         * blocks that have been persisted and a notification is sent to the
         * messaging facility. The block provider that has persisted the blocks
         * must have a higher priority than the plugin we are testing. We expect
         * that the plugin we test will create a zip file with all the blocks in
         * the notification range (we set the range to 0-14 which fits the config
         * of 10 blocks per zip), i.e. this is the happy path test for two full
         * consecutive batches to be archived in a single notification. We expect
         * all blocks to be archived.
         */
        @Test
        @DisplayName("Test happy path zip successful archival batch and a half")
        void testZipRangeHappyPathArchivalBatchAndAHalf() throws IOException {
            // generate first 15 blocks from numbers 0-14 and add them to the
            // test historical block facility
            for (int i = 0; i < 14; i++) {
                final BlockItemUnparsed[] block = SimpleTestBlockItemBuilder.createSimpleBlockUnparsedWithNumber(i);
                testHistoricalBlockFacility.handleBlockItemsReceived(new BlockItems(List.of(block), i), false);
            }
            // assert that none of the first 20 blocks are zipped yet
            for (int i = 0; i < 14; i++) {
                assertThat(BlockPath.computeExistingBlockPath(testConfig, i)).isNull();
            }
            // send a block persisted notification for the range we just created
            blockMessaging.sendBlockPersisted(
                    new PersistedNotification(0, 14, toTest.defaultPriority() + 1, BlockSource.PUBLISHER));
            // execute serially to ensure all tasks are completed
            pluginExecutor.executeSerially();
            // assert that the first 10 blocks are zipped now
            for (int i = 0; i < 10; i++) {
                assertThat(BlockPath.computeExistingBlockPath(testConfig, i)).isNotNull();
            }
            // assert that the next 5 blocks are not zipped however
            for (int i = 10; i < 15; i++) {
                assertThat(BlockPath.computeExistingBlockPath(testConfig, i)).isNull();
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
        @DisplayName("Test happy path zip archive contents")
        void testZipRangeHappyPathArchiveContents() throws IOException, ParseException {
            // generate first 10 blocks from numbers 0-9 and add them to the
            // test historical block facility
            final List<BlockUnparsed> expectedBlocks = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                final BlockItemUnparsed[] block = SimpleTestBlockItemBuilder.createSimpleBlockUnparsedWithNumber(i);
                testHistoricalBlockFacility.handleBlockItemsReceived(new BlockItems(List.of(block), i), false);
                expectedBlocks.add(new BlockUnparsed(List.of(block)));
            }
            // assert that none of the first 10 blocks are zipped yet
            for (int i = 0; i < 10; i++) {
                assertThat(BlockPath.computeExistingBlockPath(testConfig, i)).isNull();
            }
            // send a block persisted notification for the range we just created
            blockMessaging.sendBlockPersisted(
                    new PersistedNotification(0, 9, toTest.defaultPriority() + 1, BlockSource.PUBLISHER));
            // execute serially to ensure all tasks are completed
            pluginExecutor.executeSerially();
            // assert the contents of the zip file
            for (int i = 0; i < 10; i++) {
                final BlockPath blockPath = BlockPath.computeExistingBlockPath(testConfig, i);
                try (final FileSystem zipFs = FileSystems.newFileSystem(blockPath.zipFilePath())) {
                    // assert that the zip entry exists
                    final Path zipEntryPath = zipFs.getPath(blockPath.blockFileName());
                    assertThat(zipEntryPath).exists().isRegularFile();
                    final byte[] zipEntryBytes = Files.readAllBytes(zipEntryPath);
                    final BlockUnparsed actual = BlockUnparsed.PROTOBUF.parse(Bytes.wrap(zipEntryBytes));
                    assertThat(actual).isEqualTo(expectedBlocks.get(i));
                    // assert that the block file exists
                    assertThat(Files.exists(zipFs.getPath(blockPath.blockFileName())))
                            .isTrue();
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
        @DisplayName("Test happy path zip successful archival on multiple notifications")
        void testZipRangeWaitForEnoughAvailable() throws IOException {
            // generate first 5 blocks from numbers 0-4 and add them to the
            // test historical block facility
            for (int i = 0; i < 5; i++) {
                final BlockItemUnparsed[] block = SimpleTestBlockItemBuilder.createSimpleBlockUnparsedWithNumber(i);
                testHistoricalBlockFacility.handleBlockItemsReceived(new BlockItems(List.of(block), i), false);
            }
            // assert that none of the first 5 blocks are zipped yet
            for (int i = 0; i < 5; i++) {
                assertThat(BlockPath.computeExistingBlockPath(testConfig, i)).isNull();
            }
            // send a block persisted notification for the range we just created
            blockMessaging.sendBlockPersisted(
                    new PersistedNotification(0, 4, toTest.defaultPriority() + 1, BlockSource.PUBLISHER));
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
                testHistoricalBlockFacility.handleBlockItemsReceived(new BlockItems(List.of(block), i), false);
            }
            // assert that none of the first 10 blocks are zipped yet
            for (int i = 0; i < 10; i++) {
                assertThat(BlockPath.computeExistingBlockPath(testConfig, i)).isNull();
            }
            // send a block persisted notification for the range we just created
            blockMessaging.sendBlockPersisted(
                    new PersistedNotification(5, 9, toTest.defaultPriority() + 1, BlockSource.PUBLISHER));
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
                testHistoricalBlockFacility.handleBlockItemsReceived(new BlockItems(List.of(block), i), false);
            }
            // assert that none of the first 10 blocks are zipped yet
            for (int i = 0; i < 10; i++) {
                assertThat(BlockPath.computeExistingBlockPath(testConfig, i)).isNull();
            }
            // send a block persisted notification for the range we just created
            blockMessaging.sendBlockPersisted(
                    new PersistedNotification(0, 9, toTest.defaultPriority() - 1, BlockSource.PUBLISHER));
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
                testHistoricalBlockFacility.handleBlockItemsReceived(new BlockItems(List.of(block), i), false);
            }
            // assert that none of the first 10 blocks are zipped yet
            for (int i = 0; i < 10; i++) {
                assertThat(BlockPath.computeExistingBlockPath(testConfig, i)).isNull();
            }
            // send a block persisted notification for the range we just created
            blockMessaging.sendBlockPersisted(
                    new PersistedNotification(0, 9, toTest.defaultPriority(), BlockSource.PUBLISHER));
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
         * a gap in the current batch is detected. We expect that the plugin
         * will not submit a zipping task because this is the happy path test
         * where we will be able to catch the gap in the 'contains' precheck.
         * Another scenario (not for this test) is when the precheck passes,
         * but then the batching logic will not be able to collect everything.
         * That would be expected to happen due to the async nature of the
         * system as a whole.
         */
        @Test
        @DisplayName("Test no zip will be created when a gap in the current batch is detected happy path")
        void testNoZipForGapInCurrentBatchHappyPath() throws IOException {
            // generate a gap
            // generate first 3 blocks from numbers 0-2 and add them to the
            // test historical block facility
            for (int i = 0; i < 3; i++) {
                final BlockItemUnparsed[] block = SimpleTestBlockItemBuilder.createSimpleBlockUnparsedWithNumber(i);
                testHistoricalBlockFacility.handleBlockItemsReceived(new BlockItems(List.of(block), i), false);
            }
            // generate next blocks with a gap and make sure we reach the
            // threshold and add them to the test historical block facility
            // numbers 5-9
            for (int i = 5; i < 10; i++) {
                final BlockItemUnparsed[] block = SimpleTestBlockItemBuilder.createSimpleBlockUnparsedWithNumber(i);
                testHistoricalBlockFacility.handleBlockItemsReceived(new BlockItems(List.of(block), i), false);
            }
            // we now have blocks 0-2 and 5-9, so we have a gap
            // assert that none of the first 10 blocks are zipped yet
            for (int i = 0; i < 10; i++) {
                assertThat(BlockPath.computeExistingBlockPath(testConfig, i)).isNull();
            }
            // send a block persisted notification for the range we last created
            blockMessaging.sendBlockPersisted(
                    new PersistedNotification(5, 9, toTest.defaultPriority() + 1, BlockSource.PUBLISHER));
            // assert that no zipping task was submitted
            final boolean anyTaskSubmitted = pluginExecutor.wasAnyTaskSubmitted();
            assertThat(anyTaskSubmitted).isFalse();
            // assert that the first 10 blocks are not zipped
            for (int i = 0; i < 10; i++) {
                assertThat(BlockPath.computeExistingBlockPath(testConfig, i)).isNull();
                assertThat(toTest.availableBlocks().contains(i)).isFalse();
            }
        }

        /**
         * This test aims to verify that the plugin will not zip anything when
         * a gap in the current batch is detected. We expect that the plugin
         * will submit a zipping task, because we here simulate that the
         * availability precheck passes, but the batching logic is not able
         * to collect everything due to the async nature of the system. Unlike
         * the happy path test, this test will be able to catch the gap later.
         */
        @Test
        @DisplayName(
                "Test no zip will be created when a gap in the current batch is detected after successful precheck")
        void testNoZipForGapInCurrentBatchSuccessfulPrecheck() throws IOException {
            // generate a gap
            // generate first 3 blocks from numbers 0-2 and add them to the
            // test historical block facility
            for (int i = 0; i < 3; i++) {
                final BlockItemUnparsed[] block = SimpleTestBlockItemBuilder.createSimpleBlockUnparsedWithNumber(i);
                testHistoricalBlockFacility.handleBlockItemsReceived(new BlockItems(List.of(block), i), false);
            }
            // generate next blocks with a gap and make sure we reach the
            // threshold and add them to the test historical block facility
            // numbers 5-9
            for (int i = 5; i < 10; i++) {
                final BlockItemUnparsed[] block = SimpleTestBlockItemBuilder.createSimpleBlockUnparsedWithNumber(i);
                testHistoricalBlockFacility.handleBlockItemsReceived(new BlockItems(List.of(block), i), false);
            }
            // we now have blocks 0-2 and 5-9, so we have a gap
            // assert that none of the first 10 blocks are zipped yet
            for (int i = 0; i < 10; i++) {
                assertThat(BlockPath.computeExistingBlockPath(testConfig, i)).isNull();
            }
            // set a temporary override for the available blocks to contain
            // the first 10 blocks, this will simulate that the precheck passes
            // but later on we will still be able to detect the gap
            final SimpleBlockRangeSet temporaryAvailableBlocks = new SimpleBlockRangeSet();
            temporaryAvailableBlocks.add(0, 10);
            testHistoricalBlockFacility.setTemporaryAvailableBlocks(temporaryAvailableBlocks);
            // send a block persisted notification for the range we last created
            blockMessaging.sendBlockPersisted(
                    new PersistedNotification(5, 9, toTest.defaultPriority() + 1, BlockSource.PUBLISHER));
            // assert that no zipping task was submitted
            final boolean anyTaskSubmitted = pluginExecutor.wasAnyTaskSubmitted();
            assertThat(anyTaskSubmitted).isTrue();
            // make sure the task is executed
            pluginExecutor.executeSerially();
            // assert that the first 10 blocks are not zipped
            for (int i = 0; i < 10; i++) {
                assertThat(BlockPath.computeExistingBlockPath(testConfig, i)).isNull();
                assertThat(toTest.availableBlocks().contains(i)).isFalse();
            }
        }

        /**
         * This test aims to verify that the plugin will not zip anything when
         * an IOException occurs during the zipping process. This is when a task
         * is successfully submitted to the executor, but the zipping itself
         * fails.
         */
        @Test
        @DisplayName("Test no zip when IOException occurs")
        void testNoZipWhenIOException() throws IOException {
            // generate first 10 blocks from numbers 0-9 and add them to the
            // test historical block facility
            for (int i = 0; i < 10; i++) {
                final BlockItemUnparsed[] block = SimpleTestBlockItemBuilder.createSimpleBlockUnparsedWithNumber(i);
                testHistoricalBlockFacility.handleBlockItemsReceived(new BlockItems(List.of(block), i), false);
            }
            // assert that none of the first 10 blocks are zipped yet
            for (int i = 0; i < 10; i++) {
                assertThat(BlockPath.computeExistingBlockPath(testConfig, i)).isNull();
            }
            // simulate an IOException by manipulating the target zip file path
            // compute the path to the zip file that would be created
            final Path targetZipFilePath =
                    BlockPath.computeBlockPath(testConfig, 0).zipFilePath();
            Files.createDirectories(targetZipFilePath.getParent());
            Files.createFile(targetZipFilePath);
            Files.setPosixFilePermissions(targetZipFilePath, Collections.emptySet()); // no permissions
            // send a block persisted notification for the range we just created
            blockMessaging.sendBlockPersisted(
                    new PersistedNotification(0, 9, toTest.defaultPriority() + 1, BlockSource.PUBLISHER));
            // execute serially to ensure all tasks are completed
            pluginExecutor.executeSerially();
            // assert that no blocks are zipped/available
            for (int i = 0; i < 10; i++) {
                assertThat(targetZipFilePath).isRegularFile().isEmptyFile();
                assertThat(toTest.availableBlocks().contains(i)).isFalse();
            }
        }

        /**
         * This test aims to verify that a block accessor will be available for
         * the blocks that have been zipped after they have been zipped.
         */
        @Test
        @DisplayName("Test happy path zip block accessor")
        void testZipRangeBlockAccessor() {
            // generate first 10 blocks from numbers 0-9 and add them to the
            // test historical block facility
            for (int i = 0; i < 10; i++) {
                final BlockItemUnparsed[] block = SimpleTestBlockItemBuilder.createSimpleBlockUnparsedWithNumber(i);
                testHistoricalBlockFacility.handleBlockItemsReceived(new BlockItems(List.of(block), i), false);
            }
            // assert that none of the first 10 blocks have accessors yet
            for (int i = 0; i < 10; i++) {
                assertThat(toTest.block(i)).isNull();
            }
            // send a block persisted notification for the range we just created
            blockMessaging.sendBlockPersisted(
                    new PersistedNotification(0, 9, toTest.defaultPriority() + 1, BlockSource.PUBLISHER));
            // execute serially to ensure all tasks are completed
            pluginExecutor.executeSerially();
            // assert that the first 10 blocks will have an accessor
            for (int i = 0; i < 10; i++) {
                assertThat(toTest.block(i)).isNotNull();
            }
        }

        /**
         * This test aims to verify that available block accessors will contain
         * the correct contents (or will rather be able to supply the correct
         * contents) after the blocks have been zipped.
         */
        @Test
        @DisplayName("Test happy path zip block accessor contents")
        void testZipRangeBlockAccessorContents() {
            // generate first 10 blocks from numbers 0-9 and add them to the
            // test historical block facility
            final List<BlockUnparsed> expectedBlocks = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                final BlockItemUnparsed[] block = SimpleTestBlockItemBuilder.createSimpleBlockUnparsedWithNumber(i);
                testHistoricalBlockFacility.handleBlockItemsReceived(new BlockItems(List.of(block), i), false);
                expectedBlocks.add(new BlockUnparsed(List.of(block)));
            }
            // assert that none of the first 10 blocks have accessors yet
            for (int i = 0; i < 10; i++) {
                assertThat(toTest.block(i)).isNull();
            }
            // send a block persisted notification for the range we just created
            blockMessaging.sendBlockPersisted(
                    new PersistedNotification(0, 9, toTest.defaultPriority() + 1, BlockSource.PUBLISHER));
            // execute serially to ensure all tasks are completed
            pluginExecutor.executeSerially();
            // assert the contents of the now available block accessors
            for (int i = 0; i < 10; i++) {
                final BlockAccessor blockAccessor = toTest.block(i);
                final BlockUnparsed actual = blockAccessor.blockUnparsed();
                assertThat(actual).isEqualTo(expectedBlocks.get(i));
            }
        }

        /**
         * This test aims to verify that the plugin will proceed to send a
         * {@link PersistedNotification} with the correct range of blocks
         * after a successful archival.
         */
        @Test
        @DisplayName("Test happy path zip successful notification sent")
        void testZipRangeHappyPathNotificationSent() throws IOException {
            // generate first 10 blocks from numbers 0-9 and add them to the
            // test historical block facility
            for (int i = 0; i < 10; i++) {
                final BlockItemUnparsed[] block = SimpleTestBlockItemBuilder.createSimpleBlockUnparsedWithNumber(i);
                testHistoricalBlockFacility.handleBlockItemsReceived(new BlockItems(List.of(block), i), false);
            }
            // assert that none of the first 10 blocks are zipped yet
            for (int i = 0; i < 10; i++) {
                assertThat(BlockPath.computeExistingBlockPath(testConfig, i)).isNull();
            }
            // send a block persisted notification for the range we just created
            blockMessaging.sendBlockPersisted(
                    new PersistedNotification(0, 9, toTest.defaultPriority() + 1, BlockSource.PUBLISHER));
            // execute serially to ensure all tasks are completed
            pluginExecutor.executeSerially();
            // assert that a persistence notification was sent, we expect 2
            // notifications total, one in the beginning of this test and one
            // sent by the plugin itself
            final List<PersistedNotification> sentPersistedNotifications =
                    blockMessaging.getSentPersistedNotifications();
            assertThat(sentPersistedNotifications)
                    .isNotEmpty()
                    .hasSize(2)
                    .element(1)
                    .returns(0L, PersistedNotification::startBlockNumber)
                    .returns(9L, PersistedNotification::endBlockNumber)
                    .returns(toTest.defaultPriority(), PersistedNotification::blockProviderPriority);
        }

        /**
         * This test aims to verify that the plugin will properly update it's
         * available blocks list after a successful archival.
         */
        @Test
        @DisplayName("Test happy path zip blocks in range")
        void testZipRangeHappyPathBlocksInRange() {
            // generate first 10 blocks from numbers 0-9 and add them to the
            // test historical block facility
            for (int i = 0; i < 10; i++) {
                final BlockItemUnparsed[] block = SimpleTestBlockItemBuilder.createSimpleBlockUnparsedWithNumber(i);
                testHistoricalBlockFacility.handleBlockItemsReceived(new BlockItems(List.of(block), i), false);
            }
            // assert that none of the first 10 blocks appear in the available range
            for (int i = 0; i < 10; i++) {
                assertThat(toTest.availableBlocks().contains(i)).isFalse();
            }
            // send a block persisted notification for the range we just created
            blockMessaging.sendBlockPersisted(
                    new PersistedNotification(0, 9, toTest.defaultPriority() + 1, BlockSource.PUBLISHER));
            // execute serially to ensure all tasks are completed
            pluginExecutor.executeSerially();
            // assert that the first 10 blocks now appear in the available range
            final BlockRangeSet availableBlocks = toTest.availableBlocks();
            for (int i = 0; i < 10; i++) {
                final int blockNumber = i;
                assertThat(availableBlocks).returns(true, set -> set.contains(blockNumber));
            }
            assertThat(availableBlocks).returns(0L, BlockRangeSet::min).returns(9L, BlockRangeSet::max);
        }

        /**
         * This test aims to verify that the plugin will not write data if
         * an exception occurs just before actually writing anything.
         */
        @Test
        @DisplayName("Test exception during move no data written")
        void testExceptionDuringMoveNoDataWritten() throws IOException {
            // generate first 10 blocks from numbers 0-9 and add them to the
            // test historical block facility
            for (int i = 0; i < 10; i++) {
                final BlockItemUnparsed[] block = SimpleTestBlockItemBuilder.createSimpleBlockUnparsedWithNumber(i);
                testHistoricalBlockFacility.handleBlockItemsReceived(new BlockItems(List.of(block), i), false);
            }
            // assert that none of the first 10 blocks are zipped yet
            for (int i = 0; i < 10; i++) {
                assertThat(BlockPath.computeExistingBlockPath(testConfig, i)).isNull();
            }
            // send a block persisted notification for the range we just created
            blockMessaging.sendBlockPersisted(
                    new PersistedNotification(0, 9, toTest.defaultPriority() + 1, BlockSource.PUBLISHER));
            // calculate the target zip path that we expect the plugin to create
            final Path targetZipPath = BlockPath.computeBlockPath(testConfig, 0).zipFilePath();
            Files.createDirectories(targetZipPath.getParent());
            // create the file with no permissions to simulate a failure later on
            Files.createFile(targetZipPath);
            Files.setPosixFilePermissions(targetZipPath, Collections.emptySet());
            // execute serially to ensure all tasks are completed
            pluginExecutor.executeSerially();
            // assert that the first 10 blocks are not zipped, but revert proper
            // perms so we can assert the file is deleted
            assertThat(targetZipPath).isEmptyFile();
        }

        /**
         * This test aims to verify that the plugin will not return any block
         * accessors for any blocks in the batch that failed exceptionally.
         */
        @Test
        @DisplayName("Test exception during move no accessors available")
        void testExceptionDuringMoveNoAccessorsAvailable() throws IOException {
            // generate first 10 blocks from numbers 0-9 and add them to the
            // test historical block facility
            for (int i = 0; i < 10; i++) {
                final BlockItemUnparsed[] block = SimpleTestBlockItemBuilder.createSimpleBlockUnparsedWithNumber(i);
                testHistoricalBlockFacility.handleBlockItemsReceived(new BlockItems(List.of(block), i), false);
            }
            // assert that none of the first 10 blocks are zipped yet
            for (int i = 0; i < 10; i++) {
                assertThat(BlockPath.computeExistingBlockPath(testConfig, i)).isNull();
            }
            // send a block persisted notification for the range we just created
            blockMessaging.sendBlockPersisted(
                    new PersistedNotification(0, 9, toTest.defaultPriority() + 1, BlockSource.PUBLISHER));
            // calculate the target zip path that we expect the plugin to create
            final Path targetZipPath = BlockPath.computeBlockPath(testConfig, 0).zipFilePath();
            Files.createDirectories(targetZipPath.getParent());
            // create the file with no permissions to simulate a failure later on
            Files.createFile(targetZipPath);
            Files.setPosixFilePermissions(targetZipPath, Collections.emptySet());
            // execute serially to ensure all tasks are completed
            pluginExecutor.executeSerially();
            // assert that no accessor will be returned for the blocks
            for (int i = 0; i < 10; i++) {
                assertThat(toTest.block(i)).isNull();
            }
        }

        /**
         * This test aims to verify that the plugin will not update the range of
         * available blocks with any block numbers of the blocks in the batch
         * that failed exceptionally.
         */
        @Test
        @DisplayName("Test exception during move no available blocks in range")
        void testExceptionDuringMoveNoAvailableBlocksInRange() throws IOException {
            // generate first 10 blocks from numbers 0-9 and add them to the
            // test historical block facility
            for (int i = 0; i < 10; i++) {
                final BlockItemUnparsed[] block = SimpleTestBlockItemBuilder.createSimpleBlockUnparsedWithNumber(i);
                testHistoricalBlockFacility.handleBlockItemsReceived(new BlockItems(List.of(block), i), false);
            }
            // assert that none of the first 10 blocks are zipped yet
            for (int i = 0; i < 10; i++) {
                assertThat(BlockPath.computeExistingBlockPath(testConfig, i)).isNull();
            }
            // send a block persisted notification for the range we just created
            blockMessaging.sendBlockPersisted(
                    new PersistedNotification(0, 9, toTest.defaultPriority() + 1, BlockSource.PUBLISHER));
            // calculate the target zip path that we expect the plugin to create
            final Path targetZipPath = BlockPath.computeBlockPath(testConfig, 0).zipFilePath();
            Files.createDirectories(targetZipPath.getParent());
            // create the file with no permissions to simulate a failure later on
            Files.createFile(targetZipPath);
            Files.setPosixFilePermissions(targetZipPath, Collections.emptySet());
            // execute serially to ensure all tasks are completed
            pluginExecutor.executeSerially();
            // asser that available blocks do not contain any of the blocks
            for (int i = 0; i < 10; i++) {
                assertThat(toTest.availableBlocks().contains(i)).isFalse();
            }
        }

        /**
         * This test aims to verify that the plugin will not send any
         * {@link PersistedNotification} for a zip that failed exceptionally.
         */
        @Test
        @DisplayName("Test exception during move no persistence notification sent")
        void testExceptionDuringMoveNoPersistenceNotificationSent() throws IOException {
            // generate first 10 blocks from numbers 0-9 and add them to the
            // test historical block facility
            for (int i = 0; i < 10; i++) {
                final BlockItemUnparsed[] block = SimpleTestBlockItemBuilder.createSimpleBlockUnparsedWithNumber(i);
                testHistoricalBlockFacility.handleBlockItemsReceived(new BlockItems(List.of(block), i), false);
            }
            // assert that none of the first 10 blocks are zipped yet
            for (int i = 0; i < 10; i++) {
                assertThat(BlockPath.computeExistingBlockPath(testConfig, i)).isNull();
            }
            // send a block persisted notification for the range we just created
            blockMessaging.sendBlockPersisted(
                    new PersistedNotification(0, 9, toTest.defaultPriority() + 1, BlockSource.PUBLISHER));
            // calculate the target zip path that we expect the plugin to create
            final Path targetZipPath = BlockPath.computeBlockPath(testConfig, 0).zipFilePath();
            Files.createDirectories(targetZipPath.getParent());
            // create the file with no permissions to simulate a failure later on
            Files.createFile(targetZipPath);
            Files.setPosixFilePermissions(targetZipPath, Collections.emptySet());
            // execute serially to ensure all tasks are completed
            pluginExecutor.executeSerially();
            // assert that no notification was sent to the block messaging,
            // we expect only one notification to have been sent, the one we
            // initially sent in the beginning of the test
            final int totalSentNotifications =
                    blockMessaging.getSentPersistedNotifications().size();
            assertThat(totalSentNotifications).isEqualTo(1);
        }

        /**
         * This test aims to verify that the plugin will correctly zip the blocks
         * that are available at the time of startup.
         */
        @Test
        @DisplayName("Test happy path zip successful archival from start()")
        void testZipRangeHappyPathArchivalDuringStartup() throws IOException {
            // generate first 10 blocks from numbers 0-9 and add them to the
            // test historical block facility
            for (int i = 0; i < 10; i++) {
                final BlockItemUnparsed[] block = SimpleTestBlockItemBuilder.createSimpleBlockUnparsedWithNumber(i);
                testHistoricalBlockFacility.handleBlockItemsReceived(new BlockItems(List.of(block), i), false);
            }
            // assert that none of the first 10 blocks are zipped yet
            for (int i = 0; i < 10; i++) {
                assertThat(BlockPath.computeExistingBlockPath(testConfig, i)).isNull();
            }
            // assert that no task was ever submitted to the pool until now
            assertThat(pluginExecutor.wasAnyTaskSubmitted()).isFalse();
            // call the start method, we expect that it will queue a new task
            // that we can execute
            toTest.start();
            // execute serially to ensure all tasks are completed
            pluginExecutor.executeSerially();
            // assert that the first 10 blocks are zipped now
            for (int i = 0; i < 10; i++) {
                assertThat(BlockPath.computeExistingBlockPath(testConfig, i)).isNotNull();
            }
        }

        /**
         * This test aims to assert that the plugin will correctly handle the
         * retention policy threshold passed scenario. We must retain as many
         * blocks as the configured retention policy threshold.
         */
        @Test
        @DisplayName("Test retention policy threshold passed happy path")
        void testRetentionPolicyThresholdHappyPath() throws IOException {
            // generate first 150 blocks from numbers 0-149 and add them to the
            // test historical block facility
            for (int i = 0; i < 150; i++) {
                final BlockItemUnparsed[] block = SimpleTestBlockItemBuilder.createSimpleBlockUnparsedWithNumber(i);
                testHistoricalBlockFacility.handleBlockItemsReceived(new BlockItems(List.of(block), i), false);
            }
            // assert that none of the first 150 blocks are zipped yet and are
            // not present in the available blocks
            assertThat(plugin.availableBlocks().size()).isEqualTo(0);
            for (int i = 0; i < 150; i++) {
                assertThat(BlockPath.computeExistingBlockPath(testConfig, i)).isNull();
                assertThat(plugin.availableBlocks().contains(i)).isFalse();
            }
            // send a block persisted notification for the range we just created
            blockMessaging.sendBlockPersisted(
                    new PersistedNotification(0, 149, toTest.defaultPriority() + 1, BlockSource.PUBLISHER));
            // execute serially to ensure all tasks are completed
            pluginExecutor.executeSerially();
            // assert that all blocks are now zipped and the available blocks
            // is updated accordingly (this is just before applying the retention)
            assertThat(plugin.availableBlocks().size()).isEqualTo(150);
            for (int i = 0; i < 150; i++) {
                assertThat(BlockPath.computeExistingBlockPath(testConfig, i)).isNotNull();
                assertThat(plugin.availableBlocks().contains(i)).isTrue();
            }
            // send another notification to trigger the retention policy, we do
            // not need to actually persist the block
            blockMessaging.sendBlockPersisted(
                    new PersistedNotification(150, 150, toTest.defaultPriority() + 1, BlockSource.PUBLISHER));
            // assert that the size of the available blocks is now 100 (post retention policy cleanup)
            assertThat(plugin.availableBlocks().size()).isEqualTo(100);
            // assert that the first 50 blocks were cleaned up and that the
            // available blocks are updated accordingly
            for (int i = 0; i < 50; i++) {
                assertThat(BlockPath.computeExistingBlockPath(testConfig, i)).isNull();
                assertThat(plugin.availableBlocks().contains(i)).isFalse();
            }
            // assert that the rest of the blocks are still zipped and that
            // the available blocks are updated accordingly
            for (int i = 50; i < 150; i++) {
                assertThat(BlockPath.computeExistingBlockPath(testConfig, i)).isNotNull();
                assertThat(plugin.availableBlocks().contains(i)).isTrue();
            }
        }

        /**
         * This test aims to assert that the plugin will not apply the retention
         * policy threshold when it is disabled. We expect that all blocks
         * will remain zipped and available.
         */
        @Test
        @DisplayName("Test retention policy threshold disabled")
        void testRetentionPolicyThresholdDisabled() throws IOException {
            // change the retention policy to be disabled
            testConfig = new FilesHistoricConfig(testTempDir, CompressionType.NONE, 1, 0L);
            // override the config in the plugin
            start(toTest, testHistoricalBlockFacility, getConfigOverrides());
            // generate first 150 blocks from numbers 0-149 and add them to the
            // test historical block facility
            for (int i = 0; i < 150; i++) {
                final BlockItemUnparsed[] block = SimpleTestBlockItemBuilder.createSimpleBlockUnparsedWithNumber(i);
                testHistoricalBlockFacility.handleBlockItemsReceived(new BlockItems(List.of(block), i), false);
            }
            // assert that none of the first 150 blocks are zipped yet and are
            // not present in the available blocks
            assertThat(plugin.availableBlocks().size()).isEqualTo(0);
            for (int i = 0; i < 150; i++) {
                assertThat(BlockPath.computeExistingBlockPath(testConfig, i)).isNull();
                assertThat(plugin.availableBlocks().contains(i)).isFalse();
            }
            // send a block persisted notification for the range we just created
            blockMessaging.sendBlockPersisted(
                    new PersistedNotification(0, 149, toTest.defaultPriority() + 1, BlockSource.PUBLISHER));
            // execute serially to ensure all tasks are completed
            pluginExecutor.executeSerially();
            // assert that all blocks are now zipped and the available blocks
            // is updated accordingly (this is just before applying the retention)
            assertThat(plugin.availableBlocks().size()).isEqualTo(150);
            for (int i = 0; i < 150; i++) {
                assertThat(BlockPath.computeExistingBlockPath(testConfig, i)).isNotNull();
                assertThat(plugin.availableBlocks().contains(i)).isTrue();
            }
            // send another notification to trigger the retention policy, we do
            // not need to actually persist the block
            blockMessaging.sendBlockPersisted(
                    new PersistedNotification(150, 150, toTest.defaultPriority() + 1, BlockSource.PUBLISHER));
            // assert that the size of the available blocks is still 150 (post retention policy cleanup)
            assertThat(plugin.availableBlocks().size()).isEqualTo(150);
            // assert that all the blocks are still zipped and that
            // the available blocks are updated accordingly
            for (int i = 0; i < 150; i++) {
                assertThat(BlockPath.computeExistingBlockPath(testConfig, i)).isNotNull();
                assertThat(plugin.availableBlocks().contains(i)).isTrue();
            }
        }
    }
}
