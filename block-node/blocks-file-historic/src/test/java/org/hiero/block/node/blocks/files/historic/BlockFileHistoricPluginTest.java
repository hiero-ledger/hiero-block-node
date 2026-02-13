// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.blocks.files.historic;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.mockito.Mockito.mock;

import com.hedera.hapi.block.stream.Block;
import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.config.api.Configuration;
import com.swirlds.config.api.ConfigurationBuilder;
import com.swirlds.metrics.api.Metrics;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.app.fixtures.async.BlockingExecutor;
import org.hiero.block.node.app.fixtures.async.ScheduledBlockingExecutor;
import org.hiero.block.node.app.fixtures.async.TestThreadPoolManager;
import org.hiero.block.node.app.fixtures.blocks.TestBlockBuilder;
import org.hiero.block.node.app.fixtures.plugintest.NoOpServiceBuilder;
import org.hiero.block.node.app.fixtures.plugintest.PluginTestBase;
import org.hiero.block.node.app.fixtures.plugintest.SimpleInMemoryHistoricalBlockFacility;
import org.hiero.block.node.app.fixtures.plugintest.TestBlockMessagingFacility;
import org.hiero.block.node.app.fixtures.plugintest.TestHealthFacility;
import org.hiero.block.node.base.CompressionType;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.ServiceBuilder;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.PersistedNotification;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import org.hiero.block.node.spi.historicalblocks.BlockAccessor;
import org.hiero.block.node.spi.historicalblocks.BlockRangeSet;
import org.hiero.block.node.spi.historicalblocks.HistoricalBlockFacility;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for {@link BlockFileHistoricPlugin}.
 */
@DisplayName("BlockFileHistoricPlugin Tests")
class BlockFileHistoricPluginTest {
    /** Data TempDir for the current test */
    private final Path dataRoot;
    /** Working directory where the test will create zip file */
    private final Path zipWorkRoot;
    /** The test block messaging facility to use for testing. */
    private final SimpleInMemoryHistoricalBlockFacility testHistoricalBlockFacility;
    /** The test config to use for the plugin, overridable. */
    private FilesHistoricConfig testConfig;
    /** The instance under test. */
    private final BlockFileHistoricPlugin toTest;

    /**
     * Construct test environment.
     */
    BlockFileHistoricPluginTest(@TempDir final Path tmpDir) {
        dataRoot = Objects.requireNonNull(tmpDir).resolve("blocks");
        zipWorkRoot = Objects.requireNonNull(dataRoot).resolve("zipwork");
        // generate test config, for the purposes of this test, we will always
        // use 10 blocks per zip, assuming that the first zip file will contain
        // for example blocks 0-9, the second zip file will contain blocks 10-19
        // also we will not use compression, and we will use the jUnit temp dir
        testConfig = new FilesHistoricConfig(dataRoot, CompressionType.NONE, 1, 10L, 3);
        // build the plugin using the test environment
        toTest = new BlockFileHistoricPlugin();
        // initialize an in memory historical block facility to use for testing
        testHistoricalBlockFacility = new SimpleInMemoryHistoricalBlockFacility();
    }

    /**
     * Config overrides for the test environment.
     */
    private Map<String, String> getConfigOverrides() {
        final Entry<String, String> rootPath =
                Map.entry("files.historic.rootPath", testConfig.rootPath().toString());
        final Entry<String, String> compression =
                Map.entry("files.historic.compression", testConfig.compression().name());
        final Entry<String, String> powersOfTenPerZipFileContents = Map.entry(
                "files.historic.powersOfTenPerZipFileContents",
                String.valueOf(testConfig.powersOfTenPerZipFileContents()));
        final Entry<String, String> blockRetentionThreshold = Map.entry(
                "files.historic.blockRetentionThreshold", String.valueOf(testConfig.blockRetentionThreshold()));
        return Map.ofEntries(rootPath, compression, powersOfTenPerZipFileContents, blockRetentionThreshold);
    }

    /**
     * Nested class for plugin startup tests.
     */
    @Nested
    @DisplayName("Startup Tests")
    final class StartupTest
            extends PluginTestBase<BlockFileHistoricPlugin, BlockingExecutor, ScheduledExecutorService> {
        /**
         * Test Constructor.
         */
        StartupTest() {
            super(
                    new BlockingExecutor(new LinkedBlockingQueue<>()),
                    new ScheduledBlockingExecutor(new LinkedBlockingQueue<>()));
        }

        /**
         * This test aims to assert that the links and data root directories are
         * created on plugin startup.
         */
        @Test
        @DisplayName("Staging and data roots are created on startup")
        void testStagingAndDataRootsCreated() throws IOException {
            // Assert that the roots do not exist
            assertThat(dataRoot).doesNotExist();
            assertThat(zipWorkRoot).doesNotExist();
            // Now start the plugin
            start(toTest, testHistoricalBlockFacility, getConfigOverrides());
            // Assert that the roots are created
            assertThat(dataRoot).exists().isDirectory().isNotEmptyDirectory();
            assertThat(zipWorkRoot).exists().isDirectory().isEmptyDirectory();
            final Path linksRoot = dataRoot.resolve("links");
            assertThat(linksRoot).exists().isDirectory().isEmptyDirectory();
            final Path stagingRoot = dataRoot.resolve("staging");
            assertThat(stagingRoot).exists().isDirectory().isEmptyDirectory();
            try (final Stream<Path> subDirectoriesStream = Files.list(dataRoot)) {
                assertThat(subDirectoriesStream.toList())
                        .hasSize(3)
                        .containsExactlyInAnyOrder(zipWorkRoot, linksRoot, stagingRoot);
            }
        }
    }

    /**
     * Constructor and Init tests.
     */
    @Nested
    @DisplayName("Constructor & Init Tests")
    final class ConstructorAndInitTests {
        /**
         * This test aims to verify that the no args constructor of
         * {@link BlockFileHistoricPlugin} does not throw any exceptions.
         */
        @Test
        @DisplayName("Test no args constructor does not throw any exceptions")
        void testNoArgsConstructor() {
            assertThatNoException().isThrownBy(BlockFileHistoricPlugin::new);
        }

        /**
         * This test aims to verify that the
         * {@link BlockFileHistoricPlugin#init(BlockNodeContext, ServiceBuilder)}
         * method throws a {@link NullPointerException} if the context is null.
         */
        @Test
        @DisplayName("Test init throws null pointer when supplied with null context")
        void testInitNullContext() {
            final BlockFileHistoricPlugin toTest = new BlockFileHistoricPlugin();
            assertThatNullPointerException().isThrownBy(() -> toTest.init(null, new NoOpServiceBuilder()));
        }

        /**
         * This test aims to verify that the
         * {@link BlockFileHistoricPlugin#init(BlockNodeContext, ServiceBuilder)}
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
                    new TestThreadPoolManager<>(
                            new BlockingExecutor(new LinkedBlockingQueue<>()),
                            new ScheduledBlockingExecutor(new LinkedBlockingQueue<>())));
            // call
            final BlockFileHistoricPlugin toTest = new BlockFileHistoricPlugin();
            assertThatNoException().isThrownBy(() -> toTest.init(testContext, null));
        }

        /**
         * This test aims to verify that the plugin correctly renames old format
         * archive files ({@code *s.zip}) to the new format ({@code *.zip}) during
         * initialization. This ensures backwards compatibility when upgrading from
         * the old naming scheme.
         */
        @Test
        @DisplayName("Test init renames old format archives (*s.zip to *.zip)")
        void testInitRenamesOldFormatArchives(@TempDir final Path tempDir) throws IOException {
            // Create a links directory structure with old format zip files
            final Path subDir1 = tempDir.resolve("000").resolve("000");
            final Path subDir2 = tempDir.resolve("000").resolve("001");
            Files.createDirectories(subDir1);
            Files.createDirectories(subDir2);

            // Create old format zip files (ending with 's.zip')
            final Path oldFile1 = subDir1.resolve("0000s.zip");
            final Path oldFile2 = subDir1.resolve("1000s.zip");
            final Path oldFile3 = subDir2.resolve("10000s.zip");
            Files.writeString(oldFile1, "test content 1");
            Files.writeString(oldFile2, "test content 2");
            Files.writeString(oldFile3, "test content 3");

            // Verify old format files exist before initialization
            assertThat(oldFile1).exists();
            assertThat(oldFile2).exists();
            assertThat(oldFile3).exists();

            // Initialize the plugin (this should trigger the rename)
            final BlockFileHistoricPlugin plugin = new BlockFileHistoricPlugin();
            final FilesHistoricConfig configuration = ConfigurationBuilder.create()
                    .withConfigDataType(FilesHistoricConfig.class)
                    .withValue("files.historic.rootPath", tempDir.toString())
                    .build()
                    .getConfigData(FilesHistoricConfig.class);

            plugin.renameOldFormatArchives(configuration.rootPath());

            // Verify old format files no longer exist
            assertThat(oldFile1).doesNotExist();
            assertThat(oldFile2).doesNotExist();
            assertThat(oldFile3).doesNotExist();

            // Verify new format files exist with correct content
            assertThat(subDir1.resolve("0000.zip")).exists();
            assertThat(subDir1.resolve("1000.zip")).exists();
            assertThat(subDir2.resolve("10000.zip")).exists();
        }
    }

    /**
     * Plugin tests.
     */
    @Nested
    @DisplayName("Plugin Tests")
    final class PluginTests
            extends PluginTestBase<BlockFileHistoricPlugin, BlockingExecutor, ScheduledBlockingExecutor> {
        /** The test block serial executor service to use for the plugin. */
        private final BlockingExecutor pluginExecutor;

        /**
         * Construct plugin base.
         */
        PluginTests() {
            super(
                    new BlockingExecutor(new LinkedBlockingQueue<>()),
                    new ScheduledBlockingExecutor(new LinkedBlockingQueue<>()));
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
                final BlockUnparsed block =
                        TestBlockBuilder.generateBlockWithNumber(i).blockUnparsed();
                blockMessaging.sendBlockVerification(new VerificationNotification(
                        true, i, Bytes.EMPTY, new BlockUnparsed(block.blockItems()), BlockSource.PUBLISHER));
            }
            // assert that none of the first 10 blocks are zipped yet
            for (int i = 0; i < 10; i++) {
                assertThat(BlockPath.computeExistingBlockPath(testConfig, i)).isNull();
            }
            // execute serially to ensure all tasks are completed
            pluginExecutor.executeSerially();
            // assert that the first 10 blocks are zipped now
            for (int i = 0; i < 10; i++) {
                assertThat(BlockPath.computeExistingBlockPath(testConfig, i)).isNotNull();
            }
            assertThat(zipWorkRoot).exists().isDirectory().isEmptyDirectory();
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
                final BlockUnparsed block =
                        TestBlockBuilder.generateBlockWithNumber(i).blockUnparsed();
                blockMessaging.sendBlockVerification(new VerificationNotification(
                        true, i, Bytes.EMPTY, new BlockUnparsed(block.blockItems()), BlockSource.PUBLISHER));
            }
            // assert that none of the first 20 blocks are zipped yet
            for (int i = 0; i < 20; i++) {
                assertThat(BlockPath.computeExistingBlockPath(testConfig, i)).isNull();
            }
            // execute serially to ensure all tasks are completed
            pluginExecutor.executeSerially();
            // assert that the first 20 blocks are zipped now
            for (int i = 0; i < 20; i++) {
                assertThat(BlockPath.computeExistingBlockPath(testConfig, i)).isNotNull();
            }
            assertThat(zipWorkRoot).exists().isDirectory().isEmptyDirectory();
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
                final BlockUnparsed block =
                        TestBlockBuilder.generateBlockWithNumber(i).blockUnparsed();
                blockMessaging.sendBlockVerification(new VerificationNotification(
                        true, i, Bytes.EMPTY, new BlockUnparsed(block.blockItems()), BlockSource.PUBLISHER));
            }
            // assert that none of the first 20 blocks are zipped yet
            for (int i = 0; i < 14; i++) {
                assertThat(BlockPath.computeExistingBlockPath(testConfig, i)).isNull();
            }
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
            assertThat(zipWorkRoot).exists().isDirectory().isEmptyDirectory();
        }

        @Test
        @DisplayName("Test zip successful archival after failed batch")
        void testZipRangeHappyPathNotGettingStuckAfterFail() throws IOException {
            // generate first 19 blocks from numbers 0-19 and add them to the
            // test historical block facility
            // skip block 4 to simulate missing block
            for (int i = 0; i < 20; i++) {
                if (i == 4) {
                    continue;
                }
                final BlockUnparsed block =
                        TestBlockBuilder.generateBlockWithNumber(i).blockUnparsed();
                blockMessaging.sendBlockVerification(new VerificationNotification(
                        true, i, Bytes.EMPTY, new BlockUnparsed(block.blockItems()), BlockSource.PUBLISHER));
            }
            // assert that none of the first 20 blocks are zipped yet
            for (int i = 0; i < 20; i++) {
                assertThat(BlockPath.computeExistingBlockPath(testConfig, i)).isNull();
            }
            // execute serially to ensure all tasks are completed
            pluginExecutor.executeSerially();

            // assert that the first 10 blocks are not zipped because of missing block 4
            for (int i = 0; i < 10; i++) {
                assertThat(BlockPath.computeExistingBlockPath(testConfig, i)).isNull();
            }
            // assert that the next block range is zipped
            for (int i = 10; i < 20; i++) {
                assertThat(BlockPath.computeExistingBlockPath(testConfig, i)).isNotNull();
            }
            assertThat(zipWorkRoot).exists().isDirectory().isEmptyDirectory();
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
                final BlockUnparsed block =
                        TestBlockBuilder.generateBlockWithNumber(i).blockUnparsed();
                blockMessaging.sendBlockVerification(new VerificationNotification(
                        true, i, Bytes.EMPTY, new BlockUnparsed(block.blockItems()), BlockSource.PUBLISHER));
                expectedBlocks.add(new BlockUnparsed(block.blockItems()));
            }
            // assert that none of the first 10 blocks are zipped yet
            for (int i = 0; i < 10; i++) {
                assertThat(BlockPath.computeExistingBlockPath(testConfig, i)).isNull();
            }
            // execute serially to ensure all tasks are completed
            pluginExecutor.executeSerially();
            // assert the contents of the zip file
            for (int i = 0; i < 10; i++) {
                final BlockPath blockPath = BlockPath.computeExistingBlockPath(testConfig, i);
                try (final FileSystem zipFs = FileSystems.newFileSystem(blockPath.zipFilePath())) {
                    // assert that the zip entry exists
                    final Path zipEntryPath = zipFs.getPath(blockPath.blockFileName());
                    assertThat(zipEntryPath).exists().isRegularFile();
                    final byte[] zipEntryBytes =
                            blockPath.compressionType().decompress(Files.readAllBytes(zipEntryPath));
                    final BlockUnparsed actual = BlockUnparsed.PROTOBUF.parse(Bytes.wrap(zipEntryBytes));
                    assertThat(actual).isEqualTo(expectedBlocks.get(i));
                    // assert that the block file exists
                    assertThat(Files.exists(zipFs.getPath(blockPath.blockFileName())))
                            .isTrue();
                }
            }
            assertThat(zipWorkRoot).exists().isDirectory().isEmptyDirectory();
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
                final BlockUnparsed block =
                        TestBlockBuilder.generateBlockWithNumber(i).blockUnparsed();
                blockMessaging.sendBlockVerification(new VerificationNotification(
                        true, i, Bytes.EMPTY, new BlockUnparsed(block.blockItems()), BlockSource.PUBLISHER));
            }
            // assert that none of the first 5 blocks are zipped yet
            for (int i = 0; i < 5; i++) {
                assertThat(BlockPath.computeExistingBlockPath(testConfig, i)).isNull();
            }
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
                final BlockUnparsed block =
                        TestBlockBuilder.generateBlockWithNumber(i).blockUnparsed();
                blockMessaging.sendBlockVerification(new VerificationNotification(
                        true, i, Bytes.EMPTY, new BlockUnparsed(block.blockItems()), BlockSource.PUBLISHER));
            }
            // assert that none of the first 10 blocks are zipped yet
            for (int i = 0; i < 10; i++) {
                assertThat(BlockPath.computeExistingBlockPath(testConfig, i)).isNull();
            }
            // execute serially to ensure all tasks are completed
            pluginExecutor.executeSerially();
            // assert that the first 10 blocks are zipped now
            for (int i = 0; i < 10; i++) {
                assertThat(BlockPath.computeExistingBlockPath(testConfig, i)).isNotNull();
            }
            assertThat(zipWorkRoot).exists().isDirectory().isEmptyDirectory();
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
                final BlockUnparsed block =
                        TestBlockBuilder.generateBlockWithNumber(i).blockUnparsed();
                blockMessaging.sendBlockVerification(new VerificationNotification(
                        true, i, Bytes.EMPTY, new BlockUnparsed(block.blockItems()), BlockSource.PUBLISHER));
            }
            // generate next blocks with a gap and make sure we reach the
            // threshold and add them to the test historical block facility
            // numbers 5-9
            for (int i = 5; i < 10; i++) {
                final BlockUnparsed block =
                        TestBlockBuilder.generateBlockWithNumber(i).blockUnparsed();
                blockMessaging.sendBlockVerification(new VerificationNotification(
                        true, i, Bytes.EMPTY, new BlockUnparsed(block.blockItems()), BlockSource.PUBLISHER));
            }
            // we now have blocks 0-2 and 5-9, so we have a gap
            // assert that none of the first 10 blocks are zipped yet
            for (int i = 0; i < 10; i++) {
                assertThat(BlockPath.computeExistingBlockPath(testConfig, i)).isNull();
            }
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
         * This test aims to verify that the plugin can successfully archive blocks
         * that are received out of chronological order and can backfill earlier
         * ranges after initially zipping later blocks. The test simulates a scenario
         * where blocks arrive in non-sequential order (e.g., blocks 42-63 first,
         * then 30-41, then 0-14, then 15-22) and ensures that each complete batch
         * (per the configured 10 blocks per zip) is properly archived as it becomes
         * available, regardless of the arrival order.
         */
        @Test
        @DisplayName("Test successful archival of earlier block ranges received out of order")
        void testZipHandlesOutOfOrderBlocks() throws IOException {
            // Send blocks 42-63 first to simulate receiving a later range of blocks
            // before earlier ones. This establishes the out-of-order scenario.
            for (int i = 42; i < 64; i++) {
                final BlockUnparsed block =
                        TestBlockBuilder.generateBlockWithNumber(i).blockUnparsed();
                blockMessaging.sendBlockVerification(new VerificationNotification(
                        true, i, Bytes.EMPTY, new BlockUnparsed(block.blockItems()), BlockSource.PUBLISHER));
            }

            // This should pass, as we have a whole batch in the queue
            pluginExecutor.executeSerially();

            // We expect only blocks 50-59 to be zipped (a complete batch of 10 blocks).
            // Blocks 0-41 are not available yet, and blocks 42-49 and 60-63 don't form a
            // complete batch.
            for (int i = 0; i < 50; i++) {
                assertThat(BlockPath.computeExistingBlockPath(testConfig, i)).isNull();
            }
            for (int i = 50; i < 60; i++) {
                assertThat(BlockPath.computeExistingBlockPath(testConfig, i)).isNotNull();
            }
            for (int i = 60; i < 70; i++) {
                assertThat(BlockPath.computeExistingBlockPath(testConfig, i)).isNull();
            }

            // Send blocks 30-41 to backfill an earlier range. This will combine with
            // previously received blocks 42-49 to form additional complete batches.
            for (int i = 30; i < 42; i++) {
                final BlockUnparsed block =
                        TestBlockBuilder.generateBlockWithNumber(i).blockUnparsed();
                blockMessaging.sendBlockVerification(new VerificationNotification(
                        true, i, Bytes.EMPTY, new BlockUnparsed(block.blockItems()), BlockSource.PUBLISHER));
            }

            pluginExecutor.executeSerially();

            // We expect blocks 30-49 to be zipped now (two complete batches: 30-39 and 40-49).
            // The previously zipped batch 50-59 remains zipped.
            // Blocks 0-29 are still not available, and blocks 60-63 remain incomplete.
            for (int i = 0; i < 30; i++) {
                assertThat(BlockPath.computeExistingBlockPath(testConfig, i)).isNull();
            }
            for (int i = 30; i < 60; i++) {
                assertThat(BlockPath.computeExistingBlockPath(testConfig, i)).isNotNull();
            }
            for (int i = 60; i < 70; i++) {
                assertThat(BlockPath.computeExistingBlockPath(testConfig, i)).isNull();
            }

            // Send blocks 0-14 to backfill the earliest range. This will form at least
            // one complete batch (0-9) while 10-14 remain incomplete.
            for (int i = 0; i < 15; i++) {
                final BlockUnparsed block =
                        TestBlockBuilder.generateBlockWithNumber(i).blockUnparsed();
                blockMessaging.sendBlockVerification(new VerificationNotification(
                        true, i, Bytes.EMPTY, new BlockUnparsed(block.blockItems()), BlockSource.PUBLISHER));
            }

            pluginExecutor.executeSerially();

            // We expect blocks 0-9 to be zipped now (complete batch).
            // Blocks 10-29 are still not available or incomplete.
            // Previously zipped batches (30-49 and 50-59) remain zipped.
            // Blocks 60-69 remain incomplete.
            for (int i = 0; i < 10; i++) {
                assertThat(BlockPath.computeExistingBlockPath(testConfig, i)).isNotNull();
            }
            for (int i = 10; i < 30; i++) {
                assertThat(BlockPath.computeExistingBlockPath(testConfig, i)).isNull();
            }
            for (int i = 30; i < 50; i++) {
                assertThat(BlockPath.computeExistingBlockPath(testConfig, i)).isNotNull();
            }
            for (int i = 60; i < 70; i++) {
                assertThat(BlockPath.computeExistingBlockPath(testConfig, i)).isNull();
            }

            // Send blocks 15-22 to fill another gap. Combined with previously received
            // blocks 10-14, this will complete the batch 10-19.
            for (int i = 15; i < 23; i++) {
                final BlockUnparsed block =
                        TestBlockBuilder.generateBlockWithNumber(i).blockUnparsed();
                blockMessaging.sendBlockVerification(new VerificationNotification(
                        true, i, Bytes.EMPTY, new BlockUnparsed(block.blockItems()), BlockSource.PUBLISHER));
            }

            pluginExecutor.executeSerially();

            // We expect blocks 0-19 to be zipped now (batches 0-9 and 10-19).
            // Blocks 20-29 are still incomplete.
            // Previously zipped batches (30-49 and 50-59) remain zipped.
            // Blocks 60-69 remain incomplete.
            for (int i = 0; i < 20; i++) {
                assertThat(BlockPath.computeExistingBlockPath(testConfig, i)).isNotNull();
            }
            for (int i = 20; i < 30; i++) {
                assertThat(BlockPath.computeExistingBlockPath(testConfig, i)).isNull();
            }
            for (int i = 30; i < 50; i++) {
                assertThat(BlockPath.computeExistingBlockPath(testConfig, i)).isNotNull();
            }
            for (int i = 60; i < 70; i++) {
                assertThat(BlockPath.computeExistingBlockPath(testConfig, i)).isNull();
            }

            final long submittedTasks = pluginExecutor.getTaskCount();

            // Send blocks 23-27 to assert that if we don't fill the whole gap, no zipping task is submitted
            for (int i = 23; i < 27; i++) {
                final BlockUnparsed block =
                        TestBlockBuilder.generateBlockWithNumber(i).blockUnparsed();
                blockMessaging.sendBlockVerification(new VerificationNotification(
                        true, i, Bytes.EMPTY, new BlockUnparsed(block.blockItems()), BlockSource.PUBLISHER));
            }

            // assert that no zipping task was submited since last check,
            // because there is no complete batch 20-29 yet
            assertThat(pluginExecutor.getTaskCount()).isEqualTo(submittedTasks);
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
                final BlockUnparsed block =
                        TestBlockBuilder.generateBlockWithNumber(i).blockUnparsed();
                blockMessaging.sendBlockVerification(new VerificationNotification(
                        true, i, Bytes.EMPTY, new BlockUnparsed(block.blockItems()), BlockSource.PUBLISHER));
            }
            // generate next blocks with a gap and make sure we reach the
            // threshold and add them to the test historical block facility
            // numbers 5-9
            for (int i = 5; i < 10; i++) {
                final BlockUnparsed block =
                        TestBlockBuilder.generateBlockWithNumber(i).blockUnparsed();
                blockMessaging.sendBlockVerification(new VerificationNotification(
                        true, i, Bytes.EMPTY, new BlockUnparsed(block.blockItems()), BlockSource.PUBLISHER));
            }
            // we now have blocks 0-2 and 5-9, so we have a gap
            // assert that none of the first 10 blocks are zipped yet
            for (int i = 0; i < 10; i++) {
                assertThat(BlockPath.computeExistingBlockPath(testConfig, i)).isNull();
            }
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
                final BlockUnparsed block =
                        TestBlockBuilder.generateBlockWithNumber(i).blockUnparsed();
                blockMessaging.sendBlockVerification(new VerificationNotification(
                        true, i, Bytes.EMPTY, new BlockUnparsed(block.blockItems()), BlockSource.PUBLISHER));
            }
            // assert that none of the first 10 blocks are zipped yet
            for (int i = 0; i < 10; i++) {
                assertThat(BlockPath.computeExistingBlockPath(testConfig, i)).isNull();
            }
            // simulate an IOException by manipulating the target zip file path
            // compute the path to the zip file that would be created
            final Path targetZipFilePath =
                    BlockPath.computeBlockPath(testConfig, 0).zipFilePath();
            final Path targetZipDir = targetZipFilePath.getParent();
            Files.createDirectories(targetZipDir);
            // Remove write permissions from the parent directory to simulate IOException
            Files.setPosixFilePermissions(targetZipDir, Collections.emptySet());
            // execute serially to ensure all tasks are completed
            pluginExecutor.executeSerially();
            // assert that no blocks are zipped/available
            for (int i = 0; i < 10; i++) {
                assertThat(toTest.availableBlocks().contains(i)).isFalse();
            }
            assertThat(zipWorkRoot).exists().isDirectory().isEmptyDirectory();
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
                final BlockUnparsed block =
                        TestBlockBuilder.generateBlockWithNumber(i).blockUnparsed();
                blockMessaging.sendBlockVerification(new VerificationNotification(
                        true, i, Bytes.EMPTY, new BlockUnparsed(block.blockItems()), BlockSource.PUBLISHER));
            }
            // assert that none of the first 10 blocks have accessors yet
            for (int i = 0; i < 10; i++) {
                assertThat(toTest.block(i)).isNull();
            }
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
                final BlockUnparsed block =
                        TestBlockBuilder.generateBlockWithNumber(i).blockUnparsed();
                blockMessaging.sendBlockVerification(new VerificationNotification(
                        true, i, Bytes.EMPTY, new BlockUnparsed(block.blockItems()), BlockSource.PUBLISHER));
                expectedBlocks.add(new BlockUnparsed(block.blockItems()));
            }
            // assert that none of the first 10 blocks have accessors yet
            for (int i = 0; i < 10; i++) {
                assertThat(toTest.block(i)).isNull();
            }
            // execute serially to ensure all tasks are completed
            pluginExecutor.executeSerially();
            // assert the contents of the now available block accessors
            for (int i = 0; i < 10; i++) {
                try (final BlockAccessor blockAccessor = toTest.block(i)) {
                    final BlockUnparsed actual = blockAccessor.blockUnparsed();
                    assertThat(actual).isEqualTo(expectedBlocks.get(i));
                }
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
                final BlockUnparsed block =
                        TestBlockBuilder.generateBlockWithNumber(i).blockUnparsed();
                blockMessaging.sendBlockVerification(new VerificationNotification(
                        true, i, Bytes.EMPTY, new BlockUnparsed(block.blockItems()), BlockSource.PUBLISHER));
            }
            // assert that none of the first 10 blocks are zipped yet
            for (int i = 0; i < 10; i++) {
                assertThat(BlockPath.computeExistingBlockPath(testConfig, i)).isNull();
            }
            // execute serially to ensure all tasks are completed
            pluginExecutor.executeSerially();
            // assert that a persistence notification was sent, we expect 2
            // notifications total, one in the beginning of this test and one
            // sent by the plugin itself
            final List<PersistedNotification> sentPersistedNotifications =
                    blockMessaging.getSentPersistedNotifications();
            assertThat(sentPersistedNotifications)
                    .isNotEmpty()
                    .hasSize(1)
                    .element(0)
                    .returns(true, PersistedNotification::succeeded)
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
                final BlockUnparsed block =
                        TestBlockBuilder.generateBlockWithNumber(i).blockUnparsed();
                blockMessaging.sendBlockVerification(new VerificationNotification(
                        true, i, Bytes.EMPTY, new BlockUnparsed(block.blockItems()), BlockSource.PUBLISHER));
            }
            // assert that none of the first 10 blocks appear in the available range
            for (int i = 0; i < 10; i++) {
                assertThat(toTest.availableBlocks().contains(i)).isFalse();
            }
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
                final BlockUnparsed block =
                        TestBlockBuilder.generateBlockWithNumber(i).blockUnparsed();
                blockMessaging.sendBlockVerification(new VerificationNotification(
                        true, i, Bytes.EMPTY, new BlockUnparsed(block.blockItems()), BlockSource.PUBLISHER));
            }
            // assert that none of the first 10 blocks are zipped yet
            for (int i = 0; i < 10; i++) {
                assertThat(BlockPath.computeExistingBlockPath(testConfig, i)).isNull();
            }
            // calculate the target zip path that we expect the plugin to create
            final Path targetZipPath = BlockPath.computeBlockPath(testConfig, 0).zipFilePath();
            final Path targetZipDir = targetZipPath.getParent();
            Files.createDirectories(targetZipDir);
            // Remove write permissions from the parent directory to simulate a failure
            Files.setPosixFilePermissions(targetZipDir, PosixFilePermissions.fromString("r-x------"));
            // execute serially to ensure all tasks are completed
            pluginExecutor.executeSerially();
            // assert that the first 10 blocks are not zipped
            assertThat(targetZipPath).doesNotExist();
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
                final BlockUnparsed block =
                        TestBlockBuilder.generateBlockWithNumber(i).blockUnparsed();
                blockMessaging.sendBlockVerification(new VerificationNotification(
                        true, i, Bytes.EMPTY, new BlockUnparsed(block.blockItems()), BlockSource.PUBLISHER));
            }
            // assert that none of the first 10 blocks are zipped yet
            for (int i = 0; i < 10; i++) {
                assertThat(BlockPath.computeExistingBlockPath(testConfig, i)).isNull();
            }
            // calculate the target zip path that we expect the plugin to create
            final Path targetZipPath = BlockPath.computeBlockPath(testConfig, 0).zipFilePath();
            final Path targetZipDir = targetZipPath.getParent();
            Files.createDirectories(targetZipDir);
            // Remove write permissions from the parent directory to simulate a failure
            Files.setPosixFilePermissions(targetZipDir, Collections.emptySet());
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
                final BlockUnparsed block =
                        TestBlockBuilder.generateBlockWithNumber(i).blockUnparsed();
                blockMessaging.sendBlockVerification(new VerificationNotification(
                        true, i, Bytes.EMPTY, new BlockUnparsed(block.blockItems()), BlockSource.PUBLISHER));
            }
            // assert that none of the first 10 blocks are zipped yet
            for (int i = 0; i < 10; i++) {
                assertThat(BlockPath.computeExistingBlockPath(testConfig, i)).isNull();
            }
            // calculate the target zip path that we expect the plugin to create
            final Path targetZipPath = BlockPath.computeBlockPath(testConfig, 0).zipFilePath();
            final Path targetZipDir = targetZipPath.getParent();
            Files.createDirectories(targetZipDir);
            // Remove write permissions from the parent directory to simulate a failure
            Files.setPosixFilePermissions(targetZipDir, Collections.emptySet());
            // execute serially to ensure all tasks are completed
            pluginExecutor.executeSerially();
            // assert that available blocks do not contain any of the blocks
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
                final BlockUnparsed block =
                        TestBlockBuilder.generateBlockWithNumber(i).blockUnparsed();
                blockMessaging.sendBlockVerification(new VerificationNotification(
                        true, i, Bytes.EMPTY, new BlockUnparsed(block.blockItems()), BlockSource.PUBLISHER));
            }
            // assert that none of the first 10 blocks are zipped yet
            for (int i = 0; i < 10; i++) {
                assertThat(BlockPath.computeExistingBlockPath(testConfig, i)).isNull();
            }
            // calculate the target zip path that we expect the plugin to create
            final Path targetZipPath = BlockPath.computeBlockPath(testConfig, 0).zipFilePath();
            final Path targetZipDir = targetZipPath.getParent();
            Files.createDirectories(targetZipDir);
            // Remove write permissions from the parent directory to simulate a failure
            Files.setPosixFilePermissions(targetZipDir, Collections.emptySet());
            // execute serially to ensure all tasks are completed
            pluginExecutor.executeSerially();
            // assert that no notification was sent to the block messaging
            final int totalSentNotifications =
                    blockMessaging.getSentPersistedNotifications().size();
            assertThat(totalSentNotifications).isEqualTo(0);
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
                final BlockUnparsed block =
                        TestBlockBuilder.generateBlockWithNumber(i).blockUnparsed();
                blockMessaging.sendBlockVerification(new VerificationNotification(
                        true, i, Bytes.EMPTY, new BlockUnparsed(block.blockItems()), BlockSource.PUBLISHER));
            }
            // assert that none of the first 10 blocks are zipped yet
            for (int i = 0; i < 10; i++) {
                assertThat(BlockPath.computeExistingBlockPath(testConfig, i)).isNull();
            }
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
                final BlockUnparsed block =
                        TestBlockBuilder.generateBlockWithNumber(i).blockUnparsed();
                blockMessaging.sendBlockVerification(new VerificationNotification(
                        true, i, Bytes.EMPTY, new BlockUnparsed(block.blockItems()), BlockSource.PUBLISHER));
            }
            // assert that none of the first 150 blocks are zipped yet and are
            // not present in the available blocks
            assertThat(plugin.availableBlocks().size()).isZero();
            for (int i = 0; i < 150; i++) {
                assertThat(BlockPath.computeExistingBlockPath(testConfig, i)).isNull();
                assertThat(plugin.availableBlocks().contains(i)).isFalse();
            }
            // execute serially to ensure all tasks are completed
            pluginExecutor.executeSerially();
            // assert that all blocks are now zipped and the available blocks
            // is updated accordingly (this is just before applying the retention)
            assertThat(plugin.availableBlocks().size()).isEqualTo(100);
            for (int i = 0; i < 50; i++) {
                assertThat(BlockPath.computeExistingBlockPath(testConfig, i)).isNull();
                assertThat(plugin.availableBlocks().contains(i)).isFalse();
            }
            for (int i = 50; i < 150; i++) {
                assertThat(BlockPath.computeExistingBlockPath(testConfig, i)).isNotNull();
                assertThat(plugin.availableBlocks().contains(i)).isTrue();
            }

            final BlockUnparsed block =
                    TestBlockBuilder.generateBlockWithNumber(150).blockUnparsed();
            blockMessaging.sendBlockVerification(new VerificationNotification(
                    true, 150, Bytes.EMPTY, new BlockUnparsed(block.blockItems()), BlockSource.PUBLISHER));
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
            testConfig = new FilesHistoricConfig(dataRoot, CompressionType.NONE, 1, 0L, 3);
            // override the config in the plugin
            start(toTest, testHistoricalBlockFacility, getConfigOverrides());
            // generate first 150 blocks from numbers 0-149 and add them to the
            // test historical block facility
            for (int i = 0; i < 150; i++) {
                final BlockUnparsed block =
                        TestBlockBuilder.generateBlockWithNumber(i).blockUnparsed();
                blockMessaging.sendBlockVerification(new VerificationNotification(
                        true, i, Bytes.EMPTY, new BlockUnparsed(block.blockItems()), BlockSource.PUBLISHER));
            }
            // assert that none of the first 150 blocks are zipped yet and are
            // not present in the available blocks
            assertThat(plugin.availableBlocks().size()).isEqualTo(0);
            for (int i = 0; i < 150; i++) {
                assertThat(BlockPath.computeExistingBlockPath(testConfig, i)).isNull();
                assertThat(plugin.availableBlocks().contains(i)).isFalse();
            }
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
            final BlockUnparsed block =
                    TestBlockBuilder.generateBlockWithNumber(150).blockUnparsed();
            blockMessaging.sendBlockVerification(new VerificationNotification(
                    true, 150, Bytes.EMPTY, new BlockUnparsed(block.blockItems()), BlockSource.PUBLISHER));
            // assert that the size of the available blocks is still 150 (post retention policy cleanup)
            assertThat(plugin.availableBlocks().size()).isEqualTo(150);
            // assert that all the blocks are still zipped and that
            // the available blocks are updated accordingly
            for (int i = 0; i < 150; i++) {
                assertThat(BlockPath.computeExistingBlockPath(testConfig, i)).isNotNull();
                assertThat(plugin.availableBlocks().contains(i)).isTrue();
            }
        }

        /**
         * This test verifies that the retention policy correctly manages available blocks when
         * blocks are added in multiple batches that exceed the retention threshold. The test
         * simulates a scenario where an initial large set of blocks (100-199) is archived,
         * followed by an additional smaller batch (200-209) that triggers the retention policy
         * to clean up the oldest blocks while maintaining the configured threshold of 100 blocks.
         * This ensures that the plugin properly prunes old archives while keeping the most recent
         * blocks within the retention limit.
         */
        @Test
        @DisplayName("Test retention policy with complex block additions spanning multiple batches")
        void testRetentionPolicyWithComplexBlockAdditions() throws IOException {
            // Add first batch of 100 blocks (100-199) to simulate an initial archive
            // set that fills the retention threshold exactly
            for (int i = 100; i < 200; i++) {
                final BlockItemUnparsed[] block =
                        TestBlockBuilder.generateBlockWithNumber(i).asBlockItemUnparsedArray();
                blockMessaging.sendBlockVerification(new VerificationNotification(
                        true, i, Bytes.EMPTY, new BlockUnparsed(List.of(block)), BlockSource.PUBLISHER));
            }

            // Execute all pending archival tasks to zip the first batch
            pluginExecutor.executeSerially();

            // Verify that all 100 blocks from the first batch are now archived and available.
            // At this point, we are exactly at the retention threshold (100 blocks).
            assertThat(plugin.availableBlocks().size()).isEqualTo(100);
            for (int i = 100; i < 200; i++) {
                assertThat(BlockPath.computeExistingBlockPath(testConfig, i)).isNotNull();
                assertThat(plugin.availableBlocks().contains(i)).isTrue();
            }

            // Add a second batch of 10 blocks (200-209). This will push us over the
            // retention threshold, triggering cleanup of the oldest blocks.
            for (int i = 200; i < 210; i++) {
                final BlockItemUnparsed[] block =
                        TestBlockBuilder.generateBlockWithNumber(i).asBlockItemUnparsedArray();
                blockMessaging.sendBlockVerification(new VerificationNotification(
                        true, i, Bytes.EMPTY, new BlockUnparsed(List.of(block)), BlockSource.PUBLISHER));
            }

            // Execute all pending archival tasks. The retention policy should kick in
            // and remove the oldest 10 blocks (100-109) to maintain the 100-block threshold.
            pluginExecutor.executeSerially();

            // Calculate the total blocks and range after retention policy cleanup
            final long totalBlocksAfterRetention = plugin.availableBlocks().size();
            final long minAvailableBlock =
                    plugin.availableBlocks().stream().min().orElse(-1);
            final long maxAvailableBlock =
                    plugin.availableBlocks().stream().max().orElse(-1);

            // Verify retention policy worked correctly: exactly 100 blocks should remain,
            // ranging from 110 (oldest retained) to 209 (newest), with blocks 100-109 removed.
            assertThat(totalBlocksAfterRetention).isEqualTo(100);
            assertThat(minAvailableBlock).isEqualTo(110);
            assertThat(maxAvailableBlock).isEqualTo(209);
        }

        /**
         * This test verifies that the retention policy correctly handles pre-existing archived
         * blocks that were created before plugin startup, combined with newly added blocks.
         */
        @Test
        @DisplayName("Test retention policy with pre-existing archives and incremental additions")
        void testRetentionPolicyWithPreExistingArchives() throws IOException {
            // Create 90 pre-existing zip entries to simulate blocks that were archived
            // in a previous session before this plugin instance started
            for (int i = 0; i < 90; i++) {
                createPreExistingZipEntry(i);
            }

            // Verify that all pre-existing zip files were created successfully on disk
            for (int i = 0; i < 90; i++) {
                final Path zipPath = BlockPath.computeBlockPath(testConfig, i).zipFilePath();
                assertThat(zipPath).exists().isRegularFile();
            }

            // Start the plugin. It should discover and register the 90 pre-existing archives.
            start(toTest, testHistoricalBlockFacility, getConfigOverrides());

            // Verify that the plugin recognized all 90 pre-existing blocks during startup
            // and added them to the available blocks set
            assertThat(plugin.availableBlocks().size()).isEqualTo(90);
            for (int i = 0; i < 90; i++) {
                assertThat(plugin.availableBlocks().contains(i)).isTrue();
                assertThat(BlockPath.computeExistingBlockPath(testConfig, i)).isNotNull();
            }

            // Add 10 new blocks (90-99) to reach exactly the retention threshold of 100 blocks.
            // No cleanup should occur yet since we are exactly at the limit.
            for (int i = 90; i < 100; i++) {
                final BlockItemUnparsed[] block =
                        TestBlockBuilder.generateBlockWithNumber(i).asBlockItemUnparsedArray();
                blockMessaging.sendBlockVerification(new VerificationNotification(
                        true, i, Bytes.EMPTY, new BlockUnparsed(List.of(block)), BlockSource.PUBLISHER));
            }

            // Execute archival tasks for blocks 90-99
            pluginExecutor.executeSerially();

            // Verify we now have exactly 100 blocks (90 pre-existing + 10 new),
            // which is at the retention threshold limit
            assertThat(plugin.availableBlocks().size()).isEqualTo(100);
            for (int i = 0; i < 100; i++) {
                assertThat(plugin.availableBlocks().contains(i)).isTrue();
                assertThat(BlockPath.computeExistingBlockPath(testConfig, i)).isNotNull();
            }

            // Add 10 more blocks (100-109) to exceed the retention threshold.
            // This should trigger the retention policy to remove the oldest 10 blocks (0-9).
            for (int i = 100; i < 110; i++) {
                final BlockItemUnparsed[] block =
                        TestBlockBuilder.generateBlockWithNumber(i).asBlockItemUnparsedArray();
                blockMessaging.sendBlockVerification(new VerificationNotification(
                        true, i, Bytes.EMPTY, new BlockUnparsed(List.of(block)), BlockSource.PUBLISHER));
            }

            // Execute archival tasks. Retention policy should kick in and remove blocks 0-9.
            pluginExecutor.executeSerially();

            // Verify retention policy maintained the 100-block limit
            final long blocksAfterRetention = plugin.availableBlocks().size();
            assertThat(blocksAfterRetention).isEqualTo(100L);

            // Verify that the oldest 10 pre-existing blocks (0-9) were removed by retention policy
            for (int i = 0; i < 10; i++) {
                assertThat(plugin.availableBlocks().contains(i)).isFalse();
                assertThat(BlockPath.computeExistingBlockPath(testConfig, i)).isNull();
            }

            // Verify that the recently added blocks (100-109) are available
            for (int i = 100; i < 110; i++) {
                assertThat(plugin.availableBlocks().contains(i)).isTrue();
                assertThat(BlockPath.computeExistingBlockPath(testConfig, i)).isNotNull();
            }
        }

        /**
         * This test verifies that when the plugin is configured to allow zipping multiple times,
         * duplicate block verification notifications will result in re-archiving the batch,
         * which updates the zip file's modification timestamp. This behavior is controlled by
         * the {@code allowZippingMultipleTimes} configuration flag. This contrasts with the default
         * idempotent behavior where batches are archived only once.
         */
        @Test
        @DisplayName("Test configuration allows re-zipping same batch when enabled")
        void testConfigAllowsZippingMultipleTimes() throws IOException {
            // Configure plugin with allowZippingMultipleTimes = true (last parameter).
            // This special configuration allows the same batch to be re-archived when
            // duplicate notifications arrive, unlike the default idempotent behavior.
            testConfig = new FilesHistoricConfig(dataRoot, CompressionType.NONE, 1, 10L, 3);
            start(toTest, testHistoricalBlockFacility, getConfigOverrides());

            // Send the first set of block verification notifications (blocks 0-9).
            // This represents the initial arrival of blocks that need to be archived.
            for (int i = 0; i < 10; i++) {
                final BlockUnparsed block =
                        TestBlockBuilder.generateBlockWithNumber(i).blockUnparsed();
                blockMessaging.sendBlockVerification(new VerificationNotification(
                        true, i, Bytes.EMPTY, new BlockUnparsed(block.blockItems()), BlockSource.PUBLISHER));
            }

            // Execute all pending archival tasks. This should zip blocks 0-9 into a single archive.
            pluginExecutor.executeSerially();

            // Verify that the batch was successfully archived: all blocks should have zip paths
            // and should be tracked in the plugin's available blocks range.
            for (int i = 0; i < 10; i++) {
                assertThat(BlockPath.computeExistingBlockPath(testConfig, i)).isNotNull();
                assertThat(plugin.availableBlocks().contains(i)).isTrue();
            }

            // Capture the zip file's last modified timestamp from the first archival.
            // This will be compared against the timestamp after re-archiving to verify
            // that the file was actually modified (not skipped).
            final FileTime lastModifiedTime = Files.getLastModifiedTime(
                    BlockPath.computeExistingBlockPath(testConfig, 0).zipFilePath());

            // Send duplicate verification notifications for the same blocks (0-9).
            // With allowZippingMultipleTimes enabled, this should trigger re-archiving
            // instead of being skipped as it would be with the default configuration.
            for (int i = 0; i < 10; i++) {
                final BlockUnparsed block =
                        TestBlockBuilder.generateBlockWithNumber(i).blockUnparsed();
                blockMessaging.sendBlockVerification(new VerificationNotification(
                        true, i, Bytes.EMPTY, new BlockUnparsed(block.blockItems()), BlockSource.PUBLISHER));
            }

            // Execute pending tasks. With allowZippingMultipleTimes = true, this should
            // re-archive the batch, creating a new zip file with an updated timestamp.
            pluginExecutor.executeSerially();

            // Capture the zip file's timestamp after the second archival attempt.
            final FileTime lastModifiedTimeSecondPass = Files.getLastModifiedTime(
                    BlockPath.computeExistingBlockPath(testConfig, 0).zipFilePath());

            // Assert that the zip file was modified during the second pass. The newer timestamp
            // confirms that with allowZippingMultipleTimes enabled, the plugin does re-archive
            // batches instead of maintaining idempotent behavior.
            assertThat(lastModifiedTime).isLessThan(lastModifiedTimeSecondPass);
        }

        /**
         * Helper method to create a pre-existing zip entry for a block before plugin startup.
         * This simulates blocks that were previously archived.
         *
         * @param blockNumber the block number to create
         * @throws IOException if an error occurs creating the zip file
         */
        private void createPreExistingZipEntry(final long blockNumber) throws IOException {
            final BlockPath blockPath = BlockPath.computeBlockPath(testConfig, blockNumber);
            final BlockItem[] blockItems =
                    TestBlockBuilder.generateBlockWithNumber(blockNumber).asBlockItemArray();
            final Block block = new Block(List.of(blockItems));
            final Bytes blockBytes = Block.PROTOBUF.toBytes(block);
            final byte[] bytesToWrite = blockBytes.toByteArray();

            // Create directory structure
            Files.createDirectories(blockPath.dirPath());

            // Create or append to zip file
            if (Files.notExists(blockPath.zipFilePath())) {
                // Create new zip file
                Files.createFile(blockPath.zipFilePath());
                try (final ZipOutputStream zipOut =
                        new ZipOutputStream(Files.newOutputStream(blockPath.zipFilePath()))) {
                    final ZipEntry zipEntry = new ZipEntry(blockPath.blockFileName());
                    zipOut.putNextEntry(zipEntry);
                    zipOut.write(bytesToWrite);
                    zipOut.closeEntry();
                }
            } else {
                // Append to existing zip file
                final Path tempZip = blockPath.zipFilePath().resolveSibling("temp.zip");
                try (final FileSystem zipFs = FileSystems.newFileSystem(blockPath.zipFilePath());
                        final Stream<Path> entriesStream = Files.list(zipFs.getPath("/"));
                        final ZipOutputStream zipOut = new ZipOutputStream(Files.newOutputStream(tempZip))) {
                    // Copy existing entries
                    for (final Path entry : entriesStream.toList()) {
                        zipOut.putNextEntry(new ZipEntry(entry.getFileName().toString()));
                        try (final InputStream inputStream = Files.newInputStream(entry)) {
                            inputStream.transferTo(zipOut);
                        }
                        zipOut.closeEntry();
                    }
                    // Add the new entry
                    final ZipEntry newEntry = new ZipEntry(blockPath.blockFileName());
                    zipOut.putNextEntry(newEntry);
                    zipOut.write(bytesToWrite);
                    zipOut.closeEntry();
                }
                Files.move(tempZip, blockPath.zipFilePath(), StandardCopyOption.REPLACE_EXISTING);
            }
        }
    }

    /**
     * Regression tests.
     */
    @Nested
    @DisplayName("Regression Tests")
    final class RegressionTests
            extends PluginTestBase<BlockFileHistoricPlugin, BlockingExecutor, ScheduledExecutorService> {
        /** The test historical block facility scoped to this regression scenario. */
        private final SimpleInMemoryHistoricalBlockFacility regressionHistoricalBlockFacility =
                new SimpleInMemoryHistoricalBlockFacility();

        RegressionTests() {
            super(
                    new BlockingExecutor(new LinkedBlockingQueue<>()),
                    new ScheduledBlockingExecutor(new LinkedBlockingQueue<>()));
        }

        private Map<String, String> buildConfigOverrides(final FilesHistoricConfig config) {
            final Entry<String, String> rootPath =
                    Map.entry("files.historic.rootPath", config.rootPath().toString());
            final Entry<String, String> compression =
                    Map.entry("files.historic.compression", config.compression().name());
            final Entry<String, String> powersOfTenPerZipFileContents = Map.entry(
                    "files.historic.powersOfTenPerZipFileContents",
                    String.valueOf(config.powersOfTenPerZipFileContents()));
            final Entry<String, String> blockRetentionThreshold = Map.entry(
                    "files.historic.blockRetentionThreshold", String.valueOf(config.blockRetentionThreshold()));
            return Map.ofEntries(rootPath, compression, powersOfTenPerZipFileContents, blockRetentionThreshold);
        }

        @Test
        @DisplayName("init moves corrupted zip file without shutting down")
        void initMovesCorruptedZipWithoutShutdown() throws IOException {
            final Path corruptedRoot = dataRoot.resolve("corrupted-zip-root");
            testConfig = new FilesHistoricConfig(corruptedRoot, CompressionType.NONE, 1, 10L, 3);

            final BlockPath corruptedZipLocation = BlockPath.computeBlockPath(testConfig, 0L);
            Files.createDirectories(corruptedZipLocation.dirPath());
            // Intentionally write non-zip data so opening it as a FileSystem fails.
            Files.writeString(corruptedZipLocation.zipFilePath(), "this-is-not-a-valid-zip");

            final BlockFileHistoricPlugin pluginUnderTest = new BlockFileHistoricPlugin();
            start(pluginUnderTest, regressionHistoricalBlockFacility, buildConfigOverrides(testConfig));

            final TestHealthFacility healthFacility = (TestHealthFacility) blockNodeContext.serverHealth();
            assertThat(healthFacility.shutdownCalled.get()).isFalse();
            assertThat(Files.exists(corruptedZipLocation.zipFilePath())).isFalse();
            final Path quarantinedZip = corruptedRoot
                    .resolve("corrupted")
                    .resolve(corruptedRoot.relativize(corruptedZipLocation.zipFilePath()));
            assertThat(Files.exists(quarantinedZip)).isTrue();
            assertThat(pluginUnderTest.availableBlocks().size()).isZero();
        }
    }
}
