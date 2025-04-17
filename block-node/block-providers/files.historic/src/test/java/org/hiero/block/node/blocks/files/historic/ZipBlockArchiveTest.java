// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.blocks.files.historic;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.assertj.core.api.Assertions.from;

import com.hedera.hapi.block.stream.Block;
import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.config.api.Configuration;
import com.swirlds.config.api.ConfigurationBuilder;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;
import org.hiero.block.node.app.fixtures.blocks.SimpleTestBlockItemBuilder;
import org.hiero.block.node.app.fixtures.plugintest.SimpleInMemoryHistoricalBlockFacility;
import org.hiero.block.node.app.fixtures.plugintest.TestHealthFacility;
import org.hiero.block.node.base.BlockFile;
import org.hiero.block.node.base.CompressionType;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.historicalblocks.BlockAccessor;
import org.hiero.block.node.spi.historicalblocks.BlockRangeSet;
import org.hiero.hapi.block.node.BlockItemUnparsed;
import org.hiero.hapi.block.node.BlockUnparsed;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Test class for {@link ZipBlockArchive}.
 */
@DisplayName("ZipBlockArchive Tests")
class ZipBlockArchiveTest {
    /** The {@link SimpleInMemoryHistoricalBlockFacility} used for testing. */
    private SimpleInMemoryHistoricalBlockFacility historicalBlockProvider;
    /** The {@link BlockNodeContext} used for testing. */
    private BlockNodeContext testContext;
    /** The default test {@link FilesHistoricConfig} used for testing. */
    private FilesHistoricConfig testConfig;
    /** Temp dir used for testing as the File abstraction is not supported by jimfs */
    @TempDir
    private Path tempDir;
    /** The {@link ZipBlockArchive} instance to be tested. */
    private ZipBlockArchive toTest;

    /**
     * Environment setup before each test.
     */
    @BeforeEach
    void setup() {
        final Configuration configuration = ConfigurationBuilder.create()
                .withConfigDataType(FilesHistoricConfig.class)
                .build();
        // we need a custom test config, because if we resolve it using the
        // config dependency, we cannot resolve the path using the jimfs
        // once we are able to override existing config converters, we will no
        // longer need this createTestConfiguration and the production logic
        // can also be simplified and to always get the configuration via the
        // block context
        testConfig = createTestConfiguration(tempDir, 1);
        historicalBlockProvider = new SimpleInMemoryHistoricalBlockFacility();
        testContext = new BlockNodeContext(
                configuration, null, new TestHealthFacility(), null, historicalBlockProvider, null);
        toTest = new ZipBlockArchive(testContext, testConfig);
    }

    /**
     * Constructor tests for {@link ZipBlockArchive}.
     */
    @Nested
    @DisplayName("Constructor Tests")
    final class ConstructorTests {
        /**
         * This test aims to assert that the constructor of
         * {@link ZipBlockArchive} does not throw an exception when the input
         * argument is valid.
         */
        @Test
        @DisplayName("Test constructor with valid input")
        void testConstructorValidInput() {
            assertThatNoException().isThrownBy(() -> new ZipBlockArchive(testContext, testConfig));
        }

        /**
         * This test aims to assert that the constructor of
         * {@link ZipBlockArchive} throws a {@link NullPointerException} when
         * the input argument is null.
         */
        @Test
        @DisplayName("Test constructor with null input")
        @SuppressWarnings("all")
        void testConstructorNullInput() {
            assertThatNullPointerException().isThrownBy(() -> new ZipBlockArchive(null, testConfig));
        }
    }

    /**
     * Functional tests for {@link ZipBlockArchive}.
     */
    @Nested
    @DisplayName("Functional Tests")
    final class FunctionalTests {
        /**
         * This test aims to assert that the
         * {@link ZipBlockArchive#minStoredBlockNumber()} returns -1L if the
         * archive is empty.
         */
        @Test
        @DisplayName("Test minStoredBlockNumber() returns -1L when zip file is not present")
        void testMinStoredNoZipFile() {
            final ZipBlockArchive toTest = new ZipBlockArchive(testContext, testConfig);
            // assert that server is running before we call actual
            assertThat(testContext.serverHealth().isRunning()).isTrue();
            final long actual = toTest.minStoredBlockNumber();
            // assert that server is still running and -1 is returned
            assertThat(actual).isEqualTo(-1L);
            assertThat(testContext.serverHealth().isRunning()).isTrue();
        }

        /**
         * This test aims to assert that the
         * {@link ZipBlockArchive#minStoredBlockNumber()} calls shutdown on the
         * health facility if an exception occurs.
         */
        @Test
        @DisplayName("Test minStoredBlockNumber() shuts down server if exception occurs")
        void testMinStoredWithException() throws IOException {
            // create test environment, in this case we simply create an empty zip file which will produce
            // an exception when we attempt to look for an entry inside
            final BlockPath computedBlockPath00s = BlockPath.computeBlockPath(testConfig, 0L);
            Files.createDirectories(computedBlockPath00s.dirPath());
            Files.createFile(computedBlockPath00s.zipFilePath());
            // create test instance
            final ZipBlockArchive toTest = new ZipBlockArchive(testContext, testConfig);
            // assert that server is running before we call actual
            assertThat(testContext.serverHealth().isRunning()).isTrue();
            // call
            final long actual = toTest.minStoredBlockNumber();
            // assert no longer running server
            assertThat(actual).isEqualTo(-1L);
            assertThat(testContext.serverHealth().isRunning()).isFalse();
        }

        /**
         * This test aims to assert that the
         * {@link ZipBlockArchive#minStoredBlockNumber()} correctly returns the
         * lowest block number based on single existing zip file.
         */
        @Test
        @DisplayName("Test minStoredBlockNumber() correctly returns lowest block number single zip file")
        void testMinStoredSingleZipFile() throws IOException {
            // create test environment, for this test we need one zip file with two zip entries inside
            final long expected = 3L;
            createAndAddBlockEntry(expected);
            createAndAddBlockEntry(4L);
            // create test instance
            final ZipBlockArchive toTest = new ZipBlockArchive(testContext, testConfig);
            // assert that server is running before we call actual
            assertThat(testContext.serverHealth().isRunning()).isTrue();
            // call
            final long actual = toTest.minStoredBlockNumber();
            // assert expected result and still running server
            assertThat(actual).isEqualTo(expected);
            assertThat(testContext.serverHealth().isRunning()).isTrue();
        }

        /**
         * This test aims to assert that the
         * {@link ZipBlockArchive#minStoredBlockNumber()} correctly returns the
         * lowest block number based on multiple existing zip files.
         */
        @Test
        @DisplayName("Test minStoredBlockNumber() correctly returns lowest block number multiple zip file")
        void testMinStoredMultipleZipFile() throws IOException {
            // create test environment, for this test we need two zip files with two zip entries inside each
            final long expected = 3L;
            createAndAddBlockEntry(expected);
            createAndAddBlockEntry(4L);
            // zip size are 10 blocks, so the following 2 will be in another file
            createAndAddBlockEntry(13L);
            createAndAddBlockEntry(14L);
            // create test instance
            final ZipBlockArchive toTest = new ZipBlockArchive(testContext, testConfig);
            // assert that server is running before we call actual
            assertThat(testContext.serverHealth().isRunning()).isTrue();
            // call
            final long actual = toTest.minStoredBlockNumber();
            // assert expected result and still running server
            assertThat(actual).isEqualTo(expected);
            assertThat(testContext.serverHealth().isRunning()).isTrue();
        }

        /**
         * This test aims to assert that the
         * {@link ZipBlockArchive#maxStoredBlockNumber()} returns -1L if the
         * archive is empty.
         */
        @Test
        @DisplayName("Test maxStoredBlockNumber() returns -1L when zip file is not present")
        void testMaxStoredNoZipFile() {
            final ZipBlockArchive toTest = new ZipBlockArchive(testContext, testConfig);
            // assert that server is running before we call actual
            assertThat(testContext.serverHealth().isRunning()).isTrue();
            final long actual = toTest.maxStoredBlockNumber();
            // assert that server is still running and -1 is returned
            assertThat(actual).isEqualTo(-1L);
            assertThat(testContext.serverHealth().isRunning()).isTrue();
        }

        /**
         * This test aims to assert that the
         * {@link ZipBlockArchive#maxStoredBlockNumber()} calls shutdown on the
         * health facility if an exception occurs.
         */
        @Test
        @DisplayName("Test maxStoredBlockNumber() shuts down server if exception occurs")
        void testMaxStoredWithException() throws IOException {
            // create test environment, in this case we simply create an empty zip file which will produce
            // an exception when we attempt to look for an entry inside
            final BlockPath computedBlockPath00s = BlockPath.computeBlockPath(testConfig, 0L);
            Files.createDirectories(computedBlockPath00s.dirPath());
            Files.createFile(computedBlockPath00s.zipFilePath());
            // create test instance
            final ZipBlockArchive toTest = new ZipBlockArchive(testContext, testConfig);
            // assert that server is running before we call actual
            assertThat(testContext.serverHealth().isRunning()).isTrue();
            // call
            final long actual = toTest.maxStoredBlockNumber();
            // assert no longer running server
            assertThat(actual).isEqualTo(-1L);
            assertThat(testContext.serverHealth().isRunning()).isFalse();
        }

        /**
         * This test aims to assert that the
         * {@link ZipBlockArchive#maxStoredBlockNumber()} correctly returns the
         * highest block number based on single existing zip file.
         */
        @Test
        @DisplayName("Test maxStoredBlockNumber() correctly returns highest block number single zip file")
        void testMaxStoredSingleZipFile() throws IOException {
            // create test environment, for this test we need one zip file with two zip entries inside
            final long expected = 4L;
            createAndAddBlockEntry(3L);
            createAndAddBlockEntry(expected);
            // create test instance
            final ZipBlockArchive toTest = new ZipBlockArchive(testContext, testConfig);
            // assert that server is running before we call actual
            assertThat(testContext.serverHealth().isRunning()).isTrue();
            // call
            final long actual = toTest.maxStoredBlockNumber();
            // assert expected result and still running server
            assertThat(actual).isEqualTo(expected);
            assertThat(testContext.serverHealth().isRunning()).isTrue();
        }

        /**
         * This test aims to assert that the
         * {@link ZipBlockArchive#maxStoredBlockNumber()} correctly returns the
         * highest block number based on multiple existing zip files.
         */
        @Test
        @DisplayName("Test maxStoredBlockNumber() correctly returns highest block number multiple zip file")
        void testMaxStoredMultipleZipFile() throws IOException {
            // create test environment, for this test we need two zip files with two zip entries inside each
            final long expected = 14L;
            createAndAddBlockEntry(3L);
            createAndAddBlockEntry(4L);
            // zip size are 10 blocks, so the following 2 will be in another file
            createAndAddBlockEntry(13L);
            createAndAddBlockEntry(expected);
            // create test instance
            final ZipBlockArchive toTest = new ZipBlockArchive(testContext, testConfig);
            // assert that server is running before we call actual
            assertThat(testContext.serverHealth().isRunning()).isTrue();
            // call
            final long actual = toTest.maxStoredBlockNumber();
            // assert expected result and still running server
            assertThat(actual).isEqualTo(expected);
            assertThat(testContext.serverHealth().isRunning()).isTrue();
        }

        /**
         * This test aims to assert that the
         * {@link ZipBlockArchive#blockAccessor(long)} returns null if no block
         * is present
         */
        @Test
        @DisplayName("Test blockAccessor() returns null when no block is present")
        void testBlockAccessorNoBlocksPresent() {
            final ZipBlockArchive toTest = new ZipBlockArchive(testContext, testConfig);
            // call
            final BlockAccessor actual = toTest.blockAccessor(1L);
            // assert
            assertThat(actual).isNull();
        }

        /**
         * This test aims to assert that the
         * {@link ZipBlockArchive#blockAccessor(long)} returns null if block is
         * not found
         */
        @Test
        @DisplayName("Test blockAccessor() returns null when block is not found")
        void testBlockAccessorBlockNotFound() throws IOException {
            // we create a block with number 0L so we have some block present,
            createAndAddBlockEntry(0L);
            final ZipBlockArchive toTest = new ZipBlockArchive(testContext, testConfig);
            // call
            final BlockAccessor actual = toTest.blockAccessor(1L);
            // assert
            assertThat(actual).isNull();
        }

        /**
         * This test aims to assert that the
         * {@link ZipBlockArchive#blockAccessor(long)} returns a valid
         * {@link ZipBlockAccessor} if the block by the given number exists.
         */
        @Test
        @DisplayName("Test blockAccessor() returns valid ZipBlockAccessor when block with number exists")
        void testBlockAccessorFound() throws IOException {
            // create test environment, for this test we need one zip file with two zip entries inside
            final long targetBlockNumber = 1L;
            final ZipBlockAccessor expected = createAndAddBlockEntry(targetBlockNumber);
            // create test instance
            final ZipBlockArchive toTest = new ZipBlockArchive(testContext, testConfig);
            // call
            final BlockAccessor actual = toTest.blockAccessor(targetBlockNumber);
            // assert
            assertThat(actual)
                    .isNotNull()
                    .isExactlyInstanceOf(ZipBlockAccessor.class)
                    .extracting(BlockAccessor::block)
                    .isEqualTo(expected.block());
        }

        /**
         * This test aims to assert that the
         * {@link ZipBlockArchive#writeNewZipFile(long)} will successfully
         * create the target zip file
         */
        @Test
        @DisplayName("Test writeNewZipFile() successfully creates the zip")
        void testZipSuccessfullyCreated() throws IOException {
            final long firstBlockNumber = 0L;
            // add first 20 blocks to the historical block facility
            // we expect that the zip file will be created with the first 10 blocks
            // because the test config is set to 10 blocks per zip file
            for (int i = (int) firstBlockNumber; i < 20; i++) {
                final List<BlockItemUnparsed> blockItems =
                        List.of(SimpleTestBlockItemBuilder.createSimpleBlockUnparsedWithNumber(i));
                historicalBlockProvider.handleBlockItemsReceived(new BlockItems(blockItems, i));
            }
            // before test assert that all the blocks for numbers 0-19 are present in the block facility
            assertThat(historicalBlockProvider.availableBlocks())
                    .returns(0L, from(BlockRangeSet::min))
                    .returns(19L, from(BlockRangeSet::max));
            // create the instance to test
            final ZipBlockArchive toTest = new ZipBlockArchive(testContext, testConfig);
            // assert no zip file is created yet, the zip file will be all the same
            // for all the 10 zips, so we can rely on asserting based on computed path for the first block
            // expected
            final Path expected =
                    BlockPath.computeBlockPath(testConfig, firstBlockNumber).zipFilePath();
            assertThat(expected).doesNotExist();
            // call
            toTest.writeNewZipFile(firstBlockNumber);
            // assert existing zip file
            assertThat(expected)
                    .exists()
                    .isRegularFile()
                    .isReadable()
                    .isWritable()
                    .isNotEmptyFile()
                    .hasExtension("zip");
        }

        /**
         * This test aims to assert that the
         * {@link ZipBlockArchive#writeNewZipFile(long)} will produce the right
         * contents for the created zip file
         */
        @Test
        @DisplayName("Test writeNewZipFile() zip file has the right contents")
        void testZipContents() throws IOException, ParseException {
            final Map<String, BlockUnparsed> first10BlocksWithExpectedEntryNames = new TreeMap<>();
            final long firstBlockNumber = 0L;
            // add first 20 blocks to the historical block facility
            // we expect that the zip file will be created with the first 10 blocks
            // because the test config is set to 10 blocks per zip file
            // create the instance to test
            for (int i = (int) firstBlockNumber; i < 20; i++) {
                final List<BlockItemUnparsed> blockItems =
                        List.of(SimpleTestBlockItemBuilder.createSimpleBlockUnparsedWithNumber(i));
                if (i < 10) {
                    final String key = BlockFile.blockFileName(i, testConfig.compression());
                    final BlockUnparsed value = new BlockUnparsed(blockItems);
                    first10BlocksWithExpectedEntryNames.put(key, value);
                }
                historicalBlockProvider.handleBlockItemsReceived(new BlockItems(blockItems, i));
            }
            // before test assert that all the blocks for numbers 0-19 are present in the block facility
            assertThat(historicalBlockProvider.availableBlocks())
                    .returns(0L, from(BlockRangeSet::min))
                    .returns(19L, from(BlockRangeSet::max));
            final ZipBlockArchive toTest = new ZipBlockArchive(testContext, testConfig);
            // assert no zip file is created yet, the zip file will be all the same
            // for all the 10 zips, so we can rely on asserting based on computed path for the first block
            // expected
            final Path expected =
                    BlockPath.computeBlockPath(testConfig, firstBlockNumber).zipFilePath();
            assertThat(expected).doesNotExist();
            // call
            toTest.writeNewZipFile(firstBlockNumber);
            // assert that each entry's contents matches what we initially intended
            // to write
            try (final ZipFile zipFile = new ZipFile(expected.toFile())) {
                final Enumeration<? extends ZipEntry> entries = zipFile.entries();
                while (entries.hasMoreElements()) {
                    final ZipEntry zipEntry = entries.nextElement();
                    final BlockUnparsed expectedValue = first10BlocksWithExpectedEntryNames.get(zipEntry.getName());
                    try (final InputStream in = zipFile.getInputStream(zipEntry)) {
                        final byte[] rawContents = in.readAllBytes();
                        final BlockUnparsed actualValue = BlockUnparsed.PROTOBUF.parse(Bytes.wrap(rawContents));
                        assertThat(actualValue).isEqualTo(expectedValue);
                    }
                }
            }
        }
    }

    private ZipBlockAccessor createAndAddBlockEntry(final long blockNumber) throws IOException {
        final BlockPath blockPath = BlockPath.computeBlockPath(testConfig, blockNumber);
        final BlockItem[] blockItems = SimpleTestBlockItemBuilder.createSimpleBlockWithNumber(blockNumber);
        final Block block = new Block(List.of(blockItems));
        final Bytes blockBytes = Block.PROTOBUF.toBytes(block);
        return createAndAddBlockEntry(blockPath, blockBytes.toByteArray());
    }

    private ZipBlockAccessor createAndAddBlockEntry(final BlockPath blockPath, final byte[] bytesToWrite)
            throws IOException {
        // create & assert existing block file path before call
        Files.createDirectories(blockPath.dirPath());
        // it is important the output stream is closed as the compression writes a footer on close
        if (Files.notExists(blockPath.zipFilePath())) {
            Files.createFile(blockPath.zipFilePath());
            try (final ZipOutputStream zipOut = new ZipOutputStream(Files.newOutputStream(blockPath.zipFilePath()))) {
                // create a new zip entry
                final ZipEntry zipEntry = new ZipEntry(blockPath.blockFileName());
                zipOut.putNextEntry(zipEntry);
                zipOut.write(bytesToWrite);
                zipOut.closeEntry();
            }
        } else {
            final Path tempZip = blockPath.zipFilePath().resolveSibling("temp.zip");
            try (final ZipFile zipFile = new ZipFile(blockPath.zipFilePath().toFile());
                    final ZipOutputStream zipOut = new ZipOutputStream(Files.newOutputStream(tempZip))) {
                // Copy existing entries
                final Enumeration<? extends ZipEntry> entries = zipFile.entries();
                while (entries.hasMoreElements()) {
                    final ZipEntry entry = entries.nextElement();
                    zipOut.putNextEntry(new ZipEntry(entry.getName()));
                    try (final InputStream inputStream = zipFile.getInputStream(entry)) {
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
        assertThat(blockPath.zipFilePath())
                .exists()
                .isReadable()
                .isWritable()
                .isNotEmptyFile()
                .hasExtension("zip");
        try (final ZipFile zipFile = new ZipFile(blockPath.zipFilePath().toFile())) {
            final ZipEntry entry = zipFile.getEntry(blockPath.blockFileName());
            assertThat(entry).isNotNull();
            final byte[] fromZipEntry = zipFile.getInputStream(entry).readAllBytes();
            assertThat(fromZipEntry).isEqualTo(bytesToWrite);
        }
        return new ZipBlockAccessor(blockPath);
    }

    private FilesHistoricConfig createTestConfiguration(final Path basePath, final int powersOfTenPerZipFileContents) {
        // for simplicity let's use no compression
        return new FilesHistoricConfig(basePath, CompressionType.NONE, powersOfTenPerZipFileContents);
    }
}
