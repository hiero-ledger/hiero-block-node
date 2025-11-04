// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.blocks.files.historic;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import com.google.common.jimfs.Jimfs;
import com.hedera.hapi.block.stream.Block;
import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.hiero.block.node.app.fixtures.blocks.InMemoryBlockAccessor;
import org.hiero.block.node.app.fixtures.blocks.SimpleTestBlockItemBuilder;
import org.hiero.block.node.app.fixtures.plugintest.TestHealthFacility;
import org.hiero.block.node.base.BlockFile;
import org.hiero.block.node.base.CompressionType;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.historicalblocks.BlockAccessor;
import org.hiero.block.node.spi.historicalblocks.BlockAccessor.Format;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Test class for {@link ZipBlockArchive}.
 */
@DisplayName("ZipBlockArchive Tests")
class ZipBlockArchiveTest {
    /** The {@link BlockNodeContext} used for testing. */
    private BlockNodeContext testContext;
    /** The default test {@link FilesHistoricConfig} used for testing. */
    private FilesHistoricConfig testConfig;
    /** The in-memory filesystem used for testing. */
    private FileSystem jimFs;
    /** The {@link ZipBlockArchive} instance to be tested. */
    private ZipBlockArchive toTest;

    /**
     * Environment setup before each test.
     */
    @BeforeEach
    void setup() throws IOException {
        jimFs = Jimfs.newFileSystem(com.google.common.jimfs.Configuration.unix());
        final Path blocksRoot = jimFs.getPath("/blocks");
        Files.createDirectories(blocksRoot);
        testConfig = createTestConfiguration(blocksRoot, 1);
        // we need this test context because we need the health facility to be
        // available for the tests to run
        testContext = new BlockNodeContext(null, null, new TestHealthFacility(), null, null, null, null);
        // the instance under test
        toTest = new ZipBlockArchive(testContext, testConfig);
    }

    /**
     * Teardown after each test.
     */
    @AfterEach
    void tearDown() throws IOException {
        // close the jimfs filesystem
        if (jimFs != null) {
            jimFs.close();
        }
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
            assertThatNullPointerException().isThrownBy(() -> new ZipBlockArchive(testContext, null));
            assertThatNullPointerException().isThrownBy(() -> new ZipBlockArchive(null, null));
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
        @DisplayName("Test minStoredBlockNumber() Does Not shuts down server if exception occurs")
        void testMinStoredWithException() throws IOException {
            // create test environment, in this case we simply create an empty zip file which will produce
            // an exception when we attempt to look for an entry inside
            final BlockPath computedBlockPath00s = BlockPath.computeBlockPath(testConfig, 0L);
            Files.createDirectories(computedBlockPath00s.dirPath());
            Files.createFile(computedBlockPath00s.zipFilePath());
            // assert that server is running before we call actual
            assertThat(testContext.serverHealth().isRunning()).isTrue();
            // call
            final long actual = toTest.minStoredBlockNumber();
            // assert still running server
            assertThat(actual).isEqualTo(-1L);
            assertThat(testContext.serverHealth().isRunning()).isTrue();
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
        @DisplayName("Test maxStoredBlockNumber() does not shuts down server if exception occurs")
        void testMaxStoredWithException() throws IOException {
            // create test environment, in this case we simply create an empty zip file which will produce
            // an exception when we attempt to look for an entry inside
            final BlockPath computedBlockPath00s = BlockPath.computeBlockPath(testConfig, 0L);
            Files.createDirectories(computedBlockPath00s.dirPath());
            Files.createFile(computedBlockPath00s.zipFilePath());
            // assert that server is running before we call actual
            assertThat(testContext.serverHealth().isRunning()).isTrue();
            // call
            final long actual = toTest.maxStoredBlockNumber();
            // assert still running server
            assertThat(actual).isEqualTo(-1L);
            assertThat(testContext.serverHealth().isRunning()).isTrue();
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
         * {@link ZipBlockArchive#writeNewZipFile(List)} will successfully
         * create the target zip file
         */
        @Test
        @DisplayName("Test writeNewZipFile() successfully creates the zip")
        void testZipSuccessfullyCreated() throws IOException {
            // setup
            final long firstBlockNumber = 0L;
            final int batchSize = (int) Math.pow(10, testConfig.powersOfTenPerZipFileContents());
            final List<BlockAccessor> batch = new ArrayList<>();
            for (int i = (int) firstBlockNumber; i < batchSize; i++) {
                final List<BlockItem> blockItems = List.of(SimpleTestBlockItemBuilder.createSimpleBlockWithNumber(i));
                batch.add(new InMemoryBlockAccessor(blockItems));
            }
            // before test assert that all the blocks are batched
            assertThat(batch).hasSize(batchSize);
            // assert no zip file is created yet, the zip file will be all the same
            // for all the 10 zips, so we can rely on asserting based on computed path for the first block
            // expected
            final Path expected =
                    BlockPath.computeBlockPath(testConfig, firstBlockNumber).zipFilePath();
            assertThat(expected).doesNotExist();
            // call
            toTest.writeNewZipFile(batch);
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
         * {@link ZipBlockArchive#writeNewZipFile(List)} will produce the right
         * contents for the created zip file
         */
        @Test
        @DisplayName("Test writeNewZipFile() zip file has the right contents")
        void testZipContents() throws IOException, ParseException {
            // setup
            final long firstBlockNumber = 0L;
            final int batchSize = (int) Math.pow(10, testConfig.powersOfTenPerZipFileContents());
            final Map<String, Bytes> first10BlocksWithExpectedEntryNames = new TreeMap<>();
            final List<BlockAccessor> batch = new ArrayList<>();
            for (int i = (int) firstBlockNumber; i < batchSize; i++) {
                final List<BlockItem> blockItems = List.of(SimpleTestBlockItemBuilder.createSimpleBlockWithNumber(i));
                final InMemoryBlockAccessor accessor = new InMemoryBlockAccessor(blockItems);
                batch.add(accessor);
                final String key = BlockFile.blockFileName(i, testConfig.compression());
                final Bytes value = accessor.blockBytes(Format.PROTOBUF);
                first10BlocksWithExpectedEntryNames.put(key, value);
            }
            // before test assert that all the blocks are batched
            assertThat(batch).hasSize(batchSize);
            // assert no zip file is created yet, the zip file will be all the same
            // for all the 10 zips, so we can rely on asserting based on computed path for the first block
            // expected
            final Path expected =
                    BlockPath.computeBlockPath(testConfig, firstBlockNumber).zipFilePath();
            assertThat(expected).doesNotExist();
            // call
            toTest.writeNewZipFile(batch);
            // assert that each entry's contents matches what we initially intended
            // to write
            try (final FileSystem zipFs = FileSystems.newFileSystem(expected);
                    final Stream<Path> entries = Files.list(zipFs.getPath("/"))) {
                for (final Path entry : entries.toList()) {
                    final Bytes expectedValue = first10BlocksWithExpectedEntryNames.get(
                            entry.getFileName().toString());
                    try (final InputStream in = Files.newInputStream(entry)) {
                        final Block readBlock = Block.PROTOBUF.parse(Bytes.wrap(in.readAllBytes()));
                        final Bytes actualValue = Block.PROTOBUF.toBytes(readBlock);
                        assertThat(actualValue).isEqualTo(expectedValue);
                        assertThat(actualValue.toHex()).isEqualTo(expectedValue.toHex());
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
        assertThat(blockPath.zipFilePath())
                .exists()
                .isReadable()
                .isWritable()
                .isNotEmptyFile()
                .hasExtension("zip");
        try (final FileSystem zipFs = FileSystems.newFileSystem(blockPath.zipFilePath())) {
            final Path entry = zipFs.getPath(blockPath.blockFileName());
            assertThat(entry).isNotNull().exists().isRegularFile().isReadable();
            final byte[] fromZipEntry = Files.readAllBytes(entry);
            assertThat(fromZipEntry).isEqualTo(bytesToWrite);
        }
        return new ZipBlockAccessor(blockPath);
    }

    private FilesHistoricConfig createTestConfiguration(final Path basePath, final int powersOfTenPerZipFileContents) {
        // for simplicity let's use no compression
        return new FilesHistoricConfig(basePath, CompressionType.NONE, powersOfTenPerZipFileContents, 0L);
    }
}
