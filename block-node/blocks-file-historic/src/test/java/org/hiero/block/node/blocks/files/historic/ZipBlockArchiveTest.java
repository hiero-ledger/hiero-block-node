// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.blocks.files.historic;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import com.google.common.jimfs.Jimfs;
import com.hedera.hapi.block.stream.Block;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.hiero.block.node.app.fixtures.blocks.TestBlock;
import org.hiero.block.node.app.fixtures.blocks.TestBlockBuilder;
import org.hiero.block.node.app.fixtures.plugintest.TestHealthFacility;
import org.hiero.block.node.base.BlockFile;
import org.hiero.block.node.base.CompressionType;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.historicalblocks.BlockAccessor;
import org.hiero.block.node.spi.historicalblocks.BlockAccessor.Format;
import org.hiero.block.node.spi.historicalblocks.BlockAccessorBatch;
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
    /** The path to the links root directory used for testing. */
    private Path linksTempDir;
    /** The {@link ZipBlockArchive} instance to be tested. */
    private ZipBlockArchive toTest;

    /**
     * Environment setup before each test.
     */
    @BeforeEach
    void setUp() throws IOException {
        jimFs = Jimfs.newFileSystem(com.google.common.jimfs.Configuration.unix());
        final Path blocksRoot = jimFs.getPath("/blocks");
        Files.createDirectories(blocksRoot);
        linksTempDir = blocksRoot.resolve("links");
        Files.createDirectories(linksTempDir);
        /** The path to the working zip root directory, where zip archives are created, used for testing. */
        final Path zipWorkDir = blocksRoot.resolve("zipwork");
        Files.createDirectories(zipWorkDir);
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
        @DisplayName("Test minStoredBlockNumber() Does Not shut down server if exception occurs")
        void testMinStoredWithException() throws IOException {
            // create test environment, in this case we simply create an empty zip file which will produce
            // an exception when we attempt to look for an entry inside
            final BlockPath computedBlockPath00 = BlockPath.computeBlockPath(testConfig, 0L);
            Files.createDirectories(computedBlockPath00.dirPath());
            Files.createFile(computedBlockPath00.zipFilePath());
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
        @DisplayName("Test maxStoredBlockNumber() does not shut down server if exception occurs")
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
                    .extracting(BlockAccessor::blockUnparsed)
                    .isEqualTo(expected.blockUnparsed());
        }

        /**
         * This test aims to assert that the
         * {@link ZipBlockArchive#minStoredArchive()} returns empty Optional when no zip files are present.
         */
        @Test
        @DisplayName("Test minStoredArchive() returns empty when no zip files present")
        void testMinStoredArchiveNoZipFiles() throws IOException {
            // call
            final Optional<Path> actual = toTest.minStoredArchive();
            // assert empty result and server still running
            assertThat(actual).isEmpty();
            assertThat(testContext.serverHealth().isRunning()).isTrue();
        }

        /**
         * This test aims to assert that the
         * {@link ZipBlockArchive#minStoredArchive()} returns the only zip file when a single zip file exists.
         */
        @Test
        @DisplayName("Test minStoredArchive() returns single zip file")
        void testMinStoredArchiveSingleZipFile() throws IOException {
            // create test environment with one zip file
            createAndAddBlockEntry(5L);
            final Path expectedZip = BlockPath.computeBlockPath(testConfig, 5L).zipFilePath();
            // call
            final Optional<Path> actual = toTest.minStoredArchive();
            // assert
            assertThat(actual).isPresent().hasValue(expectedZip);
            assertThat(testContext.serverHealth().isRunning()).isTrue();
        }

        /**
         * This test aims to assert that the
         * {@link ZipBlockArchive#minStoredArchive()} returns the minimum zip file
         * when multiple zip files exist in the same directory.
         */
        @Test
        @DisplayName("Test minStoredArchive() returns minimum from same directory")
        void testMinStoredArchiveMultipleZipsSameDirectory() throws IOException {
            // create test environment with multiple zip files in same directory
            // Using powersOfTenPerZipFileContents=1, blocks 3 and 7 will be in the same directory
            createAndAddBlockEntry(3L);
            createAndAddBlockEntry(5L);
            createAndAddBlockEntry(7L);
            final Path expectedZip = BlockPath.computeBlockPath(testConfig, 3L).zipFilePath();
            // call
            final Optional<Path> actual = toTest.minStoredArchive();
            // assert returns the minimum (3)
            assertThat(actual).isPresent().hasValue(expectedZip);
        }

        /**
         * This test aims to assert that the
         * {@link ZipBlockArchive#minStoredArchive()} returns the zip file from the
         * minimum directory when zip files exist in different directories.
         */
        @Test
        @DisplayName("Test minStoredArchive() returns minimum across different directories")
        void testMinStoredArchiveMultipleDirectories() throws IOException {
            // create test environment with zip files in different nested directories
            // Using powersOfTenPerZipFileContents=1, these will be in different directories
            createAndAddBlockEntry(13L); // In a lower directory
            createAndAddBlockEntry(103L); // In a higher directory
            createAndAddBlockEntry(1003L); // In an even higher directory
            final Path expectedZip = BlockPath.computeBlockPath(testConfig, 13L).zipFilePath();
            // call
            final var actual = toTest.minStoredArchive();
            // assert returns the one from the minimum directory path (13)
            assertThat(actual).isPresent().hasValue(expectedZip);
        }

        /**
         * This test aims to assert that the
         * {@link ZipBlockArchive#minStoredArchive()} skips empty directories and
         * returns the minimum zip file from the next non-empty directory.
         */
        @Test
        @DisplayName("Test minStoredArchive() skips empty directories")
        void testMinStoredArchiveSkipsEmptyDirectories() throws IOException {
            // create test environment with empty directories
            // Create block files and then delete them to simulate empty directories
            createAndAddBlockEntry(3L);
            final Path zip3 = BlockPath.computeBlockPath(testConfig, 3L).zipFilePath();
            Files.delete(zip3);
            createAndAddBlockEntry(13L);
            final Path zip13 = BlockPath.computeBlockPath(testConfig, 13L).zipFilePath();
            Files.delete(zip13);
            // Create a zip file in a higher directory than the empty ones
            createAndAddBlockEntry(33L);
            final Path expectedZip = BlockPath.computeBlockPath(testConfig, 33L).zipFilePath();
            // call
            final var actual = toTest.minStoredArchive();
            // assert returns the zip from the first non-empty directory
            assertThat(actual).isPresent().hasValue(expectedZip);
        }

        /**
         * This test aims to assert that the
         * {@link ZipBlockArchive#minStoredArchive()} handles deeply nested directory structures
         * and returns the minimum zip file.
         */
        @Test
        @DisplayName("Test minStoredArchive() handles deeply nested structure")
        void testMinStoredArchiveDeeplyNested() throws IOException {
            // create test environment with deeply nested directories
            // Using powersOfTenPerZipFileContents=1 creates nested structure
            createAndAddBlockEntry(1234L);
            createAndAddBlockEntry(5678L);
            createAndAddBlockEntry(123L);
            final Path expectedZip =
                    BlockPath.computeBlockPath(testConfig, 123L).zipFilePath();
            // call
            final var actual = toTest.minStoredArchive();
            // assert returns the minimum (123)
            assertThat(actual).isPresent().hasValue(expectedZip);
        }

        /**
         * This test aims to assert that the
         * {@link ZipBlockArchive#count()} returns 0L when no zip files are present.
         */
        @Test
        @DisplayName("Test count() returns 0 when no zip files are present")
        void testCountNoZipFiles() {
            final long actual = toTest.count();
            // assert that server is still running and 0 is returned
            assertThat(actual).isZero();
            assertThat(testContext.serverHealth().isRunning()).isTrue();
        }

        /**
         * This test aims to assert that the
         * {@link ZipBlockArchive#count()} correctly returns 1 when a single zip file exists.
         */
        @Test
        @DisplayName("Test count() returns 1 for single zip file")
        void testCountSingleZipFile() throws IOException {
            // create test environment, for this test we need one zip file
            createAndAddBlockEntry(3L);
            // call
            final long actual = toTest.count();
            // assert expected result
            assertThat(actual).isEqualTo(1L);
        }

        /**
         * This test aims to assert that the
         * {@link ZipBlockArchive#count()} correctly returns the count of multiple zip files.
         */
        @Test
        @DisplayName("Test count() returns correct count for multiple zip files")
        void testCountMultipleZipFiles() throws IOException {
            // create test environment, for this test we need two zip files
            createAndAddBlockEntry(3L);
            createAndAddBlockEntry(4L);
            // zip size are 10 blocks, so the following 2 will be in another file in the same directory
            createAndAddBlockEntry(13L);
            createAndAddBlockEntry(14L);
            // call
            final long actual = toTest.count();
            // assert expected result and still running server - should be 2 zip files
            assertThat(actual).isEqualTo(2L);
            assertThat(testContext.serverHealth().isRunning()).isTrue();
        }

        /**
         * This test aims to assert that the
         * {@link ZipBlockArchive#count()} only counts zip files in numeric directories,
         * excluding files in special directories like "stage" or "links".
         */
        @Test
        @DisplayName("Test count() only counts zip files in numeric directories")
        void testCountOnlyNumericDirectories() throws IOException {
            // create test environment with zip files in numeric directories
            createAndAddBlockEntry(3L);
            createAndAddBlockEntry(13L);
            // create a zip file in a non-numeric directory (should not be counted)
            final Path stage = testConfig.rootPath().resolve("stage");
            Files.createDirectories(stage);
            final Path stageZip = stage.resolve("0.zip");
            Files.createFile(stageZip);
            // create a zip file in the links directory (should not be counted)
            final Path linksZip = linksTempDir.resolve("0.zip");
            Files.createFile(linksZip);
            // assert that server is running before we call actual
            assertThat(testContext.serverHealth().isRunning()).isTrue();
            // call
            final long actual = toTest.count();
            // assert expected result - should only count 2 zip files in numeric directories
            assertThat(actual).isEqualTo(2L);
        }

        /**
         * This test aims to assert that the
         * {@link ZipBlockArchive#count()} correctly counts zip files across different nested directories.
         */
        @Test
        @DisplayName("Test count() counts zip files in different nested directories")
        void testCountDifferentDirectories() throws IOException {
            // create test environment with zip files in different nested directories
            // Using testConfig with powersOfTenPerZipFileContents = 1, so each power of 10 gets its own directory
            createAndAddBlockEntry(3L);
            createAndAddBlockEntry(13L);
            createAndAddBlockEntry(103L);
            createAndAddBlockEntry(1003L);
            // call
            final long actual = toTest.count();
            // assert expected result - should count 4 zip files across different directories
            assertThat(actual).isEqualTo(4L);
        }

        /**
         * This test aims to assert that the
         * {@link ZipBlockArchive#createZip(BlockAccessorBatch, Path)}  will successfully
         * create the target zip file
         */
        @Test
        @DisplayName("Test writeNewZipFile() successfully creates the zip")
        void testZipSuccessfullyCreated() throws IOException {
            // setup
            final int firstBlockNumber = 0;
            final int batchSize = StrictMath.toIntExact(intPowerOfTen(testConfig.powersOfTenPerZipFileContents()));
            final BlockAccessorBatch batch = new BlockAccessorBatch();
            for (int i = firstBlockNumber; i < batchSize; i++) {
                final TestBlock block = TestBlockBuilder.generateBlockWithNumber(i);
                batch.add(block.asBlockAccessor());
            }
            // before test assert that all the blocks are batched
            assertThat(batch).hasSize(batchSize);
            // assert no zip file is created yet, the zip file will be all the same
            // for all the 10 zips, so we can rely on asserting based on computed path for the first block
            // expected
            final Path expected =
                    BlockPath.computeBlockPath(testConfig, firstBlockNumber).zipFilePath();
            assertThat(expected).doesNotExist();

            Files.createDirectories(expected.getParent());

            // call
            toTest.createZip(batch, expected);
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
         * {@link ZipBlockArchive#createZip(BlockAccessorBatch, Path)} will produce the right
         * contents for the created zip file
         */
        @Test
        @DisplayName("Test writeNewZipFile() zip file has the right contents")
        void testZipContents() throws IOException, ParseException {
            // setup
            final int firstBlockNumber = 0;
            final int batchSize = StrictMath.toIntExact(intPowerOfTen(testConfig.powersOfTenPerZipFileContents()));
            final Map<String, Bytes> first10BlocksWithExpectedEntryNames = new TreeMap<>();
            final BlockAccessorBatch batch = new BlockAccessorBatch();
            for (int i = firstBlockNumber; i < batchSize; i++) {
                final TestBlock block = TestBlockBuilder.generateBlockWithNumber(i);
                final BlockAccessor accessor = block.asBlockAccessor();
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
            Files.createDirectories(expected.getParent());
            // call
            toTest.createZip(batch, expected);
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
        final TestBlock block = TestBlockBuilder.generateBlockWithNumber(blockNumber);
        return createAndAddBlockEntry(blockPath, block.bytes().toByteArray());
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
        return new ZipBlockAccessor(blockPath, linksTempDir);
    }

    private FilesHistoricConfig createTestConfiguration(
            final Path blocksRoot, final int powersOfTenPerZipFileContents) {
        // for simplicity let's use no compression
        return new FilesHistoricConfig(blocksRoot, CompressionType.NONE, powersOfTenPerZipFileContents, 0L, 3);
    }

    /**
     * Simple power of ten function that avoids the inaccuracies possible
     * with floating point and also ensures the value must fit within
     * a long.
     *
     * @param powerToCreate the exponent to raise 10 to.  This must be
     *     between 1 and 18, inclusive.
     * @return 10 raised to (powerToCreate), or -1 if the result would not
     *     fit within a long primitive.
     */
    private long intPowerOfTen(final int powerToCreate) {
        if (powerToCreate > 18 || powerToCreate < 1) {
            return -1;
        } else {
            long currentTotal = 1;
            for (int i = 0; i < powerToCreate; i++) {
                currentTotal *= 10;
            }
            return currentTotal;
        }
    }
}
