// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.blocks.files.historic;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hiero.block.node.base.ParseHelper.standardParse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import com.hedera.hapi.block.stream.Block;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.config.api.ConfigurationBuilder;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.app.fixtures.blocks.TestBlock;
import org.hiero.block.node.app.fixtures.blocks.TestBlockBuilder;
import org.hiero.block.node.app.fixtures.plugintest.TestHealthFacility;
import org.hiero.block.node.base.CompressionType;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.historicalblocks.BlockAccessor;
import org.hiero.block.node.spi.historicalblocks.BlockAccessor.Format;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Test class for {@link CachedZipBlockAccessor}, exercised via {@link ZipBlockArchive#blockAccessor(long)} with
 * {@link FilesHistoricConfig#cachedZipAccessorEnabled()} turned on. Deliberately mirrors {@link ZipBlockAccessorTest}
 * so the two implementations can be compared directly.
 */
@DisplayName("CachedZipBlockAccessor Tests")
class CachedZipBlockAccessorTest {
    /** The testing in-memory file system. */
    private FileSystem jimfs;
    /** The configuration for the test. */
    private FilesHistoricConfig defaultConfig;
    /** The temporary data directory used for the test. */
    private Path dataTempDir;
    /** The block node context used to construct {@link ZipBlockArchive} instances for the test. */
    private BlockNodeContext testContext;

    /** Set up the test environment before each test. */
    @BeforeEach
    void setup() throws IOException {
        // Initialize the in-memory file system
        jimfs = Jimfs.newFileSystem(
                Configuration.unix()); // Set the default configuration for the test, use jimfs for paths
        dataTempDir = jimfs.getPath("/blocks");
        Files.createDirectories(dataTempDir);
        defaultConfig =
                createTestConfiguration(dataTempDir, getDefaultConfiguration().compression());
        testContext = new BlockNodeContext(
                null,
                null,
                new TestHealthFacility(),
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                new ArrayList<>(),
                new ArrayList<>());
    }

    /**
     * Tear down the test environment after each test.
     */
    @AfterEach
    void tearDown() throws IOException {
        // Close the Jimfs file system
        if (jimfs != null) {
            jimfs.close();
            jimfs = null;
        }
    }

    /**
     * Tests for the {@link CachedZipBlockAccessor} functionality.
     */
    @Nested
    @DisplayName("Functionality Tests")
    final class FunctionalityTests {
        /**
         * This test aims to verify that the {@link CachedZipBlockAccessor#blockBytes(Format)}
         * will correctly return a zipped block as bytes. This is the happy path test
         * where the compression type is the same as the compression type used to create
         * the block (zip entry inside the zip file we are trying to read).
         */
        @ParameterizedTest
        @EnumSource(CompressionType.class)
        @DisplayName("Test blockBytes() returns correctly a persisted block as bytes happy path format")
        @SuppressWarnings("DataFlowIssue")
        void testBlockBytesHappyPathFormat(final CompressionType compressionType) throws IOException {
            // build a test block
            final TestBlock block = TestBlockBuilder.generateBlockWithNumber(0);
            final FilesHistoricConfig testConfig = createTestConfiguration(dataTempDir, compressionType);
            final BlockPath blockPath = BlockPath.computeBlockPath(testConfig, block.number());
            final ZipBlockArchive archive = new ZipBlockArchive(testContext, testConfig);
            final Bytes expected = block.bytes();
            // test cachedZipBlockAccessor.blockBytes()
            final BlockAccessor toTest = createBlockAndGetAssociatedAccessor(testConfig, archive, blockPath, expected);
            assertThat(toTest).isExactlyInstanceOf(CachedZipBlockAccessor.class);
            final Format format = getHappyPathFormat(compressionType);
            // The blockBytes method should return the bytes of the block with the
            // specified format. In order to assert the same bytes, we need to decompress
            // the bytes returned by the blockBytes method and compare them to the expected.
            final Bytes testResult = toTest.blockBytes(format);
            final Bytes actual = Bytes.wrap(compressionType.decompress(testResult.toByteArray()));
            assertThat(actual).isEqualTo(expected);
        }

        /**
         * This test aims to verify that the {@link CachedZipBlockAccessor#blockBytes(Format)}
         * will correctly return a zipped block as bytes. This is the happy path test
         * where the compression type is the same as the compression type used to create
         * the block (zip entry inside the zip file we are trying to read).
         */
        @ParameterizedTest
        @EnumSource(CompressionType.class)
        @DisplayName(
                "Test blockBytes() returns correctly a persisted block as bytes happy path format - consecutive calls")
        @SuppressWarnings("DataFlowIssue")
        void testBlockBytesHappyPathFormatConsecutiveCalls(final CompressionType compressionType) throws IOException {
            // build a test block
            final TestBlock block = TestBlockBuilder.generateBlockWithNumber(0);
            final FilesHistoricConfig testConfig = createTestConfiguration(dataTempDir, compressionType);
            final BlockPath blockPath = BlockPath.computeBlockPath(testConfig, block.number());
            final ZipBlockArchive archive = new ZipBlockArchive(testContext, testConfig);
            final Bytes expected = block.bytes();
            // test cachedZipBlockAccessor.blockBytes()
            final BlockAccessor toTest = createBlockAndGetAssociatedAccessor(testConfig, archive, blockPath, expected);
            final Format format = getHappyPathFormat(compressionType);
            // The blockBytes method should return the bytes of the block with the
            // specified format. In order to assert the same bytes, we need to decompress
            // the bytes returned by the blockBytes method and compare them to the expected.
            final Bytes testResult = toTest.blockBytes(format);
            final Bytes actual = Bytes.wrap(compressionType.decompress(testResult.toByteArray()));
            assertThat(actual).isEqualTo(expected);
            // now we close the accessor
            toTest.close();
            assertThat(blockPath.zipFilePath())
                    .exists()
                    .isReadable()
                    .isWritable()
                    .isNotEmptyFile()
                    .hasExtension("zip");
            // now we create a new accessor to the same block
            final BlockAccessor toTest2 = archive.blockAccessor(blockPath.blockNumber());
            // now we should be able to access the block again
            final Bytes testResult2 = toTest2.blockBytes(format);
            final Bytes actual2 = Bytes.wrap(compressionType.decompress(testResult2.toByteArray()));
            assertThat(actual2).isEqualTo(expected);
        }

        /**
         * This test aims to verify that the {@link CachedZipBlockAccessor#blockBytes(Format)}
         * will correctly return a zipped block as bytes. This test will always use the
         * {@link Format#ZSTD_PROTOBUF} format to read the block bytes.
         */
        @ParameterizedTest
        @EnumSource(CompressionType.class)
        @DisplayName("Test blockBytes() returns correctly a persisted block as bytes using ZSTD_PROTOBUF format")
        @SuppressWarnings("DataFlowIssue")
        void testBlockBytesZSTDPROTOBUFFormat(final CompressionType compressionType) throws IOException {
            // build a test block
            final TestBlock block = TestBlockBuilder.generateBlockWithNumber(0);
            final FilesHistoricConfig testConfig = createTestConfiguration(dataTempDir, compressionType);
            final BlockPath blockPath = BlockPath.computeBlockPath(testConfig, block.number());
            final ZipBlockArchive archive = new ZipBlockArchive(testContext, testConfig);
            final Bytes expected = block.bytes();
            // test cachedZipBlockAccessor.blockBytes()
            final BlockAccessor toTest = createBlockAndGetAssociatedAccessor(testConfig, archive, blockPath, expected);
            // The blockBytes method should return the bytes of the block with the
            // specified format. In order to assert the same bytes, we need to decompress
            // the bytes returned by the blockBytes method and compare them to the expected.
            // For this test, we always use the ZSTD_PROTOBUF format to read the block bytes,
            // no matter the actual compression type used to persist the block. With this format
            // we always expect to be returned the bytes compressed using the ZStandard compression
            // algorithm.
            final Bytes testResult = toTest.blockBytes(Format.ZSTD_PROTOBUF);
            final Bytes actual = Bytes.wrap(CompressionType.ZSTD.decompress(testResult.toByteArray()));
            assertThat(actual).isEqualTo(expected);
        }

        /**
         * This test aims to verify that the {@link CachedZipBlockAccessor#blockBytes(Format)}
         * will correctly return a zipped block as bytes. This test will always use the
         * {@link Format#ZSTD_PROTOBUF} format to read the block bytes. Here we verify that two
         * consecutive accessors to the same block will return the same block.
         * Closing an accessor does not in any way interfere with the data and
         * the ability to access it.
         */
        @ParameterizedTest
        @EnumSource(CompressionType.class)
        @DisplayName(
                "Test blockBytes() returns correctly a persisted block as bytes using ZSTD_PROTOBUF format - consecutive calls")
        @SuppressWarnings("DataFlowIssue")
        void testBlockBytesZSTDPROTOBUFFormatConsecutiveCalls(final CompressionType compressionType)
                throws IOException {
            // build a test block
            final TestBlock block = TestBlockBuilder.generateBlockWithNumber(0);
            final FilesHistoricConfig testConfig = createTestConfiguration(dataTempDir, compressionType);
            final BlockPath blockPath = BlockPath.computeBlockPath(testConfig, block.number());
            final ZipBlockArchive archive = new ZipBlockArchive(testContext, testConfig);
            final Bytes expected = block.bytes();
            // test cachedZipBlockAccessor.blockBytes()
            final BlockAccessor toTest = createBlockAndGetAssociatedAccessor(testConfig, archive, blockPath, expected);
            // The blockBytes method should return the bytes of the block with the
            // specified format. In order to assert the same bytes, we need to decompress
            // the bytes returned by the blockBytes method and compare them to the expected.
            // For this test, we always use the ZSTD_PROTOBUF format to read the block bytes,
            // no matter the actual compression type used to persist the block. With this format
            // we always expect to be returned the bytes compressed using the ZStandard compression
            // algorithm.
            assertNotNull(jimfs);
            assertThat(jimfs.isOpen()).isTrue();
            assertThat(toTest.isClosed()).isFalse();
            final Bytes testResult = toTest.blockBytes(Format.ZSTD_PROTOBUF);
            final Bytes actual = Bytes.wrap(CompressionType.ZSTD.decompress(testResult.toByteArray()));
            assertThat(actual).isEqualTo(expected);
            // now we close the accessor
            toTest.close();
            assertThat(blockPath.zipFilePath())
                    .exists()
                    .isReadable()
                    .isWritable()
                    .isNotEmptyFile()
                    .hasExtension("zip");
            // now we create a new accessor to the same block
            final BlockAccessor toTest2 = archive.blockAccessor(blockPath.blockNumber());
            // now we should be able to access the block again
            final Bytes testResult2 = toTest2.blockBytes(Format.ZSTD_PROTOBUF);
            final Bytes actual2 = Bytes.wrap(CompressionType.ZSTD.decompress(testResult2.toByteArray()));
            assertThat(actual2).isEqualTo(expected);
        }

        /**
         * This test aims to verify that the {@link CachedZipBlockAccessor#blockBytes(Format)}
         * will correctly return a zipped block as bytes. This test will always use the
         * {@link Format#PROTOBUF} format to read the block bytes.
         */
        @ParameterizedTest
        @EnumSource(CompressionType.class)
        @DisplayName("Test blockBytes() returns correctly a persisted block as bytes using PROTOBUF format")
        @SuppressWarnings("DataFlowIssue")
        void testBlockBytesProtobufFormat(final CompressionType compressionType) throws IOException {
            // build a test block
            final TestBlock block = TestBlockBuilder.generateBlockWithNumber(0);
            final FilesHistoricConfig testConfig = createTestConfiguration(dataTempDir, compressionType);
            final BlockPath blockPath = BlockPath.computeBlockPath(testConfig, block.number());
            final ZipBlockArchive archive = new ZipBlockArchive(testContext, testConfig);
            final Bytes expected = block.bytes();
            // test cachedZipBlockAccessor.blockBytes()
            final BlockAccessor toTest = createBlockAndGetAssociatedAccessor(testConfig, archive, blockPath, expected);
            // The blockBytes method should return the bytes of the block with the
            // specified format.
            // For this test, we always use the PROTOBUF format to read the block bytes,
            // no matter the actual compression type used to persist the block. With this format
            // we always expect to be returned the bytes to not be compressed.
            final Bytes testResult = toTest.blockBytes(Format.PROTOBUF);
            assertThat(testResult).isEqualTo(expected);
        }

        /**
         * This test aims to verify that the {@link CachedZipBlockAccessor#blockBytes(Format)}
         * will correctly return a zipped block as bytes. This test will always use the
         * {@link Format#PROTOBUF} format to read the block bytes. Here we verify that two
         * consecutive accessors to the same block will return the same block.
         * Closing an accessor does not in any way interfere with the data and
         * the ability to access it.
         */
        @ParameterizedTest
        @EnumSource(CompressionType.class)
        @DisplayName(
                "Test blockBytes() returns correctly a persisted block as bytes using PROTOBUF format - consecutive calls")
        @SuppressWarnings("DataFlowIssue")
        void testBlockBytesProtobufFormatConsecutiveCalls(final CompressionType compressionType) throws IOException {
            // build a test block
            final TestBlock block = TestBlockBuilder.generateBlockWithNumber(0);
            final FilesHistoricConfig testConfig = createTestConfiguration(dataTempDir, compressionType);
            final BlockPath blockPath = BlockPath.computeBlockPath(testConfig, block.number());
            final ZipBlockArchive archive = new ZipBlockArchive(testContext, testConfig);
            final Bytes expected = block.bytes();
            // test cachedZipBlockAccessor.blockBytes()
            final BlockAccessor toTest = createBlockAndGetAssociatedAccessor(testConfig, archive, blockPath, expected);
            // The blockBytes method should return the bytes of the block with the
            // specified format.
            // For this test, we always use the PROTOBUF format to read the block bytes,
            // no matter the actual compression type used to persist the block. With this format
            // we always expect to be returned the bytes to not be compressed.
            final Bytes testResult = toTest.blockBytes(Format.PROTOBUF);
            assertThat(testResult).isEqualTo(expected);
            // now we close the accessor
            toTest.close();
            assertThat(blockPath.zipFilePath())
                    .exists()
                    .isReadable()
                    .isWritable()
                    .isNotEmptyFile()
                    .hasExtension("zip");
            // now we create a new accessor to the same block
            final BlockAccessor toTest2 = archive.blockAccessor(blockPath.blockNumber());
            // now we should be able to access the block again
            assertThat(toTest2.blockBytes(Format.PROTOBUF)).isEqualTo(expected);
        }

        /**
         * This test aims to verify that a persisted block can be read with
         * {@link CachedZipBlockAccessor#blockUnparsed()} and then fully parsed to a {@link Block}.
         * This ensures the round-trip of storing and retrieving zipped blocks works correctly.
         */
        @ParameterizedTest
        @EnumSource(CompressionType.class)
        @DisplayName("Test block can be read and parsed from zipped persisted data")
        @SuppressWarnings("DataFlowIssue")
        void testBlockParsedFromUnparsed(final CompressionType compressionType) throws IOException, ParseException {
            // build a test block
            final TestBlock block = TestBlockBuilder.generateBlockWithNumber(0);
            final FilesHistoricConfig testConfig = createTestConfiguration(dataTempDir, compressionType);
            final BlockPath blockPath = BlockPath.computeBlockPath(testConfig, block.number());
            final ZipBlockArchive archive = new ZipBlockArchive(testContext, testConfig);
            final Block expected = block.block();
            final Bytes protoBytes = block.bytes();
            // test cachedZipBlockAccessor.blockUnparsed() and parse
            final BlockAccessor toTest =
                    createBlockAndGetAssociatedAccessor(testConfig, archive, blockPath, protoBytes);
            final BlockUnparsed unparsed = toTest.blockUnparsed();
            assertThat(unparsed).isNotNull();
            final Block actual = standardParse(Block.PROTOBUF, BlockUnparsed.PROTOBUF.toBytes(unparsed));
            assertThat(actual).isEqualTo(expected);
        }

        /**
         * This test aims to verify that a persisted zipped block can be read and parsed correctly
         * across subsequent accessor instances. Closing an accessor does not in any way interfere
         * with the data and the ability to access it.
         */
        @ParameterizedTest
        @EnumSource(CompressionType.class)
        @DisplayName("Test block can be read and parsed - consecutive calls")
        @SuppressWarnings("DataFlowIssue")
        void testBlockParsedFromUnparsedConsecutiveCalls(final CompressionType compressionType)
                throws IOException, ParseException {
            // build a test block
            final TestBlock block = TestBlockBuilder.generateBlockWithNumber(0);
            final FilesHistoricConfig testConfig = createTestConfiguration(dataTempDir, compressionType);
            final BlockPath blockPath = BlockPath.computeBlockPath(testConfig, block.number());
            final ZipBlockArchive archive = new ZipBlockArchive(testContext, testConfig);
            final Block expected = block.block();
            final Bytes protoBytes = block.bytes();
            // test cachedZipBlockAccessor.blockUnparsed() and parse
            final BlockAccessor toTest =
                    createBlockAndGetAssociatedAccessor(testConfig, archive, blockPath, protoBytes);
            final BlockUnparsed unparsed = toTest.blockUnparsed();
            assertThat(unparsed).isNotNull();
            final Block actual = standardParse(Block.PROTOBUF, BlockUnparsed.PROTOBUF.toBytes(unparsed));
            assertThat(actual).isEqualTo(expected);
            // now we close the accessor
            toTest.close();
            assertThat(blockPath.zipFilePath())
                    .exists()
                    .isReadable()
                    .isWritable()
                    .isNotEmptyFile()
                    .hasExtension("zip");
            // now we create a new accessor to the same block
            final BlockAccessor toTest2 = archive.blockAccessor(blockPath.blockNumber());
            // now we should be able to access the block again
            final BlockUnparsed unparsed2 = toTest2.blockUnparsed();
            assertThat(unparsed2).isNotNull();
            final Block actual2 = standardParse(Block.PROTOBUF, BlockUnparsed.PROTOBUF.toBytes(unparsed2));
            assertThat(actual2).isEqualTo(expected);
        }

        /**
         * This test aims to verify that the {@link CachedZipBlockAccessor#blockUnparsed()}
         * will correctly return a zipped block unparsed.
         */
        @ParameterizedTest
        @EnumSource(CompressionType.class)
        @DisplayName("Test blockUnparsed() returns correctly a persisted block unparsed")
        void testBlockUnparsed(final CompressionType compressionType) throws IOException, ParseException {
            // build a test block
            final TestBlock block = TestBlockBuilder.generateBlockWithNumber(0);
            final FilesHistoricConfig testConfig = createTestConfiguration(dataTempDir, compressionType);
            final BlockPath blockPath = BlockPath.computeBlockPath(testConfig, block.number());
            final ZipBlockArchive archive = new ZipBlockArchive(testContext, testConfig);
            final BlockUnparsed expected = block.blockUnparsed();
            final Bytes protoBytes = BlockUnparsed.PROTOBUF.toBytes(expected);
            final BlockAccessor toTest =
                    createBlockAndGetAssociatedAccessor(testConfig, archive, blockPath, protoBytes);
            final BlockUnparsed actual = toTest.blockUnparsed();
            assertThat(actual).isEqualTo(expected);
        }

        /**
         * This test aims to verify that the {@link CachedZipBlockAccessor#blockUnparsed()}
         * will correctly return a zipped block unparsed. Here we verify that two
         * consecutive accessors to the same block will return the same block.
         * Closing an accessor does not in any way interfere with the data and
         * the ability to access it.
         */
        @ParameterizedTest
        @EnumSource(CompressionType.class)
        @DisplayName("Test blockUnparsed() returns correctly a persisted block unparsed - consecutive calls")
        void testBlockUnparsedConsecutiveCalls(final CompressionType compressionType)
                throws IOException, ParseException {
            // build a test block
            final TestBlock block = TestBlockBuilder.generateBlockWithNumber(0);
            final FilesHistoricConfig testConfig = createTestConfiguration(dataTempDir, compressionType);
            final BlockPath blockPath = BlockPath.computeBlockPath(testConfig, block.number());
            final ZipBlockArchive archive = new ZipBlockArchive(testContext, testConfig);
            final BlockUnparsed expected = block.blockUnparsed();
            final Bytes protoBytes = BlockUnparsed.PROTOBUF.toBytes(expected);
            // test cachedZipBlockAccessor.blockUnparsed()
            final BlockAccessor toTest =
                    createBlockAndGetAssociatedAccessor(testConfig, archive, blockPath, protoBytes);
            assertThat(toTest.isClosed()).isFalse();
            assertNotNull(jimfs);
            assertThat(jimfs.isOpen()).isTrue();
            final BlockUnparsed actual = toTest.blockUnparsed();
            assertThat(actual).isEqualTo(expected);
            // now we close the accessor
            toTest.close();
            assertThat(blockPath.zipFilePath())
                    .exists()
                    .isReadable()
                    .isWritable()
                    .isNotEmptyFile()
                    .hasExtension("zip");
            // now we create a new accessor to the same block
            final BlockAccessor toTest2 = archive.blockAccessor(blockPath.blockNumber());
            // now we should be able to access the block again
            assertThat(toTest2.blockUnparsed()).isEqualTo(expected);
        }

        /**
         * This test aims to verify that {@link CachedZipBlockAccessor#blockNumber()} returns the block number it
         * was constructed for.
         */
        @Test
        @DisplayName("Test blockNumber() returns the accessor's block number")
        void testBlockNumber() throws IOException {
            final long targetBlockNumber = 7L;
            final TestBlock block = TestBlockBuilder.generateBlockWithNumber(targetBlockNumber);
            final FilesHistoricConfig testConfig = createTestConfiguration(dataTempDir, CompressionType.NONE);
            final BlockPath blockPath = BlockPath.computeBlockPath(testConfig, targetBlockNumber);
            final ZipBlockArchive archive = new ZipBlockArchive(testContext, testConfig);
            final BlockAccessor toTest =
                    createBlockAndGetAssociatedAccessor(testConfig, archive, blockPath, block.bytes());
            assertThat(toTest.blockNumber()).isEqualTo(targetBlockNumber);
        }

        /**
         * This test aims to verify that {@link CachedZipBlockAccessor#blockBytes(Format)} correctly returns the
         * persisted block converted to {@link Format#JSON}.
         */
        @Test
        @DisplayName("Test blockBytes() returns correctly a persisted block as bytes using JSON format")
        void testBlockBytesJsonFormat() throws IOException, ParseException {
            final TestBlock block = TestBlockBuilder.generateBlockWithNumber(0);
            final FilesHistoricConfig testConfig = createTestConfiguration(dataTempDir, CompressionType.NONE);
            final BlockPath blockPath = BlockPath.computeBlockPath(testConfig, block.number());
            final ZipBlockArchive archive = new ZipBlockArchive(testContext, testConfig);
            final BlockAccessor toTest =
                    createBlockAndGetAssociatedAccessor(testConfig, archive, blockPath, block.bytes());
            final Bytes jsonBytes = toTest.blockBytes(Format.JSON);
            assertThat(jsonBytes).isNotNull();
            final Block parsedBack = standardParse(Block.JSON, jsonBytes);
            assertThat(parsedBack).isEqualTo(block.block());
        }

        /**
         * This test aims to verify that {@link CachedZipBlockAccessor#blockBytes(Format)} returns {@code null}
         * (rather than throwing) when the underlying, shared archive filesystem has already been closed -- e.g.
         * by {@link ZipBlockArchive#close()} on plugin shutdown, while an accessor still holds a reference.
         */
        @Test
        @DisplayName("Test blockBytes() returns null once the shared archive filesystem is closed")
        void testBlockBytesReturnsNullAfterArchiveClosed() throws IOException {
            final TestBlock block = TestBlockBuilder.generateBlockWithNumber(0);
            final FilesHistoricConfig testConfig = createTestConfiguration(dataTempDir, CompressionType.NONE);
            final BlockPath blockPath = BlockPath.computeBlockPath(testConfig, block.number());
            final ZipBlockArchive archive = new ZipBlockArchive(testContext, testConfig);
            final BlockAccessor toTest =
                    createBlockAndGetAssociatedAccessor(testConfig, archive, blockPath, block.bytes());
            // simulate plugin shutdown while a reader is still active
            archive.close();
            assertThat(toTest.blockBytes(Format.PROTOBUF)).isNull();
        }

        /**
         * This test aims to verify that {@link CachedZipBlockAccessor#blockBytes(Format)} returns {@code null}
         * (rather than throwing) when the persisted entry is not valid protobuf and {@link Format#JSON} is
         * requested, since that format requires parsing the protobuf bytes before re-encoding as JSON.
         */
        @Test
        @DisplayName("Test blockBytes() returns null for JSON format when the entry is not valid protobuf")
        void testBlockBytesJsonFormatReturnsNullOnCorruptData() throws IOException {
            final FilesHistoricConfig testConfig = createTestConfiguration(dataTempDir, CompressionType.NONE);
            final BlockPath blockPath = BlockPath.computeBlockPath(testConfig, 0L);
            final ZipBlockArchive archive = new ZipBlockArchive(testContext, testConfig);
            final Bytes garbage = Bytes.wrap("this is not a valid protobuf block".getBytes());
            final BlockAccessor toTest = createBlockAndGetAssociatedAccessor(testConfig, archive, blockPath, garbage);
            assertThat(toTest.blockBytes(Format.JSON)).isNull();
        }

        private Format getHappyPathFormat(final CompressionType compressionType) {
            return switch (compressionType) {
                case ZSTD -> Format.ZSTD_PROTOBUF;
                case NONE -> Format.PROTOBUF;
            };
        }
    }

    private BlockAccessor createBlockAndGetAssociatedAccessor(
            final FilesHistoricConfig testConfig,
            final ZipBlockArchive archive,
            final BlockPath blockPath,
            Bytes protoBytes)
            throws IOException {
        // create & assert existing block file path before call
        Files.createDirectories(blockPath.dirPath());
        // it is important the output stream is closed as the compression writes a footer on close
        Files.createFile(blockPath.zipFilePath());
        final byte[] bytesToWrite;
        switch (testConfig.compression()) {
            case NONE -> bytesToWrite = protoBytes.toByteArray();
            case ZSTD -> {
                final byte[] compressedBytes = protoBytes.toByteArray();
                bytesToWrite = CompressionType.ZSTD.compress(compressedBytes);
            }
            default -> throw new IllegalStateException("Unhandled compression type: " + testConfig.compression());
        }
        try (final ZipOutputStream zipOut = new ZipOutputStream(Files.newOutputStream(blockPath.zipFilePath()))) {
            // create a new zip entry
            final ZipEntry zipEntry = new ZipEntry(blockPath.blockFileName());
            zipOut.putNextEntry(zipEntry);
            zipOut.write(bytesToWrite);
            zipOut.closeEntry();
        }
        assertThat(blockPath.zipFilePath())
                .exists()
                .isReadable()
                .isWritable()
                .isNotEmptyFile()
                .hasExtension("zip");
        try (final FileSystem zipFs = FileSystems.newFileSystem(blockPath.zipFilePath())) {
            final Path root = zipFs.getPath("/");
            assertThat(root).isNotNull().exists().isDirectory().isReadable().isNotEmptyDirectory();
            final Path entry = root.resolve((blockPath.blockFileName()));
            assertThat(entry).isNotNull().exists().isRegularFile().isReadable();
            assertThat(Files.exists(entry)).isTrue();
            final byte[] fromZipEntry = Files.readAllBytes(entry);
            assertThat(fromZipEntry).isEqualTo(bytesToWrite);
        }
        return archive.blockAccessor(blockPath.blockNumber());
    }

    private FilesHistoricConfig createTestConfiguration(final Path dataTepDir, final CompressionType compressionType) {
        final FilesHistoricConfig localDefaultConfig = getDefaultConfiguration();
        return new FilesHistoricConfig(
                dataTepDir,
                compressionType,
                localDefaultConfig.powersOfTenPerZipFileContents(),
                localDefaultConfig.blockRetentionThreshold(),
                localDefaultConfig.maxFilesPerDir(),
                localDefaultConfig.stagedBlockNotificationsEnabled(),
                true, // cachedZipAccessorEnabled: this test class exists specifically to exercise that path
                localDefaultConfig.maxCachedZipArchives());
    }

    private FilesHistoricConfig getDefaultConfiguration() {
        return ConfigurationBuilder.create()
                .withConfigDataType(FilesHistoricConfig.class)
                .build()
                .getConfigData(FilesHistoricConfig.class);
    }
}
