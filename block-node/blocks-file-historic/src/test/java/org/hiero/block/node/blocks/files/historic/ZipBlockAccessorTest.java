// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.blocks.files.historic;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import com.hedera.hapi.block.stream.Block;
import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.config.api.ConfigurationBuilder;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.app.fixtures.blocks.SimpleTestBlockItemBuilder;
import org.hiero.block.node.base.CompressionType;
import org.hiero.block.node.spi.historicalblocks.BlockAccessor.Format;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Test class for {@link ZipBlockAccessor}.
 */
@DisplayName("ZipBlockAccessor Tests")
class ZipBlockAccessorTest {
    /** The testing in-memory file system. */
    private FileSystem jimfs;
    /** The configuration for the test. */
    private FilesHistoricConfig defaultConfig;
    /** The temporary data directory used for the test. */
    private Path dataTempDir;
    /** The temporary links directory used for the test. */
    private Path linksTempDir;

    /** Set up the test environment before each test. */
    @BeforeEach
    void setup() throws IOException {
        // Initialize the in-memory file system
        jimfs = Jimfs.newFileSystem(
                Configuration.unix()); // Set the default configuration for the test, use jimfs for paths
        dataTempDir = jimfs.getPath("/blocks");
        Files.createDirectories(dataTempDir);
        linksTempDir = dataTempDir.resolve("links");
        Files.createDirectories(linksTempDir);
        defaultConfig =
                createTestConfiguration(dataTempDir, getDefaultConfiguration().compression());
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
     * Tests for the {@link ZipBlockAccessor} constructor.
     */
    @Nested
    @DisplayName("Constructor Tests")
    final class ConstructorTests {

        /**
         * This test aims to assert that the constructor of
         * {@link ZipBlockAccessor} throws a {@link NullPointerException} when
         * the input blockPath is null.
         */
        @Test
        @DisplayName("Test constructor throws NullPointerException when blockPath is null")
        @SuppressWarnings("all")
        void testNullBlockPath() {
            assertThatNullPointerException().isThrownBy(() -> new ZipBlockAccessor(null, linksTempDir));
        }
    }

    /**
     * Tests for the {@link ZipBlockAccessor} functionality.
     */
    @Nested
    @DisplayName("Functionality Tests")
    final class FunctionalityTests {
        /**
         * This test aims to verify that the {@link ZipBlockAccessor#blockBytes(Format)}
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
            final BlockItem[] blockItems = SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocks(1);
            final FilesHistoricConfig testConfig = createTestConfiguration(dataTempDir, compressionType);
            final BlockPath blockPath = BlockPath.computeBlockPath(
                    testConfig, blockItems[0].blockHeader().number());
            final Block block = new Block(List.of(blockItems));
            final Bytes expected = Block.PROTOBUF.toBytes(block);
            // test zipBlockAccessor.blockBytes()
            final ZipBlockAccessor toTest = createBlockAndGetAssociatedAccessor(testConfig, blockPath, expected);
            final Format format = getHappyPathFormat(compressionType);
            // The blockBytes method should return the bytes of the block with the
            // specified format. In order to assert the same bytes, we need to decompress
            // the bytes returned by the blockBytes method and compare them to the expected.
            final Bytes testResult = toTest.blockBytes(format);
            final Bytes actual = Bytes.wrap(compressionType.decompress(testResult.toByteArray()));
            assertThat(actual).isEqualTo(expected);
        }

        /**
         * This test aims to verify that the {@link ZipBlockAccessor#blockBytes(Format)}
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
            final BlockItem[] blockItems = SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocks(1);
            final FilesHistoricConfig testConfig = createTestConfiguration(dataTempDir, compressionType);
            final BlockPath blockPath = BlockPath.computeBlockPath(
                    testConfig, blockItems[0].blockHeader().number());
            final Block block = new Block(List.of(blockItems));
            final Bytes expected = Block.PROTOBUF.toBytes(block);
            // test zipBlockAccessor.blockBytes()
            final ZipBlockAccessor toTest = createBlockAndGetAssociatedAccessor(testConfig, blockPath, expected);
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
            final ZipBlockAccessor toTest2 = new ZipBlockAccessor(blockPath, linksTempDir);
            // now we should be able to access the block again
            final Bytes testResult2 = toTest2.blockBytes(format);
            final Bytes actual2 = Bytes.wrap(compressionType.decompress(testResult2.toByteArray()));
            assertThat(actual2).isEqualTo(expected);
        }

        /**
         * This test aims to verify that the {@link ZipBlockAccessor#blockBytes(Format)}
         * will correctly return a zipped block as bytes. This test will always use the
         * {@link Format#ZSTD_PROTOBUF} format to read the block bytes.
         */
        @ParameterizedTest
        @EnumSource(CompressionType.class)
        @DisplayName("Test blockBytes() returns correctly a persisted block as bytes using ZSTD_PROTOBUF format")
        @SuppressWarnings("DataFlowIssue")
        void testBlockBytesZSTDPROTOBUFFormat(final CompressionType compressionType) throws IOException {
            // build a test block
            final BlockItem[] blockItems = SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocks(1);
            final FilesHistoricConfig testConfig = createTestConfiguration(dataTempDir, compressionType);
            final BlockPath blockPath = BlockPath.computeBlockPath(
                    testConfig, blockItems[0].blockHeader().number());
            final Block block = new Block(List.of(blockItems));
            final Bytes expected = Block.PROTOBUF.toBytes(block);
            // test zipBlockAccessor.blockBytes()
            final ZipBlockAccessor toTest = createBlockAndGetAssociatedAccessor(testConfig, blockPath, expected);
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
         * This test aims to verify that the {@link ZipBlockAccessor#blockBytes(Format)}
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
            final BlockItem[] blockItems = SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocks(1);
            final FilesHistoricConfig testConfig = createTestConfiguration(dataTempDir, compressionType);
            final BlockPath blockPath = BlockPath.computeBlockPath(
                    testConfig, blockItems[0].blockHeader().number());
            final Block block = new Block(List.of(blockItems));
            final Bytes expected = Block.PROTOBUF.toBytes(block);
            // test zipBlockAccessor.blockBytes()
            final ZipBlockAccessor toTest = createBlockAndGetAssociatedAccessor(testConfig, blockPath, expected);
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
            final ZipBlockAccessor toTest2 = new ZipBlockAccessor(blockPath, linksTempDir);
            // now we should be able to access the block again
            final Bytes testResult2 = toTest2.blockBytes(Format.ZSTD_PROTOBUF);
            final Bytes actual2 = Bytes.wrap(CompressionType.ZSTD.decompress(testResult2.toByteArray()));
            assertThat(actual2).isEqualTo(expected);
        }

        /**
         * This test aims to verify that the {@link ZipBlockAccessor#blockBytes(Format)}
         * will correctly return a zipped block as bytes. This test will always use the
         * {@link Format#PROTOBUF} format to read the block bytes.
         */
        @ParameterizedTest
        @EnumSource(CompressionType.class)
        @DisplayName("Test blockBytes() returns correctly a persisted block as bytes using PROTOBUF format")
        @SuppressWarnings("DataFlowIssue")
        void testBlockBytesProtobufFormat(final CompressionType compressionType) throws IOException {
            // build a test block
            final BlockItem[] blockItems = SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocks(1);
            final FilesHistoricConfig testConfig = createTestConfiguration(dataTempDir, compressionType);
            final BlockPath blockPath = BlockPath.computeBlockPath(
                    testConfig, blockItems[0].blockHeader().number());
            final Block block = new Block(List.of(blockItems));
            final Bytes expected = Block.PROTOBUF.toBytes(block);
            // test zipBlockAccessor.blockBytes()
            final ZipBlockAccessor toTest = createBlockAndGetAssociatedAccessor(testConfig, blockPath, expected);
            // The blockBytes method should return the bytes of the block with the
            // specified format.
            // For this test, we always use the PROTOBUF format to read the block bytes,
            // no matter the actual compression type used to persist the block. With this format
            // we always expect to be returned the bytes to not be compressed.
            final Bytes testResult = toTest.blockBytes(Format.PROTOBUF);
            assertThat(testResult).isEqualTo(expected);
        }

        /**
         * This test aims to verify that the {@link ZipBlockAccessor#blockBytes(Format)}
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
            final BlockItem[] blockItems = SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocks(1);
            final FilesHistoricConfig testConfig = createTestConfiguration(dataTempDir, compressionType);
            final BlockPath blockPath = BlockPath.computeBlockPath(
                    testConfig, blockItems[0].blockHeader().number());
            final Block block = new Block(List.of(blockItems));
            final Bytes expected = Block.PROTOBUF.toBytes(block);
            // test zipBlockAccessor.blockBytes()
            final ZipBlockAccessor toTest = createBlockAndGetAssociatedAccessor(testConfig, blockPath, expected);
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
            final ZipBlockAccessor toTest2 = new ZipBlockAccessor(blockPath, linksTempDir);
            // now we should be able to access the block again
            assertThat(toTest2.blockBytes(Format.PROTOBUF)).isEqualTo(expected);
        }

        /**
         * This test aims to verify that the {@link ZipBlockAccessor#blockUnparsed()}
         * will correctly return a zipped block unparsed.
         */
        @ParameterizedTest
        @EnumSource(CompressionType.class)
        @DisplayName("Test blockUnparsed() returns correctly a persisted block unparsed")
        @SuppressWarnings("DataFlowIssue")
        void testBlockUnparsed(final CompressionType compressionType) throws IOException, ParseException {
            // build a test block
            final BlockItemUnparsed[] blockItems = SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsed(1);
            final FilesHistoricConfig testConfig = createTestConfiguration(dataTempDir, compressionType);
            final Bytes blockHeaderBytes = blockItems[0].blockHeader();
            final long blockNumber =
                    BlockHeader.PROTOBUF.parse(blockHeaderBytes).number();
            final BlockPath blockPath = BlockPath.computeBlockPath(testConfig, blockNumber);
            final BlockUnparsed expected = new BlockUnparsed(List.of(blockItems));
            final Bytes protoBytes = BlockUnparsed.PROTOBUF.toBytes(expected);
            // test zipBlockAccessor.blockUnparsed()
            final ZipBlockAccessor toTest = createBlockAndGetAssociatedAccessor(testConfig, blockPath, protoBytes);
            final BlockUnparsed actual = toTest.blockUnparsed();
            assertThat(actual).isEqualTo(expected);
        }

        /**
         * This test aims to verify that the {@link ZipBlockAccessor#blockUnparsed()}
         * will correctly return a zipped block unparsed. Here we verify that two
         * consecutive accessors to the same block will return the same block.
         * Closing an accessor does not in any way interfere with the data and
         * the ability to access it.
         */
        @ParameterizedTest
        @EnumSource(CompressionType.class)
        @DisplayName("Test blockUnparsed() returns correctly a persisted block unparsed - consecutive calls")
        @SuppressWarnings("DataFlowIssue")
        void testBlockUnparsedConsecutiveCalls(final CompressionType compressionType)
                throws IOException, ParseException {
            // build a test block
            final BlockItemUnparsed[] blockItems = SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsed(1);
            final FilesHistoricConfig testConfig = createTestConfiguration(dataTempDir, compressionType);
            final Bytes blockHeaderBytes = blockItems[0].blockHeader();
            final long blockNumber =
                    BlockHeader.PROTOBUF.parse(blockHeaderBytes).number();
            final BlockPath blockPath = BlockPath.computeBlockPath(testConfig, blockNumber);
            final BlockUnparsed expected = new BlockUnparsed(List.of(blockItems));
            final Bytes protoBytes = BlockUnparsed.PROTOBUF.toBytes(expected);
            // test zipBlockAccessor.blockUnparsed()
            final ZipBlockAccessor toTest = createBlockAndGetAssociatedAccessor(testConfig, blockPath, protoBytes);
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
            final ZipBlockAccessor toTest2 = new ZipBlockAccessor(blockPath, linksTempDir);
            // now we should be able to access the block again
            assertThat(toTest2.blockUnparsed()).isEqualTo(expected);
        }

        private Format getHappyPathFormat(final CompressionType compressionType) {
            return switch (compressionType) {
                case ZSTD -> Format.ZSTD_PROTOBUF;
                case NONE -> Format.PROTOBUF;
            };
        }
    }

    private ZipBlockAccessor createBlockAndGetAssociatedAccessor(
            final FilesHistoricConfig testConfig, final BlockPath blockPath, Bytes protoBytes) throws IOException {
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
        return new ZipBlockAccessor(blockPath, linksTempDir);
    }

    private FilesHistoricConfig createTestConfiguration(final Path dataTepDir, final CompressionType compressionType) {
        final FilesHistoricConfig localDefaultConfig = getDefaultConfiguration();
        return new FilesHistoricConfig(
                dataTepDir,
                compressionType,
                localDefaultConfig.powersOfTenPerZipFileContents(),
                localDefaultConfig.blockRetentionThreshold(),
                localDefaultConfig.maxFilesPerDir());
    }

    private FilesHistoricConfig getDefaultConfiguration() {
        return ConfigurationBuilder.create()
                .withConfigDataType(FilesHistoricConfig.class)
                .build()
                .getConfigData(FilesHistoricConfig.class);
    }
}
