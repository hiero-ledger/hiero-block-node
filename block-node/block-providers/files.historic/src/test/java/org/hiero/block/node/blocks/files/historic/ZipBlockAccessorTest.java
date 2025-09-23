// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.blocks.files.historic;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

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
    /** The temporary directory used for the test. */
    private Path tempDir;

    /** Set up the test environment before each test. */
    @BeforeEach
    void setup() throws IOException {
        // Initialize the in-memory file system
        jimfs = Jimfs.newFileSystem(
                Configuration.unix()); // Set the default configuration for the test, use jimfs for paths
        tempDir = jimfs.getPath("/blocks");
        Files.createDirectories(tempDir);
        defaultConfig =
                createTestConfiguration(tempDir, getDefaultConfiguration().compression());
    }

    /**
     * Tear down the test environment after each test.
     */
    @AfterEach
    void tearDown() throws IOException {
        // Close the Jimfs file system
        if (jimfs != null) {
            jimfs.close();
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
         * {@link ZipBlockAccessor} does not throw any exceptions when the input
         * is valid.
         */
        @Test
        @DisplayName("Test constructor throws no exception when input is valid")
        void testValidConstructor() {
            final BlockPath blockPath = BlockPath.computeBlockPath(defaultConfig, 1L);
            assertThatNoException().isThrownBy(() -> new ZipBlockAccessor(blockPath));
        }

        /**
         * This test aims to assert that the constructor of
         * {@link ZipBlockAccessor} throws a {@link NullPointerException} when
         * the input blockPath is null.
         */
        @Test
        @DisplayName("Test constructor throws NullPointerException when blockPath is null")
        @SuppressWarnings("all")
        void testNullBlockPath() {
            assertThatNullPointerException().isThrownBy(() -> new ZipBlockAccessor(null));
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
            final FilesHistoricConfig testConfig = createTestConfiguration(tempDir, compressionType);
            final BlockPath blockPath = BlockPath.computeBlockPath(
                    testConfig, blockItems[0].blockHeader().number());
            final Block block = new Block(List.of(blockItems));
            final Bytes expected = Block.PROTOBUF.toBytes(block);
            // test zipBlockAccessor.block()
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
            final FilesHistoricConfig testConfig = createTestConfiguration(tempDir, compressionType);
            final BlockPath blockPath = BlockPath.computeBlockPath(
                    testConfig, blockItems[0].blockHeader().number());
            final Block block = new Block(List.of(blockItems));
            final Bytes expected = Block.PROTOBUF.toBytes(block);
            // test zipBlockAccessor.block()
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
         * {@link Format#PROTOBUF} format to read the block bytes.
         */
        @ParameterizedTest
        @EnumSource(CompressionType.class)
        @DisplayName("Test blockBytes() returns correctly a persisted block as bytes using PROTOBUF format")
        @SuppressWarnings("DataFlowIssue")
        void testBlockBytesProtobufFormat(final CompressionType compressionType) throws IOException {
            // build a test block
            final BlockItem[] blockItems = SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocks(1);
            final FilesHistoricConfig testConfig = createTestConfiguration(tempDir, compressionType);
            final BlockPath blockPath = BlockPath.computeBlockPath(
                    testConfig, blockItems[0].blockHeader().number());
            final Block block = new Block(List.of(blockItems));
            final Bytes expected = Block.PROTOBUF.toBytes(block);
            // test zipBlockAccessor.block()
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
         * This test aims to verify that the {@link ZipBlockAccessor#block()}
         * will correctly return a zipped block.
         */
        @ParameterizedTest
        @EnumSource(CompressionType.class)
        @DisplayName("Test block() returns correctly a persisted block")
        @SuppressWarnings("DataFlowIssue")
        void testBlock(final CompressionType compressionType) throws IOException {
            // build a test block
            final BlockItem[] blockItems = SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocks(1);
            final FilesHistoricConfig testConfig = createTestConfiguration(tempDir, compressionType);
            final BlockPath blockPath = BlockPath.computeBlockPath(
                    testConfig, blockItems[0].blockHeader().number());
            final Block expected = new Block(List.of(blockItems));
            final Bytes protoBytes = Block.PROTOBUF.toBytes(expected);
            // test zipBlockAccessor.block()
            final ZipBlockAccessor toTest = createBlockAndGetAssociatedAccessor(testConfig, blockPath, protoBytes);
            final Block actual = toTest.block();
            assertThat(actual).isEqualTo(expected);
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
            final FilesHistoricConfig testConfig = createTestConfiguration(tempDir, compressionType);
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
            final byte[] fromZipEntry = Files.readAllBytes(entry);
            assertThat(fromZipEntry).isEqualTo(bytesToWrite);
        }
        return new ZipBlockAccessor(blockPath);
    }

    private FilesHistoricConfig createTestConfiguration(final Path basePath, final CompressionType compressionType) {
        final FilesHistoricConfig localDefaultConfig = getDefaultConfiguration();
        return new FilesHistoricConfig(
                basePath,
                compressionType,
                localDefaultConfig.powersOfTenPerZipFileContents(),
                localDefaultConfig.blockRetentionThreshold());
    }

    private FilesHistoricConfig getDefaultConfiguration() {
        return ConfigurationBuilder.create()
                .withConfigDataType(FilesHistoricConfig.class)
                .build()
                .getConfigData(FilesHistoricConfig.class);
    }
}
