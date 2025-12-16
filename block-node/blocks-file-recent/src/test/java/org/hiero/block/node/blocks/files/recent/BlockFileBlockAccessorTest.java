// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.blocks.files.recent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIOException;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import com.hedera.hapi.block.stream.Block;
import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.config.api.ConfigurationBuilder;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.app.fixtures.blocks.SimpleTestBlockItemBuilder;
import org.hiero.block.node.base.CompressionType;
import org.hiero.block.node.spi.historicalblocks.BlockAccessor.Format;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Test class for {@link BlockFileBlockAccessor}.
 */
@DisplayName("BlockFileBlockAccessor Tests")
class BlockFileBlockAccessorTest {
    /** The data root. */
    private Path dataRoot;
    /** The links root. */
    private Path linksRoot;
    /** Test config. */
    private FilesRecentConfig config;

    /**
     * Environment setup for the test class.
     */
    @BeforeEach
    void setup(@TempDir final Path tmpRoot) throws IOException {
        dataRoot = tmpRoot.resolve("data");
        linksRoot = dataRoot.resolve("links");
        Files.createDirectories(dataRoot);
        Files.createDirectories(linksRoot);
        config = createConfig(CompressionType.NONE, dataRoot);
    }

    private FilesRecentConfig createConfig(final CompressionType compressionType, final Path dataRoot) {
        return ConfigurationBuilder.create()
                .withConfigDataType(FilesRecentConfig.class)
                .withValue(
                        "files.recent.liveRootPath", dataRoot.toAbsolutePath().toString())
                .withValue("files.recent.compression", compressionType.name())
                .build()
                .getConfigData(FilesRecentConfig.class);
    }

    /**
     * Tests for the {@link BlockFileBlockAccessor} constructor.
     */
    @Nested
    @DisplayName("Constructor Tests")
    @SuppressWarnings("all")
    final class ConstructorTests {
        /**
         * This test asserts that a {@link NullPointerException} is thrown when
         * the input block file path is null.
         */
        @Test
        void testNullBlockFilePath() {
            // call && assert
            assertThatNullPointerException()
                    .isThrownBy(() -> new BlockFileBlockAccessor(null, config.compression(), linksRoot, 0));
        }

        /**
         * This test asserts that a {@link NullPointerException} is thrown when
         * the input compression type is null.
         */
        @Test
        void testNullCompressionType() throws IOException {
            // resolve, create & assert existing block file path before call
            final Path blockFilePath = config.liveRootPath().resolve("1.blk");
            Files.createFile(blockFilePath);
            assertThat(blockFilePath)
                    .exists()
                    .isRegularFile()
                    .isEmptyFile()
                    .isReadable()
                    .isWritable();
            // call && assert
            assertThatNullPointerException()
                    .isThrownBy(() -> new BlockFileBlockAccessor(blockFilePath, null, linksRoot, 0));
        }

        /**
         * This test asserts that a {@link IOException} is thrown
         * when the input block file path is not a file.
         */
        @Test
        void testBlockFilePathNotAFile() {
            // call && assert
            assertThatIOException()
                    .isThrownBy(() ->
                            new BlockFileBlockAccessor(config.liveRootPath(), config.compression(), linksRoot, 0));
        }

        /**
         * This test asserts that a {@link IOException} is thrown
         * when the input block file path does not exist.
         */
        @Test
        void testBlockFilePathNotExists() {
            // resolve & assert not existing block file path
            final Path blockFilePath = config.liveRootPath().resolve("1.blk");
            assertThat(blockFilePath).doesNotExist();
            // call && assert
            assertThatIOException()
                    .isThrownBy(() -> new BlockFileBlockAccessor(blockFilePath, config.compression(), linksRoot, 0));
        }

        /**
         * This test asserts that a {@link NullPointerException} is thrown when
         * the input links root is null.
         */
        @Test
        void testNullLinksRootPath() throws IOException {
            // resolve, create & assert existing block file path before call
            final Path blockFilePath = config.liveRootPath().resolve("1.blk");
            Files.createFile(blockFilePath);
            assertThat(blockFilePath)
                    .exists()
                    .isRegularFile()
                    .isEmptyFile()
                    .isReadable()
                    .isWritable();
            // call && assert
            assertThatNullPointerException()
                    .isThrownBy(() -> new BlockFileBlockAccessor(blockFilePath, config.compression(), null, 0));
        }

        /**
         * This test asserts that a {@link IOException} is thrown when
         * the input links root is not a directory.
         */
        @Test
        void testLinksRootPathDoesNotExist() throws IOException {
            // resolve, create & assert existing block file path before call
            final Path blockFilePath = config.liveRootPath().resolve("1.blk");
            Files.createFile(blockFilePath);
            assertThat(blockFilePath)
                    .exists()
                    .isRegularFile()
                    .isEmptyFile()
                    .isReadable()
                    .isWritable();
            final Path localLinksRoot = config.liveRootPath().resolve("localLinks");
            assertThat(localLinksRoot).doesNotExist();
            // call && assert
            assertThatIOException()
                    .isThrownBy(
                            () -> new BlockFileBlockAccessor(blockFilePath, config.compression(), localLinksRoot, 0));
        }

        /**
         * This test asserts that a {@link IOException} is thrown when
         * the input links root is not a directory.
         */
        @Test
        void testLinksRootPathNotADir() throws IOException {
            // resolve, create & assert existing block file path before call
            final Path blockFilePath = config.liveRootPath().resolve("1.blk");
            Files.createFile(blockFilePath);
            assertThat(blockFilePath)
                    .exists()
                    .isRegularFile()
                    .isEmptyFile()
                    .isReadable()
                    .isWritable();
            final Path localLinksRoot = config.liveRootPath().resolve("localLinks");
            Files.createFile(localLinksRoot);
            assertThat(localLinksRoot)
                    .exists()
                    .isRegularFile()
                    .isEmptyFile()
                    .isReadable()
                    .isWritable();
            // call && assert
            assertThatIOException()
                    .isThrownBy(
                            () -> new BlockFileBlockAccessor(blockFilePath, config.compression(), localLinksRoot, 0));
        }

        /**
         * This test asserts that a {@link IOException} is thrown
         * when the input links root dir path does not exist.
         */
        @Test
        void testRootDirDoesNotExist() throws IOException {
            // resolve, create & assert existing block file path before call
            final Path blockFilePath = config.liveRootPath().resolve("1.blk");
            Files.createFile(blockFilePath);
            assertThat(blockFilePath)
                    .exists()
                    .isRegularFile()
                    .isEmptyFile()
                    .isReadable()
                    .isWritable();
            // resolve & assert not existing links root
            final Path localLinksRoot = config.liveRootPath().resolve("localLinks");
            assertThat(localLinksRoot).doesNotExist();
            // call && assert
            assertThatIOException()
                    .isThrownBy(
                            () -> new BlockFileBlockAccessor(blockFilePath, config.compression(), localLinksRoot, 0));
        }

        /**
         * This test asserts that a {@link IOException} is thrown
         * when the input links root dir path is actually a file.
         */
        @Test
        void testRootDirIsFile() throws IOException {
            // resolve, create & assert existing block file path before call
            final Path blockFilePath = config.liveRootPath().resolve("1.blk");
            Files.createFile(blockFilePath);
            assertThat(blockFilePath)
                    .exists()
                    .isRegularFile()
                    .isEmptyFile()
                    .isReadable()
                    .isWritable();
            // resolve & assert links root is a file
            final Path localLinksRoot = config.liveRootPath().resolve("localLinks");
            Files.createFile(localLinksRoot);
            assertThat(localLinksRoot)
                    .exists()
                    .isRegularFile()
                    .isEmptyFile()
                    .isReadable()
                    .isWritable();
            // call && assert
            assertThatIOException()
                    .isThrownBy(
                            () -> new BlockFileBlockAccessor(blockFilePath, config.compression(), localLinksRoot, 0));
        }

        /**
         * This test asserts that no exception is thrown when the input
         * parameters are valid.
         */
        @Test
        void testValidConstructor() throws IOException {
            // resolve, create & assert existing block file path before call
            final Path blockFilePath = config.liveRootPath().resolve("1.blk");
            Files.createFile(blockFilePath);
            assertThat(blockFilePath)
                    .exists()
                    .isRegularFile()
                    .isEmptyFile()
                    .isReadable()
                    .isWritable();
            // call && assert
            assertThatNoException()
                    .isThrownBy(() -> new BlockFileBlockAccessor(blockFilePath, config.compression(), linksRoot, 0));
        }
    }

    /**
     * Tests for the {@link BlockFileBlockAccessor} functionality.
     */
    @Nested
    @DisplayName("Functionality Tests")
    final class FunctionalityTests {
        /**
         * This test aims to verify that the {@link BlockFileBlockAccessor#block()} will correctly return a
         * persisted block.
         */
        @ParameterizedTest
        @EnumSource(CompressionType.class)
        @DisplayName("Test block method returns correctly a persisted block")
        void testBlock(final CompressionType compressionType) throws IOException {
            // create block file path before call
            final Path blockFilePath = config.liveRootPath().resolve("0.blk" + compressionType.extension());
            // build a test block
            final BlockItem[] blockItems = SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocks(1);
            final Block expected = new Block(List.of(blockItems));
            final Bytes protoBytes = Block.PROTOBUF.toBytes(expected);
            // create instance to test
            final BlockFileBlockAccessor toTest =
                    createBlockAndGetAssociatedAccessor(0, blockFilePath, compressionType, protoBytes);
            // test accessor.block()
            final Block actual = toTest.block();
            assertThat(actual).isEqualTo(expected);
        }

        /**
         * This test aims to verify that the {@link BlockFileBlockAccessor#block()} will correctly return a
         * persisted block. Here we aim to verify that when one accessor closes and deletes the link it has
         * created, this will not affect actual data and subsequent accessors can still retrieve the data.
         */
        @ParameterizedTest
        @EnumSource(CompressionType.class)
        @DisplayName("Test block method returns correctly a persisted block - subsequent reads")
        void testBlockSubsequentReads(final CompressionType compressionType) throws IOException {
            // create block file path before call
            final long blockNumber = 0;
            final Path blockFilePath =
                    config.liveRootPath().resolve(blockNumber + ".blk" + compressionType.extension());
            // build a test block
            final BlockItem[] blockItems = SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocks(1);
            final Block expected = new Block(List.of(blockItems));
            final Bytes protoBytes = Block.PROTOBUF.toBytes(expected);
            // create instance to test
            final BlockFileBlockAccessor toTest =
                    createBlockAndGetAssociatedAccessor(blockNumber, blockFilePath, compressionType, protoBytes);
            // test accessor.block()
            final Block actual = toTest.block();
            assertThat(actual).isEqualTo(expected);
            // calling close will drop the hard link, and accessor will no longer
            // be able to find the data
            toTest.close();
            // assert that the actual data still exists
            assertThat(blockFilePath).exists().isRegularFile().isNotEmptyFile().isReadable();
            // assert that the accessor can no longer find the data
            assertThat(toTest.block()).isNull();
            // now create a new accessor
            final BlockFileBlockAccessor toTest2 = new BlockFileBlockAccessor(
                    blockFilePath, createConfig(compressionType, dataRoot).compression(), linksRoot, blockNumber);
            // assert that the second accessor can retrieve the same data as did the first one
            assertThat(toTest2.block()).isEqualTo(expected);
        }

        /**
         * This test aims to verify that the {@link BlockFileBlockAccessor#block()} will correctly handle
         * IOExceptions encountered when attempting to persist blocks.
         */
        @ParameterizedTest
        @EnumSource(CompressionType.class)
        @DisplayName("Test block() method correctly handles an IOException")
        void testBlockIOException(final CompressionType compressionType) throws IOException {
            // create block file path before call
            final Path blockFilePath = config.liveRootPath().resolve("0.blk" + compressionType.extension());
            // build a test block
            final BlockItem[] blockItems = SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocks(1);
            final Block expected = new Block(List.of(blockItems));
            final Bytes protoBytes = Block.PROTOBUF.toBytes(expected);
            // create instance to test
            final BlockFileBlockAccessor toTest =
                    createBlockAndGetAssociatedAccessor(0, blockFilePath, compressionType, protoBytes);

            // calling close will drop the hard link, and accessor will no longer
            // be able to find the data
            toTest.close();

            // assert that the actual data still exists
            assertThat(blockFilePath).exists().isRegularFile().isNotEmptyFile().isReadable();

            assertThatNoException().isThrownBy(toTest::block);
            assertThat(toTest.block()).isNull();
        }

        /**
         * This test aims to verify that the {@link BlockFileBlockAccessor#block()} will correctly handle
         * protobuf parse exception encountered when attempting to persist blocks.
         */
        @ParameterizedTest
        @EnumSource(CompressionType.class)
        @DisplayName("Test block() method correctly handles proto parse exception")
        void testBlockParseException(final CompressionType compressionType) throws IOException {
            // create block file path before call
            final Path blockFilePath = config.liveRootPath().resolve("0.blk" + compressionType.extension());

            // provide empty byte array to simulate parse exception
            final Bytes protoBytes = Bytes.wrap(new byte[48]);
            // create instance to test
            final BlockFileBlockAccessor toTest =
                    createBlockAndGetAssociatedAccessor(0, blockFilePath, compressionType, protoBytes);

            assertThatNoException().isThrownBy(toTest::block);
            assertThat(toTest.block()).isNull();
        }

        /**
         * This test aims to verify that the {@link BlockFileBlockAccessor#blockUnparsed()} will correctly return a
         * persisted block.
         */
        @ParameterizedTest
        @EnumSource(CompressionType.class)
        @DisplayName("Test blockUnparsed method returns correctly a persisted block")
        void testBlockUnparsed(final CompressionType compressionType) throws IOException {
            // create block file path before call
            final Path blockFilePath = config.liveRootPath().resolve("0.blk" + compressionType.extension());
            // build a test block
            final BlockItemUnparsed[] blockItems = SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsed(1);
            final BlockUnparsed expected = new BlockUnparsed(List.of(blockItems));
            final Bytes protoBytes = BlockUnparsed.PROTOBUF.toBytes(expected);
            // create instance to test
            final BlockFileBlockAccessor toTest =
                    createBlockAndGetAssociatedAccessor(0, blockFilePath, compressionType, protoBytes);
            // test accessor.blockUnparsed()
            final BlockUnparsed actual = toTest.blockUnparsed();
            assertThat(actual).isEqualTo(expected);
        }

        /**
         * This test aims to verify that the {@link BlockFileBlockAccessor#blockUnparsed()} will correctly return a
         * persisted block. Here we aim to verify that when one accessor closes and deletes the link it has
         * created, this will not affect actual data and subsequent accessors can still retrieve the data.
         */
        @ParameterizedTest
        @EnumSource(CompressionType.class)
        @DisplayName("Test blockUnparsed method returns correctly a persisted block")
        void testBlockUnparsedSubsequentReads(final CompressionType compressionType) throws IOException {
            // create block file path before call
            final long blockNumber = 0;
            final Path blockFilePath =
                    config.liveRootPath().resolve(blockNumber + ".blk" + compressionType.extension());
            // build a test block
            final BlockItemUnparsed[] blockItems = SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsed(1);
            final BlockUnparsed expected = new BlockUnparsed(List.of(blockItems));
            final Bytes protoBytes = BlockUnparsed.PROTOBUF.toBytes(expected);
            // create instance to test
            final BlockFileBlockAccessor toTest =
                    createBlockAndGetAssociatedAccessor(blockNumber, blockFilePath, compressionType, protoBytes);
            // test accessor.blockUnparsed()
            final BlockUnparsed actual = toTest.blockUnparsed();
            assertThat(actual).isEqualTo(expected);
            // calling close will drop the hard link, and accessor will no longer
            // be able to find the data
            toTest.close();
            // assert that the actual data still exists
            assertThat(blockFilePath).exists().isRegularFile().isNotEmptyFile().isReadable();
            // assert that the accessor can no longer find the data
            assertThat(toTest.blockUnparsed()).isNull();
            // now create a new accessor
            final BlockFileBlockAccessor toTest2 = new BlockFileBlockAccessor(
                    blockFilePath, createConfig(compressionType, dataRoot).compression(), linksRoot, blockNumber);
            // assert that the second accessor can retrieve the same data as did the first one
            assertThat(toTest2.blockUnparsed()).isEqualTo(expected);
        }

        /**
         * This test aims to verify that the {@link BlockFileBlockAccessor#blockUnparsed()} will correctly handle
         * IOException encountered when attempting to persist blocks.
         */
        @ParameterizedTest
        @EnumSource(CompressionType.class)
        @DisplayName("Test blockUnparsed() method correctly handles an IOException")
        void testBlockUnparsedIOException(final CompressionType compressionType) throws IOException {
            // create block file path before call
            final Path blockFilePath = config.liveRootPath().resolve("0.blk" + compressionType.extension());
            // build a test block
            final BlockItem[] blockItems = SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocks(1);
            final Block expected = new Block(List.of(blockItems));
            final Bytes protoBytes = Block.PROTOBUF.toBytes(expected);
            // create instance to test
            final BlockFileBlockAccessor toTest =
                    createBlockAndGetAssociatedAccessor(0, blockFilePath, compressionType, protoBytes);

            // calling close will drop the hard link, and accessor will no longer
            // be able to find the data
            toTest.close();

            // assert that the actual data still exists
            assertThat(blockFilePath).exists().isRegularFile().isNotEmptyFile().isReadable();

            assertThatNoException().isThrownBy(toTest::blockUnparsed);
            assertThat(toTest.blockUnparsed()).isNull();
        }

        /**
         * This test aims to verify that the {@link BlockFileBlockAccessor#blockUnparsed()} will correctly handle
         * protobuf parse exceptions encountered when attempting to persist blocks.
         */
        @ParameterizedTest
        @EnumSource(CompressionType.class)
        @DisplayName("Test blockUnparsed() method correctly handles proto parse exception")
        void testBlockUnparseException(final CompressionType compressionType) throws IOException {
            // create block file path before call
            final Path blockFilePath = config.liveRootPath().resolve("0.blk" + compressionType.extension());

            // provide empty byte array to simulate parse exception
            final Bytes protoBytes = Bytes.wrap(new byte[48]);
            // create instance to test
            final BlockFileBlockAccessor toTest =
                    createBlockAndGetAssociatedAccessor(0, blockFilePath, compressionType, protoBytes);

            assertThatNoException().isThrownBy(toTest::blockUnparsed);
            assertThat(toTest.blockUnparsed()).isNull();
        }

        /**
         * This test aims to verify that the {@link BlockFileBlockAccessor#blockBytes(Format)} will correctly
         * return a persisted block as bytes.
         */
        @ParameterizedTest
        @EnumSource(CompressionType.class)
        @DisplayName("Test blockBytes method returns correctly a persisted block as bytes")
        void testBlockBytes(final CompressionType compressionType) throws IOException {
            // create block file path before call
            final Path blockFilePath = config.liveRootPath().resolve("0.blk" + compressionType.extension());
            // create instance to test
            final BlockFileBlockAccessor toTest =
                    buildAndCreateBlockAndGetAssociatedAccessor(0, blockFilePath, compressionType, 1);
            // test accessor.blockBytes()
            final Format format =
                    switch (compressionType) {
                        case ZSTD -> Format.ZSTD_PROTOBUF;
                        case NONE -> Format.PROTOBUF;
                    };
            final Bytes expectedFileBytes = Bytes.wrap(Files.readAllBytes(blockFilePath));
            final String expected = expectedFileBytes.toHex();
            final Bytes actual = toTest.blockBytes(format);
            assertThat(actual.toHex()).isEqualTo(expected);
        }

        /**
         * This test aims to verify that the {@link BlockFileBlockAccessor#blockBytes(Format)} will correctly
         * return a persisted block as bytes. Here we aim to verify that when one accessor closes and deletes the
         * link it has created, this will not affect actual data and subsequent accessors can still retrieve the data.
         */
        @ParameterizedTest
        @EnumSource(CompressionType.class)
        @DisplayName("Test blockBytes method returns correctly a persisted block as bytes - subsequent reads")
        void testBlockBytesSubsequentReads(final CompressionType compressionType) throws IOException {
            // create block file path before call
            final long blockNumber = 0;
            final Path blockFilePath =
                    config.liveRootPath().resolve(blockNumber + ".blk" + compressionType.extension());
            // create instance to test
            final BlockFileBlockAccessor toTest =
                    buildAndCreateBlockAndGetAssociatedAccessor(blockNumber, blockFilePath, compressionType, 1);
            // test accessor.blockBytes()
            final Format format =
                    switch (compressionType) {
                        case ZSTD -> Format.ZSTD_PROTOBUF;
                        case NONE -> Format.PROTOBUF;
                    };
            final Bytes expectedFileBytes = Bytes.wrap(Files.readAllBytes(blockFilePath));
            final String expected = expectedFileBytes.toHex();
            final Bytes actual = toTest.blockBytes(format);
            assertThat(actual.toHex()).isEqualTo(expected);
            // calling close will drop the hard link, and accessor will no longer
            // be able to find the data
            toTest.close();
            // assert that the actual data still exists
            assertThat(blockFilePath).exists().isRegularFile().isNotEmptyFile().isReadable();
            // assert that the accessor can no longer find the data
            assertThat(toTest.blockBytes(format)).isNull();
            // now create a new accessor
            final BlockFileBlockAccessor toTest2 = new BlockFileBlockAccessor(
                    blockFilePath, createConfig(compressionType, dataRoot).compression(), linksRoot, blockNumber);
            // assert that the second accessor can retrieve the same data as did the first one
            assertThat(toTest2.blockBytes(format).toHex()).isEqualTo(expected);
        }

        /**
         * This test aims to verify that the {@link BlockFileBlockAccessor#blockBytes(Format)} will correctly
         * handle IOExceptions encountered.
         */
        @ParameterizedTest
        @EnumSource(CompressionType.class)
        @DisplayName("Test blockBytes method correctly handles an IOException")
        void testBlockBytesIOException(final CompressionType compressionType) throws IOException {
            // create block file path before call
            final Path blockFilePath = config.liveRootPath().resolve("0.blk" + compressionType.extension());
            // create instance to test
            final BlockFileBlockAccessor toTest =
                    buildAndCreateBlockAndGetAssociatedAccessor(0, blockFilePath, compressionType, 1);
            // test accessor.blockBytes()
            final Format format =
                    switch (compressionType) {
                        case ZSTD -> Format.ZSTD_PROTOBUF;
                        case NONE -> Format.PROTOBUF;
                    };

            // calling close will drop the hard link, and accessor will no longer
            // be able to find the data
            toTest.close();

            // assert that the actual data still exists
            assertThat(blockFilePath).exists().isRegularFile().isNotEmptyFile().isReadable();

            assertThatNoException().isThrownBy(() -> toTest.blockBytes(format));
            assertThat(toTest.blockBytes(format)).isNull();
        }

        /**
         * This test aims to verify that the {@link BlockFileBlockAccessor#blockBytes(Format)} will correctly
         * handle an IOException when the format is protobuf zstd but the compression is none.
         */
        @Test
        @DisplayName("Test blockBytes method correctly handles an IOException on ZSTF protobuf but no compression")
        void testBlockBytesZSTDIOException() throws IOException {
            // create block file path before call
            final CompressionType compressionType = CompressionType.NONE;
            final Path blockFilePath = config.liveRootPath().resolve("0.blk" + compressionType.extension());

            // create instance to test
            final BlockFileBlockAccessor toTest =
                    buildAndCreateBlockAndGetAssociatedAccessor(0, blockFilePath, compressionType, 1);

            // calling close will drop the hard link, and accessor will no longer
            // be able to find the data
            toTest.close();

            // assert that the actual data still exists
            assertThat(blockFilePath).exists().isRegularFile().isNotEmptyFile().isReadable();

            assertThatNoException().isThrownBy(() -> toTest.blockBytes(Format.ZSTD_PROTOBUF));
            assertThat(toTest.blockBytes(Format.ZSTD_PROTOBUF)).isNull();
        }

        /**
         * This test aims to assert that the {@link BlockFileBlockAccessor#close()}
         * will not remove the actual block file, only the hard link created and
         * will not alter the data thereof.
         */
        @Test
        @DisplayName(
                "Test close() method removes the hard link, does not remove the actual block file and does not alter data")
        void testCloseDoesNotRemoveBlockFile() throws IOException {
            // create block file path before call
            final CompressionType compressionType = CompressionType.NONE;
            final Path actual = config.liveRootPath().resolve("0.blk" + compressionType.extension());
            // create instance to test
            final BlockFileBlockAccessor toTest =
                    buildAndCreateBlockAndGetAssociatedAccessor(0, actual, compressionType, 1);
            // assert that the hardlink was created due to issuing the accessor
            assertThat(linksRoot).exists().isNotEmptyDirectory();
            // read the contents of the actual block file before close()
            final byte[] expected = Files.readAllBytes(actual);
            // call
            toTest.close();
            // assert that the actual data still exists
            assertThat(actual)
                    .exists()
                    .isRegularFile()
                    .isNotEmptyFile()
                    .isReadable()
                    .hasBinaryContent(expected);
            // assert that the hardlink was removed
            assertThat(linksRoot).exists().isEmptyDirectory();
        }

        private BlockFileBlockAccessor createBlockAndGetAssociatedAccessor(
                long blockNumber, final Path blockFilePath, final CompressionType compressionType, Bytes protoBytes)
                throws IOException {

            // create & assert existing block file path before call
            Files.createFile(blockFilePath);
            assertThat(blockFilePath).exists().isEmptyFile();
            // it is important the output stream is closed as the compression writes a footer on close
            try (final OutputStream out = compressionType.wrapStream(Files.newOutputStream(blockFilePath))) {
                protoBytes.writeTo(out);
            }
            // assert the test block file is populated
            assertThat(blockFilePath).isNotEmptyFile();
            return new BlockFileBlockAccessor(
                    blockFilePath, createConfig(compressionType, dataRoot).compression(), linksRoot, blockNumber);
        }

        private BlockFileBlockAccessor buildAndCreateBlockAndGetAssociatedAccessor(
                long blockNumber,
                final Path blockFilePath,
                final CompressionType compressionType,
                final int numberOfBlocks)
                throws IOException {
            final BlockItemUnparsed[] blockItems1 =
                    SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsed(numberOfBlocks);
            final BlockUnparsed expected1 = new BlockUnparsed(List.of(blockItems1));
            final Bytes protoBytes = BlockUnparsed.PROTOBUF.toBytes(expected1);

            return createBlockAndGetAssociatedAccessor(blockNumber, blockFilePath, compressionType, protoBytes);
        }
    }
}
