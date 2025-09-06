// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.blocks.files.recent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import com.hedera.hapi.block.stream.Block;
import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
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
 * Test class for {@link BlockFileBlockAccessor}.
 */
@DisplayName("BlockFileBlockAccessor Tests")
class BlockFileBlockAccessorTest {
    /** The testing in-memory file system. */
    private FileSystem jimfs;
    /** Test Base Path, resolved under jimfs. */
    private Path testBasePath;

    /**
     * Environment setup for the test class.
     */
    @BeforeEach
    void setup() throws IOException {
        // Initialize the in-memory file system
        jimfs = Jimfs.newFileSystem(Configuration.unix());
        testBasePath = jimfs.getPath("/tmp");
        Files.createDirectories(testBasePath);
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
                    .isThrownBy(() -> new BlockFileBlockAccessor(null, CompressionType.NONE, 0));
        }

        /**
         * This test asserts that a {@link NullPointerException} is thrown when
         * the input compression type is null.
         */
        @Test
        void testNullCompressionType() throws IOException {
            // resolve, create & assert existing block file path before call
            final Path blockFilePath = testBasePath.resolve("1.blk");
            Files.createFile(blockFilePath);
            assertThat(blockFilePath)
                    .exists()
                    .isRegularFile()
                    .isEmptyFile()
                    .isReadable()
                    .isWritable();
            // call && assert
            assertThatNullPointerException().isThrownBy(() -> new BlockFileBlockAccessor(blockFilePath, null, 0));
        }

        /**
         * This test asserts that a {@link IllegalArgumentException} is thrown
         * when the input block file path is not a file.
         */
        @Test
        void testBlockFilePathNotAFile() {
            // call && assert
            assertThatIllegalArgumentException()
                    .isThrownBy(() -> new BlockFileBlockAccessor(testBasePath, CompressionType.NONE, 0));
        }

        /**
         * This test asserts that a {@link IllegalArgumentException} is thrown
         * when the input block file path does not exist.
         */
        @Test
        void testBlockFilePathNotExists() {
            // resolve & assert not existing block file path
            final Path blockFilePath = testBasePath.resolve("1.blk");
            assertThat(blockFilePath).doesNotExist();
            // call && assert
            assertThatIllegalArgumentException()
                    .isThrownBy(() -> new BlockFileBlockAccessor(blockFilePath, CompressionType.NONE, 0));
        }

        /**
         * This test asserts that no exception is thrown when the input
         * parameters are valid.
         */
        @Test
        void testValidConstructor() throws IOException {
            // resolve, create & assert existing block file path before call
            final Path blockFilePath = testBasePath.resolve("1.blk");
            Files.createFile(blockFilePath);
            assertThat(blockFilePath)
                    .exists()
                    .isRegularFile()
                    .isEmptyFile()
                    .isReadable()
                    .isWritable();
            // call && assert
            assertThatNoException()
                    .isThrownBy(() -> new BlockFileBlockAccessor(blockFilePath, CompressionType.NONE, 0));
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
            final Path blockFilePath = testBasePath.resolve("0.blk" + compressionType.extension());
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
         * This test aims to verify that the {@link BlockFileBlockAccessor#block()} will correctly handle
         * IOExceptions encountered when attempting to persist blocks.
         */
        @ParameterizedTest
        @EnumSource(CompressionType.class)
        @DisplayName("Test block() method correctly handles an IOException")
        void testBlockIOException(final CompressionType compressionType) throws IOException {
            // create block file path before call
            final Path blockFilePath = testBasePath.resolve("0.blk" + compressionType.extension());
            // build a test block
            final BlockItem[] blockItems = SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocks(1);
            final Block expected = new Block(List.of(blockItems));
            final Bytes protoBytes = Block.PROTOBUF.toBytes(expected);
            // create instance to test
            final BlockFileBlockAccessor toTest =
                    createBlockAndGetAssociatedAccessor(0, blockFilePath, compressionType, protoBytes);

            // delete the file to simulate NoSuchFileException IOException
            Files.delete(blockFilePath);

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
            final Path blockFilePath = testBasePath.resolve("0.blk" + compressionType.extension());

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
            final Path blockFilePath = testBasePath.resolve("0.blk" + compressionType.extension());
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
         * This test aims to verify that the {@link BlockFileBlockAccessor#blockUnparsed()} will correctly handle
         * IOException encountered when attempting to persist blocks.
         */
        @ParameterizedTest
        @EnumSource(CompressionType.class)
        @DisplayName("Test blockUnparsed() method correctly handles an IOException")
        void testBlockUnparsedIOException(final CompressionType compressionType) throws IOException {
            // create block file path before call
            final Path blockFilePath = testBasePath.resolve("0.blk" + compressionType.extension());
            // build a test block
            final BlockItem[] blockItems = SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocks(1);
            final Block expected = new Block(List.of(blockItems));
            final Bytes protoBytes = Block.PROTOBUF.toBytes(expected);
            // create instance to test
            final BlockFileBlockAccessor toTest =
                    createBlockAndGetAssociatedAccessor(0, blockFilePath, compressionType, protoBytes);

            // delete the file to simulate NoSuchFileException IOException
            Files.delete(blockFilePath);

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
            final Path blockFilePath = testBasePath.resolve("0.blk" + compressionType.extension());

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
            final Path blockFilePath = testBasePath.resolve("0.blk" + compressionType.extension());
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
         * handle IOExceptions encountered.
         */
        @ParameterizedTest
        @EnumSource(CompressionType.class)
        @DisplayName("Test blockBytes method correctly handles an IOException")
        void testBlockBytesIOException(final CompressionType compressionType) throws IOException {
            // create block file path before call
            final Path blockFilePath = testBasePath.resolve("0.blk" + compressionType.extension());
            // create instance to test
            final BlockFileBlockAccessor toTest =
                    buildAndCreateBlockAndGetAssociatedAccessor(0, blockFilePath, compressionType, 1);
            // test accessor.blockBytes()
            final Format format =
                    switch (compressionType) {
                        case ZSTD -> Format.ZSTD_PROTOBUF;
                        case NONE -> Format.PROTOBUF;
                    };

            // delete the file to simulate NoSuchFileException IOException
            Files.delete(blockFilePath);

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
            final Path blockFilePath = testBasePath.resolve("0.blk" + compressionType.extension());

            // create instance to test
            final BlockFileBlockAccessor toTest =
                    buildAndCreateBlockAndGetAssociatedAccessor(0, blockFilePath, compressionType, 1);

            // delete the file to simulate NoSuchFileException IOException
            Files.delete(blockFilePath);

            assertThatNoException().isThrownBy(() -> toTest.blockBytes(Format.ZSTD_PROTOBUF));
            assertThat(toTest.blockBytes(Format.ZSTD_PROTOBUF)).isNull();
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
            return new BlockFileBlockAccessor(blockFilePath, compressionType, blockNumber);
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
