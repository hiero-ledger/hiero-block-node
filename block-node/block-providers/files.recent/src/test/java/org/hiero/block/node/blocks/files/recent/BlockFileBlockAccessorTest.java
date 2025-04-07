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
import com.hedera.pbj.runtime.UncheckedParseException;
import com.hedera.pbj.runtime.io.buffer.BufferedData;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.assertj.core.api.Assertions;
import org.hiero.block.node.app.fixtures.blocks.SimpleTestBlockItemBuilder;
import org.hiero.block.node.base.CompressionType;
import org.hiero.block.node.spi.historicalblocks.BlockAccessor.Format;
import org.hiero.hapi.block.node.BlockItemUnparsed;
import org.hiero.hapi.block.node.BlockUnparsed;
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
         * the input base path is null.
         */
        @Test
        void testNullBasePath() throws IOException {
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
            assertThatNullPointerException()
                    .isThrownBy(() -> new BlockFileBlockAccessor(null, blockFilePath, CompressionType.NONE));
        }

        /**
         * This test asserts that a {@link NullPointerException} is thrown when
         * the input block file path is null.
         */
        @Test
        void testNullBlockFilePath() {
            // call && assert
            assertThatNullPointerException()
                    .isThrownBy(() -> new BlockFileBlockAccessor(testBasePath, null, CompressionType.NONE));
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
            assertThatNullPointerException()
                    .isThrownBy(() -> new BlockFileBlockAccessor(testBasePath, blockFilePath, null));
        }

        /**
         * This test asserts that a {@link IllegalArgumentException} is thrown
         * when the input block file path is not a file.
         */
        @Test
        void testBlockFilePathNotAFile() {
            // call && assert
            assertThatIllegalArgumentException()
                    .isThrownBy(() -> new BlockFileBlockAccessor(testBasePath, testBasePath, CompressionType.NONE));
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
                    .isThrownBy(() -> new BlockFileBlockAccessor(testBasePath, blockFilePath, CompressionType.NONE));
        }

        /**
         * This test asserts that a {@link NullPointerException} is thrown when
         * the input base path is not a directory.
         */
        @Test
        void testBasePathNotADirectory() throws IOException {
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
            assertThatIllegalArgumentException()
                    .isThrownBy(() -> new BlockFileBlockAccessor(blockFilePath, blockFilePath, CompressionType.NONE));
        }

        /**
         * This test asserts that a {@link NullPointerException} is thrown when
         * the input base path does not exist.
         */
        @Test
        void testBasePathNotExists() throws IOException {
            // resolve, create & assert existing block file path before call
            final Path blockFilePath = testBasePath.resolve("1.blk");
            Files.createFile(blockFilePath);
            assertThat(blockFilePath)
                    .exists()
                    .isRegularFile()
                    .isEmptyFile()
                    .isReadable()
                    .isWritable();
            // resolve & assert not existing base path
            final Path basePath = testBasePath.resolve("non-existing");
            assertThat(basePath).doesNotExist();
            // call && assert
            assertThatIllegalArgumentException()
                    .isThrownBy(() -> new BlockFileBlockAccessor(basePath, blockFilePath, CompressionType.NONE));
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
                    .isThrownBy(() -> new BlockFileBlockAccessor(testBasePath, blockFilePath, CompressionType.NONE));
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
            final Path blockFilePath = testBasePath.resolve("0.blk".concat(compressionType.extension()));
            // build a test block
            final BlockItem[] blockItems = SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocks(1);
            final Block expected = new Block(List.of(blockItems));
            final Bytes protoBytes = Block.PROTOBUF.toBytes(expected);
            // create instance to test
            final BlockFileBlockAccessor toTest = createBlockAndGetAssociatedAccessor(blockFilePath, compressionType, protoBytes);
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
            final Path blockFilePath = testBasePath.resolve("0.blk".concat(compressionType.extension()));
            // build a test block
            final BlockItem[] blockItems = SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocks(1);
            final Block expected = new Block(List.of(blockItems));
            final Bytes protoBytes = Block.PROTOBUF.toBytes(expected);
            // create instance to test
            final BlockFileBlockAccessor toTest = createBlockAndGetAssociatedAccessor(blockFilePath, compressionType, protoBytes);

            // delete the file to simulate NoSuchFileException IOException
            Files.delete(blockFilePath);

            // test accessor.block() expecting IOException
            try {
                toTest.block();
                assertThat(false).isTrue(); // exception should have been thrown
            } catch (Throwable e) {
                // expected
                Assertions.assertThat(e).isInstanceOf(UncheckedIOException.class);
            }
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
            final Path blockFilePath = testBasePath.resolve("0.blk".concat(compressionType.extension()));

            // provide empty byte array to simulate parse exception
            final Bytes protoBytes = Bytes.wrap(new byte[48]);
            // create instance to test
            final BlockFileBlockAccessor toTest = createBlockAndGetAssociatedAccessor(blockFilePath, compressionType, protoBytes);

            // test accessor.block() expecting IOException
            try {
                toTest.block();
                assertThat(false).isTrue(); // exception should have been thrown
            } catch (Throwable e) {
                // expected
                Assertions.assertThat(e).isInstanceOf(UncheckedParseException.class);
            }
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
            final Path blockFilePath = testBasePath.resolve("0.blk".concat(compressionType.extension()));
            // build a test block
            final BlockItemUnparsed[] blockItems = SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsed(1);
            final BlockUnparsed expected = new BlockUnparsed(List.of(blockItems));
            final Bytes protoBytes = BlockUnparsed.PROTOBUF.toBytes(expected);
            // create instance to test
            final BlockFileBlockAccessor toTest = createBlockAndGetAssociatedAccessor(blockFilePath, compressionType, protoBytes);
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
            final Path blockFilePath = testBasePath.resolve("0.blk".concat(compressionType.extension()));
            // build a test block
            final BlockItem[] blockItems = SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocks(1);
            final Block expected = new Block(List.of(blockItems));
            final Bytes protoBytes = Block.PROTOBUF.toBytes(expected);
            // create instance to test
            final BlockFileBlockAccessor toTest = createBlockAndGetAssociatedAccessor(blockFilePath, compressionType, protoBytes);

            // delete the file to simulate NoSuchFileException IOException
            Files.delete(blockFilePath);

            // test accessor.block() expecting IOException
            try {
                toTest.blockUnparsed();
                assertThat(false).isTrue(); // exception should have been thrown
            } catch (Throwable e) {
                // expected
                Assertions.assertThat(e).isInstanceOf(UncheckedIOException.class);
            }
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
            final Path blockFilePath = testBasePath.resolve("0.blk".concat(compressionType.extension()));

            // provide empty byte array to simulate parse exception
            final Bytes protoBytes = Bytes.wrap(new byte[48]);
            // create instance to test
            final BlockFileBlockAccessor toTest = createBlockAndGetAssociatedAccessor(blockFilePath, compressionType, protoBytes);

            // test accessor.block() expecting IOException
            try {
                toTest.blockUnparsed();
                assertThat(false).isTrue(); // exception should have been thrown
            } catch (Throwable e) {
                // expected
                Assertions.assertThat(e).isInstanceOf(UncheckedParseException.class);
            }
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
            final Path blockFilePath = testBasePath.resolve("0.blk".concat(compressionType.extension()));
            // create instance to test
            final BlockFileBlockAccessor toTest = buildAndCreateBlockAndGetAssociatedAccessor(blockFilePath, compressionType, 1);
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
            final Path blockFilePath = testBasePath.resolve("0.blk".concat(compressionType.extension()));
            // create instance to test
            final BlockFileBlockAccessor toTest = buildAndCreateBlockAndGetAssociatedAccessor(blockFilePath, compressionType, 1);
            // test accessor.blockBytes()
            final Format format =
                    switch (compressionType) {
                        case ZSTD -> Format.ZSTD_PROTOBUF;
                        case NONE -> Format.PROTOBUF;
                    };

            // delete the file to simulate NoSuchFileException IOException
            Files.delete(blockFilePath);

            // test accessor.blockBytes() expecting IOException
            try {
                toTest.blockBytes(format);
                assertThat(false).isTrue(); // exception should have been thrown
            } catch (Throwable e) {
                // expected
                Assertions.assertThat(e).isInstanceOf(UncheckedIOException.class);
            }
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
            final Path blockFilePath = testBasePath.resolve("0.blk".concat(compressionType.extension()));

            // create instance to test
            final BlockFileBlockAccessor toTest = buildAndCreateBlockAndGetAssociatedAccessor(blockFilePath, compressionType, 1);

            // delete the file to simulate NoSuchFileException IOException
            Files.delete(blockFilePath);

            // test accessor.blockBytes() expecting IOException
            try {
                toTest.blockBytes(Format.ZSTD_PROTOBUF);
                assertThat(false).isTrue(); // exception should have been thrown
            } catch (Throwable e) {
                // expected
                Assertions.assertThat(e).isInstanceOf(UncheckedIOException.class);
            }
        }

        /**
         * This test aims to verify that the {@link BlockFileBlockAccessor#writeBytesTo(Format, OutputStream)}
         * will correctly write bytes to the target {@link OutputStream}.
         */
        @ParameterizedTest
        @EnumSource(CompressionType.class)
        @DisplayName("Test writeBytesTo method will correctly write bytes to the target OutputStream")
        void testWriteBytesToOutputStream(final CompressionType compressionType) throws IOException {
            // create block file path before call
            final Path blockFilePath = testBasePath.resolve("0.blk".concat(compressionType.extension()));
            // create instance to test
            final BlockFileBlockAccessor toTest = buildAndCreateBlockAndGetAssociatedAccessor(blockFilePath, compressionType, 1);
            // test accessor.writeBytesTo(OutputStream)
            final Format format =
                    switch (compressionType) {
                        case ZSTD -> Format.ZSTD_PROTOBUF;
                        case NONE -> Format.PROTOBUF;
                    };
            final Bytes expectedFileBytes = Bytes.wrap(Files.readAllBytes(blockFilePath));
            final String expected = expectedFileBytes.toHex();
            final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            toTest.writeBytesTo(format, byteArrayOutputStream);
            final Bytes actual = Bytes.wrap(byteArrayOutputStream.toByteArray());
            assertThat(actual.toHex()).isEqualTo(expected);
        }

        /**
         * This test aims to verify that the {@link BlockFileBlockAccessor#writeBytesTo(Format, OutputStream)}
         * will correctly manage IOExceptions.
         */
        @ParameterizedTest
        @EnumSource(CompressionType.class)
        @DisplayName("Test writeBytesTo method will correctly handle IOException flows")
        void testWriteBytesToOutputStreamIOException(final CompressionType compressionType) throws IOException {
            // create block file path before call
            final Path blockFilePath = testBasePath.resolve("0.blk".concat(compressionType.extension()));
            // create instance to test
            final BlockFileBlockAccessor toTest = buildAndCreateBlockAndGetAssociatedAccessor(blockFilePath, compressionType, 1);
            // test accessor.writeBytesTo(OutputStream)
            final Format format =
                    switch (compressionType) {
                        case ZSTD -> Format.ZSTD_PROTOBUF;
                        case NONE -> Format.PROTOBUF;
                    };

            final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

            // delete the file to simulate NoSuchFileException IOException
            Files.delete(blockFilePath);

            // test accessor.writeBytesTo() expecting IOException
            try {
                toTest.writeBytesTo(format, byteArrayOutputStream);
                assertThat(false).isTrue(); // exception should have been thrown
            } catch (Throwable e) {
                // expected
                Assertions.assertThat(e).isInstanceOf(UncheckedIOException.class);
            }
        }

        /**
         * This test aims to verify that the {@link BlockFileBlockAccessor#writeBytesTo(Format, OutputStream)} will correctly
         * handle an IOException when the format is protobuf zstd but the compression is none.
         */
        @Test
        @DisplayName("Test writeBytesTo method will correctly handle IOException flows on taret OutputStream ZSTF protobuf but no compression")
        void testWriteBytesToOutputStreamZSTDIOException() throws IOException {
            // create block file path before call
            final CompressionType compressionType = CompressionType.NONE;
            final Path blockFilePath = testBasePath.resolve("0.blk".concat(compressionType.extension()));
            // create instance to test
            final BlockFileBlockAccessor toTest = buildAndCreateBlockAndGetAssociatedAccessor(blockFilePath, compressionType, 1);

            final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

            // delete the file to simulate NoSuchFileException IOException
            Files.delete(blockFilePath);

            // test accessor.writeBytesTo() expecting IOException
            try {
                toTest.writeBytesTo(Format.ZSTD_PROTOBUF, byteArrayOutputStream);
                assertThat(false).isTrue(); // exception should have been thrown
            } catch (Throwable e) {
                // expected
                Assertions.assertThat(e).isInstanceOf(UncheckedIOException.class);
            }
        }

        /**
         * This test aims to verify that the
         * {@link BlockFileBlockAccessor#writeBytesTo(Format, com.hedera.pbj.runtime.io.WritableSequentialData)}
         * will correctly write bytes to the target {@link com.hedera.pbj.runtime.io.WritableSequentialData}.
         */
        @ParameterizedTest
        @EnumSource(CompressionType.class)
        @DisplayName("Test writeBytesTo method will correctly write bytes to the target WritableSequentialData")
        void testWriteBytesToWritableSequentialData(final CompressionType compressionType) throws IOException {
            // create block file path before call
            final Path blockFilePath = testBasePath.resolve("0.blk".concat(compressionType.extension()));
            // create instance to test
            final BlockFileBlockAccessor toTest = buildAndCreateBlockAndGetAssociatedAccessor(blockFilePath, compressionType, 1);
            // test accessor.writeBytesTo(BufferedData)
            final Format format =
                    switch (compressionType) {
                        case ZSTD -> Format.ZSTD_PROTOBUF;
                        case NONE -> Format.PROTOBUF;
                    };
            final Bytes expectedFileBytes = Bytes.wrap(Files.readAllBytes(blockFilePath));
            final String expected = expectedFileBytes.toHex();
            final BufferedData bufferedData = BufferedData.allocate((int) expectedFileBytes.length());
            toTest.writeBytesTo(format, bufferedData);
            final Bytes actual = bufferedData.getBytes(0, bufferedData.length());
            assertThat(actual.toHex()).isEqualTo(expected);
        }

        /**
         * This test aims to verify that the {@link BlockFileBlockAccessor#writeBytesTo(Format, com.hedera.pbj.runtime.io.WritableSequentialData)}
         * will correctly manage IOExceptions.
         */
        @ParameterizedTest
        @EnumSource(CompressionType.class)
        @DisplayName("Test writeBytesTo method will correctly handle IOException in the WritableSequentialData paths")
        void testWriteBytesToWritableSequentialDataIOException(final CompressionType compressionType) throws IOException {
            // create block file path before call
            final Path blockFilePath = testBasePath.resolve("0.blk".concat(compressionType.extension()));
            // create instance to test
            final BlockFileBlockAccessor toTest = buildAndCreateBlockAndGetAssociatedAccessor(blockFilePath, compressionType, 1);
            // test accessor.writeBytesTo(BufferedData)
            final Format format =
                    switch (compressionType) {
                        case ZSTD -> Format.ZSTD_PROTOBUF;
                        case NONE -> Format.PROTOBUF;
                    };
            final Bytes expectedFileBytes = Bytes.wrap(Files.readAllBytes(blockFilePath));
            final BufferedData bufferedData = BufferedData.allocate((int) expectedFileBytes.length());

            // delete the file to simulate NoSuchFileException IOException
            Files.delete(blockFilePath);

            // test accessor.blockBytes() expecting IOException
            try {
                toTest.writeBytesTo(format, bufferedData);
                assertThat(false).isTrue(); // exception should have been thrown
            } catch (Throwable e) {
                // expected
                Assertions.assertThat(e).isInstanceOf(UncheckedIOException.class);
            }
        }

        /**
         * This test aims to verify that the {@link BlockFileBlockAccessor#writeBytesTo(Format, com.hedera.pbj.runtime.io.WritableSequentialData)}
         * will correctly handle an IOException when the format is protobuf zstd but the compression is none.
         */
        @Test
        @DisplayName("Test writeBytesTo method will correctly handle IOException in the WritableSequentialData paths on ZSTF protobuf but no compression")
        void testWriteBytesToWritableSequentialDataZSTDIOException() throws IOException {
            // create block file path before call
            final CompressionType compressionType = CompressionType.NONE;
            final Path blockFilePath = testBasePath.resolve("0.blk".concat(compressionType.extension()));
            // create instance to test
            final BlockFileBlockAccessor toTest = buildAndCreateBlockAndGetAssociatedAccessor(blockFilePath, compressionType, 1);
            // test accessor.writeBytesTo(BufferedData)
            final Bytes expectedFileBytes = Bytes.wrap(Files.readAllBytes(blockFilePath));
            final BufferedData bufferedData = BufferedData.allocate((int) expectedFileBytes.length());

            // delete the file to simulate NoSuchFileException IOException
            Files.delete(blockFilePath);

            // test accessor.blockBytes() expecting IOException
            try {
                toTest.writeBytesTo(Format.ZSTD_PROTOBUF, bufferedData);
                assertThat(false).isTrue(); // exception should have been thrown
            } catch (Throwable e) {
                // expected
                Assertions.assertThat(e).isInstanceOf(UncheckedIOException.class);
            }
        }

        /**
         * This test aims to verify that the
         * {@link BlockFileBlockAccessor#writeTo(Format, Path)}
         * will correctly write bytes to the target {@link Path}.
         */
        @ParameterizedTest
        @EnumSource(CompressionType.class)
        @DisplayName("Test writeTo method will correctly write bytes to the target Path")
        void testWriteToPath(final CompressionType compressionType) throws IOException {
            // create block file path before call
            final Path blockFilePath = testBasePath.resolve("0.blk".concat(compressionType.extension()));
            // create instance to test
            final BlockFileBlockAccessor toTest = buildAndCreateBlockAndGetAssociatedAccessor(blockFilePath, compressionType, 1);
            // test accessor.writeTo(Path)
            final Format format =
                    switch (compressionType) {
                        case ZSTD -> Format.ZSTD_PROTOBUF;
                        case NONE -> Format.PROTOBUF;
                    };
            final Bytes expectedFileBytes = Bytes.wrap(Files.readAllBytes(blockFilePath));
            final String expected = expectedFileBytes.toHex();
            final Path otherBlockFilePath = testBasePath.resolve("1.blk");
            toTest.writeTo(format, otherBlockFilePath);
            final String actual =
                    Bytes.wrap(Files.readAllBytes(otherBlockFilePath)).toHex();
            assertThat(actual).isEqualTo(expected);
        }

        /**
         * This test aims to verify that the {@link BlockFileBlockAccessor#delete()}will correctly delete
         * an existing persisted block.
         */
        @ParameterizedTest
        @EnumSource(CompressionType.class)
        @DisplayName("Test delete method will correctly delete a persisted block")
        void testBlockDelete(final CompressionType compressionType) throws IOException {
            // create block file path before call
            final Path blockFilePath = testBasePath.resolve("0.blk".concat(compressionType.extension()));
            // create instance to test
            final BlockFileBlockAccessor toTest = buildAndCreateBlockAndGetAssociatedAccessor(blockFilePath, compressionType, 1);
            // test accessor.delete()
            toTest.delete();
            assertThat(blockFilePath).doesNotExist();
        }

        @ParameterizedTest
        @EnumSource(CompressionType.class)
        void testDeleteNestedEmptyParentDirectory(final CompressionType compressionType) throws IOException {
            // create parents dirs
            final Path depth1Dir = Files.createDirectories(Path.of(testBasePath + "/depth1"));
            final Path depth2Dir = Files.createDirectories(Path.of(testBasePath + "/depth1/depth2"));
            final Path depth3Dir = Files.createDirectories(Path.of(testBasePath + "/depth1/depth2/depth3"));

            // create 1st block file at depth 2, leaving depth 1 empty
            final Path blockFile1Path = depth2Dir.resolve("7.blk".concat(compressionType.extension()));
            // create instance to test
            final BlockFileBlockAccessor toTest1 = buildAndCreateBlockAndGetAssociatedAccessor(blockFile1Path, compressionType, 2);

            // create 2nd block file at depth 3
            final Path blockFile2Path = depth3Dir.resolve("8.blk".concat(compressionType.extension()));
            // create instance to test
            final BlockFileBlockAccessor toTest2 = buildAndCreateBlockAndGetAssociatedAccessor(blockFile2Path, compressionType, 3);

            // test accessor2.delete() expecting depth 1 and 2 contents to remain but 3 to be removed
            toTest2.delete();
            assertThat(blockFile2Path).doesNotExist();
            assertThat(depth3Dir).doesNotExist();
            assertThat(depth2Dir).exists();
            assertThat(blockFile1Path).exists();
            assertThat(depth1Dir).exists();

            // test accessor1.delete() expecting depth 1 and 2 contents to be removed
            toTest1.delete();
            assertThat(depth2Dir).doesNotExist();
            assertThat(depth1Dir).doesNotExist();
            assertThat(blockFile1Path).doesNotExist();
        }

        private BlockFileBlockAccessor createBlockAndGetAssociatedAccessor(
                final Path blockFilePath, final CompressionType compressionType, Bytes protoBytes)
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
            return new BlockFileBlockAccessor(testBasePath, blockFilePath, compressionType);
        }

        private BlockFileBlockAccessor buildAndCreateBlockAndGetAssociatedAccessor(
                final Path blockFilePath, final CompressionType compressionType, final int numberOfBlocks)
                throws IOException {
            final BlockItemUnparsed[] blockItems1 = SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsed(numberOfBlocks);
            final BlockUnparsed expected1 = new BlockUnparsed(List.of(blockItems1));
            final Bytes protoBytes = BlockUnparsed.PROTOBUF.toBytes(expected1);

            return createBlockAndGetAssociatedAccessor(blockFilePath, compressionType, protoBytes);
        }
    }
}
