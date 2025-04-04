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
import com.hedera.pbj.runtime.io.buffer.BufferedData;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
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
            final BlockFileBlockAccessor toTest = createInstanceToTest(blockFilePath, compressionType, protoBytes);
            // test accessor.block()
            final Block actual = toTest.block();
            assertThat(actual).isEqualTo(expected);
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
            final BlockFileBlockAccessor toTest = createInstanceToTest(blockFilePath, compressionType, protoBytes);
            // test accessor.blockUnparsed()
            final BlockUnparsed actual = toTest.blockUnparsed();
            assertThat(actual).isEqualTo(expected);
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
            // build a test block
            final BlockItemUnparsed[] blockItems = SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsed(1);
            final BlockUnparsed block = new BlockUnparsed(List.of(blockItems));
            final Bytes protoBytes = BlockUnparsed.PROTOBUF.toBytes(block);
            // create instance to test
            final BlockFileBlockAccessor toTest = createInstanceToTest(blockFilePath, compressionType, protoBytes);
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
         * This test aims to verify that the {@link BlockFileBlockAccessor#writeBytesTo(Format, OutputStream)}
         * will correctly write bytes to the target {@link OutputStream}.
         */
        @ParameterizedTest
        @EnumSource(CompressionType.class)
        @DisplayName("Test writeBytesTo method will correctly write bytes to the target OutputStream")
        void testWriteBytesToOutputStream(final CompressionType compressionType) throws IOException {
            // create block file path before call
            final Path blockFilePath = testBasePath.resolve("0.blk".concat(compressionType.extension()));
            // build a test block
            final BlockItemUnparsed[] blockItems = SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsed(1);
            final BlockUnparsed block = new BlockUnparsed(List.of(blockItems));
            final Bytes protoBytes = BlockUnparsed.PROTOBUF.toBytes(block);
            // create instance to test
            final BlockFileBlockAccessor toTest = createInstanceToTest(blockFilePath, compressionType, protoBytes);
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
            // build a test block
            final BlockItemUnparsed[] blockItems = SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsed(1);
            final BlockUnparsed block = new BlockUnparsed(List.of(blockItems));
            final Bytes protoBytes = BlockUnparsed.PROTOBUF.toBytes(block);
            // create instance to test
            final BlockFileBlockAccessor toTest = createInstanceToTest(blockFilePath, compressionType, protoBytes);
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
            // build a test block
            final BlockItemUnparsed[] blockItems = SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsed(1);
            final BlockUnparsed block = new BlockUnparsed(List.of(blockItems));
            final Bytes protoBytes = BlockUnparsed.PROTOBUF.toBytes(block);
            // create instance to test
            final BlockFileBlockAccessor toTest = createInstanceToTest(blockFilePath, compressionType, protoBytes);
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
            // build a test block
            final BlockItemUnparsed[] blockItems = SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsed(1);
            final BlockUnparsed expected = new BlockUnparsed(List.of(blockItems));
            final Bytes protoBytes = BlockUnparsed.PROTOBUF.toBytes(expected);
            // create instance to test
            final BlockFileBlockAccessor toTest = createInstanceToTest(blockFilePath, compressionType, protoBytes);
            // test accessor.delete()
            toTest.delete();
            assertThat(blockFilePath).doesNotExist();
        }

        private BlockFileBlockAccessor createInstanceToTest(
                final Path blockFilePath, final CompressionType compressionType, final Bytes protoBytes)
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
    }
}
