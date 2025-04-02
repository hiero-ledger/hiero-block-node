// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.blocks.files.recent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.hiero.block.node.app.fixtures.blocks.BlockUtils.toBlockUnparsed;
import static org.junit.jupiter.api.Assertions.assertEquals;

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
import org.hiero.hapi.block.node.BlockUnparsed;
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
    /** Test Base Path, resolved under jimfs. */
    private Path testBasePath;

    /**
     * Environment setup for the test class.
     */
    @SuppressWarnings("resource")
    @BeforeEach
    void setup() throws IOException {
        // Initialize the in-memory file system
        final FileSystem jimfs = Jimfs.newFileSystem(Configuration.unix());
        testBasePath = jimfs.getPath("/tmp");
        Files.createDirectories(testBasePath);
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
    @DisplayName("Functionality tests")
    final class FunctionalityTests {
        @ParameterizedTest
        @EnumSource(CompressionType.class)
        //        @DisplayName("Test that the block file path is created correctly") todo
        void test(final CompressionType compressionType) throws IOException {
            // create & assert existing block file path before call
            final Path blockFilePath = testBasePath.resolve("1.blk".concat(compressionType.extension()));
            Files.createFile(blockFilePath);
            assertThat(blockFilePath)
                    .exists()
                    .isRegularFile()
                    .isEmptyFile()
                    .isReadable()
                    .isWritable();
            final BlockItem[] blockItems = SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocks(1);
            final Block targetBlock = new Block(List.of(blockItems));
            final Bytes expectedProtobufBytes = Block.PROTOBUF.toBytes(targetBlock);
            // it is important the output stream is closed as the compression writes a footer on close
            try (final OutputStream out = compressionType.wrapStream(Files.newOutputStream(blockFilePath))) {
                expectedProtobufBytes.writeTo(out);
            }
            assertThat(blockFilePath).isNotEmptyFile();
            // test accessor.block()
            final BlockFileBlockAccessor blockAccessor =
                    new BlockFileBlockAccessor(testBasePath, blockFilePath, compressionType);
            final Block actualBlock = blockAccessor.block();
            assertEquals(targetBlock, actualBlock);
            // test accessor.blockUnparsed()
            final BlockUnparsed actualBlockUnparsed = blockAccessor.blockUnparsed();
            final BlockUnparsed expectedBlockUnparsed = toBlockUnparsed(actualBlock);
            assertEquals(expectedBlockUnparsed, actualBlockUnparsed);
            final var format =
                    switch (compressionType) {
                        case ZSTD -> Format.ZSTD_PROTOBUF;
                        case NONE -> Format.PROTOBUF;
                    };
            final Bytes expectedFileBytes = Bytes.wrap(Files.readAllBytes(blockFilePath));
            final String expectedFileHex = expectedFileBytes.toHex();
            // test accessor.blockBytes()
            final Bytes actualBytes = blockAccessor.blockBytes(format);
            assertEquals(expectedFileHex, actualBytes.toHex());
            // test accessor.writeBytesTo(OutputStream)
            final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            blockAccessor.writeBytesTo(format, byteArrayOutputStream);
            final Bytes actualOutputStreamBytes = Bytes.wrap(byteArrayOutputStream.toByteArray());
            assertEquals(expectedFileHex, actualOutputStreamBytes.toHex());
            // test accessor.writeBytesTo(BufferedData)
            final BufferedData bufferedData = BufferedData.allocate((int) expectedFileBytes.length());
            blockAccessor.writeBytesTo(format, bufferedData);
            final Bytes bufferedDataBytes = bufferedData.getBytes(0, bufferedData.length());
            assertEquals(expectedFileHex, bufferedDataBytes.toHex());
            // test accessor.writeTo(Path)
            final Path blockFilePath2 = testBasePath.resolve("2.blk");
            blockAccessor.writeTo(format, blockFilePath2);
            final String file2BytesHex =
                    Bytes.wrap(Files.readAllBytes(blockFilePath2)).toHex();
            assertEquals(expectedFileHex, file2BytesHex);
        }
    }
}
