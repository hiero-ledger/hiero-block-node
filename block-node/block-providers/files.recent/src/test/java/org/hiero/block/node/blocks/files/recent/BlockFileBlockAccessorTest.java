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
import org.hiero.block.node.app.fixtures.blocks.SimpleTestBlockItemBuilder;
import org.hiero.block.node.base.CompressionType;
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
            final Bytes bytes = Block.PROTOBUF.toBytes(targetBlock);
            final byte[] expectedBytes = bytes.toByteArray();
            final OutputStream out = compressionType.wrapStream(Files.newOutputStream(blockFilePath));
            bytes.writeTo(out);
            out.flush();
            assertThat(blockFilePath).isNotEmptyFile();
            final Block actualBlock = new BlockFileBlockAccessor(testBasePath, blockFilePath, compressionType).block();
            System.out.println();
            // todo this throws when ZSTD is used, maybe because of the buffered streams used underneath,
            //   need some more research before completing the test
        }
    }
}
