// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.blocks.files.historic;

import org.junit.jupiter.api.DisplayName;

/**
 * Test class for {@link ZipBlockAccessor}.
 */
@DisplayName("ZipBlockAccessor Tests")
class ZipBlockAccessorTest {
    // todo create tests

    //    /** The testing in-memory file system. */
    //    private FileSystem jimfs;
    //    /** Test Base Path, resolved under jimfs. */
    //    private Path testBasePath;
    //
    //    /**
    //     * Environment setup for the test class.
    //     */
    //    @BeforeEach
    //    void setup() throws IOException {
    //        // Initialize the in-memory file system
    //        jimfs = Jimfs.newFileSystem(Configuration.unix());
    //        testBasePath = jimfs.getPath("/tmp");
    //        Files.createDirectories(testBasePath);
    //    }
    //
    //    /**
    //     * Tear down the test environment after each test.
    //     */
    //    @AfterEach
    //    void tearDown() throws IOException {
    //        // Close the Jimfs file system
    //        if (jimfs != null) {
    //            jimfs.close();
    //        }
    //    }
    //
    //    /**
    //     * Tests for the {@link ZipBlockAccessor} constructor.
    //     */
    //    @Nested
    //    @DisplayName("Constructor Tests")
    //    final class ConstructorTests {
    //        /**
    //         * This test asserts that no exception is thrown when the input parameters are valid.
    //         */
    //        @Test
    //        void testValidConstructor() {
    //            final BlockPath blockPath = new BlockPath(testBasePath.resolve("test.zip"), "block.blk");
    //            assertThatNoException().isThrownBy(() -> new ZipBlockAccessor(blockPath));
    //        }
    //
    //        /**
    //         * This test asserts that a {@link NullPointerException} is thrown when the block path is null.
    //         */
    //        @Test
    //        void testNullBlockPath() {
    //            assertThatExceptionOfType(NullPointerException.class)
    //                    .isThrownBy(() -> new ZipBlockAccessor(null));
    //        }
    //    }
    //
    //    /**
    //     * Tests for the {@link ZipBlockAccessor} functionality.
    //     */
    //    @Nested
    //    @DisplayName("Functionality Tests")
    //    final class FunctionalityTests {
    //        /**
    //         * This test verifies that the {@link ZipBlockAccessor#block()} correctly returns a persisted block.
    //         */
    //        @Test
    //        void testBlock() throws IOException {
    //            final Block expectedBlock = createTestBlock();
    //            final BlockPath blockPath = createZipWithBlock(expectedBlock);
    //
    //            final ZipBlockAccessor accessor = new ZipBlockAccessor(blockPath);
    //            final Block actualBlock = accessor.block();
    //
    //            assertThat(actualBlock).isEqualTo(expectedBlock);
    //        }
    //
    //        /**
    //         * This test verifies that the {@link ZipBlockAccessor#block()} correctly handles parse exceptions.
    //         */
    //        @Test
    //        void testBlockParseException() throws IOException {
    //            final BlockPath blockPath = createZipWithInvalidBlock();
    //
    //            final ZipBlockAccessor accessor = new ZipBlockAccessor(blockPath);
    //
    //            assertThatExceptionOfType(UncheckedParseException.class).isThrownBy(accessor::block);
    //        }
    //
    //        /**
    //         * This test verifies that the {@link ZipBlockAccessor#blockBytes(Format)} correctly returns block bytes.
    //         */
    //        @Test
    //        void testBlockBytes() throws IOException {
    //            final Block expectedBlock = createTestBlock();
    //            final BlockPath blockPath = createZipWithBlock(expectedBlock);
    //
    //            final ZipBlockAccessor accessor = new ZipBlockAccessor(blockPath);
    //            final Bytes actualBytes = accessor.blockBytes(Format.PROTOBUF);
    //
    //            assertThat(actualBytes).isEqualTo(Block.PROTOBUF.toBytes(expectedBlock));
    //        }
    //
    //        /**
    //         * This test verifies that the {@link ZipBlockAccessor#blockBytes(Format)} correctly handles IO
    // exceptions.
    //         */
    //        @Test
    //        void testBlockBytesIOException() {
    //            final BlockPath blockPath = new BlockPath(testBasePath.resolve("nonexistent.zip"), "block.blk");
    //
    //            final ZipBlockAccessor accessor = new ZipBlockAccessor(blockPath);
    //
    //            assertThatExceptionOfType(IOException.class).isThrownBy(() -> accessor.blockBytes(Format.PROTOBUF));
    //        }
    //
    //        private Block createTestBlock() {
    //            return new Block(List.of());
    //        }
    //
    //        private BlockPath createZipWithBlock(Block block) throws IOException {
    //            final Path zipPath = testBasePath.resolve("test.zip");
    //            final String blockFileName = "block.blk";
    //
    //            try (ZipOutputStream zipOut = new ZipOutputStream(Files.newOutputStream(zipPath))) {
    //                zipOut.putNextEntry(new ZipEntry(blockFileName));
    //                Block.PROTOBUF.toBytes(block).writeTo(zipOut);
    //                zipOut.closeEntry();
    //            }
    //
    //            return new BlockPath(zipPath, blockFileName);
    //        }
    //
    //        private BlockPath createZipWithInvalidBlock() throws IOException {
    //            final Path zipPath = testBasePath.resolve("test.zip");
    //            final String blockFileName = "block.blk";
    //
    //            try (ZipOutputStream zipOut = new ZipOutputStream(Files.newOutputStream(zipPath))) {
    //                zipOut.putNextEntry(new ZipEntry(blockFileName));
    //                zipOut.write(new byte[48]); // Invalid block data
    //                zipOut.closeEntry();
    //            }
    //
    //            return new BlockPath(zipPath, blockFileName);
    //        }
    //    }
}
