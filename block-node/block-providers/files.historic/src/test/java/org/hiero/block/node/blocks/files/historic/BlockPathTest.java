// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.blocks.files.historic;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.from;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.util.stream.Stream;
import org.hiero.block.node.base.CompressionType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Test for {@link BlockPath}.
 */
class BlockPathTest {
    /** The testing in-memory file system. */
    private FileSystem jimfs;
    /** The configuration for the test. */
    private FilesHistoricConfig defaultConfig;

    /** Set up the test environment before each test. */
    @BeforeEach
    void setup() {
        // Initialize the in-memory file system
        jimfs = Jimfs.newFileSystem(Configuration.unix());
        defaultConfig = new FilesHistoricConfig(
                jimfs.getPath("/opt/hashgraph/blocknode/data/historic"), CompressionType.ZSTD, 3, 1, 4);
    }

    /** Tear down the test environment after each test. */
    @AfterEach
    void tearDown() throws IOException {
        // Close the Jimfs file system
        if (jimfs != null) {
            jimfs.close();
        }
    }

    // todo finish constructor tests

    //    @Nested
    //    @DisplayName("Constructor Tests")
    //    final class ConstructorTests {
    //        @Test
    //        @DisplayName("Test constructor with valid inputs")
    //        void testConstructor() {
    //            final BlockPath blockPath = new BlockPath(
    //                    Paths.get("/opt/hashgraph/blocknode/data/historic"),
    //                    Paths.get("/opt/hashgraph/blocknode/data/historic/0000000000123456789.zip"),
    //                    "0000000000123456789",
    //                    "0000000000123456789.blk.zstd");
    //
    //            assertEquals(Paths.get("/opt/hashgraph/blocknode/data/historic"), blockPath.dirPath());
    //            assertEquals(
    //                    Paths.get("/opt/hashgraph/blocknode/data/historic/0000000000123456789.zip"),
    //                    blockPath.zipFilePath());
    //            assertEquals("0000000000123456789", blockPath.blockNumStr());
    //            assertEquals("0000000000123456789.blk.zstd", blockPath.blockFileName());
    //        }
    //    }

    @Nested
    @DisplayName("Functionality Tests")
    final class FunctionalityTests {
        @ParameterizedTest
        @MethodSource("org.hiero.block.node.blocks.files.historic.BlockPathTest#argumentsForBlockPath")
        @DisplayName("Test computeBlockPath with valid inputs")
        void testComputeBlockPath(
                final long blockNumber,
                final String expectedBlockNumStr,
                final String expectedBlockFileName,
                final String zipFilePath) {
            final Path expectedZipFilePath = jimfs.getPath(zipFilePath);
            final Path expectedDirPath = expectedZipFilePath.getParent();
            final BlockPath actual = BlockPath.computeBlockPath(defaultConfig, blockNumber);
            assertThat(actual)
                    .isNotNull()
                    .returns(expectedBlockNumStr, from(BlockPath::blockNumStr))
                    .returns(expectedBlockFileName, from(BlockPath::blockFileName))
                    .returns(expectedZipFilePath, from(BlockPath::zipFilePath))
                    .returns(expectedDirPath, from(BlockPath::dirPath));
        }
    }

    private static Stream<Arguments> argumentsForBlockPath() {
        return Stream.of(
                Arguments.of(
                        123_456_789L,
                        "0000000000123456789",
                        "0000000000123456789.blk.zstd",
                        "/opt/hashgraph/blocknode/data/historic/000/000/000/012/345/6000s.zip"),
                Arguments.of(
                        1_234_567_890_123_456_789L,
                        "1234567890123456789",
                        "1234567890123456789.blk.zstd",
                        "/opt/hashgraph/blocknode/data/historic/123/456/789/012/345/6000s.zip"),
                Arguments.of(
                        0L,
                        "0000000000000000000",
                        "0000000000000000000.blk.zstd",
                        "/opt/hashgraph/blocknode/data/historic/000/000/000/000/000/0000s.zip"),
                Arguments.of(
                        10L,
                        "0000000000000000010",
                        "0000000000000000010.blk.zstd",
                        "/opt/hashgraph/blocknode/data/historic/000/000/000/000/000/0000s.zip"),
                Arguments.of(
                        100L,
                        "0000000000000000100",
                        "0000000000000000100.blk.zstd",
                        "/opt/hashgraph/blocknode/data/historic/000/000/000/000/000/0000s.zip"),
                Arguments.of(
                        1_000L,
                        "0000000000000001000",
                        "0000000000000001000.blk.zstd",
                        "/opt/hashgraph/blocknode/data/historic/000/000/000/000/000/1000s.zip"),
                Arguments.of(
                        10_000L,
                        "0000000000000010000",
                        "0000000000000010000.blk.zstd",
                        "/opt/hashgraph/blocknode/data/historic/000/000/000/000/001/0000s.zip"),
                Arguments.of(
                        100_000L,
                        "0000000000000100000",
                        "0000000000000100000.blk.zstd",
                        "/opt/hashgraph/blocknode/data/historic/000/000/000/000/010/0000s.zip"),
                Arguments.of(
                        1_000_000L,
                        "0000000000001000000",
                        "0000000000001000000.blk.zstd",
                        "/opt/hashgraph/blocknode/data/historic/000/000/000/000/100/0000s.zip"),
                Arguments.of(
                        10_000_000L,
                        "0000000000010000000",
                        "0000000000010000000.blk.zstd",
                        "/opt/hashgraph/blocknode/data/historic/000/000/000/001/000/0000s.zip"),
                Arguments.of(
                        100_000_000L,
                        "0000000000100000000",
                        "0000000000100000000.blk.zstd",
                        "/opt/hashgraph/blocknode/data/historic/000/000/000/010/000/0000s.zip"),
                Arguments.of(
                        1_000_000_000L,
                        "0000000001000000000",
                        "0000000001000000000.blk.zstd",
                        "/opt/hashgraph/blocknode/data/historic/000/000/000/100/000/0000s.zip"),
                Arguments.of(
                        10_000_000_000L,
                        "0000000010000000000",
                        "0000000010000000000.blk.zstd",
                        "/opt/hashgraph/blocknode/data/historic/000/000/001/000/000/0000s.zip"),
                Arguments.of(
                        100_000_000_000L,
                        "0000000100000000000",
                        "0000000100000000000.blk.zstd",
                        "/opt/hashgraph/blocknode/data/historic/000/000/010/000/000/0000s.zip"),
                Arguments.of(
                        1_000_000_000_000L,
                        "0000001000000000000",
                        "0000001000000000000.blk.zstd",
                        "/opt/hashgraph/blocknode/data/historic/000/000/100/000/000/0000s.zip"),
                Arguments.of(
                        10_000_000_000_000L,
                        "0000010000000000000",
                        "0000010000000000000.blk.zstd",
                        "/opt/hashgraph/blocknode/data/historic/000/001/000/000/000/0000s.zip"),
                Arguments.of(
                        100_000_000_000_000L,
                        "0000100000000000000",
                        "0000100000000000000.blk.zstd",
                        "/opt/hashgraph/blocknode/data/historic/000/010/000/000/000/0000s.zip"),
                Arguments.of(
                        1_000_000_000_000_000L,
                        "0001000000000000000",
                        "0001000000000000000.blk.zstd",
                        "/opt/hashgraph/blocknode/data/historic/000/100/000/000/000/0000s.zip"),
                Arguments.of(
                        10_000_000_000_000_000L,
                        "0010000000000000000",
                        "0010000000000000000.blk.zstd",
                        "/opt/hashgraph/blocknode/data/historic/001/000/000/000/000/0000s.zip"),
                Arguments.of(
                        100_000_000_000_000_000L,
                        "0100000000000000000",
                        "0100000000000000000.blk.zstd",
                        "/opt/hashgraph/blocknode/data/historic/010/000/000/000/000/0000s.zip"),
                Arguments.of(
                        Long.MAX_VALUE,
                        "9223372036854775807",
                        "9223372036854775807.blk.zstd",
                        "/opt/hashgraph/blocknode/data/historic/922/337/203/685/477/5000s.zip"));
    }
}
