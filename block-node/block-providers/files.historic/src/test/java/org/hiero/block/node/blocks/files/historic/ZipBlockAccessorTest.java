// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.blocks.files.historic;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import com.github.luben.zstd.Zstd;
import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import com.hedera.hapi.block.stream.Block;
import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.config.api.ConfigurationBuilder;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;
import org.hiero.block.node.app.fixtures.blocks.SimpleTestBlockItemBuilder;
import org.hiero.block.node.base.CompressionType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
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

    @TempDir
    private Path tempDir;

    /** Set up the test environment before each test. */
    @BeforeEach
    void setup() {
        // Initialize the in-memory file system
        jimfs = Jimfs.newFileSystem(
                Configuration.unix()); // Set the default configuration for the test, use jimfs for paths
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
         * This test aims to assert that the
         * {@link ZipBlockAccessor#delete()} method throws an
         * {@link UnsupportedOperationException} when called.
         */
        @Test
        @DisplayName("Test delete throws UnsupportedOperationException")
        void testZipBlockAccessorFunctionality() {
            // construct a valid ZipBlockAccessor
            final BlockPath blockPath = BlockPath.computeBlockPath(defaultConfig, 1L);
            final ZipBlockAccessor actual = new ZipBlockAccessor(blockPath);
            assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(actual::delete);
        }

        /**
         * This test aims to verify that the {@link ZipBlockAccessor#block()}
         * will correctly return a zipped block.
         */
        @ParameterizedTest
        @EnumSource(CompressionType.class)
        @DisplayName("Test block method returns correctly a persisted block")
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
            // todo for now will not work with no compression as the production logic needs to support that
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
                bytesToWrite = Zstd.compress(compressedBytes);
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
        try (final ZipFile zipFile = new ZipFile(blockPath.zipFilePath().toFile())) {
            assertThat(zipFile.size()).isEqualTo(1);
            final ZipEntry entry = zipFile.getEntry(blockPath.blockFileName());
            assertThat(entry).isNotNull();
            final byte[] fromZipEntry = zipFile.getInputStream(entry).readAllBytes();
            assertThat(fromZipEntry).isEqualTo(bytesToWrite);
        }
        return new ZipBlockAccessor(blockPath);
    }

    private FilesHistoricConfig createTestConfiguration(final Path basePath, final CompressionType compressionType) {
        final FilesHistoricConfig localDefaultConfig = getDefaultConfiguration();
        return new FilesHistoricConfig(basePath, compressionType, localDefaultConfig.powersOfTenPerZipFileContents());
    }

    private FilesHistoricConfig getDefaultConfiguration() {
        return ConfigurationBuilder.create()
                .withConfigDataType(FilesHistoricConfig.class)
                .build()
                .getConfigData(FilesHistoricConfig.class);
    }
}
