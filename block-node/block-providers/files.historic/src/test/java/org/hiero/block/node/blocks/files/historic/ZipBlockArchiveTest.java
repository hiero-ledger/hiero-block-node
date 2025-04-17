// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.blocks.files.historic;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import com.google.common.jimfs.Jimfs;
import com.swirlds.config.api.Configuration;
import com.swirlds.config.api.ConfigurationBuilder;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;
import org.hiero.block.node.app.fixtures.plugintest.SimpleInMemoryHistoricalBlockFacility;
import org.hiero.block.node.app.fixtures.plugintest.TestHealthFacility;
import org.hiero.block.node.spi.BlockNodeContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Test class for {@link ZipBlockArchive}.
 */
@DisplayName("ZipBlockArchive Tests")
class ZipBlockArchiveTest {
    /** The in-memory {@link FileSystem} used for testing. */
    private FileSystem jimfs;
    /** The {@link SimpleInMemoryHistoricalBlockFacility} used for testing. */
    private SimpleInMemoryHistoricalBlockFacility historicalBlockProvider;
    /** The {@link BlockNodeContext} used for testing. */
    private BlockNodeContext testContext;
    /** The default test {@link FilesHistoricConfig} used for testing. */
    private FilesHistoricConfig defaultTestConfig;
    /** Temp dir used for testing as the File abstraction is not supported by jimfs */
    @TempDir
    private Path tempDir;

    /**
     * Environment setup before each test.
     */
    @BeforeEach
    void setup() {
        jimfs = Jimfs.newFileSystem(com.google.common.jimfs.Configuration.unix());
        final Configuration configuration = ConfigurationBuilder.create()
                .withConfigDataType(FilesHistoricConfig.class)
                .build();
        // we need a custom test config, because if we resolve it using the
        // config dependency, we cannot resolve the path using the jimfs
        // once we are able to override existing config converters, we will no
        // longer need this createTestConfiguration and the production logic
        // can also be simplified and to always get the configuration via the
        // block context
        defaultTestConfig = createTestConfiguration(jimfs.getPath("/opt/hashgraph/blocknode/data/historic"), 1);
        historicalBlockProvider = new SimpleInMemoryHistoricalBlockFacility();
        testContext = new BlockNodeContext(
                configuration, null, new TestHealthFacility(), null, historicalBlockProvider, null);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (jimfs != null) {
            jimfs.close();
        }
    }

    /**
     * Constructor tests for {@link ZipBlockArchive}.
     */
    @Nested
    @DisplayName("Constructor Tests")
    final class ConstructorTests {
        /**
         * This test aims to assert that the constructor of
         * {@link ZipBlockArchive} does not throw an exception when the input
         * argument is valid.
         */
        @Test
        @DisplayName("Test constructor with valid input")
        void testConstructorValidInput() {
            assertThatNoException().isThrownBy(() -> new ZipBlockArchive(testContext, defaultTestConfig));
        }

        /**
         * This test aims to assert that the constructor of
         * {@link ZipBlockArchive} throws a {@link NullPointerException} when
         * the input argument is null.
         */
        @Test
        @DisplayName("Test constructor with null input")
        @SuppressWarnings("all")
        void testConstructorNullInput() {
            assertThatNullPointerException().isThrownBy(() -> new ZipBlockArchive(null, defaultTestConfig));
        }
    }

    /**
     * Functional tests for {@link ZipBlockArchive}.
     */
    @Nested
    @DisplayName("Functional Tests")
    final class FunctionalTests {
        /**
         * This test aims to assert that the
         * {@link ZipBlockArchive#minStoredBlockNumber()} returns -1L if the
         * archive is empty.
         */
        @Test
        @DisplayName("Test minStoredBlockNumber() returns -1L when zip file is not present")
        void testMinStoredNoZipFile() throws IOException {
            //
            final Path testRootPath = tempDir.resolve("testRootPath");
            Files.createDirectories(testRootPath);
            final ZipBlockArchive toTest = new ZipBlockArchive(testContext, createTestConfiguration(testRootPath, 1));
            // assert that server is running before we call actual
            assertThat(testContext.serverHealth().isRunning()).isTrue();
            final long actual = toTest.minStoredBlockNumber();
            // assert that server is still running and -1 is returned
            assertThat(actual).isEqualTo(-1L);
            assertThat(testContext.serverHealth().isRunning()).isTrue();
        }

        /**
         * This test aims to assert that the
         * {@link ZipBlockArchive#minStoredBlockNumber()} calls shutdown on the
         * health facility if an exception occurs.
         */
        @Test
        @DisplayName("Test minStoredBlockNumber() shuts down server if exception occurs")
        void testMinStoredEmptyZipFile() throws IOException {
            // create test config, use the temp dir as we will work with the File abstraction
            final Path testRootPath = tempDir.resolve("testRootPath");
            Files.createDirectories(testRootPath);
            final FilesHistoricConfig testConfiguration = createTestConfiguration(testRootPath, 1);
            // create test environment, in this case we simply create an empty zip file which will produce
            // an exception when we attempt to look for an entry inside
            final BlockPath computedBlockPath00s = BlockPath.computeBlockPath(testConfiguration, 0L);
            Files.createDirectories(computedBlockPath00s.dirPath());
            Files.createFile(computedBlockPath00s.zipFilePath());
            // create test instance
            final ZipBlockArchive toTest = new ZipBlockArchive(testContext, testConfiguration);
            // assert that server is running before we call actual
            assertThat(testContext.serverHealth().isRunning()).isTrue();
            // call
            final long actual = toTest.minStoredBlockNumber();
            // assert no longer running server
            assertThat(actual).isEqualTo(-1L);
            assertThat(testContext.serverHealth().isRunning()).isFalse();
        }

        /**
         * This test aims to assert that the
         * {@link ZipBlockArchive#maxStoredBlockNumber()} returns -1L if the
         * archive is empty.
         */
        @Test
        @DisplayName("Test maxStoredBlockNumber() returns -1L when zip file is not present")
        void testMaxStoredNoZipFile() throws IOException {
            final Path testRootPath = tempDir.resolve("testRootPath");
            Files.createDirectories(testRootPath);
            final ZipBlockArchive toTest = new ZipBlockArchive(testContext, createTestConfiguration(testRootPath, 1));
            final long actual = toTest.maxStoredBlockNumber();
            assertThat(actual).isEqualTo(-1L);
        }
    }

    private ZipBlockAccessor createAndAddBlockEntry(final BlockPath blockPath, final byte[] bytesToWrite)
            throws IOException {
        // create & assert existing block file path before call
        Files.createDirectories(blockPath.dirPath());
        // it is important the output stream is closed as the compression writes a footer on close
        if (Files.notExists(blockPath.dirPath())) {
            Files.createFile(blockPath.zipFilePath());
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
            final ZipEntry entry = zipFile.getEntry(blockPath.blockFileName());
            assertThat(entry).isNotNull();
            final byte[] fromZipEntry = zipFile.getInputStream(entry).readAllBytes();
            assertThat(fromZipEntry).isEqualTo(bytesToWrite);
        }
        return new ZipBlockAccessor(blockPath);
    }

    private FilesHistoricConfig createTestConfiguration(final Path basePath, final int powersOfTenPerZipFileContents) {
        final FilesHistoricConfig localDefaultConfig = getDefaultConfiguration();
        return new FilesHistoricConfig(basePath, localDefaultConfig.compression(), powersOfTenPerZipFileContents);
    }

    private FilesHistoricConfig getDefaultConfiguration() {
        return ConfigurationBuilder.create()
                .withConfigDataType(FilesHistoricConfig.class)
                .build()
                .getConfigData(FilesHistoricConfig.class);
    }
}
