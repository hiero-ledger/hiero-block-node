// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.server.persistence.storage.compression;

import static com.hedera.block.server.util.PersistTestUtils.PERSISTENCE_STORAGE_COMPRESSION_LEVEL;
import static org.assertj.core.api.Assertions.assertThat;

import com.github.luben.zstd.ZstdOutputStream;
import com.hedera.block.server.persistence.storage.PersistenceStorageConfig;
import com.hedera.block.server.persistence.storage.PersistenceStorageConfig.CompressionType;
import com.hedera.block.server.util.TestConfigUtil;
import com.swirlds.config.api.Configuration;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.stream.Stream;
import org.hiero.block.common.utils.FileUtilities;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests for the {@link ZstdCompression} class.
 */
@SuppressWarnings("FieldCanBeLocal")
class ZstdCompressionTest {
    @TempDir
    private Path testTempDir;

    private PersistenceStorageConfig persistenceStorageConfig;
    private ZstdCompression toTest;

    @BeforeEach
    void setUp() throws IOException {
        Map<String, String> configMap = Map.of(PERSISTENCE_STORAGE_COMPRESSION_LEVEL, String.valueOf(6));
        Configuration config = TestConfigUtil.getTestBlockNodeConfiguration(configMap);
        persistenceStorageConfig = config.getConfigData(PersistenceStorageConfig.class);
        toTest = ZstdCompression.of(persistenceStorageConfig);
    }

    /**
     * This test aims to verify that the
     * {@link ZstdCompression#getCompressionFileExtension()} method returns the
     * Zstandard (Zstd) compression file extension ".zstd".
     */
    @Test
    void testGetCompressionFileExtension() {
        assertThat(toTest.getCompressionFileExtension())
                .isNotNull()
                .isNotBlank()
                .isEqualTo(".zstd");
    }

    /**
     * This test aims to verify that the
     * {@link NoOpCompression#wrap(OutputStream)} correctly wraps a valid
     * provided {@link OutputStream} and utilizes the Zstandard compression
     * algorithm when writing the data to it`s destination.
     *
     * @param testData parameterized, test data
     * @throws IOException if an I/O exception occurs
     */
    @ParameterizedTest
    @MethodSource("testData")
    void testSuccessfulCompression(final String testData) throws IOException {
        final Path actual = testTempDir.resolve(FileUtilities.appendExtension(
                Path.of("successfulCompression.txt"), toTest.getCompressionFileExtension()));
        Files.createFile(actual);

        // assert that the raw target file exists
        assertThat(actual).exists();

        final byte[] byteArrayTestData = testData.getBytes(StandardCharsets.UTF_8);
        try (final OutputStream out = toTest.wrap(Files.newOutputStream(actual))) {
            out.write(byteArrayTestData);
        }
        assertThat(actual)
                .exists()
                .isReadable()
                .isRegularFile()
                .isNotEmptyFile()
                .hasSameBinaryContentAs(actualZstdCompression(byteArrayTestData));
    }

    /**
     * This test aims to verify that the
     * {@link Compression#wrap(InputStream, PersistenceStorageConfig.CompressionType)}
     * correctly wraps a valid provided {@link InputStream} and reads the test
     * data from it`s destination but decompresses it using the Zstandard
     * compression algorithm.
     *
     * @param testData parameterized, test data
     * @throws IOException if an I/O exception occurs
     */
    @ParameterizedTest
    @MethodSource("testData")
    void testSuccessfulDecompression(final String testData) throws IOException {
        final Path toRead = testTempDir.resolve("successfulDecompression.txt");
        Files.createFile(toRead);

        final byte[] expected = testData.getBytes(StandardCharsets.UTF_8);
        try (final OutputStream out = new ZstdOutputStream(Files.newOutputStream(toRead))) {
            out.write(expected);
        }

        // assert that the target file exists and is a regular, non-empty file
        assertThat(toRead).exists().isReadable().isRegularFile();

        // read data
        final byte[] actual;
        try (final InputStream in = toTest.wrap(Files.newInputStream(toRead), CompressionType.ZSTD)) {
            actual = in.readAllBytes();
        }
        assertThat(actual).isNotNull().isEqualTo(expected);
    }

    private Path actualZstdCompression(final byte[] byteArrayTestData) throws IOException {
        final Path tempFile = testTempDir.resolve(
                FileUtilities.appendExtension(Path.of("tempComparisonFile.txt"), toTest.getCompressionFileExtension()));
        try (final ZstdOutputStream out =
                new ZstdOutputStream(Files.newOutputStream(tempFile), persistenceStorageConfig.compressionLevel())) {
            out.write(byteArrayTestData);
            return tempFile;
        }
    }

    private static Stream<Arguments> testData() {
        return Stream.of(
                Arguments.of(""),
                Arguments.of(" "),
                Arguments.of("\t "),
                Arguments.of("Some Random test data"),
                Arguments.of("Other Random test data"),
                Arguments.of("11110000"),
                Arguments.of(" a "),
                Arguments.of("\t a "),
                Arguments.of("\n a "));
    }
}
