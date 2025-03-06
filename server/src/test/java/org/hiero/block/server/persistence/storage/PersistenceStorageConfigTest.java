// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.persistence.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.from;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.stream.Stream;
import org.hiero.block.server.persistence.storage.PersistenceStorageConfig.CompressionType;
import org.hiero.block.server.persistence.storage.PersistenceStorageConfig.StorageType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Test class that tests the functionality of the
 * {@link PersistenceStorageConfig}
 */
class PersistenceStorageConfigTest {
    private static final Path HASHGRAPH_ROOT_ABSOLUTE_PATH =
            Path.of("/opt/hashgraph/").toAbsolutePath();
    private static final Path PERSISTENCE_STORAGE_ROOT_ABSOLUTE_PATH =
            HASHGRAPH_ROOT_ABSOLUTE_PATH.resolve("blocknode/data/");
    // Default compression level (as set in the config annotation)
    private static final int DEFAULT_COMPRESSION_LEVEL = 3;
    // NoOp compression level boundaries
    private static final int LOWER_BOUNDARY_FOR_NO_OP_COMPRESSION = Integer.MIN_VALUE;
    private static final int DEFAULT_VALUE_FOR_NO_OP_COMPRESSION = DEFAULT_COMPRESSION_LEVEL;
    private static final int UPPER_BOUNDARY_FOR_NO_OP_COMPRESSION = Integer.MAX_VALUE;
    // Zstd compression level boundaries
    private static final int LOWER_BOUNDARY_FOR_ZSTD_COMPRESSION = 0;
    private static final int DEFAULT_VALUE_FOR_ZSTD_COMPRESSION = DEFAULT_COMPRESSION_LEVEL;
    private static final int UPPER_BOUNDARY_FOR_ZSTD_COMPRESSION = 20;
    // Archiving defaults
    private static final int DEFAULT_ARCHIVE_BATCH_SIZE = 1000;

    @AfterEach
    void tearDown() {
        if (!Files.exists(HASHGRAPH_ROOT_ABSOLUTE_PATH)) {
            return;
        }
        try (final Stream<Path> walk = Files.walk(HASHGRAPH_ROOT_ABSOLUTE_PATH)) {
            walk.sorted(Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.delete(p);
                } catch (final IOException e) {
                    throw new RuntimeException(e);
                }
            });
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * This test aims to verify that the {@link PersistenceStorageConfig} class
     * correctly returns the storage type that was set in the constructor.
     *
     * @param storageType parameterized, the storage type to test
     */
    @ParameterizedTest
    @EnumSource(StorageType.class)
    void testPersistenceStorageConfigStorageTypes(final StorageType storageType) {
        final PersistenceStorageConfig actual = new PersistenceStorageConfig(
                Path.of(""),
                Path.of(""),
                storageType,
                CompressionType.NONE,
                DEFAULT_COMPRESSION_LEVEL,
                DEFAULT_ARCHIVE_BATCH_SIZE);
        assertThat(actual).returns(storageType, from(PersistenceStorageConfig::type));
    }

    /**
     * This test aims to verify that the {@link PersistenceStorageConfig} class
     * correctly sets the live and archive root paths.
     *
     * @param liveRootPathToTest parameterized, the live root path to test
     * @param expectedLiveRootPathToTest parameterized, the expected live root
     * @param archiveRootPathToTest parameterized, the archive root path to test
     * @param expectedArchiveRootPathToTest parameterized, the expected archive
     * root
     */
    @ParameterizedTest
    @MethodSource({"validAbsoluteDefaultRootPaths", "validAbsoluteNonDefaultRootPaths"})
    void testPersistenceStorageConfigHappyPaths(
            final Path liveRootPathToTest,
            final Path expectedLiveRootPathToTest,
            final Path archiveRootPathToTest,
            final Path expectedArchiveRootPathToTest) {
        final PersistenceStorageConfig actual = new PersistenceStorageConfig(
                liveRootPathToTest,
                archiveRootPathToTest,
                StorageType.BLOCK_AS_LOCAL_FILE,
                CompressionType.NONE,
                DEFAULT_COMPRESSION_LEVEL,
                DEFAULT_ARCHIVE_BATCH_SIZE);
        assertThat(actual)
                .returns(expectedLiveRootPathToTest, from(PersistenceStorageConfig::liveRootPath))
                .returns(expectedArchiveRootPathToTest, from(PersistenceStorageConfig::archiveRootPath));
    }

    /**
     * This test aims to verify that the {@link PersistenceStorageConfig} class
     * correctly returns the compression level that was set in the constructor.
     *
     * @param compressionLevel parameterized, the compression level to test
     */
    @ParameterizedTest
    @MethodSource("validCompressionLevels")
    void testPersistenceStorageConfigValidCompressionLevel(
            final CompressionType compressionType, final int compressionLevel) {
        final PersistenceStorageConfig actual = new PersistenceStorageConfig(
                Path.of(""),
                Path.of(""),
                StorageType.BLOCK_AS_LOCAL_FILE,
                compressionType,
                compressionLevel,
                DEFAULT_ARCHIVE_BATCH_SIZE);
        assertThat(actual).returns(compressionLevel, from(PersistenceStorageConfig::compressionLevel));
    }

    /**
     * This test aims to verify that the {@link PersistenceStorageConfig} class
     * correctly throws an {@link IllegalArgumentException} when the compression
     * level is invalid.
     *
     * @param compressionLevel parameterized, the compression level to test
     */
    @ParameterizedTest
    @MethodSource("invalidCompressionLevels")
    void testPersistenceStorageConfigInvalidCompressionLevel(
            final CompressionType compressionType, final int compressionLevel) {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> new PersistenceStorageConfig(
                        Path.of(""),
                        Path.of(""),
                        StorageType.BLOCK_AS_LOCAL_FILE,
                        compressionType,
                        compressionLevel,
                        DEFAULT_ARCHIVE_BATCH_SIZE));
    }

    /**
     * This test aims to verify that the {@link PersistenceStorageConfig} class
     * correctly returns the compression type that was set in the constructor.
     *
     * @param compressionType parameterized, the compression type to test
     */
    @ParameterizedTest
    @EnumSource(CompressionType.class)
    void testPersistenceStorageConfigCompressionTypes(final CompressionType compressionType) {
        final PersistenceStorageConfig actual = new PersistenceStorageConfig(
                Path.of(""),
                Path.of(""),
                StorageType.NO_OP,
                compressionType,
                DEFAULT_COMPRESSION_LEVEL,
                DEFAULT_ARCHIVE_BATCH_SIZE);
        assertThat(actual).returns(compressionType, from(PersistenceStorageConfig::compression));
    }

    /**
     * This test aims to verify that the {@link PersistenceStorageConfig} class
     * correctly returns the archive batch size that was set in the constructor.
     *
     * @param archiveGroupSize parameterized, the archive batch size to test
     */
    @ParameterizedTest
    @MethodSource("validArchiveGroupSizes")
    void testPersistenceStorageConfigValidArchiveGroupSizes(final int archiveGroupSize) {
        final PersistenceStorageConfig actual = new PersistenceStorageConfig(
                Path.of(""),
                Path.of(""),
                StorageType.NO_OP,
                CompressionType.NONE,
                DEFAULT_COMPRESSION_LEVEL,
                archiveGroupSize);
        assertThat(actual).returns(archiveGroupSize, from(PersistenceStorageConfig::archiveGroupSize));
    }

    /**
     * This test aims to verify that the {@link PersistenceStorageConfig} class
     * correctly throws an {@link IllegalArgumentException} when the archive batch
     * size is invalid.
     *
     * @param archiveGroupSize parameterized, the archive batch size to test
     */
    @ParameterizedTest
    @MethodSource("invalidArchiveGroupSizes")
    void testPersistenceStorageConfigInvalidArchiveGroupSizes(final int archiveGroupSize) {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> new PersistenceStorageConfig(
                        Path.of(""),
                        Path.of(""),
                        StorageType.NO_OP,
                        CompressionType.NONE,
                        DEFAULT_COMPRESSION_LEVEL,
                        archiveGroupSize));
    }

    /**
     * The default absolute paths. We expect these to allow the persistence
     * config to be instantiated. Providing a blank string is accepted, it will
     * create the config instance with it's internal defaults.
     */
    @SuppressWarnings("all")
    private static Stream<Arguments> validAbsoluteDefaultRootPaths() {
        final Path defaultLiveRootAbsolutePath =
                PERSISTENCE_STORAGE_ROOT_ABSOLUTE_PATH.resolve("live/").toAbsolutePath();
        final Path defaultArchiveRootAbsolutePath =
                PERSISTENCE_STORAGE_ROOT_ABSOLUTE_PATH.resolve("archive/").toAbsolutePath();

        // test against the default liveRootPath and archiveRootPath
        final String liveToTest1 = defaultLiveRootAbsolutePath.toString();
        final String liveExpected1 = defaultLiveRootAbsolutePath.toString();
        final String archiveToTest1 = defaultArchiveRootAbsolutePath.toString();
        final String archiveExpected1 = defaultArchiveRootAbsolutePath.toString();

        // blank liveRootPath results in the default liveRootPath to be used
        final String liveToTest2 = "";
        final String liveExpected2 = liveToTest2;
        final String archiveToTest2 = defaultArchiveRootAbsolutePath.toString();
        final String archiveExpected2 = defaultArchiveRootAbsolutePath.toString();

        // blank archiveRootPath results in the default archiveRootPath to be used
        final String liveToTest3 = defaultLiveRootAbsolutePath.toString();
        final String liveExpected3 = defaultLiveRootAbsolutePath.toString();
        final String archiveToTest3 = "";
        final String archiveExpected3 = archiveToTest3;

        // blank liveRootPath and archiveRootPath results in the default liveRootPath and archiveRootPath to be used
        final String liveToTest6 = "";
        final String liveExpected6 = liveToTest6;
        final String archiveToTest6 = "";
        final String archiveExpected6 = archiveToTest6;

        return Stream.of(
                Arguments.of(liveToTest1, liveExpected1, archiveToTest1, archiveExpected1),
                Arguments.of(liveToTest2, liveExpected2, archiveToTest2, archiveExpected2),
                Arguments.of(liveToTest3, liveExpected3, archiveToTest3, archiveExpected3),
                Arguments.of(liveToTest6, liveExpected6, archiveToTest6, archiveExpected6));
    }

    /**
     * Somve valid absolute paths that are not the default paths. We expect
     * these to allow the persistence config to be instantiated.
     */
    @SuppressWarnings("all")
    private static Stream<Arguments> validAbsoluteNonDefaultRootPaths() {
        final String liveToTest1 = PERSISTENCE_STORAGE_ROOT_ABSOLUTE_PATH
                .resolve("nondefault/live/")
                .toString();
        final String archiveToTest1 = PERSISTENCE_STORAGE_ROOT_ABSOLUTE_PATH
                .resolve("nondefault/archive/")
                .toString();

        final String liveToTest2 = PERSISTENCE_STORAGE_ROOT_ABSOLUTE_PATH
                .resolve("another/nondefault/live/")
                .toString();
        final String archiveToTest2 = PERSISTENCE_STORAGE_ROOT_ABSOLUTE_PATH
                .resolve("another/nondefault/archive/")
                .toString();

        return Stream.of(
                Arguments.of(liveToTest1, liveToTest1, archiveToTest1, archiveToTest1),
                Arguments.of(liveToTest2, liveToTest2, archiveToTest2, archiveToTest2));
    }

    private static Stream<Arguments> validCompressionLevels() {
        return Stream.of(
                Arguments.of(
                        CompressionType.NONE,
                        LOWER_BOUNDARY_FOR_NO_OP_COMPRESSION), // lower boundary for NO_OP compression
                Arguments.of(
                        CompressionType.NONE,
                        DEFAULT_VALUE_FOR_NO_OP_COMPRESSION), // default value for NO_OP compression
                Arguments.of(
                        CompressionType.NONE,
                        UPPER_BOUNDARY_FOR_NO_OP_COMPRESSION), // upper boundary for NO_OP compression
                Arguments.of(
                        CompressionType.ZSTD,
                        LOWER_BOUNDARY_FOR_ZSTD_COMPRESSION), // lower boundary for ZSTD compression
                Arguments.of(
                        CompressionType.ZSTD, DEFAULT_VALUE_FOR_ZSTD_COMPRESSION), // default value for ZSTD compression
                Arguments.of(
                        CompressionType.ZSTD,
                        UPPER_BOUNDARY_FOR_ZSTD_COMPRESSION) // upper boundary for ZSTD compression
                );
    }

    private static Stream<Arguments> invalidCompressionLevels() {
        return Stream.of(
                Arguments.of(CompressionType.ZSTD, LOWER_BOUNDARY_FOR_ZSTD_COMPRESSION - 1),
                Arguments.of(CompressionType.ZSTD, UPPER_BOUNDARY_FOR_ZSTD_COMPRESSION + 1));
    }

    private static Stream<Arguments> validArchiveGroupSizes() {
        return Stream.of(
                Arguments.of(10),
                Arguments.of(100),
                Arguments.of(1_000),
                Arguments.of(10_000),
                Arguments.of(100_000),
                Arguments.of(1_000_000),
                Arguments.of(10_000_000),
                Arguments.of(100_000_000),
                Arguments.of(1_000_000_000));
    }

    private static Stream<Arguments> invalidArchiveGroupSizes() {
        return Stream.of(
                Arguments.of(1),
                Arguments.of(0),
                Arguments.of(-1),
                Arguments.of(-2),
                Arguments.of(-10),
                Arguments.of(-20),
                Arguments.of(-50),
                Arguments.of(-100),
                Arguments.of(-1_000),
                Arguments.of(-10_000),
                Arguments.of(-100_000),
                Arguments.of(-1_000_000),
                Arguments.of(-10_000_000),
                Arguments.of(-100_000_000),
                Arguments.of(-1_000_000_000));
    }
}
