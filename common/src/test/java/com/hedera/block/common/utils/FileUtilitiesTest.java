// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.common.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIOException;

import java.io.File;
import java.io.IOException;
import java.lang.System.Logger.Level;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests for {@link FileUtilities} functionality.
 */
class FileUtilitiesTest {
    @TempDir
    private Path tempDir;

    /**
     * This test aims to verify that a folder path will be created for a given
     * path if it does not exist. First we assert that the path we want to make
     * does not exist, then we run the actual method and assert that the path
     * was created and is an empty folder.
     *
     * @param tempDir junit temp dir
     * @throws IOException propagated from {@link FileUtilities#createFolderPathIfNotExists(Path, Level, String)}
     */
    @Test
    void testCreateFolderPathIfNotExists(@TempDir final Path tempDir) throws IOException {
        final String newDir = "newDir";
        final Path toCreate = tempDir.resolve(newDir);

        // ensure the temp directory is empty in the beginning
        assertThat(tempDir).isEmptyDirectory();
        assertThat(toCreate).doesNotExist();

        // run actual
        FileUtilities.createFolderPathIfNotExists(toCreate, Level.ERROR, "test dir 1");

        // assert
        assertThat(toCreate).exists().isDirectory();
        assertThat(tempDir.toFile().listFiles()).hasSize(1).contains(toCreate.toFile());
    }

    /**
     * This test aims to verify that a folder path will not be created for a
     * given path if it already exists. First we assert that the path we want to
     * make already exists, then we run the actual method and assert that the
     * path was unchanged and is an empty folder and nothing else was created.
     *
     * @param tempDir junit temp dir
     * @throws IOException propagated from {@link FileUtilities#createFolderPathIfNotExists(Path, Level, String)}
     */
    @Test
    void testSkipFolderCreationIfPathExists(@TempDir final Path tempDir) throws IOException {
        final String newDir = "newDir";
        final Path toCreate = tempDir.resolve(newDir);

        // ensure the temp directory is empty in the beginning
        assertThat(tempDir).isEmptyDirectory();
        assertThat(toCreate).doesNotExist();

        // create 'newDir'
        Files.createDirectory(toCreate);

        // ensure the temp directory contains only 'newDir' before running actual
        final File tempDirAsFile = tempDir.toFile();
        final File toCreateAsFile = toCreate.toFile();
        assertThat(tempDirAsFile.listFiles()).hasSize(1).contains(toCreateAsFile);
        assertThat(toCreate).exists().isDirectory();

        // run actual
        FileUtilities.createFolderPathIfNotExists(toCreate, Level.ERROR, "test dir 1");

        // assert
        assertThat(toCreate).exists().isDirectory();
        assertThat(tempDirAsFile.listFiles()).hasSize(1).contains(toCreateAsFile);
    }

    /**
     * This test aims to verify that reading a gzip file that exists and is
     * valid, will return a byte array with the expected content.
     *
     * @param filePath parameterized, to read gzip files from
     * @param expectedContent parameterized, expected content after reading the file
     * @throws IOException propagated from {@link FileUtilities#readGzipFileUnsafe(Path)}
     */
    @ParameterizedTest
    @MethodSource("validGzipFiles")
    void testReadGzipFileUnsafe(final Path filePath, final String expectedContent) throws IOException {
        final byte[] actualContent = FileUtilities.readGzipFileUnsafe(filePath);
        assertThat(actualContent)
                .isNotNull()
                .isNotEmpty()
                .asString()
                .isNotNull()
                .isNotBlank()
                .isEqualTo(expectedContent);
    }

    /**
     * This test aims to verify that reading an invalid gzip file throws an
     * {@link IOException}.
     *
     * @param filePath parameterized, to read gzip files from
     */
    @ParameterizedTest
    @MethodSource("invalidFiles")
    void testReadGzipFileUnsafeThrows(final Path filePath) {
        assertThatIOException().isThrownBy(() -> FileUtilities.readGzipFileUnsafe(filePath));
    }

    /**
     * This test aims to verify that reading a file that exists and is valid and
     * is found by the extension parameter, will return a byte array with the
     * expected content.
     *
     * @param filePath parameterized, to read files from
     * @param expectedContent parameterized, expected content after reading the file
     * @throws IOException propagated from {@link FileUtilities#readFileBytesUnsafe(Path)}
     * and {@link FileUtilities#readFileBytesUnsafe(Path, String, String)}
     */
    @ParameterizedTest
    @MethodSource({"validGzipFiles", "validBlkFiles"})
    void testReadFileBytesUnsafe(final Path filePath, final String expectedContent) throws IOException {
        final Consumer<byte[]> asserts = actual -> {
            assertThat(actual)
                    .isNotNull()
                    .isNotEmpty()
                    .asString()
                    .isNotNull()
                    .isNotBlank()
                    .isEqualTo(expectedContent);
        };

        final byte[] actualContent = FileUtilities.readFileBytesUnsafe(filePath, ".blk", ".gz");
        assertThat(actualContent).satisfies(asserts);

        // overloaded has same extensions as above
        final byte[] actualContentOverloaded = FileUtilities.readFileBytesUnsafe(filePath);
        assertThat(actualContentOverloaded).satisfies(asserts);
    }

    /**
     * This test aims to verify that reading a file that is not recognized by
     * the block file extension we provide, will return null.
     *
     * @throws IOException propagated from {@link FileUtilities#readFileBytesUnsafe(Path)}
     * and {@link FileUtilities#readFileBytesUnsafe(Path, String, String)}
     */
    @Test
    void testReadFileBytesUnsafeReturnsNull() throws IOException {
        final Path path = Path.of("build/resources/test//nonexistent.unrecognized");

        final byte[] actualContent = FileUtilities.readFileBytesUnsafe(path, ".blk", ".gz");
        assertThat(actualContent).isNull();

        final byte[] actualContentOverloaded = FileUtilities.readFileBytesUnsafe(path);
        assertThat(actualContentOverloaded).isNull();
    }

    /**
     * This test aims to verify that reading an invalid file, be that it is
     * in some way corrupted or nonexistent, will throw an {@link IOException}.
     *
     * @param filePath parameterized, to read block files from
     */
    @ParameterizedTest
    @MethodSource("invalidFiles")
    void testReadFileBytesUnsafeThrows(final Path filePath) {
        assertThatIOException().isThrownBy(() -> FileUtilities.readFileBytesUnsafe(filePath));
        assertThatIOException().isThrownBy(() -> FileUtilities.readFileBytesUnsafe(filePath, ".blk", ".gz"));
    }

    /**
     * This test aims to verify that {@link FileUtilities#createFile(Path)}
     * correctly creates a file at the given path alongside with any missing
     * intermediary directories.
     *
     * @param toCreate parameterized, target to create
     * @param tempDir junit temp dir
     */
    @ParameterizedTest
    @MethodSource("filesToCreate")
    void testCreateFile(final Path toCreate, @TempDir final Path tempDir) throws IOException {
        final Path expected = tempDir.resolve(toCreate);
        assertThat(expected).doesNotExist();
        FileUtilities.createFile(expected);
        assertThat(expected).exists().isRegularFile();
    }

    /**
     * This test aims to verify that the
     * {@link FileUtilities#appendExtension(Path, String)} method correctly
     * appends the given extension to the given path.
     *
     * @param filePath parameterized, to append extensions to
     * @param extension parameterized, extension to append
     */
    @ParameterizedTest
    @MethodSource("filesWithExtensions")
    void testAppendFileExtension(final Path filePath, final String extension) {
        final Path pathToTest = tempDir.resolve(filePath);
        final Path actual = FileUtilities.appendExtension(pathToTest, extension);
        assertThat(actual).hasFileName(filePath + extension);
    }

    private static Stream<Arguments> validGzipFiles() {
        return Stream.of(
                Arguments.of("build/resources/test//valid1.txt.gz", "valid1"),
                Arguments.of("build/resources/test//valid2.txt.gz", "valid2"));
    }

    private static Stream<Arguments> validBlkFiles() {
        return Stream.of(
                Arguments.of("build/resources/test//valid1.blk", "valid1blk"),
                Arguments.of("build/resources/test//valid2.blk", "valid2blk"));
    }

    private static Stream<Arguments> invalidFiles() {
        return Stream.of(
                Arguments.of("build/resources/test//invalid1.gz"),
                Arguments.of("build/resources/test//nonexistent.gz"),
                Arguments.of("build/resources/test//nonexistent.blk"));
    }

    private static Stream<Arguments> filesToCreate() {
        return Stream.of(Arguments.of("temp1.txt"), Arguments.of("some_folder/temp2.txt"));
    }

    private static Stream<Arguments> filesWithExtensions() {
        return Stream.of(
                Arguments.of("valid1", ".blk"),
                Arguments.of("valid1", ""),
                Arguments.of("valid2", ".gz"),
                Arguments.of("valid2", ".zstd"));
    }
}
