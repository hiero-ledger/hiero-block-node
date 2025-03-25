// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.common.utils;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.lang.System.Logger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Objects;
import java.util.Set;
import java.util.zip.GZIPInputStream;

/** A utility class that deals with logic related to dealing with files. */
public final class FileUtilities {
    private static final Logger LOGGER = System.getLogger(FileUtilities.class.getName());

    /**
     * The default file permissions for new files.
     * <p>
     * Default permissions are set to: rw-r--r--
     */
    private static final FileAttribute<Set<PosixFilePermission>> DEFAULT_FILE_PERMISSIONS =
            PosixFilePermissions.asFileAttribute(Set.of(
                    PosixFilePermission.OWNER_READ,
                    PosixFilePermission.OWNER_WRITE,
                    PosixFilePermission.GROUP_READ,
                    PosixFilePermission.OTHERS_READ));

    /**
     * Default folder permissions for new folders.
     * <p>
     * Default permissions are set to: rwxr-xr-x
     */
    public static final FileAttribute<Set<PosixFilePermission>> DEFAULT_FOLDER_PERMISSIONS =
            PosixFilePermissions.asFileAttribute(Set.of(
                    PosixFilePermission.OWNER_READ,
                    PosixFilePermission.OWNER_WRITE,
                    PosixFilePermission.OWNER_EXECUTE,
                    PosixFilePermission.GROUP_READ,
                    PosixFilePermission.GROUP_EXECUTE,
                    PosixFilePermission.OTHERS_READ,
                    PosixFilePermission.OTHERS_EXECUTE));

    /**
     * Create a new path (folder) if it does not exist.
     * Any folders created will use default permissions.
     *
     * @param toCreate valid, non-null instance of {@link Path} to be created
     * @param logLevel valid, non-null instance of {@link System.Logger.Level} to use
     * @param semanticPathName valid, non-blank {@link String} used for logging that represents the
     *     desired path semantically
     * @throws IOException if the path cannot be created
     */
    public static void createFolderPathIfNotExists(
            @NonNull final Path toCreate,
            @NonNull final System.Logger.Level logLevel,
            @NonNull final String semanticPathName)
            throws IOException {
        createFolderPathIfNotExists(toCreate, logLevel, DEFAULT_FOLDER_PERMISSIONS, semanticPathName);
    }

    /**
     * Create a new path (folder) if it does not exist.
     *
     * @param toCreate The path to be created.
     * @param logLevel The logging level to use when logging this event.
     * @param permissions Permissions to use when creating the path.
     * @param semanticPathName A name (non-blank) to represent the path in a logging
     *     statement.
     * @throws IOException if the path cannot be created due to a filesystem
     *     error.
     */
    public static void createFolderPathIfNotExists(
            @NonNull final Path toCreate,
            @NonNull final System.Logger.Level logLevel,
            @NonNull final FileAttribute<Set<PosixFilePermission>> permissions,
            @NonNull final String semanticPathName)
            throws IOException {
        Objects.requireNonNull(toCreate);
        Objects.requireNonNull(logLevel);
        Objects.requireNonNull(permissions);
        Preconditions.requireNotBlank(semanticPathName);
        if (Files.notExists(toCreate)) {
            Files.createDirectories(toCreate, permissions);
            final String logMessage = "Created [%s] at '%s'".formatted(semanticPathName, toCreate);
            LOGGER.log(logLevel, logMessage);
        } else {
            final String logMessage = "Requested [%s] not created because the directory already exists at '%s'"
                    .formatted(semanticPathName, toCreate);
            LOGGER.log(logLevel, logMessage);
        }
    }

    /**
     * Read a GZIP file and return the content as a byte array.
     * <p>
     * This method is _unsafe_ because it reads the entire file content into
     * a single byte array, which can cause memory issues, and may fail if the
     * file contains a large amount of data.
     *
     * @param filePath Path to the GZIP file.
     * @return byte array containing the _uncompressed_ content of the GZIP file.
     * @throws IOException if unable to read the file.
     * @throws OutOfMemoryError if a byte array large enough to contain the
     *     file contents cannot be allocated (either because it exceeds MAX_INT
     *     bytes or exceeds available heap memory).
     */
    public static byte[] readGzipFileUnsafe(@NonNull final Path filePath) throws IOException {
        Objects.requireNonNull(filePath);
        try (final var gzipInputStream = new GZIPInputStream(Files.newInputStream(filePath))) {
            return gzipInputStream.readAllBytes();
        }
    }

    /**
     * Read a file and return the content as a byte array.
     * <p>
     * This method uses default extensions for gzip and block files.
     * <p>
     * This method is _unsafe_ because it reads the entire file content into
     * a single byte array, which can cause memory issues, and may fail if the
     * file contains a large amount of data.
     *
     * @param filePath Path to the file
     * @return byte array of the content of the file or null if the file extension is not
     *     supported
     * @throws IOException if unable to read the file.
     * @throws OutOfMemoryError if a byte array large enough to contain the
     *     file contents cannot be allocated (either because it exceeds MAX_INT
     *     bytes or exceeds available heap memory).
     */
    public static byte[] readFileBytesUnsafe(@NonNull final Path filePath) throws IOException {
        return readFileBytesUnsafe(filePath, ".blk", ".gz");
    }

    /**
     * Read a file and return the content as a byte array.
     * <p>
     * This method is _unsafe_ because it reads the entire file content into
     * a single byte array, which can cause memory issues, and may fail if the
     * file contains a large amount of data.
     *
     * @param filePath Path to the file to read.
     * @param blockFileExtension A file extension for block files.
     * @param gzipFileExtension A file extension for gzip files.
     * @return A byte array with the full contents of the file, or null if the
     *     file extension requested does not match at least one of the
     *     extensions provided (GZip or Block).
     * @throws IOException if unable to read the file.
     * @throws OutOfMemoryError if a byte array large enough to contain the
     *     file contents cannot be allocated (either because it exceeds MAX_INT
     *     bytes or exceeds available heap memory).
     */
    public static byte[] readFileBytesUnsafe(
            @NonNull final Path filePath,
            @NonNull final String blockFileExtension,
            @NonNull final String gzipFileExtension)
            throws IOException {
        final String filePathAsString = Objects.requireNonNull(filePath).toString();
        Objects.requireNonNull(blockFileExtension);
        Objects.requireNonNull(gzipFileExtension);
        if (filePathAsString.endsWith(gzipFileExtension)) {
            return readGzipFileUnsafe(filePath);
        } else if (filePathAsString.endsWith(blockFileExtension)) {
            return Files.readAllBytes(filePath);
        } else {
            return null;
        }
    }

    /**
     * This method creates a new file at the given path. The method ensures that
     * the full path to the target file will be created, including all missing
     * intermediary directories.
     *
     * @param pathToCreate the path to create
     * @throws IOException if the file cannot be created or if it already exists
     */
    public static void createFile(@NonNull final Path pathToCreate) throws IOException {
        Files.createDirectories(pathToCreate.getParent());
        Files.createFile(pathToCreate);
    }

    /**
     * This method appends an extension to a given path.
     *
     * @param path to append the extension to
     * @param extension to append to the path
     * @return a new path with the extension appended
     */
    @NonNull
    public static Path appendExtension(@NonNull final Path path, @NonNull final String extension) {
        return path.resolveSibling(path.getFileName() + Objects.requireNonNull(extension));
    }

    private FileUtilities() {}
}
