// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.blocks.files.historic;

import static java.lang.System.Logger.Level.INFO;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

/**
 * The ZipBlockAccessor class provides access to a block stored in a zip file.
 */
final class ZipBlockAccessor extends AbstractZipBlockAccessor {
    private static final String FAILED_TO_DELETE_LINK_MESSAGE =
            "Failed to delete accessor link for block: %s, zipFilePath: %s, entryName: %s";
    /** Message logged when the provided path to a zip file is not a regular file or does not exist. */
    private static final String INVALID_ZIP_FILE_PATH_MESSAGE =
            "Provided path to zip file is not a regular file or does not exist: %s";
    /** The absolute path to the zip file, used for logging. */
    private final Path absoluteZipFilePath;
    /** Path to the temporary hardlink for the zip file behind this accessor. */
    private final Path zipFileLink;

    /**
     * Constructs a ZipBlockAccessor with the specified block path.
     *
     * @param blockPath the block path
     */
    ZipBlockAccessor(@NonNull final BlockPath blockPath, @NonNull final Path linksRootPath) throws IOException {
        super(blockPath, blockPath.zipFilePath().toAbsolutePath());
        final Path zipFilePath = blockPath.zipFilePath();
        absoluteZipFilePath = zipFilePath.toAbsolutePath();
        if (!Files.isRegularFile(zipFilePath)) {
            final String msg = INVALID_ZIP_FILE_PATH_MESSAGE.formatted(zipFilePath);
            throw new IOException(msg);
        }
        final Path linkBase = linksRootPath.resolve(blockPath.zipFilePath());
        zipFileLink = createTempLink(linkBase);
    }

    /** Bound on retry attempts in {@link #createTempLink}; see its javadoc for why this can stay small. */
    private static final int MAX_LINK_ATTEMPTS = 10;

    /**
     * Creates a hard link at a name derived from {@code linkBase}, appending a random suffix on every attempt
     * rather than an incrementing counter, and letting {@link Files#createLink} itself be the single source of
     * truth for "is this name taken" (no separate exists-check, which would race two accessors linking the same
     * block concurrently). A previous version used a plain incrementing counter for the suffix, which meant
     * concurrent accessors racing for the same block name could keep landing on the same next candidate and
     * colliding again on retry. A random suffix instead makes any single attempt collide with another
     * concurrent accessor only by extremely unlucky chance, so {@link #MAX_LINK_ATTEMPTS} can stay small.
     */
    @NonNull
    private Path createTempLink(final Path linkBase) throws IOException {
        for (int attempt = 0; attempt < MAX_LINK_ATTEMPTS; attempt++) {
            final Path candidateLink = linkBase.getParent().resolve(linkBase.getFileName() + "." + UUID.randomUUID());
            try {
                return Files.createLink(candidateLink, absoluteZipFilePath);
            } catch (final FileAlreadyExistsException ignored) {
                // Vanishingly unlikely with a random suffix; try again with a fresh name.
            }
        }
        final String message = "Unable to create link after %d attempts for %s";
        throw new IOException(message.formatted(MAX_LINK_ATTEMPTS, linkBase));
    }

    @Override
    protected Bytes readEntry(@NonNull final Format format) throws IOException {
        try (final FileSystem zipFileSystem = FileSystems.newFileSystem(zipFileLink)) {
            final Path entry = zipFileSystem.getPath(blockPathData.blockFileName());
            return getBytesFromPath(format, entry, blockPathData.compressionType());
        }
    }

    @Override
    public void close() {
        try {
            Files.delete(zipFileLink);
        } catch (final RuntimeException | IOException e) {
            final String message = FAILED_TO_DELETE_LINK_MESSAGE.formatted(
                    blockNumber(), absoluteZipFilePath, blockPathData.blockFileName());
            LOGGER.log(INFO, message, e);
        }
    }

    @Override
    public boolean isClosed() {
        return !Files.exists(zipFileLink);
    }
}
