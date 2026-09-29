// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.blocks.files.historic;

import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.WARNING;
import static org.hiero.block.node.base.ParseHelper.standardParse;

import com.hedera.hapi.block.stream.Block;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.io.InputStream;
import java.lang.System.Logger;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.UUID;
import org.hiero.block.node.base.CompressionType;
import org.hiero.block.node.spi.historicalblocks.BlockAccessor;

/**
 * The ZipBlockAccessor class provides access to a block stored in a zip file.
 */
final class ZipBlockAccessor implements BlockAccessor {
    private static final String FAILED_TO_DELETE_LINK_MESSAGE =
            "Failed to delete accessor link for block: %s, zipFilePath: %s, entryName: %s";
    /** The logger for this class. */
    private final Logger LOGGER = System.getLogger(getClass().getName());
    /** Message logged when the protobuf codec fails to parse data */
    private static final String FAILED_TO_PARSE_MESSAGE =
            "Failed to parse block: %s, zipFilePath: %s, zipEntryName: %s";
    /** Message logged when data cannot be read from a block file */
    private static final String FAILED_TO_READ_MESSAGE = "Failed to read block: %s, zipFilePath: %s, zipEntryName: %s";
    /** Message logged when the provided path to a zip file is not a regular file or does not exist. */
    private static final String INVALID_ZIP_FILE_PATH_MESSAGE =
            "Provided path to zip file is not a regular file or does not exist: %s";
    /** The absolute path to the zip file, used for logging. */
    private final Path absoluteZipFilePath;
    /** All path and block information for the block accessed */
    private final BlockPath blockPathData;
    /** Block number this accessor manages. */
    private final long blockNumber;
    /** Path to the temporary hardlink for the zip file behind this accessor. */
    private final Path zipFileLink;

    /**
     * Constructs a ZipBlockAccessor with the specified block path.
     *
     * @param blockPath the block path
     */
    ZipBlockAccessor(@NonNull final BlockPath blockPath, @NonNull final Path linksRootPath) throws IOException {
        blockPathData = blockPath;
        blockNumber = blockPath.blockNumber();
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
    public long blockNumber() {
        return blockNumber;
    }

    @Override
    public Bytes blockBytes(@NonNull final Format format) {
        Objects.requireNonNull(format);
        String entryName = blockPathData.blockFileName();
        try (final FileSystem zipFileSystem = FileSystems.newFileSystem(zipFileLink)) {
            final Path entry = zipFileSystem.getPath(blockPathData.blockFileName());
            return getBytesFromPath(format, entry, blockPathData.compressionType());
        } catch (final RuntimeException | IOException e) {
            final String message = FAILED_TO_READ_MESSAGE.formatted(blockNumber, absoluteZipFilePath, entryName);
            LOGGER.log(WARNING, message, e);
            return null;
        }
    }

    /**
     * Get the bytes from the specified path, converting to the desired format if necessary.
     *
     * @param responseFormat the desired format of the data
     * @param sourcePath the path to the source file
     * @param sourceCompression the compression type of the source data
     * @return the bytes of the block in the desired format, or null if the block cannot be read
     * @throws IOException if unable to read or decompress the data.
     */
    private Bytes getBytesFromPath(
            final Format responseFormat, final Path sourcePath, final CompressionType sourceCompression)
            throws IOException {
        try (final InputStream in = Files.newInputStream(sourcePath);
                final InputStream wrapped = sourceCompression.wrapStream(in)) {
            Bytes sourceData =
                    switch (responseFormat) {
                        case JSON, PROTOBUF -> Bytes.wrap(wrapped.readAllBytes());
                        case ZSTD_PROTOBUF -> {
                            if (sourceCompression == CompressionType.ZSTD) {
                                yield Bytes.wrap(in.readAllBytes());
                            } else {
                                yield Bytes.wrap(CompressionType.ZSTD.compress(wrapped.readAllBytes()));
                            }
                        }
                    };
            if (Format.JSON == responseFormat) {
                return getJsonBytesFromProtobufBytes(sourceData);
            } else {
                return sourceData;
            }
        }
    }

    /**
     * Parse protobuf bytes to a `Block`, then generate JSON bytes from that
     * object.
     * <p>This is computationally _expensive_ and incurs a heavy GC load, so it
     * should only be used for testing and debugging.
     *
     * @return a Bytes containing the JSON serialized content of the block.
     *     Returns null if the file bytes cannot be read or cannot be parsed.
     */
    private Bytes getJsonBytesFromProtobufBytes(final Bytes sourceData) {
        if (sourceData != null) {
            try {
                return Block.JSON.toBytes(standardParse(Block.PROTOBUF, sourceData, Integer.MAX_VALUE));
            } catch (final RuntimeException | ParseException e) {
                String entryName = blockPathData.blockFileName();
                final String message = FAILED_TO_PARSE_MESSAGE.formatted(blockNumber, absoluteZipFilePath, entryName);
                LOGGER.log(WARNING, message, e);
                return null;
            }
        } else {
            return null;
        }
    }

    @Override
    public void close() {
        try {
            Files.delete(zipFileLink);
        } catch (final RuntimeException | IOException e) {
            final String message = FAILED_TO_DELETE_LINK_MESSAGE.formatted(
                    blockNumber, absoluteZipFilePath, blockPathData.blockFileName());
            LOGGER.log(INFO, message, e);
        }
    }

    @Override
    public boolean isClosed() {
        return !Files.exists(zipFileLink);
    }
}
