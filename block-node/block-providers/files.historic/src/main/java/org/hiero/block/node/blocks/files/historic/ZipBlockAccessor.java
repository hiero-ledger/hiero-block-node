// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.blocks.files.historic;

import static java.lang.System.Logger.Level.WARNING;

import com.hedera.hapi.block.stream.Block;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import org.hiero.block.node.base.CompressionType;
import org.hiero.block.node.spi.historicalblocks.BlockAccessor;

/**
 * The ZipBlockAccessor class provides access to a block stored in a zip file.
 */
final class ZipBlockAccessor implements BlockAccessor {
    /** The logger for this class. */
    private final System.Logger LOGGER = System.getLogger(getClass().getName());
    /** Message logged when the protobuf codec fails to parse data */
    private static final String FAILED_TO_PARSE_MESSAGE =
            "Failed to parse block: %s, zipFilePath: %s, zipEntryName: %s";
    /** Message logged when data cannot be read from a block file */
    private static final String FAILED_TO_READ_MESSAGE = "Failed to read block: %s, zipFilePath: %s, zipEntryName: %s";
    /** Message logged when the provided path to a zip file is not a regular file or does not exist. */
    private static final String INVALID_ZIP_FILE_PATH_MESSAGE =
            "Provided path to zip file is not a regular file or does not exist: %s";
    /** The absolute path to the zip file, used for logging. */
    private final String absoluteZipFilePath;
    /** All path and block information for the block accessed */
    private final BlockPath blockPathData;
    /** Block number this accessor manages. */
    private final long blockNumber;

    /**
     * Constructs a ZipBlockAccessor with the specified block path.
     *
     * @param blockPath the block path
     */
    ZipBlockAccessor(@NonNull final BlockPath blockPath) throws IOException {
        blockPathData = blockPath;
        blockNumber = blockPath.blockNumber();
        final Path zipFilePath = blockPath.zipFilePath();
        this.absoluteZipFilePath = zipFilePath.toAbsolutePath().toString();
        if (!Files.isRegularFile(zipFilePath)) {
            final String msg = INVALID_ZIP_FILE_PATH_MESSAGE.formatted(zipFilePath);
            throw new IOException(msg);
        }
    }

    @Override
    public long blockNumber() {
        return blockNumber;
    }

    @Override
    public Bytes blockBytes(@NonNull final Format format) {
        Objects.requireNonNull(format);
        String entryName = blockPathData.blockFileName();
        try (final FileSystem zipFs = FileSystems.newFileSystem(blockPathData.zipFilePath())) {
            final Path entry = zipFs.getPath(entryName);
            return getBytesFromPath(format, entry, blockPathData.compressionType());
        } catch (final UncheckedIOException | IOException e) {
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
                return Block.JSON.toBytes(Block.PROTOBUF.parse(sourceData));
            } catch (final UncheckedIOException | ParseException e) {
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
        // Nothing to do here at the moment.
    }
}
