// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.blocks.files.recent;

import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.WARNING;

import com.hedera.hapi.block.stream.Block;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Stream;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.base.CompressionType;
import org.hiero.block.node.spi.historicalblocks.BlockAccessor;

/**
 * An implementation of the {@link BlockAccessor} interface that provides access to a block stored in a file with
 * optional compression types on that file. It aims to provide the most efficient transfer for each combination of
 * input and output formats.
 */
final class BlockFileBlockAccessor implements BlockAccessor {
    /** The logger for this class. */
    private static final System.Logger LOGGER = System.getLogger(BlockFileBlockAccessor.class.getName());
    /** Message logged when the protobuf codec fails to parse data */
    private static final String FAILED_TO_PARSE_MESSAGE = "Failed to parse block from file %s.";
    /** Message logged when data cannot be read from a block file */
    private static final String FAILED_TO_READ_MESSAGE = "Failed to read block from file %s.";
    /** Message logged when the provided path to a block file is not a regular file or does not exist. */
    private static final String INVALID_BLOCK_FILE_PATH_MESSAGE =
            "Provided path to block file is not a regular file or does not exist: %s";
    /** The path to the block file. */
    private final Path blockFileLink;
    /** The compression type used for the block file. */
    private final CompressionType compressionType;
    /** The block number of the block. */
    private final long blockNumber;
    /** The absolute path to the block file, used for logging. */
    private final String absolutePathToBlock;

    /**
     * Constructs a BlockFileBlockAccessor with the specified block file path and compression type.
     *
     * @param blockFilePath the path to the block file, must exist
     * @param linksRootPath the root path where hard links to block files will be created
     * @param blockNumber the block number of the block
     */
    BlockFileBlockAccessor(@NonNull final Path blockFilePath, @NonNull final Path linksRootPath, final long blockNumber)
            throws IOException {
        if (!Files.isRegularFile(blockFilePath)) {
            final String msg = INVALID_BLOCK_FILE_PATH_MESSAGE.formatted(blockFilePath);
            throw new IOException(msg);
        }
        this.absolutePathToBlock = blockFilePath.toAbsolutePath().toString();
        this.compressionType = determineCompressionType(blockFilePath);
        this.blockNumber = blockNumber;
        // create a hard link to the block file for the duration of the accessor's life
        final Path link = linksRootPath.resolve(UUID.randomUUID().toString());
        this.blockFileLink = Files.createLink(link, blockFilePath);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long blockNumber() {
        return blockNumber;
    }

    /**
     * Get the compression type of the block file.
     *
     * @return the compression type
     */
    CompressionType compressionType() {
        return compressionType;
    }

    /**
     * {@inheritDoc}
     * <p>Note, we only override here to change the logging message.
     * The method should be otherwise identical to the default.
     */
    @Override
    public BlockUnparsed blockUnparsed() {
        try {
            final Bytes rawData = blockBytes(Format.PROTOBUF);
            return rawData == null ? null : BlockUnparsed.PROTOBUF.parse(rawData);
        } catch (final UncheckedIOException | ParseException e) {
            LOGGER.log(WARNING, FAILED_TO_PARSE_MESSAGE.formatted(absolutePathToBlock), e);
            return null;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Bytes blockBytes(@NonNull final Format format) {
        Objects.requireNonNull(format);
        try {
            return getBytesFromPath(format, blockFileLink, compressionType);
        } catch (final UncheckedIOException | IOException e) {
            LOGGER.log(WARNING, FAILED_TO_READ_MESSAGE.formatted(absolutePathToBlock), e);
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
                final String message = FAILED_TO_PARSE_MESSAGE.formatted(absolutePathToBlock);
                LOGGER.log(WARNING, message, e);
                return null;
            }
        } else {
            return null;
        }
    }

    /**
     * This method deletes the link to the block file.
     */
    @Override
    public void close() {
        try {
            Files.delete(blockFileLink);
        } catch (final IOException e) {
            final String message = "Failed to delete accessor link for block: %d, path: %s"
                    .formatted(blockNumber, absolutePathToBlock);
            LOGGER.log(INFO, message, e);
        }
    }

    @Override
    public boolean isClosed() {
        return !Files.exists(blockFileLink);
    }

    /**
     * Tries to determine the compression type of a block file by examining its magic bytes.
     * <p>
     * This method reads the beginning of the file and compares it against the magic bytes
     * of all known compression types. If a match is found, the corresponding compression
     * type is returned. If no magic bytes match, the file is assumed to be uncompressed.
     *
     * @param verifiedBlockPath the path to the block file that has been verified to exist
     * @return the compression type of the file, or {@link CompressionType#NONE} if the file
     *         is not compressed or uses a compression type without magic bytes
     * @throws IOException if an I/O error occurs while reading the file header
     */
    static CompressionType determineCompressionType(Path verifiedBlockPath) throws IOException {
        // Get all available compression types to check
        final CompressionType[] compressionOpts = CompressionType.values();
        // Find the longest magic bytes sequence to determine how many bytes we need to read
        int max = 0;
        for (CompressionType ct : compressionOpts) {
            if (ct.magicBytes().length > max) {
                max = ct.magicBytes().length;
            }
        }

        // Read the file header once with enough bytes to check all compression types
        final byte[] fileHeader;
        try (final InputStream is = Files.newInputStream(verifiedBlockPath)) {
            fileHeader = is.readNBytes(max);
        }

        // Check each compression type's magic bytes against the file header
        for (CompressionType currentOpt : compressionOpts) {
            final byte[] magicBytes = currentOpt.magicBytes();
            if (magicBytes.length > 0 && fileHeader.length >= magicBytes.length && Arrays.compare(fileHeader, 0, magicBytes.length, magicBytes, 0, magicBytes.length) == 0) {
                    return currentOpt;
            }
        }

        // At the moment all supported actual compressions (ZSTD) have magic bytes. Not finding any
        // means that the file is not compressed. If in the future we add a compression type without
        // magic bytes, then we might need to fall back to file extension-based detection before returning NONE.
        return CompressionType.NONE;
    }
}
