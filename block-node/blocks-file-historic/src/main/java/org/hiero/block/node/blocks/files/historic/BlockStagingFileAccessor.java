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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import org.hiero.block.common.utils.Preconditions;
import org.hiero.block.node.base.CompressionType;
import org.hiero.block.node.spi.historicalblocks.BlockAccessor;

/**
 * An implementation of the {@link BlockAccessor} interface that provides access to blocks stored in the staging folder with
 * optional compression types on that file. It aims to provide the most efficient transfer for each combination of
 * input and output formats.
 */
final class BlockStagingFileAccessor implements BlockAccessor {
    /** Message logged when the protobuf codec fails to parse data */
    private static final String FAILED_TO_PARSE_MESSAGE = "Failed to parse block from file %s.";
    /** Message logged when data cannot be read from a block file */
    private static final String FAILED_TO_READ_MESSAGE = "Failed to read block from file %s.";
    /** The logger for this class. */
    private final System.Logger LOGGER = System.getLogger(getClass().getName());
    /** The path to the block file. */
    private final Path blockFilePath;
    /** The compression type used for the block file. */
    private final CompressionType compressionType;
    /** The block number of the block. */
    private final long blockNumber;

    /**
     * Constructs a BlockStagingFileAccessor with the specified block file path and compression type.
     *
     * @param blockFilePath   the path to the block file, must exist
     * @param compressionType the compression type used for the block file
     * @param blockNumber     the block number of the block
     */
    BlockStagingFileAccessor(
            @NonNull final Path blockFilePath, @NonNull final CompressionType compressionType, final long blockNumber) {
        this.blockFilePath = Preconditions.requireRegularFile(blockFilePath);
        this.compressionType = Objects.requireNonNull(compressionType);
        this.blockNumber = blockNumber;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long blockNumber() {
        return blockNumber;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Bytes blockBytes(@NonNull final Format format) {
        Objects.requireNonNull(format);
        try {
            return getBytesFromPath(format, blockFilePath, compressionType);
        } catch (final UncheckedIOException | IOException e) {
            LOGGER.log(WARNING, FAILED_TO_READ_MESSAGE.formatted(blockFilePath), e);
            return null;
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Note, it is important that this _not_ remove the staging file. The
     * attempt to create a zip might not have succeeded, so the files may
     * need to remain in place (in fact that is perhaps more common than not)
     * until the next attempt.
     */
    @Override
    public void close() {}

    @Override
    public boolean isClosed() {
        return false; // close does nothing, so this instance is always "open".
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
                final String message = FAILED_TO_PARSE_MESSAGE.formatted(blockFilePath);
                LOGGER.log(WARNING, message, e);
                return null;
            }
        } else {
            return null;
        }
    }
}
