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
    private static final String FAILED_TO_PARSE_MESSAGE = "Failed to parse block from file %s.";
    /** Message logged when data cannot be read from a block file */
    private static final String FAILED_TO_READ_MESSAGE = "Failed to read block from file %s.";
    /** The block path. */
    private final BlockPath blockPath;

    /**
     * Constructs a ZipBlockAccessor with the specified block path.
     *
     * @param blockPath the block path
     */
    ZipBlockAccessor(@NonNull final BlockPath blockPath) {
        this.blockPath = Objects.requireNonNull(blockPath);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long blockNumber() {
        // TODO maybe there is nice option here than having to parse the string
        return Long.parseLong(blockPath.blockNumStr());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Bytes blockBytes(@NonNull final Format format) {
        Objects.requireNonNull(format);
        CompressionType compressionType = blockPath.compressionType();
        try (final FileSystem zipFs = FileSystems.newFileSystem(blockPath.zipFilePath())) {
            final Path entry = zipFs.getPath(blockPath.blockFileName());
            return getBytesFromPath(format, entry, compressionType);
        } catch (final UncheckedIOException | IOException e) {
            LOGGER.log(WARNING, FAILED_TO_READ_MESSAGE.formatted(blockPath), e);
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
                final String message = FAILED_TO_PARSE_MESSAGE.formatted(blockPath);
                LOGGER.log(WARNING, message, e);
                return null;
            }
        } else {
            return null;
        }
    }
}
