// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.blocks.files.recent;

import static java.lang.System.Logger.Level.WARNING;

import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdOutputStream;
import com.hedera.hapi.block.stream.Block;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import org.hiero.block.common.utils.Preconditions;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.base.CompressionType;
import org.hiero.block.node.spi.historicalblocks.BlockAccessor;

/**
 * An implementation of the {@link BlockAccessor} interface that provides access to a block stored in a file with
 * optional compression types on that file. It aims to provide the most efficient transfer for each combination of
 * input and output formats.
 */
final class BlockFileBlockAccessor implements BlockAccessor {
    /** The size of the buffer used for reading and writing files. */
    private static final int BUFFER_SIZE = 1024 * 1024;
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
     * Constructs a BlockFileBlockAccessor with the specified block file path and compression type.
     *
     * @param blockFilePath   the path to the block file, must exist
     * @param compressionType the compression type used for the block file
     * @param blockNumber     the block number of the block
     */
    BlockFileBlockAccessor(
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
     * <p>Note, we only override here to change the logging message.
     * The method should be otherwise identical to the default.
     */
    @Override
    public Block block() {
        try {
            final Bytes rawData = blockBytes(Format.PROTOBUF);
            return rawData == null ? null : Block.PROTOBUF.parse(rawData);
        } catch (final UncheckedIOException | ParseException e) {
            LOGGER.log(WARNING, FAILED_TO_PARSE_MESSAGE.formatted(blockFilePath), e);
            return null;
        }
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
            LOGGER.log(WARNING, FAILED_TO_PARSE_MESSAGE.formatted(blockFilePath), e);
            return null;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Bytes blockBytes(@NonNull final Format format) {
        Objects.requireNonNull(format);
        try (final InputStream in = new BufferedInputStream(Files.newInputStream(blockFilePath), BUFFER_SIZE);
                final InputStream wrapped = compressionType.wrapStream(in)) {
            Bytes sourceData =
                    switch (format) {
                        case JSON, PROTOBUF -> Bytes.wrap(wrapped.readAllBytes());
                        case ZSTD_PROTOBUF -> {
                            if (compressionType == CompressionType.ZSTD) {
                                yield Bytes.wrap(in.readAllBytes());
                            } else {
                                yield Bytes.wrap(Zstd.compress(wrapped.readAllBytes()));
                            }
                        }
                    };
            if (Format.JSON == format) {
                return getJsonBytesFromProtobufBytes(sourceData);
            } else {
                return sourceData;
            }
        } catch (final UncheckedIOException | IOException e) {
            LOGGER.log(WARNING, FAILED_TO_READ_MESSAGE.formatted(blockFilePath), e);
            return null;
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

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeTo(@NonNull final Format format, @NonNull final Path path) throws IOException {
        Objects.requireNonNull(format);
        Objects.requireNonNull(path);
        try (final InputStream in = Files.newInputStream(blockFilePath);
                final InputStream buffered = new BufferedInputStream(in, BUFFER_SIZE);
                final InputStream wrapped = compressionType.wrapStream(buffered)) {
            switch (format) {
                case JSON -> {
                    try (final OutputStream out = Files.newOutputStream(path)) {
                        final Bytes input = Bytes.wrap(wrapped.readAllBytes());
                        final Bytes jsonBytes = getJsonBytesFromProtobufBytes(input);
                        if (jsonBytes != null) {
                            jsonBytes.writeTo(out);
                        }
                    }
                }
                case PROTOBUF -> {
                    try (final OutputStream out = Files.newOutputStream(path)) {
                        in.transferTo(out);
                    }
                }
                case ZSTD_PROTOBUF -> {
                    if (compressionType == CompressionType.ZSTD) {
                        Files.copy(blockFilePath, path);
                    } else {
                        try (final OutputStream out = new BufferedOutputStream(
                                new ZstdOutputStream(Files.newOutputStream(path)), BUFFER_SIZE)) {
                            wrapped.transferTo(out);
                        }
                    }
                }
            }
        }
    }
}
