// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.blocks.files.recent;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.WARNING;

import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdOutputStream;
import com.hedera.hapi.block.stream.Block;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.UncheckedParseException;
import com.hedera.pbj.runtime.io.WritableSequentialData;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.hedera.pbj.runtime.io.stream.ReadableStreamingData;
import com.hedera.pbj.runtime.io.stream.WritableStreamingData;
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
import org.hiero.block.node.base.CompressionType;
import org.hiero.block.node.spi.historicalblocks.BlockAccessor;
import org.hiero.hapi.block.node.BlockUnparsed;

/**
 * An implementation of the {@link BlockAccessor} interface that provides access to a block stored in a file with
 * optional compression types on that file. It aims to provide the most efficient transfer for each combination of
 * input and output formats.
 */
final class BlockFileBlockAccessor implements BlockAccessor {
    /** The size of the buffer used for reading and writing files. */
    private static final int BUFFER_SIZE = 1024 * 1024;
    /** The logger for this class. */
    private final System.Logger LOGGER = System.getLogger(getClass().getName());
    /** The base directory for all block files. */
    private final Path baseDir;
    /** The path to the block file. */
    private final Path blockFilePath;
    /** The compression type used for the block file. */
    private final CompressionType compressionType;

    /**
     * Constructs a BlockFileBlockAccessor with the specified block file path and compression type.
     *
     * @param baseDir         the base directory for all block files, must exist
     * @param blockFilePath   the path to the block file, must exist
     * @param compressionType the compression type used for the block file
     */
    BlockFileBlockAccessor(
            @NonNull final Path baseDir,
            @NonNull final Path blockFilePath,
            @NonNull final CompressionType compressionType) {
        this.baseDir = Preconditions.requireDirectory(baseDir);
        this.blockFilePath = Preconditions.requireRegularFile(blockFilePath);
        this.compressionType = Objects.requireNonNull(compressionType);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void delete() {
        try {
            // log we are deleting the block file
            LOGGER.log(DEBUG, "Deleting block file: " + blockFilePath);
            // delete the block file
            Files.deleteIfExists(blockFilePath);
            // clean up any empty parent directories up to the base directory
            Path parentDir = blockFilePath.getParent();
            while (parentDir != null && !parentDir.equals(baseDir)) {
                try (var filesList = Files.list(parentDir)) {
                    if (filesList.findAny().isPresent()) {
                        break;
                    }
                } catch (IOException e) {
                    LOGGER.log(WARNING, "Failed to list files in directory: " + parentDir, e);
                }
                // we did not find any files in the directory, so delete it
                Files.deleteIfExists(parentDir);
                // move up to the parent directory
                parentDir = parentDir.getParent();
            }
        } catch (IOException e) {
            LOGGER.log(WARNING, "Failed to delete block file: " + blockFilePath, e);
            throw new RuntimeException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Block block() {
        try (final ReadableStreamingData in = new ReadableStreamingData(compressionType.wrapStream(
                new BufferedInputStream(Files.newInputStream(blockFilePath), BUFFER_SIZE)))) {
            return Block.PROTOBUF.parse(in);
        } catch (final IOException e) {
            LOGGER.log(WARNING, "Failed to read block from file: " + blockFilePath, e);
            throw new UncheckedIOException(e);
        } catch (final ParseException e) {
            LOGGER.log(WARNING, "Failed to parse block from file: " + blockFilePath, e);
            throw new UncheckedParseException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BlockUnparsed blockUnparsed() {
        try (final ReadableStreamingData in = new ReadableStreamingData(compressionType.wrapStream(
                new BufferedInputStream(Files.newInputStream(blockFilePath), BUFFER_SIZE)))) {
            return BlockUnparsed.PROTOBUF.parse(in);
        } catch (final IOException e) {
            LOGGER.log(WARNING, "Failed to read block (unparsed) from file: " + blockFilePath, e);
            throw new UncheckedIOException(e);
        } catch (final ParseException e) {
            LOGGER.log(WARNING, "Failed to parse block (unparsed) from file: " + blockFilePath, e);
            throw new UncheckedParseException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Bytes blockBytes(@NonNull final Format format) throws IllegalArgumentException {
        Objects.requireNonNull(format);
        return switch (format) {
            case JSON -> Block.JSON.toBytes(block());
            case PROTOBUF -> {
                try (final InputStream in = compressionType.wrapStream(
                        new BufferedInputStream(Files.newInputStream(blockFilePath), BUFFER_SIZE))) {
                    yield Bytes.wrap(in.readAllBytes());
                } catch (final IOException e) {
                    LOGGER.log(WARNING, "Failed to read block (bytes) from file: " + blockFilePath, e);
                    throw new UncheckedIOException(e);
                }
            }
            case ZSTD_PROTOBUF -> {
                if (compressionType == CompressionType.ZSTD) {
                    try (final InputStream in =
                            new BufferedInputStream(Files.newInputStream(blockFilePath), BUFFER_SIZE)) {
                        yield Bytes.wrap(in.readAllBytes());
                    } catch (final IOException e) {
                        LOGGER.log(WARNING, "Failed to read block (bytes) from file: " + blockFilePath, e);
                        throw new UncheckedIOException(e);
                    }
                } else {
                    try (final InputStream in = compressionType.wrapStream(
                            new BufferedInputStream(Files.newInputStream(blockFilePath), BUFFER_SIZE))) {
                        yield Bytes.wrap(Zstd.compress(in.readAllBytes()));
                    } catch (final IOException e) {
                        LOGGER.log(WARNING, "Failed to read block (bytes) from file: " + blockFilePath, e);
                        throw new UncheckedIOException(e);
                    }
                }
            }
        };
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeBytesTo(@NonNull final Format format, @NonNull final WritableSequentialData output)
            throws IllegalArgumentException {
        Objects.requireNonNull(format);
        Objects.requireNonNull(output);
        switch (format) {
            case JSON -> Block.JSON.toBytes(block()).writeTo(output);
            case PROTOBUF -> {
                try (final InputStream in = compressionType.wrapStream(Files.newInputStream(blockFilePath))) {
                    final byte[] buffer = new byte[BUFFER_SIZE];
                    int read;
                    while ((read = in.read(buffer, 0, buffer.length)) >= 0) {
                        output.writeBytes(buffer, 0, read);
                    }
                } catch (final IOException e) {
                    LOGGER.log(WARNING, "Failed to read block from file: " + blockFilePath, e);
                    throw new UncheckedIOException(e);
                }
            }
            case ZSTD_PROTOBUF -> {
                if (compressionType == CompressionType.ZSTD) {
                    try (final InputStream in = Files.newInputStream(blockFilePath)) {
                        final byte[] buffer = new byte[BUFFER_SIZE];
                        int read;
                        while ((read = in.read(buffer, 0, buffer.length)) >= 0) {
                            output.writeBytes(buffer, 0, read);
                        }
                    } catch (final IOException e) {
                        LOGGER.log(WARNING, "Failed to read block from file: " + blockFilePath, e);
                        throw new UncheckedIOException(e);
                    }
                } else {
                    try (final InputStream in = compressionType.wrapStream(
                            new BufferedInputStream(Files.newInputStream(blockFilePath), BUFFER_SIZE))) {
                        output.writeBytes(Zstd.compress(in.readAllBytes()));
                    } catch (final IOException e) {
                        LOGGER.log(WARNING, "Failed to read block from file: " + blockFilePath, e);
                        throw new UncheckedIOException(e);
                    }
                }
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeBytesTo(@NonNull final Format format, @NonNull final OutputStream output)
            throws IllegalArgumentException {
        Objects.requireNonNull(format);
        Objects.requireNonNull(output);
        switch (format) {
            case JSON -> Block.JSON.toBytes(block()).writeTo(output);
            case PROTOBUF -> {
                try (final InputStream in = compressionType.wrapStream(Files.newInputStream(blockFilePath))) {
                    final byte[] buffer = new byte[BUFFER_SIZE];
                    int read;
                    while ((read = in.read(buffer, 0, buffer.length)) >= 0) {
                        output.write(buffer, 0, read);
                    }
                } catch (final IOException e) {
                    LOGGER.log(WARNING, "Failed to read block from file: " + blockFilePath, e);
                    throw new UncheckedIOException(e);
                }
            }
            case ZSTD_PROTOBUF -> {
                if (compressionType == CompressionType.ZSTD) {
                    try (final InputStream in = Files.newInputStream(blockFilePath)) {
                        in.transferTo(output);
                    } catch (final IOException e) {
                        LOGGER.log(WARNING, "Failed to read block from file: " + blockFilePath, e);
                        throw new UncheckedIOException(e);
                    }
                } else {
                    try (final InputStream in = compressionType.wrapStream(
                            new BufferedInputStream(Files.newInputStream(blockFilePath), BUFFER_SIZE))) {
                        output.write(Zstd.compress(in.readAllBytes()));
                    } catch (final IOException e) {
                        LOGGER.log(WARNING, "Failed to read block from file: " + blockFilePath, e);
                        throw new UncheckedIOException(e);
                    }
                }
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeTo(@NonNull final Format format, @NonNull final Path path) throws IOException {
        Objects.requireNonNull(format);
        Objects.requireNonNull(path);
        switch (format) {
            case JSON -> {
                try (final WritableStreamingData out = new WritableStreamingData(Files.newOutputStream(path))) {
                    Block.JSON.write(block(), out);
                }
            }
            case PROTOBUF -> {
                try (final OutputStream out = new BufferedOutputStream(Files.newOutputStream(path), BUFFER_SIZE);
                        final InputStream in = compressionType.wrapStream(
                                new BufferedInputStream(Files.newInputStream(blockFilePath), BUFFER_SIZE))) {
                    in.transferTo(out);
                }
            }
            case ZSTD_PROTOBUF -> {
                if (compressionType == CompressionType.ZSTD) {
                    Files.copy(blockFilePath, path);
                } else {
                    try (final OutputStream out = new BufferedOutputStream(
                                    new ZstdOutputStream(Files.newOutputStream(path)), BUFFER_SIZE);
                            final InputStream in = compressionType.wrapStream(
                                    new BufferedInputStream(Files.newInputStream(blockFilePath), BUFFER_SIZE))) {
                        in.transferTo(out);
                    }
                }
            }
        }
    }
}
