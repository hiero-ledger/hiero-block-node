// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.blocks.files.historic;

import static java.lang.System.Logger.Level.ERROR;

import com.hedera.hapi.block.stream.Block;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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
        return switch (format) {
            case JSON -> Block.JSON.toBytes(block());
            case PROTOBUF -> {
                try (final FileSystem zipFs = FileSystems.newFileSystem(blockPath.zipFilePath());
                        final InputStream wrappedInputStream = blockPath
                                .compressionType()
                                .wrapStream(Files.newInputStream(zipFs.getPath(blockPath.blockFileName())))) {
                    yield Bytes.wrap(wrappedInputStream.readAllBytes());
                } catch (final IOException e) {
                    LOGGER.log(ERROR, "Failed to read block from zip file", e);
                    yield null;
                }
            }
            case ZSTD_PROTOBUF -> {
                try (final FileSystem zipFs = FileSystems.newFileSystem(blockPath.zipFilePath())) {
                    final Path entry = zipFs.getPath(blockPath.blockFileName());
                    try (final InputStream in = Files.newInputStream(entry)) {
                        // if the compression type of the block is ZSTD then we
                        // simply need to return the bytes
                        if (blockPath.compressionType() == CompressionType.ZSTD) {
                            yield Bytes.wrap(in.readAllBytes());
                        } else {
                            // else we need to wrap the stream to read the decompressed
                            // data and then to compress with ZSTD
                            try (final InputStream wrapped =
                                    blockPath.compressionType().wrapStream(in)) {
                                yield Bytes.wrap(CompressionType.ZSTD.compress(wrapped.readAllBytes()));
                            }
                        }
                    }
                } catch (final IOException e) {
                    LOGGER.log(ERROR, "Failed to read block from zip file", e);
                    yield null;
                }
            }
        };
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeBytesTo(@NonNull final Format format, @NonNull final OutputStream output) {
        Objects.requireNonNull(format);
        Objects.requireNonNull(output);
        switch (format) {
            case JSON -> blockBytes(format).writeTo(output);
            case PROTOBUF -> {
                try (final FileSystem zipFs = FileSystems.newFileSystem(blockPath.zipFilePath());
                        final InputStream wrappedInputStream = blockPath
                                .compressionType()
                                .wrapStream(Files.newInputStream(zipFs.getPath(blockPath.blockFileName())))) {
                    wrappedInputStream.transferTo(output);
                } catch (final IOException e) {
                    LOGGER.log(ERROR, "Failed to read block from zip file", e);
                }
            }
            case ZSTD_PROTOBUF -> {
                try (final FileSystem zipFs = FileSystems.newFileSystem(blockPath.zipFilePath())) {
                    final Path entry = zipFs.getPath(blockPath.blockFileName());
                    try (final InputStream in = Files.newInputStream(entry)) {
                        // if the compression type of the block is ZSTD then we
                        // simply need to return the bytes
                        if (blockPath.compressionType() == CompressionType.ZSTD) {
                            in.transferTo(output);
                        } else {
                            // else we need to wrap the stream to read the decompressed
                            // data and then to compress with ZSTD
                            try (final InputStream wrappedIn =
                                            blockPath.compressionType().wrapStream(Files.newInputStream(entry));
                                    final OutputStream wrappedOut = CompressionType.ZSTD.wrapStream(output)) {
                                wrappedIn.transferTo(wrappedOut);
                            }
                        }
                    }
                } catch (final IOException e) {
                    LOGGER.log(ERROR, "Failed to read block from zip file", e);
                }
            }
        }
    }
}
