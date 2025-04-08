// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.blocks.files.historic;

import static java.lang.System.Logger.Level.ERROR;

import com.github.luben.zstd.ZstdInputStream;
import com.hedera.hapi.block.stream.Block;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.UncheckedParseException;
import com.hedera.pbj.runtime.io.WritableSequentialData;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import org.hiero.block.node.spi.historicalblocks.BlockAccessor;
import org.hiero.hapi.block.node.BlockUnparsed;

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
    public void delete() {
        // TODO feels like this should be implemented but there is no good way to delete a single file from a zip file
        //  without extracting the whole zip file, deleting the file, and then re-creating the zip file. So we could
        //  implement this but it would be slow and not very useful. So for now we just throw an exception. To properly
        //  support we need to create a new API for moving and deleting batches of blocks.
        BlockAccessor.super.delete();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Block block() {
        try {
            return Block.PROTOBUF.parse(blockBytes(Format.PROTOBUF));
        } catch (final ParseException e) {
            LOGGER.log(ERROR, "Failed to parse block", e);
            throw new UncheckedParseException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BlockUnparsed blockUnparsed() {
        try {
            return BlockUnparsed.PROTOBUF.parse(blockBytes(Format.PROTOBUF));
        } catch (final ParseException e) {
            LOGGER.log(ERROR, "Failed to parse block", e);
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
                try (final ZipFile zipFile = new ZipFile(blockPath.zipFilePath().toFile())) {
                    final ZipEntry entry = zipFile.getEntry(blockPath.blockFileName());
                    try (final ZstdInputStream in = new ZstdInputStream(zipFile.getInputStream(entry))) {
                        yield Bytes.wrap(in.readAllBytes());
                    }
                } catch (final IOException e) {
                    LOGGER.log(ERROR, "Failed to read block from zip file", e);
                    throw new UncheckedIOException(e);
                }
            }
            case ZSTD_PROTOBUF -> {
                try (final ZipFile zipFile = new ZipFile(blockPath.zipFilePath().toFile())) {
                    final ZipEntry entry = zipFile.getEntry(blockPath.blockFileName());
                    try (final InputStream in = zipFile.getInputStream(entry)) {
                        yield Bytes.wrap(in.readAllBytes());
                    }
                } catch (final IOException e) {
                    LOGGER.log(ERROR, "Failed to read block from zip file", e);
                    throw new UncheckedIOException(e);
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
        // This could be more efficient and require less RAM but for now this is fine
        output.writeBytes(blockBytes(format));
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
            case JSON -> blockBytes(format).writeTo(output);
            case PROTOBUF -> {
                try (final ZipFile zipFile = new ZipFile(blockPath.zipFilePath().toFile())) {
                    final ZipEntry entry = zipFile.getEntry(blockPath.blockFileName());
                    try (final ZstdInputStream in = new ZstdInputStream(zipFile.getInputStream(entry))) {
                        in.transferTo(output);
                    }
                } catch (final IOException e) {
                    LOGGER.log(ERROR, "Failed to read block from zip file", e);
                    throw new UncheckedIOException(e);
                }
            }
            case ZSTD_PROTOBUF -> {
                try (final ZipFile zipFile = new ZipFile(blockPath.zipFilePath().toFile())) {
                    final ZipEntry entry = zipFile.getEntry(blockPath.blockFileName());
                    try (final InputStream in = zipFile.getInputStream(entry)) {
                        in.transferTo(output);
                    }
                } catch (final IOException e) {
                    LOGGER.log(ERROR, "Failed to read block from zip file", e);
                    throw new UncheckedIOException(e);
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
                try (final OutputStream output = Files.newOutputStream(path)) {
                    blockBytes(format).writeTo(output);
                }
            }
            case PROTOBUF -> {
                try (final ZipFile zipFile = new ZipFile(blockPath.zipFilePath().toFile())) {
                    final ZipEntry entry = zipFile.getEntry(blockPath.blockFileName());
                    try (final ZstdInputStream in = new ZstdInputStream(zipFile.getInputStream(entry));
                            final OutputStream output = Files.newOutputStream(path)) {
                        in.transferTo(output);
                    }
                } catch (final IOException e) {
                    LOGGER.log(ERROR, "Failed to read block from zip file", e);
                    throw new UncheckedIOException(e);
                }
            }
            case ZSTD_PROTOBUF -> {
                try (final ZipFile zipFile = new ZipFile(blockPath.zipFilePath().toFile())) {
                    final ZipEntry entry = zipFile.getEntry(blockPath.blockFileName());
                    try (final InputStream in = zipFile.getInputStream(entry);
                            final OutputStream output = Files.newOutputStream(path)) {
                        in.transferTo(output);
                    }
                } catch (final IOException e) {
                    LOGGER.log(ERROR, "Failed to read block from zip file", e);
                    throw new UncheckedIOException(e);
                }
            }
        }
    }
}
