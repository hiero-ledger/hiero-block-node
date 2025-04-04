// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.blocks.files.historic;

import com.github.luben.zstd.ZstdInputStream;
import com.hedera.hapi.block.stream.Block;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.WritableSequentialData;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.ZipFile;
import org.hiero.block.node.spi.historicalblocks.BlockAccessor;
import org.hiero.hapi.block.node.BlockUnparsed;

/**
 * The ZipBlockAccessor class provides access to a block stored in a zip file.
 */
public class ZipBlockAccessor implements BlockAccessor {
    /** The logger for this class. */
    private final System.Logger LOGGER = System.getLogger(getClass().getName());
    /** The block path. */
    private final BlockPath blockPath;

    /**
     * Constructs a ZipBlockAccessor with the specified block path.
     *
     * @param blockPath the block path
     */
    ZipBlockAccessor(BlockPath blockPath) {
        this.blockPath = blockPath;
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
        } catch (ParseException e) {
            LOGGER.log(System.Logger.Level.ERROR, "Failed to parse block", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BlockUnparsed blockUnparsed() {
        try {
            return BlockUnparsed.PROTOBUF.parse(blockBytes(Format.PROTOBUF));
        } catch (ParseException e) {
            LOGGER.log(System.Logger.Level.ERROR, "Failed to parse block", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Bytes blockBytes(final Format format) throws IllegalArgumentException {
        return switch (format) {
            case JSON -> Block.JSON.toBytes(block());
            case PROTOBUF -> {
                try (ZipFile zipFile = new ZipFile(blockPath.zipFilePath().toFile())) {
                    final var entry = zipFile.getEntry(blockPath.blockFileName());
                    try (var in = new ZstdInputStream(zipFile.getInputStream(entry))) {
                        yield Bytes.wrap(in.readAllBytes());
                    }
                } catch (IOException e) {
                    LOGGER.log(System.Logger.Level.ERROR, "Failed to read block from zip file", e);
                    throw new RuntimeException(e);
                }
            }
            case ZSTD_PROTOBUF -> {
                try (ZipFile zipFile = new ZipFile(blockPath.zipFilePath().toFile())) {
                    final var entry = zipFile.getEntry(blockPath.blockFileName());
                    try (var in = zipFile.getInputStream(entry)) {
                        yield Bytes.wrap(in.readAllBytes());
                    }
                } catch (IOException e) {
                    LOGGER.log(System.Logger.Level.ERROR, "Failed to read block from zip file", e);
                    throw new RuntimeException(e);
                }
            }
        };
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeBytesTo(final Format format, final WritableSequentialData output) throws IllegalArgumentException {
        // This could be more efficient and require less RAM but for now this is fine
        output.writeBytes(blockBytes(format));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeBytesTo(final Format format, final OutputStream output) throws IllegalArgumentException {
        switch (format) {
            case JSON -> blockBytes(format).writeTo(output);
            case PROTOBUF -> {
                try (ZipFile zipFile = new ZipFile(blockPath.zipFilePath().toFile())) {
                    final var entry = zipFile.getEntry(blockPath.blockFileName());
                    try (var in = new ZstdInputStream(zipFile.getInputStream(entry))) {
                        in.transferTo(output);
                    }
                } catch (IOException e) {
                    LOGGER.log(System.Logger.Level.ERROR, "Failed to read block from zip file", e);
                    throw new RuntimeException(e);
                }
            }
            case ZSTD_PROTOBUF -> {
                try (ZipFile zipFile = new ZipFile(blockPath.zipFilePath().toFile())) {
                    final var entry = zipFile.getEntry(blockPath.blockFileName());
                    try (var in = zipFile.getInputStream(entry)) {
                        in.transferTo(output);
                    }
                } catch (IOException e) {
                    LOGGER.log(System.Logger.Level.ERROR, "Failed to read block from zip file", e);
                    throw new RuntimeException(e);
                }
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeTo(final Format format, final Path path) throws IOException {
        switch (format) {
            case JSON -> {
                try (final var output = Files.newOutputStream(path)) {
                    blockBytes(format).writeTo(output);
                }
            }
            case PROTOBUF -> {
                try (ZipFile zipFile = new ZipFile(blockPath.zipFilePath().toFile())) {
                    final var entry = zipFile.getEntry(blockPath.blockFileName());
                    try (var in = new ZstdInputStream(zipFile.getInputStream(entry));
                            var output = Files.newOutputStream(path)) {
                        in.transferTo(output);
                    }
                } catch (IOException e) {
                    LOGGER.log(System.Logger.Level.ERROR, "Failed to read block from zip file", e);
                    throw new RuntimeException(e);
                }
            }
            case ZSTD_PROTOBUF -> {
                try (ZipFile zipFile = new ZipFile(blockPath.zipFilePath().toFile())) {
                    final var entry = zipFile.getEntry(blockPath.blockFileName());
                    try (var in = zipFile.getInputStream(entry);
                            var output = Files.newOutputStream(path)) {
                        in.transferTo(output);
                    }
                } catch (IOException e) {
                    LOGGER.log(System.Logger.Level.ERROR, "Failed to read block from zip file", e);
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
