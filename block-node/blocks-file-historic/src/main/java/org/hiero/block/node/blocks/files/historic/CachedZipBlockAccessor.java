// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.blocks.files.historic;

import static java.lang.System.Logger.Level.WARNING;
import static java.util.Objects.requireNonNull;
import static org.hiero.block.node.base.ParseHelper.standardParse;

import com.hedera.hapi.block.stream.Block;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.io.InputStream;
import java.lang.System.Logger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Consumer;
import org.hiero.block.node.base.CompressionType;
import org.hiero.block.node.spi.historicalblocks.BlockAccessor;

/**
 * The CachedZipBlockAccessor class provides access to a block stored in a zip file, sharing a single open
 * filesystem across every accessor currently reading from the same archive rather than opening (and indexing)
 * its own filesystem per block, unlike {@link ZipBlockAccessor}.
 * <p>
 * Reads go through a {@link ZipBlockArchive.ArchiveHandle} shared and reference-counted across every accessor
 * currently reading from the same archive. {@link #close()} releases this accessor's reference; the underlying
 * filesystem is not necessarily closed at that point, since {@link ZipBlockArchive} keeps recently-used archives
 * cached for reuse by later reads.
 * <p>
 * This class exists alongside {@link ZipBlockAccessor} (rather than replacing it) so the two implementations can
 * be switched between via {@link FilesHistoricConfig#cachedZipAccessorEnabled()} and profiled/compared, and so
 * there is a known-safe fallback if a shared, concurrently-read zip filesystem ever proves unsafe in practice.
 */
final class CachedZipBlockAccessor implements BlockAccessor {
    /** The logger for this class. */
    private final Logger LOGGER = System.getLogger(getClass().getName());
    /** Message logged when the protobuf codec fails to parse data */
    private static final String FAILED_TO_PARSE_MESSAGE =
            "Failed to parse block: %s, zipFilePath: %s, zipEntryName: %s";
    /** Message logged when data cannot be read from a block file */
    private static final String FAILED_TO_READ_MESSAGE = "Failed to read block: %s, zipFilePath: %s, zipEntryName: %s";
    /** All path and block information for the block accessed */
    private final BlockPath blockPathData;
    /** Block number this accessor manages. */
    private final long blockNumber;
    /** The shared, reference-counted handle to this block's archive filesystem. */
    private final ZipBlockArchive.ArchiveHandle archiveHandle;
    /** Releases this accessor's reference to {@link #archiveHandle} on {@link #close()}. */
    private final Consumer<ZipBlockArchive.ArchiveHandle> releaseArchive;
    /** Whether this accessor has been closed. */
    private volatile boolean closed = false;

    /**
     * Constructs a CachedZipBlockAccessor for a block resolved within an already-acquired archive handle.
     *
     * @param blockPath the resolved block path
     * @param archiveHandle the caller's reference to the shared archive filesystem, acquired for this accessor
     * @param releaseArchive callback invoked exactly once, on {@link #close()}, to release the reference
     */
    CachedZipBlockAccessor(
            @NonNull final BlockPath blockPath,
            @NonNull final ZipBlockArchive.ArchiveHandle archiveHandle,
            @NonNull final Consumer<ZipBlockArchive.ArchiveHandle> releaseArchive) {
        blockPathData = requireNonNull(blockPath);
        blockNumber = blockPath.blockNumber();
        this.archiveHandle = requireNonNull(archiveHandle);
        this.releaseArchive = requireNonNull(releaseArchive);
    }

    @Override
    public long blockNumber() {
        return blockNumber;
    }

    @Override
    public Bytes blockBytes(@NonNull final Format format) {
        requireNonNull(format);
        final String entryName = blockPathData.blockFileName();
        try {
            final Path entry = archiveHandle.fileSystem().getPath(entryName);
            return getBytesFromPath(format, entry, blockPathData.compressionType());
        } catch (final RuntimeException | IOException e) {
            final String message =
                    FAILED_TO_READ_MESSAGE.formatted(blockNumber, blockPathData.zipFilePath(), entryName);
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
     * @param sourceData the protobuf-encoded block bytes, never null
     * @return a Bytes containing the JSON serialized content of the block.
     *     Returns null if the bytes cannot be parsed.
     */
    private Bytes getJsonBytesFromProtobufBytes(final Bytes sourceData) {
        try {
            return Block.JSON.toBytes(standardParse(Block.PROTOBUF, sourceData, Integer.MAX_VALUE));
        } catch (final RuntimeException | ParseException e) {
            final String entryName = blockPathData.blockFileName();
            final String message =
                    FAILED_TO_PARSE_MESSAGE.formatted(blockNumber, blockPathData.zipFilePath(), entryName);
            LOGGER.log(WARNING, message, e);
            return null;
        }
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            releaseArchive.accept(archiveHandle);
        }
    }

    @Override
    public boolean isClosed() {
        return closed;
    }
}
