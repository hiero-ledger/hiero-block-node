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
import org.hiero.block.node.base.CompressionType;
import org.hiero.block.node.spi.historicalblocks.BlockAccessor;

/**
 * Base class for {@link BlockAccessor}s that read a single block entry out of a zip archive. It holds the block
 * identity, and the read, format conversion, and error handling that do not depend on how the archive's
 * filesystem is obtained. Subclasses decide that (a per-accessor filesystem opened over a temporary hard link in
 * {@link ZipBlockAccessor}, or a shared, reference-counted one in {@link CachedZipBlockAccessor}) by implementing
 * {@link #readEntry(Format)}, and own their own close semantics.
 */
abstract class AbstractZipBlockAccessor implements BlockAccessor {
    /** The logger for this class. */
    protected final Logger LOGGER = System.getLogger(getClass().getName());
    /** Message logged when the protobuf codec fails to parse data */
    private static final String FAILED_TO_PARSE_MESSAGE =
            "Failed to parse block: %s, zipFilePath: %s, zipEntryName: %s";
    /** Message logged when data cannot be read from a block file */
    private static final String FAILED_TO_READ_MESSAGE = "Failed to read block: %s, zipFilePath: %s, zipEntryName: %s";
    /** All path and block information for the block accessed */
    protected final BlockPath blockPathData;
    /** Block number this accessor manages. */
    private final long blockNumber;
    /** The path to the zip file, used for logging. */
    private final Path logZipFilePath;

    /**
     * @param blockPath the resolved block path
     * @param logZipFilePath the zip file path to report in log messages
     */
    AbstractZipBlockAccessor(@NonNull final BlockPath blockPath, @NonNull final Path logZipFilePath) {
        blockPathData = requireNonNull(blockPath);
        blockNumber = blockPath.blockNumber();
        this.logZipFilePath = requireNonNull(logZipFilePath);
    }

    /**
     * Reads this accessor's block entry from its archive, in the requested format. Implementations obtain the
     * archive filesystem, resolve {@link BlockPath#blockFileName()} in it and delegate to
     * {@link #getBytesFromPath}. Any exception thrown is logged and results in a null return from
     * {@link #blockBytes}.
     */
    protected abstract Bytes readEntry(@NonNull Format format) throws IOException;

    @Override
    public long blockNumber() {
        return blockNumber;
    }

    @Override
    public Bytes blockBytes(@NonNull final Format format) {
        requireNonNull(format);
        try {
            return readEntry(format);
        } catch (final RuntimeException | IOException e) {
            final String message =
                    FAILED_TO_READ_MESSAGE.formatted(blockNumber, logZipFilePath, blockPathData.blockFileName());
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
     * @return the bytes of the block in the desired format
     * @throws IOException if unable to read or decompress the data.
     */
    protected final Bytes getBytesFromPath(
            final Format responseFormat, final Path sourcePath, final CompressionType sourceCompression)
            throws IOException {
        try (final InputStream in = Files.newInputStream(sourcePath);
                final InputStream wrapped = sourceCompression.wrapStream(in)) {
            final Bytes sourceData =
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
            final String message =
                    FAILED_TO_PARSE_MESSAGE.formatted(blockNumber, logZipFilePath, blockPathData.blockFileName());
            LOGGER.log(WARNING, message, e);
            return null;
        }
    }
}
