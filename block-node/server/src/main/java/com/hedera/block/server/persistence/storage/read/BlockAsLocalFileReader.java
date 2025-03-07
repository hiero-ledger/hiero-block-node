// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.server.persistence.storage.read;

import com.hedera.block.common.utils.Preconditions;
import com.hedera.block.server.persistence.storage.PersistenceStorageConfig.CompressionType;
import com.hedera.block.server.persistence.storage.compression.Compression;
import com.hedera.block.server.persistence.storage.path.ArchiveBlockPath;
import com.hedera.block.server.persistence.storage.path.BlockPathResolver;
import com.hedera.block.server.persistence.storage.path.LiveBlockPath;
import com.hedera.hapi.block.BlockUnparsed;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.stream.ReadableStreamingData;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/**
 * A Block reader that reads block-as-file.
 */
public final class BlockAsLocalFileReader implements LocalBlockReader<BlockUnparsed> {
    private final BlockPathResolver pathResolver;
    private final Compression compression;

    /**
     * Constructor.
     *
     * @param pathResolver valid, {@code non-null} instance of
     * {@link BlockPathResolver} used to resolve paths to block files
     */
    private BlockAsLocalFileReader(
            @NonNull final Compression compression, @NonNull final BlockPathResolver pathResolver) {
        this.pathResolver = Objects.requireNonNull(pathResolver);
        this.compression = Objects.requireNonNull(compression);
    }

    /**
     * This method creates and returns a new instance of {@link BlockAsLocalFileReader}.
     *
     * @param compression valid, {@code non-null} instance of {@link Compression}
     * @param pathResolver valid, {@code non-null} instance of
     * {@link BlockPathResolver} used to resolve paths to block files
     * @return a new, fully initialized instance of {@link BlockAsLocalFileReader}
     */
    public static BlockAsLocalFileReader of(
            @NonNull final Compression compression, @NonNull final BlockPathResolver pathResolver) {
        return new BlockAsLocalFileReader(compression, pathResolver);
    }

    @NonNull
    @Override
    public Optional<BlockUnparsed> read(final long blockNumber) throws IOException, ParseException {
        Preconditions.requireWhole(blockNumber);
        final Optional<LiveBlockPath> optBlockPath = pathResolver.findLiveBlock(blockNumber);
        if (optBlockPath.isPresent()) {
            final LiveBlockPath liveBlockPath = optBlockPath.get();
            final Path actualPathToBlock = liveBlockPath.dirPath().resolve(liveBlockPath.blockFileName());
            final BlockUnparsed value;
            try (final InputStream in = Files.newInputStream(actualPathToBlock)) {
                value = doRead(in, liveBlockPath.compressionType());
            }
            return Optional.of(value);
        } else {
            final Optional<ArchiveBlockPath> optArchivedBlock = pathResolver.findArchivedBlock(blockNumber);
            if (optArchivedBlock.isPresent()) {
                final ArchiveBlockPath archiveBlockPath = optArchivedBlock.get();
                final Path zipFilePath = archiveBlockPath.dirPath().resolve(archiveBlockPath.zipFileName());
                final BlockUnparsed value;
                // @todo(741) update reader to use zipfs to read blocks
                try (final ZipFile zipFile = new ZipFile(zipFilePath.toFile())) {
                    final ZipEntry entry = zipFile.getEntry(archiveBlockPath.zipEntryName());
                    final InputStream in = zipFile.getInputStream(entry);
                    value = doRead(in, archiveBlockPath.compressionType());
                }
                return Optional.of(value);
            }
            return Optional.empty();
        }
    }

    private BlockUnparsed doRead(final InputStream in, final CompressionType compressionType)
            throws IOException, ParseException {
        try (final ReadableStreamingData data = new ReadableStreamingData(compression.wrap(in, compressionType))) {
            return BlockUnparsed.PROTOBUF.parse(data);
        }
    }
}
