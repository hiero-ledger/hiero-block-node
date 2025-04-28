// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.blocks.files.historic;

import static org.hiero.block.node.base.BlockFile.blockNumberFromFile;
import static org.hiero.block.node.blocks.files.historic.BlockPath.computeBlockPath;
import static org.hiero.block.node.blocks.files.historic.BlockPath.computeExistingBlockPath;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.IntStream;
import java.util.zip.CRC32;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;
import org.hiero.block.node.base.BlockFile;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.historicalblocks.BlockAccessor;
import org.hiero.block.node.spi.historicalblocks.BlockAccessor.Format;
import org.hiero.block.node.spi.historicalblocks.HistoricalBlockFacility;

/**
 * The ZipBlockArchive class provides methods for creating and managing zip files containing blocks.
 * It allows for writing new zip files and accessing individual blocks within the zip files.
 */
class ZipBlockArchive {
    /** The logger for this class. */
    private final System.Logger LOGGER = System.getLogger(getClass().getName());
    /** The block node context. */
    private final BlockNodeContext context;
    /** The configuration for the historic files. */
    private final FilesHistoricConfig config;
    /** The historical block facility. */
    private final HistoricalBlockFacility historicalBlockFacility;
    /** The number of blocks per zip file. */
    private final int numberOfBlocksPerZipFile;
    /** The format for the blocks. */
    private final Format format;

    /**
     * Constructor for ZipBlockArchive.
     *
     * @param context The block node context
     * @param filesHistoricConfig Configuration to be used internally
     */
    ZipBlockArchive(@NonNull final BlockNodeContext context, @NonNull final FilesHistoricConfig filesHistoricConfig) {
        this.context = Objects.requireNonNull(context);
        this.config = Objects.requireNonNull(filesHistoricConfig);
        this.historicalBlockFacility = Objects.requireNonNull(context.historicalBlockProvider());
        numberOfBlocksPerZipFile = (int) Math.pow(10, this.config.powersOfTenPerZipFileContents());
        format = switch (this.config.compression()) {
            case ZSTD -> Format.ZSTD_PROTOBUF;
            case NONE -> Format.PROTOBUF;};
    }

    /**
     * Write a new zip file containing blocks, reads the batch of blocks from the HistoricalBlockFacility.
     *
     * @param firstBlockNumber The first block number to write
     * @throws IOException If an error occurs writing the block
     * @return A list of block accessors for the blocks written to the zip file, can be used to delete the blocks
     */
    List<BlockAccessor> writeNewZipFile(long firstBlockNumber) throws IOException {
        final long lastBlockNumber = firstBlockNumber + numberOfBlocksPerZipFile - 1;
        // compute block path
        final BlockPath firstBlockPath = computeBlockPath(config, firstBlockNumber);
        // create directories
        Files.createDirectories(firstBlockPath.dirPath());
        // create list for all block accessors, so we can delete files after we are done
        final List<BlockAccessor> blockAccessors = IntStream.rangeClosed((int) firstBlockNumber, (int) lastBlockNumber)
                .mapToObj(historicalBlockFacility::block)
                .toList();
        // create zip file path
        try (ZipOutputStream zipOutputStream = new ZipOutputStream(new BufferedOutputStream(
                Files.newOutputStream(
                        firstBlockPath.zipFilePath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE),
                1024 * 1204))) {
            // don't compress the zip file as files are already compressed
            zipOutputStream.setMethod(ZipOutputStream.STORED);
            // todo should we not also set the level to Deflater.NO_COMPRESSION
            for (long blockNumber = firstBlockNumber; blockNumber <= lastBlockNumber; blockNumber++) {
                // compute block filename
                // todo should we also not append the compression extension to the filename?
                // todo I feel like the accessor should generally be getting us the block file name
                //   what if the file is zstd compressed but the current runtime compression is none?
                //   then the file name would be wrong? For now appending, maybe a slight cleanup is in order for this
                //   logic.
                final String blockFileName = BlockFile.blockFileName(blockNumber, config.compression());
                // get block accessor
                final BlockAccessor blockAccessor = blockAccessors.get((int) (blockNumber - firstBlockNumber));
                // get the bytes to write, we have to do this as we need to know the size
                final Bytes bytes = blockAccessor.blockBytes(format);
                // calculate CRC-32 checksum
                CRC32 crc = new CRC32();
                crc.update(bytes.toByteArray());
                // create zip entry
                final ZipEntry zipEntry = new ZipEntry(blockFileName);
                zipEntry.setSize(bytes.length());
                zipEntry.setCompressedSize(bytes.length());
                zipEntry.setCrc(crc.getValue());
                zipOutputStream.putNextEntry(zipEntry);
                // write compressed block content
                bytes.writeTo(zipOutputStream);
                // close zip entry
                zipOutputStream.closeEntry();
            }
        }
        // return block accessors
        // todo should these accessors be returned? they were here because a delete
        //   was supposed to be called on them, but we have changed the approach
        return blockAccessors;
    }

    /**
     * Get a block accessor for a block number
     *
     * @param blockNumber The block number
     * @return The block accessor for the block number
     */
    BlockAccessor blockAccessor(long blockNumber) {
        try {
            // get existing block path or null if we cannot find it
            final BlockPath blockPath = computeExistingBlockPath(config, blockNumber);
            return blockPath == null ? null : new ZipBlockAccessor(blockPath);
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Finds the minimum block number in a directory structure of zip files.
     *
     * @return the minimum block number, or -1 if no block files are found
     */
    long minStoredBlockNumber() {
        // find the lowest block number first
        Path lowestPath = config.rootPath();
        while (lowestPath != null) {
            // get the first directory in the path
            try (var childFilesStream = Files.list(lowestPath)) {
                List<Path> childFiles = childFilesStream.toList();
                // check if we are a directory of directories
                final Optional<Path> min = childFiles.stream()
                        .filter(Files::isDirectory)
                        .min(Comparator.comparingLong(
                                path -> Long.parseLong(path.getFileName().toString())));
                if (min.isPresent()) {
                    lowestPath = min.get();
                } else {
                    // we are at the deepest directory, check for block files
                    final Optional<Path> zipFilePath = childFiles.stream()
                            .filter(Files::isRegularFile)
                            .filter(path -> path.getFileName().toString().endsWith(".zip"))
                            .min(Comparator.comparingLong(filePath -> {
                                String fileName = filePath.getFileName().toString();
                                return Long.parseLong(fileName.substring(0, fileName.indexOf('s')));
                            }));
                    if (zipFilePath.isPresent()) {
                        try (var zipFile = new ZipFile(zipFilePath.get().toFile())) {
                            return zipFile.stream()
                                    .mapToLong(entry -> blockNumberFromFile(entry.getName()))
                                    .min()
                                    .orElse(-1);
                        }
                    } else {
                        // no zip files found in min directory
                        return -1;
                    }
                }
            } catch (final Exception e) {
                LOGGER.log(System.Logger.Level.ERROR, "Error reading directory: " + lowestPath, e);
                context.serverHealth()
                        .shutdown(
                                ZipBlockArchive.class.getName(),
                                "Error reading directory: " + lowestPath + " because " + e.getMessage());
                lowestPath = null;
            }
        }
        return -1;
    }

    /**
     * Finds the maximum block number in a directory structure of zip files.
     *
     * @return the maximum block number, or -1 if no block files are found
     */
    long maxStoredBlockNumber() {
        // find the highest block number
        Path highestPath = config.rootPath();
        while (highestPath != null) {
            // get the first directory in the path
            try (var childFilesStream = Files.list(highestPath)) {
                List<Path> childFiles = childFilesStream.toList();
                // check if we are a directory of directories
                final Optional<Path> max = childFiles.stream()
                        .filter(Files::isDirectory)
                        .max(Comparator.comparingLong(
                                path -> Long.parseLong(path.getFileName().toString())));
                if (max.isPresent()) {
                    highestPath = max.get();
                } else {
                    // we are at the deepest directory, check for block files
                    final Optional<Path> zipFilePath = childFiles.stream()
                            .filter(Files::isRegularFile)
                            .filter(path -> path.getFileName().toString().endsWith(".zip"))
                            .max(Comparator.comparingLong(filePath -> {
                                String fileName = filePath.getFileName().toString();
                                return Long.parseLong(fileName.substring(0, fileName.indexOf('s')));
                            }));
                    if (zipFilePath.isPresent()) {
                        try (var zipFile = new ZipFile(zipFilePath.get().toFile())) {
                            return zipFile.stream()
                                    .mapToLong(entry -> blockNumberFromFile(entry.getName()))
                                    .max()
                                    .orElse(-1);
                        }
                    } else {
                        // no zip files found in max directory
                        return -1;
                    }
                }
            } catch (final Exception e) {
                LOGGER.log(System.Logger.Level.ERROR, "Error reading directory: " + highestPath, e);
                context.serverHealth()
                        .shutdown(
                                ZipBlockArchive.class.getName(),
                                "Error reading directory: " + highestPath + " because " + e.getMessage());
                highestPath = null;
            }
        }
        return -1;
    }
}
