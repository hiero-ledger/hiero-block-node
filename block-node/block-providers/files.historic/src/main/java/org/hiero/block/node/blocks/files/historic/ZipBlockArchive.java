// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.blocks.files.historic;

import static java.lang.System.Logger.Level.ERROR;
import static java.lang.System.Logger.Level.WARNING;
import static org.hiero.block.node.base.BlockFile.blockNumberFromFile;
import static org.hiero.block.node.blocks.files.historic.BlockPath.computeBlockPath;
import static org.hiero.block.node.blocks.files.historic.BlockPath.computeExistingBlockPath;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;
import java.util.zip.CRC32;
import java.util.zip.Deflater;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.hiero.block.node.base.BlockFile;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.historicalblocks.BlockAccessor;
import org.hiero.block.node.spi.historicalblocks.BlockAccessor.Format;

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
    /** The format for the blocks. */
    private final Format format;

    /**
     * Constructor for ZipBlockArchive.
     *
     * @param filesHistoricConfig Configuration to be used internally
     */
    ZipBlockArchive(@NonNull final BlockNodeContext context, @NonNull final FilesHistoricConfig filesHistoricConfig) {
        this.context = Objects.requireNonNull(context);
        this.config = Objects.requireNonNull(filesHistoricConfig);
        format = switch (this.config.compression()) {
            case ZSTD -> Format.ZSTD_PROTOBUF;
            case NONE -> Format.PROTOBUF;
        };
    }

    /**
     * Write a new zip file containing the input batch of blocks.
     *
     * @return The size of the zip file created
     * @throws IOException If an error occurs writing the block
     */
    long writeNewZipFile(List<BlockAccessor> batch) throws IOException {
        // compute zip path
        final BlockPath firstBlockPath =
                computeBlockPath(config, batch.getFirst().blockNumber());
        // create directories
        Files.createDirectories(firstBlockPath.dirPath());
        // create zip file
        final Path zipFilePath = firstBlockPath.zipFilePath();
        try (final ZipOutputStream zipOutputStream = new ZipOutputStream(new BufferedOutputStream(
                Files.newOutputStream(zipFilePath, StandardOpenOption.CREATE, StandardOpenOption.WRITE),
                1024 * 1204))) {
            // don't compress the zip file as files are already compressed
            zipOutputStream.setMethod(ZipOutputStream.STORED);
            zipOutputStream.setLevel(Deflater.NO_COMPRESSION);
            for (final BlockAccessor blockAccessor : batch) {
                // compute block filename
                final String blockFileName = BlockFile.blockFileName(blockAccessor.blockNumber(), config.compression());
                // get the bytes to write, we have to do this as we need to know the size
                // it is here possible that the accessor will no longer be able to access the block
                // because it is possible that the block has been deleted or has been moved
                final Bytes bytes = blockAccessor.blockBytes(format);
                // calculate CRC-32 checksum
                final CRC32 crc = new CRC32();
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
        } catch (final UncheckedIOException e) {
            // adding this because we are potentially throwing an unchecked
            // io exception, if that is the case, we want to throw the cause
            // which is expected, but if that is not the case, that may indicate
            // we have an issue that we should look at
            throw e.getCause();
        }
        // if we have reached here, this means that the zip file was created
        // successfully
        // return the size of the zip file created
        return Files.size(zipFilePath);
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
                        .filter(path -> !path.equals(config.stagingPath()))
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
                        try (final FileSystem zipFs = FileSystems.newFileSystem(zipFilePath.get());
                                final Stream<Path> entries = Files.list(zipFs.getPath("/"))) {
                            return entries.mapToLong(entry -> blockNumberFromFile(entry.getFileName()))
                                    .min()
                                    .orElse(-1);
                        }
                    } else {
                        // no zip files found in min directory
                        return -1;
                    }
                }
            } catch (final Exception e) {
                LOGGER.log(ERROR, "Error reading directory: " + lowestPath, e);
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
                        .filter(path -> !path.equals(config.stagingPath()))
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
                        try (final FileSystem zipFs = FileSystems.newFileSystem(zipFilePath.get());
                                final Stream<Path> entries = Files.list(zipFs.getPath("/"))) {
                            return entries.mapToLong(entry -> blockNumberFromFile(entry.getFileName()))
                                    .max()
                                    .orElse(-1);
                        }
                    } else {
                        // no zip files found in max directory
                        return -1;
                    }
                }
            } catch (final Exception e) {
                LOGGER.log(ERROR, "Error reading directory: " + highestPath, e);
                context.serverHealth()
                        .shutdown(
                                ZipBlockArchive.class.getName(),
                                "Error reading directory: " + highestPath + " because " + e.getMessage());
                highestPath = null;
            }
        }
        return -1;
    }

    /**
     * Calculates the total size of all stored zip files in the archive.
     * <p>
     * WARNING: This method walks through the directory structure starting from
     * the root path and sums the sizes of all zip files found. this might become
     * expensive if there are many zip large files, as will become overtime.
     * This method should only be used at startup, and eventually should be
     * replaced by a more efficient way to track the size of the zip files.
     *
     * @return The total bytes stored in all zip files
     */
    long calculateTotalStoredBytes() {
        // todo(1249), optimizations or even making it non-blocking are needed here.
        try (Stream<Path> pathStream = Files.walk(config.rootPath())) {
            return pathStream
                    .filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().endsWith(".zip"))
                    .mapToLong(file -> {
                        try {
                            return Files.size(file);
                        } catch (IOException e) {
                            LOGGER.log(WARNING, "Failed to get size of file: " + file, e);
                            return 0;
                        }
                    })
                    .sum();
        } catch (IOException e) {
            LOGGER.log(ERROR, "Error walking directory structure to calculate total bytes stored", e);
            return 0;
        }
    }
}
