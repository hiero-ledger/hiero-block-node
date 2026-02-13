// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.blocks.files.historic;

import static java.lang.System.Logger.Level.ERROR;
import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.WARNING;
import static org.hiero.block.node.base.BlockFile.blockNumberFromFile;
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
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;
import java.util.zip.CRC32;
import java.util.zip.Deflater;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipOutputStream;
import org.hiero.block.node.base.BlockFile;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.historicalblocks.BlockAccessor;
import org.hiero.block.node.spi.historicalblocks.BlockAccessor.Format;
import org.hiero.block.node.spi.historicalblocks.BlockAccessorBatch;

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
    /** root path for temporary hard links to zip files */
    private final Path linksRootPath;
    /** The format for the blocks. */
    private final Format format;

    /**
     * Constructor for ZipBlockArchive.
     *
     * @param filesHistoricConfig Configuration to be used internally
     */
    ZipBlockArchive(@NonNull final BlockNodeContext context, @NonNull final FilesHistoricConfig filesHistoricConfig)
            throws IOException {
        this.context = Objects.requireNonNull(context);
        this.config = Objects.requireNonNull(filesHistoricConfig);
        linksRootPath = config.rootPath().resolve("links");
        format = switch (this.config.compression()) {
            case ZSTD -> Format.ZSTD_PROTOBUF;
            case NONE -> Format.PROTOBUF;
        };
    }

    /**
     * Write a new zip file containing the input batch of blocks.
     *
     * @throws IOException If an error occurs writing the block
     */
    void createZip(BlockAccessorBatch batch, Path destinationFile) throws IOException {
        try (final ZipOutputStream zipOutputStream = new ZipOutputStream(new BufferedOutputStream(
                Files.newOutputStream(destinationFile, StandardOpenOption.CREATE, StandardOpenOption.WRITE),
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
    }

    /**
     * Get a block accessor for a block number
     *
     * @param blockNumber The block number
     * @return The block accessor for the block number
     */
    BlockAccessor blockAccessor(long blockNumber) {
        try {
            // get existing block path or null if we cannot find it or create accessor for
            final BlockPath blockPath = computeExistingBlockPath(config, blockNumber);
            return blockPath == null ? null : new ZipBlockAccessor(blockPath, linksRootPath);
        } catch (final IOException e) {
            LOGGER.log(INFO, "Could not create zip block accessor", e);
            return null;
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
                        .filter(this::isNumericDirectory)
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
                                return Long.parseLong(fileName.substring(0, fileName.lastIndexOf('.')));
                            }));
                    if (zipFilePath.isPresent()) {
                        final Path candidateZip = zipFilePath.get();
                        try (final FileSystem zipFs = FileSystems.newFileSystem(candidateZip);
                                final Stream<Path> entries = Files.list(zipFs.getPath("/"))) {
                            return entries.mapToLong(entry -> blockNumberFromFile(entry.getFileName()))
                                    .min()
                                    .orElse(-1);
                        } catch (final ZipException zipException) {
                            if (handleCorruptedZipFile(candidateZip, zipException)) {
                                continue;
                            }
                            return -1;
                        }
                    } else {
                        // no zip files found in min directory
                        return -1;
                    }
                }
            } catch (final Exception e) {
                LOGGER.log(ERROR, "Error reading directory: " + lowestPath, e);

                final String shutdownMessage = "Error reading directory %s because %s.".formatted(lowestPath, e);

                context.serverHealth().shutdown(ZipBlockArchive.class.getName(), shutdownMessage);
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
                        .filter(this::isNumericDirectory)
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
                                return Long.parseLong(fileName.substring(0, fileName.lastIndexOf('.')));
                            }));
                    if (zipFilePath.isPresent()) {
                        final Path candidateZip = zipFilePath.get();
                        try (final FileSystem zipFs = FileSystems.newFileSystem(candidateZip);
                                final Stream<Path> entries = Files.list(zipFs.getPath("/"))) {
                            return entries.mapToLong(entry -> blockNumberFromFile(entry.getFileName()))
                                    .max()
                                    .orElse(-1);
                        } catch (final ZipException zipException) {
                            if (handleCorruptedZipFile(candidateZip, zipException)) {
                                continue;
                            }
                            return -1;
                        }
                    } else {
                        // no zip files found in max directory
                        return -1;
                    }
                }
            } catch (final Exception e) {
                LOGGER.log(ERROR, "Error reading directory: %s".formatted(highestPath), e);
                final String shutdownMessage =
                        "Error reading directory: %s because %s".formatted(highestPath, e.getMessage());
                context.serverHealth().shutdown(ZipBlockArchive.class.getName(), shutdownMessage);
                highestPath = null;
            }
        }
        return -1;
    }

    Optional<Path> minStoredArchive() throws IOException {
        return findMinArchive(config.rootPath());
    }

    private Optional<Path> findMinArchive(Path currentPath) throws IOException {
        try (Stream<Path> childrenStream = Files.list(currentPath)) {
            List<Path> children = childrenStream.toList();

            // Get numeric directories sorted by their numeric value (minimum first)
            List<Path> numericDirs = children.stream()
                    .filter(this::isNumericDirectory)
                    .sorted(Comparator.comparingLong(
                            path -> Long.parseLong(path.getFileName().toString())))
                    .toList();

            // Get zip files sorted by their numeric value (minimum first)
            List<Path> zipFiles = children.stream()
                    .filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().endsWith(".zip"))
                    .filter(path -> isNumericDirectory(path.getParent()))
                    .sorted(Comparator.comparingLong(filePath -> {
                        String fileName = filePath.getFileName().toString();
                        return Long.parseLong(fileName.substring(0, fileName.lastIndexOf('.')));
                    }))
                    .toList();

            // If this directory has zip files, return the minimum
            if (!zipFiles.isEmpty()) {
                return Optional.of(zipFiles.getFirst());
            }

            // Otherwise, recursively search numeric subdirectories in order (minimum first)
            for (Path dir : numericDirs) {
                Optional<Path> result = findMinArchive(dir);
                if (result.isPresent()) {
                    return result;
                }
                // Directory (and its subtree) was empty, continue to next sibling
            }

            return Optional.empty();
        }
    }

    long count() {
        try (Stream<Path> pathStream = Files.walk(config.rootPath())) {
            return pathStream
                    .filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().endsWith(".zip"))
                    .filter(path -> isNumericDirectory(path.getParent()))
                    .count();
        } catch (final Exception e) {
            LOGGER.log(ERROR, "Error walking directory structure to count zip files", e);
            final String shutdownMessage =
                    "Error walking directory structure to count zip files because %s".formatted(e.getMessage());
            context.serverHealth().shutdown(ZipBlockArchive.class.getName(), shutdownMessage);
            return 0;
        }
    }

    /**
     * Attempt to quarantine a corrupted zip file and decide whether processing can continue.
     *
     * @param corruptedZip the path to the corrupted zip file
     * @param cause the {@link ZipException} that was thrown when attempting to read the file
     * @return {@code true} if the file was successfully moved out of the active path, {@code false} otherwise
     */
    private boolean handleCorruptedZipFile(final Path corruptedZip, final ZipException cause) {
        final String warningMessage =
                "Detected corrupted zip file: %s, attempting self-healing move".formatted(corruptedZip);
        LOGGER.log(WARNING, warningMessage, cause);
        try {
            final Path relativeZipPath = config.rootPath().relativize(corruptedZip);
            final Path quarantinedZip = config.rootPath().resolve("corrupted").resolve(relativeZipPath);
            Files.createDirectories(quarantinedZip.getParent());
            Files.move(corruptedZip, quarantinedZip, StandardCopyOption.REPLACE_EXISTING);
            LOGGER.log(INFO, "Moved corrupted zip file to quarantine: {0} -> {1}", corruptedZip, quarantinedZip);
            return true;
        } catch (final IOException deletionException) {
            LOGGER.log(ERROR, "Failed to move corrupted zip file: %s".formatted(corruptedZip), deletionException);
            String shutdownMessage =
                    "Unable to move corrupted zip file: %s because %s".formatted(corruptedZip, deletionException);
            context.serverHealth().shutdown(ZipBlockArchive.class.getName(), shutdownMessage);
            return false;
        }
    }

    /**
     * Checks whether the provided path represents a directory that follows the numeric naming scheme used for block
     * hierarchies.
     *
     * @param candidate the path to inspect
     * @return {@code true} if the directory name is purely numeric, {@code false} otherwise
     */
    private boolean isNumericDirectory(final Path candidate) {
        if (!Files.isDirectory(candidate)) {
            return false;
        }
        final String name = candidate.getFileName().toString();
        return !name.isEmpty() && name.chars().allMatch(Character::isDigit);
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
