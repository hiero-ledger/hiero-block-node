// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.blocks.files.historic;

import static org.hiero.block.node.base.BlockFile.BLOCK_FILE_EXTENSION;
import static org.hiero.block.node.base.BlockFile.blockNumberFormated;
import static org.hiero.block.node.base.BlockFile.blockNumberFromFile;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;
import org.hiero.block.common.utils.FileUtilities;
import org.hiero.block.node.base.BlockFile;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.historicalblocks.BlockAccessor;
import org.hiero.block.node.spi.historicalblocks.BlockAccessor.Format;
import org.hiero.block.node.spi.historicalblocks.HistoricalBlockFacility;

/**
 * The ZipBlockArchive class provides methods for creating and managing zip files containing blocks.
 * It allows for writing new zip files and accessing individual blocks within the zip files.
 */
public class ZipBlockArchive {
    /** The logger for this class. */
    private final System.Logger LOGGER = System.getLogger(getClass().getName());

    private final BlockNodeContext context;
    private final FilesHistoricConfig historicConfig;
    private final HistoricalBlockFacility historicalBlockFacility;
    private final int numberOfBlocksPerZipFile;
    private final Format format;

    /**
     * Constructor for ZipBlockArchive.
     *
     * @param context The block node context
     * @param historicConfig The configuration for the historic files
     */
    public ZipBlockArchive(BlockNodeContext context, FilesHistoricConfig historicConfig) {
        this.context = context;
        this.historicalBlockFacility = context.historicalBlockProvider();
        this.historicConfig = historicConfig;
        numberOfBlocksPerZipFile = (int) Math.pow(10, historicConfig.digitsPerZipFileName());
        format = switch (historicConfig.compression()) {
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
    public List<BlockAccessor> writeNewZipFile(long firstBlockNumber) throws IOException {
        final long lastBlockNumber = firstBlockNumber + numberOfBlocksPerZipFile - 1;
        // compute block path
        final BlockPath firstBlockPath = computeBlockPath(firstBlockNumber);
        // create directories
        Files.createDirectories(firstBlockPath.dirPath, FileUtilities.DEFAULT_FOLDER_PERMISSIONS);
        // create list for all block accessors, so we can delete files after we are done
        final List<BlockAccessor> blockAccessors = IntStream.rangeClosed((int) firstBlockNumber, (int) lastBlockNumber)
                .mapToObj(historicalBlockFacility::block)
                .toList();
        // create zip file path
        try (ZipOutputStream zipOutputStream = new ZipOutputStream(new BufferedOutputStream(
                Files.newOutputStream(firstBlockPath.zipFilePath, StandardOpenOption.CREATE, StandardOpenOption.WRITE),
                1024 * 1204))) {
            // don't compress the zip file as files are already compressed
            zipOutputStream.setMethod(ZipOutputStream.STORED);
            for (long blockNumber = firstBlockNumber; blockNumber < lastBlockNumber; blockNumber++) {
                // compute block filename
                final String blockFileName = BlockFile.blockFileName(blockNumber);
                // get block accessor
                final BlockAccessor blockAccessor = blockAccessors.get((int) (blockNumber - firstBlockNumber));
                // create zip entry
                zipOutputStream.putNextEntry(new ZipEntry(blockFileName));
                // write compressed block content
                blockAccessor.writeBytesTo(format, zipOutputStream);
                // close zip entry
                zipOutputStream.closeEntry();
            }
            zipOutputStream.putNextEntry(new ZipEntry(firstBlockPath.blockFileName));
            zipOutputStream.closeEntry();
        }
        // return block accessors
        return blockAccessors;
    }

    /**
     * Get a block accessor for a block number
     *
     * @param blockNumber The block number
     * @return The block accessor for the block number
     */
    public BlockAccessor blockAccessor(long blockNumber) {
        final BlockPath blockPath = computeBlockPath(blockNumber);
        if (Files.exists(blockPath.zipFilePath)) {
            return new ZipBlockAccessor(blockPath);
        }
        return null;
    }

    /**
     * Finds the minimum block number in a directory structure of zip files.
     *
     * @return the minimum block number, or -1 if no block files are found
     */
    public long minStoredBlockNumber() {
        // find the lowest block number first
        Path lowestPath = historicConfig.historicRootPath();
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
                                return Long.parseLong(fileName.substring(0, fileName.indexOf('.')));
                            }));
                    if (zipFilePath.isPresent()) {
                        try (var zipFile = new ZipFile(zipFilePath.get().toFile())) {
                            return zipFile.stream()
                                    .mapToLong(entry -> blockNumberFromFile(entry.getName()))
                                    .min()
                                    .orElse(-1);
                        } catch (IOException e) {
                            LOGGER.log(System.Logger.Level.ERROR, "Failed to read zip file", e);
                            context.serverHealth()
                                    .shutdown(
                                            ZipBlockArchive.class.getName(),
                                            "Error reading directory: " + lowestPath + " because " + e.getMessage());
                        }
                    } else {
                        // no zip files found in min directory
                        return -1;
                    }
                }
            } catch (Exception e) {
                LOGGER.log(System.Logger.Level.ERROR, "Error reading directory: " + lowestPath, e);
                context.serverHealth()
                        .shutdown(
                                ZipBlockArchive.class.getName(),
                                "Error reading directory: " + lowestPath + " because " + e.getMessage());
            }
        }
        return -1;
    }

    /**
     * Finds the maximum block number in a directory structure of zip files.
     *
     * @return the maximum block number, or -1 if no block files are found
     */
    public long maxStoredBlockNumber() {
        // find the highest block number
        Path highestPath = historicConfig.historicRootPath();
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
                                return Long.parseLong(fileName.substring(0, fileName.indexOf('.')));
                            }));
                    if (zipFilePath.isPresent()) {
                        try (var zipFile = new ZipFile(zipFilePath.get().toFile())) {
                            return zipFile.stream()
                                    .mapToLong(entry -> blockNumberFromFile(entry.getName()))
                                    .max()
                                    .orElse(-1);
                        } catch (IOException e) {
                            LOGGER.log(System.Logger.Level.ERROR, "Failed to read zip file", e);
                            context.serverHealth()
                                    .shutdown(
                                            ZipBlockArchive.class.getName(),
                                            "Error reading directory: " + highestPath + " because " + e.getMessage());
                        }
                    } else {
                        // no zip files found in max directory
                        return -1;
                    }
                }
            } catch (Exception e) {
                LOGGER.log(System.Logger.Level.ERROR, "Error reading directory: " + highestPath, e);
                context.serverHealth()
                        .shutdown(
                                ZipBlockArchive.class.getName(),
                                "Error reading directory: " + highestPath + " because " + e.getMessage());
            }
        }
        return -1;
    }

    /**
     * Compute the path to a block file
     *
     * @param blockNumber The block number
     * @return The path to the block file
     */
    private BlockPath computeBlockPath(long blockNumber) {
        // convert block number to string
        final String blockNumberStr = blockNumberFormated(blockNumber);
        // split string into digits for zip and for directories
        final int offsetToZip =
                blockNumberStr.length() - historicConfig.digitsPerZipFileName() - historicConfig.digitsPerDir();
        final String directoryDigits = blockNumberStr.substring(0, offsetToZip);
        final String zipFileNameDigits =
                blockNumberStr.substring(offsetToZip, offsetToZip + historicConfig.digitsPerZipFileName());
        // start building path to zip file
        Path dirPath = historicConfig.historicRootPath();
        for (int i = 0; i < directoryDigits.length(); i += historicConfig.digitsPerDir()) {
            final String dirName =
                    directoryDigits.substring(i, Math.min(i + historicConfig.digitsPerDir(), directoryDigits.length()));
            dirPath = dirPath.resolve(dirName);
        }
        // create zip file name
        final String zipFileName = zipFileNameDigits + "000s.zip";
        final String fileName = blockNumberStr
                + BLOCK_FILE_EXTENSION
                + historicConfig.compression().extension();
        return new BlockPath(dirPath, dirPath.resolve(zipFileName), blockNumberStr, fileName);
    }

    /**
     * Record for block path components
     *
     * @param dirPath The directory path for the directory that contains the zip file
     * @param zipFilePath The path to the zip file
     * @param blockNumStr The block number as a string
     * @param blockFileName The name of the block file in the zip file
     */
    public record BlockPath(Path dirPath, Path zipFilePath, String blockNumStr, String blockFileName) {}
}
