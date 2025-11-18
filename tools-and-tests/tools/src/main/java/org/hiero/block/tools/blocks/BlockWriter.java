// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks;

import com.hedera.hapi.block.stream.Block;
import com.hedera.pbj.runtime.io.stream.WritableStreamingData;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import java.util.zip.CRC32;
import java.util.zip.Deflater;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.hiero.block.node.base.BlockFile;
import org.hiero.block.node.base.CompressionType;

/**
 * Utility class for writing blocks to disk in the Hiero Block Node historic files format.
 *
 * <h2>Storage Format</h2>
 * <p>This class writes blocks using the same format as the {@code BlockFileHistoricPlugin}. The format
 * organizes blocks in a hierarchical directory structure with zip file archives.</p>
 *
 * <h3>Directory Structure</h3>
 * <p>Block numbers are formatted as 19-digit zero-padded strings (e.g., block 123 becomes "0000000000000000123").
 * These digits are split into directory levels and zip file names:</p>
 *
 * <ul>
 *   <li><b>Directory Levels:</b> Every 3 digits creates one directory level (1000 subdirectories per level)</li>
 *   <li><b>Zip File Selection:</b> 1 digit selects which zip file (10 zip files per bottom-level directory)</li>
 *   <li><b>Blocks per Zip:</b> Configurable via {@code powersOfTenPerZipFileContents} (default 4 = 10,000 blocks per zip)</li>
 * </ul>
 *
 * <h3>Path Example</h3>
 * <p>For block number 1,234,567,890,123,456,789 with default settings (powersOfTenPerZipFileContents=4):</p>
 * <pre>
 * Block number: 1234567890123456789
 * Formatted:    0001234567890123456789 (19 digits)
 * Split:        000/123/456/789/012/345/6789  (directory structure)
 * Directory:    baseDir/000/123/456/789/012/345/
 * Zip file:     60000s.zip                     (6 = digit at position, 0000 = 4 zeros for 10K blocks)
 * Block file:   0001234567890123456789.blk.zstd (inside zip)
 * </pre>
 *
 * <h3>Zip File Format</h3>
 * <ul>
 *   <li><b>Compression:</b> Individual block files are compressed (ZSTD or NONE)</li>
 *   <li><b>Zip Method:</b> STORED (no additional zip-level compression)</li>
 *   <li><b>Naming:</b> {digit}{zeros}s.zip (e.g., "00000s.zip", "10000s.zip" for 10K blocks/zip)</li>
 *   <li><b>Contents:</b> Multiple .blk.zstd (or .blk) files, one per block</li>
 * </ul>
 *
 * <h3>Block File Format</h3>
 * <ul>
 *   <li><b>File name:</b> {19-digit-block-number}.blk{compression-extension}</li>
 *   <li><b>With ZSTD:</b> 0000000000000000123.blk.zstd</li>
 *   <li><b>With NONE:</b> 0000000000000000123.blk</li>
 *   <li><b>Content:</b> Protobuf-serialized Block, optionally ZSTD compressed</li>
 * </ul>
 *
 * <h3>Configuration Compatibility</h3>
 * <p>This writer uses the same defaults as {@code FilesHistoricConfig}:</p>
 * <ul>
 *   <li><b>Compression:</b> ZSTD (default)</li>
 *   <li><b>Powers of Ten:</b> 4 (10,000 blocks per zip file, default)</li>
 *   <li><b>Digits per Directory:</b> 3 (1,000 subdirectories per level)</li>
 *   <li><b>Zip File Name Digits:</b> 1 (10 zip files per directory)</li>
 * </ul>
 */
@SuppressWarnings("DataFlowIssue")
public class BlockWriter {

    /**
     * Record for block path components.
     *
     * @param dirPath The directory path for the directory that contains the zip file
     * @param zipFilePath The full path to the zip file
     * @param blockNumStr The block number as a 19-digit zero-padded string
     * @param blockFileName The name of the block file inside the zip file (e.g., "0000000000000000123.blk.zstd")
     * @param compressionType The compression type used for the block file
     */
    public record BlockPath(
            Path dirPath,
            Path zipFilePath,
            String blockNumStr,
            String blockFileName,
            CompressionType compressionType) {}

    /** The format for block numbers in file names (19 digits, zero-padded) */
    private static final NumberFormat BLOCK_NUMBER_FORMAT = new DecimalFormat("0000000000000000000");
    /** The base extension for block files (without compression extension) */
    private static final String BLOCK_FILE_EXTENSION = ".blk";
    /** The number of block number digits per directory level (3 = 1000 directories per level) */
    private static final int DIGITS_PER_DIR = 3;
    /** The number of digits for zip file name selection (1 = 10 zip files per directory) */
    private static final int DIGITS_PER_ZIP_FILE_NAME = 1;
    /** Default number of blocks per zip file in powers of 10 (4 = 10,000 blocks per zip) */
    private static final int DEFAULT_POWERS_OF_TEN_PER_ZIP = 4;
    /** Default compression type to match FilesHistoricConfig default */
    public static final CompressionType DEFAULT_COMPRESSION = CompressionType.ZSTD;

    /**
     * Write a block using default settings (ZSTD compression, UNCOMPRESSED_ZIP archive type, 10,000 blocks per zip).
     *
     * @param baseDirectory The base directory for the block files
     * @param block The block to write
     * @return The path to the block file
     * @throws IOException If an error occurs writing the block
     */
    public static BlockPath writeBlock(final Path baseDirectory, final Block block) throws IOException {
        return writeBlock(
                baseDirectory,
                block,
                BlockArchiveType.UNCOMPRESSED_ZIP,
                DEFAULT_COMPRESSION,
                DEFAULT_POWERS_OF_TEN_PER_ZIP);
    }

    /**
     * Write a block with specified archive type using default settings (ZSTD compression, 10,000 blocks per zip if using UNCOMPRESSED_ZIP).
     *
     * @param baseDirectory The base directory for the block files
     * @param block The block to write
     * @param archiveType The archive type (UNCOMPRESSED_ZIP or INDIVIDUAL_FILES)
     * @return The path to the block file
     * @throws IOException If an error occurs writing the block
     */
    public static BlockPath writeBlock(final Path baseDirectory, final Block block, final BlockArchiveType archiveType)
            throws IOException {
        return writeBlock(baseDirectory, block, archiveType, DEFAULT_COMPRESSION, DEFAULT_POWERS_OF_TEN_PER_ZIP);
    }

    /**
     * Write a block with specified archive type and custom settings.
     *
     * @param baseDirectory The base directory for the block files
     * @param block The block to write
     * @param archiveType The archive type (UNCOMPRESSED_ZIP or INDIVIDUAL_FILES)
     * @param compressionType The compression type to use (ZSTD or NONE)
     * @param powersOfTenPerZipFileContents The number of blocks per zip in powers of 10 (1-6: 10, 100, 1K, 10K, 100K, 1M) - only used for UNCOMPRESSED_ZIP
     * @return The path to the block file
     * @throws IOException If an error occurs writing the block
     */
    public static BlockPath writeBlock(
            final Path baseDirectory,
            final Block block,
            final BlockArchiveType archiveType,
            final CompressionType compressionType,
            final int powersOfTenPerZipFileContents)
            throws IOException {
        return switch (archiveType) {
            case UNCOMPRESSED_ZIP ->
                writeBlockToZip(baseDirectory, block, compressionType, powersOfTenPerZipFileContents);
            case INDIVIDUAL_FILES -> writeBlockAsIndividualFile(baseDirectory, block, compressionType);
        };
    }

    /**
     * Write a block to a zip file with custom settings (legacy method for backward compatibility).
     *
     * @param baseDirectory The base directory for the block files
     * @param block The block to write
     * @param compressionType The compression type to use (ZSTD or NONE)
     * @param powersOfTenPerZipFileContents The number of blocks per zip in powers of 10 (1-6: 10, 100, 1K, 10K, 100K, 1M)
     * @return The path to the block file
     * @throws IOException If an error occurs writing the block
     */
    public static BlockPath writeBlock(
            final Path baseDirectory,
            final Block block,
            final CompressionType compressionType,
            final int powersOfTenPerZipFileContents)
            throws IOException {
        return writeBlockToZip(baseDirectory, block, compressionType, powersOfTenPerZipFileContents);
    }

    /**
     * Write a block to a zip file with custom settings.
     *
     * @param baseDirectory The base directory for the block files
     * @param block The block to write
     * @param compressionType The compression type to use (ZSTD or NONE)
     * @param powersOfTenPerZipFileContents The number of blocks per zip in powers of 10 (1-6: 10, 100, 1K, 10K, 100K, 1M)
     * @return The path to the block file
     * @throws IOException If an error occurs writing the block
     */
    private static BlockPath writeBlockToZip(
            final Path baseDirectory,
            final Block block,
            final CompressionType compressionType,
            final int powersOfTenPerZipFileContents)
            throws IOException {
        // get block number from block header
        final var firstBlockItem = block.items().getFirst();
        final long blockNumber = firstBlockItem.blockHeader().number();
        // compute a block path
        final BlockPath blockPath =
                computeBlockPath(baseDirectory, blockNumber, compressionType, powersOfTenPerZipFileContents);
        // create directories
        Files.createDirectories(blockPath.dirPath);
        // append a block to a zip file, creating a zip file if it doesn't exist
        try (final ZipOutputStream zipOutputStream = openOrCreateZipFile(blockPath.zipFilePath)) {
            // calculate CRC-32 checksum and get bytes
            final byte[] blockBytes = serializeBlock(block, compressionType);
            final CRC32 crc = new CRC32();
            crc.update(blockBytes);
            // create zip entry
            final ZipEntry zipEntry = new ZipEntry(blockPath.blockFileName);
            zipEntry.setSize(blockBytes.length);
            zipEntry.setCompressedSize(blockBytes.length);
            zipEntry.setCrc(crc.getValue());
            zipOutputStream.putNextEntry(zipEntry);
            // write compressed block content
            zipOutputStream.write(blockBytes);
            // close zip entry
            zipOutputStream.closeEntry();
        }
        // return block path
        return blockPath;
    }

    /**
     * Write a block as an individual file in a nested directory structure.
     *
     * @param baseDirectory The base directory for the block files
     * @param block The block to write
     * @param compressionType The compression type to use (ZSTD or NONE)
     * @return The path to the block file
     * @throws IOException If an error occurs writing the block
     */
    private static BlockPath writeBlockAsIndividualFile(
            final Path baseDirectory, final Block block, final CompressionType compressionType) throws IOException {
        // get block number from block header
        final var firstBlockItem = block.items().getFirst();
        final long blockNumber = firstBlockItem.blockHeader().number();
        // use BlockFile utility to compute nested directory path (3 digits per directory level)
        final Path blockFilePath =
                BlockFile.nestedDirectoriesBlockFilePath(baseDirectory, blockNumber, compressionType, DIGITS_PER_DIR);
        // create parent directories
        Files.createDirectories(blockFilePath.getParent());
        // serialize and compress the block
        final byte[] blockBytes = serializeBlock(block, compressionType);
        // write the compressed bytes to file
        Files.write(blockFilePath, blockBytes);
        // return a BlockPath record for consistency
        final String blockNumStr = BLOCK_NUMBER_FORMAT.format(blockNumber);
        final String blockFileName = blockNumStr + BLOCK_FILE_EXTENSION + compressionType.extension();
        return new BlockPath(blockFilePath.getParent(), blockFilePath, blockNumStr, blockFileName, compressionType);
    }

    /**
     * Get the highest block number stored in a directory structure.
     *
     * @param baseDirectory The base directory for the block files
     * @param compressionType The compression type to search for
     * @return The highest block number, or -1 if no blocks are found
     */
    public static long maxStoredBlockNumber(final Path baseDirectory, final CompressionType compressionType) {
        // find the highest block number
        Path highestPath = baseDirectory;
        while (highestPath != null) {
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
                    // we are at the deepest directory, check for zip files
                    final Optional<Path> zipFilePath = childFiles.stream()
                            .filter(Files::isRegularFile)
                            .filter(path -> path.getFileName().toString().endsWith(".zip"))
                            .max(Comparator.comparingLong(filePath -> {
                                String fileName = filePath.getFileName().toString();
                                return Long.parseLong(fileName.substring(0, fileName.indexOf('s')));
                            }));
                    if (zipFilePath.isPresent()) {
                        return maxBlockNumberInZip(zipFilePath.get(), compressionType);
                    } else {
                        return -1;
                    }
                }
            } catch (final Exception e) {
                return -1;
            }
        }
        return -1;
    }

    /**
     * Get the lowest block number stored in a directory structure.
     *
     * @param baseDirectory The base directory for the block files
     * @param compressionType The compression type to search for
     * @return The lowest block number, or -1 if no blocks are found
     */
    public static long minStoredBlockNumber(final Path baseDirectory, final CompressionType compressionType) {
        // find the lowest block number
        Path lowestPath = baseDirectory;
        while (lowestPath != null) {
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
                    // we are at the deepest directory, check for zip files
                    final Optional<Path> zipFilePath = childFiles.stream()
                            .filter(Files::isRegularFile)
                            .filter(path -> path.getFileName().toString().endsWith(".zip"))
                            .min(Comparator.comparingLong(filePath -> {
                                String fileName = filePath.getFileName().toString();
                                return Long.parseLong(fileName.substring(0, fileName.indexOf('s')));
                            }));
                    if (zipFilePath.isPresent()) {
                        return minBlockNumberInZip(zipFilePath.get(), compressionType);
                    } else {
                        return -1;
                    }
                }
            } catch (final Exception e) {
                return -1;
            }
        }
        return -1;
    }

    /**
     * Compute the path to a block file using default settings.
     *
     * @param baseDirectory The base directory for the block files
     * @param blockNumber The block number
     * @return The path to the block file
     */
    public static BlockPath computeBlockPath(final Path baseDirectory, final long blockNumber) {
        return computeBlockPath(baseDirectory, blockNumber, DEFAULT_COMPRESSION, DEFAULT_POWERS_OF_TEN_PER_ZIP);
    }

    /**
     * Compute the path to a block file with custom settings.
     *
     * @param baseDirectory The base directory for the block files
     * @param blockNumber The block number
     * @param compressionType The compression type (ZSTD or NONE)
     * @param powersOfTenPerZipFileContents The number of blocks per zip in powers of 10 (1-6)
     * @return The path to the block file
     */
    public static BlockPath computeBlockPath(
            final Path baseDirectory,
            final long blockNumber,
            final CompressionType compressionType,
            final int powersOfTenPerZipFileContents) {
        // convert block number to string
        final String blockNumberStr = BLOCK_NUMBER_FORMAT.format(blockNumber);
        // split string into digits for zip and for directories
        final int offsetToZip = blockNumberStr.length() - DIGITS_PER_ZIP_FILE_NAME - powersOfTenPerZipFileContents;
        final String directoryDigits = blockNumberStr.substring(0, offsetToZip);
        final String zipFileNameDigits = blockNumberStr.substring(offsetToZip, offsetToZip + DIGITS_PER_ZIP_FILE_NAME);
        // start building a path to a zip file
        Path dirPath = baseDirectory;
        for (int i = 0; i < directoryDigits.length(); i += DIGITS_PER_DIR) {
            final String dirName = directoryDigits.substring(i, Math.min(i + DIGITS_PER_DIR, directoryDigits.length()));
            dirPath = dirPath.resolve(dirName);
        }
        // create a zip file name
        final String zipFileName = zipFileNameDigits + "0".repeat(powersOfTenPerZipFileContents) + "s.zip";
        final Path zipFilePath = dirPath.resolve(zipFileName);
        // create the block file name
        final String fileName = blockNumberStr + BLOCK_FILE_EXTENSION + compressionType.extension();
        return new BlockPath(dirPath, zipFilePath, blockNumberStr, fileName, compressionType);
    }

    /**
     * Open an existing zip file or create a new one for appending blocks.
     *
     * @param zipFilePath The path to the zip file
     * @return A ZipOutputStream configured for STORED mode
     * @throws IOException If an error occurs
     */
    private static ZipOutputStream openOrCreateZipFile(final Path zipFilePath) throws IOException {
        final ZipOutputStream zipOutputStream;
        if (Files.exists(zipFilePath)) {
            // open existing zip for append - need to copy and recreate
            // for simplicity, we'll use FileSystem approach
            final FileSystem fs = FileSystems.newFileSystem(zipFilePath, (ClassLoader) null);
            // close and reopen for writing
            fs.close();
        }
        // create new or overwrite
        zipOutputStream = new ZipOutputStream(new BufferedOutputStream(
                Files.newOutputStream(zipFilePath, StandardOpenOption.CREATE, StandardOpenOption.APPEND), 1024 * 1024));
        // don't compress the zip file as files are already compressed
        zipOutputStream.setMethod(ZipOutputStream.STORED);
        zipOutputStream.setLevel(Deflater.NO_COMPRESSION);
        return zipOutputStream;
    }

    /**
     * Serialize a block to bytes with optional compression.
     *
     * @param block The block to serialize
     * @param compressionType The compression type
     * @return The serialized bytes
     * @throws IOException If an error occurs
     */
    private static byte[] serializeBlock(final Block block, final CompressionType compressionType) throws IOException {
        final java.io.ByteArrayOutputStream byteStream = new java.io.ByteArrayOutputStream();
        try (final WritableStreamingData out = new WritableStreamingData(compressionType.wrapStream(byteStream))) {
            Block.PROTOBUF.write(block, out);
        }
        return byteStream.toByteArray();
    }

    /**
     * Find the maximum block number in a zip file.
     *
     * @param zipFilePath The path to the zip file
     * @param compressionType The compression type to search for
     * @return The maximum block number, or -1 if none found
     */
    private static long maxBlockNumberInZip(final Path zipFilePath, final CompressionType compressionType) {
        try (final FileSystem zipFs = FileSystems.newFileSystem(zipFilePath);
                final Stream<Path> entries = Files.list(zipFs.getPath("/"))) {
            final String extension = BLOCK_FILE_EXTENSION + compressionType.extension();
            return entries.filter(path -> path.getFileName().toString().endsWith(extension))
                    .mapToLong(BlockWriter::blockNumberFromFile)
                    .max()
                    .orElse(-1);
        } catch (final IOException e) {
            return -1;
        }
    }

    /**
     * Find the minimum block number in a zip file.
     *
     * @param zipFilePath The path to the zip file
     * @param compressionType The compression type to search for
     * @return The minimum block number, or -1 if none found
     */
    private static long minBlockNumberInZip(final Path zipFilePath, final CompressionType compressionType) {
        try (final FileSystem zipFs = FileSystems.newFileSystem(zipFilePath);
                final Stream<Path> entries = Files.list(zipFs.getPath("/"))) {
            final String extension = BLOCK_FILE_EXTENSION + compressionType.extension();
            return entries.filter(path -> path.getFileName().toString().endsWith(extension))
                    .mapToLong(BlockWriter::blockNumberFromFile)
                    .min()
                    .orElse(-1);
        } catch (final IOException e) {
            return -1;
        }
    }

    /**
     * Extract the block number from a file path.
     *
     * @param file The file path
     * @return The block number
     */
    private static long blockNumberFromFile(final Path file) {
        final String fileName = file.getFileName().toString();
        return Long.parseLong(fileName.substring(0, fileName.indexOf('.')));
    }

    /**
     * Simple main method to test the block path computation.
     *
     * @param args The command line arguments
     */
    public static void main(String[] args) {
        System.out.println("Testing BlockWriter path computation with default settings (ZSTD, 10K blocks/zip):\n");
        for (long blockNumber : new long[] {0, 123, 1000, 10000, 100000, 1234567890123456789L}) {
            final var blockPath = computeBlockPath(Path.of("data"), blockNumber);
            System.out.println("Block " + blockNumber + ":");
            System.out.println("  Dir:      " + blockPath.dirPath);
            System.out.println("  Zip:      " + blockPath.zipFilePath.getFileName());
            System.out.println("  File:     " + blockPath.blockFileName);
            System.out.println();
        }
    }
}
