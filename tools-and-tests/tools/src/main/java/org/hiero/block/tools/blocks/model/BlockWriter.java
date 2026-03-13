// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.model;

import com.hedera.hapi.block.stream.Block;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
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
@SuppressWarnings({"DataFlowIssue", "unused"})
public class BlockWriter {

    /**
     * Abstraction over zip writing that supports both fresh zip files and resuming into an
     * existing partial zip.
     *
     * <p>Obtain instances via {@link BlockWriter#openZipForAppend(Path)}. Always close in a
     * try-with-resources or finally block so the Central Directory is flushed correctly.</p>
     *
     * <p>Two implementations exist:</p>
     * <ul>
     *   <li>{@link StreamBlockZipAppender} — backed by {@link ZipOutputStream}. Used for new
     *       (non-existing) zip files. Fast: writes entries directly without reading any existing
     *       state.</li>
     *   <li>{@link FsBlockZipAppender} — backed by a {@link java.nio.file.FileSystem} over the
     *       existing zip. Used when a partial zip is already present on disk (mid-zip resume).
     *       The ZipFileSystem reads the current Central Directory on open and rewrites it
     *       correctly on {@link #close()}, avoiding the {@link ZipOutputStream} APPEND-mode bug
     *       where the internal byte-offset counter starts at 0 regardless of existing file
     *       content.</li>
     * </ul>
     */
    public interface BlockZipAppender extends AutoCloseable {
        /**
         * Write one pre-serialized block entry into this zip.
         *
         * @param entryName the zip entry name (e.g., {@code "0000000000000000123.blk.zstd"})
         * @param bytes the pre-serialized, optionally compressed block bytes
         * @throws IOException if writing fails
         */
        void writeEntry(String entryName, byte[] bytes) throws IOException;

        /** Flush and close the zip, writing or updating the Central Directory. */
        @Override
        void close() throws IOException;
    }

    /**
     * Fast {@link BlockZipAppender} backed by a long-lived {@link ZipOutputStream}.
     * Used for new zip files where no prior entries exist.
     */
    private static final class StreamBlockZipAppender implements BlockZipAppender {
        private final ZipOutputStream zos;

        StreamBlockZipAppender(final Path zipFilePath) throws IOException {
            this.zos = openOrCreateZipFile(zipFilePath);
        }

        @Override
        public void writeEntry(final String entryName, final byte[] bytes) throws IOException {
            final CRC32 crc = new CRC32();
            crc.update(bytes);
            final ZipEntry entry = new ZipEntry(entryName);
            entry.setSize(bytes.length);
            entry.setCompressedSize(bytes.length);
            entry.setCrc(crc.getValue());
            zos.putNextEntry(entry);
            zos.write(bytes);
            zos.closeEntry();
        }

        @Override
        public void close() throws IOException {
            zos.close();
        }
    }

    /**
     * Resume-safe {@link BlockZipAppender} backed by a {@link java.nio.file.FileSystem} over an
     * existing zip file.
     *
     * <p>Used when the wrap command is resumed mid-zip: the ZipFileSystem reads the current
     * Central Directory on open and correctly rewrites it with all entries (old + new) on
     * {@link #close()}. Entries are written in STORED mode ({@code compressionMethod=STORED},
     * supported since JDK 16) to match the entries already in the file.</p>
     */
    private static final class FsBlockZipAppender implements BlockZipAppender {
        private final FileSystem zipFs;

        FsBlockZipAppender(final Path zipFilePath) throws IOException {
            // "compressionMethod" env key for jdk.nio.zipfs requires JDK 16+.
            this.zipFs = FileSystems.newFileSystem(zipFilePath, Map.of("compressionMethod", "STORED"));
        }

        @Override
        public void writeEntry(final String entryName, final byte[] bytes) throws IOException {
            Files.write(zipFs.getPath("/" + entryName), bytes);
        }

        @Override
        public void close() throws IOException {
            zipFs.close();
        }
    }

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

    /** The number of digits for zero-padded block numbers in file names */
    private static final int BLOCK_NUMBER_DIGITS = 19;
    /** The base extension for block files (without compression extension) */
    private static final String BLOCK_FILE_EXTENSION = ".blk";
    /** The number of block number digits per directory level (3 = 1000 directories per level) */
    private static final int DIGITS_PER_DIR = 3;
    /** The number of digits for zip file name selection (1 = 10 zip files per directory) */
    private static final int DIGITS_PER_ZIP_FILE_NAME = 1;
    /** Default number of blocks per zip file in powers of 10 (4 = 10,000 blocks per zip) */
    public static final int DEFAULT_POWERS_OF_TEN_PER_ZIP = 4;
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
    @SuppressWarnings("UnusedReturnValue")
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
        // get the block number from the block header
        final var firstBlockItem = block.items().getFirst();
        final long blockNumber = firstBlockItem.blockHeader().number();
        // compute a block path
        final BlockPath blockPath =
                computeBlockPath(baseDirectory, blockNumber, compressionType, powersOfTenPerZipFileContents);
        // create directories
        Files.createDirectories(blockPath.dirPath);
        // serialize block bytes first
        final byte[] blockBytes = serializeBlock(block, compressionType);
        // append a block to a zip file, creating a zip file if it doesn't exist
        // openZipForAppend uses FsBlockZipAppender for existing zips (correct CEN handling) and
        // StreamBlockZipAppender for new zips, so multiple blocks sharing the same zip are safe.
        try (final BlockZipAppender appender = openZipForAppend(blockPath.zipFilePath)) {
            appender.writeEntry(blockPath.blockFileName, blockBytes);
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
        final String blockNumStr = String.format("%0" + BLOCK_NUMBER_DIGITS + "d", blockNumber);
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
                    //noinspection OptionalIsPresent
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
                    return zipFilePath
                            .map(path -> minBlockNumberInZip(path, compressionType))
                            .orElse(-1L);
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
     * Returns the first block number of the zip-file range that contains {@code blockNumber}.
     *
     * <p>For example, with the default 10,000 blocks per zip ({@code powersOfTen=4}),
     * block 15,000 maps to zip-range start 10,000, and block 10,000 also maps to 10,000.
     * Used by the resume logic in
     * {@link org.hiero.block.tools.blocks.ToWrappedBlocksCommand} to detect a mid-zip
     * resume and back up to the zip boundary before rewriting.
     *
     * @param blockNumber the block number to query
     * @param powersOfTenPerZipFileContents number of blocks per zip in powers of 10 (default {@link #DEFAULT_POWERS_OF_TEN_PER_ZIP})
     * @return the first block number of the zip range containing {@code blockNumber}
     */
    public static long zipRangeFirstBlock(final long blockNumber, final int powersOfTenPerZipFileContents) {
        final long blocksPerZip = (long) Math.pow(10, powersOfTenPerZipFileContents);
        return (blockNumber / blocksPerZip) * blocksPerZip;
    }

    /**
     * Compute the path to a block file for the given archive type using default compression.
     *
     * <p>For {@link BlockArchiveType#UNCOMPRESSED_ZIP} this delegates to
     * {@link #computeBlockPath(Path, long)}. For {@link BlockArchiveType#INDIVIDUAL_FILES} it
     * computes the nested-directory path produced by {@code BlockFile.nestedDirectoriesBlockFilePath},
     * returning a {@link BlockPath} whose {@code zipFilePath} is the individual block file path and
     * whose {@code dirPath} is the file's parent directory.
     *
     * @param baseDirectory The base directory for the block files
     * @param blockNumber The block number
     * @param archiveType The archive type (UNCOMPRESSED_ZIP or INDIVIDUAL_FILES)
     * @return The path to the block file
     */
    public static BlockPath computeBlockPath(
            final Path baseDirectory, final long blockNumber, final BlockArchiveType archiveType) {
        return switch (archiveType) {
            case UNCOMPRESSED_ZIP -> computeBlockPath(baseDirectory, blockNumber);
            case INDIVIDUAL_FILES -> {
                final Path blockFilePath = BlockFile.nestedDirectoriesBlockFilePath(
                        baseDirectory, blockNumber, DEFAULT_COMPRESSION, DIGITS_PER_DIR);
                final String blockNumStr = String.format("%0" + BLOCK_NUMBER_DIGITS + "d", blockNumber);
                final String blockFileName = blockNumStr + BLOCK_FILE_EXTENSION + DEFAULT_COMPRESSION.extension();
                yield new BlockPath(
                        blockFilePath.getParent(), blockFilePath, blockNumStr, blockFileName, DEFAULT_COMPRESSION);
            }
        };
    }

    /**
     * Serialize and compress a block to bytes.
     *
     * <p>Exposed as a public method so that pipeline stages can serialize blocks concurrently
     * on a dedicated thread pool while the convert thread immediately moves on to the next block.
     *
     * @param block The block to serialize
     * @param compressionType The compression type (ZSTD or NONE)
     * @return The serialized (and optionally compressed) block bytes
     */
    public static byte[] serializeBlockToBytes(final Block block, final CompressionType compressionType) {
        return serializeBlock(block, compressionType);
    }

    /**
     * Open (or create) a zip file for sequential block writing, returning the appropriate
     * {@link BlockZipAppender} for the situation.
     *
     * <p>Two cases are handled automatically:</p>
     * <ul>
     *   <li><b>New zip</b> (file does not yet exist, or is empty): returns a
     *       {@link StreamBlockZipAppender} backed by a freshly created {@link ZipOutputStream}.
     *       This is the fast path used when writing a complete 10,000-block zip range in one
     *       uninterrupted pass.</li>
     *   <li><b>Existing partial zip</b> (file already has content from a previous run): returns a
     *       {@link FsBlockZipAppender} backed by a {@link java.nio.file.FileSystem} over the zip.
     *       The ZipFileSystem reads the existing Central Directory on open and rewrites it
     *       correctly with all entries (old + new) on {@link BlockZipAppender#close()}. This
     *       avoids the classic {@link ZipOutputStream} APPEND-mode bug: when
     *       {@code ZipOutputStream} is layered over an append-mode stream, its internal
     *       byte-offset counter starts at 0 regardless of the file's current size, causing the
     *       CEN to record wrong entry offsets and producing
     *       {@code "invalid CEN header (bad signature)"} errors on the next read.</li>
     * </ul>
     *
     * <p>The caller is responsible for keeping the returned {@link BlockZipAppender} open while
     * writing the current zip range and closing it when the block number crosses into the next
     * range (or at the end of processing), so the Central Directory is written exactly once.</p>
     *
     * @param zipFilePath the path to the zip file
     * @return a {@link BlockZipAppender} ready for writing
     * @throws IOException if an I/O error occurs, or if the existing zip file is corrupt and
     *     cannot be opened (run {@code blocks repair-zips} before resuming)
     */
    public static BlockZipAppender openZipForAppend(final Path zipFilePath) throws IOException {
        if (Files.exists(zipFilePath) && Files.size(zipFilePath) > 0) {
            // Resume into an existing partial zip: use ZipFileSystem so the CEN is updated
            // correctly without the ZipOutputStream APPEND-mode offset bug.
            try {
                return new FsBlockZipAppender(zipFilePath);
            } catch (IOException e) {
                throw new IOException(
                        "Cannot open partial zip for resume (it may be corrupt): " + zipFilePath
                                + ". Run 'blocks repair-zips' on the output directory and retry.",
                        e);
            }
        }
        // New zip: use the fast ZipOutputStream path.
        return new StreamBlockZipAppender(zipFilePath);
    }

    /**
     * Write a pre-serialized block entry into an already-open {@link BlockZipAppender}.
     *
     * <p>Delegates to {@link BlockZipAppender#writeEntry}, which handles CRC computation and
     * entry creation appropriate for the underlying implementation (stream or filesystem).
     * The caller is responsible for managing the appender lifecycle (opening and closing it).
     *
     * @param appender the open zip appender to write to
     * @param blockPath the block path record containing the block file name
     * @param blockBytes the pre-serialized (and optionally pre-compressed) block bytes
     * @throws IOException if an I/O error occurs
     */
    public static void writeBlockEntry(
            final BlockZipAppender appender, final BlockPath blockPath, final byte[] blockBytes) throws IOException {
        appender.writeEntry(blockPath.blockFileName(), blockBytes);
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
        final String blockNumberStr = String.format("%0" + BLOCK_NUMBER_DIGITS + "d", blockNumber);
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
     * Open or create a zip file for writing, always starting from byte offset zero.
     *
     * <p><b>Why TRUNCATE_EXISTING instead of APPEND:</b> {@link ZipOutputStream} maintains an
     * internal byte-offset counter starting at 0. When used with {@code APPEND} on an existing
     * file, the OS file position starts at the end of the file (offset N), but
     * {@code ZipOutputStream} still counts from 0. The Central Directory it writes on
     * {@link ZipOutputStream#close()} therefore records entry offsets relative to 0, while the
     * entries are physically at offsets ≥ N. Any standard zip reader will then follow those wrong
     * offsets and find garbage instead of local file headers, producing
     * {@code "invalid CEN header (bad signature)"} errors. Using {@code TRUNCATE_EXISTING}
     * ensures the stream always starts at file offset 0, keeping the internal counter in sync with
     * the actual file position.
     *
     * <p>This method is only used internally by {@link StreamBlockZipAppender} for <em>new</em>
     * zip files. For existing partial zips (mid-zip resume), {@link #openZipForAppend(Path)}
     * returns a {@link FsBlockZipAppender} instead, which handles the Central Directory correctly.
     *
     * @param zipFilePath the path to the zip file
     * @return a {@link ZipOutputStream} configured for STORED mode, positioned at byte 0
     * @throws IOException if an I/O error occurs
     */
    private static ZipOutputStream openOrCreateZipFile(final Path zipFilePath) throws IOException {
        // CREATE creates the file if absent; TRUNCATE_EXISTING resets an existing file to length 0
        // so that ZipOutputStream's internal offset counter (which always starts at 0) stays in
        // sync with the actual position in the file.  Using APPEND here was the original bug.
        final ZipOutputStream zipOutputStream = new ZipOutputStream(new BufferedOutputStream(
                Files.newOutputStream(zipFilePath, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING),
                1024 * 1024));
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
     */
    private static byte[] serializeBlock(final Block block, final CompressionType compressionType) {
        // PBJ toBytes() uses measureRecord() internally for exact allocation then writes once - no size guessing.
        // Compress the resulting byte[] in one shot to minimise JNI call overhead vs streaming through
        // ZstdOutputStream.
        return compressionType.compress(Block.PROTOBUF.toBytes(block).toByteArray());
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
}
