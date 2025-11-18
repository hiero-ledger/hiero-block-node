// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks;

import com.hedera.hapi.block.stream.Block;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.stream.ReadableStreamingData;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import org.hiero.block.node.base.BlockFile;
import org.hiero.block.node.base.CompressionType;

/**
 * Class for reading blocks from disk as written by {@link org.hiero.block.tools.blocks.BlockWriter}.
 *
 * <p>This reader automatically detects the storage format used by BlockWriter:
 * <ul>
 *   <li><b>INDIVIDUAL_FILES:</b> Blocks stored as individual files in nested directories</li>
 *   <li><b>UNCOMPRESSED_ZIP:</b> Blocks stored in uncompressed zip files</li>
 * </ul>
 *
 * <p>The reader also auto-detects compression (ZSTD or NONE) based on file extensions.
 *
 * <p>Format detection is cached per base directory for performance when reading multiple blocks.
 */
public class BlockReader {
    /** The number of block number digits per directory level (3 = 1000 directories per level) */
    private static final int DIGITS_PER_DIR = 3;

    /**
     * Cache of detected storage formats per base directory.
     * Key: absolute path of base directory
     * Value: detected format information
     */
    private static final ConcurrentHashMap<Path, StorageFormat> FORMAT_CACHE = new ConcurrentHashMap<>();

    /**
     * Storage format information for a base directory.
     *
     * @param compressionType The compression type used (ZSTD or NONE)
     * @param archiveType The archive type used (INDIVIDUAL_FILES or UNCOMPRESSED_ZIP)
     * @param powersOfTen The powers of ten for zip files (only relevant for UNCOMPRESSED_ZIP), -1 for INDIVIDUAL_FILES
     */
    public record StorageFormat(CompressionType compressionType, BlockArchiveType archiveType, int powersOfTen) {}

    /**
     * Read a block by auto-detecting storage format and compression settings.
     *
     * <p>Detection strategy:
     * <ol>
     *   <li>Check the cache for previously detected format information</li>
     *   <li>If not cached, try to find the block as an individual file (INDIVIDUAL_FILES format)</li>
     *   <li>If not found, try to find the block in a zip file (UNCOMPRESSED_ZIP format)</li>
     *   <li>Auto-detect compression type based on file extension (.blk.zstd or .blk)</li>
     *   <li>Cache the detected format for future reads</li>
     * </ol>
     *
     * <p>Format information is cached per base directory for performance when reading multiple blocks
     * from the same directory.
     *
     * @param baseDirectory The base directory for the block files
     * @param blockNumber The block number
     * @return The block
     * @throws IOException If an error occurs reading the block or if the block is not found
     */
    public static Block readBlock(final Path baseDirectory, final long blockNumber) throws IOException {
        // Normalize to an absolute path for consistent cache keys
        final Path absoluteBaseDir = baseDirectory.toAbsolutePath().normalize();

        // Check cache first
        StorageFormat cachedFormat = FORMAT_CACHE.get(absoluteBaseDir);

        if (cachedFormat != null) {
            // Use cached format information
            return readBlockWithFormat(absoluteBaseDir, blockNumber, cachedFormat);
        }

        // Not in cache - detect format and populate cache
        StorageFormat detectedFormat = detectStorageFormat(absoluteBaseDir, blockNumber);
        FORMAT_CACHE.put(absoluteBaseDir, detectedFormat);

        return readBlockWithFormat(absoluteBaseDir, blockNumber, detectedFormat);
    }

    /**
     * Read multiple blocks as a stream (lazy evaluation).
     *
     * <p>This method returns a Stream that lazily reads blocks one at a time. The storage format
     * is detected once (on the first block read) and cached for all subsequent reads, making this
     * very efficient for reading many blocks from the same directory.
     *
     * <p>The returned Stream will throw {@link UncheckedIOException} if any block cannot be read.
     * To handle errors on a per-block basis, consider using:
     * <pre>
     * stream.map(block -> {
     *     try {
     *         return readBlock(baseDirectory, blockNumber);
     *     } catch (IOException e) {
     *         // Handle error for this specific block
     *         return null;
     *     }
     * }).filter(Objects::nonNull)
     * </pre>
     *
     * @param baseDirectory The base directory for the block files
     * @param startBlockNumber The starting block number (inclusive)
     * @param endBlockNumber The ending block number (inclusive)
     * @return A stream of blocks from startBlockNumber to endBlockNumber (both inclusive)
     * @throws IllegalArgumentException If startBlockNumber > endBlockNumber
     */
    public static Stream<Block> readBlocks(
            final Path baseDirectory, final long startBlockNumber, final long endBlockNumber) {
        if (startBlockNumber > endBlockNumber) {
            throw new IllegalArgumentException(
                    "startBlockNumber (" + startBlockNumber + ") must be <= endBlockNumber (" + endBlockNumber + ")");
        }

        return LongStream.rangeClosed(startBlockNumber, endBlockNumber).mapToObj(blockNumber -> {
            try {
                return readBlock(baseDirectory, blockNumber);
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to read block " + blockNumber + " from " + baseDirectory, e);
            }
        });
    }

    /**
     * Read a block using known storage format information.
     *
     * @param baseDirectory The base directory for the block files
     * @param blockNumber The block number
     * @param format The storage format information
     * @return The block
     * @throws IOException If an error occurs reading the block
     */
    private static Block readBlockWithFormat(
            final Path baseDirectory, final long blockNumber, final StorageFormat format) throws IOException {
        return switch (format.archiveType) {
            case INDIVIDUAL_FILES -> {
                final Path blockFilePath = BlockFile.nestedDirectoriesBlockFilePath(
                        baseDirectory, blockNumber, format.compressionType, DIGITS_PER_DIR);
                yield readBlockFromIndividualFile(blockFilePath, format.compressionType);
            }
            case UNCOMPRESSED_ZIP ->
                readBlockFromZipWithPowersOfTen(baseDirectory, blockNumber, format.compressionType, format.powersOfTen);
        };
    }

    /**
     * Detect the storage format used in a base directory.
     *
     * @param baseDirectory The base directory for the block files
     * @param blockNumber The block number to use for detection
     * @return The detected storage format
     * @throws IOException If an error occurs during detection or if the block is not found
     */
    private static StorageFormat detectStorageFormat(final Path baseDirectory, final long blockNumber)
            throws IOException {
        // Try INDIVIDUAL_FILES format with ZSTD compression first (most common)
        Path zstdPath = BlockFile.nestedDirectoriesBlockFilePath(
                baseDirectory, blockNumber, CompressionType.ZSTD, DIGITS_PER_DIR);
        if (Files.exists(zstdPath)) {
            return new StorageFormat(CompressionType.ZSTD, BlockArchiveType.INDIVIDUAL_FILES, -1);
        }

        // Try INDIVIDUAL_FILES format with no compression
        Path noCompPath = BlockFile.nestedDirectoriesBlockFilePath(
                baseDirectory, blockNumber, CompressionType.NONE, DIGITS_PER_DIR);
        if (Files.exists(noCompPath)) {
            return new StorageFormat(CompressionType.NONE, BlockArchiveType.INDIVIDUAL_FILES, -1);
        }

        // Try UNCOMPRESSED_ZIP format with different powers of ten
        for (int powersOfTen = 1; powersOfTen <= 6; powersOfTen++) {
            // Try ZSTD compression
            BlockWriter.BlockPath zstdZipPath =
                    BlockWriter.computeBlockPath(baseDirectory, blockNumber, CompressionType.ZSTD, powersOfTen);
            if (Files.exists(zstdZipPath.zipFilePath()) && blockExistsInZip(zstdZipPath)) {
                return new StorageFormat(CompressionType.ZSTD, BlockArchiveType.UNCOMPRESSED_ZIP, powersOfTen);
            }

            // Try no compression
            BlockWriter.BlockPath noneZipPath =
                    BlockWriter.computeBlockPath(baseDirectory, blockNumber, CompressionType.NONE, powersOfTen);
            if (Files.exists(noneZipPath.zipFilePath()) && blockExistsInZip(noneZipPath)) {
                return new StorageFormat(CompressionType.NONE, BlockArchiveType.UNCOMPRESSED_ZIP, powersOfTen);
            }
        }

        throw new IOException(
                "Block " + blockNumber + " not found in " + baseDirectory + " - unable to detect storage format");
    }

    /**
     * Read a block from a zip file with known powers of ten configurations.
     *
     * @param baseDirectory The base directory for the block files
     * @param blockNumber The block number
     * @param compressionType The compression type
     * @param powersOfTen The powers of ten for the zip file
     * @return The block
     * @throws IOException If an error occurs reading the block or if the block is not found
     */
    private static Block readBlockFromZipWithPowersOfTen(
            final Path baseDirectory,
            final long blockNumber,
            final CompressionType compressionType,
            final int powersOfTen)
            throws IOException {
        final BlockWriter.BlockPath blockPath =
                BlockWriter.computeBlockPath(baseDirectory, blockNumber, compressionType, powersOfTen);

        if (!Files.exists(blockPath.zipFilePath())) {
            throw new IOException("Zip file not found: " + blockPath.zipFilePath());
        }

        try (final FileSystem zipFs = FileSystems.newFileSystem(blockPath.zipFilePath())) {
            final Path blockFileInZip = zipFs.getPath("/", blockPath.blockFileName());

            if (!Files.exists(blockFileInZip)) {
                throw new IOException("Block " + blockNumber + " not found in zip file: " + blockPath.zipFilePath());
            }

            final byte[] compressedBytes = Files.readAllBytes(blockFileInZip);
            return deserializeBlock(compressedBytes, compressionType);
        }
    }

    /**
     * Check if a block exists in a zip file.
     *
     * @param blockPath The block path information
     * @return true if the block file exists in the zip
     */
    private static boolean blockExistsInZip(final BlockWriter.BlockPath blockPath) {
        try (final FileSystem zipFs = FileSystems.newFileSystem(blockPath.zipFilePath())) {
            final Path blockFileInZip = zipFs.getPath("/", blockPath.blockFileName());
            return Files.exists(blockFileInZip);
        } catch (IOException e) {
            return false;
        }
    }

    /**
     * Read a block from an individual file (INDIVIDUAL_FILES format).
     *
     * @param blockFilePath The path to the block file
     * @param compressionType The compression type
     * @return The block
     * @throws IOException If an error occurs reading the block
     */
    private static Block readBlockFromIndividualFile(final Path blockFilePath, final CompressionType compressionType)
            throws IOException {
        final byte[] compressedBytes = Files.readAllBytes(blockFilePath);
        return deserializeBlock(compressedBytes, compressionType);
    }

    /**
     * Deserialize a block from compressed bytes.
     *
     * @param compressedBytes The compressed block bytes
     * @param compressionType The compression type
     * @return The deserialized block
     * @throws IOException If an error occurs deserializing the block
     */
    private static Block deserializeBlock(final byte[] compressedBytes, final CompressionType compressionType)
            throws IOException {
        try (final ByteArrayInputStream byteStream = new ByteArrayInputStream(compressedBytes);
                final InputStream decompressedStream = compressionType.wrapStream(byteStream);
                final ReadableStreamingData in = new ReadableStreamingData(decompressedStream)) {
            return Block.PROTOBUF.parse(in);
        } catch (ParseException e) {
            throw new IOException("Failed to parse block from bytes", e);
        }
    }
}
