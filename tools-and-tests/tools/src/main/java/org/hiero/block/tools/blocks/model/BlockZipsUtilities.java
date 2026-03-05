// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.model;

import com.github.luben.zstd.ZstdInputStream;
import com.hedera.hapi.block.stream.Block;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;
import org.hiero.block.node.base.CompressionType;

/**
 * Shared utilities for discovering, reading, decompressing and parsing block files from directories and zip archives.
 * Used by both {@code ValidateBlocksCommand} and {@code BlockInfo}.
 */
@SuppressWarnings("DuplicatedCode")
public final class BlockZipsUtilities {

    /** Pattern to match block file names and extract the block number. */
    private static final Pattern BLOCK_FILE_PATTERN = Pattern.compile("^(\\d+)\\.blk(\\.gz|\\.zstd)?$");

    /** Private constructor to prevent instantiation. */
    private BlockZipsUtilities() {}

    /**
     * Represents a source for reading a block — either a standalone file or an entry in a zip archive.
     *
     * @param blockNumber the block number extracted from the filename
     * @param filePath the path to the file (either the block file or the zip file)
     * @param zipEntryName the name of the entry within the zip file, or null for standalone files
     * @param compressionType the compression type of the block data (null for .gz files)
     */
    public record BlockSource(long blockNumber, Path filePath, String zipEntryName, CompressionType compressionType) {
        /** Check if this is a zip entry source. */
        public boolean isZipEntry() {
            return zipEntryName != null;
        }
    }

    /**
     * A decoded block together with its block number (from the filename).
     *
     * @param block the parsed block
     * @param blockNumber the block number
     */
    public record ParsedBlock(Block block, long blockNumber) {}

    /**
     * Extract a block number from a filename matching block file patterns.
     *
     * @param fileName the file name to parse
     * @return the block number, or -1 if not a valid block file name
     */
    public static long extractBlockNumber(String fileName) {
        Matcher matcher = BLOCK_FILE_PATTERN.matcher(fileName);
        if (matcher.matches()) {
            return Long.parseLong(matcher.group(1));
        }
        return -1;
    }

    /**
     * Determine the compression type from a file name.
     *
     * @param fileName the file name
     * @return the compression type, or null for .gz files (handled separately)
     */
    public static CompressionType getCompressionType(String fileName) {
        if (fileName.endsWith(".gz")) {
            return null;
        } else if (fileName.endsWith(".zstd")) {
            return CompressionType.ZSTD;
        }
        return CompressionType.NONE;
    }

    /**
     * Find all block sources from the given files/directories, sorted by block number.
     *
     * @param files array of files or directories to scan
     * @param corruptZipCounter if non-null, corrupt zips increment this counter and print a warning instead of
     *     throwing; if null, corrupt zips print a warning and are skipped silently
     * @return list of block sources sorted by block number
     */
    public static List<BlockSource> findBlockSources(Path[] files, AtomicLong corruptZipCounter) {
        List<BlockSource> sources = new ArrayList<>();
        for (Path file : files) {
            if (!Files.exists(file)) {
                System.err.println("File not found : " + file);
                continue;
            }
            if (Files.isDirectory(file)) {
                findBlocksInDirectory(file, sources, corruptZipCounter);
            } else if (file.getFileName().toString().endsWith(".zip")) {
                findBlocksInZip(file, sources, corruptZipCounter);
            } else {
                String fileName = file.getFileName().toString();
                long blockNum = extractBlockNumber(fileName);
                if (blockNum >= 0) {
                    CompressionType compression = getCompressionType(fileName);
                    sources.add(new BlockSource(blockNum, file, null, compression));
                }
            }
        }
        sources.sort(Comparator.comparingLong(BlockSource::blockNumber));
        return sources;
    }

    /**
     * Recursively finds block files in a directory, including entries within zip archives.
     *
     * @param dir the directory to search
     * @param sources list to add sources to
     * @param corruptZipCounter optional counter for corrupt zips (may be null)
     */
    public static void findBlocksInDirectory(Path dir, List<BlockSource> sources, AtomicLong corruptZipCounter) {
        try (Stream<Path> filesStream = Files.walk(dir)) {
            filesStream.filter(Files::isRegularFile).forEach(path -> {
                String fileName = path.getFileName().toString();
                if (fileName.endsWith(".zip")) {
                    findBlocksInZip(path, sources, corruptZipCounter);
                } else {
                    long blockNum = extractBlockNumber(fileName);
                    if (blockNum >= 0) {
                        CompressionType compression = getCompressionType(fileName);
                        sources.add(new BlockSource(blockNum, path, null, compression));
                    }
                }
            });
        } catch (IOException e) {
            System.err.println("Error scanning directory " + dir + ": " + e.getMessage());
        }
    }

    /**
     * Finds block files inside a zip archive using {@link FileSystem}.
     *
     * @param zipPath path to the zip file
     * @param sources list to add sources to
     * @param corruptZipCounter optional counter for corrupt zips (may be null)
     */
    public static void findBlocksInZip(Path zipPath, List<BlockSource> sources, AtomicLong corruptZipCounter) {
        try (FileSystem zipFs = FileSystems.newFileSystem(zipPath)) {
            for (Path root : zipFs.getRootDirectories()) {
                try (Stream<Path> filesStream = Files.walk(root)) {
                    filesStream.filter(Files::isRegularFile).forEach(path -> {
                        String fileName = path.getFileName().toString();
                        long blockNum = extractBlockNumber(fileName);
                        if (blockNum >= 0) {
                            CompressionType compression = getCompressionType(fileName);
                            sources.add(new BlockSource(blockNum, zipPath, path.toString(), compression));
                        }
                    });
                }
            }
        } catch (IOException e) {
            if (corruptZipCounter != null) {
                corruptZipCounter.incrementAndGet();
            }
            System.err.println("Warning: skipping corrupt zip: " + zipPath.getFileName() + " — " + e.getMessage());
        }
    }

    /**
     * Decompresses raw bytes and parses them as a {@link Block} protobuf.
     *
     * @param raw compressed or uncompressed block bytes
     * @param isZstd true if the bytes are zstd-compressed
     * @param isGz true if the bytes are gzip-compressed
     * @return the parsed Block
     * @throws Exception if decompression or parsing fails
     */
    public static Block decompressAndParse(byte[] raw, boolean isZstd, boolean isGz) throws Exception {
        final byte[] blockBytes;
        if (isZstd) {
            try (InputStream is = new ZstdInputStream(new ByteArrayInputStream(raw))) {
                blockBytes = is.readAllBytes();
            }
        } else if (isGz) {
            try (InputStream is = new GZIPInputStream(new ByteArrayInputStream(raw))) {
                blockBytes = is.readAllBytes();
            }
        } else {
            blockBytes = raw;
        }
        return Block.PROTOBUF.parse(Bytes.wrap(blockBytes));
    }

    /**
     * Read block data from a {@link BlockSource} and decompress it.
     *
     * @param source the block source
     * @return array containing {@code [compressedBytes, uncompressedBytes]}
     * @throws IOException if an error occurs reading or decompressing
     */
    public static byte[][] readBlockData(BlockSource source) throws IOException {
        byte[] compressedBytes;

        if (source.isZipEntry()) {
            try (FileSystem zipFs = FileSystems.newFileSystem(source.filePath())) {
                Path entryPath = zipFs.getPath("/", source.zipEntryName());
                compressedBytes = Files.readAllBytes(entryPath);
            }
        } else {
            compressedBytes = Files.readAllBytes(source.filePath());
        }

        // Decompress based on compression type
        byte[] uncompressedBytes;
        if (source.compressionType() == null) {
            // .gz files
            try (InputStream in = new GZIPInputStream(new ByteArrayInputStream(compressedBytes))) {
                uncompressedBytes = in.readAllBytes();
            }
        } else {
            uncompressedBytes = source.compressionType().decompress(compressedBytes);
        }

        return new byte[][] {compressedBytes, uncompressedBytes};
    }

    /**
     * Convenience: determine isZstd/isGz flags from a {@link BlockSource}'s compression type.
     *
     * @param source the block source
     * @return a two-element boolean array: {@code [isZstd, isGz]}
     */
    public static boolean[] compressionFlags(BlockSource source) {
        boolean isZstd = source.compressionType() == CompressionType.ZSTD;
        boolean isGz = source.compressionType() == null;
        return new boolean[] {isZstd, isGz};
    }

    /**
     * Convenience: determine isZstd/isGz flags from a filename.
     *
     * @param fileName the file name
     * @return a two-element boolean array: {@code [isZstd, isGz]}
     */
    public static boolean[] compressionFlags(String fileName) {
        boolean isZstd = fileName.endsWith(".zstd");
        boolean isGz = fileName.endsWith(".gz");
        return new boolean[] {isZstd, isGz};
    }
}
