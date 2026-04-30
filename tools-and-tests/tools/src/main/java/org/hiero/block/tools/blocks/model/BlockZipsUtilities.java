// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.model;

import com.github.luben.zstd.ZstdInputStream;
import com.hedera.hapi.block.stream.Block;
import com.hedera.pbj.runtime.io.buffer.BufferedData;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.base.CompressionType;
import org.hiero.block.tools.blocks.validation.ProtobufParsingConstants;

/**
 * Shared utilities for discovering, reading, decompressing and parsing block files from directories and zip archives.
 * Used by both {@code ValidateBlocksCommand} and {@code BlockInfo}.
 */
@SuppressWarnings("DuplicatedCode")
public final class BlockZipsUtilities {

    /** Pattern to match block file names and extract the block number. */
    private static final Pattern BLOCK_FILE_PATTERN = Pattern.compile("^(\\d+)\\.blk(\\.gz|\\.zstd)?$");

    /** Pattern to match zip file names used by BlockWriter (e.g. "50000s.zip"). */
    private static final Pattern ZIP_NAME_PATTERN = Pattern.compile("^(\\d+)s\\.zip$");

    /** Default blocks per zip file (10^4 = 10,000). */
    public static final int DEFAULT_BLOCKS_PER_ZIP = (int) Math.pow(10, BlockWriter.DEFAULT_POWERS_OF_TEN_PER_ZIP);

    /** Private constructor to prevent instantiation. */
    private BlockZipsUtilities() {}

    /**
     * Represents a source for reading a block — either a standalone file, an entry in a zip archive,
     * or a whole zip archive (lazy discovery).
     *
     * <p>For whole-zip sources, {@code zipEntryName} is null and {@code filePath} ends with ".zip".
     * The {@code blockNumber} is the inferred first block in the zip range, used for sorting and
     * resume filtering. Actual entries are discovered lazily during processing.
     *
     * @param blockNumber the block number extracted from the filename (or inferred first block for whole-zip)
     * @param filePath the path to the file (either the block file or the zip file)
     * @param zipEntryName the name of the entry within the zip file, or null for standalone/whole-zip
     * @param compressionType the compression type of the block data (null for .gz files or whole-zip)
     */
    public record BlockSource(long blockNumber, Path filePath, String zipEntryName, CompressionType compressionType) {
        /** Check if this is a per-entry zip source (entry name already known). */
        public boolean isZipEntry() {
            return zipEntryName != null;
        }

        /** Check if this is a whole-zip source (entries discovered lazily during processing). */
        public boolean isWholeZip() {
            return zipEntryName == null
                    && filePath != null
                    && filePath.getFileName().toString().endsWith(".zip");
        }
    }

    /**
     * Block that has been decompressed, parsed, and pre-validated (stateless checks run in parallel).
     *
     * @param block the shallow-parsed block
     * @param blockNumber the block number
     * @param preValidationName the name of the validation that failed, or null if all passed
     * @param preValidationError the first validation failure from the parallel stage, or null if all passed
     */
    public record PreValidatedBlock(
            BlockUnparsed block, long blockNumber, String preValidationName, Exception preValidationError) {}

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
     * Zip archives are represented as whole-zip sources (one per zip) to avoid opening
     * them during discovery. Actual entries are discovered lazily during processing.
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
     * Recursively finds block files in a directory, including zip archives (as whole-zip sources).
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
     * Registers a zip archive as a block source. On the default filesystem, creates a lightweight
     * whole-zip source by inferring the first block number from the zip path (no zip opening).
     * Falls back to per-entry discovery for in-memory filesystems (jimfs) used in tests.
     *
     * @param zipPath path to the zip file
     * @param sources list to add sources to
     * @param corruptZipCounter optional counter for corrupt zips (may be null)
     */
    public static void findBlocksInZip(Path zipPath, List<BlockSource> sources, AtomicLong corruptZipCounter) {
        if (zipPath.getFileSystem() == FileSystems.getDefault()) {
            // Lightweight: infer block range from path, don't open the zip
            long firstBlock = inferFirstBlockFromZipPath(zipPath);
            if (firstBlock >= 0) {
                sources.add(new BlockSource(firstBlock, zipPath, null, null));
            } else {
                // Can't infer — skip with warning
                System.err.println("Warning: cannot infer block range from zip: " + zipPath.getFileName());
            }
        } else {
            // In-memory filesystem (jimfs tests): open and discover entries
            try {
                findBlocksInZipViaFileSystem(zipPath, sources);
            } catch (IOException e) {
                if (corruptZipCounter != null) {
                    corruptZipCounter.incrementAndGet();
                }
                System.err.println("Warning: skipping corrupt zip: " + zipPath.getFileName() + " — " + e.getMessage());
            }
        }
    }

    /**
     * Infers the first block number contained in a zip file from its path in the block storage
     * trie structure. The directory names and zip filename encode the block number range:
     * e.g., {@code basedir/000/000/004/50000s.zip} → first block 450000.
     *
     * @param zipPath the path to the zip file
     * @return the first block number, or -1 if the path doesn't match the expected pattern
     */
    public static long inferFirstBlockFromZipPath(Path zipPath) {
        // Validate zip filename matches pattern
        String zipName = zipPath.getFileName().toString();
        Matcher zipMatcher = ZIP_NAME_PATTERN.matcher(zipName);
        if (!zipMatcher.matches()) {
            return -1;
        }
        String numericPart = zipMatcher.group(1); // e.g., "50000" from "50000s.zip"

        // Walk up parent directories, collecting numeric directory names
        List<String> dirNames = new ArrayList<>();
        Path parent = zipPath.getParent();
        while (parent != null && parent.getFileName() != null) {
            String name = parent.getFileName().toString();
            if (name.matches("\\d+")) {
                dirNames.add(name);
            } else {
                break; // Stop at first non-numeric directory (base dir)
            }
            parent = parent.getParent();
        }
        Collections.reverse(dirNames);

        // Build the full block number: dir digits + zip digit prefix + trailing zeros
        StringBuilder digits = new StringBuilder();
        for (String dir : dirNames) {
            digits.append(dir);
        }
        // First char of numericPart is the zip-level digit; remaining chars are range zeros
        digits.append(numericPart.charAt(0));
        // Pad to 19 digits with zeros (block number is always 19 digits)
        while (digits.length() < 19) {
            digits.append('0');
        }
        return Long.parseLong(digits.toString());
    }

    /**
     * Estimates the total number of blocks across all sources. Whole-zip sources are assumed
     * to contain {@link #DEFAULT_BLOCKS_PER_ZIP} blocks each; standalone and per-entry sources
     * count as one block each.
     *
     * @param sources the block sources
     * @return estimated total block count
     */
    public static long estimateTotalBlocks(List<BlockSource> sources) {
        long total = 0;
        for (BlockSource s : sources) {
            total += s.isWholeZip() ? DEFAULT_BLOCKS_PER_ZIP : 1;
        }
        return total;
    }

    /**
     * Expands whole-zip sources into per-entry sources by opening each zip and discovering entries.
     * Non-zip sources are passed through unchanged. This is intended for commands that process
     * individual blocks (e.g. BlockInfo) rather than streaming millions of blocks.
     *
     * @param sources the sources to expand
     * @return a new list with whole-zip sources replaced by per-entry sources
     */
    public static List<BlockSource> expandWholeZipSources(List<BlockSource> sources) {
        List<BlockSource> expanded = new ArrayList<>();
        for (BlockSource s : sources) {
            if (s.isWholeZip()) {
                try {
                    findBlocksInZipViaZipFile(s.filePath(), expanded);
                } catch (IOException e) {
                    System.err.println(
                            "Warning: cannot expand zip: " + s.filePath().getFileName() + " — " + e);
                }
            } else {
                expanded.add(s);
            }
        }
        expanded.sort(Comparator.comparingLong(BlockSource::blockNumber));
        return expanded;
    }

    /** Fast discovery using {@link java.util.zip.ZipFile} — opens CEN but avoids filesystem objects per entry. */
    private static void findBlocksInZipViaZipFile(Path zipPath, List<BlockSource> sources) throws IOException {
        try (java.util.zip.ZipFile zf = new java.util.zip.ZipFile(zipPath.toFile())) {
            java.util.Enumeration<? extends java.util.zip.ZipEntry> entries = zf.entries();
            while (entries.hasMoreElements()) {
                java.util.zip.ZipEntry entry = entries.nextElement();
                if (entry.isDirectory()) continue;
                String entryName = entry.getName();
                int lastSlash = entryName.lastIndexOf('/');
                String fileName = lastSlash >= 0 ? entryName.substring(lastSlash + 1) : entryName;
                long blockNum = extractBlockNumber(fileName);
                if (blockNum >= 0) {
                    CompressionType compression = getCompressionType(fileName);
                    sources.add(new BlockSource(blockNum, zipPath, "/" + entryName, compression));
                }
            }
        }
    }

    /** Fallback discovery using {@link FileSystem} — works with in-memory filesystems (jimfs). */
    private static void findBlocksInZipViaFileSystem(Path zipPath, List<BlockSource> sources) throws IOException {
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
    @SuppressWarnings("unused")
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
        return Block.PROTOBUF.parse(
                BufferedData.wrap(blockBytes), true, false, 1000, ProtobufParsingConstants.MAX_PARSE_SIZE);
    }
    /**
     * Decompresses raw bytes and parses them as a {@link BlockUnparsed} protobuf. BlockUnparsed is shallow parsed so
     * block items are parsed enough to know their type and bytes but if you need more you will have to do a second
     * parse on the block item. This is handy when you only need to parse a small subset of block items.
     *
     * @param raw compressed or uncompressed block bytes
     * @param isZstd true if the bytes are zstd-compressed
     * @param isGz true if the bytes are gzip-compressed
     * @return the parsed Block
     * @throws Exception if decompression or parsing fails
     */
    public static BlockUnparsed decompressAndPartialParse(byte[] raw, boolean isZstd, boolean isGz) throws Exception {
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
        return BlockUnparsed.PROTOBUF.parse(
                BufferedData.wrap(blockBytes), true, false, 1000, ProtobufParsingConstants.MAX_PARSE_SIZE);
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
