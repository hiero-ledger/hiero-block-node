// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.blocks.files.recent;

import static java.nio.file.FileVisitResult.CONTINUE;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import org.hiero.block.node.base.BlockFile;
import org.hiero.block.node.base.CompressionType;

public class RecentBlockPath {

    /** The extension for compressed block files */
    public static final String BLOCK_FILE_EXTENSION = ".blk";
    /** The maximum depth of directory to walk.
     * This is the deepest path permitted if we set digits per directory to `1`. */
    private static final int MAX_FILE_SEARCH_DEPTH = 20;
    /** The format for block numbers in file names */
    private static final NumberFormat BLOCK_NUMBER_FORMAT = new DecimalFormat("0000000000000000000");

    /**
     * Set of all block number in a directory structure. The base path is the root of the directory structure. The
     * method will traverse the directory structure and find all block numbers.
     *
     * @param basePath the base path
     * @param compressionType the compression type
     * @return the minimum block number, or -1 if no block files are found
     */
    static Set<Long> nestedDirectoriesAllBlockNumbers(Path basePath, CompressionType compressionType) {
        try {
            final Set<Long> blockNumbers = new HashSet<>();
            final String fullExtension = BLOCK_FILE_EXTENSION + compressionType.extension();
            Files.walkFileTree(
                    basePath,
                    Set.of(),
                    MAX_FILE_SEARCH_DEPTH,
                    new BlockNumberCollectionVisitor(blockNumbers, fullExtension));
            return blockNumbers;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Generates a path to a compressed block file based on the given base path and block number. This is for a nested
     * directory structure where it splits the block number digits into chunks of the specified size
     * {@code filesPerDir}. So for example, if the block number is 1234567890123456789 and basePath is "foo" then
     * the path will be "foo/123/456/789/012/345/6/1234567890123456789.blk.zstd".
     *
     * @param basePath the base path
     * @param blockNumber the block number
     * @param compressionType the compression type
     * @param filesPerDir the max number of files or directories per directory
     * @return the path to the raw block file
     */
    public static Path nestedDirectoriesBlockFilePath(
            Path basePath, long blockNumber, CompressionType compressionType, int filesPerDir) {
        final String blockNumberStr = BLOCK_NUMBER_FORMAT.format(blockNumber);
        // remove the last digits from the block number for the files in last directory
        final String blockNumberDir = blockNumberStr.substring(0, blockNumberStr.length() - filesPerDir);
        // start building path to zip file
        Path dirPath = basePath;
        for (int i = 0; i < blockNumberDir.length(); i += filesPerDir) {
            final String dirName = blockNumberStr.substring(i, Math.min(i + filesPerDir, blockNumberDir.length()));
            dirPath = dirPath.resolve(dirName);
        }
        // append the block file name
        return standaloneBlockFilePath(dirPath, blockNumber, compressionType);
    }

    /**
     * Generates a path to a compressed block file based on the given base path and block number. This is for a single
     * standalone block file.
     *
     * @param basePath the base path
     * @param blockNumber the block number
     * @param compressionType the compression type
     * @return the path to the raw block file
     */
    public static Path standaloneBlockFilePath(Path basePath, long blockNumber, CompressionType compressionType) {
        return basePath.resolve(blockFileName(blockNumber) + compressionType.extension());
    }

    /**
     * Generates a block file name based on the given block number.
     *
     * @param blockNumber the block number
     * @return the formatted block file name
     */
    public static String blockFileName(long blockNumber) {
        return BLOCK_NUMBER_FORMAT.format(blockNumber) + BLOCK_FILE_EXTENSION;
    }

    private static class BlockNumberCollectionVisitor implements FileVisitor<Path> {
        private final String blockFileExtension;
        private final Set<Long> blockNumbersFound;

        public BlockNumberCollectionVisitor(
                @NonNull final Set<Long> blockNumbers, @NonNull final String fullExtension) {
            blockFileExtension = Objects.requireNonNull(fullExtension);
            blockNumbersFound = Objects.requireNonNull(blockNumbers);
        }

        @Override
        public FileVisitResult preVisitDirectory(final Path dir, final BasicFileAttributes attrs) throws IOException {
            Objects.requireNonNull(dir);
            return CONTINUE;
        }

        @Override
        public FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs) throws IOException {
            if (file.toString().endsWith(blockFileExtension)) {
                blockNumbersFound.add(BlockFile.blockNumberFromFile(file));
            }
            return CONTINUE;
        }

        @Override
        public FileVisitResult visitFileFailed(final Path file, final IOException exc) throws IOException {
            throw Objects.requireNonNull(exc);
        }

        @Override
        public FileVisitResult postVisitDirectory(final Path dir, final IOException exc) throws IOException {
            if (exc == null) {
                Objects.requireNonNull(dir);
                return CONTINUE;
            } else {
                throw exc;
            }
        }
    }
}
