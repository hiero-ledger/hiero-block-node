package org.hiero.block.node.base;

import java.nio.file.Path;
import java.text.DecimalFormat;
import java.text.NumberFormat;

/**
 * Utility class for generating block file names.
 * <p>
 * The block file names are formatted as 19 digits followed by the ".blk" extension.
 * The digits are zero-padded to ensure a consistent length.
 */
public class BlockFile {
    /** The format for block numbers in file names */
    private static final NumberFormat BLOCK_NUMBER_FORMAT = new DecimalFormat("0000000000000000000");
    /** The extension for compressed block files */
    private static final String BLOCK_FILE_EXTENSION = ".blk";

    /**
     * Generates a block file name based on the given block number.
     *
     * @param blockNumber the block number
     * @return the formatted block file name
     */
    public static String blockFileName(long blockNumber) {
        return BLOCK_NUMBER_FORMAT.format(blockNumber) + BLOCK_FILE_EXTENSION;
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
     * Generates a path to a compressed block file based on the given base path and block number. This is for a nested
     * directory structure where it splits the block number digits into chunks of the specified size
     * {@code filesPerDir}. So for examole, if the block number is 1234567890123456789 and basePath is "foo" then
     * the path will be "foo/123/456/789/012/345/6/1234567890123456789.blk.zstd".
     *
     * @param basePath the base path
     * @param blockNumber the block number
     * @param compressionType the compression type
     * @param filesPerDir the max number of files or directories per directory
     * @return the path to the raw block file
     */
    public static Path nestedDirectoriesBlockFilePath(Path basePath, long blockNumber, CompressionType compressionType,
            int filesPerDir) {
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
        return standaloneBlockFilePath(basePath, blockNumber, compressionType);
    }
}
