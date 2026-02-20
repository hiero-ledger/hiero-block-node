// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.blocks.files.recent;

import java.nio.file.Files;
import java.nio.file.Path;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import org.hiero.block.node.base.CompressionType;

/**
 * A record representing a recent block file path with its associated metadata.
 * <p>
 * This class provides utilities for computing and resolving block file paths in a nested
 * directory structure. Block files are stored with names based on their block numbers,
 * formatted with zero-padding to ensure consistent length.
 * <p>
 * For example, block number 1234567890123456789 with filesPerDir=3 would be stored at:
 * <pre>
 * basePath/123/456/789/012/345/6/1234567890123456789.blk.zstd
 * </pre>
 *
 * @param path the file system path to the block file
 * @param blockNumber the block number
 * @param compressionType the compression type used for the block file
 */
public record RecentBlockPath(Path path, long blockNumber, CompressionType compressionType) {

    /** The extension for compressed block files */
    public static final String BLOCK_FILE_EXTENSION = ".blk";
    /** The format for block numbers in file names */
    private static final NumberFormat BLOCK_NUMBER_FORMAT = new DecimalFormat("0000000000000000000");

    /**
     * Computes the expected block file path for a given block number based on the
     * provided configuration.
     * <p>
     * This method constructs the path where a block file should be stored according to
     * the nested directory structure defined by the configuration. The path is computed
     * deterministically based on the block number, but the file may not exist yet.
     *
     * @param config the recent files configuration containing base path, compression type,
     *               and directory structure settings
     * @param blockNumber the block number for which to compute the path
     * @return a RecentBlockPath containing the computed path, block number, and compression type
     */
    public static RecentBlockPath computeBlockPath(FilesRecentConfig config, final long blockNumber) {
        final Path path = nestedDirectoriesBlockFilePath(
                config.liveRootPath(), blockNumber, config.compression(), config.maxFilesPerDir());
        return new RecentBlockPath(path, blockNumber, config.compression());
    }

    /**
     * Finds and returns the existing block file path for a given block number.
     * <p>
     * This method first attempts to locate the block file using the configured compression
     * type and directory structure. If the file is not found at the expected location, it
     * searches for the block file with alternative compression types in the same directory.
     * This is useful when the compression type might have changed or is unknown.
     *
     * @param config the recent files configuration containing base path, compression type,
     *               and directory structure settings
     * @param blockNumber the block number to locate
     * @return a RecentBlockPath for the existing block file, or {@code null} if no matching
     *         file is found with any compression type
     */
    public static RecentBlockPath computeExistingBlockPath(FilesRecentConfig config, final long blockNumber) {
        final RecentBlockPath recentBlockPath = computeBlockPath(config, blockNumber);
        if (Files.exists(recentBlockPath.path())) {
            return recentBlockPath;
        }
        final Path parentPath = recentBlockPath.path().getParent();
        final CompressionType[] compressionTypes = CompressionType.values();
        for (CompressionType compressionType : compressionTypes) {
            final Path potentialPath = standaloneBlockFilePath(parentPath, blockNumber, compressionType);
            if (Files.exists(potentialPath)) {
                return new RecentBlockPath(potentialPath, blockNumber, compressionType);
            }
        }
        return null;
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
    private static Path nestedDirectoriesBlockFilePath(
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
    private static Path standaloneBlockFilePath(Path basePath, long blockNumber, CompressionType compressionType) {
        return basePath.resolve(blockFileName(blockNumber) + compressionType.extension());
    }

    /**
     * Generates a block file name based on the given block number.
     *
     * @param blockNumber the block number
     * @return the formatted block file name
     */
    private static String blockFileName(long blockNumber) {
        return BLOCK_NUMBER_FORMAT.format(blockNumber) + BLOCK_FILE_EXTENSION;
    }
}
