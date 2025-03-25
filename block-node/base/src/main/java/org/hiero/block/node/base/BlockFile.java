package org.hiero.block.node.base;

import java.nio.file.Files;
import java.nio.file.Path;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

/**
 * Utility class for generating block file names.
 * <p>
 * The block file names are formatted as 19 digits followed by the ".blk" extension.
 * The digits are zero-padded to ensure a consistent length.
 */
@SuppressWarnings("unused")
public final class BlockFile {
    /** The logger for this class. */
    private static final System.Logger LOGGER = System.getLogger(BlockFile.class.getName());
    /** The format for block numbers in file names */
    private static final NumberFormat BLOCK_NUMBER_FORMAT = new DecimalFormat("0000000000000000000");
    /** The extension for compressed block files */
    public static final String BLOCK_FILE_EXTENSION = ".blk";

    /**
     * The block number format for use in file name. For example block 123 becomes "000000000000000123".
     *
     * @param blockNumber the block number
     * @return the formatted block number
     */
    public static String blockNumberFormated(long blockNumber) {
        return BLOCK_NUMBER_FORMAT.format(blockNumber);
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

    /**
     * Generates a block file name based on the given block number.
     *
     * @param blockNumber the block number
     * @param compressionType the compression type
     * @return the formatted block file name
     */
    public static String blockFileName(long blockNumber, CompressionType compressionType) {
        return BLOCK_NUMBER_FORMAT.format(blockNumber) + BLOCK_FILE_EXTENSION + compressionType.extension();
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

    /**
     * Finds the minimum block number in a directory structure. This is used to find the min block number in a
     * directory structure where the block numbers are stored in a nested directory structure. The base path is the
     * root of the directory structure. The method will traverse the directory structure and find the min block number.
     *
     * @param basePath the base path
     * @param compressionType the compression type
     * @return the minimum block number, or -1 if no block files are found
     */
    public static long nestedDirectoriesMinBlockNumber(Path basePath, CompressionType compressionType) {
        final String fullExtension = BLOCK_FILE_EXTENSION + compressionType.extension();
        long minBlockNumber = -1;
        // find the lowest block number first
        Path lowestPath = basePath;
        while (lowestPath != null) {
            // get the first directory in the path
            try (var childFilesStream = Files.list(lowestPath)) {
                List<Path> childFiles = childFilesStream.toList();
                // check if we are a directory of directories
                final Optional<Path> min = childFiles.stream()
                        .filter(Files::isDirectory)
                        .min(Comparator.comparingLong(path -> Long.parseLong(path.getFileName().toString())));
                if (min.isPresent()) {
                    lowestPath = min.get();
                } else {
                    // we are at the deepest directory, check for block files
                    minBlockNumber = childFiles.stream()
                            .filter(Files::isRegularFile)
                            .filter(path -> path.getFileName().toString().endsWith(fullExtension))
                            .mapToLong(BlockFile::blockNumberFromFile)
                            .min().orElse(-1);
                    break;
                }
            } catch (Exception e) {
                LOGGER.log(System.Logger.Level.ERROR, "Error reading directory: " + lowestPath, e);
                // handle exception
                break;
            }
        }
        return minBlockNumber;
    }

    /**
     * Finds the maximum block number in a directory structure. This is used to find the max block number in a
     * directory structure where the block numbers are stored in a nested directory structure. The base path is the
     * root of the directory structure. The method will traverse the directory structure and find the max block number.
     *
     * @param basePath the base path
     * @param compressionType the compression type
     * @return the maximum block number, or -1 if no block files are found
     */
    public static long nestedDirectoriesMaxBlockNumber(Path basePath, CompressionType compressionType) {
        final String fullExtension = BLOCK_FILE_EXTENSION + compressionType.extension();
        long maxBlockNumber = -1;
        // find the highest block number
        Path highestPath = basePath;
        while(highestPath != null) {
            // get the first directory in the path
            try(var childFilesStream = Files.list(highestPath)) {
                List<Path> childFiles = childFilesStream.toList();
                // check if we are a directory of directories
                final Optional<Path> max = childFiles.stream()
                        .filter(Files::isDirectory)
                        .max(Comparator.comparingLong(path -> Long.parseLong(path.getFileName().toString())));
                if (max.isPresent()) {
                    highestPath = max.get();
                } else {
                    // we are at the deepest directory, check for block files
                    maxBlockNumber = childFiles.stream()
                            .filter(Files::isRegularFile)
                            .filter(path -> path.getFileName().toString().endsWith(fullExtension))
                            .mapToLong(BlockFile::blockNumberFromFile)
                            .max().orElse(-1);
                    break;
                }
            } catch (Exception e) {
                LOGGER.log(System.Logger.Level.ERROR, "Error reading directory: " + highestPath, e);
                // handle exception
                break;
            }
        }
        return maxBlockNumber;
    }

    /**
     * Extracts the block number from a file name. The file name is expected to be in the format
     * {@code "0000000000000000000.blk.xyz"} where the block number is the first 19 digits and the rest is the file
     * extension.
     *
     * @param file the path for file to extract the block number from
     * @return the block number
     */
    public static long blockNumberFromFile(final Path file) {
        return blockNumberFromFile(file.getFileName().toString());
    }

    /**
     * Extracts the block number from a file name. The file name is expected to be in the format
     * {@code "0000000000000000000.blk.xyz"} where the block number is the first 19 digits and the rest is the file
     * extension.
     *
     * @param fileName the file name to extract the block number from
     * @return the block number
     */
    public static long blockNumberFromFile(final String fileName) {
        return Long.parseLong(fileName.substring(0, fileName.indexOf('.')));
    }
}
