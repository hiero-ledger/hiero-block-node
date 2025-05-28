// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.blocks.files.historic;

import static org.hiero.block.node.base.BlockFile.BLOCK_FILE_EXTENSION;
import static org.hiero.block.node.base.BlockFile.blockNumberFormated;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import org.hiero.block.common.utils.Preconditions;
import org.hiero.block.node.base.CompressionType;

/**
 * Record for block path components
 *
 * @param dirPath       The directory path for the directory that contains the zip file
 * @param zipFilePath   The path to the zip file
 * @param blockNumStr   The block number as a string
 * @param blockFileName The name of the block file in the zip file
 */
record BlockPath(
        Path dirPath, Path zipFilePath, String blockNumStr, String blockFileName, CompressionType compressionType) {
    /** The number of block number digits per directory level. For example 3 = 1000 directories in each directory */
    public static final int DIGITS_PER_DIR = 3;
    /** The number of digits of zip files in bottom level directory, For example 1 = 10 zip files in each directory */
    public static final int DIGITS_PER_ZIP_FILE_NAME = 1;

    /**
     * Constructor.
     *
     * @param dirPath       valid, non-null path to the directory that contains the zip file
     * @param zipFilePath   valid, non-null path to the zip file
     * @param blockNumStr   valid, non-blank block number string
     * @param blockFileName valid, non-blank block file name
     */
    BlockPath {
        Objects.requireNonNull(dirPath);
        Objects.requireNonNull(zipFilePath);
        Preconditions.requireNotBlank(blockNumStr);
        Preconditions.requireNotBlank(blockFileName);
        Objects.requireNonNull(compressionType);
    }

    /**
     * Compute the path to a block file.
     *
     * @param config      The configuration for the block provider, must be non-null
     * @param blockNumber The block number, must be a whole number
     *
     * @return The path to the block file
     */
    static BlockPath computeBlockPath(@NonNull final FilesHistoricConfig config, final long blockNumber) {
        Objects.requireNonNull(config);
        Preconditions.requireWhole(blockNumber);
        // convert block number to string
        final String blockNumberStr = blockNumberFormated(blockNumber);
        // split string into digits for zip and for directories
        // offsetToZip is the number of digits in the block number that will be split into directories
        final int offsetToZip =
                blockNumberStr.length() - DIGITS_PER_ZIP_FILE_NAME - config.powersOfTenPerZipFileContents();
        // slice the block number string, directory part
        final String directoryDigits = blockNumberStr.substring(0, offsetToZip);
        // slice the block number string, zip file part, with DIGITS_PER_ZIP_FILE_NAME = 1 this is always 1 digit
        final String zipFileNameDigits = blockNumberStr.substring(offsetToZip, offsetToZip + DIGITS_PER_ZIP_FILE_NAME);
        // start building directory path to zip file, by slicing directoryDigits by DIGITS_PER_DIR
        Path dirPath = config.rootPath();
        for (int i = 0; i < directoryDigits.length(); i += DIGITS_PER_DIR) {
            final String dirName = directoryDigits.substring(i, Math.min(i + DIGITS_PER_DIR, directoryDigits.length()));
            dirPath = dirPath.resolve(dirName);
        }
        // create zip file name, there are always 10 zip files in each base directory as DIGITS_PER_ZIP_FILE_NAME = 1
        // the name of the zip file is the set of values stored in the zip. So if there are 1000 files in a zip file
        // the zip file names will be 0000s.zip, 1000s.zip, 2000s.zip, 3000s.zip etc.
        final String zipFileName = zipFileNameDigits + "0".repeat(config.powersOfTenPerZipFileContents()) + "s.zip";
        // create the block file name, this is always the block number with the BLOCK_FILE_EXTENSION it is always the
        // whole number so if the file is manually expanded the blocks always have the full block number so there are
        // no duplicates or confusion.
        final String fileName =
                blockNumberStr + BLOCK_FILE_EXTENSION + config.compression().extension();
        // assemble the BlockPath from all the components
        return new BlockPath(dirPath, dirPath.resolve(zipFileName), blockNumberStr, fileName, config.compression());
    }

    static BlockPath computeExistingBlockPath(@NonNull final FilesHistoricConfig config, final long blockNumber)
            throws IOException {
        // compute the path to the block file based on current configuration
        final BlockPath computed = computeBlockPath(config, blockNumber);
        // check if the zip file exists
        if (Files.exists(computed.zipFilePath)) {
            try (final FileSystem zipFS = FileSystems.newFileSystem(computed.zipFilePath)) {
                // check if the block file exists
                if (Files.exists(zipFS.getPath(computed.blockFileName))) {
                    return computed;
                } else {
                    // if happy path not found, check if persisted with another
                    // compression extension.
                    final CompressionType[] compressionOpts =
                            config.compression().getDeclaringClass().getEnumConstants();
                    // noinspection ForLoopReplaceableByForEach
                    for (int i = 0; i < compressionOpts.length; i++) {
                        final CompressionType currentOpt = compressionOpts[i];
                        // we are only
                        if (!currentOpt.equals(config.compression())) {
                            // check if the block file exists
                            final String newFileName =
                                    computed.blockNumStr + BLOCK_FILE_EXTENSION + currentOpt.extension();
                            if (Files.exists(zipFS.getPath(newFileName))) {
                                // if found, update and return
                                return new BlockPath(
                                        computed.dirPath,
                                        computed.zipFilePath,
                                        computed.blockNumStr,
                                        newFileName,
                                        currentOpt);
                            }
                        }
                    }
                }
            }
        }
        // if none found, return null as we could not find the block existing
        return null;
    }
}
