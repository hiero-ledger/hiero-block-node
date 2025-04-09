// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.blocks.files.historic;

import static org.hiero.block.node.base.BlockFile.BLOCK_FILE_EXTENSION;
import static org.hiero.block.node.base.BlockFile.blockNumberFormated;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.nio.file.Path;
import java.util.Objects;
import org.hiero.block.common.utils.Preconditions;

/**
 * Record for block path components
 *
 * @param dirPath       The directory path for the directory that contains the zip file
 * @param zipFilePath   The path to the zip file
 * @param blockNumStr   The block number as a string
 * @param blockFileName The name of the block file in the zip file
 */
record BlockPath(Path dirPath, Path zipFilePath, String blockNumStr, String blockFileName) {
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
        final int offsetToZip = blockNumberStr.length() - config.digitsPerZipFileName() - config.digitsPerDir();
        final String directoryDigits = blockNumberStr.substring(0, offsetToZip);
        final String zipFileNameDigits =
                blockNumberStr.substring(offsetToZip, offsetToZip + config.digitsPerZipFileName());
        // start building path to zip file
        Path dirPath = config.rootPath();
        for (int i = 0; i < directoryDigits.length(); i += config.digitsPerDir()) {
            final String dirName =
                    directoryDigits.substring(i, Math.min(i + config.digitsPerDir(), directoryDigits.length()));
            dirPath = dirPath.resolve(dirName);
        }
        // create zip file name
        final String zipFileName = zipFileNameDigits + "000s.zip";
        final String fileName =
                blockNumberStr + BLOCK_FILE_EXTENSION + config.compression().extension();
        return new BlockPath(dirPath, dirPath.resolve(zipFileName), blockNumberStr, fileName);
    }
}
