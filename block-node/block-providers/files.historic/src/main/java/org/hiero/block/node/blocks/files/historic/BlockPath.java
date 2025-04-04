// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.blocks.files.historic;

import static org.hiero.block.node.base.BlockFile.BLOCK_FILE_EXTENSION;
import static org.hiero.block.node.base.BlockFile.blockNumberFormated;

import java.nio.file.Path;

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
     * Compute the path to a block file
     *
     * @param blockNumber The block number
     * @return The path to the block file
     */
    static BlockPath computeBlockPath(final FilesHistoricConfig config, final long blockNumber) {
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
