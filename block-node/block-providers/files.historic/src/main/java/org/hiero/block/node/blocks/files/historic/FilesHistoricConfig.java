// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.blocks.files.historic;

import com.swirlds.config.api.ConfigData;
import com.swirlds.config.api.ConfigProperty;
import com.swirlds.config.api.validation.annotation.Max;
import com.swirlds.config.api.validation.annotation.Min;
import java.nio.file.Path;
import java.util.Objects;
import org.hiero.block.common.utils.Preconditions;
import org.hiero.block.node.base.CompressionType;
import org.hiero.block.node.base.Loggable;

/**
 * Use this configuration across the files recent plugin.
 *
 * @param rootPath provides the root path for saving historic blocks
 * @param compression compression type to use for the storage. It is assumed this never changes while a node is running
 *                    and has existing files.
 * @param digitsPerDir the number of block number digits per directory level. For example 3 = 1000 directories in each
 *                     directory
 * @param digitsPerZipFileName the number of digits of zip files in bottom level directory, For example 1 = 10 zip files
 *                             in each directory
 * @param digitsPerZipFileContents the number of digits of files in the zip file. For example 4 = 1000 files in each zip
 */
@ConfigData("files.historic")
public record FilesHistoricConfig(
        @Loggable @ConfigProperty(defaultValue = "/opt/hashgraph/blocknode/data/historic") Path rootPath,
        @Loggable @ConfigProperty(defaultValue = "ZSTD") CompressionType compression,
        @Loggable @ConfigProperty(defaultValue = "3") @Min(1) @Max(6) int digitsPerDir,
        @Loggable @ConfigProperty(defaultValue = "1") @Min(1) @Max(10) int digitsPerZipFileName,
        @Loggable @ConfigProperty(defaultValue = "4") @Min(1) @Max(10) int digitsPerZipFileContents) {
    /**
     * Constructor.
     */
    public FilesHistoricConfig {
        Objects.requireNonNull(rootPath);
        Objects.requireNonNull(compression);
        Preconditions.requireInRange(digitsPerDir, 1, 6);
        Preconditions.requireInRange(digitsPerZipFileName, 1, 10);
        Preconditions.requireInRange(digitsPerZipFileContents, 1, 10);
    }
}
