// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.blocks.files.historic;

import com.swirlds.config.api.ConfigData;
import com.swirlds.config.api.ConfigProperty;
import com.swirlds.config.api.validation.annotation.Max;
import com.swirlds.config.api.validation.annotation.Min;
import java.nio.file.Path;
import java.util.Objects;
import org.hiero.block.node.base.CompressionType;
import org.hiero.block.node.base.config.Loggable;

/**
 * Use this configuration across the files recent plugin.
 *
 * @param historicRootPath provides the root path for saving historic blocks
 * @param compression compression type to use for the storage. It is assumed this never changes while a node is running
 *                    and has existing files.
 * @param digitsPerDir the number of block number digits per directory level
 * @param digitsPerZipFileName the number of zip files in bottom level directory
 */
@ConfigData("persistence.storage")
public record FilesHistoricConfig(
        @Loggable @ConfigProperty(defaultValue = "/opt/hashgraph/blocknode/data/historic") Path historicRootPath,
        @Loggable @ConfigProperty(defaultValue = "ZSTD") CompressionType compression,
        @Loggable @ConfigProperty(defaultValue = "3") @Min(1) @Max(6) int digitsPerDir,
        @Loggable @ConfigProperty(defaultValue = "1") @Min(1) @Max(10) int digitsPerZipFileName) {
    /**
     * Constructor.
     */
    public FilesHistoricConfig {
        Objects.requireNonNull(historicRootPath);
    }
}
