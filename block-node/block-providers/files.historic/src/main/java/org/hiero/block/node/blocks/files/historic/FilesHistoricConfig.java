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
 * and has existing files.
 * @param powersOfTenPerZipFileContents the number files in a zip file specified in powers of ten. Can can be one of
 * 1 = 10, 2 = 100, 3 = 1000, 4 = 10,000, 5 = 100,000, or 6 = 1,000,000 files per
 * zip. Changing this is handy for testing, as having to wait for 10,000 blocks to be
 * created is a long time.
 * @param blockRetentionThreshold the retention policy threshold (count of blocks to keep). For the historic
 * plugin, this value determines how many zips (archived batches) to retain. For instance if set to 5 and if the
 * {@link #powersOfTenPerZipFileContents} is set to 3, then this means that 5 zips will be retained and these zips
 * contain 10^3 blocks, i.e. 5_000 blocks effectively retained. If set to 0 (zero), blocks will be retained
 * indefinitely.
 */
@ConfigData("files.historic")
public record FilesHistoricConfig(
        @Loggable @ConfigProperty(defaultValue = "/opt/hiero/block-node/data/historic") Path rootPath,
        @Loggable @ConfigProperty(defaultValue = "ZSTD") CompressionType compression,
        @Loggable @ConfigProperty(defaultValue = "4") @Min(1) @Max(6) int powersOfTenPerZipFileContents,
        @Loggable @ConfigProperty(defaultValue = "0") long blockRetentionThreshold) {
    /**
     * Constructor.
     */
    public FilesHistoricConfig {
        Objects.requireNonNull(rootPath);
        Objects.requireNonNull(compression);
        Preconditions.requireInRange(powersOfTenPerZipFileContents, 1, 6);
        Preconditions.requireGreaterOrEqual(blockRetentionThreshold, 0L);
    }
}
