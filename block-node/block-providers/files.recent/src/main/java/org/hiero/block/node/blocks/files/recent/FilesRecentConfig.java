// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.blocks.files.recent;

import com.swirlds.config.api.ConfigData;
import com.swirlds.config.api.ConfigProperty;
import java.nio.file.Path;
import java.util.Objects;
import org.hiero.block.common.utils.Preconditions;
import org.hiero.block.node.base.CompressionType;
import org.hiero.block.node.base.Loggable;

/**
 * Use this configuration across the files recent plugin.
 *
 * @param liveRootPath provides the root path for saving blocks live
 * @param compression compression type to use for the storage. It is assumed this never changes while a node is running
 * and has existing files.
 * @param maxFilesPerDir number of files per directory. This is used to limit the number of files in a directory to avoid
 * file system issues.
 * @param retentionPolicyThreshold the retention policy threshold (count of blocks to keep). Must be positive.
 */
@ConfigData("files.recent")
public record FilesRecentConfig(
        @Loggable @ConfigProperty(defaultValue = "/opt/hiero/block-node/data/live") Path liveRootPath,
        @Loggable @ConfigProperty(defaultValue = "ZSTD") CompressionType compression,
        @Loggable @ConfigProperty(defaultValue = "3") int maxFilesPerDir,
        @Loggable @ConfigProperty(defaultValue = "96_000") long retentionPolicyThreshold) {
    /**
     * Constructor.
     */
    public FilesRecentConfig {
        Objects.requireNonNull(liveRootPath);
        Objects.requireNonNull(compression);
        Preconditions.requirePositive(maxFilesPerDir);
        Preconditions.requirePositive(retentionPolicyThreshold);
    }
}
