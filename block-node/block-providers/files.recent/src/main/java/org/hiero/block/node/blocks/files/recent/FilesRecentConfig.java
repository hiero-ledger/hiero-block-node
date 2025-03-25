// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.blocks.files.recent;

import com.swirlds.config.api.ConfigData;
import com.swirlds.config.api.ConfigProperty;
import java.nio.file.Path;
import java.util.Objects;
import org.hiero.block.node.base.CompressionType;
import org.hiero.block.node.base.Loggable;

/**
 * Use this configuration across the files recent plugin.
 *
 * @param liveRootPath provides the root path for saving blocks live
 * @param unverifiedRootPath provides the root path for unverified blocks
 * @param compression compression type to use for the storage. It is assumed this never changes while a node is running
 *                    and has existing files.
 * @param maxFilesPerDir number of files per directory. This is used to limit the number of files in a directory to avoid
 *                    file system issues.
 */
@ConfigData("persistence.storage")
public record FilesRecentConfig(
        @Loggable @ConfigProperty(defaultValue = "/opt/hashgraph/blocknode/data/live") Path liveRootPath,
        @Loggable @ConfigProperty(defaultValue = "/opt/hashgraph/blocknode/data/unverified") Path unverifiedRootPath,
        @Loggable @ConfigProperty(defaultValue = "ZSTD") CompressionType compression,
        @Loggable @ConfigProperty(defaultValue = "3") int maxFilesPerDir) {
    /**
     * Constructor.
     */
    public FilesRecentConfig {
        Objects.requireNonNull(liveRootPath);
        Objects.requireNonNull(unverifiedRootPath);
    }
}
