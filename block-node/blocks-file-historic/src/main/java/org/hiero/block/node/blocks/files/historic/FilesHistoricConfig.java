// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.blocks.files.historic;

import com.swirlds.config.api.ConfigData;
import com.swirlds.config.api.ConfigProperty;
import com.swirlds.config.api.validation.annotation.Max;
import com.swirlds.config.api.validation.annotation.Min;
import java.nio.file.Path;
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
 * @param stagedBlockNotificationsEnabled whether a Persisted Notification is sent for every block as soon as it
 * is staged, rather than once per zip batch (using the last block number of the batch). Defaults to disabled, so
 * that by default only a single Persisted Notification is sent per successfully archived zip batch, matching the
 * plugin's legacy behavior. Note: a staged block is not retrievable via {@code block(long)} until its batch is
 * zipped.
 * @param cachedZipAccessorEnabled if enabled, block reads share a small, bounded cache of open zip archive
 * filesystems ({@link CachedZipBlockAccessor}) instead of each read opening its own via a temporary hard link
 * ({@link ZipBlockAccessor}, the default). The cached accessor avoids repeatedly reopening the same archive for
 * consecutive reads, at the cost of sharing one open filesystem across concurrent readers of that archive.
 * @param maxCachedZipArchives maximum number of open zip archive filesystems to keep cached (shared and
 * reference-counted across concurrent readers) at once when {@link #cachedZipAccessorEnabled} is set. Reads for
 * blocks in the same archive reuse the cached filesystem instead of reopening it; archives not currently in use
 * are evicted least-recently-used first once this many are cached.
 */
@ConfigData("files.historic")
public record FilesHistoricConfig(
        // spotless:off - long annotations on record components must stay on one line
        @Loggable @ConfigProperty(defaultValue = "/opt/hiero/block-node/data/historic") Path rootPath,
        @Loggable @ConfigProperty(defaultValue = "ZSTD") CompressionType compression,
        @Loggable @ConfigProperty(defaultValue = "4") @Min(1) @Max(6) int powersOfTenPerZipFileContents,
        @Loggable @ConfigProperty(defaultValue = "0") @Min(0) long blockRetentionThreshold,
        @Loggable @ConfigProperty(defaultValue = "3") @Min(1) int maxFilesPerDir,
        @Loggable @ConfigProperty(defaultValue = "false") boolean stagedBlockNotificationsEnabled,
        @Loggable @ConfigProperty(defaultValue = "false") boolean cachedZipAccessorEnabled,
        @Loggable @ConfigProperty(defaultValue = "8") @Min(1) int maxCachedZipArchives) {
        // spotless:on
}
