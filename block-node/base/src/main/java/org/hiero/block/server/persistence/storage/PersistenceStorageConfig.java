// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.persistence.storage;

import com.swirlds.config.api.ConfigData;
import com.swirlds.config.api.ConfigProperty;
import com.swirlds.config.api.validation.annotation.Max;
import com.swirlds.config.api.validation.annotation.Min;
import java.nio.file.Path;
import java.util.Objects;
import org.hiero.block.common.utils.Preconditions;
import org.hiero.block.server.config.logging.Loggable;

/**
 * Use this configuration across the persistence storage package.
 *
 * @param liveRootPath provides the root path for saving blocks live
 * @param archiveRootPath provides the root path for archived blocks
 * @param type storage type
 * @param compression compression type to use for the storage
 * @param compressionLevel compression level used by the compression algorithm
 * Non-PRODUCTION values should only be used for troubleshooting and development purposes.
 * @param archiveGroupSize the number of blocks to archive in a single group
 */
@ConfigData("persistence.storage")
public record PersistenceStorageConfig(
        @Loggable @ConfigProperty(defaultValue = "/opt/hashgraph/blocknode/data/live") Path liveRootPath,
        @Loggable @ConfigProperty(defaultValue = "/opt/hashgraph/blocknode/data/archive") Path archiveRootPath,
        @Loggable @ConfigProperty(defaultValue = "/opt/hashgraph/blocknode/data/unverified") Path unverifiedRootPath,
        @Loggable @ConfigProperty(defaultValue = "BLOCK_AS_LOCAL_FILE") StorageType type,
        @Loggable @ConfigProperty(defaultValue = "ZSTD") CompressionType compression,
        @Loggable @ConfigProperty(defaultValue = "3") @Min(0) @Max(20) int compressionLevel,
        @Loggable @ConfigProperty(defaultValue = "1_000") @Min(10) int archiveGroupSize,
        @Loggable @ConfigProperty(defaultValue = "1024") @Min(64) @Max(2048) int executionQueueLimit,
        @Loggable @ConfigProperty(defaultValue = "THREAD_POOL") ExecutorType executorType,
        @Loggable @ConfigProperty(defaultValue = "6") @Min(1) @Max(16) int threadCount,
        @Loggable @ConfigProperty(defaultValue = "60000") @Min(0) long threadKeepAliveTime,
        @Loggable @ConfigProperty(defaultValue = "false") boolean useVirtualThreads) {
    /**
     * Constructor.
     */
    public PersistenceStorageConfig {
        Objects.requireNonNull(liveRootPath);
        Objects.requireNonNull(archiveRootPath);
        Objects.requireNonNull(unverifiedRootPath);
        Objects.requireNonNull(type);
        Objects.requireNonNull(executorType);
        compression.verifyCompressionLevel(compressionLevel);
        // @todo(742) verify that the group size has not changed once it has
        //    been set initially
        Preconditions.requireGreaterOrEqual(
                archiveGroupSize,
                10,
                "persistence.storage.archiveGroupSize [%d] is required to be greater or equal than [%d].");
        Preconditions.requirePositivePowerOf10(
                archiveGroupSize,
                "persistence.storage.archiveGroupSize [%d] is required to be a positive power of 10.");
        Preconditions.requirePositivePowerOf10(archiveGroupSize);
        Preconditions.requireWhole(threadKeepAliveTime);
        Preconditions.requireInRange(
                executionQueueLimit,
                64,
                2048,
                "persistence.storage.executionQueueLimit [%d] is required to be between [%d] and [%d].");
        Preconditions.requireInRange(
                threadCount, 1, 16, "persistence.storage.threadCount [%d] is required to be between [%d] and [%d].");
    }

    /**
     * An enum that reflects the type of Block Storage Persistence that is
     * currently used within the given server instance. During runtime one
     * should only query for the storage type that was configured by calling
     * {@link PersistenceStorageConfig#type()} on an instance of the persistence
     * storage config that was only constructed via
     * {@link com.swirlds.config.api.Configuration#getConfigData(Class)}!
     */
    public enum StorageType {
        /**
         * This type of storage stores Blocks as individual files with the Block
         * number as a unique file name and persisted in a trie structure with
         * digit-per-folder
         * (see <a href="https://github.com/hashgraph/hedera-block-node/issues/125">#125</a>).
         * This is also the default setting for the server if it is not
         * explicitly specified via an environment variable or app.properties.
         */
        BLOCK_AS_LOCAL_FILE,
        /**
         * This type of storage does nothing.
         */
        NO_OP
    }

    /**
     * An enum that defines the type of executor to use for async writers.
     * <p>
     * Different executor types have different performance characteristics and are suitable
     * for different workloads and deployment environments.
     */
    public enum ExecutorType {
        /**
         * A fixed-size thread pool executor that maintains a specified number of threads.
         * Recommended for environments where consistent performance and resource usage are important.
         */
        THREAD_POOL,

        /**
         * An executor with a single worker thread.
         * Suitable for low-throughput environments or when strict ordering of task execution is required.
         */
        SINGLE_THREAD,

        /**
         * Uses the ForkJoinPool for task execution.
         * Best for compute-intensive workloads that benefit from work-stealing.
         */
        FORK_JOIN,
    }

    /**
     * An enum that reflects the type of compression that is used to compress
     * the blocks that are stored within the persistence storage.
     */
    public enum CompressionType {
        /**
         * This type of compression is used to compress the blocks using the
         * `Zstandard` algorithm.
         */
        ZSTD(0, 20, ".zstd"),
        /**
         * This type means no compression will be done.
         */
        NONE(Integer.MIN_VALUE, Integer.MAX_VALUE, "");

        private final int minCompressionLevel;
        private final int maxCompressionLevel;
        private final String fileExtension;

        CompressionType(final int minCompressionLevel, final int maxCompressionLevel, final String fileExtension) {
            this.minCompressionLevel = minCompressionLevel;
            this.maxCompressionLevel = maxCompressionLevel;
            this.fileExtension = fileExtension;
        }

        public void verifyCompressionLevel(final int levelToCheck) {
            Preconditions.requireInRange(
                    levelToCheck,
                    minCompressionLevel,
                    maxCompressionLevel,
                    "persistence.storage.compressionLevel [%d] is required to be in the range [%d, %d] boundaries included.");
        }

        public String getFileExtension() {
            return fileExtension;
        }
    }
}
