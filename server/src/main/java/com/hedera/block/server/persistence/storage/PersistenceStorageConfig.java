// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.server.persistence.storage;

import static com.hedera.block.server.Constants.BLOCK_NODE_ARCHIVE_ROOT_DIRECTORY_SEMANTIC_NAME;
import static com.hedera.block.server.Constants.BLOCK_NODE_LIVE_ROOT_DIRECTORY_SEMANTIC_NAME;

import com.hedera.block.common.utils.Preconditions;
import com.hedera.block.common.utils.StringUtilities;
import com.hedera.block.server.config.logging.Loggable;
import com.swirlds.config.api.ConfigData;
import com.swirlds.config.api.ConfigProperty;
import com.swirlds.config.api.validation.annotation.Max;
import com.swirlds.config.api.validation.annotation.Min;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

/**
 * Use this configuration across the persistence storage package.
 *
 * @param liveRootPath provides the root path for saving blocks live
 * @param archiveRootPath provides the root path for archived blocks
 * @param type storage type
 * @param compression compression type to use for the storage
 * @param compressionLevel compression level used by the compression algorithm
 * Non-PRODUCTION values should only be used for troubleshooting and development purposes.
 */
@ConfigData("persistence.storage")
public record PersistenceStorageConfig(
        // @todo(#371) - the default life/archive root path must be absolute starting from /opt
        @Loggable @ConfigProperty(defaultValue = "") String liveRootPath,
        // @todo(#371) - the default life/archive root path must be absolute starting from /opt
        @Loggable @ConfigProperty(defaultValue = "") String archiveRootPath,
        @Loggable @ConfigProperty(defaultValue = "BLOCK_AS_LOCAL_FILE") StorageType type,
        @Loggable @ConfigProperty(defaultValue = "ZSTD") CompressionType compression,
        @Loggable @ConfigProperty(defaultValue = "3") @Min(0) @Max(20) int compressionLevel,
        @Loggable @ConfigProperty(defaultValue = "BLOCK_AS_LOCAL_FILE") ArchiveType archiveType,
        @Loggable @ConfigProperty(defaultValue = "1_000")
                int archiveBatchSize) { // @todo(517) rename batch to group size
    // @todo(#371) - the default life/archive root path must be absolute starting from /opt
    private static final String LIVE_ROOT_PATH =
            Path.of("hashgraph/blocknode/data/live/").toAbsolutePath().toString();
    // @todo(#371) - the default life/archive root path must be absolute starting from /opt
    private static final String ARCHIVE_ROOT_PATH =
            Path.of("hashgraph/blocknode/data/archive/").toAbsolutePath().toString();

    /**
     * Constructor.
     */
    public PersistenceStorageConfig {
        Objects.requireNonNull(type);
        Preconditions.requirePositivePowerOf10(archiveBatchSize);
        compression.verifyCompressionLevel(compressionLevel);
        liveRootPath = resolvePath(liveRootPath, LIVE_ROOT_PATH, BLOCK_NODE_LIVE_ROOT_DIRECTORY_SEMANTIC_NAME);
        archiveRootPath =
                resolvePath(archiveRootPath, ARCHIVE_ROOT_PATH, BLOCK_NODE_ARCHIVE_ROOT_DIRECTORY_SEMANTIC_NAME);
    }

    /**
     * This method attempts to resolve a given configured path. If the input
     * path is blank, a default path is used. The resolved path must be
     * absolute! If the path resolution is successful, at attempt is made to
     * create the directory path. If the directory path cannot be created, an
     * {@link UncheckedIOException} is thrown.
     *
     * @param pathToResolve the path to resolve
     * @param defaultIfBlank the default path if the path to resolve is blank
     * @param semanticPathName the semantic name of the path used for logging
     * @return the resolved path
     * @throws IllegalArgumentException if the resolved path is not absolute
     * @throws UncheckedIOException if the resolved path cannot be created
     */
    @NonNull
    private String resolvePath(
            final String pathToResolve, @NonNull final String defaultIfBlank, @NonNull final String semanticPathName) {
        final Path normalized = getNormalizedPath(pathToResolve, defaultIfBlank);
        createDirectoryPath(normalized, semanticPathName);
        return normalized.toString();
    }

    /**
     * This method normalizes a given path. If the path to normalize is blank,
     * a default path is used. The normalized path must be absolute!
     *
     * @param pathToNormalize the path to normalize
     * @param defaultIfBlank the default path if the path to normalize is blank
     * @throws IllegalArgumentException if the path to normalize is not absolute
     */
    @NonNull
    private Path getNormalizedPath(final String pathToNormalize, @NonNull final String defaultIfBlank) {
        final String actualToNormalize = StringUtilities.isBlank(pathToNormalize) ? defaultIfBlank : pathToNormalize;
        return Path.of(actualToNormalize).normalize().toAbsolutePath();
    }

    /**
     * This method creates a directory path at the given target path. If the
     * directory path cannot be created, an {@link UncheckedIOException} is
     * thrown.
     *
     * @param targetPath the target path to create the directory path
     * @param semanticPathName the semantic name of the path used for logging
     * @throws UncheckedIOException if the directory path cannot be created
     */
    private void createDirectoryPath(@NonNull final Path targetPath, @NonNull final String semanticPathName) {
        try {
            Files.createDirectories(targetPath);
        } catch (final IOException e) {
            final String classname = this.getClass().getName();
            final String message = "Unable to instantiate [%s]! Unable to create the [%s] path that was provided!"
                    .formatted(classname, semanticPathName);
            throw new UncheckedIOException(message, e);
        }
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
            Preconditions.requireInRange(levelToCheck, minCompressionLevel, maxCompressionLevel);
        }

        public String getFileExtension() {
            return fileExtension;
        }
    }

    /**
     * An enum that reflects the type of archive that is used to archive
     * the blocks that are stored within the persistence storage.
     */
    public enum ArchiveType {
        /**
         * This type of archive is used to archive the blocks as individual files
         * with the Block number as a unique file name and persisted in a trie
         * structure with digit-per-folder. Acts in a very similar fashion as
         * {@link StorageType#BLOCK_AS_LOCAL_FILE} in terms of how the blocks are
         * stored, but instead of having a directory where certain blocks must
         * reside based on the trie structure, there will be a zip in its place.
         * Entries inside the zip are continued to be resolved as if they were
         * part of the trie structure, meaning the actual files are saved under
         * directories where they would usually reside as if they were saved
         * under the block-as-file live persistence trie.
         * <pre>
         *     E.G.
         *     Imagine we are archiving each 1000 blocks. So if we want to
         *     archive blocks 1000-1999, here is how the archive would look
         *     juxtaposed with the live root path:
         *
         *     LIVE ROOT PATH:
         *     (folders are up to dept 18, long max digit size - 1):
         *     /live/0/0/0.../1/0/0/0000000000000001000.blk
         *     ...
         *     /live/0/0/0.../1/0/0/0000000000000001999.blk
         *
         *     ARCHIVE ROOT PATH:
         *     (folders are up to dept 18, long max digit size - 1):
         *     /archive/0/0/0.../1.zip/0/0/0000000000000001000.blk
         *     ...
         *     /archive/0/0/0.../1.zip/0/0/0000000000000001999.blk
         * </pre>
         */
        BLOCK_AS_LOCAL_FILE,
        /**
         * This type of archive does nothing, essentially acting as if it were
         * disabled.
         */
        NO_OP
    }
}
