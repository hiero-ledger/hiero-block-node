// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.server.persistence.storage.path;

import com.hedera.block.server.persistence.storage.PersistenceStorageConfig.CompressionType;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.nio.file.Path;
import java.util.Objects;
import org.hiero.block.common.utils.Preconditions;

/**
 * A record that represents a detailed path to a Block that is stored under the
 * archive storage root.
 */
public record ArchiveBlockPath(
        @NonNull Path dirPath,
        @NonNull String zipFileName,
        @NonNull String zipEntryName,
        @NonNull CompressionType compressionType,
        long blockNumber) {
    public ArchiveBlockPath {
        Objects.requireNonNull(dirPath);
        Objects.requireNonNull(zipFileName);
        Objects.requireNonNull(zipEntryName);
        Objects.requireNonNull(compressionType);
        Preconditions.requireWhole(blockNumber);
    }
}
