// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.server.persistence.storage.path;

import com.hedera.block.server.persistence.storage.PersistenceStorageConfig.CompressionType;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.nio.file.Path;
import java.util.Objects;
import org.hiero.block.common.utils.Preconditions;

/**
 * A record that represents a detailed path to a Block that is stored under the
 * live storage root.
 */
public record LiveBlockPath(
        long blockNumber,
        @NonNull Path dirPath,
        @NonNull String blockFileName,
        @NonNull CompressionType compressionType) {
    public LiveBlockPath {
        Preconditions.requireWhole(blockNumber);
        Objects.requireNonNull(dirPath);
        Objects.requireNonNull(blockFileName);
        Objects.requireNonNull(compressionType);
    }
}
