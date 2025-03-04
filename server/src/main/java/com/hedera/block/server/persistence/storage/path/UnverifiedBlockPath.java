// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.server.persistence.storage.path;

import com.hedera.block.common.utils.Preconditions;
import com.hedera.block.server.persistence.storage.PersistenceStorageConfig.CompressionType;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.nio.file.Path;
import java.util.Objects;

/**
 * A record that represents a detailed path to an unverified Block that is
 * stored under the unverified storage root.
 */
public record UnverifiedBlockPath(
        long blockNumber,
        @NonNull Path dirPath,
        @NonNull String blockFileName,
        @NonNull CompressionType compressionType) {
    public UnverifiedBlockPath {
        Preconditions.requireWhole(blockNumber);
        Objects.requireNonNull(dirPath);
        Objects.requireNonNull(blockFileName);
        Objects.requireNonNull(compressionType);
    }
}
