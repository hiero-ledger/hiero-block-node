// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.persistence.storage.path;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.nio.file.Path;
import org.hiero.block.server.persistence.storage.PersistenceStorageConfig.CompressionType;

/**
 * TODO: add documentation
 */
public record ArchiveBlockPath(
        @NonNull Path dirPath,
        @NonNull String zipFileName,
        @NonNull String zipEntryName,
        @NonNull CompressionType compressionType,
        long blockNumber) {}
