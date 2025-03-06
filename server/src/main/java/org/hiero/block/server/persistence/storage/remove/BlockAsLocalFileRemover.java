// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.persistence.storage.remove;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import org.hiero.block.common.utils.FileUtilities;
import org.hiero.block.common.utils.Preconditions;
import org.hiero.block.server.persistence.storage.PersistenceStorageConfig.CompressionType;
import org.hiero.block.server.persistence.storage.path.BlockPathResolver;

/**
 * A Block remover that handles block-as-local-file.
 */
public final class BlockAsLocalFileRemover implements LocalBlockRemover {
    private final BlockPathResolver pathResolver;

    /**
     * Constructor.
     *
     * @param pathResolver valid, {@code non-null} instance of
     * {@link BlockPathResolver} used to resolve paths to block files
     */
    public BlockAsLocalFileRemover(@NonNull final BlockPathResolver pathResolver) {
        this.pathResolver = Objects.requireNonNull(pathResolver);
    }

    @Override
    public boolean removeLiveUnverified(final long blockNumber) throws IOException {
        Preconditions.requireWhole(blockNumber);
        final Path resolvedRawUnverifiedPath = pathResolver.resolveLiveRawUnverifiedPathToBlock(blockNumber);
        final CompressionType[] allCompressionTypes = CompressionType.values();
        for (int i = 0; i < allCompressionTypes.length; i++) {
            final CompressionType compressionType = allCompressionTypes[i];
            final Path compressionExtendedUnverifiedPath =
                    FileUtilities.appendExtension(resolvedRawUnverifiedPath, compressionType.getFileExtension());
            if (Files.deleteIfExists(compressionExtendedUnverifiedPath)) {
                return true;
            }
        }
        return false;
    }
}
