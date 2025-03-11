// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.persistence.storage.remove;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;
import org.hiero.block.common.utils.Preconditions;
import org.hiero.block.server.persistence.storage.path.BlockPathResolver;
import org.hiero.block.server.persistence.storage.path.UnverifiedBlockPath;

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
    public boolean removeUnverified(final long blockNumber) throws IOException {
        Preconditions.requireWhole(blockNumber);
        final Optional<UnverifiedBlockPath> optPath = pathResolver.findUnverifiedBlock(blockNumber);
        if (optPath.isPresent()) {
            final UnverifiedBlockPath path = optPath.get();
            final Path targetPath = path.dirPath().resolve(path.blockFileName());
            return Files.deleteIfExists(targetPath);
        } else {
            return false;
        }
    }
}
