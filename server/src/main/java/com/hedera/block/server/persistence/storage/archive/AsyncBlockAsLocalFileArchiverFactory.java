// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.server.persistence.storage.archive;

import com.hedera.block.server.persistence.storage.PersistenceStorageConfig;
import com.hedera.block.server.persistence.storage.path.BlockPathResolver;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Objects;
import javax.inject.Inject;

/**
 * A factory for creating {@link AsyncBlockAsLocalFileArchiver} instances.
 */
public final class AsyncBlockAsLocalFileArchiverFactory implements AsyncLocalBlockArchiverFactory {
    private final PersistenceStorageConfig config;
    private final BlockPathResolver pathResolver;

    @Inject
    public AsyncBlockAsLocalFileArchiverFactory(
            @NonNull final PersistenceStorageConfig config, @NonNull final BlockPathResolver pathResolver) {
        this.config = Objects.requireNonNull(config);
        this.pathResolver = Objects.requireNonNull(pathResolver);
    }

    @NonNull
    @Override
    public AsyncLocalBlockArchiver create(final long blockNumberThreshold) {
        return new AsyncBlockAsLocalFileArchiver(blockNumberThreshold, config, pathResolver);
    }
}
