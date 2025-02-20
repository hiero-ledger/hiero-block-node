// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.server.persistence.storage.archive;

import edu.umd.cs.findbugs.annotations.NonNull;
import javax.inject.Inject;

/**
 * A factory for creating {@link AsyncNoOpArchiver} instances.
 */
public final class AsyncNoOpArchiverFactory implements AsyncLocalBlockArchiverFactory {
    @Inject
    public AsyncNoOpArchiverFactory() {}

    @NonNull
    @Override
    public AsyncLocalBlockArchiver create(final long blockNumberThreshold) {
        return new AsyncNoOpArchiver(blockNumberThreshold);
    }
}
