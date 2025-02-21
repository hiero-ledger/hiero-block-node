// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.server.persistence.storage.archive;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A factory for creating {@link AsyncLocalBlockArchiver} instances.
 */
public interface AsyncLocalBlockArchiverFactory {
    /**
     * This method will create a newly constructed, valid and non-null instance
     * of {@link AsyncLocalBlockArchiver}.
     *
     * @param blockNumberThreshold the block number threshold passed, must
     * @return new, valid and non-null {@link AsyncLocalBlockArchiver} instance
     */
    @NonNull
    AsyncLocalBlockArchiver create(final long blockNumberThreshold);
}
