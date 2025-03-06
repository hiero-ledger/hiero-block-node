// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.persistence.storage.write;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Factory for creating the proper instance of an {@link AsyncBlockWriter}.
 */
public interface AsyncBlockWriterFactory {
    /**
     * Factory method, creates a new instance of an {@link AsyncBlockWriter}.
     *
     * @param blockNumber the block number for the block that this writer will
     * process, must be a valid block number
     * @return new, fully initialized instance of an {@link AsyncBlockWriter}
     */
    @NonNull
    AsyncBlockWriter create(final long blockNumber);
}
