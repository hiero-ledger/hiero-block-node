// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.persistence.storage.write;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Objects;

/**
 * A record that represents the result of a block persistence operation.
 */
public record BlockPersistenceResult(long blockNumber, @NonNull BlockPersistenceStatus status) {
    public BlockPersistenceResult {
        Objects.requireNonNull(status);
    }

    /**
     * An enumeration of possible block persistence statuses.
     */
    public enum BlockPersistenceStatus {
        /**
         * The block was successfully persisted.
         */
        SUCCESS,
        /**
         * The block was not persisted due to a bad block number. Bad block
         * numbers are numbers that are strictly negative.
         */
        BAD_BLOCK_NUMBER,
        /**
         * The block was not persisted due to an incomplete block. A block is
         * considered incomplete if the block stream sends a new block header
         * without first sending a block proof.
         */
        INCOMPLETE_BLOCK,
        /**
         * The block was not persisted due to a failure during write. This
         * status will be returned if an IOException is thrown during the
         * writing of the block. All side effects that may have occurred during
         * the writing of the block will be rolled back.
         */
        FAILURE_DURING_WRITE,
        /**
         * The block was not persisted due to an interruption of the writer
         * during its operation. This status will be returned if the writer is
         * interrupted during the writing of the block. All side effects that
         * may have occurred during the writing of the block will be rolled
         * back.
         */
        PERSISTENCE_INTERRUPTED,
        /**
         * The block was not persisted due to a duplicate block. This status will
         * be returned if the block already exists in the storage AND is verified.
         */
        DUPLICATE_BLOCK,
        /**
         * A block persistence revert failed. This status will be returned if
         * the writer is unable to revert the persistence of the block. Occurs
         * during cleanup in case of an unsuccessful write. Usually, we should
         * never expect to see this status.
         */
        FAILURE_DURING_REVERT,
    }
}
