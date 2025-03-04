// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.server.ack;

import com.hedera.block.server.persistence.StreamPersistenceHandlerImpl;
import com.hedera.block.server.persistence.storage.write.BlockPersistenceResult;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Interface for managing blocks, their persistence, verification, and failure.
 * Responsible for sending Block Acknowledgements to the producer.
 */
public interface AckHandler {
    /**
     * Register the persistence handler. Temporary solution due to lack of
     * critical infrastructure. This should be removed as soon as architectural
     * changes, which would allow us to publish results which would then be
     * picked up, are implemented.
     */
    void registerPersistence(@NonNull final StreamPersistenceHandlerImpl persistence);

    /**
     * Called when we receive a "persistence" result.
     */
    void blockPersisted(@NonNull final BlockPersistenceResult persistenceResult);

    /**
     * Called when we receive a "verified" event for the given blockNumber,
     * with the computed blockHash.
     */
    void blockVerified(long blockNumber, @NonNull Bytes blockHash);

    /**
     * Called by the Verification Service when we get a verification failure for the given blockNumber.
     */
    void blockVerificationFailed(long blockNumber);
}
