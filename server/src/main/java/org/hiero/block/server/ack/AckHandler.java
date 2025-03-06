// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.ack;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import org.hiero.block.server.persistence.storage.write.BlockPersistenceResult;

/**
 * Interface for managing blocks, their persistence, verification, and failure.
 * Responsible for sending Block Acknowledgements to the producer.
 */
public interface AckHandler {

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
