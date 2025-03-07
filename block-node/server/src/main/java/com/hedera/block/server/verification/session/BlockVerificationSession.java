// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.server.verification.session;

import com.hedera.block.server.verification.VerificationResult;
import com.hedera.hapi.block.BlockItemUnparsed;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Defines the contract for a block verification session.
 */
public interface BlockVerificationSession {

    /**
     * Append new block items to be processed by this verification session.
     *
     * @param blockItems the list of block items to process.
     */
    void appendBlockItems(@NonNull List<BlockItemUnparsed> blockItems);

    /**
     * Indicates whether the verification session is still running.
     *
     * @return true if running; false otherwise.
     */
    boolean isRunning();

    /**
     * Returns a future that completes with the verification result of the entire block
     * once verification is complete.
     *
     * @return a CompletableFuture for the verification result.
     */
    CompletableFuture<VerificationResult> getVerificationResult();
}
