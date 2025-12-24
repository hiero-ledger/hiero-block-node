// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.verification.session;

import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.List;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;

/**
 * A session for verifying a single block. A new session is created for each block to verify.
 */
public interface VerificationSession {
    /**
     * Processes the provided block items by updating the tree hashers.
     *
     * @param blockItems the block items to process
     * @throws ParseException if a parsing error occurs
     */
    void processBlockItems(List<BlockItemUnparsed> blockItems) throws ParseException;

    /**
     * Finalizes the verification process and constructs a VerificationNotification.
     * @param rootHashOfAllBlockHashesTree the known root hash of all block hashes tree
     * @param previousBlockHash the known previous block hash
     * @return the verification notification with result either success or failure
     */
    VerificationNotification finalizeVerification(Bytes rootHashOfAllBlockHashesTree, Bytes previousBlockHash);
}
