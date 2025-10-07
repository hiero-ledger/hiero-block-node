// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.verification.session;

import com.hedera.pbj.runtime.ParseException;
import java.util.List;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;

/**
 * A session for verifying a single block. A new session is created for each block to verify.
 */
public interface VerificationSession {
    /**
     * Processes the provided block items by updating the tree hashers.
     * If the last item has a block proof, final verification is triggered.
     *
     * @param blockItems the block items to process
     * @return VerificationNotification indicating the result of the verification if these items included the final
     * block proof otherwise null
     * @throws ParseException if a parsing error occurs
     */
    VerificationNotification processBlockItems(List<BlockItemUnparsed> blockItems) throws ParseException;
}
