// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.verification.session;

import com.hedera.pbj.runtime.ParseException;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
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
    VerificationNotification processBlockItems(BlockItems blockItems) throws ParseException;
}
