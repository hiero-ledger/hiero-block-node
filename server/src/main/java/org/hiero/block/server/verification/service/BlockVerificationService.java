// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.verification.service;

import com.hedera.hapi.block.BlockItemUnparsed;
import com.hedera.pbj.runtime.ParseException;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.List;

/**
 * Service that handles the verification of block items, it receives items from the handler.
 */
public interface BlockVerificationService {
    /**
     * Everytime the handler receives a block item, it will call this method to verify the block item.
     *
     * @param blockItems the block items to add to the verification service
     * @throws ParseException if the block items are invalid
     */
    void onBlockItemsReceived(@NonNull List<BlockItemUnparsed> blockItems) throws ParseException;
}
