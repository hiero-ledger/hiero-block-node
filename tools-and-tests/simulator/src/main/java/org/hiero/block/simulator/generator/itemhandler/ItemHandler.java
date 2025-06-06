// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.generator.itemhandler;

import com.hedera.hapi.block.stream.protoc.BlockItem;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.simulator.exception.BlockSimulatorParsingException;

/**
 * Interface defining the contract for handling different types of block items.
 * Implementations handle specific types of items like block headers, proofs, events, and transactions.
 */
public interface ItemHandler {
    /**
     * Returns the block item in its protobuf format.
     *
     * @return The constructed BlockItem
     */
    BlockItem getItem();

    /**
     * Converts the block item to its unparsed format.
     *
     * @return The block item in unparsed format
     * @throws BlockSimulatorParsingException if there is an error parsing the block item
     */
    BlockItemUnparsed unparseBlockItem() throws BlockSimulatorParsingException;
}
