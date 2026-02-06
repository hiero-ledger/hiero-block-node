// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks;

import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.hapi.streams.RecordStreamItem;
import java.util.List;

/**
 * Interface for providing network-specific amendments to blocks during conversion.
 *
 * <p>Different networks may require different amendments to be inserted into blocks.
 * For example, mainnet requires genesis STATE_CHANGES for block 0, while other networks
 * may not need any amendments.
 *
 * <p>There are two types of amendments:
 * <ul>
 *   <li><b>Genesis amendments</b> - STATE_CHANGES for block 0 representing initial network state.
 *       These are inserted after BLOCK_HEADER and before the stream of block items.</li>
 *   <li><b>Missing transaction amendments</b> - RecordStreamItems that were missing from the
 *       original record stream. These are merged into the RecordStreamFile within the block.</li>
 * </ul>
 *
 * <p>Implementations of this interface can be selected via CLI flags to support
 * processing blocks from different networks.
 */
public interface AmendmentProvider {

    /**
     * Gets the network name this provider handles.
     *
     * @return the network name (e.g., "mainnet", "testnet", "none")
     */
    String getNetworkName();

    /**
     * Checks if the specified block requires genesis amendments.
     * Genesis amendments are STATE_CHANGES representing initial network state for block 0.
     *
     * @param blockNumber the block number to check
     * @return true if this block needs genesis amendments (typically only block 0)
     */
    boolean hasGenesisAmendments(long blockNumber);

    /**
     * Gets the genesis amendments (STATE_CHANGES) for block 0.
     *
     * <p>Genesis amendments are inserted after BLOCK_HEADER and before RECORD_FILE,
     * so they represent the initial state before any transactions are processed.
     *
     * @param blockNumber the block number (should be 0 for genesis)
     * @return the list of STATE_CHANGES BlockItems for genesis, or empty list if none
     */
    List<BlockItem> getGenesisAmendments(long blockNumber);

    /**
     * Gets the missing RecordStreamItems that should be merged into the specified block.
     *
     * <p>Missing transactions are transactions that were not included in the original
     * record stream but should have been. These are loaded from the mirror node errata
     * data and merged into the RecordStreamFile within the block.
     *
     * @param blockNumber the block number to get missing transactions for
     * @return the list of RecordStreamItems to merge, or empty list if none
     */
    default List<RecordStreamItem> getMissingRecordStreamItems(long blockNumber) {
        return List.of();
    }

    /**
     * Creates an amendment provider based on the network name.
     *
     * @param network the network name (e.g., "mainnet", "testnet", "none")
     * @return the appropriate AmendmentProvider for the network
     */
    static AmendmentProvider createAmendmentProvider(String network) {
        return switch (network.toLowerCase()) {
            case "mainnet" -> new MainnetAmendmentProvider();
            case "none", "disabled" -> new NoOpAmendmentProvider();
            default -> {
                System.out.println("No specific amendments for network: " + network + ", using no-op provider");
                yield new NoOpAmendmentProvider(network);
            }
        };
    }
}
