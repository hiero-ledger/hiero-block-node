// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks;

import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.hapi.streams.RecordStreamItem;
import java.util.List;
import org.hiero.block.tools.states.MainnetBlockZeroState;

/**
 * Amendment provider for Hedera mainnet.
 *
 * <p>Provides two types of amendments:
 * <ul>
 *   <li><b>Genesis amendments</b> - STATE_CHANGES for block 0 representing the initial
 *       network state at stream start (09/13/2019). These are inserted as BlockItems after
 *       BLOCK_HEADER and before RECORD_FILE so they represent state before any transactions.</li>
 *   <li><b>Missing transaction amendments</b> - RecordStreamItems that were missing from the
 *       original record stream. These are added to the RecordFileItem.amendments field,
 *       keeping original data unchanged while providing corrections separately.</li>
 * </ul>
 *
 * <p>Block structure with amendments:
 * <pre>
 * [0] BLOCK_HEADER
 * [1+] STATE_CHANGES (genesis state items, block 0 only)
 * [N-2] RECORD_FILE (with amendments in separate field)
 * [N-1] BLOCK_FOOTER
 * [N] BLOCK_PROOF
 * </pre>
 */
public class MainnetAmendmentProvider implements AmendmentProvider {

    /** Genesis is always block 0 */
    private static final long GENESIS_BLOCK = 0L;

    /** Lazy-initialized missing transactions index */
    private MissingTransactionsIndex missingTransactionsIndex;

    /** Flag indicating if we've attempted to load the index */
    private boolean indexLoadAttempted = false;

    /**
     * Creates a MainnetAmendmentProvider.
     */
    public MainnetAmendmentProvider() {
        System.out.println("Initialized mainnet amendment provider");
    }

    @Override
    public String getNetworkName() {
        return "mainnet";
    }

    @Override
    public boolean hasGenesisAmendments(long blockNumber) {
        return blockNumber == GENESIS_BLOCK;
    }

    @Override
    public List<BlockItem> getGenesisAmendments(long blockNumber) {
        if (blockNumber == GENESIS_BLOCK) {
            return loadGenesisState();
        }
        return List.of();
    }

    /**
     * Loads the genesis state for block 0.
     *
     * <p>The genesis state contains STATE_CHANGES items representing the initial
     * network state before any transactions were recorded. This state existed at
     * stream start (09/13/2019) due to network activity before recording began.
     *
     * @return the list of STATE_CHANGES BlockItems for genesis
     */
    private List<BlockItem> loadGenesisState() {
        return MainnetBlockZeroState.loadStartBlockZeroStateChanges();
    }

    @Override
    public List<RecordStreamItem> getMissingRecordStreamItems(long blockNumber) {
        MissingTransactionsIndex index = getOrLoadIndex();
        if (index == null) {
            return List.of();
        }
        return index.getTransactionsForBlock(blockNumber);
    }

    /**
     * Lazy-loads the missing transactions index.
     *
     * <p>The index is loaded on first access and cached for subsequent calls.
     * If the required files (missing_transactions.gz, block_times.bin) are not
     * available, null is returned and no amendments will be applied.
     *
     * @return the index, or null if not available
     */
    private synchronized MissingTransactionsIndex getOrLoadIndex() {
        if (!indexLoadAttempted) {
            indexLoadAttempted = true;
            try {
                missingTransactionsIndex = MissingTransactionsIndex.createDefault();
                if (missingTransactionsIndex != null) {
                    System.out.println("Loaded missing transactions index: "
                            + missingTransactionsIndex.getTotalTransactionCount() + " transactions across "
                            + missingTransactionsIndex.getBlockCount() + " blocks");
                }
            } catch (Exception e) {
                System.out.println("Warning: Could not load missing transactions index: " + e.getMessage());
                missingTransactionsIndex = null;
            }
        }
        return missingTransactionsIndex;
    }
}
