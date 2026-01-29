// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks;

import com.hedera.hapi.block.stream.Block;
import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/**
 * Handles the genesis STATE_CHANGES amendment for block 0.
 *
 * <p>Block 0 (genesis) requires STATE_CHANGES items that represent the initial
 * network state before any transactions. This class loads those state changes
 * from a JSON resource file and provides them for insertion during block wrapping.
 *
 * <p>The genesis state changes are inserted at {@link WrappedBlockIndex#STATE_CHANGES}
 * position in the wrapped block, before the BLOCK_FOOTER.
 *
 * <p>Block structure after amendment:
 * <pre>
 * [0] BLOCK_HEADER
 * [1] RECORD_FILE
 * [2+] STATE_CHANGES (genesis state items)
 * [N-1] BLOCK_FOOTER
 * [N] BLOCK_PROOF
 * </pre>
 */
public class GenesisStateAmendment {

    /** Genesis is always block 0 */
    private static final long GENESIS_BLOCK = 0L;

    /** Resource file containing the genesis state changes */
    private static final String GENESIS_STATE_FILE = "blockstream-round-33312259.json";

    /** The genesis STATE_CHANGES items */
    private final List<BlockItem> stateChanges;

    /** Whether genesis state was successfully loaded */
    private final boolean loaded;

    /**
     * Creates a GenesisStateAmendment for the specified network.
     *
     * @param network the network name (e.g., "mainnet", "testnet")
     */
    public GenesisStateAmendment(String network) {
        if ("mainnet".equalsIgnoreCase(network)) {
            List<BlockItem> items = loadGenesisState();
            this.stateChanges = items != null ? items : List.of();
            this.loaded = items != null && !items.isEmpty();
            if (loaded) {
                System.out.println("Loaded genesis STATE_CHANGES for block " + GENESIS_BLOCK + " with "
                        + stateChanges.size() + " items");
            }
        } else {
            this.stateChanges = List.of();
            this.loaded = false;
        }
    }

    /**
     * Creates a GenesisStateAmendment for mainnet (default).
     */
    public GenesisStateAmendment() {
        this("mainnet");
    }

    /**
     * Loads the genesis state changes from the JSON resource file.
     *
     * @return the list of STATE_CHANGES BlockItems, or null if loading failed
     */
    private List<BlockItem> loadGenesisState() {
        try (InputStream is = getClass().getClassLoader().getResourceAsStream(GENESIS_STATE_FILE)) {
            if (is == null) {
                System.err.println("Warning: Genesis state file not found: " + GENESIS_STATE_FILE);
                return null;
            }
            byte[] jsonBytes = is.readAllBytes();
            Block genesisBlock = Block.JSON.parse(Bytes.wrap(jsonBytes));
            List<BlockItem> items = genesisBlock.items();

            // Validate all items are STATE_CHANGES
            if (items != null) {
                for (BlockItem item : items) {
                    if (!item.hasStateChanges()) {
                        System.err.println("Warning: Genesis state file contains non-STATE_CHANGES item: "
                                + item.item().kind());
                    }
                }
            }
            return items;
        } catch (IOException | ParseException e) {
            System.err.println(
                    "Warning: Failed to load genesis state from " + GENESIS_STATE_FILE + ": " + e.getMessage());
            return null;
        }
    }

    /**
     * Checks if this is the genesis block that needs state changes.
     *
     * @param blockNumber the block number to check
     * @return true if this is block 0 and genesis state is loaded
     */
    public boolean isGenesisBlock(long blockNumber) {
        return blockNumber == GENESIS_BLOCK && loaded;
    }

    /**
     * Gets the genesis STATE_CHANGES items.
     *
     * @return the list of STATE_CHANGES BlockItems, or empty list if not loaded
     */
    public List<BlockItem> getStateChanges() {
        return stateChanges;
    }

    /**
     * Gets the genesis block number.
     *
     * @return 0 (genesis is always block 0)
     */
    public long getGenesisBlockNumber() {
        return GENESIS_BLOCK;
    }

    /**
     * Checks if genesis state was successfully loaded.
     *
     * @return true if genesis state is available
     */
    public boolean isLoaded() {
        return loaded;
    }
}
