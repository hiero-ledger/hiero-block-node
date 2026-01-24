// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks;

import static org.junit.jupiter.api.Assertions.*;

import com.hedera.hapi.block.stream.BlockItem;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link GenesisStateAmendment}.
 */
class GenesisStateAmendmentTest {

    @Test
    @DisplayName("Genesis block number is always 0")
    void testGenesisBlockNumber() {
        GenesisStateAmendment amendment = new GenesisStateAmendment();
        assertEquals(0L, amendment.getGenesisBlockNumber());
    }

    @Test
    @DisplayName("isGenesisBlock returns true only for block 0 when loaded")
    void testIsGenesisBlock() {
        GenesisStateAmendment amendment = new GenesisStateAmendment();

        if (amendment.isLoaded()) {
            assertTrue(amendment.isGenesisBlock(0), "Block 0 should be genesis");
            assertFalse(amendment.isGenesisBlock(1), "Block 1 should not be genesis");
            assertFalse(amendment.isGenesisBlock(-1), "Block -1 should not be genesis");
            assertFalse(amendment.isGenesisBlock(100), "Block 100 should not be genesis");
        } else {
            // If not loaded, isGenesisBlock should return false even for block 0
            assertFalse(amendment.isGenesisBlock(0), "Block 0 should not be genesis when not loaded");
        }
    }

    @Test
    @DisplayName("Non-mainnet network has no genesis state")
    void testNonMainnetNetwork() {
        GenesisStateAmendment testnetAmendment = new GenesisStateAmendment("testnet");
        assertFalse(testnetAmendment.isLoaded(), "Testnet should not have genesis state loaded");
        assertFalse(testnetAmendment.isGenesisBlock(0), "Testnet block 0 should not be genesis");
        assertTrue(testnetAmendment.getStateChanges().isEmpty(), "Testnet should have empty state changes");
    }

    @Test
    @DisplayName("Unknown network has no genesis state")
    void testUnknownNetwork() {
        GenesisStateAmendment unknownAmendment = new GenesisStateAmendment("unknown");
        assertFalse(unknownAmendment.isLoaded(), "Unknown network should not have genesis state loaded");
        assertTrue(unknownAmendment.getStateChanges().isEmpty(), "Unknown network should have empty state changes");
    }

    @Test
    @DisplayName("Mainnet genesis state contains STATE_CHANGES items")
    void testMainnetGenesisStateChanges() {
        GenesisStateAmendment amendment = new GenesisStateAmendment("mainnet");

        if (amendment.isLoaded()) {
            List<BlockItem> stateChanges = amendment.getStateChanges();
            assertNotNull(stateChanges, "State changes should not be null");
            assertFalse(stateChanges.isEmpty(), "State changes should not be empty");

            // Verify all items are STATE_CHANGES type
            for (BlockItem item : stateChanges) {
                assertTrue(
                        item.hasStateChanges(),
                        "All genesis items should be STATE_CHANGES, found: "
                                + item.item().kind());
            }
        }
    }

    @Test
    @DisplayName("getStateChanges returns empty list when not loaded")
    void testGetStateChangesWhenNotLoaded() {
        GenesisStateAmendment amendment = new GenesisStateAmendment("testnet");
        List<BlockItem> stateChanges = amendment.getStateChanges();
        assertNotNull(stateChanges, "State changes should not be null");
        assertTrue(stateChanges.isEmpty(), "State changes should be empty for non-mainnet");
    }

    @Test
    @DisplayName("Default constructor uses mainnet")
    void testDefaultConstructor() {
        GenesisStateAmendment defaultAmendment = new GenesisStateAmendment();
        GenesisStateAmendment mainnetAmendment = new GenesisStateAmendment("mainnet");

        assertEquals(defaultAmendment.isLoaded(), mainnetAmendment.isLoaded());
        assertEquals(
                defaultAmendment.getStateChanges().size(),
                mainnetAmendment.getStateChanges().size());
    }
}
