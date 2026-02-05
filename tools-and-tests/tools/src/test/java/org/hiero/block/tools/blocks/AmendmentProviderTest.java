// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks;

import static org.junit.jupiter.api.Assertions.*;

import com.hedera.hapi.block.stream.BlockItem;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link AmendmentProvider} implementations.
 */
class AmendmentProviderTest {

    @Nested
    @DisplayName("MainnetAmendmentProvider tests")
    class MainnetAmendmentProviderTests {

        @Test
        @DisplayName("Network name is mainnet")
        void testNetworkName() {
            MainnetAmendmentProvider provider = new MainnetAmendmentProvider();
            assertEquals("mainnet", provider.getNetworkName());
        }

        @Test
        @DisplayName("hasGenesisAmendments returns true only for block 0")
        void testHasGenesisAmendments() {
            MainnetAmendmentProvider provider = new MainnetAmendmentProvider();
            assertTrue(provider.hasGenesisAmendments(0), "Block 0 should have genesis amendments");
            assertFalse(provider.hasGenesisAmendments(1), "Block 1 should not have genesis amendments");
            assertFalse(provider.hasGenesisAmendments(-1), "Block -1 should not have genesis amendments");
            assertFalse(provider.hasGenesisAmendments(100), "Block 100 should not have genesis amendments");
        }

        @Test
        @DisplayName("getGenesisAmendments returns empty list for non-genesis blocks")
        void testGetGenesisAmendmentsNonGenesis() {
            MainnetAmendmentProvider provider = new MainnetAmendmentProvider();
            List<BlockItem> amendments = provider.getGenesisAmendments(1);
            assertNotNull(amendments, "Amendments should not be null");
            assertTrue(amendments.isEmpty(), "Amendments should be empty for non-genesis blocks");
        }

        @Test
        @DisplayName("getGenesisAmendments returns state changes for block 0")
        void testGetGenesisAmendmentsBlockZero() {
            MainnetAmendmentProvider provider = new MainnetAmendmentProvider();
            List<BlockItem> amendments = provider.getGenesisAmendments(0);
            assertNotNull(amendments, "Amendments should not be null");
            assertFalse(amendments.isEmpty(), "Block 0 should have genesis state amendments");
            // Verify all items are STATE_CHANGES
            for (BlockItem item : amendments) {
                assertTrue(item.hasStateChanges(), "Genesis amendment should be a STATE_CHANGES item");
            }
        }

        @Test
        @DisplayName("hasTransactionAmendments returns false (not implemented yet)")
        void testHasTransactionAmendments() {
            MainnetAmendmentProvider provider = new MainnetAmendmentProvider();
            assertFalse(provider.hasTransactionAmendments(0), "Block 0 should not have transaction amendments yet");
            assertFalse(provider.hasTransactionAmendments(1), "Block 1 should not have transaction amendments yet");
        }

        @Test
        @DisplayName("getTransactionAmendments returns empty list (not implemented yet)")
        void testGetTransactionAmendments() {
            MainnetAmendmentProvider provider = new MainnetAmendmentProvider();
            assertTrue(provider.getTransactionAmendments(0).isEmpty(), "Transaction amendments should be empty");
            assertTrue(provider.getTransactionAmendments(1).isEmpty(), "Transaction amendments should be empty");
        }
    }

    @Nested
    @DisplayName("NoOpAmendmentProvider tests")
    class NoOpAmendmentProviderTests {

        @Test
        @DisplayName("Default constructor uses 'none' as network name")
        void testDefaultNetworkName() {
            NoOpAmendmentProvider provider = new NoOpAmendmentProvider();
            assertEquals("none", provider.getNetworkName());
        }

        @Test
        @DisplayName("Custom network name is preserved")
        void testCustomNetworkName() {
            NoOpAmendmentProvider provider = new NoOpAmendmentProvider("testnet");
            assertEquals("testnet", provider.getNetworkName());
        }

        @Test
        @DisplayName("hasGenesisAmendments always returns false")
        void testHasGenesisAmendments() {
            NoOpAmendmentProvider provider = new NoOpAmendmentProvider();
            assertFalse(provider.hasGenesisAmendments(0), "Block 0 should not have genesis amendments");
            assertFalse(provider.hasGenesisAmendments(1), "Block 1 should not have genesis amendments");
            assertFalse(provider.hasGenesisAmendments(100), "Block 100 should not have genesis amendments");
        }

        @Test
        @DisplayName("getGenesisAmendments always returns empty list")
        void testGetGenesisAmendments() {
            NoOpAmendmentProvider provider = new NoOpAmendmentProvider();
            assertTrue(provider.getGenesisAmendments(0).isEmpty(), "Block 0 genesis amendments should be empty");
            assertTrue(provider.getGenesisAmendments(1).isEmpty(), "Block 1 genesis amendments should be empty");
        }

        @Test
        @DisplayName("hasTransactionAmendments always returns false")
        void testHasTransactionAmendments() {
            NoOpAmendmentProvider provider = new NoOpAmendmentProvider();
            assertFalse(provider.hasTransactionAmendments(0), "Block 0 should not have transaction amendments");
            assertFalse(provider.hasTransactionAmendments(1), "Block 1 should not have transaction amendments");
        }

        @Test
        @DisplayName("getTransactionAmendments always returns empty list")
        void testGetTransactionAmendments() {
            NoOpAmendmentProvider provider = new NoOpAmendmentProvider();
            assertTrue(
                    provider.getTransactionAmendments(0).isEmpty(), "Block 0 transaction amendments should be empty");
            assertTrue(
                    provider.getTransactionAmendments(1).isEmpty(), "Block 1 transaction amendments should be empty");
        }
    }
}
