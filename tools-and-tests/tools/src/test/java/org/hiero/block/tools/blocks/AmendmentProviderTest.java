// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks;

import static org.junit.jupiter.api.Assertions.*;

import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.hapi.streams.RecordStreamItem;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
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
        @DisplayName("getMissingRecordStreamItems returns empty list when index not available")
        void testGetMissingRecordStreamItemsNoIndex() {
            // When missing_transactions.gz doesn't exist, should return empty list
            MainnetAmendmentProvider provider = new MainnetAmendmentProvider();
            List<RecordStreamItem> items = provider.getMissingRecordStreamItems(100);
            assertNotNull(items, "Should return non-null list");
            assertTrue(items.isEmpty(), "Should return empty list when index not available");
        }
    }

    @Nested
    @DisplayName("TestnetAmendmentProvider tests")
    class TestnetAmendmentProviderTests {

        @Test
        @DisplayName("Network name is testnet")
        void testNetworkName() {
            TestnetAmendmentProvider provider = new TestnetAmendmentProvider();
            assertEquals("testnet", provider.getNetworkName());
        }

        @Test
        @DisplayName("hasGenesisAmendments always returns false (record streams are complete from genesis)")
        void testHasGenesisAmendments() {
            TestnetAmendmentProvider provider = new TestnetAmendmentProvider();
            assertFalse(provider.hasGenesisAmendments(0), "Block 0 should not have genesis amendments");
            assertFalse(provider.hasGenesisAmendments(1), "Block 1 should not have genesis amendments");
            assertFalse(provider.hasGenesisAmendments(-1), "Block -1 should not have genesis amendments");
            assertFalse(provider.hasGenesisAmendments(100), "Block 100 should not have genesis amendments");
        }

        @Test
        @DisplayName("getGenesisAmendments always returns empty list")
        void testGetGenesisAmendmentsNonGenesis() {
            TestnetAmendmentProvider provider = new TestnetAmendmentProvider();
            assertTrue(provider.getGenesisAmendments(0).isEmpty(), "Block 0 should have no genesis amendments");
            assertTrue(provider.getGenesisAmendments(1).isEmpty(), "Block 1 should have no genesis amendments");
        }

        @Test
        @DisplayName("getMissingRecordStreamItems returns empty list (default implementation)")
        void testGetMissingRecordStreamItems() {
            TestnetAmendmentProvider provider = new TestnetAmendmentProvider();
            List<RecordStreamItem> items = provider.getMissingRecordStreamItems(100);
            assertNotNull(items, "Should return non-null list");
            assertTrue(items.isEmpty(), "Testnet should not have missing transaction amendments");
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
        @DisplayName("getMissingRecordStreamItems always returns empty list (default implementation)")
        void testGetMissingRecordStreamItems() {
            NoOpAmendmentProvider provider = new NoOpAmendmentProvider();
            List<RecordStreamItem> items0 = provider.getMissingRecordStreamItems(0);
            List<RecordStreamItem> items1 = provider.getMissingRecordStreamItems(100);

            assertNotNull(items0, "Should return non-null list");
            assertTrue(items0.isEmpty(), "Block 0 missing items should be empty");
            assertNotNull(items1, "Should return non-null list");
            assertTrue(items1.isEmpty(), "Block 100 missing items should be empty");
        }
    }

    @Nested
    @DisplayName("createAmendmentProvider factory method tests")
    class CreateAmendmentProviderTests {

        @Test
        @DisplayName("Testnet creates TestnetAmendmentProvider with 'testnet' network name")
        void testTestnetCreatesTestnetProvider() {
            AmendmentProvider provider = AmendmentProvider.createAmendmentProvider("testnet");
            assertInstanceOf(TestnetAmendmentProvider.class, provider);
            assertEquals("testnet", provider.getNetworkName());
        }

        @Test
        @DisplayName("Mainnet creates MainnetAmendmentProvider")
        void testMainnetCreatesMainnetProvider() {
            AmendmentProvider provider = AmendmentProvider.createAmendmentProvider("mainnet");
            assertInstanceOf(MainnetAmendmentProvider.class, provider);
        }

        @Test
        @DisplayName("None creates NoOpAmendmentProvider")
        void testNoneCreatesNoOpProvider() {
            AmendmentProvider provider = AmendmentProvider.createAmendmentProvider("none");
            assertInstanceOf(NoOpAmendmentProvider.class, provider);
            assertEquals("none", provider.getNetworkName());
        }

        @Test
        @DisplayName("Disabled creates NoOpAmendmentProvider")
        void testDisabledCreatesNoOpProvider() {
            AmendmentProvider provider = AmendmentProvider.createAmendmentProvider("disabled");
            assertInstanceOf(NoOpAmendmentProvider.class, provider);
            assertEquals("none", provider.getNetworkName());
        }

        @Test
        @DisplayName("Factory method is case-insensitive")
        void testCaseInsensitive() {
            AmendmentProvider upper = AmendmentProvider.createAmendmentProvider("TESTNET");
            assertInstanceOf(TestnetAmendmentProvider.class, upper);
            assertEquals("testnet", upper.getNetworkName());

            AmendmentProvider mixed = AmendmentProvider.createAmendmentProvider("Mainnet");
            assertInstanceOf(MainnetAmendmentProvider.class, mixed);
        }

        @Test
        @DisplayName("Unknown network falls through to default NoOpAmendmentProvider and prints warning")
        void testUnknownNetworkFallsThrough() {
            PrintStream originalOut = System.out;
            ByteArrayOutputStream captured = new ByteArrayOutputStream();
            System.setOut(new PrintStream(captured));
            try {
                AmendmentProvider provider = AmendmentProvider.createAmendmentProvider("unknown");
                assertInstanceOf(NoOpAmendmentProvider.class, provider);
                assertEquals("unknown", provider.getNetworkName());
                String output = captured.toString();
                assertTrue(
                        output.contains("No specific amendments for network: unknown"),
                        "Expected warning message in System.out, got: " + output);
            } finally {
                System.setOut(originalOut);
            }
        }
    }

    @Nested
    @DisplayName("AmendmentProvider interface default methods")
    class InterfaceDefaultMethodTests {

        @Test
        @DisplayName("Default getMissingRecordStreamItems returns empty list")
        void testDefaultGetMissingRecordStreamItems() {
            // Create a minimal implementation that only implements required methods
            AmendmentProvider minimalProvider = new AmendmentProvider() {
                @Override
                public String getNetworkName() {
                    return "test";
                }

                @Override
                public boolean hasGenesisAmendments(long blockNumber) {
                    return false;
                }

                @Override
                public List<BlockItem> getGenesisAmendments(long blockNumber) {
                    return List.of();
                }
            };

            // The default method should return empty list
            List<RecordStreamItem> items = minimalProvider.getMissingRecordStreamItems(42);
            assertNotNull(items, "Default method should return non-null list");
            assertTrue(items.isEmpty(), "Default method should return empty list");
        }
    }
}
