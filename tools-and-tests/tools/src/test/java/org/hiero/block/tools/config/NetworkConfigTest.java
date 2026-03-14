// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.config;

import static org.junit.jupiter.api.Assertions.*;

import java.time.LocalDate;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link NetworkConfig}.
 */
class NetworkConfigTest {

    @Nested
    @DisplayName("Testnet configuration")
    class TestnetConfigTests {

        private final NetworkConfig testnet = NetworkConfig.testnet();

        @Test
        @DisplayName("Network name is testnet")
        void testNetworkName() {
            assertEquals("testnet", testnet.networkName());
        }

        @Test
        @DisplayName("GCS bucket name includes 2024-02 suffix from testnet reset")
        void testGcsBucketName() {
            assertEquals("hedera-testnet-streams-2024-02", testnet.gcsBucketName());
        }

        @Test
        @DisplayName("Bucket path prefix is recordstreams/")
        void testBucketPathPrefix() {
            assertEquals("recordstreams/", testnet.bucketPathPrefix());
        }

        @Test
        @DisplayName("Mirror node API URL points to testnet")
        void testMirrorNodeApiUrl() {
            assertEquals("https://testnet.mirrornode.hedera.com/api/v1/", testnet.mirrorNodeApiUrl());
        }

        @Test
        @DisplayName("Genesis date is 2024-02-01")
        void testGenesisDate() {
            assertEquals(LocalDate.of(2024, 2, 1), testnet.genesisDate());
        }

        @Test
        @DisplayName("Genesis timestamp matches testnet first block")
        void testGenesisTimestamp() {
            assertEquals("2024-02-01T18_35_20.644859297Z", testnet.genesisTimestamp());
        }

        @Test
        @DisplayName("Node account ID range is 3 to 9 (7 nodes)")
        void testNodeAccountIdRange() {
            assertEquals(3, testnet.minNodeAccountId());
            assertEquals(9, testnet.maxNodeAccountId());
        }

        @Test
        @DisplayName("Total HBAR supply is 50 billion HBAR in tinybar")
        void testTotalHbarSupply() {
            assertEquals(5_000_000_000_000_000_000L, testnet.totalHbarSupplyTinybar());
        }

        @Test
        @DisplayName("Genesis address book resource is testnet-specific")
        void testGenesisAddressBookResource() {
            assertEquals("testnet-genesis-address-book.proto.bin", testnet.genesisAddressBookResource());
        }
    }

    @Nested
    @DisplayName("Mainnet configuration")
    class MainnetConfigTests {

        private final NetworkConfig mainnet = NetworkConfig.mainnet();

        @Test
        @DisplayName("Network name is mainnet")
        void testNetworkName() {
            assertEquals("mainnet", mainnet.networkName());
        }

        @Test
        @DisplayName("GCS bucket name is hedera-mainnet-streams")
        void testGcsBucketName() {
            assertEquals("hedera-mainnet-streams", mainnet.gcsBucketName());
        }

        @Test
        @DisplayName("Genesis date is 2019-09-13")
        void testGenesisDate() {
            assertEquals(LocalDate.of(2019, 9, 13), mainnet.genesisDate());
        }

        @Test
        @DisplayName("Node account ID range is 3 to 37")
        void testNodeAccountIdRange() {
            assertEquals(3, mainnet.minNodeAccountId());
            assertEquals(37, mainnet.maxNodeAccountId());
        }
    }

    @Nested
    @DisplayName("fromName factory method")
    class FromNameTests {

        @Test
        @DisplayName("fromName('testnet') returns testnet config")
        void testFromNameTestnet() {
            NetworkConfig config = NetworkConfig.fromName("testnet");
            assertEquals("testnet", config.networkName());
            assertEquals("hedera-testnet-streams-2024-02", config.gcsBucketName());
        }

        @Test
        @DisplayName("fromName('mainnet') returns mainnet config")
        void testFromNameMainnet() {
            NetworkConfig config = NetworkConfig.fromName("mainnet");
            assertEquals("mainnet", config.networkName());
        }

        @Test
        @DisplayName("fromName is case-insensitive")
        void testFromNameCaseInsensitive() {
            NetworkConfig config = NetworkConfig.fromName("TESTNET");
            assertEquals("testnet", config.networkName());
        }

        @Test
        @DisplayName("fromName throws for unknown network")
        void testFromNameUnknown() {
            assertThrows(IllegalArgumentException.class, () -> NetworkConfig.fromName("devnet"));
        }
    }
}
