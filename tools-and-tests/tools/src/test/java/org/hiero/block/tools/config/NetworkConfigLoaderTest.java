// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.config;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class NetworkConfigLoaderTest {

    @TempDir
    Path tempDir;

    @Test
    void testLoadFromPath() throws IOException {
        // Create a test config file
        final String json = """
                {
                  "networkName": "testnet-custom",
                  "gcsBucketName": "test-bucket",
                  "bucketPathPrefix": "recordstreams/",
                  "mirrorNodeApiUrl": "https://test.mirrornode.example.com/api/v1/",
                  "genesisDate": "2024-01-01",
                  "genesisTimestamp": "2024-01-01T00_00_00.000000000Z",
                  "minNodeAccountId": 3,
                  "maxNodeAccountId": 9,
                  "totalHbarSupplyTinybar": 5000000000000000000,
                  "genesisAddressBookResource": "test-address-book.proto.bin"
                }
                """;

        final Path configPath = tempDir.resolve("test-config.json");
        Files.writeString(configPath, json);

        // Load the config
        final NetworkConfig config = NetworkConfigLoader.loadFromPath(configPath);

        // Verify all fields
        assertEquals("testnet-custom", config.networkName());
        assertEquals("test-bucket", config.gcsBucketName());
        assertEquals("recordstreams/", config.bucketPathPrefix());
        assertEquals("https://test.mirrornode.example.com/api/v1/", config.mirrorNodeApiUrl());
        assertEquals(LocalDate.of(2024, 1, 1), config.genesisDate());
        assertEquals("2024-01-01T00_00_00.000000000Z", config.genesisTimestamp());
        assertEquals(3, config.minNodeAccountId());
        assertEquals(9, config.maxNodeAccountId());
        assertEquals(5000000000000000000L, config.totalHbarSupplyTinybar());
        assertEquals("test-address-book.proto.bin", config.genesisAddressBookResource());
    }

    @Test
    void testLoadFromPath_FileNotFound() {
        final Path nonExistentPath = tempDir.resolve("non-existent.json");

        final IllegalArgumentException exception =
                assertThrows(IllegalArgumentException.class, () -> NetworkConfigLoader.loadFromPath(nonExistentPath));

        assertTrue(exception.getMessage().contains("not found"));
    }

    @Test
    void testLoadFromPath_InvalidJson() throws IOException {
        final Path configPath = tempDir.resolve("invalid.json");
        Files.writeString(configPath, "{invalid json");

        final IllegalArgumentException exception =
                assertThrows(IllegalArgumentException.class, () -> NetworkConfigLoader.loadFromPath(configPath));

        assertTrue(exception.getMessage().contains("Invalid JSON"));
    }

    @Test
    void testLoadFromPath_MissingRequiredField() throws IOException {
        final String json = """
                {
                  "gcsBucketName": "test-bucket"
                }
                """;

        final Path configPath = tempDir.resolve("incomplete.json");
        Files.writeString(configPath, json);

        final IllegalArgumentException exception =
                assertThrows(IllegalArgumentException.class, () -> NetworkConfigLoader.loadFromPath(configPath));

        assertTrue(exception.getMessage().contains("missing required field"));
    }

    @Test
    void testFromName_Mainnet() {
        final NetworkConfig config = NetworkConfig.fromName("mainnet");
        assertEquals("mainnet", config.networkName());
        assertEquals("hedera-mainnet-streams", config.gcsBucketName());
    }

    @Test
    void testFromName_Testnet() {
        final NetworkConfig config = NetworkConfig.fromName("testnet");
        assertEquals("testnet", config.networkName());
        assertEquals("hedera-testnet-streams-2024-02", config.gcsBucketName());
    }

    @Test
    void testFromName_UnknownNetwork() {
        final IllegalArgumentException exception =
                assertThrows(IllegalArgumentException.class, () -> NetworkConfig.fromName("unknown"));

        assertTrue(exception.getMessage().contains("Unknown network"));
        assertTrue(exception.getMessage().contains("mainnet, testnet, previewnet, other"));
    }
}
